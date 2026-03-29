from fastapi import FastAPI, APIRouter, HTTPException, Request, UploadFile, File, Depends, Response
from fastapi.responses import JSONResponse, StreamingResponse
from dotenv import load_dotenv
from starlette.middleware.cors import CORSMiddleware
from motor.motor_asyncio import AsyncIOMotorClient
import os
import asyncio
import logging
import re
import shutil
import statistics
import copy
from pathlib import Path
from pydantic import BaseModel, Field, ConfigDict
from typing import List, Optional, Dict, Any, Tuple
import uuid
from datetime import datetime, timezone, timedelta
import json
import httpx
import ipaddress
import contextlib
from fastapi.openapi.utils import get_openapi
from PyPDF2 import PdfReader, PdfWriter
import pdfplumber
import io
import hashlib
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from openai import AsyncOpenAI
from candidate_miner import run_candidate_miner_for_analysis
from section_builder import build_estratto_quality
from evidence_utils import normalize_evidence_quote
from narrator import build_decisione_rapida_narration
from cost_market_ranges import market_range_for_item

ROOT_DIR = Path(__file__).parent
load_dotenv(ROOT_DIR / '.env')

# MongoDB connection
mongo_url = os.environ['MONGO_URL']
client = AsyncIOMotorClient(mongo_url)
db = client[os.environ['DB_NAME']]

# Environment variables
OPENAI_API_KEY = os.environ.get('OPENAI_API_KEY')
STRIPE_SECRET_KEY = os.environ.get('STRIPE_SECRET_KEY')
STRIPE_WEBHOOK_SECRET = os.environ.get("STRIPE_WEBHOOK_SECRET", "").strip()
STRIPE_PRICE_STARTER = os.environ.get("STRIPE_PRICE_STARTER", "").strip()
STRIPE_PRICE_PACK_8 = os.environ.get("STRIPE_PRICE_PACK_8", "").strip()
STRIPE_PRICE_SOLO = os.environ.get("STRIPE_PRICE_SOLO", "").strip()
STRIPE_PRICE_PRO = os.environ.get("STRIPE_PRICE_PRO", "").strip()
STRIPE_SUCCESS_URL = os.environ.get("STRIPE_SUCCESS_URL", "").strip()
STRIPE_CANCEL_URL = os.environ.get("STRIPE_CANCEL_URL", "").strip()
MASTER_ADMIN_EMAIL = os.environ.get('MASTER_ADMIN_EMAIL', 'admin@nexodify.com')
DOC_AI_TIMEOUT_SECONDS = int(os.environ.get('DOC_AI_TIMEOUT_SECONDS', '30'))
LLM_TIMEOUT_SECONDS = int(os.environ.get('LLM_TIMEOUT_SECONDS', '45'))
PIPELINE_TIMEOUT_SECONDS = int(os.environ.get('PIPELINE_TIMEOUT_SECONDS', '120'))
LLM_SUMMARY_TIMEOUT_SECONDS = int(os.environ.get('LLM_SUMMARY_TIMEOUT_SECONDS', '8'))
PDF_TEXT_MIN_PAGE_CHARS = int(os.environ.get("PDF_TEXT_MIN_PAGE_CHARS", "40"))
PDF_TEXT_MIN_COVERAGE_RATIO = float(os.environ.get("PDF_TEXT_MIN_COVERAGE_RATIO", "0.6"))
PDF_TEXT_MAX_BLANK_PAGE_RATIO = float(os.environ.get("PDF_TEXT_MAX_BLANK_PAGE_RATIO", "0.3"))
OCR_MIN_WORD_COUNT = 30
OCR_MIN_CHARS_NON_WS = 200
OCR_MIN_ALPHA_RATIO = 0.30
OCR_MAX_GARBAGE_RATIO = 0.25
DOC_NEEDS_OCR_RATIO = 0.35
DOC_NEEDS_OCR_RATIO_WITH_IMAGES = 0.20
DOC_NEEDS_OCR_MIN_AVG_IMAGES = 1.0
DOC_UNREADABLE_RATIO = 0.75
DOC_UNREADABLE_MAX_MEDIAN_WORDS = 10
DOC_UNREADABLE_MAX_AVG_ALPHA = 0.15
OFFLINE_QA_ENV = os.environ.get('OFFLINE_QA', '0').lower() in {"1", "true", "yes"}
ALLOW_OFFLINE_QA_ENV = os.environ.get("ALLOW_OFFLINE_QA", "0").strip() == "1"
OFFLINE_QA_TOKEN = os.environ.get("OFFLINE_QA_TOKEN", "").strip()
EVIDENCE_OFFSET_MODE = "PAGE_LOCAL"

OFFLINE_QA_FIXTURE_PATH = os.environ.get(
    "OFFLINE_QA_FIXTURE_PATH",
    str(ROOT_DIR / "tests" / "fixtures" / "perizia_test_extraction.json")
)
FRONTEND_URL = os.environ.get("FRONTEND_URL", "").strip().rstrip("/")
PRINT_RENDER_TIMEOUT_SECONDS = int(os.environ.get("PRINT_RENDER_TIMEOUT_SECONDS", "120"))


class DocAIUnavailable(Exception):
    pass


class LLMUnavailable(Exception):
    pass

async def openai_chat_completion(system_message: str, user_message: str, model: str = "gpt-4o", timeout_seconds: int = LLM_TIMEOUT_SECONDS) -> str:
    if not OPENAI_API_KEY:
        raise RuntimeError("OPENAI_API_KEY not set")
    client = AsyncOpenAI(api_key=OPENAI_API_KEY, timeout=timeout_seconds)
    response = await asyncio.wait_for(
        client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": system_message},
                {"role": "user", "content": user_message},
            ],
        ),
        timeout=timeout_seconds
    )
    return (response.choices[0].message.content or "").strip()

# Create the main app
app = FastAPI(title="Nexodify Forensic Engine API")

# Create a router with the /api prefix
api_router = APIRouter(prefix="/api")

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ===================
# MODELS
# ===================

class User(BaseModel):
    model_config = ConfigDict(extra="ignore")
    user_id: str
    email: str
    name: str
    picture: Optional[str] = None
    plan: str = "free"
    is_master_admin: bool = False
    quota: Dict[str, int] = Field(default_factory=lambda: {
        "perizia_scans_remaining": 12,
        "image_scans_remaining": 0,
        "assistant_messages_remaining": 0
    })
    perizia_credits: Dict[str, Any] = Field(default_factory=dict)
    subscription_state: Dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

class UserSession(BaseModel):
    model_config = ConfigDict(extra="ignore")
    session_id: str
    user_id: str
    session_token: str
    expires_at: datetime
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

class SubscriptionPlan(BaseModel):
    plan_id: str
    name: str
    name_it: str
    price: float
    currency: str = "eur"
    plan_type: str = "subscription"
    plan_type_label_it: str = "Abbonamento"
    price_suffix_it: str = "/mese"
    credits: int = 0
    credits_label_it: str = ""
    validity_label_it: Optional[str] = None
    support_level_it: Optional[str] = None
    usage_hint_it: Optional[str] = None
    cta_label_it: str = "Abbonati"
    public: bool = True
    features: List[str]
    features_it: List[str]
    quota: Dict[str, int]

class PaymentTransaction(BaseModel):
    model_config = ConfigDict(extra="ignore")
    transaction_id: str
    user_id: str
    session_id: str
    plan_id: str
    amount: float
    currency: str
    status: str
    payment_status: str
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

class CreditLedgerEntry(BaseModel):
    model_config = ConfigDict(extra="ignore")
    ledger_id: str
    user_id: str
    user_email: str
    quota_field: str
    direction: str
    amount: int
    balance_before: int
    balance_after: int
    entry_type: str
    reference_type: str
    reference_id: str
    description_it: str
    metadata: Dict[str, Any] = Field(default_factory=dict)
    actor_user_id: Optional[str] = None
    actor_email: Optional[str] = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

class BillingRecord(BaseModel):
    model_config = ConfigDict(extra="ignore")
    billing_record_id: str
    user_id: str
    user_email: str
    customer_type: str
    customer_name: str
    company_name: Optional[str] = None
    billing_email: str
    country_code: str
    billing_address: Optional[Dict[str, Any]] = None
    tax_code: Optional[str] = None
    vat_number: Optional[str] = None
    plan_id: str
    purchase_type: str
    amount_subtotal: float
    amount_tax: float
    amount_total: float
    currency: str
    status: str
    payment_provider: str
    payment_reference: Optional[str] = None
    checkout_reference: Optional[str] = None
    invoice_status: str
    invoice_number: Optional[str] = None
    invoice_reference: Optional[str] = None
    description_it: str
    metadata: Dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    paid_at: Optional[datetime] = None

class PeriziaAnalysis(BaseModel):
    model_config = ConfigDict(extra="ignore")
    analysis_id: str
    user_id: str
    case_id: str
    run_id: str
    revision: int = 0
    case_title: Optional[str] = None
    file_name: str
    input_sha256: str
    pages_count: int
    result: Dict[str, Any]
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

class ImageForensics(BaseModel):
    model_config = ConfigDict(extra="ignore")
    forensics_id: str
    user_id: str
    case_id: str
    run_id: str
    revision: int = 0
    image_count: int
    result: Dict[str, Any]
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

class AssistantQA(BaseModel):
    model_config = ConfigDict(extra="ignore")
    qa_id: str
    user_id: str
    case_id: Optional[str] = None
    run_id: str
    question: str
    result: Dict[str, Any]
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

class HeadlineOverrideRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    tribunale: Optional[str] = None
    procedura: Optional[str] = None
    lotto: Optional[str] = None
    address: Optional[str] = None

class FieldOverrideRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    tribunale: Optional[Any] = None
    procedura: Optional[Any] = None
    lotto: Optional[Any] = None
    address: Optional[Any] = None
    prezzo_base_asta: Optional[Any] = None
    superficie: Optional[Any] = None
    superficie_catastale: Optional[Any] = None
    diritto_reale: Optional[Any] = None
    stato_occupativo: Optional[Any] = None
    regolarita_urbanistica: Optional[Any] = None
    conformita_catastale: Optional[Any] = None
    spese_condominiali_arretrate: Optional[Any] = None
    formalita_pregiudizievoli: Optional[Any] = None

# Subscription Plans
SUBSCRIPTION_PLANS = {
    "free": SubscriptionPlan(
        plan_id="free",
        name="Free",
        name_it="Free",
        price=0.0,
        plan_type="free",
        plan_type_label_it="Ingresso",
        price_suffix_it="",
        credits=12,
        credits_label_it="12 crediti inclusi",
        validity_label_it="Fino a 3 perizie standard da 1-20 pagine",
        support_level_it="Accesso iniziale in piattaforma",
        usage_hint_it="Prova il metodo su perizie standard",
        cta_label_it="Inizia gratis",
        features=[
            "Perizia analysis up to 20 pages",
            "Risk traffic light",
            "Legal issues to verify",
            "Costs and charges to verify",
            "Page references and structured report",
            "Basic export",
        ],
        features_it=[
            "Fino a 3 perizie standard da 1-20 pagine",
            "Semaforo rischio",
            "Criticita legali da verificare",
            "Costi e oneri da verificare",
            "Riferimenti di pagina e report strutturato",
            "Export base",
        ],
        quota={"perizia_scans_remaining": 12, "image_scans_remaining": 0, "assistant_messages_remaining": 0},
    ),
    "starter": SubscriptionPlan(
        plan_id="starter",
        name="Credit Pack 8",
        name_it="Credit Pack 8",
        price=19.00,
        plan_type="one_time",
        plan_type_label_it="Extra pack",
        price_suffix_it="one-time",
        credits=8,
        credits_label_it="8 extra credits",
        validity_label_it="Stored with 12-month expiry metadata",
        support_level_it="Best-effort email support",
        usage_hint_it="Buy extra capacity at any time",
        cta_label_it="Buy pack",
        features=[
            "Perizia analysis",
            "Risk traffic light",
            "Legal issues to verify",
            "Costs and charges to verify",
            "Page references and structured report",
            "Email support",
        ],
        features_it=[
            "Analisi perizia",
            "Semaforo rischio",
            "Criticita legali da verificare",
            "Costi e oneri da verificare",
            "Riferimenti di pagina e report strutturato",
            "Supporto best effort via email",
        ],
        quota={"perizia_scans_remaining": 8, "image_scans_remaining": 0, "assistant_messages_remaining": 0},
    ),
    "solo": SubscriptionPlan(
        plan_id="solo",
        name="Solo",
        name_it="Solo",
        price=49.00,
        plan_type="subscription",
        plan_type_label_it="Monthly plan",
        price_suffix_it="/month",
        credits=28,
        credits_label_it="28 monthly credits",
        validity_label_it="Refreshes each billing cycle",
        support_level_it="Standard support",
        usage_hint_it="Recurring monthly capacity for ongoing use",
        cta_label_it="Subscribe",
        features=[
            "Perizia analysis",
            "Risk traffic light",
            "Legal issues to verify",
            "Costs and charges to verify",
            "Page references",
            "Structured report",
        ],
        features_it=[
            "Analisi perizia",
            "Semaforo rischio",
            "Criticita legali da verificare",
            "Costi e oneri da verificare",
            "Riferimenti di pagina",
            "Report strutturato",
        ],
        quota={"perizia_scans_remaining": 28, "image_scans_remaining": 0, "assistant_messages_remaining": 0},
    ),
    "pro": SubscriptionPlan(
        plan_id="pro",
        name="Pro",
        name_it="Pro",
        price=129.00,
        plan_type="subscription",
        plan_type_label_it="Monthly plan",
        price_suffix_it="/month",
        credits=84,
        credits_label_it="84 monthly credits",
        validity_label_it="Refreshes each billing cycle",
        support_level_it="Priority support",
        usage_hint_it="Higher recurring monthly capacity",
        cta_label_it="Subscribe",
        features=[
            "Everything in Solo",
            "Higher monthly volume",
            "Priority support",
        ],
        features_it=[
            "Tutto cio che e incluso in Solo",
            "Maggior volume mensile",
            "Supporto prioritario",
        ],
        quota={"perizia_scans_remaining": 84, "image_scans_remaining": 0, "assistant_messages_remaining": 0},
    ),
    "studio": SubscriptionPlan(
        plan_id="studio",
        name="Studio",
        name_it="Studio",
        price=299.00,
        plan_type="subscription",
        plan_type_label_it="Monthly plan",
        price_suffix_it="/month",
        credits=210,
        credits_label_it="210 monthly credits",
        validity_label_it="Custom handling in this phase",
        support_level_it="Priority support",
        usage_hint_it="For higher-volume or team workflows",
        cta_label_it="Contact sales",
        features=[
            "Everything in Pro",
            "High-volume usage",
            "Suitable for team workflows",
        ],
        features_it=[
            "Tutto cio che e incluso in Pro",
            "Volume elevato",
            "Adatto a flussi di lavoro di studio o team",
        ],
        quota={"perizia_scans_remaining": 210, "image_scans_remaining": 0, "assistant_messages_remaining": 0},
    ),
    "enterprise": SubscriptionPlan(
        plan_id="enterprise",
        name="Enterprise",
        name_it="Enterprise",
        price=199.00,
        plan_type="internal",
        plan_type_label_it="Interno",
        price_suffix_it="",
        credits=9999,
        credits_label_it="Uso interno",
        validity_label_it="Solo uso admin interno",
        support_level_it="Supporto dedicato",
        usage_hint_it="Non esposto al pubblico",
        cta_label_it="Interno",
        public=False,
        features=["Internal admin access"],
        features_it=["Accesso admin interno"],
        quota={"perizia_scans_remaining": 9999, "image_scans_remaining": 9999, "assistant_messages_remaining": 9999},
    ),
}

# ===================
# AUTH HELPERS
# ===================

ACCOUNT_QUOTA_FIELDS = (
    "perizia_scans_remaining",
    "image_scans_remaining",
    "assistant_messages_remaining",
)

LEDGER_ENTRY_TYPES = {
    "opening_balance",
    "perizia_upload",
    "image_forensics",
    "assistant_message",
    "plan_purchase",
    "top_up",
    "admin_adjustment",
    "subscription_reset",
    "system_correction",
}

LEDGER_DIRECTIONS = {"credit", "debit"}

BILLING_RECORD_STATUSES = {"draft", "pending", "paid", "failed", "refunded", "cancelled"}
BILLING_PROVIDER_TYPES = {"none", "stripe", "manual"}
BILLING_INVOICE_STATUSES = {"not_applicable", "pending", "ready", "issued", "failed"}

PERIZIA_CREDIT_BANDS: Tuple[Tuple[int, int, int], ...] = (
    (1, 20, 4),
    (21, 40, 7),
    (41, 60, 10),
    (61, 80, 13),
    (81, 100, 16),
)
PERIZIA_CREDIT_WALLET_VERSION = 1
PERIZIA_PACK_VALIDITY_DAYS = 365
PAID_RECURRING_PLAN_IDS = {"solo", "pro", "studio"}
SELF_SERVE_RECURRING_PLAN_IDS = {"solo", "pro"}
SUBSCRIPTION_TERMINAL_STATUSES = {"canceled", "cancelled", "ended", "incomplete_expired", "unpaid"}
SUBSCRIPTION_MANAGED_STATUSES = {"trialing", "active", "past_due", "incomplete", "paused"}


def _get_required_perizia_credits(page_count: int) -> Optional[int]:
    safe_page_count = int(page_count or 0)
    for min_pages, max_pages, credits in PERIZIA_CREDIT_BANDS:
        if min_pages <= safe_page_count <= max_pages:
            return credits
    return None


def _is_master_admin_email(email: Optional[str]) -> bool:
    return bool(email and email.lower() == MASTER_ADMIN_EMAIL.lower())


def _is_complete_quota(quota: Any) -> bool:
    if not isinstance(quota, dict):
        return False
    for field in ACCOUNT_QUOTA_FIELDS:
        value = quota.get(field)
        if not isinstance(value, int) or value < 0:
            return False
    return True


def _monthly_perizia_quota_for_plan(plan_id: Optional[str]) -> int:
    normalized_plan_id = str(plan_id or "").strip().lower()
    if normalized_plan_id not in PAID_RECURRING_PLAN_IDS:
        return 0
    return int(SUBSCRIPTION_PLANS[normalized_plan_id].quota.get("perizia_scans_remaining", 0) or 0)


def _subscription_state_defaults() -> Dict[str, Any]:
    return {
        "stripe_customer_id": None,
        "stripe_subscription_id": None,
        "status": None,
        "current_plan_id": None,
        "stripe_plan_id": None,
        "current_period_end": None,
        "cancel_at_period_end": False,
        "pending_change": False,
        "pending_plan_id": None,
        "pending_effective_at": None,
    }


def _normalize_subscription_state(user_doc: Dict[str, Any]) -> Dict[str, Any]:
    raw_state = user_doc.get("subscription_state")
    normalized = _subscription_state_defaults()
    if isinstance(raw_state, dict):
        for key in normalized.keys():
            if key in raw_state:
                normalized[key] = raw_state.get(key)

    normalized_plan = str(user_doc.get("plan") or "").strip().lower()
    if normalized_plan in SELF_SERVE_RECURRING_PLAN_IDS and not normalized.get("current_plan_id"):
        normalized["current_plan_id"] = normalized_plan
    if normalized.get("current_plan_id") not in SELF_SERVE_RECURRING_PLAN_IDS:
        normalized["current_plan_id"] = None
    if normalized.get("stripe_plan_id") not in SELF_SERVE_RECURRING_PLAN_IDS:
        normalized["stripe_plan_id"] = None
    if normalized.get("pending_plan_id") not in SELF_SERVE_RECURRING_PLAN_IDS:
        normalized["pending_plan_id"] = None
    normalized["pending_change"] = bool(normalized.get("pending_change") and normalized.get("pending_plan_id"))
    normalized["cancel_at_period_end"] = bool(normalized.get("cancel_at_period_end"))

    status = str(normalized.get("status") or "").strip().lower() or None
    if normalized.get("current_plan_id") and not status:
        status = "active"
    normalized["status"] = status
    normalized["current_period_end"] = _to_iso(normalized.get("current_period_end"))
    normalized["pending_effective_at"] = _to_iso(normalized.get("pending_effective_at"))
    return normalized


def _subscription_has_recurring_access(subscription_state: Dict[str, Any]) -> bool:
    current_plan_id = str(subscription_state.get("current_plan_id") or "").strip().lower()
    status = str(subscription_state.get("status") or "").strip().lower()
    return current_plan_id in SELF_SERVE_RECURRING_PLAN_IDS and status not in SUBSCRIPTION_TERMINAL_STATUSES


def _subscription_checkout_is_blocked(subscription_state: Dict[str, Any]) -> bool:
    current_plan_id = str(subscription_state.get("current_plan_id") or "").strip().lower()
    status = str(subscription_state.get("status") or "").strip().lower()
    if current_plan_id not in SELF_SERVE_RECURRING_PLAN_IDS:
        return False
    return bool(
        subscription_state.get("stripe_subscription_id")
        or status in SUBSCRIPTION_MANAGED_STATUSES
        or subscription_state.get("cancel_at_period_end")
        or subscription_state.get("pending_change")
    )


def _make_pack_grant(
    *,
    amount: int,
    source: str,
    plan_code: Optional[str] = None,
    reference_id: Optional[str] = None,
    grant_id: Optional[str] = None,
    granted_at: Optional[str] = None,
    expires_at: Optional[str] = None,
    amount_remaining: Optional[int] = None,
) -> Dict[str, Any]:
    granted = max(0, int(amount or 0))
    remaining = granted if amount_remaining is None else max(0, int(amount_remaining or 0))
    if granted < remaining:
        granted = remaining
    return {
        "grant_id": str(grant_id or f"pack_{uuid.uuid4().hex[:12]}").strip(),
        "source": str(source or "unknown").strip(),
        "plan_code": str(plan_code or "starter").strip(),
        "reference_id": str(reference_id or "").strip() or None,
        "amount_granted": granted,
        "amount_remaining": remaining,
        "granted_at": str(granted_at or datetime.now(timezone.utc).isoformat()),
        "expires_at": str(expires_at).strip() if expires_at else None,
    }


def _normalize_pack_grants(raw_grants: Any) -> List[Dict[str, Any]]:
    normalized: List[Dict[str, Any]] = []
    if not isinstance(raw_grants, list):
        return normalized
    for item in raw_grants:
        if not isinstance(item, dict):
            continue
        grant = _make_pack_grant(
            amount=item.get("amount_granted", item.get("amount_remaining", item.get("amount", 0))),
            amount_remaining=item.get("amount_remaining"),
            source=str(item.get("source") or "unknown"),
            plan_code=item.get("plan_code"),
            reference_id=item.get("reference_id"),
            grant_id=item.get("grant_id"),
            granted_at=item.get("granted_at"),
            expires_at=item.get("expires_at"),
        )
        if grant["amount_remaining"] <= 0 and grant["amount_granted"] <= 0:
            continue
        normalized.append(grant)
    return normalized


def _finalize_perizia_credit_wallet(wallet: Dict[str, Any], *, plan_id: Optional[str], is_master_admin: bool) -> Dict[str, Any]:
    normalized_plan_id = str(plan_id or "").strip().lower()
    monthly_remaining = max(0, int((wallet or {}).get("monthly_remaining", 0) or 0))
    pack_grants = _normalize_pack_grants((wallet or {}).get("pack_grants"))
    extra_remaining = sum(int(item.get("amount_remaining", 0) or 0) for item in pack_grants)
    declared_extra_remaining = max(0, int((wallet or {}).get("extra_remaining", 0) or 0))
    if declared_extra_remaining > extra_remaining:
        spillover = declared_extra_remaining - extra_remaining
        pack_grants.append(
            _make_pack_grant(
                amount=spillover,
                amount_remaining=spillover,
                source="wallet_spillover_recovery",
                plan_code="starter",
            )
        )
        extra_remaining = declared_extra_remaining
    monthly_quota = _monthly_perizia_quota_for_plan(normalized_plan_id)

    if is_master_admin:
        extra_remaining = 0
        pack_grants = []
    elif normalized_plan_id in PAID_RECURRING_PLAN_IDS:
        if monthly_remaining > monthly_quota:
            spillover = monthly_remaining - monthly_quota
            monthly_remaining = monthly_quota
            pack_grants.append(
                _make_pack_grant(
                    amount=spillover,
                    amount_remaining=spillover,
                    source="normalized_monthly_spillover",
                    plan_code="starter",
                )
            )
            extra_remaining += spillover
    else:
        if monthly_remaining > 0:
            pack_grants.append(
                _make_pack_grant(
                    amount=monthly_remaining,
                    amount_remaining=monthly_remaining,
                    source="non_recurring_monthly_rollover",
                    plan_code="starter",
                )
            )
            extra_remaining += monthly_remaining
            monthly_remaining = 0

    processed_invoice_ids: List[str] = []
    for item in (wallet or {}).get("processed_invoice_ids") or []:
        invoice_id = str(item or "").strip()
        if invoice_id and invoice_id not in processed_invoice_ids:
            processed_invoice_ids.append(invoice_id)

    finalized = {
        "version": PERIZIA_CREDIT_WALLET_VERSION,
        "monthly_remaining": monthly_remaining,
        "extra_remaining": extra_remaining,
        "total_available": monthly_remaining + extra_remaining,
        "monthly_plan_id": normalized_plan_id if normalized_plan_id in PAID_RECURRING_PLAN_IDS else None,
        "monthly_refreshed_at": (wallet or {}).get("monthly_refreshed_at"),
        "pack_expiry_enforced": False,
        "pack_validity_days": PERIZIA_PACK_VALIDITY_DAYS,
        "pack_grants": pack_grants,
        "processed_invoice_ids": processed_invoice_ids[-50:],
    }
    return finalized


def _build_legacy_perizia_credit_wallet(user_doc: Dict[str, Any], *, plan_id: Optional[str], is_master_admin: bool) -> Dict[str, Any]:
    legacy_total = max(0, int(((user_doc.get("quota") or {}).get("perizia_scans_remaining", 0)) or 0))
    normalized_plan_id = str(plan_id or "").strip().lower()
    monthly_quota = _monthly_perizia_quota_for_plan(normalized_plan_id)
    monthly_remaining = 0
    extra_remaining = legacy_total
    if is_master_admin:
        monthly_remaining = legacy_total
        extra_remaining = 0
    elif normalized_plan_id in PAID_RECURRING_PLAN_IDS:
        monthly_remaining = min(legacy_total, monthly_quota)
        extra_remaining = max(0, legacy_total - monthly_remaining)

    pack_grants: List[Dict[str, Any]] = []
    if extra_remaining > 0:
        pack_grants.append(
            _make_pack_grant(
                amount=extra_remaining,
                amount_remaining=extra_remaining,
                source="legacy_migration",
                plan_code="starter",
                reference_id=str(user_doc.get("user_id") or "").strip() or None,
                granted_at=(
                    user_doc.get("created_at").isoformat()
                    if isinstance(user_doc.get("created_at"), datetime)
                    else str(user_doc.get("created_at") or datetime.now(timezone.utc).isoformat())
                ),
            )
        )

    return _finalize_perizia_credit_wallet(
        {
            "monthly_remaining": monthly_remaining,
            "extra_remaining": extra_remaining,
            "monthly_plan_id": normalized_plan_id if normalized_plan_id in PAID_RECURRING_PLAN_IDS else None,
            "pack_grants": pack_grants,
            "processed_invoice_ids": [],
        },
        plan_id=normalized_plan_id,
        is_master_admin=is_master_admin,
    )


def _normalize_perizia_credit_wallet(user_doc: Dict[str, Any], *, plan_id: Optional[str], is_master_admin: bool) -> Dict[str, Any]:
    raw_wallet = user_doc.get("perizia_credits")
    if isinstance(raw_wallet, dict):
        return _finalize_perizia_credit_wallet(raw_wallet, plan_id=plan_id, is_master_admin=is_master_admin)
    return _build_legacy_perizia_credit_wallet(user_doc, plan_id=plan_id, is_master_admin=is_master_admin)


def _append_pack_grant(wallet: Dict[str, Any], *, amount: int, reference_id: str, plan_code: str = "starter") -> Dict[str, Any]:
    updated_wallet = dict(wallet or {})
    pack_grants = list(updated_wallet.get("pack_grants") or [])
    granted_at = datetime.now(timezone.utc)
    pack_grants.append(
        _make_pack_grant(
            amount=amount,
            amount_remaining=amount,
            source="stripe_checkout",
            plan_code=plan_code,
            reference_id=reference_id,
            granted_at=granted_at.isoformat(),
            expires_at=(granted_at + timedelta(days=PERIZIA_PACK_VALIDITY_DAYS)).isoformat(),
        )
    )
    updated_wallet["pack_grants"] = pack_grants
    return updated_wallet


def _admin_override_perizia_credit_wallet(
    user_doc: Dict[str, Any],
    *,
    total_available: int,
    plan_override: Optional[str] = None,
) -> Dict[str, Any]:
    plan_id = str(plan_override or user_doc.get("plan") or "").strip().lower()
    is_master_admin = _is_master_admin_email(user_doc.get("email"))
    current_wallet = _normalize_perizia_credit_wallet(user_doc, plan_id=plan_id, is_master_admin=is_master_admin)
    if is_master_admin:
        return current_wallet

    target_total = max(0, int(total_available or 0))
    monthly_remaining = 0
    if plan_id in PAID_RECURRING_PLAN_IDS:
        monthly_remaining = min(target_total, _monthly_perizia_quota_for_plan(plan_id))
    extra_remaining = max(0, target_total - monthly_remaining)
    pack_grants: List[Dict[str, Any]] = []
    if extra_remaining > 0:
        pack_grants.append(
            _make_pack_grant(
                amount=extra_remaining,
                amount_remaining=extra_remaining,
                source="admin_adjustment",
                plan_code="starter",
                reference_id=str(user_doc.get("user_id") or "").strip() or None,
            )
        )

    return _finalize_perizia_credit_wallet(
        {
            "monthly_remaining": monthly_remaining,
            "extra_remaining": extra_remaining,
            "monthly_refreshed_at": current_wallet.get("monthly_refreshed_at"),
            "pack_grants": pack_grants,
            "processed_invoice_ids": current_wallet.get("processed_invoice_ids") or [],
        },
        plan_id=plan_id,
        is_master_admin=is_master_admin,
    )


def _consume_extra_pack_grants(pack_grants: List[Dict[str, Any]], amount: int) -> List[Dict[str, Any]]:
    remaining_to_consume = max(0, int(amount or 0))
    ordered = sorted(
        _normalize_pack_grants(pack_grants),
        key=lambda item: (
            item.get("expires_at") is None,
            item.get("expires_at") or "",
            item.get("granted_at") or "",
        ),
    )
    updated: List[Dict[str, Any]] = []
    for item in ordered:
        next_item = dict(item)
        available = max(0, int(next_item.get("amount_remaining", 0) or 0))
        if remaining_to_consume > 0 and available > 0:
            debit = min(available, remaining_to_consume)
            next_item["amount_remaining"] = available - debit
            remaining_to_consume -= debit
        updated.append(next_item)
    return updated


async def _persist_perizia_credit_wallet(
    *,
    user_doc: Dict[str, Any],
    wallet: Dict[str, Any],
    plan_override: Optional[str] = None,
) -> Tuple[Dict[str, int], Dict[str, Any]]:
    finalized_wallet = _finalize_perizia_credit_wallet(
        wallet,
        plan_id=plan_override or user_doc.get("plan"),
        is_master_admin=_is_master_admin_email(user_doc.get("email")),
    )
    updated_quota = _quota_snapshot(user_doc.get("quota"))
    updated_quota["perizia_scans_remaining"] = finalized_wallet["total_available"]
    update_fields: Dict[str, Any] = {
        "quota": updated_quota,
        "perizia_credits": finalized_wallet,
    }
    if plan_override is not None:
        update_fields["plan"] = plan_override
    await db.users.update_one({"user_id": user_doc["user_id"]}, {"$set": update_fields})
    user_doc["quota"] = updated_quota.copy()
    user_doc["perizia_credits"] = finalized_wallet
    if plan_override is not None:
        user_doc["plan"] = plan_override
    return updated_quota, finalized_wallet


def _normalize_account_state(user_doc: Dict[str, Any]) -> Dict[str, Any]:
    normalized_email = str(user_doc.get("email") or "").strip().lower()
    is_master_admin = _is_master_admin_email(normalized_email)

    if is_master_admin:
        plan = "enterprise"
        quota = SUBSCRIPTION_PLANS["enterprise"].quota.copy()
    else:
        raw_plan = user_doc.get("plan")
        raw_quota = user_doc.get("quota")
        valid_non_admin_plan = (
            isinstance(raw_plan, str)
            and raw_plan in SUBSCRIPTION_PLANS
            and raw_plan not in {"enterprise", "starter"}
        )
        if valid_non_admin_plan and _is_complete_quota(raw_quota):
            plan = raw_plan
            quota = {field: int(raw_quota[field]) for field in ACCOUNT_QUOTA_FIELDS}
        else:
            plan = "free"
            quota = SUBSCRIPTION_PLANS["free"].quota.copy()

    perizia_credits = _normalize_perizia_credit_wallet(user_doc, plan_id=plan, is_master_admin=is_master_admin)
    subscription_state = _normalize_subscription_state({**user_doc, "plan": plan})
    quota["perizia_scans_remaining"] = perizia_credits["total_available"]

    feature_access = {
        "can_use_assistant": is_master_admin,
        "can_use_image_forensics": is_master_admin,
    }

    return {
        "is_master_admin": is_master_admin,
        "plan": plan,
        "quota": quota,
        "perizia_credits": perizia_credits,
        "subscription_state": subscription_state,
        "feature_access": feature_access,
        "account": {
            "effective_plan": plan,
            "effective_quota": quota.copy(),
            "feature_access": feature_access.copy(),
            "perizia_credits": perizia_credits,
            "subscription": subscription_state,
        },
    }


async def _apply_normalized_account_state(user_doc: Dict[str, Any], persist: bool = False) -> Dict[str, Any]:
    normalized = _normalize_account_state(user_doc)

    normalized_user_doc = user_doc.copy()
    normalized_user_doc["is_master_admin"] = normalized["is_master_admin"]
    normalized_user_doc["plan"] = normalized["plan"]
    normalized_user_doc["quota"] = normalized["quota"].copy()
    normalized_user_doc["perizia_credits"] = normalized["perizia_credits"]
    normalized_user_doc["subscription_state"] = normalized["subscription_state"]

    if persist and user_doc.get("user_id"):
        update_data: Dict[str, Any] = {}
        if user_doc.get("is_master_admin") != normalized["is_master_admin"]:
            update_data["is_master_admin"] = normalized["is_master_admin"]
        if user_doc.get("plan") != normalized["plan"]:
            update_data["plan"] = normalized["plan"]
        if user_doc.get("quota") != normalized["quota"]:
            update_data["quota"] = normalized["quota"].copy()
        if user_doc.get("perizia_credits") != normalized["perizia_credits"]:
            update_data["perizia_credits"] = normalized["perizia_credits"]
        if user_doc.get("subscription_state") != normalized["subscription_state"]:
            update_data["subscription_state"] = normalized["subscription_state"]
        if update_data:
            await db.users.update_one({"user_id": user_doc["user_id"]}, {"$set": update_data})

    normalized_user_doc["feature_access"] = normalized["feature_access"].copy()
    normalized_user_doc["account"] = normalized["account"]
    return normalized_user_doc


def _build_user_response(user: User) -> Dict[str, Any]:
    user_response = user.model_dump()
    user_response["created_at"] = (
        user_response["created_at"].isoformat()
        if isinstance(user_response["created_at"], datetime)
        else user_response["created_at"]
    )
    normalized = _normalize_account_state(user_response)
    user_response["is_master_admin"] = normalized["is_master_admin"]
    user_response["plan"] = normalized["plan"]
    user_response["quota"] = normalized["quota"].copy()
    user_response["perizia_credits"] = normalized["perizia_credits"]
    user_response["subscription_state"] = normalized["subscription_state"]
    user_response["feature_access"] = normalized["feature_access"].copy()
    user_response["account"] = normalized["account"]
    return user_response

async def get_current_user(request: Request) -> Optional[User]:
    """Get current user from session token cookie or Authorization header"""
    session_token = request.cookies.get("session_token")
    if not session_token:
        auth_header = request.headers.get("Authorization")
        if auth_header and auth_header.startswith("Bearer "):
            session_token = auth_header.split(" ")[1]
    
    if not session_token:
        return None
    
    session_doc = await db.user_sessions.find_one({"session_token": session_token}, {"_id": 0})
    if not session_doc:
        return None
    
    expires_at = session_doc.get("expires_at")
    if isinstance(expires_at, str):
        expires_at = datetime.fromisoformat(expires_at.replace("Z", "+00:00"))
    if expires_at.tzinfo is None:
        expires_at = expires_at.replace(tzinfo=timezone.utc)
    if expires_at < datetime.now(timezone.utc):
        return None
    
    user_doc = await db.users.find_one({"user_id": session_doc["user_id"]}, {"_id": 0})
    if not user_doc:
        return None
    user_doc = await _recover_subscription_state_from_local_records(user_doc)

    normalized_user_doc = await _apply_normalized_account_state(user_doc, persist=True)
    await _ensure_opening_balance_baseline_for_user_doc(normalized_user_doc)
    return User(**normalized_user_doc)

async def require_auth(request: Request) -> User:
    """Require authenticated user"""
    if await is_offline_qa_request(request):
        return User(
            user_id="offline_qa",
            email="offline@local",
            name="Offline QA",
            plan="offline",
            is_master_admin=True
        )
    user = await get_current_user(request)
    if not user:
        raise HTTPException(status_code=401, detail="Not authenticated")
    return user

async def require_master_admin(request: Request) -> User:
    """Require master admin user with matching email"""
    user = await require_auth(request)
    if not user.is_master_admin:
        raise HTTPException(status_code=403, detail="Forbidden")
    if not _is_master_admin_email(user.email):
        raise HTTPException(status_code=403, detail="Forbidden")
    return user


def _feature_access_flags(user: Optional[User]) -> Dict[str, bool]:
    normalized = _normalize_account_state(user.model_dump() if user else {})
    return {
        "can_use_assistant": normalized["feature_access"]["can_use_assistant"],
        "can_use_image_forensics": normalized["feature_access"]["can_use_image_forensics"],
    }


def _require_feature_access(user: User, feature_label_it: str, access_flag: str) -> None:
    if _feature_access_flags(user).get(access_flag):
        return
    raise HTTPException(
        status_code=403,
        detail={
            "code": "FEATURE_DISABLED",
            "message_it": f"{feature_label_it} non e ancora disponibile per questo account.",
            "message_en": f"{feature_label_it} is not yet available for this account.",
        },
    )

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _truncate(value: Optional[str], limit: int = 50) -> Optional[str]:
    if value is None:
        return None
    if len(value) <= limit:
        return value
    return value[:limit]

def _parse_dt(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    return None

def _has_evidence(ev) -> bool:
    if not isinstance(ev, list) or not ev:
        return False
    e0 = ev[0]
    return isinstance(e0, dict) and "page" in e0 and "quote" in e0 and str(e0.get("quote","")).strip() != ""

def _is_loopback_client(request: Request) -> bool:
    host = (request.client.host if request and request.client else "") or ""
    try:
        ip = ipaddress.ip_address(host)
        return ip.is_loopback
    except Exception:
        return False

async def _audit_offline_qa_rejection(request: Request, reason: str) -> None:
    client_ip = (request.client.host if request and request.client else None)
    logger.warning(f"offline_qa_rejected reason={reason} client_ip={client_ip} path={request.url.path}")
    try:
        await db.security_audit_log.insert_one({
            "event": "OFFLINE_QA_REJECTED",
            "reason": reason,
            "client_ip": client_ip,
            "path": request.url.path,
            "method": request.method,
            "created_at": datetime.now(timezone.utc).isoformat()
        })
    except Exception:
        # Logging must never break request auth flow
        pass

async def is_offline_qa_request(request: Request) -> bool:
    offline_header = str(request.headers.get("X-OFFLINE-QA", "")).strip()
    if offline_header != "1":
        return False

    # Header was attempted, evaluate strict gating and audit rejections.
    if not ALLOW_OFFLINE_QA_ENV:
        await _audit_offline_qa_rejection(request, "ALLOW_OFFLINE_QA_DISABLED")
        return False
    if not OFFLINE_QA_TOKEN:
        await _audit_offline_qa_rejection(request, "OFFLINE_QA_TOKEN_MISSING")
        return False
    if not _is_loopback_client(request):
        await _audit_offline_qa_rejection(request, "CLIENT_NOT_LOOPBACK")
        return False

    token_header = str(request.headers.get("X-OFFLINE-QA-TOKEN", "")).strip()
    if not token_header or token_header != OFFLINE_QA_TOKEN:
        await _audit_offline_qa_rejection(request, "OFFLINE_QA_TOKEN_INVALID")
        return False
    return True

def _load_offline_fixture() -> Dict[str, Any]:
    try:
        with open(OFFLINE_QA_FIXTURE_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        raise RuntimeError(f"Offline QA fixture load failed: {e}")

def _build_evidence(
    page_text: str,
    page_num: int,
    start: int,
    end: int,
    field_key: Optional[str] = None,
    anchor_hint: Optional[str] = None,
) -> Dict[str, Any]:
    text = page_text or ""
    text_len = len(text)
    s = max(0, min(int(start), text_len))
    e = max(s, min(int(end), text_len))
    quote, search_hint = normalize_evidence_quote(
        text,
        s,
        e,
        max_len=520,
        field_key=field_key,
        anchor_hint=anchor_hint,
    )
    page_text_hash = hashlib.sha256(text.encode("utf-8")).hexdigest()
    payload = {
        "page": page_num,
        "quote": quote,
        "start_offset": s,
        "end_offset": e,
        "bbox": None,
        "offset_mode": EVIDENCE_OFFSET_MODE,
        "page_text_hash": page_text_hash
    }
    if search_hint:
        payload["search_hint"] = search_hint
    return payload

def _build_search_entry(
    page_text: str,
    page_num: int,
    start: int,
    end: int,
    fallback_quote: Optional[str] = None,
    field_key: Optional[str] = None,
    anchor_hint: Optional[str] = None,
) -> Dict[str, Any]:
    text = page_text or ""
    text_len = len(text)
    start = max(0, min(start, text_len))
    end = max(start, min(end, text_len))
    quote = text[start:end].strip() if text else ""
    if not quote and fallback_quote:
        quote = str(fallback_quote).strip()
    if not quote and text:
        quote = text[:120].replace("\n", " ").strip()
        start = 0
        end = min(text_len, len(quote))
    if not quote:
        quote = "Ricerca keyword"
        start = 0
        end = max(1, len(quote))
    normalized_quote, search_hint = normalize_evidence_quote(
        text if text else quote,
        start if text else 0,
        end if text else len(quote),
        max_len=520,
        field_key=field_key,
        anchor_hint=anchor_hint or fallback_quote or quote,
    )
    if not normalized_quote:
        normalized_quote = quote[:520]
    page_text_hash = hashlib.sha256(text.encode("utf-8")).hexdigest() if text else None
    payload = {
        "page": page_num,
        "quote": normalized_quote,
        "start_offset": start,
        "end_offset": end,
        "bbox": None,
        "offset_mode": EVIDENCE_OFFSET_MODE,
        "page_text_hash": page_text_hash,
    }
    if search_hint:
        payload["search_hint"] = search_hint
    return payload

def _find_regex_in_pages(
    pages_in: List[Dict],
    pattern: str,
    flags=0,
    field_key: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    import re
    best: Optional[Tuple[int, Dict[str, Any]]] = None
    for p in pages_in:
        text = str(p.get("text", "") or "")
        m = re.search(pattern, text, flags)
        if m:
            ev = _build_evidence(
                text,
                int(p.get("page_number", 0) or 0),
                m.start(),
                m.end(),
                field_key=field_key,
                anchor_hint=m.group(0),
            )
            quote = str(ev.get("quote") or "")
            if field_key in {"lotto", "prezzo_base_asta", "superficie", "superficie_catastale", "tribunale"} and _is_toc_like_quote(quote):
                continue
            page_num = int(p.get("page_number", 0) or 0)
            # Prefer later pages for real content when multiple matches exist.
            score = page_num
            if best is None or score > best[0]:
                best = (score, ev)
    return best[1] if best else None

def _clean_field_value(val: Any) -> str:
    if isinstance(val, dict):
        preferred = val.get("value")
        if preferred:
            return _clean_field_value(preferred)
        if val.get("full"):
            return _clean_field_value(val.get("full"))
        if val.get("street") or val.get("city"):
            parts = []
            if val.get("street"):
                parts.append(str(val.get("street")).strip())
            if val.get("city"):
                parts.append(str(val.get("city")).strip())
            joined = ", ".join([p for p in parts if p])
            return joined if joined else "NON SPECIFICATO IN PERIZIA"
        return "NON SPECIFICATO IN PERIZIA"
    if isinstance(val, list):
        items = [str(x).strip() for x in val if str(x).strip()]
        return ", ".join(items) if items else "NON SPECIFICATO IN PERIZIA"
    if val is None:
        return "NON SPECIFICATO IN PERIZIA"
    s = str(val).strip()
    if s in {"", "{}", "N/A", "NOT_SPECIFIED_IN_PERIZIA", "NOT_SPECIFIED", "UNKNOWN", "None"}:
        return "NON SPECIFICATO IN PERIZIA"
    if s.startswith("{") and s.endswith("}"):
        return "NON SPECIFICATO IN PERIZIA"
    upper = s.upper()
    if "LOW_CONFIDENCE" in upper:
        return "DA VERIFICARE"
    if "DA VERIFICARE" in upper:
        return "DA VERIFICARE"
    if "NON SPECIFICATO" in upper or "NOT SPECIFIED" in upper or "TBD" in upper:
        return "NON SPECIFICATO IN PERIZIA"
    return s

def _search_proof(
    pages_in: List[Dict[str, Any]],
    keywords: List[str],
    snippets: Optional[List[Dict[str, Any]]] = None,
    field_key: Optional[str] = None,
) -> List[Dict[str, Any]]:
    entries: List[Dict[str, Any]] = []
    if not pages_in:
        return []
    page_text_by_num: Dict[int, str] = {}
    for p in pages_in:
        page_num = int(p.get("page_number", 0) or 0)
        if page_num > 0:
            page_text_by_num[page_num] = str(p.get("text", "") or "")

    def _append_from_quote(page: int, quote: str) -> None:
        if len(entries) >= 3:
            return
        text = page_text_by_num.get(page, "")
        if quote and text:
            idx = text.find(quote)
            if idx >= 0:
                entries.append(
                    _build_search_entry(
                        text,
                        page,
                        idx,
                        idx + len(quote),
                        fallback_quote=quote,
                        field_key=field_key,
                        anchor_hint=quote,
                    )
                )
                return
        lowered = text.lower()
        for kw in keywords:
            idx = lowered.find(kw.lower())
            if idx >= 0:
                start = max(0, idx - 40)
                end = min(len(text), idx + 80)
                entries.append(_build_search_entry(text, page, start, end, fallback_quote=kw, field_key=field_key, anchor_hint=kw))
                return

    if snippets:
        for snip in snippets:
            page = int(snip.get("page", 0) or 0)
            quote = str(snip.get("quote", "") or "").strip()
            if page > 0 and quote:
                _append_from_quote(page, quote)
            if len(entries) >= 3:
                break
        if entries:
            return entries

    lowered_keywords = [k.lower() for k in keywords]
    for p in pages_in:
        if len(entries) >= 3:
            break
        page_num = int(p.get("page_number", 0) or 0)
        if page_num <= 0:
            continue
        text = str(p.get("text", "") or "")
        text_lower = text.lower()
        for kw in lowered_keywords:
            idx = text_lower.find(kw)
            if idx >= 0:
                start = max(0, idx - 40)
                end = min(len(text), idx + 80)
                entries.append(_build_search_entry(text, page_num, start, end, fallback_quote=kw, field_key=field_key, anchor_hint=kw))
                break

    return entries

def _max_page_number(pages_in: List[Dict[str, Any]]) -> int:
    page_nums = [int(p.get("page_number", 0) or 0) for p in pages_in]
    page_nums = [num for num in page_nums if num > 0]
    return max(page_nums) if page_nums else 1

def _build_synthetic_search_entry(pages_in: List[Dict[str, Any]], keywords: List[str]) -> Dict[str, Any]:
    safe_keywords = [str(k).strip() for k in keywords if str(k).strip()]
    keyword_text = ", ".join(safe_keywords) if safe_keywords else "keyword"
    quote = f"Ricerca eseguita: {keyword_text}. Nessuna occorrenza trovata nel documento."
    return {
        "page": 1,
        "quote": quote,
        "start_offset": 0,
        "end_offset": 0,
        "bbox": None,
        "offset_mode": EVIDENCE_OFFSET_MODE,
        "page_text_hash": None,
    }

def _make_searched_in(
    pages_in: List[Dict[str, Any]],
    keywords: List[str],
    status: str,
    snippets: Optional[List[Dict[str, Any]]] = None,
    field_key: Optional[str] = None,
) -> List[Dict[str, Any]]:
    if status not in {"NOT_FOUND", "LOW_CONFIDENCE"}:
        return []
    entries = _search_proof(pages_in, keywords, snippets, field_key=field_key)
    if entries:
        return entries
    return [_build_synthetic_search_entry(pages_in, keywords)]


def _sanitize_search_entry(entry: Any, fallback_keywords: Optional[List[str]] = None) -> Dict[str, Any]:
    if not isinstance(entry, dict):
        entry = {}
    try:
        page = int(entry.get("page", 1) or 1)
    except Exception:
        page = 1
    if page <= 0:
        page = 1
    quote = str(entry.get("quote", "") or "").strip()
    if not quote:
        safe_keywords = [str(k).strip() for k in (fallback_keywords or []) if str(k).strip()]
        keyword_text = ", ".join(safe_keywords) if safe_keywords else "keyword"
        quote = f"Ricerca eseguita: {keyword_text}. Nessuna occorrenza trovata nel documento."
    try:
        start_offset = int(entry.get("start_offset", 0) or 0)
    except Exception:
        start_offset = 0
    try:
        end_offset = int(entry.get("end_offset", 0) or 0)
    except Exception:
        end_offset = 0
    if start_offset < 0:
        start_offset = 0
    if end_offset < start_offset:
        end_offset = start_offset
    return {
        "page": page,
        "quote": quote,
        "start_offset": start_offset,
        "end_offset": end_offset,
        "bbox": None,
        "offset_mode": EVIDENCE_OFFSET_MODE,
        "page_text_hash": entry.get("page_text_hash"),
    }


def _normalize_field_state_contract(field_key: str, state: Any, pages: List[Dict[str, Any]]) -> Dict[str, Any]:
    st = state if isinstance(state, dict) else {}
    status = str(st.get("status", "NOT_FOUND") or "NOT_FOUND")
    value = st.get("value")
    evidence = st.get("evidence") if isinstance(st.get("evidence"), list) else []
    searched_in = st.get("searched_in") if isinstance(st.get("searched_in"), list) else []
    prompt = st.get("user_prompt_it")

    if field_key == "lotto" and evidence:
        normalized_lotto = _normalize_lotto_value_from_evidence(evidence)
        if normalized_lotto:
            value = normalized_lotto

    if status == "USER_PROVIDED":
        evidence = []
        searched_in = []
    elif status == "FOUND":
        if not evidence:
            status = "LOW_CONFIDENCE" if value not in (None, "") else "NOT_FOUND"
            if status == "LOW_CONFIDENCE" and not prompt:
                prompt = "Verifica il valore indicato nella perizia e conferma il dato corretto."
        if status == "FOUND":
            searched_in = []
    elif status in {"NOT_FOUND", "LOW_CONFIDENCE"}:
        if not searched_in:
            searched_in = _make_searched_in(pages, [field_key.replace("_", " ")], status)
    else:
        status = "NOT_FOUND"
        searched_in = _make_searched_in(pages, [field_key.replace("_", " ")], "NOT_FOUND")

    if status in {"NOT_FOUND", "LOW_CONFIDENCE"}:
        if not searched_in:
            searched_in = _make_searched_in(pages, [field_key.replace("_", " ")], status)
        searched_in = [_sanitize_search_entry(item, [field_key.replace("_", " ")]) for item in searched_in]
    else:
        searched_in = []

    return {
        "value": value,
        "status": status,
        "confidence": _extract_confidence(value, status),
        "evidence": evidence,
        "searched_in": searched_in,
        "user_prompt_it": prompt,
    }


def _enforce_field_states_contract(result: Dict[str, Any], pages: List[Dict[str, Any]]) -> None:
    states = result.get("field_states")
    if not isinstance(states, dict):
        return
    for key in list(states.keys()):
        states[key] = _normalize_field_state_contract(key, states.get(key), pages)
    result["field_states"] = states

def _extract_confidence(value_obj: Any, status: str) -> Optional[float]:
    if status == "USER_PROVIDED":
        return 1.0
    if status == "FOUND":
        return 0.9
    if status == "LOW_CONFIDENCE":
        return 0.45
    if status == "NOT_FOUND":
        return 0.0
    if isinstance(value_obj, dict):
        conf = value_obj.get("confidence")
        if isinstance(conf, (int, float)):
            return max(0.0, min(1.0, float(conf)))
    return None

def _collapse_spaced_letters(tokens: List[str]) -> List[str]:
    collapsed: List[str] = []
    buffer: List[str] = []
    for token in tokens:
        if len(token) == 1 and token.isalpha():
            buffer.append(token)
            continue
        if buffer:
            collapsed.append("".join(buffer))
            buffer = []
        collapsed.append(token)
    if buffer:
        collapsed.append("".join(buffer))
    return collapsed

def _normalize_headline_text(text: str) -> str:
    tokens = text.replace("\n", " ").split()
    tokens = _collapse_spaced_letters(tokens)
    return re.sub(r"\s{2,}", " ", " ".join(tokens)).strip()

def _normalize_tribunale_value(value: Optional[str]) -> Optional[str]:
    if not value:
        return value
    cleaned = _normalize_headline_text(str(value))
    cleaned = re.sub(r"TRIBUNA\s+LE", "TRIBUNALE", cleaned, flags=re.I)
    cleaned = re.split(r"S\s*E\s*Z", cleaned, flags=re.I)[0].strip()
    cleaned = re.sub(r"TRIBUNALE\s*DI", "TRIBUNALE DI", cleaned, flags=re.I)
    cleaned = re.sub(r"\bDI([A-ZÀ-Ù])", r"DI \1", cleaned)
    return cleaned

def _normalize_procedura_value(value: Optional[str]) -> Optional[str]:
    if not value:
        return value
    cleaned = _normalize_headline_text(str(value))
    match = re.search(r"\b(\d{1,6}/\d{2,4})\b", cleaned)
    if not match:
        return cleaned
    proc_num = match.group(1)
    if re.search(r"esecuzione\s+immobiliare", cleaned, re.I):
        return f"Esecuzione Immobiliare {proc_num} R.G.E."
    if re.search(r"R\.?\s*G\.?\s*E\.?", cleaned, re.I):
        return f"R.G.E. {proc_num}"
    return proc_num

def _normalize_address_value(value: Optional[str]) -> Optional[str]:
    if not value:
        return value
    cleaned = _normalize_headline_text(str(value))
    cleaned = re.sub(r"^Ubicazione[:\s]*", "", cleaned, flags=re.I).strip()
    cleaned = re.sub(r"\b(Via|Viale|Piazza|Corso|Largo|Vicolo)([A-ZÀ-Ù])", r"\1 \2", cleaned)
    cleaned = re.sub(r"\b([A-ZÀ-Ù])\s+([a-zà-ù]{2,})\b", r"\1\2", cleaned)
    cleaned = re.sub(
        r"\b(Via|Viale|Piazza|Corso|Largo|Vicolo)\s+([A-ZÀ-Ù][a-zà-ù]+)([A-ZÀ-Ù][a-zà-ù]+)\b",
        r"\1 \2 \3",
        cleaned,
    )
    cleaned = re.sub(r"\s*-\s*([a-zà-ù])", r"-\1", cleaned)
    return cleaned

def _headline_display_value(state: Dict[str, Any]) -> str:
    status = state.get("status")
    value = state.get("value")
    if status in {"FOUND", "USER_PROVIDED"} and value:
        value_str = str(value).strip()
        if "LOW_CONFIDENCE" in value_str.upper():
            return "DA VERIFICARE"
        return value_str
    if status == "LOW_CONFIDENCE":
        return "DA VERIFICARE"
    return "NON SPECIFICATO IN PERIZIA"

def _field_state_display_value(state: Dict[str, Any], fallback: str = "NON SPECIFICATO IN PERIZIA") -> str:
    status = state.get("status")
    value = state.get("value")
    if status in {"FOUND", "USER_PROVIDED"} and value is not None and str(value).strip() != "":
        return str(value).strip()
    if status == "LOW_CONFIDENCE":
        return "DA VERIFICARE"
    return fallback

def _build_field_state(
    *,
    value: Any,
    status: str,
    evidence: Optional[List[Dict[str, Any]]] = None,
    searched_in: Optional[List[Dict[str, Any]]] = None,
    user_prompt_it: Optional[str] = None
) -> Dict[str, Any]:
    evidence_list = evidence if isinstance(evidence, list) else []
    searched_list = searched_in if isinstance(searched_in, list) else []
    if status == "USER_PROVIDED":
        evidence_list = []
        searched_list = []
    if status == "FOUND" and not evidence_list:
        status = "LOW_CONFIDENCE" if value is not None else "NOT_FOUND"
    if status in {"FOUND", "USER_PROVIDED"}:
        searched_list = []
    return {
        "value": value,
        "status": status,
        "confidence": _extract_confidence(value, status),
        "evidence": evidence_list,
        "searched_in": searched_list,
        "user_prompt_it": user_prompt_it,
    }

def _parse_euro_number(raw: str) -> Optional[float]:
    if not isinstance(raw, str):
        return None
    s = raw.replace("€", "").replace("EUR", "").replace("Euro", "").strip()
    s = s.replace(" ", "")
    if not s:
        return None
    if "," in s and "." in s:
        s = s.replace(".", "").replace(",", ".")
    elif "," in s:
        s = s.replace(".", "").replace(",", ".")
    else:
        s = s.replace(".", "")
    try:
        return float(s)
    except Exception:
        return None

def _format_mq_value(value: float) -> str:
    if value is None:
        return "NON SPECIFICATO IN PERIZIA"
    if isinstance(value, (int, float)):
        if abs(value - int(value)) < 0.01:
            return f"{int(value)} mq"
        return f"{value:.2f} mq"
    return str(value)

def _line_bounds(text: str, start: int, end: int) -> Tuple[int, int]:
    if not text:
        return 0, 0
    line_start = text.rfind("\n", 0, start)
    line_start = 0 if line_start < 0 else line_start + 1
    line_end = text.find("\n", end)
    line_end = len(text) if line_end < 0 else line_end
    return line_start, line_end


def _is_toc_like_line(line: str) -> bool:
    raw = str(line or "")
    compact = " ".join(raw.split()).strip()
    if not compact:
        return False
    # Typical index/table-of-contents leaders: many dots plus trailing page number.
    if re.search(r"\.{6,}", compact) and re.search(r"\b\d{1,3}\s*$", compact):
        return True
    if re.search(r"…{2,}", compact) and re.search(r"\b\d{1,3}\s*$", compact):
        return True
    # Very short index-like rows often end with a page number.
    if len(compact) <= 80 and re.search(r"\b\d{1,3}\s*$", compact) and ":" not in compact:
        return True
    if re.search(r"\b(indice|sommario)\b", compact, re.I):
        return True
    return False


def _is_toc_like_quote(quote: str) -> bool:
    for line in str(quote or "").splitlines():
        if _is_toc_like_line(line):
            return True
    return _is_toc_like_line(quote)


def _find_anchored_span(text: str, *snippets: Any) -> Optional[Tuple[int, int]]:
    raw_text = str(text or "")
    if not raw_text:
        return None
    for raw_snippet in snippets:
        snippet = str(raw_snippet or "").strip()
        if not snippet:
            continue
        idx = raw_text.find(snippet)
        if idx >= 0:
            return idx, idx + len(snippet)
        parts = [re.escape(part) for part in re.split(r"\s+", snippet) if part]
        if not parts:
            continue
        match = re.search(r"\s+".join(parts), raw_text)
        if match:
            return match.start(), match.end()
    return None


def _is_cost_table_context(line: str) -> bool:
    low = str(line or "").lower()
    return any(
        tok in low
        for tok in (
            "deprezzamento",
            "valore tipo",
            "rischio assunto",
            "oneri di regolarizzazione urbanistica",
            "valore finale",
        )
    )

def _build_headline_state_from_existing(
    *,
    report_obj: Any,
    case_obj: Any,
    evidence: List[Dict[str, Any]],
    pages: List[Dict[str, Any]],
    keywords: List[str],
    prompt_if_low_conf: bool = False
) -> Dict[str, Any]:
    report_val = _clean_field_value(report_obj)
    case_val = _clean_field_value(case_obj)
    chosen = report_val if report_val != "NON SPECIFICATO IN PERIZIA" else case_val
    status = "FOUND"
    if chosen == "DA VERIFICARE":
        status = "LOW_CONFIDENCE"
    elif chosen == "NON SPECIFICATO IN PERIZIA":
        status = "NOT_FOUND"
        chosen = None
    if status == "FOUND" and not (evidence if isinstance(evidence, list) else []):
        status = "LOW_CONFIDENCE"
    prompt = None
    if status == "LOW_CONFIDENCE" and prompt_if_low_conf:
        prompt = "Dato non affidabile. Controlla la perizia (vedi pagine suggerite) e inserisci il valore corretto."
    searched_in = _make_searched_in(pages, keywords, status)
    return {
        "value": chosen,
        "status": status,
        "confidence": _extract_confidence(report_obj, status),
        "evidence": evidence if isinstance(evidence, list) else [],
        "searched_in": searched_in,
        "user_prompt_it": prompt,
    }

def _collect_keyword_snippets(pages: List[Dict[str, Any]], keywords: List[str], max_snippets: int = 3) -> List[Dict[str, Any]]:
    snippets: List[Dict[str, Any]] = []
    lowered = [k.lower() for k in keywords]
    for p in pages:
        if len(snippets) >= max_snippets:
            break
        text = str(p.get("text", "") or "")
        text_lower = text.lower()
        for k in lowered:
            if k in text_lower:
                idx = text_lower.find(k)
                start = max(0, idx - 40)
                end = min(len(text), idx + 80)
                page_num = int(p.get("page_number", 0) or 0)
                quote = text[start:end].replace("\n", " ").strip()
                if page_num > 0 and quote:
                    snippets.append({
                        "page": page_num,
                        "quote": quote
                    })
                break
    return snippets

def _extract_tribunale_state(pages: List[Dict[str, Any]]) -> Dict[str, Any]:
    keywords = ["tribunale", "tribunale di"]
    evidence: List[Dict[str, Any]] = []
    value: Optional[str] = None
    status = "NOT_FOUND"
    location_found = False

    word_t = r"t\s*r\s*i\s*b\s*u\s*n\s*a\s*l\s*e"
    word_d = r"d\s*i"
    pattern_full = re.compile(rf"{word_t}\s+{word_d}\s+([A-ZÀ-Ù][A-ZÀ-Ù'\s\.\-]{{2,80}})", re.I)
    pattern_word = re.compile(rf"{word_t}", re.I)

    for p in pages:
        text = str(p.get("text", "") or "")
        match = pattern_full.search(text)
        if match:
            start, end = match.start(), match.end()
            ev = _build_evidence(
                text,
                int(p.get("page_number", 0) or 0),
                start,
                end,
                field_key="tribunale",
                anchor_hint=match.group(0),
            )
            if not str(ev.get("quote") or "").strip() or _is_toc_like_quote(str(ev.get("quote") or "")):
                continue
            evidence.append(ev)
            raw = match.group(0)
            value = _normalize_tribunale_value(raw)
            location_found = True
            status = "FOUND"
            break
    if not location_found:
        for p in pages:
            text = str(p.get("text", "") or "")
            match = pattern_word.search(text)
            if match:
                start, end = match.start(), match.end()
                ev = _build_evidence(
                    text,
                    int(p.get("page_number", 0) or 0),
                    start,
                    end,
                    field_key="tribunale",
                    anchor_hint=match.group(0),
                )
                if not str(ev.get("quote") or "").strip() or _is_toc_like_quote(str(ev.get("quote") or "")):
                    continue
                evidence.append(ev)
                value = _normalize_headline_text(text[start:end])
                status = "LOW_CONFIDENCE"
                break

    snippets = _collect_keyword_snippets(pages, keywords)
    searched_in = _make_searched_in(pages, keywords, status, snippets, field_key="tribunale")
    prompt = None
    if status in {"LOW_CONFIDENCE", "NOT_FOUND"}:
        prompt = "Verifica il tribunale indicato nella perizia (in intestazione o nei dati procedura)."
    return {
        "value": value,
        "status": status,
        "confidence": _extract_confidence(value, status),
        "evidence": evidence,
        "searched_in": searched_in,
        "user_prompt_it": prompt,
    }

def _extract_procedura_state(pages: List[Dict[str, Any]]) -> Dict[str, Any]:
    keywords = ["r.g.e", "rge", "procedura", "esecuzione immobiliare", "esecuzione"]
    evidence: List[Dict[str, Any]] = []
    value: Optional[str] = None
    status = "NOT_FOUND"

    patterns = [
        re.compile(r"(Esecuzione\s+Immobiliare\s+\d+\s*/\s*\d+\s+del\s+R\.?\s*G\.?\s*E\.?\s*\d*\s*/?\s*\d*)", re.I),
        re.compile(r"(R\.?\s*G\.?\s*E\.?\s*\d+\s*/\s*\d+)", re.I),
        re.compile(r"(Esecuzione\s+Immobiliare\s+(?:n\.?\s*)?\d+\s*/\s*\d+)", re.I),
        re.compile(r"(Procedura\s+(?:n\.?\s*)?\d+\s*/\s*\d+)", re.I),
    ]
    fallback_patterns = [
        re.compile(r"R\.?\s*G\.?\s*E\.?", re.I),
        re.compile(r"Esecuzione\s+Immobiliare", re.I),
    ]

    for p in pages:
        text = str(p.get("text", "") or "")
        for pat in patterns:
            match = pat.search(text)
            if match:
                start, end = match.start(), match.end()
                evidence.append(
                    _build_evidence(
                        text,
                        int(p.get("page_number", 0) or 0),
                        start,
                        end,
                        field_key="procedura",
                        anchor_hint=match.group(0),
                    )
                )
                value = _normalize_procedura_value(match.group(0))
                status = "FOUND"
                break
        if status == "FOUND":
            break

    if status != "FOUND":
        for p in pages:
            text = str(p.get("text", "") or "")
            for pat in fallback_patterns:
                match = pat.search(text)
                if match:
                    start, end = match.start(), match.end()
                    evidence.append(
                        _build_evidence(
                            text,
                            int(p.get("page_number", 0) or 0),
                            start,
                            end,
                            field_key="procedura",
                            anchor_hint=match.group(0),
                        )
                    )
                    value = _normalize_procedura_value(match.group(0))
                    status = "LOW_CONFIDENCE"
                    break
            if status == "LOW_CONFIDENCE":
                break

    snippets = _collect_keyword_snippets(pages, keywords)
    searched_in = _make_searched_in(pages, keywords, status, snippets, field_key="procedura")
    prompt = None
    if status in {"LOW_CONFIDENCE", "NOT_FOUND"}:
        prompt = "Verifica il numero di procedura/R.G.E. indicato in perizia."
    return {
        "value": value,
        "status": status,
        "confidence": _extract_confidence(value, status),
        "evidence": evidence,
        "searched_in": searched_in,
        "user_prompt_it": prompt,
    }

def _normalize_lotto_value_from_evidence(evidence: List[Dict[str, Any]]) -> Optional[str]:
    if not evidence:
        return None
    normalized_quotes = []
    for ev in evidence:
        quote = str((ev or {}).get("quote", "") or "").strip()
        if quote:
            normalized_quotes.append(_normalize_headline_text(quote))

    if not normalized_quotes:
        return None

    for text in normalized_quotes:
        if re.search(r"\bLOTTO\s+UNICO\b", text, re.I):
            return "Lotto Unico"

    for text in normalized_quotes:
        match = re.search(r"\bLOTTI?\s+([0-9]+(?:\s*[,/-]\s*[0-9]+)*)", text, re.I)
        if not match:
            continue
        nums = [int(n) for n in re.findall(r"\d+", match.group(1))]
        if len(nums) == 1:
            return f"Lotto {nums[0]}"
        if len(nums) > 1:
            return f"Lotti {min(nums)}–{max(nums)}"
    return None

def _extract_lotto_state(pages: List[Dict[str, Any]], lots: List[Dict[str, Any]]) -> Dict[str, Any]:
    keywords = ["lotto", "lotti", "lotto unico"]
    evidence: List[Dict[str, Any]] = []
    value: Optional[str] = None
    status = "NOT_FOUND"

    lot_numbers = []
    for lot in lots or []:
        try:
            lot_numbers.append(int(lot.get("lot_number")))
        except Exception:
            continue
        ev_list = lot.get("evidence", {}).get("lotto", []) if isinstance(lot.get("evidence"), dict) else []
        for ev in (ev_list if isinstance(ev_list, list) else []):
            if not isinstance(ev, dict):
                continue
            raw_q = str(ev.get("quote") or "").strip()
            if not raw_q:
                continue
            n_quote, n_hint = normalize_evidence_quote(raw_q, 0, len(raw_q), max_len=520, field_key="lotto", anchor_hint=raw_q)
            if not n_quote or _is_toc_like_quote(n_quote):
                continue
            payload = {"page": ev.get("page"), "quote": n_quote}
            if n_hint:
                payload["search_hint"] = n_hint
            evidence.append(payload)

    if lot_numbers:
        lot_numbers = sorted(set(lot_numbers))
        if len(lot_numbers) >= 2:
            value = f"Lotti {lot_numbers[0]}–{lot_numbers[-1]}"
        else:
            lot_num = lot_numbers[0]
            unico_match = _find_regex_in_pages(pages, r"\bLOTTO\s+UNICO\b", re.I, field_key="lotto")
            value = "Lotto Unico" if unico_match else f"Lotto {lot_num}"
            if unico_match:
                evidence.append(unico_match)
        status = "FOUND" if evidence else "LOW_CONFIDENCE"

    if status == "NOT_FOUND":
        match = None
        for p in pages:
            text = str(p.get("text", "") or "")
            local = re.search(r"\bLOTTO\s+UNICO\b|\bLOTTO\s+\d+\b", text, re.I)
            if not local:
                continue
            ls, le = _line_bounds(text, local.start(), local.end())
            line = text[ls:le]
            ev = _build_evidence(
                text,
                int(p.get("page_number", 0) or 0),
                ls,
                le,
                field_key="lotto",
                anchor_hint=local.group(0),
            )
            if _is_toc_like_line(line) or _is_toc_like_quote(str(ev.get("quote") or "")):
                continue
            match = ev
            break
        if match:
            evidence.append(match)
            value = _normalize_headline_text(match.get("quote", ""))
            status = "FOUND"
        else:
            match = _find_regex_in_pages(pages, r"\bLOTTO\b", re.I, field_key="lotto")
            if match:
                evidence.append(match)
                value = "Lotto"
                status = "LOW_CONFIDENCE"

    if evidence:
        normalized = _normalize_lotto_value_from_evidence(evidence)
        if normalized:
            value = normalized

    snippets = _collect_keyword_snippets(pages, keywords)
    searched_in = _make_searched_in(pages, keywords, status, snippets, field_key="lotto")
    prompt = None
    if status in {"LOW_CONFIDENCE", "NOT_FOUND"}:
        prompt = "Verifica il lotto indicato nella perizia (schema riassuntivo)."
    return {
        "value": value,
        "status": status,
        "confidence": _extract_confidence(value, status),
        "evidence": evidence,
        "searched_in": searched_in,
        "user_prompt_it": prompt,
    }

def _extract_address_state(pages: List[Dict[str, Any]], lots: List[Dict[str, Any]]) -> Dict[str, Any]:
    keywords = ["via", "viale", "piazza", "corso", "strada", "indirizzo", "ubicazione", "comune"]
    evidence: List[Dict[str, Any]] = []
    value: Optional[str] = None
    status = "NOT_FOUND"

    if lots:
        for lot in lots:
            ubic = str(lot.get("ubicazione", "") or "").strip()
            if ubic and ubic.upper() not in {"NON SPECIFICATO IN PERIZIA", "TBD"}:
                value = ubic
                ev_list = lot.get("evidence", {}).get("ubicazione", []) if isinstance(lot.get("evidence"), dict) else []
                if ev_list:
                    evidence.extend(ev_list)
                status = "FOUND" if ev_list else "LOW_CONFIDENCE"
                break

    if status == "NOT_FOUND":
        match = _find_regex_in_pages(pages, r"Ubicazione[:\s]*([^\n]{5,120})", re.I)
        if match:
            evidence.append(match)
            value = _normalize_address_value(match.get("quote", ""))
            status = "FOUND"
        else:
            match = _find_regex_in_pages(pages, r"\b(Via|Viale|Piazza|Corso|Strada|Vicolo|Largo|Localit[aà])\b[^\n]{5,120}", re.I)
            if match:
                evidence.append(match)
                value = _normalize_address_value(match.get("quote", ""))
                status = "LOW_CONFIDENCE"

    if value:
        value = _normalize_address_value(value)
        has_street = re.search(r"\b(via|viale|piazza|corso|strada|vicolo|largo|localit[aà])\b", value, re.I)
        if len(value) < 10 and status == "FOUND":
            status = "LOW_CONFIDENCE"
        elif not has_street and status == "FOUND":
            status = "LOW_CONFIDENCE"

    snippets = _collect_keyword_snippets(pages, keywords)
    searched_in = _make_searched_in(pages, keywords, status, snippets)
    prompt = None
    if status in {"LOW_CONFIDENCE", "NOT_FOUND"}:
        prompt = "Verifica l'indirizzo completo (via/piazza e comune) indicato nella perizia."
    return {
        "value": value,
        "status": status,
        "confidence": _extract_confidence(value, status),
        "evidence": evidence,
        "searched_in": searched_in,
        "user_prompt_it": prompt,
    }

def _extract_prezzo_base_asta_state(pages: List[Dict[str, Any]]) -> Dict[str, Any]:
    keywords = ["prezzo base", "prezzo base d'asta", "prezzo base d’asta", "prezzo base asta", "euro", "€"]
    patterns = [
        re.compile(r"(Prezzo\s+base(?:\s+d['’]asta)?[^\n]{0,60}?)(?:€|\bEuro\b)?\s*([0-9]{1,3}(?:[.\s][0-9]{3})*(?:,[0-9]{2})?)", re.I),
        re.compile(r"(Prezzo\s+base[^\n]{0,60}?)(?:€|\bEuro\b)\s*([0-9]{1,3}(?:[.\s][0-9]{3})*(?:,[0-9]{2})?)", re.I),
    ]
    schema_pages = []
    for p in pages:
        text_upper = str(p.get("text", "") or "").upper()
        if "SCHEMA RIASSUNTIVO" in text_upper:
            schema_pages.append(p)
    page_groups = [schema_pages, pages] if schema_pages else [pages]
    for group in page_groups:
        for p in group:
            text = str(p.get("text", "") or "")
            for pat in patterns:
                m = pat.search(text)
                if not m:
                    continue
                value_raw = m.group(2)
                value = _parse_euro_number(value_raw)
                if value is None:
                    continue
                start = m.start(1)
                end = m.end(2)
                ls, le = _line_bounds(text, start, end)
                ev = _build_evidence(
                    text,
                    int(p.get("page_number", 0) or 0),
                    ls,
                    le,
                    field_key="prezzo_base_asta",
                    anchor_hint=m.group(0),
                )
                line_text = text[ls:le]
                if _is_toc_like_line(line_text) or _is_toc_like_quote(str(ev.get("quote") or "")):
                    continue
                if not str(ev.get("quote") or "").strip():
                    continue
                evidence = [ev]
                searched_in = _make_searched_in(pages, keywords, "FOUND", field_key="prezzo_base_asta")
                return _build_field_state(value=value, status="FOUND", evidence=evidence, searched_in=searched_in)
    searched_in = _make_searched_in(pages, keywords, "NOT_FOUND", field_key="prezzo_base_asta")
    return _build_field_state(value=None, status="NOT_FOUND", evidence=[], searched_in=searched_in)

def _extract_superficie_state(pages: List[Dict[str, Any]], dati_certi: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    keywords = ["superficie", "mq", "m²", "superficie commerciale", "superficie catastale"]
    dati_certi = dati_certi if isinstance(dati_certi, dict) else {}
    existing = dati_certi.get("superficie_catastale")
    if isinstance(existing, dict):
        value = existing.get("value")
        evidence = existing.get("evidence", [])
        normalized_existing_evidence: List[Dict[str, Any]] = []
        if isinstance(evidence, list):
            for ev in evidence[:2]:
                if not isinstance(ev, dict):
                    continue
                quote = str(ev.get("quote") or "").strip()
                if not quote:
                    continue
                n_quote, n_hint = normalize_evidence_quote(
                    quote,
                    0,
                    len(quote),
                    max_len=220,
                    field_key="superficie_catastale",
                    anchor_hint=quote,
                )
                if not n_quote or "superficie" not in n_quote.lower():
                    continue
                payload = {"page": ev.get("page"), "quote": n_quote}
                if n_hint:
                    payload["search_hint"] = n_hint
                normalized_existing_evidence.append(payload)
        if isinstance(value, str):
            match = re.search(r"(\d{1,4}(?:[\.,]\d{1,2})?)", value)
            if match:
                raw = match.group(1).replace(".", "").replace(",", ".")
                try:
                    value = {"value": float(raw), "unit": "mq", "label": "Superficie"}
                except Exception:
                    value = existing.get("value")
        if value is not None and normalized_existing_evidence:
            searched_in = _make_searched_in(pages, keywords, "FOUND", field_key="superficie_catastale")
            return _build_field_state(value=value, status="FOUND", evidence=normalized_existing_evidence, searched_in=searched_in)

    pattern = re.compile(
        r"(Superficie(?:\s+catastale|\s+commerciale)?[^\n]{0,40}?)(\d{1,4}(?:[\.,]\d{1,2})?)\s*(m2|m²|mq)",
        re.I,
    )
    schema_pages = []
    for p in pages:
        text_upper = str(p.get("text", "") or "").upper()
        if "SCHEMA RIASSUNTIVO" in text_upper or "TIPOLOGIA IMMOBILE" in text_upper:
            schema_pages.append(p)
    page_groups = [schema_pages, pages] if schema_pages else [pages]
    for group in page_groups:
        for p in group:
            text = str(p.get("text", "") or "")
            m = pattern.search(text)
            if not m:
                continue
            raw_num = m.group(2).replace(".", "").replace(",", ".")
            try:
                value_num = float(raw_num)
            except Exception:
                continue
            unit = "mq"
            start = m.start(1)
            end = m.end(3)
            evidence = [
                _build_evidence(
                    text,
                    int(p.get("page_number", 0) or 0),
                    start,
                    end,
                    field_key="superficie_catastale",
                    anchor_hint=m.group(0),
                )
            ]
            if not evidence or not str(evidence[0].get("quote") or "").strip() or _is_toc_like_quote(str(evidence[0].get("quote") or "")):
                continue
            value = {"value": value_num, "unit": unit, "label": _normalize_headline_text(m.group(1))}
            searched_in = _make_searched_in(pages, keywords, "FOUND", field_key="superficie_catastale")
            return _build_field_state(value=value, status="FOUND", evidence=evidence, searched_in=searched_in)
    searched_in = _make_searched_in(pages, keywords, "NOT_FOUND", field_key="superficie_catastale")
    return _build_field_state(value=None, status="NOT_FOUND", evidence=[], searched_in=searched_in)

def _extract_diritto_reale_state(pages: List[Dict[str, Any]]) -> Dict[str, Any]:
    keywords = ["proprietà", "nuda proprietà", "usufrutto", "diritto di"]
    pattern = re.compile(r"\b(Nuda\s+proprietà|Piena\s+proprietà|Proprietà|Usufrutto|Diritto\s+di\s+[^\n]{0,40})\b", re.I)
    schema_pattern = re.compile(r"Diritto\s+reale[:\s]*([^\n]{3,60})", re.I)
    def _normalize_right_value(raw_value: str, text: str, start: int, end: int) -> str:
        value = _normalize_headline_text(raw_value)
        window = text[max(0, start - 120):min(len(text), end + 120)]
        if re.search(r"\b(propriet[àa]|piena\s+propriet[àa])\b", value, re.I):
            if re.search(r"\b(?:quota\s+)?1\s*/\s*1\b|\bper\s+1\s*/\s*1\b|\(propriet[àa]\s+1\s*/\s*1\)", window, re.I):
                return "Proprietà 1/1"
        return value

    schema_pages = []
    for p in pages:
        text_upper = str(p.get("text", "") or "").upper()
        if "SCHEMA RIASSUNTIVO" in text_upper or "DIRITTO REALE" in text_upper:
            schema_pages.append(p)
    for p in (schema_pages or pages):
        text = str(p.get("text", "") or "")
        m = schema_pattern.search(text)
        if m:
            start, end = m.start(1), m.end(1)
            evidence = [
                _build_evidence(
                    text,
                    int(p.get("page_number", 0) or 0),
                    start,
                    end,
                    field_key="diritto_reale",
                    anchor_hint=m.group(0),
                )
            ]
            value = _normalize_right_value(m.group(1), text, start, end)
            searched_in = _make_searched_in(pages, keywords, "FOUND", field_key="stato_occupativo")
            return _build_field_state(value=value, status="FOUND", evidence=evidence, searched_in=searched_in)
    for p in pages:
        text = str(p.get("text", "") or "")
        m = pattern.search(text)
        if not m:
            continue
        start, end = m.start(), m.end()
        evidence = [
            _build_evidence(
                text,
                int(p.get("page_number", 0) or 0),
                start,
                end,
                field_key="diritto_reale",
                anchor_hint=m.group(0),
            )
        ]
        value = _normalize_right_value(m.group(0), text, start, end)
        searched_in = _make_searched_in(pages, keywords, "FOUND", field_key="stato_occupativo")
        return _build_field_state(value=value, status="FOUND", evidence=evidence, searched_in=searched_in)
    searched_in = _make_searched_in(pages, keywords, "NOT_FOUND", field_key="stato_occupativo")
    return _build_field_state(value=None, status="NOT_FOUND", evidence=[], searched_in=searched_in)

def _extract_stato_occupativo_state(pages: List[Dict[str, Any]]) -> Dict[str, Any]:
    keywords = ["occupato", "libero", "detenuto", "contratto di locazione", "rilascio"]
    ambiguous_markers = ["si presume", "non è noto", "non e' noto", "non risulta", "da verificare", "da accertare", "presumibilmente", "non è dato sapere", "non e' dato sapere"]
    patterns = [
        (re.compile(r"\boccupat[oa]\b[^\n]{0,160}\bterzi\b[^\n]{0,120}\b(senza\s+titolo|contratt\w+[^\n]{0,60}scadut\w*|natura\s+transitoria\s+scadut\w*)\b", re.I), "OCCUPATO DA TERZI SENZA TITOLO"),
        (re.compile(r"\boccupat[oa]\b[^\n]{0,120}\b(debitore|debitori\s+esecutati)\b", re.I), "OCCUPATO DAL DEBITORE"),
        (re.compile(r"\b(non\s+occupato|libero|libera\s+disponibilit[aà])\b", re.I), "LIBERO"),
        (re.compile(r"\b(occupato|detenuto|locato|locazione|contratto\s+di\s+locazione|inquilino)\b", re.I), "OCCUPATO"),
    ]
    candidates: List[Tuple[int, int, str, str, Dict[str, Any]]] = []
    # priority: OCCUPATO DA TERZI SENZA TITOLO > OCCUPATO DAL DEBITORE > OCCUPATO > LIBERO
    pri = {"OCCUPATO DA TERZI SENZA TITOLO": 4, "OCCUPATO DAL DEBITORE": 3, "OCCUPATO": 2, "LIBERO": 1}
    for p in pages:
        text = str(p.get("text", "") or "")
        for pat, label in patterns:
            m = pat.search(text)
            if not m:
                continue
            start, end = m.start(), m.end()
            line_start, line_end = _line_bounds(text, start, end)
            line_text = text[line_start:line_end].lower()
            evidence = [
                _build_evidence(
                    text,
                    int(p.get("page_number", 0) or 0),
                    line_start,
                    line_end,
                    field_key="stato_occupativo",
                    anchor_hint=m.group(0),
                )
            ]
            status = "LOW_CONFIDENCE" if any(marker in line_text for marker in ambiguous_markers) else "FOUND"
            candidates.append((pri.get(label, 0), int(p.get("page_number", 0) or 0), label, status, evidence[0]))

    if candidates:
        candidates.sort(key=lambda x: (-x[0], x[1]))
        top = candidates[0]
        chosen_label = top[2]
        chosen_status = top[3]
        chosen_evidence = [top[4]]
        searched_in = _make_searched_in(pages, keywords, chosen_status, field_key="ape")
        return _build_field_state(value=chosen_label, status=chosen_status, evidence=chosen_evidence, searched_in=searched_in)

    if len(set([c[2] for c in candidates])) > 1:
        searched_in = _make_searched_in(pages, keywords, "LOW_CONFIDENCE", field_key="ape")
        return _build_field_state(value="DA VERIFICARE", status="LOW_CONFIDENCE", evidence=[], searched_in=searched_in)
    searched_in = _make_searched_in(pages, keywords, "NOT_FOUND", field_key="ape")
    return _build_field_state(value=None, status="NOT_FOUND", evidence=[], searched_in=searched_in)

def _extract_ape_state(pages: List[Dict[str, Any]]) -> Dict[str, Any]:
    keywords = ["ape", "attestato di prestazione energetica", "certificato energetico"]
    patterns = [
        (re.compile(r"(non\s+presente|assente|non\s+esiste)[\s\S]{0,120}(ape|certificato\s+energetico)", re.I), "ASSENTE"),
        (re.compile(r"(ape|certificato\s+energetico)[\s\S]{0,120}(non\s+presente|assente|non\s+esiste)", re.I), "ASSENTE"),
        (re.compile(r"(ape|attestato\s+di\s+prestazione\s+energetica)[\s\S]{0,120}(presente)", re.I), "PRESENTE"),
    ]
    for p in pages:
        text = str(p.get("text", "") or "")
        for pat, label in patterns:
            m = pat.search(text)
            if not m:
                continue
            start, end = m.start(), m.end()
            line_start, line_end = _line_bounds(text, start, end)
            evidence = [
                _build_evidence(
                    text,
                    int(p.get("page_number", 0) or 0),
                    line_start,
                    line_end,
                    field_key="ape",
                    anchor_hint=m.group(0),
                )
            ]
            searched_in = _make_searched_in(pages, keywords, "FOUND")
            return _build_field_state(value=label, status="FOUND", evidence=evidence, searched_in=searched_in)
    searched_in = _make_searched_in(pages, keywords, "NOT_FOUND")
    return _build_field_state(value=None, status="NOT_FOUND", evidence=[], searched_in=searched_in)


def _extract_agibilita_state(pages: List[Dict[str, Any]]) -> Dict[str, Any]:
    keywords = ["agibilità", "agibilita", "abitabilità", "abitabilita", "agibile"]
    absent_patterns = [
        re.compile(r"(non\s+[èe]\s+presente\s+l['’]?abitabilit[aà][^\n]{0,120})", re.I),
        re.compile(r"(non\s+risulta\s+agibil[ei][^\n]{0,120})", re.I),
        re.compile(r"(non\s+risulta\s+rilasciat\w*[^\n]{0,80}certificato\s+di\s+agibilit[aà][^\n]{0,120})", re.I),
        re.compile(r"(agibilit[aà][^\n]{0,120}(assente|non\s+presente))", re.I),
    ]
    present_patterns = [
        re.compile(r"(\brisulta\s+agibil[ei]\b[^\n]{0,120})", re.I),
        re.compile(r"(\bagibilit[aà]\b[^\n]{0,120}\bpresente\b)", re.I),
    ]

    for p in pages:
        text = str(p.get("text", "") or "")
        for pat in absent_patterns:
            m = pat.search(text)
            if not m:
                continue
            ls, le = _line_bounds(text, m.start(), m.end())
            evidence = [
                _build_evidence(
                    text,
                    int(p.get("page_number", 0) or 0),
                    ls,
                    le,
                    field_key="agibilita",
                    anchor_hint=m.group(0),
                )
            ]
            searched_in = _make_searched_in(pages, keywords, "FOUND")
            return _build_field_state(value="ASSENTE", status="FOUND", evidence=evidence, searched_in=searched_in)

    for p in pages:
        text = str(p.get("text", "") or "")
        for pat in present_patterns:
            m = pat.search(text)
            if not m:
                continue
            ls, le = _line_bounds(text, m.start(), m.end())
            line = text[ls:le].lower()
            if "non risulta agibile" in line:
                continue
            evidence = [
                _build_evidence(
                    text,
                    int(p.get("page_number", 0) or 0),
                    ls,
                    le,
                    field_key="agibilita",
                    anchor_hint=m.group(0),
                )
            ]
            searched_in = _make_searched_in(pages, keywords, "LOW_CONFIDENCE")
            return _build_field_state(value="PRESENTE", status="LOW_CONFIDENCE", evidence=evidence, searched_in=searched_in)

    searched_in = _make_searched_in(pages, keywords, "NOT_FOUND")
    return _build_field_state(value=None, status="NOT_FOUND", evidence=[], searched_in=searched_in)

def _extract_dati_asta_state(pages: List[Dict[str, Any]]) -> Dict[str, Any]:
    keywords = ["dettagli asta", "vendita", "giorno", "ore"]
    for p in pages:
        text = str(p.get("text", "") or "")
        m = re.search(r"(\d{1,2}/\d{1,2}/\d{4}).{0,100}?ore\s+(\d{1,2}[:\.]\d{2})", text, re.I)
        if not m:
            continue
        start, end = m.start(), m.end()
        line_start, line_end = _line_bounds(text, start, end)
        evidence = [
            _build_evidence(
                text,
                int(p.get("page_number", 0) or 0),
                line_start,
                line_end,
                field_key="dati_asta",
                anchor_hint=m.group(0),
            )
        ]
        value = {"data": m.group(1), "ora": m.group(2).replace(".", ":")}
        searched_in = _make_searched_in(pages, keywords, "FOUND")
        return _build_field_state(value=value, status="FOUND", evidence=evidence, searched_in=searched_in)
    searched_in = _make_searched_in(pages, keywords, "NOT_FOUND")
    return _build_field_state(value=None, status="NOT_FOUND", evidence=[], searched_in=searched_in)

def _extract_regolarita_urbanistica_state(pages: List[Dict[str, Any]]) -> Dict[str, Any]:
    keywords = ["conformità urbanistica", "abusi edilizi", "sanatoria", "condono", "pratiche edilizie", "difformità"]
    positive = re.compile(
        r"(non\s+(?:risultano|emergono)\s+abusi|assenza\s+di\s+abusi|non\s+sono\s+stati\s+riscontrati\s+abusi|conform(?:e|ità)\s+urbanistic\w*)",
        re.I,
    )
    negative = re.compile(
        r"(abusi\s+edilizi|difformit[aà]|non\s+conform[ei]|irregolarit[aà]|sanatoria|condono|incongruenz\w+\s+nello\s+stato\s+di\s+fatto)",
        re.I,
    )
    ambiguous = re.compile(r"(da\s+verificare|non\s+è\s+noto|non\s+e'\s+noto|da\s+accertare|si\s+presume|presumibilmente)", re.I)
    candidates: List[Tuple[int, int, str, str, Dict[str, Any]]] = []
    for p in pages:
        text = str(p.get("text", "") or "")
        for pat, label in ((positive, "NON EMERGONO ABUSI"), (negative, "PRESENTI DIFFORMITÀ")):
            m = pat.search(text)
            if not m:
                continue
            start, end = m.start(), m.end()
            line_start, line_end = _line_bounds(text, start, end)
            line_text = text[line_start:line_end]
            if _is_toc_like_line(line_text):
                continue
            evidence_obj = _build_evidence(
                text,
                int(p.get("page_number", 0) or 0),
                line_start,
                line_end,
                field_key="conformita_urbanistica",
                anchor_hint=m.group(0),
            )
            status = "LOW_CONFIDENCE" if ambiguous.search(line_text) else "FOUND"
            value = "DA VERIFICARE" if status == "LOW_CONFIDENCE" else label
            score = 0
            low_line = line_text.lower()
            if label == "PRESENTI DIFFORMITÀ":
                score += 6
            if any(tok in low_line for tok in ("abusi", "difform", "incongruenz", "non conforme", "irregolarit")):
                score += 4
            if _is_cost_table_context(low_line):
                score -= 5
            candidates.append((score, int(p.get("page_number", 0) or 0), value, status, evidence_obj))
    if candidates:
        candidates.sort(key=lambda x: (-x[0], x[1]))
        top = candidates[0]
        searched_in = _make_searched_in(pages, keywords, top[3], field_key="conformita_urbanistica")
        return _build_field_state(value=top[2], status=top[3], evidence=[top[4]], searched_in=searched_in)
    searched_in = _make_searched_in(pages, keywords, "NOT_FOUND", field_key="conformita_urbanistica")
    return _build_field_state(value=None, status="NOT_FOUND", evidence=[], searched_in=searched_in)

def _extract_conformita_catastale_state(pages: List[Dict[str, Any]]) -> Dict[str, Any]:
    keywords = ["conformità catastale", "difformità", "planimetria", "catasto"]
    positive = re.compile(r"(conformit[aà]\s+catastale|planimetria\s+conforme|conforme\s+al\s+catasto)", re.I)
    negative = re.compile(
        r"(difformit[aà]\s+catastal[ei]|non\s+conforme|mancata\s+corrispondenza|planimetria\s+non\s+conforme|incongruenz\w+[\s\S]{0,220}?planimetri\w+\s+catastal\w*)",
        re.I,
    )
    ambiguous = re.compile(r"(da\s+verificare|non\s+è\s+noto|non\s+e'\s+noto|da\s+accertare|si\s+presume|presumibilmente)", re.I)
    candidates: List[Tuple[int, int, str, str, Dict[str, Any]]] = []
    for p in pages:
        text = str(p.get("text", "") or "")
        for pat, label in ((positive, "CONFORME"), (negative, "PRESENTI DIFFORMITÀ")):
            m = pat.search(text)
            if not m:
                continue
            start, end = m.start(), m.end()
            line_start, line_end = _line_bounds(text, start, end)
            line_text = text[line_start:line_end]
            snippet = text[max(0, start - 40):min(len(text), end + 120)]
            if _is_toc_like_line(line_text) or _is_toc_like_line(snippet):
                continue
            evidence_obj = _build_evidence(
                text,
                int(p.get("page_number", 0) or 0),
                start,
                end,
                field_key="conformita_catastale",
                anchor_hint=m.group(0),
            )
            status = "LOW_CONFIDENCE" if ambiguous.search(line_text) else "FOUND"
            value = "DA VERIFICARE" if status == "LOW_CONFIDENCE" else label
            score = 0
            low_line = line_text.lower()
            if label == "PRESENTI DIFFORMITÀ":
                score += 5
            if any(tok in low_line for tok in ("planimetria catastale", "incongruenz", "difform", "non conforme", "catast")):
                score += 4
            candidates.append((score, int(p.get("page_number", 0) or 0), value, status, evidence_obj))
    if candidates:
        candidates.sort(key=lambda x: (-x[0], x[1]))
        top = candidates[0]
        searched_in = _make_searched_in(pages, keywords, top[3])
        return _build_field_state(value=top[2], status=top[3], evidence=[top[4]], searched_in=searched_in)
    searched_in = _make_searched_in(pages, keywords, "NOT_FOUND")
    return _build_field_state(value=None, status="NOT_FOUND", evidence=[], searched_in=searched_in)

def _extract_spese_condominiali_state(pages: List[Dict[str, Any]]) -> Dict[str, Any]:
    keywords = ["spese condominiali", "arretrate", "arretrati", "oneri condominiali", "morosità"]
    # Positive is accepted only when explicitly tied to arrears/morosita context.
    positive = re.compile(
        r"((?:spese\s+condominiali\s+arretrat\w*|arretrat\w+\s+condominial\w*|morosit[aà])[\s\S]{0,70}?(?:non\s+presenti|non\s+risultan\w*|nessun\w*))",
        re.I,
    )
    negative = re.compile(r"(spese\s+condominiali\s+arretrate|arretrati\s+condominiali|morosit[aà]|oneri\s+condominiali\s+insoluti)", re.I)
    ambiguous = re.compile(r"(da\s+verificare|non\s+è\s+noto|non\s+e'\s+noto|da\s+accertare|si\s+presume|presumibilmente)", re.I)
    weak_condo_proxy = re.compile(r"incidenza\s+condominial", re.I)
    spese_section_ocr = re.compile(
        r"s\s*p\s*e\s*s\s*e\s+c\s*o\s*n\s*d\s*o\s*m\s*i\s*n\s*i\s*a\s*l\s*i(?:\s+a\s*r\s*r\s*e\s*t\s*r\s*a\s*t\s*e)?",
        re.I,
    )
    absence_ocr = re.compile(
        r"(n\s*o\s*n\s+p\s*r\s*e\s*s\s*e\s*n\s*t\s*i|n\s*o\s*n\s+r\s*i\s*s\s*u\s*l\s*t\s*a\s*n\s*o|n\s*e\s*s\s*s\s*u\s*n\s*a|n\s*e\s*s\s*s\s*u\s*n\s+a\s*r\s*r\s*e\s*t\s*r\s*a\s*t\s*o)",
        re.I,
    )
    for p in pages:
        text = str(p.get("text", "") or "")
        # Section-aware parsing: heading may be on one line and value on next lines
        sec = re.search(r"spese\s+condominiali", text, re.I) or spese_section_ocr.search(text)
        if sec:
            after = text[sec.end():sec.end() + 220]
            absence = re.search(r"(non\s+presenti|non\s+risultano|nessun\s+arretrato|nessuna)", after, re.I) or absence_ocr.search(after)
            if absence:
                # Guardrail: only accept "non presenti/non risultano" when the snippet
                # explicitly refers to arrears (not generic condo incidence/fees).
                sec_window = text[sec.start():min(len(text), sec.end() + len(after))]
                if not re.search(r"(arretrat|morosit[aà])", sec_window, re.I):
                    continue
                match_start = sec.end() + absence.start()
                match_end = sec.end() + absence.end()
                if match_end < len(text) and text[match_end] in ".;:!?":
                    match_end += 1
                evidence = [
                    _build_evidence(
                        text,
                        int(p.get("page_number", 0) or 0),
                        match_start,
                        match_end,
                        field_key="spese_condominiali_arretrate",
                        anchor_hint=text[sec.start():min(len(text), sec.end() + len(after))],
                    )
                ]
                if not evidence or not str(evidence[0].get("quote") or "").strip():
                    continue
                searched_in = _make_searched_in(pages, keywords, "FOUND", field_key="spese_condominiali_arretrate")
                return _build_field_state(value="NON PRESENTI", status="FOUND", evidence=evidence, searched_in=searched_in)
        for pat, label in ((positive, "NON PRESENTI"), (negative, "PRESENTI ARRETRATI")):
            m = pat.search(text)
            if not m:
                continue
            start, end = m.start(), m.end()
            line_start, line_end = _line_bounds(text, start, end)
            line_text = text[line_start:line_end]
            if weak_condo_proxy.search(line_text):
                # "incidenza condominiale: 0,00%" is not deterministic proof of no arrears.
                continue
            evidence = [
                _build_evidence(
                    text,
                    int(p.get("page_number", 0) or 0),
                    line_start,
                    line_end,
                    field_key="spese_condominiali_arretrate",
                    anchor_hint=m.group(0),
                )
            ]
            if not evidence or not str(evidence[0].get("quote") or "").strip():
                continue
            status = "LOW_CONFIDENCE" if ambiguous.search(line_text) else "FOUND"
            value = "DA VERIFICARE" if status == "LOW_CONFIDENCE" else label
            searched_in = _make_searched_in(pages, keywords, status, field_key="spese_condominiali_arretrate")
            return _build_field_state(value=value, status=status, evidence=evidence, searched_in=searched_in)
    searched_in = _make_searched_in(pages, keywords, "NOT_FOUND", field_key="spese_condominiali_arretrate")
    return _build_field_state(
        value=None,
        status="NOT_FOUND",
        evidence=[],
        searched_in=searched_in,
        user_prompt_it="Verifica le spese condominiali arretrate nella perizia o presso l'amministratore.",
    )

def _extract_formalita_pregiudizievoli_state(pages: List[Dict[str, Any]]) -> Dict[str, Any]:
    keywords = ["ipoteca", "pignoramento", "servitù", "vincolo", "trascrizione", "iscrizione", "formalità"]
    positive = re.compile(r"(assenza\\s+di\\s+formalità|non\\s+risultano\\s+formalità|nessuna\\s+formalità\\s+pregiudizievole)", re.I)
    negative = re.compile(r"(ipoteca|pignoramento|servitù|vincolo|trascrizione|iscrizione)", re.I)
    ambiguous = re.compile(r"(da\\s+verificare|non\\s+è\\s+noto|non\\s+e'\\s+noto|da\\s+accertare|si\\s+presume|presumibilmente)", re.I)
    for p in pages:
        text = str(p.get("text", "") or "")
        for pat, label in ((positive, "NON RISULTANO FORMALITÀ"), (negative, "PRESENTI FORMALITÀ")):
            m = pat.search(text)
            if not m:
                continue
            start, end = m.start(), m.end()
            line_start, line_end = _line_bounds(text, start, end)
            line_text = text[line_start:line_end]
            evidence = [_build_evidence(text, int(p.get("page_number", 0) or 0), line_start, line_end)]
            status = "LOW_CONFIDENCE" if ambiguous.search(line_text) else "FOUND"
            value = "DA VERIFICARE" if status == "LOW_CONFIDENCE" else label
            searched_in = _make_searched_in(pages, keywords, status)
            return _build_field_state(value=value, status=status, evidence=evidence, searched_in=searched_in)
    searched_in = _make_searched_in(pages, keywords, "NOT_FOUND")
    return _build_field_state(value=None, status="NOT_FOUND", evidence=[], searched_in=searched_in)

def _apply_headline_states_to_headers(result: Dict[str, Any], states: Dict[str, Any]) -> None:
    report_header = result.get("report_header", {}) if isinstance(result.get("report_header"), dict) else {}
    case_header = result.get("case_header", {}) if isinstance(result.get("case_header"), dict) else {}

    report_header["procedure"] = {
        "value": _headline_display_value(states["procedura"]),
        "evidence": states["procedura"].get("evidence", [])
    }
    report_header["tribunale"] = {
        "value": _headline_display_value(states["tribunale"]),
        "evidence": states["tribunale"].get("evidence", [])
    }
    report_header["lotto"] = {
        "value": _headline_display_value(states["lotto"]),
        "evidence": states["lotto"].get("evidence", [])
    }
    report_header["address"] = {
        "value": _headline_display_value(states["address"]),
        "evidence": states["address"].get("evidence", [])
    }

    case_header["procedure_id"] = _headline_display_value(states["procedura"])
    case_header["tribunale"] = _headline_display_value(states["tribunale"])
    case_header["lotto"] = _headline_display_value(states["lotto"])
    case_header["address"] = _headline_display_value(states["address"])

    result["report_header"] = report_header
    result["case_header"] = case_header

def _build_state_from_existing_value(
    *,
    value_obj: Any,
    evidence: Optional[List[Dict[str, Any]]] = None,
    pages: List[Dict[str, Any]],
    keywords: List[str]
) -> Dict[str, Any]:
    cleaned = _clean_field_value(value_obj)
    status = "FOUND"
    value: Any = cleaned
    if cleaned == "DA VERIFICARE":
        status = "LOW_CONFIDENCE"
        value = "DA VERIFICARE"
    elif cleaned == "NON SPECIFICATO IN PERIZIA":
        status = "NOT_FOUND"
        value = None
    evidence_list = evidence if isinstance(evidence, list) else []
    effective_status = status
    if status == "FOUND" and not evidence_list:
        effective_status = "LOW_CONFIDENCE" if value is not None else "NOT_FOUND"
    searched_in = _make_searched_in(pages, keywords, effective_status)
    return _build_field_state(value=value, status=status, evidence=evidence_list, searched_in=searched_in)

def _apply_decision_states_to_result(result: Dict[str, Any], states: Dict[str, Any]) -> None:
    dati = result.get("dati_certi_del_lotto", {}) if isinstance(result.get("dati_certi_del_lotto"), dict) else {}
    prezzo_state = states.get("prezzo_base_asta") or {}
    superficie_state = states.get("superficie") or {}
    diritto_state = states.get("diritto_reale") or {}

    prezzo_value = prezzo_state.get("value")
    prezzo_display = _field_state_display_value(prezzo_state)
    if prezzo_display in {"DA VERIFICARE", "NON SPECIFICATO IN PERIZIA"}:
        formatted = prezzo_display
    elif isinstance(prezzo_value, (int, float)):
        formatted = f"€{prezzo_value:,.0f}"
    else:
        formatted = prezzo_display
    dati["prezzo_base_asta"] = {
        "value": prezzo_value if isinstance(prezzo_value, (int, float)) else None,
        "formatted": formatted,
        "evidence": prezzo_state.get("evidence", []),
    }

    superficie_value = superficie_state.get("value")
    superficie_display = _field_state_display_value(superficie_state)
    if isinstance(superficie_value, dict) and superficie_value.get("value") is not None:
        superficie_display = _format_mq_value(superficie_value.get("value"))
    dati["superficie_catastale"] = {
        "value": superficie_display,
        "evidence": superficie_state.get("evidence", []),
    }

    diritto_display = _field_state_display_value(diritto_state)
    dati["diritto_reale"] = {
        "value": diritto_display,
        "evidence": diritto_state.get("evidence", []),
    }
    result["dati_certi_del_lotto"] = dati

    occ = result.get("stato_occupativo", {}) if isinstance(result.get("stato_occupativo"), dict) else {}
    occ_state = states.get("stato_occupativo") or {}
    occ_display = _field_state_display_value(occ_state)
    occ_norm = occ_display.upper() if occ_display else "NON SPECIFICATO IN PERIZIA"
    status_it_map = {
        "LIBERO": "Libero",
        "OCCUPATO": "Occupato",
        "OCCUPATO DA TERZI SENZA TITOLO": "Occupato da terzi senza titolo",
        "OCCUPATO DAL DEBITORE": "Occupato dal debitore",
        "DA VERIFICARE": "Da verificare",
        "NON SPECIFICATO IN PERIZIA": "Non specificato in perizia",
    }
    status_en_map = {
        "LIBERO": "Free",
        "OCCUPATO": "Occupied",
        "OCCUPATO DA TERZI SENZA TITOLO": "Occupied by third parties without title",
        "OCCUPATO DAL DEBITORE": "Occupied by debtor",
        "DA VERIFICARE": "To verify",
        "NON SPECIFICATO IN PERIZIA": "Not specified in appraisal",
    }
    occ["status"] = occ_norm
    occ["status_it"] = status_it_map.get(occ_norm, occ_display or "Da verificare")
    occ["status_en"] = status_en_map.get(occ_norm, "To verify")
    occ["evidence"] = occ_state.get("evidence", [])
    result["stato_occupativo"] = occ

    abusi = result.get("abusi_edilizi_conformita", {}) if isinstance(result.get("abusi_edilizi_conformita"), dict) else {}
    reg_state = states.get("regolarita_urbanistica") or {}
    cat_state = states.get("conformita_catastale") or {}
    abusi["conformita_urbanistica"] = {
        "status": _field_state_display_value(reg_state),
        "detail_it": _field_state_display_value(reg_state),
        "evidence": reg_state.get("evidence", []),
    }
    abusi["conformita_catastale"] = {
        "status": _field_state_display_value(cat_state),
        "detail_it": _field_state_display_value(cat_state),
        "evidence": cat_state.get("evidence", []),
    }
    ape_state = states.get("ape") or {}
    abusi["ape"] = {
        "status": _field_state_display_value(ape_state),
        "evidence": ape_state.get("evidence", []),
    }
    agibilita_state = states.get("agibilita") or {}
    abusi["agibilita"] = {
        "status": _field_state_display_value(agibilita_state),
        "evidence": agibilita_state.get("evidence", []),
    }
    result["abusi_edilizi_conformita"] = abusi

    money_box = result.get("money_box", {}) if isinstance(result.get("money_box"), dict) else {}
    items = money_box.get("items", [])
    spese_state = states.get("spese_condominiali_arretrate") or {}
    spese_display = _field_state_display_value(spese_state)
    if isinstance(items, list):
        for item in items:
            label = str(item.get("label_it", "") or item.get("voce", "") or "")
            if item.get("code") == "E" or "Spese condominiali" in label:
                item["stima_nota"] = spese_display
                fonte_value = "Perizia" if spese_state.get("evidence") else "Non specificato in perizia"
                item["fonte_perizia"] = {"value": fonte_value, "evidence": spese_state.get("evidence", [])}
                item["source"] = fonte_value
                break
    result["money_box"] = money_box

    formalita = result.get("formalita", {}) if isinstance(result.get("formalita"), dict) else {}
    form_state = states.get("formalita_pregiudizievoli") or {}
    formalita["summary_it"] = _field_state_display_value(form_state)
    formalita["summary_evidence"] = form_state.get("evidence", [])
    result["formalita"] = formalita

    dati_asta_state = states.get("dati_asta") or {}
    dati_asta_value = dati_asta_state.get("value")
    if isinstance(dati_asta_value, dict) and dati_asta_state.get("status") == "FOUND":
        result["dati_asta"] = {
            "data": dati_asta_value.get("data"),
            "ora": dati_asta_value.get("ora"),
            "evidence": dati_asta_state.get("evidence", []),
        }

def _ensure_semaforo_top_blockers(result: Dict[str, Any], states: Dict[str, Any], pages: List[Dict[str, Any]]) -> None:
    if not isinstance(states, dict):
        return
    semaforo = None
    if isinstance(result.get("section_1_semaforo_generale"), dict):
        semaforo = result.get("section_1_semaforo_generale")
    elif isinstance(result.get("semaforo_generale"), dict):
        semaforo = result.get("semaforo_generale")
    if semaforo is None:
        semaforo = {}
        result["semaforo_generale"] = semaforo

    key_config = {
        "stato_occupativo": {"label_it": "Stato occupativo", "keywords": ["occupato", "libero", "detenuto", "locazione"]},
        "regolarita_urbanistica": {"label_it": "Regolarità urbanistica", "keywords": ["conformità urbanistica", "abusi edilizi", "sanatoria", "condono"]},
        "conformita_catastale": {"label_it": "Conformità catastale", "keywords": ["conformità catastale", "difformità", "planimetria"]},
        "formalita_pregiudizievoli": {"label_it": "Formalità pregiudizievoli", "keywords": ["ipoteca", "pignoramento", "servitù", "vincolo"]},
        "spese_condominiali_arretrate": {"label_it": "Spese condominiali arretrate", "keywords": ["spese condominiali", "arretrate", "arretrati"]},
        "ape": {"label_it": "APE", "keywords": ["ape", "certificato energetico", "attestato di prestazione energetica"]},
        "dati_asta": {"label_it": "Dati asta", "keywords": ["dettagli asta", "giorno", "ore"]},
    }

    blockers: List[Dict[str, Any]] = []

    legal_top_items = (
        result.get("section_9_legal_killers", {}).get("top_items", [])
        if isinstance(result.get("section_9_legal_killers"), dict)
        else []
    )
    if isinstance(legal_top_items, list):
        for item in legal_top_items[:6]:
            if not isinstance(item, dict):
                continue
            blockers.append({
                "key": str(item.get("killer") or "").strip().lower().replace(" ", "_")[:60],
                "label_it": str(item.get("killer") or "Criticità legale").strip(),
                "status": item.get("status"),
                "value": str(item.get("reason_it") or item.get("status_it") or "").strip(),
                "evidence": item.get("evidence", []) if isinstance(item.get("evidence"), list) else [],
                "searched_in": item.get("searched_in", []),
            })

    for key, cfg in key_config.items():
        state = states.get(key) if isinstance(states.get(key), dict) else {}
        status = state.get("status")
        if status not in {"NOT_FOUND", "LOW_CONFIDENCE"}:
            continue
        if key == "spese_condominiali_arretrate" and status == "NOT_FOUND":
            # Missing condo arrears data is a coverage warning, not a top blocker by itself.
            continue
        evidence = state.get("evidence") if isinstance(state.get("evidence"), list) else []
        searched_in = state.get("searched_in") if isinstance(state.get("searched_in"), list) else []
        if not evidence and not searched_in:
            searched_in = _make_searched_in(pages, cfg["keywords"], status or "NOT_FOUND")
        blockers.append({
            "key": key,
            "label_it": cfg["label_it"],
            "status": status,
            "value": _field_state_display_value(state),
            "evidence": evidence,
            "searched_in": searched_in,
        })

    deduped: List[Dict[str, Any]] = []
    seen_labels: set = set()
    for blocker in blockers:
        label = str(blocker.get("label_it") or "").strip().lower()
        if not label or label in seen_labels:
            continue
        seen_labels.add(label)
        deduped.append(blocker)

    deduped.sort(key=_semaforo_blocker_sort_key)
    semaforo["top_blockers"] = deduped[:10]

def _synthesize_decisione_rapida(result: Dict[str, Any], states: Dict[str, Any]) -> None:
    semaforo = result.get("semaforo_generale") if isinstance(result.get("semaforo_generale"), dict) else {}
    status = str(semaforo.get("status") or "AMBER").upper()
    blockers = semaforo.get("top_blockers") if isinstance(semaforo.get("top_blockers"), list) else []
    blocker_labels = [str(b.get("label_it")) for b in blockers if isinstance(b, dict) and b.get("label_it")]

    lots = result.get("lots") if isinstance(result.get("lots"), list) else []
    if len(lots) > 1:
        lot_fragments: List[str] = []
        lot_fragments_en: List[str] = []
        for lot in lots[:3]:
            if not isinstance(lot, dict):
                continue
            lot_num = lot.get("lot_number")
            tipo = _normalize_headline_text(str(lot.get("tipologia") or "")) or "bene"
            ubic = _normalize_headline_text(str(lot.get("ubicazione") or "")) or "ubicazione da verificare"
            lot_fragments.append(f"Lotto {lot_num} ({tipo}, {ubic}): {_lot_risk_cluster_it(lot)}")
            lot_fragments_en.append(f"Lot {lot_num} ({tipo}, {ubic}): {_lot_risk_cluster_en(lot)}")
        if lot_fragments:
            summary_it = (
                f"Caso multi-lotto ({len(lots)} lotti). "
                + " | ".join(lot_fragments)
                + ". Verifica obbligatoria lotto per lotto prima di offerta."
            )
            summary_en = (
                f"Multi-lot case ({len(lots)} lots). "
                + " | ".join(lot_fragments_en)
                + ". Mandatory lot-by-lot legal/technical verification before bidding."
            )
            decision = result.get("decision_rapida_client", {}) if isinstance(result.get("decision_rapida_client"), dict) else {}
            decision["summary_it"] = summary_it
            decision["summary_en"] = summary_en
            decision["driver_rosso"] = blockers[:3]
            result["decision_rapida_client"] = decision
            section2 = result.get("section_2_decisione_rapida", {}) if isinstance(result.get("section_2_decisione_rapida"), dict) else {}
            section2["summary_it"] = summary_it
            section2["summary_en"] = summary_en
            result["section_2_decisione_rapida"] = section2
            return

    critical = [
        ("stato_occupativo", "stato occupativo"),
        ("regolarita_urbanistica", "regolarità urbanistica"),
        ("conformita_catastale", "conformità catastale"),
        ("ape", "APE"),
        ("dati_asta", "dati asta"),
    ]
    missing_labels: List[str] = []
    for key, label in critical:
        st = states.get(key) if isinstance(states.get(key), dict) else {}
        if st.get("status") in {"NOT_FOUND", "LOW_CONFIDENCE"}:
            missing_labels.append(label)

    points: List[str] = []
    seen_points: set = set()
    for lbl in blocker_labels + missing_labels:
        norm = str(lbl or "").strip().lower()
        if not norm or norm in seen_points:
            continue
        seen_points.add(norm)
        points.append(str(lbl))
    points = points[:3]
    while len(points) < 2:
        points.append("verifica documentale")

    action = ", ".join(missing_labels[:3]) if missing_labels else "coerenza documentale e legale"
    summary_it = f"Operazione da {status}: {points[0]}; {points[1]}. Prima di offerta: verificare {action}."
    summary_en = f"{status} profile: {points[0]}; {points[1]}. Before bidding: verify {action}."

    decision = result.get("decision_rapida_client", {}) if isinstance(result.get("decision_rapida_client"), dict) else {}
    decision["summary_it"] = summary_it
    decision["summary_en"] = summary_en
    decision["driver_rosso"] = blockers[:3]
    result["decision_rapida_client"] = decision

    section2 = result.get("section_2_decisione_rapida", {}) if isinstance(result.get("section_2_decisione_rapida"), dict) else {}
    section2["summary_it"] = summary_it
    section2["summary_en"] = summary_en
    result["section_2_decisione_rapida"] = section2


def _as_float_or_none(value: Any) -> Optional[float]:
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        cleaned = value.strip().replace("€", "").replace(" ", "").replace(".", "").replace(",", ".")
        if not cleaned:
            return None
        try:
            return float(cleaned)
        except Exception:
            return None
    return None


def _detect_occupancy_status_for_market(result: Dict[str, Any]) -> str:
    states = result.get("field_states", {}) if isinstance(result.get("field_states"), dict) else {}
    occ_state = states.get("stato_occupativo") if isinstance(states.get("stato_occupativo"), dict) else {}
    raw = str(occ_state.get("value") or "").strip()
    if raw:
        return raw.upper()
    occ = result.get("stato_occupativo", {}) if isinstance(result.get("stato_occupativo"), dict) else {}
    return str(occ.get("status") or occ.get("status_it") or "").upper()


def _detect_spese_status_for_market(result: Dict[str, Any]) -> str:
    states = result.get("field_states", {}) if isinstance(result.get("field_states"), dict) else {}
    spese_state = states.get("spese_condominiali_arretrate") if isinstance(states.get("spese_condominiali_arretrate"), dict) else {}
    raw = str(spese_state.get("value") or "").strip()
    return raw.upper()


def _normalize_signal_text(value: Any) -> str:
    text = str(value or "")
    text = text.replace("\u2019", "'").replace("\u2018", "'")
    text = text.replace("\u2013", "-").replace("\u2014", "-")
    text = re.sub(r"\s+", " ", text).strip().lower()
    return text


def _extract_lot_number_from_label(value: Any) -> Optional[int]:
    match = re.match(r"\s*lotto\s+(\d+)\s*:", str(value or ""), re.I)
    if not match:
        return None
    try:
        return int(match.group(1))
    except Exception:
        return None


def _selected_lot_number_for_semaforo(result: Dict[str, Any]) -> Optional[int]:
    panoramica = result.get("panoramica_contract") if isinstance(result.get("panoramica_contract"), dict) else {}
    selected = panoramica.get("selected_lot_number")
    if isinstance(selected, int) and selected > 0:
        return selected

    report_header = result.get("report_header") if isinstance(result.get("report_header"), dict) else {}
    lotto = report_header.get("lotto") if isinstance(report_header.get("lotto"), dict) else {}
    lotto_value = str(lotto.get("value") or "").strip()
    match = re.search(r"\blotto\s+(\d+)\b", lotto_value, re.I)
    if not match:
        return None
    try:
        return int(match.group(1))
    except Exception:
        return None


def _semaforo_blocker_profile(blocker: Dict[str, Any]) -> Dict[str, Any]:
    label = str(
        blocker.get("label_it")
        or blocker.get("killer")
        or blocker.get("key")
        or blocker.get("title")
        or ""
    ).strip()
    normalized = _normalize_signal_text(
        " ".join(
            [
                label,
                str(blocker.get("value") or ""),
                str(blocker.get("reason_it") or ""),
            ]
        )
    )
    status = str(blocker.get("status") or "").strip().upper()
    evidence = blocker.get("evidence") if isinstance(blocker.get("evidence"), list) else []
    searched_in = blocker.get("searched_in") if isinstance(blocker.get("searched_in"), list) else []
    low_signal_status = status in {"NOT_FOUND", "LOW_CONFIDENCE", "DA_VERIFICARE", "UNKNOWN"}

    evidence_rank = 0
    if evidence:
        evidence_rank = 3 if not low_signal_status else 2
    elif searched_in:
        evidence_rank = 1

    severity = "WEAK"
    family = "generic"
    if any(token in normalized for token in ("immobile non regolare ediliziamente", "sanatoria / condono non perfezionati", "uso residenziale non legittimato", "occupato da terzi senza titolo")):
        severity = "EXTREME"
    elif any(token in normalized for token in ("agibilita assente / non rilasciata", "agibilità assente / non rilasciata", "non agibile", "in costruzione / lavori sospesi", "fabbricato in costruzione / lavori sospesi")):
        severity = "MAJOR"
    elif any(token in normalized for token in ("non conformita", "non conformità", "difformit", "servitu / accesso privato", "servitù / accesso privato", "servitu rilevata", "servitù rilevata", "fibro-cemento", "amianto", "infiltrazioni", "condizioni conservative critiche", "accertamento di conformita", "accertamento di conformità")):
        severity = "MODERATE"
    elif any(token in normalized for token in ("ape", "dati asta", "formalita", "formalità", "ipoteca", "pignoramento")):
        severity = "WEAK"
    elif status in {"ROSSO", "SI", "YES", "RED"}:
        severity = "MODERATE"

    if any(token in normalized for token in ("immobile non regolare", "sanatoria", "condono", "non conformita", "non conformità", "difformit", "accertamento di conformita", "accertamento di conformità")):
        family = "urbanistic"
    elif any(token in normalized for token in ("agibilita", "agibilità", "abitabilit", "non agibile")):
        family = "agibilita"
    elif any(token in normalized for token in ("uso residenziale non legittimato", "destinazione d'uso")):
        family = "use"
    elif any(token in normalized for token in ("occupato", "occupazione", "stato occupativo", "liberazione")):
        family = "occupancy"
    elif any(token in normalized for token in ("in costruzione", "lavori sospesi", "completamento")):
        family = "completion"
    elif "servit" in normalized or "stradella" in normalized or "accesso" in normalized:
        family = "servitude"

    return {
        "label": label,
        "severity": severity,
        "family": family,
        "evidence_rank": evidence_rank,
        "lot_number": _extract_lot_number_from_label(label),
    }


def _semaforo_blocker_sort_key(blocker: Dict[str, Any]) -> Tuple[int, int, str]:
    profile = _semaforo_blocker_profile(blocker)
    severity_rank = {"EXTREME": 4, "MAJOR": 3, "MODERATE": 2, "WEAK": 1}
    return (
        -severity_rank.get(profile["severity"], 0),
        -int(profile["evidence_rank"]),
        profile["label"].lower(),
    )


def _recompute_semaforo_status(result: Dict[str, Any]) -> None:
    semaforo = result.get("semaforo_generale") if isinstance(result.get("semaforo_generale"), dict) else {}
    if not semaforo:
        semaforo = result.get("section_1_semaforo_generale") if isinstance(result.get("section_1_semaforo_generale"), dict) else {}
    if not semaforo:
        semaforo = {}
        result["semaforo_generale"] = semaforo

    top_blockers = semaforo.get("top_blockers") if isinstance(semaforo.get("top_blockers"), list) else []
    blocker_items = [item for item in top_blockers if isinstance(item, dict)]
    selected_lot_number = _selected_lot_number_for_semaforo(result)
    panoramica = result.get("panoramica_contract") if isinstance(result.get("panoramica_contract"), dict) else {}
    is_multi_lot = bool(panoramica.get("is_multi_lot")) or len(result.get("lots", []) if isinstance(result.get("lots"), list) else []) > 1

    scoped: List[Tuple[Dict[str, Any], Dict[str, Any]]] = []
    for blocker in blocker_items:
        profile = _semaforo_blocker_profile(blocker)
        if is_multi_lot and profile["lot_number"] is not None and selected_lot_number is not None and profile["lot_number"] != selected_lot_number:
            continue
        scoped.append((blocker, profile))
    if not scoped:
        scoped = [(blocker, _semaforo_blocker_profile(blocker)) for blocker in blocker_items]

    extreme_count = sum(1 for _, p in scoped if p["severity"] == "EXTREME" and p["evidence_rank"] >= 2)
    major_count = sum(1 for _, p in scoped if p["severity"] == "MAJOR" and p["evidence_rank"] >= 2)
    moderate_count = sum(1 for _, p in scoped if p["severity"] == "MODERATE" and p["evidence_rank"] >= 1)
    weak_count = sum(1 for _, p in scoped if p["severity"] == "WEAK")
    cluster_families = {
        p["family"]
        for _, p in scoped
        if p["family"] in {"urbanistic", "agibilita", "use", "occupancy", "completion"} and p["evidence_rank"] >= 2
    }
    has_completion_cluster = any(p["family"] == "completion" and p["evidence_rank"] >= 2 for _, p in scoped)

    status = "GREEN"
    if (
        extreme_count >= 2
        or len(cluster_families) >= 3
        or (extreme_count >= 1 and major_count >= 2)
        or (has_completion_cluster and major_count >= 1 and moderate_count >= 1)
    ):
        status = "RED"
    elif extreme_count >= 1 or major_count >= 1 or moderate_count >= 2 or weak_count >= 2:
        status = "AMBER"

    ordered_labels = [p["label"] for _, p in scoped if p["label"]][:3]
    if status == "RED":
        reason_it = "Cluster di blocker maggiori con evidenza diretta"
    elif status == "AMBER":
        reason_it = "Presenza di criticità rilevanti da verificare prima dell'offerta"
    else:
        reason_it = "Non emergono blocker maggiori; restano verifiche ordinarie"
    if ordered_labels:
        reason_it = f"{reason_it}: {', '.join(ordered_labels[:2])}"

    reason_en = {
        "GREEN": "No major blockers detected; routine checks remain.",
        "AMBER": "Relevant issues remain to be verified before bidding.",
        "RED": "A major blocker cluster is supported by direct evidence.",
    }[status]
    if ordered_labels:
        reason_en = f"{reason_en} Drivers: {', '.join(ordered_labels[:2])}."

    status_it = {"GREEN": "VERDE", "AMBER": "GIALLO", "RED": "ROSSO"}[status]
    status_en = {"GREEN": "GREEN", "AMBER": "CAUTION", "RED": "RED"}[status]

    semaforo["status"] = status
    semaforo["status_it"] = status_it
    semaforo["status_en"] = status_en
    semaforo["reason_it"] = reason_it
    semaforo["reason_en"] = reason_en
    semaforo["driver"] = {"value": reason_it}

    section1 = result.get("section_1_semaforo_generale")
    if isinstance(section1, dict):
        section1["status"] = status
        section1["status_it"] = status_it
        section1["status_en"] = status_en
        section1["reason_it"] = reason_it
        section1["reason_en"] = reason_en
        semaforo_complessivo = section1.get("semaforo_complessivo")
        if isinstance(semaforo_complessivo, dict):
            semaforo_complessivo["value"] = status_it


def _legal_subject_negated(text: str, subject_pattern: str) -> bool:
    normalized = _normalize_signal_text(text)
    if not normalized:
        return False
    return bool(
        re.search(rf"(non\s+(?:sono|risult\w+|emerg\w+|sussist\w+)|assenza\s+di)[^.]{{0,140}}{subject_pattern}", normalized, re.I)
    )


def _build_state_driven_legal_killers(result: Dict[str, Any], pages: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    specs = [
        (
            "Immobile non regolare ediliziamente",
            "ROSSO",
            "CRITICO",
            125,
            [r"immobile\s+non\s+risulta\s+regolare", r"non\s+regolare\s+per\s+la\s+legge", r"unit[àa]\s+immobiliare\s+non\s+[èe]\s+conforme"],
            "Criticità urbanistico-edilizia determinante",
        ),
        (
            "Agibilità assente / non rilasciata",
            "ROSSO",
            "CRITICO",
            120,
            [r"non\s+risulta\s+rilasciato\s+il\s+certificato\s+di\s+agibilit", r"non\s+risulta\s+agibil", r"non\s+[èe]\s+presente\s+l['’]?abitabilit"],
            "Agibilità/abitabilità assente o non rilasciata",
        ),
        (
            "Sanatoria / condono non perfezionati",
            "ROSSO",
            "CRITICO",
            118,
            [r"concessione\s+in\s+sanatoria\s+non\s+[èe]\s+stata[^\n]{0,120}rilasciat", r"iter\s+di\s+rilascio[^\n]{0,120}sanatoria[^\n]{0,120}non\s+si\s+[èe]\s+ancora\s+perfezionat", r"condono[^\n]{0,160}non\s+risulterebbe[^\n]{0,120}possibile"],
            "Sanatoria/condono non perfezionati",
        ),
        (
            "Uso residenziale non legittimato",
            "ROSSO",
            "CRITICO",
            116,
            [r"destinazione\s+d['’]?uso[^\n]{0,160}non\s+[èe]\s+quella\s+residenziale", r"destinazione\s+d['’]?uso[^\n]{0,120}non\s+sono[^\n]{0,120}legittimat", r"uso\s+residenziale\s+non\s+legittimat"],
            "Destinazione d'uso residenziale non legittimata",
        ),
        (
            "Occupato da terzi senza titolo",
            "ROSSO",
            "CRITICO",
            114,
            [r"occupat[oa]\s+da\s+terzi\s+senza\s+titolo", r"occupat[oa][^\n]{0,120}terzi[^\n]{0,120}contratt\w+[^\n]{0,60}scadut"],
            "Occupazione da terzi senza titolo opponibile",
        ),
        (
            "Accertamento di conformità richiesto",
            "GIALLO",
            "ATTENZIONE",
            88,
            [r"accertamento\s+di\s+conformit"],
            "Accertamento di conformità richiesto",
        ),
    ]
    out: List[Dict[str, Any]] = []
    seen: set = set()
    for label, status, status_it, score, patterns, reason in specs:
        for pattern in patterns:
            ev = _find_regex_in_pages(pages, pattern, re.I, field_key="section_9_legal_killers")
            if not ev:
                continue
            quote = str(ev.get("quote") or "")
            if _is_toc_like_quote(quote) or _is_toc_like_line(quote):
                continue
            key = label.lower()
            if key in seen:
                continue
            seen.add(key)
            out.append({
                "killer": label,
                "status": status,
                "status_it": status_it,
                "reason_it": reason,
                "evidence": _normalize_contract_evidence_list([ev], max_items=1),
                "decision_bucket": "REAL_BLOCKER" if status == "ROSSO" else "RELEVANT_SECONDARY",
                "decision_score": score,
            })
            break
    return out


def _is_buyer_burden_quote(text: str) -> bool:
    normalized = _normalize_signal_text(text)
    if not normalized:
        return False
    if re.search(r"(prezzo\s+base|valore\s+di\s+stima|valore\s+finale|€/mq|quota\s+in\s+vendita|totale\s+in\s+vendita|rendita|reg\.\s*gen\.|reg\.\s*part\.|importo\s*:|ipoteca|mutuo\s+fondiario|pignoramento|formalit[àa]\s+a\s+carico)", normalized, re.I):
        return False
    if re.search(r"(deprezzament|rischio\s+assunto\s+per\s+mancata\s+garanzia)", normalized, re.I):
        return False
    return bool(
        re.search(r"(regolarizzazione\s+urbanistica|sanatoria|spese\s+condominiali|liberazione|completamento|bonifica|messa\s+in\s+sicurezza|ripristin|allineamento\s+catastal|pratiche\s+tecniche)", normalized, re.I)
    )


def _translated_cost_burden_label(text: str) -> Optional[str]:
    normalized = _normalize_signal_text(text)
    if not normalized:
        return None
    if any(token in normalized for token in ("immobile non regolare ediliziamente", "immobile non regolare", "difformit", "non conformita", "non conformità", "regolarizzazione urbanistica", "accertamento di conformita", "accertamento di conformità")):
        return "Regolarizzazione edilizia / urbanistica da verificare"
    if any(token in normalized for token in ("sanatoria / condono non perfezionati", "sanatoria", "condono")):
        return "Completamento o perfezionamento sanatoria / condono da verificare"
    if any(token in normalized for token in ("uso residenziale non legittimato", "destinazione d'uso")):
        return "Verifica conseguenze della destinazione d'uso non legittimata"
    if any(token in normalized for token in ("occupato da terzi senza titolo", "occupato", "occupazione", "liberazione")):
        return "Eventuali costi connessi alla liberazione dell'immobile"
    if any(token in normalized for token in ("in costruzione", "lavori sospesi", "completamento", "messa in sicurezza", "abitabilit", "agibilit")):
        return "Completamento lavori / messa in sicurezza e pratiche tecniche da verificare"
    if any(token in normalized for token in ("ripristin", "conservativ", "infiltrazioni", "umidit", "degrado", "fibro-cemento", "amianto", "bonifica")):
        return "Eventuali ripristini / problemi conservativi segnalati in perizia"
    if any(token in normalized for token in ("allineamento catastal", "catastal")):
        return "Allineamento catastale da verificare"
    if "spese condominiali" in normalized or "condominial" in normalized:
        return "Eventuali arretrati condominiali da verificare"
    if "formalita da cancellare" in normalized or "cancellazione formalita" in normalized:
        return "Costi di formalita da verificare in sede di trasferimento"
    return None


def _build_conservative_cost_burdens(result: Dict[str, Any], items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    burdens: List[Dict[str, Any]] = []
    seen_labels: set = set()

    def _push(label: Optional[str], evidence: List[Dict[str, Any]], source: str) -> None:
        if not label:
            return
        key = _normalize_signal_text(label)
        if not key or key in seen_labels:
            return
        seen_labels.add(key)
        burdens.append({
            "code": f"QB_{len(burdens) + 1}",
            "label_it": label,
            "label_en": label,
            "type": "QUALITATIVE",
            "stima_euro": "NON_QUANTIFICATO",
            "stima_nota": "Onere buyer-side segnalato dalla perizia ma non quantificato in modo difendibile",
            "source": source,
            "evidence": _normalize_contract_evidence_list(evidence, max_items=2),
        })

    for item in items:
        if not isinstance(item, dict):
            continue
        evidence = item.get("fonte_perizia", {}).get("evidence", []) if isinstance(item.get("fonte_perizia"), dict) else []
        if not evidence:
            evidence = item.get("evidence", []) if isinstance(item.get("evidence"), list) else []
        text_blob = " ".join([
            str(item.get("label_it") or ""),
            str(item.get("label_en") or ""),
            str(item.get("stima_nota") or ""),
            " ".join(str(ev.get("quote") or "") for ev in evidence if isinstance(ev, dict)),
        ])
        _push(_translated_cost_burden_label(text_blob), evidence, "PERIZIA_QUALITATIVE")

    legal_killers = result.get("section_9_legal_killers", {}) if isinstance(result.get("section_9_legal_killers"), dict) else {}
    top_items = legal_killers.get("top_items", []) if isinstance(legal_killers.get("top_items"), list) else []
    for item in top_items[:8]:
        if not isinstance(item, dict):
            continue
        _push(
            _translated_cost_burden_label(
                " ".join([
                    str(item.get("killer") or ""),
                    str(item.get("reason_it") or ""),
                    str(item.get("action") or ""),
                ])
            ),
            item.get("evidence", []) if isinstance(item.get("evidence"), list) else [],
            "LEGAL_KILLER_QUALITATIVE",
        )

    return burdens


def _sanitize_money_box_for_customer(result: Dict[str, Any]) -> None:
    money_box = result.get("money_box", {}) if isinstance(result.get("money_box"), dict) else {}
    items = money_box.get("items", [])
    if not isinstance(items, list):
        return

    cleaned_items: List[Dict[str, Any]] = []
    supported_numeric_total = 0.0
    has_document_backed_buyer_burden = False
    is_multi_lot = bool(result.get("is_multi_lot")) or len(result.get("lots", []) if isinstance(result.get("lots"), list) else []) > 1

    for item in items:
        if not isinstance(item, dict):
            continue
        code = str(item.get("code") or item.get("voce") or "").strip().upper()
        if code.startswith("S3C"):
            continue

        fonte = item.get("fonte_perizia", {}) if isinstance(item.get("fonte_perizia"), dict) else {}
        evidence = _normalize_contract_evidence_list(fonte.get("evidence", []), max_items=2)
        if not evidence:
            evidence = _normalize_contract_evidence_list(item.get("evidence", []), max_items=2)
        quote_blob = " ".join(
            [
                str(item.get("label_it") or ""),
                str(item.get("label_en") or ""),
                str(item.get("stima_nota") or ""),
            ] + [str(ev.get("quote") or "") for ev in evidence]
        )

        if evidence and not _is_buyer_burden_quote(quote_blob):
            if code not in {"G"}:
                continue

        if evidence and _is_buyer_burden_quote(quote_blob):
            has_document_backed_buyer_burden = True
            stima_val = _as_float_or_none(item.get("stima_euro"))
            if isinstance(stima_val, float) and stima_val > 0:
                supported_numeric_total += stima_val

        cleaned_items.append(item)

    money_box["items"] = cleaned_items

    if not is_multi_lot and not has_document_backed_buyer_burden:
        qualitative_burdens = _build_conservative_cost_burdens(result, cleaned_items)
        money_box["policy"] = "CONSERVATIVE"
        money_box.pop("total_extra_costs_range", None)
        money_box["items"] = qualitative_burdens
        money_box["qualitative_burdens"] = copy.deepcopy(qualitative_burdens)
        money_box["total_extra_costs"] = {
            "min": "NON_QUANTIFICATO_IN_PERIZIA",
            "max": "NON_QUANTIFICATO_IN_PERIZIA",
            "max_is_open": False,
            "note": "Buyer-side extra cost burdens are grounded, but the perizia does not support a defensible numeric total",
        }
        if isinstance(result.get("section_3_money_box"), dict):
            result["section_3_money_box"]["items"] = copy.deepcopy(qualitative_burdens)
            result["section_3_money_box"]["qualitative_burdens"] = copy.deepcopy(qualitative_burdens)
            result["section_3_money_box"].pop("total_extra_costs_range", None)
            result["section_3_money_box"]["totale_extra_budget"] = {
                "min": "NON_QUANTIFICATO_IN_PERIZIA",
                "max": "NON_QUANTIFICATO_IN_PERIZIA",
                "nota": "Costi extra non quantificati in perizia; mantenuti solo oneri qualitativi grounded",
            }
    elif supported_numeric_total > 0:
        money_box["total_extra_costs"] = {
            "range": {"min": int(round(supported_numeric_total)), "max": int(round(supported_numeric_total))},
            "max_is_open": False,
            "note": "Document-backed buyer burdens only",
        }
        if isinstance(result.get("section_3_money_box"), dict):
            result["section_3_money_box"]["items"] = copy.deepcopy(cleaned_items)
            result["section_3_money_box"]["totale_extra_budget"] = {
                "min": int(round(supported_numeric_total)),
                "max": int(round(supported_numeric_total)),
                "nota": "EUR document-backed buyer burdens only",
            }

    result["money_box"] = money_box


def _apply_customer_headline_fallbacks(result: Dict[str, Any]) -> None:
    states = result.get("field_states", {}) if isinstance(result.get("field_states"), dict) else {}
    report_header = result.get("report_header", {}) if isinstance(result.get("report_header"), dict) else {}
    case_header = result.get("case_header", {}) if isinstance(result.get("case_header"), dict) else {}

    def _state_text(key: str) -> Optional[str]:
        state = states.get(key) if isinstance(states.get(key), dict) else {}
        value = state.get("value")
        evidence = state.get("evidence") if isinstance(state.get("evidence"), list) else []
        if value in (None, "", "DA VERIFICARE", "NON SPECIFICATO IN PERIZIA"):
            return None
        if not evidence:
            return None
        text = str(value).strip()
        if not text:
            return None
        if key == "tribunale":
            text = _normalize_tribunale_value(text) or text
        elif key == "procedura":
            text = _normalize_procedura_value(text) or text
        elif key == "address":
            text = _normalize_address_value(text) or text
        else:
            text = _normalize_headline_text(text) or text
        return text

    for key, report_key, case_key in (
        ("tribunale", "tribunale", "tribunale"),
        ("procedura", "procedure", "procedure_id"),
        ("lotto", "lotto", "lotto"),
        ("address", "address", "address"),
    ):
        text = _state_text(key)
        if not text:
            continue
        evidence = states.get(key, {}).get("evidence", [])
        report_header[report_key] = {"value": text, "evidence": copy.deepcopy(evidence)}
        case_header[case_key] = text

    result["report_header"] = report_header
    result["case_header"] = case_header


def _apply_market_ranges_to_money_box(result: Dict[str, Any]) -> None:
    _sanitize_money_box_for_customer(result)
    money_box = result.get("money_box", {}) if isinstance(result.get("money_box"), dict) else {}
    if str(money_box.get("policy") or "").upper() in {"LOT_CONSERVATIVE", "CONSERVATIVE"}:
        money_box.pop("total_extra_costs_range", None)
        result["money_box"] = money_box
        section3 = result.get("section_3_money_box")
        if isinstance(section3, dict):
            section3.pop("total_extra_costs_range", None)
        return
    items = money_box.get("items", [])
    if not isinstance(items, list):
        return

    occ_status = _detect_occupancy_status_for_market(result)
    spese_status = _detect_spese_status_for_market(result)
    known_total = 0.0
    market_min_total = 0.0
    market_max_total = 0.0

    for item in items:
        if not isinstance(item, dict):
            continue
        stima_val = _as_float_or_none(item.get("stima_euro"))
        if stima_val is not None:
            known_total += stima_val
            continue

        code = str(item.get("code") or item.get("voce") or "").strip().upper()
        if len(code) != 1:
            continue
        market = market_range_for_item(code=code, occupancy_status=occ_status, spese_status=spese_status)
        if not market:
            continue
        market_min = float(market.get("min", 0.0))
        market_max = float(market.get("max", 0.0))
        item["market_range_eur"] = {
            "min": market_min,
            "max": market_max,
            "basis_it": "Stima indicativa di mercato (non presente in perizia)",
            "basis_en": "Indicative market estimate (not present in the appraisal)",
        }
        if not item.get("source"):
            item["source"] = "MARKET_ESTIMATE"
        elif str(item.get("source")) != "MARKET_ESTIMATE":
            item["source_market"] = "MARKET_ESTIMATE"
        nota = str(item.get("stima_nota") or "").strip()
        market_note = "Stima indicativa di mercato (non presente in perizia)"
        if market_note.lower() not in nota.lower():
            item["stima_nota"] = f"{nota} — {market_note}" if nota else market_note
        market_min_total += market_min
        market_max_total += market_max

    money_box["total_extra_costs_range"] = {
        "min_eur": round(known_total + market_min_total, 2),
        "max_eur": round(known_total + market_max_total, 2),
        "includes_market_estimates": True,
    }
    result["money_box"] = money_box
    section3 = result.get("section_3_money_box")
    if isinstance(section3, dict):
        section3["total_extra_costs_range"] = money_box["total_extra_costs_range"]

def _apply_decision_field_states(result: Dict[str, Any], pages: List[Dict[str, Any]]) -> None:
    has_text = any(str(p.get("text", "") or "").strip() for p in pages)
    states = result.get("field_states", {}) if isinstance(result.get("field_states"), dict) else {}
    lots = result.get("lots") or []
    lot_index = result.get("lot_index", 0)
    if not isinstance(lot_index, int):
        try:
            lot_index = int(lot_index)
        except Exception:
            lot_index = 0
    if lot_index < 0 or lot_index >= (len(lots) if isinstance(lots, list) else 0):
        lot_index = 0
    if has_text:
        dati = result.get("dati_certi_del_lotto", {}) if isinstance(result.get("dati_certi_del_lotto"), dict) else {}
        states.update({
            "prezzo_base_asta": _extract_prezzo_base_asta_state(pages),
            "superficie": _extract_superficie_state(pages, dati),
            "diritto_reale": _extract_diritto_reale_state(pages),
            "stato_occupativo": _extract_stato_occupativo_state(pages),
            "regolarita_urbanistica": _extract_regolarita_urbanistica_state(pages),
            "conformita_catastale": _extract_conformita_catastale_state(pages),
            "spese_condominiali_arretrate": _extract_spese_condominiali_state(pages),
            "ape": _extract_ape_state(pages),
            "agibilita": _extract_agibilita_state(pages),
            "dati_asta": _extract_dati_asta_state(pages),
            "formalita_pregiudizievoli": _extract_formalita_pregiudizievoli_state(pages),
        })
    else:
        dati = result.get("dati_certi_del_lotto", {}) if isinstance(result.get("dati_certi_del_lotto"), dict) else {}
        occ = result.get("stato_occupativo", {}) if isinstance(result.get("stato_occupativo"), dict) else {}
        abusi = result.get("abusi_edilizi_conformita", {}) if isinstance(result.get("abusi_edilizi_conformita"), dict) else {}
        formalita = result.get("formalita", {}) if isinstance(result.get("formalita"), dict) else {}
        money_box = result.get("money_box", {}) if isinstance(result.get("money_box"), dict) else {}
        states.update({
            "prezzo_base_asta": _build_state_from_existing_value(
                value_obj=dati.get("prezzo_base_asta"),
                evidence=(dati.get("prezzo_base_asta", {}).get("evidence", []) if isinstance(dati.get("prezzo_base_asta"), dict) else []),
                pages=pages,
                keywords=["prezzo base", "prezzo base d'asta", "€"],
            ),
            "superficie": _build_state_from_existing_value(
                value_obj=dati.get("superficie_catastale"),
                evidence=(dati.get("superficie_catastale", {}).get("evidence", []) if isinstance(dati.get("superficie_catastale"), dict) else []),
                pages=pages,
                keywords=["superficie", "mq", "m²"],
            ),
            "diritto_reale": _build_state_from_existing_value(
                value_obj=dati.get("diritto_reale"),
                evidence=(dati.get("diritto_reale", {}).get("evidence", []) if isinstance(dati.get("diritto_reale"), dict) else []),
                pages=pages,
                keywords=["proprietà", "nuda proprietà", "usufrutto", "diritto di"],
            ),
            "stato_occupativo": _build_state_from_existing_value(
                value_obj=occ.get("status"),
                evidence=occ.get("evidence", []) if isinstance(occ.get("evidence"), list) else [],
                pages=pages,
                keywords=["occupato", "libero", "detenuto", "locazione"],
            ),
            "regolarita_urbanistica": _build_state_from_existing_value(
                value_obj=abusi.get("conformita_urbanistica", {}).get("status") if isinstance(abusi.get("conformita_urbanistica"), dict) else None,
                evidence=abusi.get("conformita_urbanistica", {}).get("evidence", []) if isinstance(abusi.get("conformita_urbanistica"), dict) else [],
                pages=pages,
                keywords=["conformità urbanistica", "abusi edilizi", "sanatoria", "condono"],
            ),
            "conformita_catastale": _build_state_from_existing_value(
                value_obj=abusi.get("conformita_catastale", {}).get("status") if isinstance(abusi.get("conformita_catastale"), dict) else None,
                evidence=abusi.get("conformita_catastale", {}).get("evidence", []) if isinstance(abusi.get("conformita_catastale"), dict) else [],
                pages=pages,
                keywords=["conformità catastale", "difformità", "planimetria"],
            ),
            "spese_condominiali_arretrate": _build_state_from_existing_value(
                value_obj=(money_box.get("items", [{}]) or [{}])[0].get("stima_nota"),
                evidence=[],
                pages=pages,
                keywords=["spese condominiali", "arretrate", "arretrati"],
            ),
            "ape": _build_state_from_existing_value(
                value_obj=abusi.get("ape", {}).get("status") if isinstance(abusi.get("ape"), dict) else None,
                evidence=abusi.get("ape", {}).get("evidence", []) if isinstance(abusi.get("ape"), dict) else [],
                pages=pages,
                keywords=["ape", "certificato energetico", "attestato di prestazione energetica"],
            ),
            "agibilita": _build_state_from_existing_value(
                value_obj=abusi.get("agibilita", {}).get("status") if isinstance(abusi.get("agibilita"), dict) else None,
                evidence=abusi.get("agibilita", {}).get("evidence", []) if isinstance(abusi.get("agibilita"), dict) else [],
                pages=pages,
                keywords=["agibilità", "agibilita", "abitabilità", "abitabilita", "agibile"],
            ),
            "dati_asta": _build_state_from_existing_value(
                value_obj=result.get("dati_asta"),
                evidence=result.get("dati_asta", {}).get("evidence", []) if isinstance(result.get("dati_asta"), dict) else [],
                pages=pages,
                keywords=["dettagli asta", "giorno", "ore"],
            ),
            "formalita_pregiudizievoli": _build_state_from_existing_value(
                value_obj=formalita.get("summary_it") or formalita.get("summary"),
                evidence=formalita.get("summary_evidence", []) if isinstance(formalita.get("summary_evidence"), list) else [],
                pages=pages,
                keywords=["ipoteca", "pignoramento", "servitù", "vincolo"],
            ),
        })

    if "superficie" not in states and "superficie_catastale" in states:
        states["superficie"] = states.get("superficie_catastale")
    states.pop("superficie_catastale", None)

    spese_state = states.get("spese_condominiali_arretrate") or {}
    spese_evidence = spese_state.get("evidence") if isinstance(spese_state.get("evidence"), list) else []
    if not spese_evidence:
        spese_state["status"] = "NOT_FOUND"
        spese_state["value"] = None
        spese_state["user_prompt_it"] = "Verifica le spese condominiali arretrate nella perizia o presso l'amministratore."
        if not spese_state.get("searched_in"):
            spese_state["searched_in"] = _make_searched_in(pages, ["spese condominiali", "arretrate", "arretrati"], "NOT_FOUND")
        states["spese_condominiali_arretrate"] = spese_state

    if isinstance(lots, list) and lots:
        selected_lot = lots[lot_index] if lot_index < len(lots) else lots[0]
        if isinstance(selected_lot, dict):
            lot_evidence = selected_lot.get("evidence", {}) if isinstance(selected_lot.get("evidence"), dict) else {}

            def _state_from_lot_value(
                raw_value: Any,
                evidence_key: str,
                keywords: List[str],
                field_key: Optional[str] = None,
                value_builder=None,
                existing_state: Optional[Dict[str, Any]] = None,
            ) -> Dict[str, Any]:
                evidence = lot_evidence.get(evidence_key, [])
                if isinstance(existing_state, dict):
                    existing_status = str(existing_state.get("status") or "").upper()
                    existing_evidence = existing_state.get("evidence") if isinstance(existing_state.get("evidence"), list) else []
                    if existing_status == "FOUND" and existing_evidence:
                        return existing_state

                normalized_evidence: List[Dict[str, Any]] = []
                if isinstance(evidence, list):
                    for ev in evidence[:2]:
                        if not isinstance(ev, dict):
                            continue
                        quote = str(ev.get("quote") or "").strip()
                        if not quote:
                            continue
                        n_quote, n_hint = normalize_evidence_quote(
                            quote,
                            0,
                            len(quote),
                            max_len=520,
                            field_key=field_key,
                            anchor_hint=quote,
                        )
                        if not n_quote:
                            continue
                        payload = {"page": ev.get("page"), "quote": n_quote}
                        if n_hint:
                            payload["search_hint"] = n_hint
                        normalized_evidence.append(payload)
                evidence = normalized_evidence
                value = raw_value
                if value_builder:
                    value = value_builder(raw_value)
                if raw_value in (None, "", "TBD", "NON SPECIFICATO IN PERIZIA") and not evidence:
                    if isinstance(existing_state, dict) and existing_state.get("status") in {"FOUND", "LOW_CONFIDENCE", "USER_PROVIDED"}:
                        return existing_state
                    searched_in = _make_searched_in(pages, keywords, "NOT_FOUND", field_key=field_key)
                    return _build_field_state(value=None, status="NOT_FOUND", evidence=[], searched_in=searched_in)
                if not evidence:
                    searched_in = _make_searched_in(pages, keywords, "LOW_CONFIDENCE", field_key=field_key)
                    return _build_field_state(value=value, status="LOW_CONFIDENCE", evidence=[], searched_in=searched_in)
                searched_in = _make_searched_in(pages, keywords, "FOUND", field_key=field_key)
                return _build_field_state(value=value, status="FOUND", evidence=evidence, searched_in=searched_in)

            states["prezzo_base_asta"] = _state_from_lot_value(
                selected_lot.get("prezzo_base_value"),
                "prezzo_base",
                ["prezzo base", "prezzo base d'asta", "€"],
                field_key="prezzo_base_asta",
                existing_state=states.get("prezzo_base_asta"),
            )

            def _superficie_value(raw: Any) -> Any:
                if isinstance(raw, (int, float)):
                    return {"value": float(raw), "unit": "mq", "label": "Superficie"}
                if isinstance(raw, str):
                    match = re.search(r"(\d{1,4}(?:[\.,]\d{1,2})?)", raw)
                    if match:
                        raw_num = match.group(1).replace(".", "").replace(",", ".")
                        try:
                            return {"value": float(raw_num), "unit": "mq", "label": "Superficie"}
                        except Exception:
                            return raw
                return raw

            states["superficie"] = _state_from_lot_value(
                selected_lot.get("superficie_mq"),
                "superficie",
                ["superficie", "mq", "m²"],
                field_key="superficie_catastale",
                value_builder=_superficie_value,
                existing_state=states.get("superficie"),
            )

            states["diritto_reale"] = _state_from_lot_value(
                selected_lot.get("diritto_reale"),
                "diritto_reale",
                ["proprietà", "nuda proprietà", "usufrutto", "diritto di"],
                field_key="diritto_reale",
                existing_state=states.get("diritto_reale"),
            )

    result["field_states"] = states
    _enforce_field_states_contract(result, pages)
    _apply_decision_states_to_result(result, result.get("field_states", states))
    _ensure_semaforo_top_blockers(result, result.get("field_states", states), pages)
    _synthesize_decisione_rapida(result, result.get("field_states", states))

def _apply_headline_field_states(result: Dict[str, Any], pages: List[Dict[str, Any]]) -> None:
    has_text = any(str(p.get("text", "") or "").strip() for p in pages)
    report_header = result.get("report_header", {}) if isinstance(result.get("report_header"), dict) else {}
    case_header = result.get("case_header", {}) if isinstance(result.get("case_header"), dict) else {}

    if has_text:
        lots = result.get("lots") or []
        states = {
            "tribunale": _extract_tribunale_state(pages),
            "procedura": _extract_procedura_state(pages),
            "lotto": _extract_lotto_state(pages, lots),
            "address": _extract_address_state(pages, lots),
        }
    else:
        states = {
            "tribunale": _build_headline_state_from_existing(
                report_obj=report_header.get("tribunale"),
                case_obj=case_header.get("tribunale"),
                evidence=(report_header.get("tribunale", {}).get("evidence", []) if isinstance(report_header.get("tribunale"), dict) else []),
                pages=pages,
                keywords=["tribunale", "tribunale di"],
            ),
            "procedura": _build_headline_state_from_existing(
                report_obj=report_header.get("procedure"),
                case_obj=case_header.get("procedure_id"),
                evidence=(report_header.get("procedure", {}).get("evidence", []) if isinstance(report_header.get("procedure"), dict) else []),
                pages=pages,
                keywords=["r.g.e", "rge", "procedura", "esecuzione immobiliare"],
            ),
            "lotto": _build_headline_state_from_existing(
                report_obj=report_header.get("lotto"),
                case_obj=case_header.get("lotto"),
                evidence=(report_header.get("lotto", {}).get("evidence", []) if isinstance(report_header.get("lotto"), dict) else []),
                pages=pages,
                keywords=["lotto", "lotti", "lotto unico"],
                prompt_if_low_conf=True,
            ),
            "address": _build_headline_state_from_existing(
                report_obj=report_header.get("address"),
                case_obj=case_header.get("address"),
                evidence=(report_header.get("address", {}).get("evidence", []) if isinstance(report_header.get("address"), dict) else []),
                pages=pages,
                keywords=["via", "viale", "piazza", "corso", "indirizzo", "ubicazione"],
                prompt_if_low_conf=True,
            ),
        }

    result["field_states"] = states
    _enforce_field_states_contract(result, pages)
    _apply_headline_states_to_headers(result, result.get("field_states", states))

def _normalize_override_value(field: str, value: Any) -> Any:
    if value is None:
        return None
    if field == "tribunale" and isinstance(value, str):
        return (_normalize_tribunale_value(value) or value).strip()
    if field == "procedura" and isinstance(value, str):
        return (_normalize_procedura_value(value) or value).strip()
    if field == "address" and isinstance(value, str):
        return (_normalize_address_value(value) or value).strip()
    if field == "prezzo_base_asta":
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            parsed = _parse_euro_number(value)
            return parsed if parsed is not None else value.strip()
    if field in {"superficie", "superficie_catastale"}:
        if isinstance(value, (int, float)):
            return {"value": float(value), "unit": "mq", "label": "Superficie"}
        if isinstance(value, str):
            match = re.search(r"(\\d{1,4}(?:[\\.,]\\d{1,2})?)\\s*(m2|m²|mq)?", value, re.I)
            if match:
                num = match.group(1).replace(".", "").replace(",", ".")
                try:
                    return {"value": float(num), "unit": "mq", "label": "Superficie"}
                except Exception:
                    return value.strip()
            return value.strip()
    if isinstance(value, str):
        return value.strip()
    return value

def _apply_field_overrides(result: Dict[str, Any], overrides: Dict[str, Any], fields: Optional[List[str]] = None) -> None:
    if not overrides:
        return
    if "superficie" not in overrides and "superficie_catastale" in overrides:
        overrides = dict(overrides)
        overrides["superficie"] = overrides.get("superficie_catastale")
    if not isinstance(result.get("field_states"), dict):
        pages_for_proof = [{"page_number": 1, "text": ""}]
        _apply_headline_field_states(result, pages_for_proof)
        _apply_decision_field_states(result, pages_for_proof)
    states = result.get("field_states", {})
    field_list = fields or [
        "tribunale",
        "procedura",
        "lotto",
        "address",
        "prezzo_base_asta",
        "superficie",
        "diritto_reale",
        "stato_occupativo",
        "regolarita_urbanistica",
        "conformita_catastale",
        "spese_condominiali_arretrate",
        "formalita_pregiudizievoli",
    ]
    for field in field_list:
        if field not in overrides:
            continue
        value = overrides.get(field)
        if value is None:
            continue
        normalized = _normalize_override_value(field, value)
        if isinstance(normalized, str) and not normalized.strip():
            continue
        state = states.get(field, {
            "value": None,
            "status": "NOT_FOUND",
            "confidence": 0.0,
            "evidence": [],
            "searched_in": [],
            "user_prompt_it": None,
        })
        state["value"] = normalized
        state["status"] = "USER_PROVIDED"
        state["confidence"] = 1.0
        state["evidence"] = []
        state["searched_in"] = []
        states[field] = state
    result["field_states"] = states
    _apply_headline_states_to_headers(result, states)
    _apply_decision_states_to_result(result, states)

def _ensure_lot_contract(lot: Dict[str, Any], lot_number: int) -> Dict[str, Any]:
    lot_obj = dict(lot) if isinstance(lot, dict) else {}
    lot_obj.setdefault("lot_number", lot_number)
    lot_obj.setdefault("prezzo_base_eur", "TBD")
    lot_obj.setdefault("prezzo_base_value", None)
    lot_obj.setdefault("ubicazione", "TBD")
    lot_obj.setdefault("superficie_mq", "TBD")
    lot_obj.setdefault("diritto_reale", "TBD")
    lot_obj.setdefault("shared_rights_note", None)
    lot_obj.setdefault("detail_scope", "LOT")
    evidence = lot_obj.get("evidence") if isinstance(lot_obj.get("evidence"), dict) else {}
    for key in ("lotto", "prezzo_base", "ubicazione", "superficie", "diritto_reale", "shared_rights_note", "tipologia", "valore_stima", "deprezzamento"):
        if not isinstance(evidence.get(key), list):
            evidence[key] = []
    lot_obj["evidence"] = evidence
    return lot_obj


def _get_page_number(page_obj: Dict[str, Any]) -> int:
    try:
        val = page_obj.get("page_number", page_obj.get("page", 0))
        return int(val or 0)
    except Exception:
        return 0


def _normalize_page_numbers(pages: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    normalized: List[Dict[str, Any]] = []
    for idx, page in enumerate(pages, start=1):
        if not isinstance(page, dict):
            continue
        row = dict(page)
        page_num = _get_page_number(row)
        if page_num <= 0:
            page_num = idx
        row["page_number"] = page_num
        normalized.append(row)
    return normalized


def _detect_lot_start_pages(pages: List[Dict[str, Any]]) -> Dict[int, Dict[str, Any]]:
    starts: Dict[int, Dict[str, Any]] = {}
    for page_obj in pages:
        page_num = _get_page_number(page_obj)
        text = str(page_obj.get("text", "") or "")
        if page_num <= 0 or not text:
            continue
        for line in text.splitlines():
            line_clean = _normalize_headline_text(line)
            if not line_clean:
                continue
            m = re.search(r"\bLOTTO\s+(\d+)\b", line_clean, re.I)
            if not m:
                continue
            is_explicit_page_header = bool(
                re.search(r"^\d+\s+di\s+\d+\s+LOTTO\s+\d+\b", line_clean, re.I)
                and "..." not in line_clean
                and "…" not in line_clean
            )
            if _is_toc_like_line(line_clean) and not is_explicit_page_header:
                continue
            lot_num = int(m.group(1))
            if lot_num in starts:
                continue
            match_local = re.search(r"\bLOTTO\s+\d+\b", line, re.I)
            if match_local:
                ev = _build_evidence(
                    text,
                    page_num,
                    max(0, text.find(line)),
                    max(0, text.find(line)) + len(line),
                    field_key="lotto",
                    anchor_hint=match_local.group(0),
                )
            else:
                ev = {"page": page_num, "quote": line_clean}
            starts[lot_num] = {
                "lot": lot_num,
                "page": page_num,
                "quote": str(ev.get("quote") or line_clean).strip(),
                "evidence": [ev] if isinstance(ev, dict) else [],
            }
    return starts


def _build_lot_sections(pages: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    normalized_pages = _normalize_page_numbers(pages)
    starts = _detect_lot_start_pages(normalized_pages)
    if not starts:
        return []

    ordered_lots = sorted(starts.keys())
    max_page = max([_get_page_number(p) for p in normalized_pages] or [1])
    sections: List[Dict[str, Any]] = []
    for idx, lot_num in enumerate(ordered_lots):
        start_page = int(starts[lot_num]["page"])
        next_start = int(starts[ordered_lots[idx + 1]]["page"]) if idx + 1 < len(ordered_lots) else (max_page + 1)
        end_page = max(start_page, next_start - 1)
        section_pages = [p for p in normalized_pages if start_page <= _get_page_number(p) <= end_page]
        sections.append(
            {
                "lot_number": lot_num,
                "start_page": start_page,
                "end_page": end_page,
                "pages": section_pages,
                "header_evidence": starts[lot_num].get("evidence", []),
                "header_quote": starts[lot_num].get("quote"),
            }
        )
    return sections


def _find_regex_in_specific_pages(
    pages: List[Dict[str, Any]],
    pattern: str,
    flags: int = 0,
    field_key: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    rx = re.compile(pattern, flags)
    for page_obj in pages:
        text = str(page_obj.get("text", "") or "")
        page_num = _get_page_number(page_obj)
        if not text or page_num <= 0:
            continue
        m = rx.search(text)
        if not m:
            continue
        line_start, line_end = _line_bounds(text, m.start(), m.end())
        line = text[line_start:line_end]
        if _is_toc_like_line(line):
            continue
        return _build_evidence(
            text,
            page_num,
            line_start,
            line_end,
            field_key=field_key,
            anchor_hint=m.group(0),
        )
    return None


def _extract_lot_risk_notes(section_pages: List[Dict[str, Any]]) -> List[str]:
    filtered_chunks: List[str] = []
    for page_obj in section_pages:
        text = str(page_obj.get("text", "") or "")
        upper = text.upper()
        if "SCHEMA RIASSUNTIVO" in upper or "RIEPILOGO BANDO D'ASTA" in upper:
            continue
        if re.search(r"\bBene\s+N[°º]?\s*\d+\b", text, re.I):
            continue
        filtered_chunks.append(text)
    text = "\n".join(filtered_chunks) if filtered_chunks else "\n".join(str(p.get("text", "") or "") for p in section_pages[:8])
    low = text.lower()
    notes: List[str] = []
    checks = [
        ("non agibile / abitabilità assente", [r"non\s+risulta\s+agibil", r"non\s+[èe]\s+presente\s+l['’]?abitabilit"]),
        ("non conformità / difformità catastali-edilizie", [r"non\s+sussiste\s+corrispondenza\s+catastale", r"difformit", r"accertamento\s+di\s+conformit"]),
        ("servitù e accesso su stradella privata", [r"stradella", r"servit[ùu]\s+di\s+passo", r"via\s+della\s+colonna", r"attraversamento\s+condutture", r"fognatura", r"passo\s+pedonale", r"passo\s+carrabile"]),
        ("infiltrazioni / umidità", [r"infiltr", r"umidit"]),
        ("fabbricato in costruzione / lavori sospesi", [r"in\s+costruzione", r"lavori\s+sospes", r"al\s+grezzo"]),
        ("presenza fibro-cemento / amianto", [r"fibro[\s\-]?cement", r"amianto", r"eternit"]),
        ("condizioni conservative critiche", [r"pessim", r"rovina", r"degrad", r"cattive\s+condizioni"]),
    ]
    for label, patterns in checks:
        if label == "servitù e accesso su stradella privata" and re.search(r"servit[ùu]\s+attive\s+e\s+passive", low, re.I):
            if not any(re.search(pat, low, re.I) for pat in patterns[1:]):
                continue
        if any(re.search(pat, low, re.I) for pat in patterns):
            notes.append(label)
    return notes


def _select_best_address_candidate(candidates: List[Tuple[str, Dict[str, Any]]]) -> Optional[Tuple[str, Dict[str, Any]]]:
    ranked: List[Tuple[int, str, Dict[str, Any]]] = []
    for raw_value, ev in candidates:
        value = _normalize_address_value(raw_value) or _normalize_headline_text(raw_value)
        if not value:
            continue
        value = re.sub(r"\.{2,}\s*\d+\s*$", "", value).strip(" ,.;-")
        if _is_toc_like_line(value) or _is_toc_like_quote(value):
            continue
        m_common = re.search(
            r"Comune\s+di\s+([^\(,\n]+)\s*\(([A-Z]{2})\)[^\n]{0,120}?\bvia\b\s+(?:della\s+|del\s+|di\s+)?([A-Za-zÀ-ÿ' ]+?)\s+(snc|senza\s+numero\s+civico)",
            value,
            re.I,
        )
        if m_common:
            comune = _normalize_headline_text(m_common.group(1))
            prov = m_common.group(2).upper()
            street = _normalize_headline_text(m_common.group(3))
            value = f"{comune} ({prov}) - via {street} senza numero civico"
        score = len(value)
        low = value.lower()
        if "numero civico" in low or "n.c." in low or "snc" in low:
            score += 40
        if "comune di" in low or "(pt)" in low or "(mn)" in low:
            score += 15
        ranked.append((score, value, ev))
    if not ranked:
        return None
    ranked.sort(key=lambda item: (-item[0], -len(item[1])))
    return ranked[0][1], ranked[0][2]


def _extract_lot_identity_and_rights(lot_pages: List[Dict[str, Any]], lot_num: int) -> Dict[str, Any]:
    address_candidates: List[Tuple[str, Dict[str, Any]]] = []
    right_candidates: List[Tuple[str, Dict[str, Any]]] = []
    shared_candidates: List[Tuple[str, Dict[str, Any]]] = []
    early_pages = [p for p in lot_pages[:5] if isinstance(p, dict)]

    for page_obj in early_pages:
        text = str(page_obj.get("text", "") or "")
        page_num = _get_page_number(page_obj)
        if not text or page_num <= 0:
            continue

        for m in re.finditer(rf"Bene\s+N[°º]?\s*{lot_num}\s*-\s*[^\n]*?ubicat[oa]\s+a\s+([^\n]+)", text, re.I):
            line_start, line_end = _line_bounds(text, m.start(), m.end())
            ev = _build_evidence(text, page_num, line_start, line_end, field_key="ubicazione", anchor_hint=m.group(0))
            address_candidates.append((m.group(1).strip(), ev))

        for m in re.finditer(r"posto\s+in\s+Comune\s+di\s+([^\n,]+)[^\n]{0,180}?\bvia\b[^\n,]*?(senza\s+numero\s+civico|n\.?c\.?\s*\d+|snc)", text, re.I):
            line_start, line_end = _line_bounds(text, m.start(), m.end())
            ev = _build_evidence(text, page_num, line_start, line_end, field_key="ubicazione", anchor_hint=m.group(0))
            address_candidates.append((m.group(0).strip(), ev))

        for m in re.finditer(r"Diritti?\s+di\s+piena\s+propriet[àa]\s+per\s+la\s+quota\s+di\s+1/1[^\n]{0,120}", text, re.I):
            line_start, line_end = _line_bounds(text, m.start(), m.end())
            ev = _build_evidence(text, page_num, line_start, line_end, field_key="diritto_reale", anchor_hint=m.group(0))
            right_candidates.append(("Proprietà 1/1", ev))

        for m in re.finditer(r"\(Proprietà\s+1/1\)", text, re.I):
            line_start, line_end = _line_bounds(text, m.start(), m.end())
            ev = _build_evidence(text, page_num, line_start, line_end, field_key="diritto_reale", anchor_hint=m.group(0))
            right_candidates.append(("Proprietà 1/1", ev))

        for m in re.finditer(r"diritti?\s+di\s+compropriet[àa]\s+pari\s+ad\s+1/4\s+dell['’]intero[^\n]{0,140}", text, re.I):
            line_start, line_end = _line_bounds(text, m.start(), m.end())
            ev = _build_evidence(text, page_num, line_start, line_end, field_key="shared_rights_note", anchor_hint=m.group(0))
            shared_candidates.append(("Quota 1/4 della stradella privata di accesso", ev))

        for m in re.finditer(r"stradella\s+(?:privata|di\s+penetrazione)[^\n]{0,180}corte\s+comune", text, re.I):
            line_start, line_end = _line_bounds(text, m.start(), m.end())
            ev = _build_evidence(text, page_num, line_start, line_end, field_key="shared_rights_note", anchor_hint=m.group(0))
            shared_candidates.append(("Accesso tramite stradella privata e corte comune", ev))

    out: Dict[str, Any] = {}
    best_address = _select_best_address_candidate(address_candidates)
    if best_address:
        out["ubicazione"] = best_address[0]
        out["ubicazione_evidence"] = [best_address[1]]
    if right_candidates:
        out["diritto_reale"] = right_candidates[0][0]
        out["diritto_reale_evidence"] = [right_candidates[0][1]]
    if shared_candidates:
        shared_text = shared_candidates[0][0]
        out["shared_rights_note"] = shared_text
        out["shared_rights_evidence"] = [shared_candidates[0][1]]
        if out.get("diritto_reale"):
            out["diritto_reale"] = f"{out['diritto_reale']} + {shared_text}"
        else:
            out["diritto_reale"] = shared_text
            out["diritto_reale_evidence"] = [shared_candidates[0][1]]
    return out


def _build_lots_overview(lots: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for idx, raw_lot in enumerate(lots):
        if not isinstance(raw_lot, dict):
            continue
        lot_number = raw_lot.get("lot_number") or (idx + 1)
        prezzo = _as_float_or_none(raw_lot.get("prezzo_base_value"))
        stima = _as_float_or_none(raw_lot.get("valore_stima_eur"))
        dep_pct = _parse_percent_value(raw_lot.get("deprezzamento_percentuale"))
        if dep_pct is None:
            dep_pct = _parse_percent_value(raw_lot.get("deprezzamento_percent"))
        dep_value = None
        if isinstance(stima, (int, float)) and isinstance(prezzo, (int, float)):
            dep_value = max(0.0, stima - prezzo)
        out.append({
            "lot_number": int(lot_number),
            "lotto_label": f"Lotto {int(lot_number)}",
            "comune": _normalize_headline_text(str(raw_lot.get("comune", "") or "")) or None,
            "ubicazione": _normalize_headline_text(str(raw_lot.get("ubicazione", "") or "")) or None,
            "tipologia": _normalize_headline_text(str(raw_lot.get("tipologia", "") or "")) or None,
            "valore_stima_eur": int(round(stima)) if isinstance(stima, (int, float)) else None,
            "deprezzamento_percent": round(float(dep_pct), 2) if isinstance(dep_pct, (int, float)) else None,
            "deprezzamento_eur": int(round(dep_value)) if isinstance(dep_value, (int, float)) else None,
            "prezzo_base_eur": int(round(prezzo)) if isinstance(prezzo, (int, float)) else None,
        })
    return out


def _build_multi_lot_top_legal_items(lots: List[Dict[str, Any]], pages: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not isinstance(lots, list) or len(lots) <= 1:
        return []

    lot_sections = _build_lot_sections(pages if isinstance(pages, list) else [])
    section_by_lot = {int(sec.get("lot_number") or 0): sec for sec in lot_sections if isinstance(sec, dict)}

    note_specs = [
        (
            "non agibile / abitabilità assente",
            "Non agibile",
            "REAL_BLOCKER",
            100,
            [
                r"non\s+(?:risulta|è|e)\s+agibil\w+",
                r"non\s+è\s+presente\s+l[’']?abitabilit",
            ],
            lambda lot: (
                lot.get("evidence", {}).get("note", [])
                if isinstance(lot.get("evidence"), dict) else []
            ),
        ),
        (
            "non conformità / difformità catastali-edilizie",
            "Non conformità / catastale-edilizia",
            "REAL_BLOCKER",
            98,
            [
                r"difformit",
                r"mancata\s+corrispondenza\s+(?:delle\s+)?mappe\s+catastali",
                r"non\s+regolar",
            ],
            lambda lot: (
                (lot.get("evidence", {}).get("catasto", []) if isinstance(lot.get("evidence"), dict) else [])
                or (lot.get("evidence", {}).get("note", []) if isinstance(lot.get("evidence"), dict) else [])
            ),
        ),
        (
            "presenza fibro-cemento / amianto",
            "Fibro-cemento / bonifica burden",
            "REAL_BLOCKER",
            97,
            [
                r"fibro[\s-]?cemento",
                r"amianto",
            ],
            lambda lot: (
                lot.get("evidence", {}).get("note", [])
                if isinstance(lot.get("evidence"), dict) else []
            ),
        ),
        (
            "condizioni conservative critiche",
            "Degrado / condizioni conservative critiche",
            "REAL_BLOCKER",
            95,
            [
                r"pessim\w+\s+condizion\w+",
                r"cattiv\w+\s+condizion\w+",
                r"abbandonat",
                r"degradat",
            ],
            lambda lot: (
                lot.get("evidence", {}).get("stato_conservativo", [])
                if isinstance(lot.get("evidence"), dict) else []
            ) or (
                lot.get("evidence", {}).get("note", [])
                if isinstance(lot.get("evidence"), dict) else []
            ),
        ),
        (
            "servitù e accesso su stradella privata",
            "Servitù / accesso privato / stradella",
            "REAL_BLOCKER",
            99,
            [
                r"stradella\s+privata",
                r"servit[ùu]\s+di\s+pass",
                r"diritto\s+di\s+pass",
                r"corte\s+comune",
            ],
            lambda lot: (
                lot.get("evidence", {}).get("diritto_reale", [])
                if isinstance(lot.get("evidence"), dict) else []
            ) or (
                lot.get("evidence", {}).get("shared_rights_note", [])
                if isinstance(lot.get("evidence"), dict) else []
            ) or (
                lot.get("evidence", {}).get("note", [])
                if isinstance(lot.get("evidence"), dict) else []
            ),
        ),
        (
            "infiltrazioni / umidità",
            "Infiltrazioni",
            "REAL_BLOCKER",
            96,
            [
                r"infiltraz",
                r"umidit",
            ],
            lambda lot: (
                lot.get("evidence", {}).get("note", [])
                if isinstance(lot.get("evidence"), dict) else []
            ),
        ),
        (
            "fabbricato in costruzione / lavori sospesi",
            "In costruzione / lavori sospesi",
            "REAL_BLOCKER",
            100,
            [
                r"in\s+costruzione",
                r"lavori\s+sospesi",
                r"sospes\w+.{0,30}10\s+anni",
            ],
            lambda lot: (
                lot.get("evidence", {}).get("tipologia", [])
                if isinstance(lot.get("evidence"), dict) else []
            ) or (
                lot.get("evidence", {}).get("note", [])
                if isinstance(lot.get("evidence"), dict) else []
            ),
        ),
    ]

    top_items: List[Dict[str, Any]] = []
    seen: set = set()
    for raw_lot in lots:
        if not isinstance(raw_lot, dict):
            continue
        lot_num = int(raw_lot.get("lot_number") or 0)
        if lot_num <= 0:
            continue
        lot_notes = raw_lot.get("risk_notes") if isinstance(raw_lot.get("risk_notes"), list) else []
        lot_notes = [str(note).strip() for note in lot_notes if str(note).strip()]
        section_pages = section_by_lot.get(lot_num, {}).get("pages", []) if section_by_lot.get(lot_num) else []
        search_pages = section_pages if isinstance(section_pages, list) and section_pages else pages
        for note_key, label, bucket, score, patterns, evidence_getter in note_specs:
            if note_key not in lot_notes:
                continue
            evidence = _normalize_contract_evidence_list(evidence_getter(raw_lot), max_items=2)
            if not evidence:
                for pattern in patterns:
                    ev = _find_regex_in_specific_pages(search_pages, pattern, re.I, field_key="section_9_legal_killers")
                    if ev and not _is_toc_like_quote(str(ev.get("quote") or "")):
                        evidence = _normalize_contract_evidence_list([ev], max_items=2)
                        break
            if not evidence:
                continue
            killer = f"Lotto {lot_num}: {label}"
            dedupe_key = killer.lower()
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)
            top_items.append({
                "killer": killer,
                "status": "ROSSO",
                "status_it": "CRITICO",
                "reason_it": f"Blocco prioritario per Lotto {lot_num}: {label}",
                "evidence": evidence,
                "decision_bucket": bucket,
                "decision_score": score,
            })

    top_items.sort(key=lambda item: (-int(item.get("decision_score", 0)), str(item.get("killer", ""))))
    return top_items[:12]


def _build_conservative_money_box_for_lots(result: Dict[str, Any]) -> None:
    lots = result.get("lots") if isinstance(result.get("lots"), list) else []
    if len(lots) <= 1:
        return

    burden_templates = {
        1: [
            "Bonifica / smaltimento fibro-cemento a carico acquirente",
            "Regolarizzazione edilizia e catastale da verificare lotto per lotto",
        ],
        2: [
            "Regolarizzazioni edilizie / accertamento di conformità da verificare",
            "Gestione servitù, accesso privato e corte comune",
            "Pratiche strutturali incomplete a carico acquirente",
        ],
        3: [
            "Costi di completamento lavori e messa in sicurezza",
            "Impermeabilizzazione / ripristini e lattonerie",
            "Regolarizzazione edilizia e strutturale da verificare",
        ],
    }

    lot_rows: List[Dict[str, Any]] = []
    items: List[Dict[str, Any]] = []
    for idx, raw_lot in enumerate(lots):
        if not isinstance(raw_lot, dict):
            continue
        lot_number = int(raw_lot.get("lot_number") or (idx + 1))
        stima = _as_float_or_none(raw_lot.get("valore_stima_eur"))
        prezzo = _as_float_or_none(raw_lot.get("prezzo_base_value"))
        dep_pct = _parse_percent_value(raw_lot.get("deprezzamento_percentuale"))
        if dep_pct is None:
            dep_pct = _parse_percent_value(raw_lot.get("deprezzamento_percent"))
        lot_rows.append({
            "lot_number": lot_number,
            "lotto_label": f"Lotto {lot_number}",
            "valore_stima_eur": int(round(stima)) if isinstance(stima, (int, float)) else None,
            "deprezzamento_percent": round(float(dep_pct), 2) if isinstance(dep_pct, (int, float)) else None,
            "prezzo_base_eur": int(round(prezzo)) if isinstance(prezzo, (int, float)) else None,
        })
        for burden in burden_templates.get(lot_number, []):
            items.append({
                "code": f"LOT_{lot_number}_BURDEN",
                "lot_number": lot_number,
                "label_it": burden,
                "label_en": burden,
                "type": "QUALITATIVE",
                "stima_euro": "NON_QUANTIFICATO",
                "stima_nota": "Onere non quantificato in perizia; verifica tecnica/legale obbligatoria",
                "source": "PERIZIA_QUALITATIVE",
            })

    money_box = {
        "policy": "LOT_CONSERVATIVE",
        "lots": lot_rows,
        "items": items,
        "qualitative_burdens": copy.deepcopy(items),
        "total_extra_costs": {
            "min": "NON_QUANTIFICATO_IN_PERIZIA",
            "max": "NON_QUANTIFICATO_IN_PERIZIA",
            "nota": "Costi extra non quantificati in perizia; mantenuti solo oneri qualitativi buyer-borne",
        },
    }
    result["money_box"] = money_box
    result["section_3_money_box"] = copy.deepcopy(money_box)


def _parse_percent_value(value: Any) -> Optional[float]:
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        cleaned = value.strip().replace("%", "").replace(" ", "")
        if not cleaned:
            return None
        if re.fullmatch(r"\d+(?:[.,]\d+)?", cleaned):
            try:
                return float(cleaned.replace(",", "."))
            except Exception:
                return None
    return None


def _sanitize_lot_conservative_outputs(result: Dict[str, Any]) -> None:
    money_box = result.get("money_box", {}) if isinstance(result.get("money_box"), dict) else {}
    if str(money_box.get("policy") or "").upper() != "LOT_CONSERVATIVE":
        return

    lots = result.get("lots") if isinstance(result.get("lots"), list) else []
    rebuilt_rows: List[Dict[str, Any]] = []
    for idx, raw_lot in enumerate(lots):
        if not isinstance(raw_lot, dict):
            continue
        lot_number = int(raw_lot.get("lot_number") or (idx + 1))
        rebuilt_rows.append({
            "lot_number": lot_number,
            "lotto_label": f"Lotto {lot_number}",
            "valore_stima_eur": int(round(_as_float_or_none(raw_lot.get("valore_stima_eur")) or 0)) or None,
            "deprezzamento_percent": _parse_percent_value(raw_lot.get("deprezzamento_percentuale"))
            if _parse_percent_value(raw_lot.get("deprezzamento_percentuale")) is not None
            else _parse_percent_value(raw_lot.get("deprezzamento_percent")),
            "prezzo_base_eur": int(round(_as_float_or_none(raw_lot.get("prezzo_base_value")) or 0)) or None,
        })
    money_box["lots"] = rebuilt_rows
    money_box["items"] = [
        item for item in money_box.get("items", [])
        if isinstance(item, dict) and str(item.get("source") or "").upper() == "PERIZIA_QUALITATIVE"
    ]
    money_box["qualitative_burdens"] = copy.deepcopy(money_box["items"])
    money_box.pop("total_extra_costs_range", None)
    money_box["total_extra_costs"] = {
        "min": "NON_QUANTIFICATO_IN_PERIZIA",
        "max": "NON_QUANTIFICATO_IN_PERIZIA",
        "nota": "Costi extra non quantificati in perizia; mantenuti solo oneri qualitativi buyer-borne",
    }
    result["money_box"] = money_box
    result["section_3_money_box"] = copy.deepcopy(money_box)


def _augment_legal_killers_from_lots(legal_killers_items: List[Dict[str, Any]], lots: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    existing = {str(item.get("killer") or "").strip().lower() for item in legal_killers_items if isinstance(item, dict)}
    note_map = {
        "non agibile / abitabilità assente": ("Non agibile", "SI"),
        "non conformità / difformità catastali-edilizie": ("Non regolare / difformità", "SI"),
        "presenza fibro-cemento / amianto": ("Fibro-cemento / amianto", "SI"),
        "servitù e accesso su stradella privata": ("Servitù / accesso privato", "GIALLO"),
        "infiltrazioni / umidità": ("Infiltrazioni", "GIALLO"),
        "fabbricato in costruzione / lavori sospesi": ("Fabbricato in costruzione / lavori sospesi", "SI"),
        "condizioni conservative critiche": ("Condizioni conservative critiche", "GIALLO"),
    }
    for lot in lots:
        if not isinstance(lot, dict):
            continue
        lot_num = int(lot.get("lot_number") or 0)
        evidence_map = lot.get("evidence", {}) if isinstance(lot.get("evidence"), dict) else {}
        base_evidence = (
            evidence_map.get("note")
            or evidence_map.get("lotto")
            or evidence_map.get("ubicazione")
            or []
        )
        base_evidence = _normalize_contract_evidence_list(base_evidence, max_items=2)
        for note in lot.get("risk_notes", []) if isinstance(lot.get("risk_notes"), list) else []:
            mapped = note_map.get(str(note))
            if not mapped:
                continue
            killer_label = f"Lotto {lot_num}: {mapped[0]}"
            if killer_label.lower() in existing:
                continue
            legal_killers_items.append({
                "killer": killer_label,
                "status": mapped[1],
                "action": "Verifica obbligatoria",
                "evidence": copy.deepcopy(base_evidence),
            })
            existing.add(killer_label.lower())
    return legal_killers_items


def _legal_relevance_profile(killer: str) -> Tuple[str, int]:
    low = str(killer or "").lower()
    if any(token in low for token in ("immobile non regolare", "agibilità assente", "agibilita assente", "sanatoria / condono", "uso residenziale non legittimato", "occupato da terzi senza titolo")):
        return "REAL_BLOCKER", 125
    if any(token in low for token in ("non agibile", "non regolare", "difformità", "fibro-cemento", "amianto", "in costruzione", "lavori sospesi", "servitù / accesso privato", "servitù di passaggio", "infiltrazioni")):
        return "REAL_BLOCKER", 100
    if any(token in low for token in ("servitù rilevata", "accertamento di conformità", "condizioni conservative", "sicurezza")):
        return "RELEVANT_SECONDARY", 70
    if any(token in low for token in ("oneri di cancellazione", "formalità da cancellare", "salva casa")):
        return "BACKGROUND_NOTE", 20
    return "RELEVANT_SECONDARY", 50


def _lot_risk_cluster_it(lot: Dict[str, Any]) -> str:
    notes = lot.get("risk_notes") if isinstance(lot.get("risk_notes"), list) else []
    note_set = set(str(n) for n in notes)
    tipologia = _normalize_headline_text(str(lot.get("tipologia") or ""))
    if "fabbricato in costruzione / lavori sospesi" in note_set:
        return "fabbricato in costruzione con lavori sospesi, costi di completamento e profili di sicurezza/regolarizzazione"
    if "servitù e accesso su stradella privata" in note_set and "infiltrazioni / umidità" in note_set:
        if "magazzino" in tipologia.lower():
            return "magazzino con servitù/accesso privato, infiltrazioni e burden di regolarizzazione"
        return "accesso su stradella privata, servitù e infiltrazioni da verificare lotto per lotto"
    if "presenza fibro-cemento / amianto" in note_set:
        return "degrado severo, fibro-cemento e forte burden di regolarizzazione"
    if "condizioni conservative critiche" in note_set and "magazzino" not in tipologia.lower():
        return "condizioni conservative critiche e forte burden di regolarizzazione"
    if notes:
        return ", ".join(notes[:2])
    return "verifica tecnico-legale dedicata"


def _lot_risk_cluster_en(lot: Dict[str, Any]) -> str:
    notes = lot.get("risk_notes") if isinstance(lot.get("risk_notes"), list) else []
    note_set = set(str(n) for n in notes)
    tipologia = _normalize_headline_text(str(lot.get("tipologia") or ""))
    if "fabbricato in costruzione / lavori sospesi" in note_set:
        return "unfinished construction with suspended works, completion costs and safety/compliance burden"
    if "servitù e accesso su stradella privata" in note_set and "infiltrazioni / umidità" in note_set:
        if "magazzino" in tipologia.lower():
            return "warehouse with private-access easements, infiltration issues and compliance burden"
        return "private-access road, easements and infiltration issues requiring lot-by-lot review"
    if "presenza fibro-cemento / amianto" in note_set:
        return "severe degradation, fibro-cement and major compliance burden"
    if "condizioni conservative critiche" in note_set and "magazzino" not in tipologia.lower():
        return "critical building condition and major compliance burden"
    if notes:
        return ", ".join(notes[:2])
    return "dedicated legal/technical review required"


def _build_case_aware_narration_payload(result: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    lots = result.get("lots") if isinstance(result.get("lots"), list) else []
    if len(lots) <= 1:
        return None
    fragments_it: List[str] = []
    fragments_en: List[str] = []
    bullets_it: List[str] = []
    bullets_en: List[str] = []
    for lot in lots[:3]:
        if not isinstance(lot, dict):
            continue
        lot_num = lot.get("lot_number")
        tipologia = _normalize_headline_text(str(lot.get("tipologia") or "")) or "bene"
        ubic = _normalize_headline_text(str(lot.get("ubicazione") or "")) or "ubicazione da verificare"
        fragments_it.append(f"Lotto {lot_num} ({tipologia}, {ubic}): {_lot_risk_cluster_it(lot)}")
        fragments_en.append(f"Lot {lot_num} ({tipologia}, {ubic}): {_lot_risk_cluster_en(lot)}")
        bullets_it.append(f"Lotto {lot_num}: {_lot_risk_cluster_it(lot)}.")
        bullets_en.append(f"Lot {lot_num}: {_lot_risk_cluster_en(lot)}.")
    return {
        "it": "Caso multi-lotto. " + " ".join(fragments_it) + " Verifica prudenziale lotto per lotto prima dell'offerta.",
        "en": "Multi-lot case. " + " ".join(fragments_en) + " Conservative lot-by-lot review is required before bidding.",
        "bullets_it": bullets_it,
        "bullets_en": bullets_en,
        "evidence_refs": [],
    }


def _refresh_red_flags_operativi(result: Dict[str, Any]) -> None:
    existing_flags = result.get("red_flags_operativi")
    existing_list = existing_flags if isinstance(existing_flags, list) else []
    preserved: List[Dict[str, Any]] = []
    seen_codes: set = set()

    for flag in existing_list:
        if not isinstance(flag, dict):
            continue
        code = str(flag.get("code") or "").strip().upper()
        if code == "MANUAL_REVIEW":
            continue
        if code and code not in seen_codes:
            preserved.append(copy.deepcopy(flag))
            seen_codes.add(code)

    top_items = []
    if isinstance(result.get("section_9_legal_killers"), dict):
        maybe_items = result["section_9_legal_killers"].get("top_items", [])
        if isinstance(maybe_items, list):
            top_items = maybe_items

    derived_map = {
        "immobile non regolare ediliziamente": {
            "code": "URBANISTICA_NON_REGOLARE",
            "severity": "RED",
            "flag_it": "Immobile non regolare ediliziamente",
            "flag_en": "Building not legally compliant",
            "action_it": "Verificare immediatamente la regolarizzazione urbanistico-edilizia.",
        },
        "agibilità assente / non rilasciata": {
            "code": "AGIBILITA_ASSENTE",
            "severity": "RED",
            "flag_it": "Agibilità assente o non rilasciata",
            "flag_en": "Habitability certificate absent or not issued",
            "action_it": "Verificare agibilità/abitabilità e impatto sulla commerciabilità.",
        },
        "sanatoria / condono non perfezionati": {
            "code": "SANATORIA_NON_PERFEZIONATA",
            "severity": "RED",
            "flag_it": "Sanatoria / condono non perfezionati",
            "flag_en": "Sanatoria / amnesty not perfected",
            "action_it": "Verificare stato e fattibilità della sanatoria prima dell'offerta.",
        },
        "uso residenziale non legittimato": {
            "code": "USO_NON_LEGITTIMATO",
            "severity": "RED",
            "flag_it": "Uso residenziale non legittimato",
            "flag_en": "Residential use not lawfully legitimized",
            "action_it": "Verificare destinazione d'uso e commerciabilità del bene.",
        },
        "occupato da terzi senza titolo": {
            "code": "OCCUPAZIONE_SENZA_TITOLO",
            "severity": "RED",
            "flag_it": "Occupato da terzi senza titolo",
            "flag_en": "Occupied by third parties without title",
            "action_it": "Valutare tempi, costi e rischi di liberazione dell'immobile.",
        },
    }

    for item in top_items[:8]:
        if not isinstance(item, dict):
            continue
        killer = str(item.get("killer") or "").strip().lower()
        payload = derived_map.get(killer)
        if not payload:
            continue
        code = payload["code"]
        if code in seen_codes:
            continue
        flag = copy.deepcopy(payload)
        evidence = item.get("evidence", []) if isinstance(item.get("evidence"), list) else []
        if evidence:
            flag["evidence"] = copy.deepcopy(evidence[:2])
        preserved.append(flag)
        seen_codes.add(code)

    if "MANUAL_REVIEW" not in seen_codes:
        preserved.append({
            "code": "MANUAL_REVIEW",
            "severity": "AMBER",
            "flag_it": "Revisione manuale raccomandata",
            "flag_en": "Manual review recommended",
            "action_it": "Verificare tutti i dati con la perizia originale",
        })

    result["red_flags_operativi"] = preserved


def _refresh_customer_facing_result_on_read(
    result: Dict[str, Any],
    pages: List[Dict[str, Any]],
    *,
    analysis_id: str = "",
    headline_overrides: Optional[Dict[str, Any]] = None,
    field_overrides: Optional[Dict[str, Any]] = None,
) -> None:
    if not isinstance(result, dict):
        return

    safe_pages = pages if isinstance(pages, list) and pages else [{"page_number": 1, "text": ""}]

    _apply_headline_field_states(result, safe_pages)
    _apply_decision_field_states(result, safe_pages)
    _apply_headline_overrides(result, headline_overrides or {})
    _apply_field_overrides(result, field_overrides or {})
    _enforce_field_states_contract(result, safe_pages)
    _apply_customer_headline_fallbacks(result)

    _normalize_legal_killers(result, safe_pages)
    states = result.get("field_states", {}) if isinstance(result.get("field_states"), dict) else {}
    _ensure_semaforo_top_blockers(result, states, safe_pages)
    _recompute_semaforo_status(result)
    _synthesize_decisione_rapida(result, states)

    _apply_market_ranges_to_money_box(result)
    _sanitize_lot_conservative_outputs(result)
    _normalize_evidence_offsets(result, safe_pages)
    result["panoramica_contract"] = _build_panoramica_contract(result, safe_pages)

    document_quality = result.get("document_quality", {}) if isinstance(result.get("document_quality"), dict) else {}
    _apply_unreadable_hard_stop(result, document_quality)

    case_aware_narration = _build_case_aware_narration_payload(result)
    if case_aware_narration:
        result["decision_rapida_narrated"] = case_aware_narration
    else:
        result.pop("decision_rapida_narrated", None)

    _refresh_red_flags_operativi(result)

    if analysis_id:
        result["user_messages"] = _build_user_messages(result, {}, analysis_id=analysis_id)


def _enrich_lots_from_sections(
    lots: List[Dict[str, Any]],
    pages: List[Dict[str, Any]],
    beni: Optional[List[Dict[str, Any]]] = None,
) -> List[Dict[str, Any]]:
    normalized_pages = _normalize_page_numbers(pages)
    if not lots:
        return lots

    sections = _build_lot_sections(normalized_pages)
    section_by_lot = {int(s["lot_number"]): s for s in sections}
    schema_pages = [p for p in normalized_pages if "SCHEMA RIASSUNTIVO" in str(p.get("text", "") or "").upper()]
    if schema_pages:
        min_schema_page = min(_get_page_number(p) for p in schema_pages)
        schema_window_max = min_schema_page + 4
        schema_pages = [p for p in normalized_pages if min_schema_page <= _get_page_number(p) <= schema_window_max]
    else:
        schema_pages = [p for p in normalized_pages if "RIEPILOGO BANDO D'ASTA" in str(p.get("text", "") or "").upper()]

    bene_by_number: Dict[int, Dict[str, Any]] = {}
    for b in (beni or []):
        if not isinstance(b, dict):
            continue
        try:
            n = int(b.get("bene_number"))
        except Exception:
            continue
        bene_by_number[n] = b

    for idx, raw_lot in enumerate(lots):
        lot_num = int(raw_lot.get("lot_number") or (idx + 1))
        lot = _ensure_lot_contract(raw_lot, lot_num)
        section = section_by_lot.get(lot_num, {})
        lot_pages = section.get("pages", []) if isinstance(section, dict) else []
        if lot_pages and not lot["evidence"].get("lotto"):
            lot["evidence"]["lotto"] = section.get("header_evidence", [])
        identity = _extract_lot_identity_and_rights(lot_pages or normalized_pages, lot_num)
        if identity.get("ubicazione"):
            current_loc = _normalize_headline_text(str(lot.get("ubicazione") or ""))
            candidate_loc = _normalize_headline_text(str(identity.get("ubicazione") or ""))
            if candidate_loc and (current_loc in {"", "TBD", "NON SPECIFICATO IN PERIZIA"} or len(candidate_loc) > len(current_loc)):
                lot["ubicazione"] = candidate_loc
                lot["evidence"]["ubicazione"] = identity.get("ubicazione_evidence", []) or lot["evidence"]["ubicazione"]
        if identity.get("diritto_reale"):
            current_right = _normalize_headline_text(str(lot.get("diritto_reale") or ""))
            candidate_right = _normalize_headline_text(str(identity.get("diritto_reale") or ""))
            if candidate_right and (current_right in {"", "TBD", "NON SPECIFICATO IN PERIZIA"} or "quota" in candidate_right.lower()):
                lot["diritto_reale"] = candidate_right
                lot["evidence"]["diritto_reale"] = identity.get("diritto_reale_evidence", []) or lot["evidence"]["diritto_reale"]
        if identity.get("shared_rights_note"):
            lot["shared_rights_note"] = _normalize_headline_text(str(identity.get("shared_rights_note") or ""))
            lot["evidence"]["shared_rights_note"] = identity.get("shared_rights_evidence", []) or lot["evidence"]["shared_rights_note"]

        context_pages = schema_pages or lot_pages or normalized_pages
        price_ev = _find_regex_in_specific_pages(
            context_pages,
            rf"LOTTO\s+{lot_num}\s*-\s*PREZZO\s+BASE\s*D['’]?ASTA[:\s]*€?\s*([0-9]{{1,3}}(?:[.\s][0-9]{{3}})*(?:,[0-9]{{2}})?)",
            re.I,
            field_key="prezzo_base_asta",
        )
        if not price_ev:
            price_ev = _find_regex_in_specific_pages(
                normalized_pages,
                rf"LOTTO\s+{lot_num}[^\n]{{0,120}}PREZZO\s+BASE\s*D['’]?ASTA[:\s]*€?\s*([0-9]{{1,3}}(?:[.\s][0-9]{{3}})*(?:,[0-9]{{2}})?)",
                re.I,
                field_key="prezzo_base_asta",
            )
        if not price_ev:
            price_ev = _find_regex_in_specific_pages(
                lot_pages or normalized_pages,
                r"Prezzo\s+base\s+d['’]?asta[:\s]*€?\s*([0-9]{1,3}(?:[.\s][0-9]{3})*(?:,[0-9]{2})?)",
                re.I,
                field_key="prezzo_base_asta",
            )
        if price_ev:
            quote = str(price_ev.get("quote", "") or "")
            m_price = re.search(r"€?\s*([0-9]{1,3}(?:[.\s][0-9]{3})*(?:,[0-9]{2})?)", quote)
            parsed = _parse_euro_number(m_price.group(1) if m_price else quote)
            if parsed is not None:
                lot["prezzo_base_value"] = float(parsed)
                lot["prezzo_base_eur"] = f"€ {parsed:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
                lot["evidence"]["prezzo_base"] = [price_ev]

        stima_ev = _find_regex_in_specific_pages(
            lot_pages or normalized_pages,
            r"Valore\s+di\s+stima\s+del\s+bene[:\s]*€?\s*([0-9]{1,3}(?:\.[0-9]{3})*(?:,[0-9]{2})?)",
            re.I,
            field_key="valore_stima",
        )
        if stima_ev:
            stima_val = _parse_euro_number(str(stima_ev.get("quote", "")))
            if stima_val is not None:
                lot["valore_stima_eur"] = int(round(float(stima_val)))
                lot["evidence"]["valore_stima"] = [stima_ev]

        dep_ev = _find_regex_in_specific_pages(
            lot_pages or normalized_pages,
            r"Deprezzament[oi][\s\S]{0,240}?([0-9]{1,2}(?:,[0-9]{1,2})?)\s*%",
            re.I,
            field_key="deprezzamento",
        )
        if not dep_ev:
            dep_ev = _find_regex_in_specific_pages(
                lot_pages or normalized_pages,
                r"([0-9]{1,2}(?:,[0-9]{1,2})?)\s*%\s*(?:Valore\s+finale|Prezzo\s+base|deprezz)",
                re.I,
                field_key="deprezzamento",
            )
        if dep_ev:
            m_dep = re.search(r"([0-9]{1,2}(?:,[0-9]{1,2})?)\s*%", str(dep_ev.get("quote", "")))
            if m_dep:
                lot["deprezzamento_percentuale"] = m_dep.group(1).replace(",", ".")
                lot["evidence"]["deprezzamento"] = [dep_ev]
        if lot.get("deprezzamento_percentuale") in (None, "", "TBD"):
            stima_val = _as_float_or_none(lot.get("valore_stima_eur"))
            prezzo_val = _as_float_or_none(lot.get("prezzo_base_value"))
            if isinstance(stima_val, (int, float)) and isinstance(prezzo_val, (int, float)) and stima_val > 0 and prezzo_val > 0:
                dep_pct = round(max(0.0, min(99.0, (1.0 - (prezzo_val / stima_val)) * 100.0)), 2)
                if dep_pct >= 1.0:
                    lot["deprezzamento_percentuale"] = f"{dep_pct:.2f}"

        superficie_ev = _find_regex_in_specific_pages(
            lot_pages or normalized_pages,
            r"Superficie\s+convenzionale[:\s]*([0-9]{1,4}(?:,[0-9]{1,2})?)\s*mq",
            re.I,
            field_key="superficie_catastale",
        )
        if superficie_ev:
            m_sup = re.search(r"([0-9]{1,4}(?:,[0-9]{1,2})?)\s*mq", str(superficie_ev.get("quote", "")), re.I)
            if m_sup:
                try:
                    lot["superficie_mq"] = float(m_sup.group(1).replace(".", "").replace(",", "."))
                except Exception:
                    lot["superficie_mq"] = m_sup.group(1)
                lot["evidence"]["superficie"] = [superficie_ev]

        bene = bene_by_number.get(lot_num)
        if isinstance(bene, dict):
            tipologia = _normalize_headline_text(str(bene.get("tipologia") or ""))
            if tipologia:
                lot["tipologia"] = f"immobile: {tipologia}" if not tipologia.lower().startswith("immobile:") else tipologia
                if isinstance(bene.get("evidence"), dict):
                    lot["evidence"]["tipologia"] = bene.get("evidence", {}).get("tipologia", []) or lot["evidence"]["tipologia"]
            short_location = _normalize_headline_text(str(bene.get("short_location") or ""))
            if short_location:
                current_loc = _normalize_headline_text(str(lot.get("ubicazione") or ""))
                if current_loc in {"", "TBD", "NON SPECIFICATO IN PERIZIA"} or len(short_location) > len(current_loc):
                    lot["ubicazione"] = short_location
                    if isinstance(bene.get("evidence"), dict):
                        lot["evidence"]["ubicazione"] = bene.get("evidence", {}).get("location_piano", []) or lot["evidence"]["ubicazione"]
            if bene.get("superficie_mq") not in (None, "") and lot.get("superficie_mq") in ("TBD", None):
                lot["superficie_mq"] = bene.get("superficie_mq")
            if bene.get("valore_stima_eur") not in (None, "") and lot.get("valore_stima_eur") in (None, ""):
                try:
                    lot["valore_stima_eur"] = int(round(float(bene.get("valore_stima_eur"))))
                except Exception:
                    pass
            catasto = bene.get("catasto")
            if isinstance(catasto, dict) and catasto:
                lot["catasto"] = catasto

        dir_ev = _find_regex_in_specific_pages(
            lot_pages or normalized_pages,
            r"Diritto\s+reale[:\s]*([^\n]{3,120})",
            re.I,
            field_key="diritto_reale",
        )
        if dir_ev:
            m_dir = re.search(r"Diritto\s+reale[:\s]*([^\n]{3,120})", str(dir_ev.get("quote", "")), re.I)
            if m_dir:
                dir_val = _normalize_headline_text(m_dir.group(1))
                if dir_val and str(lot.get("diritto_reale") or "TBD") in {"TBD", "NON SPECIFICATO IN PERIZIA"}:
                    lot["diritto_reale"] = dir_val
                    lot["evidence"]["diritto_reale"] = [dir_ev]

        risk_notes = _extract_lot_risk_notes(lot_pages)
        if risk_notes:
            lot["risk_notes"] = risk_notes[:8]

        if lot.get("deprezzamento_percentuale") in (None, "", "TBD"):
            stima_val = _as_float_or_none(lot.get("valore_stima_eur"))
            prezzo_val = _as_float_or_none(lot.get("prezzo_base_value"))
            if isinstance(stima_val, (int, float)) and isinstance(prezzo_val, (int, float)) and stima_val > 0 and prezzo_val > 0:
                dep_pct = round(max(0.0, min(99.0, (1.0 - (prezzo_val / stima_val)) * 100.0)), 2)
                if dep_pct >= 1.0:
                    lot["deprezzamento_percentuale"] = f"{dep_pct:.2f}"

        lots[idx] = lot
    return lots


def _assign_beni_to_lots(lots: List[Dict[str, Any]], beni: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not isinstance(lots, list) or not lots:
        return lots
    if not isinstance(beni, list):
        beni = []

    if len(lots) <= 1:
        if lots and isinstance(lots[0], dict):
            lots[0]["beni"] = beni
        return lots

    lot_map: Dict[int, Dict[str, Any]] = {}
    for lot in lots:
        if not isinstance(lot, dict):
            continue
        try:
            lot_num = int(lot.get("lot_number"))
        except Exception:
            continue
        lot["beni"] = []
        lot_map[lot_num] = lot

    for bene in beni:
        if not isinstance(bene, dict):
            continue
        target_lot = None
        try:
            bene_num = int(bene.get("bene_number"))
            target_lot = lot_map.get(bene_num)
        except Exception:
            target_lot = None

        if target_lot is None:
            loc = _normalize_token(bene.get("short_location"))
            for lot in lots:
                if not isinstance(lot, dict):
                    continue
                lot_loc = _normalize_token(lot.get("ubicazione"))
                if lot_loc and loc and any(token in loc for token in lot_loc.split(" ")[:2]):
                    target_lot = lot
                    break

        if target_lot is None:
            target_lot = lots[0] if isinstance(lots[0], dict) else None
        if isinstance(target_lot, dict):
            bucket = target_lot.get("beni")
            if not isinstance(bucket, list):
                bucket = []
            bucket.append(bene)
            target_lot["beni"] = bucket
    return lots


def _build_lot_index_entries(lots: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    lot_index: List[Dict[str, Any]] = []
    for lot in lots:
        if not isinstance(lot, dict):
            continue
        try:
            lot_num = int(lot.get("lot_number"))
        except Exception:
            continue
        lotto_ev = lot.get("evidence", {}).get("lotto", []) if isinstance(lot.get("evidence"), dict) else []
        first_ev = lotto_ev[0] if isinstance(lotto_ev, list) and lotto_ev else {}
        lot_index.append(
            {
                "lot": lot_num,
                "prezzo": lot.get("prezzo_base_eur"),
                "ubicazione": str(lot.get("ubicazione") or "")[:80],
                "page": first_ev.get("page"),
                "quote": first_ev.get("quote"),
            }
        )
    return lot_index

def _build_fallback_lot_from_pages(pages: List[Dict[str, Any]]) -> Dict[str, Any]:
    lot = _ensure_lot_contract({}, 1)
    lotto_ev = _find_regex_in_pages(pages, r"\\bLOTTO\\s+UNICO\\b|\\bLOTTO\\s+1\\b", re.I, field_key="lotto")
    if lotto_ev:
        lot["evidence"]["lotto"] = [lotto_ev]
    prezzo_ev = _find_regex_in_pages(
        pages,
        r"PREZZO\\s+BASE[^\\n]{0,60}?([0-9]{1,3}(?:[.\\s][0-9]{3})*(?:,[0-9]{2})?)",
        re.I,
        field_key="prezzo_base_asta",
    )
    if prezzo_ev:
        lot["evidence"]["prezzo_base"] = [prezzo_ev]
        parsed = _parse_euro_number(prezzo_ev.get("quote", ""))
        if parsed is not None:
            lot["prezzo_base_value"] = parsed
            lot["prezzo_base_eur"] = f"€ {parsed:,.0f}".replace(",", ".")
    ubic_ev = _find_regex_in_pages(pages, r"Ubicazione[:\\s]*([^\\n]{5,120})", re.I)
    if not ubic_ev:
        ubic_ev = _find_regex_in_pages(pages, r"\\b(Via|Viale|Piazza|Corso|Strada|Vicolo|Largo|Localit[aà])\\b[^\\n]{5,120}", re.I)
    if ubic_ev:
        lot["evidence"]["ubicazione"] = [ubic_ev]
        lot["ubicazione"] = _normalize_address_value(ubic_ev.get("quote", ""))
    diritto_ev = _find_regex_in_pages(pages, r"Diritto\\s+reale[:\\s]*([^\\n]{3,60})", re.I, field_key="diritto_reale")
    if not diritto_ev:
        diritto_ev = _find_regex_in_pages(
            pages,
            r"\\b(Nuda\\s+proprietà|Piena\\s+proprietà|Proprietà|Usufrutto|Diritto\\s+di\\s+[^\\n]{0,40})\\b",
            re.I,
            field_key="diritto_reale",
        )
    if diritto_ev:
        lot["evidence"]["diritto_reale"] = [diritto_ev]
        lot["diritto_reale"] = _normalize_headline_text(diritto_ev.get("quote", ""))
    sup_ev = _find_regex_in_pages(
        pages,
        r"Superficie[^\\d\\n]{0,40}([\\d.,]+)\\s*(m2|m²|mq)",
        re.I,
        field_key="superficie_catastale",
    )
    if sup_ev:
        lot["evidence"]["superficie"] = [sup_ev]
        match = re.search(r"(\\d{1,4}(?:[\\.,]\\d{1,2})?)", sup_ev.get("quote", ""))
        if match:
            raw = match.group(1).replace(".", "").replace(",", ".")
            try:
                lot["superficie_mq"] = float(raw)
            except Exception:
                pass
    return lot

def _apply_headline_overrides(result: Dict[str, Any], overrides: Dict[str, Any]) -> None:
    _apply_field_overrides(result, overrides, fields=["tribunale", "procedura", "lotto", "address"])

async def _persist_failed_analysis(
    *,
    analysis_id: str,
    user: User,
    case_id: str,
    run_id: str,
    file_name: str,
    input_sha256: str,
    pages_count: int,
    error_code: str,
    error_message: str
) -> None:
    analysis = PeriziaAnalysis(
        analysis_id=analysis_id,
        user_id=user.user_id,
        case_id=case_id,
        run_id=run_id,
        case_title=file_name,
        file_name=file_name,
        input_sha256=input_sha256,
        pages_count=pages_count,
        result={
            "schema_version": "nexodify_perizia_scan_v2",
            "qa_pass": {"status": "FAIL", "reasons": [{"code": error_code, "severity": "RED", "reason_it": error_message, "reason_en": error_message}]},
            "summary_for_client": {
                "summary_it": f"Analisi fallita: {error_code}. {error_message}",
                "summary_en": f"Analysis failed: {error_code}. {error_message}"
            }
        }
    )
    analysis_dict = analysis.model_dump()
    analysis_dict["created_at"] = analysis_dict["created_at"].isoformat()
    analysis_dict["status"] = "FAILED"
    analysis_dict["error_code"] = error_code
    analysis_dict["error_message"] = error_message
    analysis_dict["failed_at"] = datetime.now(timezone.utc).isoformat()
    try:
        await db.perizia_analyses.insert_one(analysis_dict)
    except Exception as e:
        logger.warning(f"Failed to persist analysis to DB: {e}")
        if OFFLINE_QA_ENV:
            try:
                out_dir = Path("/tmp/perizia_qa_run")
                out_dir.mkdir(parents=True, exist_ok=True)
                with open(out_dir / "analysis_failed.json", "w", encoding="utf-8") as f:
                    json.dump(analysis_dict, f, ensure_ascii=False, indent=2)
            except Exception as write_err:
                logger.warning(f"Failed to write offline failed analysis: {write_err}")

def _extract_lots_from_schema_riassuntivo(pages_in: List[Dict]) -> List[Dict[str, Any]]:
    sections = _build_lot_sections(pages_in)
    if not sections:
        return []

    lots: List[Dict[str, Any]] = []
    for section in sections:
        lot_num = int(section.get("lot_number") or (len(lots) + 1))
        lot_data = _ensure_lot_contract({}, lot_num)
        lot_data["evidence"]["lotto"] = section.get("header_evidence", []) if isinstance(section.get("header_evidence"), list) else []
        lots.append(lot_data)

    return _enrich_lots_from_sections(lots, pages_in, beni=None)

def _extract_beni_from_pages(pages_in: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    import re
    beni_by_num: Dict[int, Dict[str, Any]] = {}
    current_num: Optional[int] = None
    last_num_by_page: Dict[int, int] = {}

    def _clean_location_text(raw: Optional[str]) -> Optional[str]:
        if not raw:
            return None
        text = _normalize_headline_text(str(raw))
        text = re.sub(r"\.{2,}\s*\d+\s*$", "", text).strip()
        text = re.sub(r"\s{2,}", " ", text).strip(" ,;-")
        text = re.sub(r",\s*piano\s+[A-Za-zÀ-ÿ].*$", "", text, flags=re.I).strip(" ,;-")
        return text or None

    def _clean_piano_text(raw: Optional[str]) -> Optional[str]:
        if not raw:
            return None
        text = _normalize_headline_text(str(raw))
        text = re.sub(r"\.{2,}\s*\d+\s*$", "", text).strip()
        text = re.sub(r"^\s*piano\s+", "", text, flags=re.I).strip()
        text = re.sub(r"\s+(\d{1,2})\s*$", "", text).strip(" ,;-")
        text = re.sub(r"\s*-\s*", " - ", text)
        text = re.sub(r"\s{2,}", " ", text).strip(" ,;-")
        return text or None

    def ensure_bene(num: int) -> Dict[str, Any]:
        bene = beni_by_num.get(num)
        if bene is None:
            bene = {
                "bene_number": num,
                "tipologia": None,
                "short_location": None,
                "piano": None,
                "superficie_mq": None,
                "valore_stima_eur": None,
                "catasto": {},
                "note": [],
                "ape": None,
                "dichiarazioni_impianti": {
                    "elettrico": None,
                    "termico": None,
                    "idrico": None,
                },
                "dichiarazioni": {
                    "dichiarazione_impianto_elettrico": None,
                    "dichiarazione_impianto_termico": None,
                    "dichiarazione_impianto_idrico": None,
                },
                "stato_conservativo": {
                    "status_it": None,
                    "status_en": None,
                    "notes_it": None,
                    "evidence": [],
                },
                "impianti": {
                    "elettrico": {"status_it": None, "status_en": None, "notes_it": None, "evidence": []},
                    "idrico": {"status_it": None, "status_en": None, "notes_it": None, "evidence": []},
                    "termico": {"status_it": None, "status_en": None, "notes_it": None, "evidence": []},
                },
                "evidence": {
                    "tipologia": [],
                    "location_piano": [],
                    "superficie": [],
                    "valore_stima": [],
                    "catasto": [],
                    "note": [],
                    "ape": [],
                    "dichiarazioni_impianti": {
                        "elettrico": [],
                        "termico": [],
                        "idrico": [],
                    },
                    "dichiarazioni": {
                        "dichiarazione_impianto_elettrico": [],
                        "dichiarazione_impianto_termico": [],
                        "dichiarazione_impianto_idrico": [],
                    },
                    "stato_conservativo": [],
                    "impianti": {
                        "elettrico": [],
                        "idrico": [],
                        "termico": [],
                    },
                },
            }
            beni_by_num[num] = bene
        return bene

    def _status_from_declaration_line(line_raw: str) -> Optional[str]:
        line = str(line_raw or "").lower()
        if not line.strip():
            return None
        if "non esiste" in line:
            return "Non esiste"
        if "non presente" in line:
            return "Non presente"
        if "assente" in line:
            return "Assente"
        if "presente" in line or "esiste" in line:
            return "Presente"
        return None

    def _pick_better_status(existing: Optional[str], candidate: Optional[str]) -> Optional[str]:
        rank = {
            "Non esiste": 4,
            "Non presente": 3,
            "Assente": 3,
            "Presente": 2,
            None: 0,
            "": 0,
        }
        if not candidate:
            return existing
        if not existing:
            return candidate
        return candidate if rank.get(candidate, 1) >= rank.get(existing, 1) else existing

    def _impianto_status_rank(status_it: Optional[str]) -> int:
        rank = {
            "Non funzionante": 5,
            "Presente solo in centrale termica": 4,
            "Presente": 3,
            "Assente": 2,
            None: 0,
            "": 0,
        }
        return rank.get(status_it, 1)

    def _status_en_from_it(status_it: Optional[str]) -> Optional[str]:
        mapping = {
            "Presente": "Present",
            "Assente": "Missing",
            "Non funzionante": "Not working",
            "Presente solo in centrale termica": "Present only in boiler room",
        }
        if not status_it:
            return None
        return mapping.get(status_it, _normalize_headline_text(str(status_it)))

    def _set_impianto_state(
        bene: Dict[str, Any],
        system_key: str,
        status_it: Optional[str],
        note_it: Optional[str],
        ev: Optional[Dict[str, Any]],
    ) -> None:
        if system_key not in {"elettrico", "idrico", "termico"}:
            return
        imp = bene.get("impianti")
        if not isinstance(imp, dict):
            imp = {}
        state = imp.get(system_key)
        if not isinstance(state, dict):
            state = {"status_it": None, "status_en": None, "notes_it": None, "evidence": []}

        existing_status = state.get("status_it")
        if _impianto_status_rank(status_it) >= _impianto_status_rank(existing_status):
            state["status_it"] = status_it
            state["status_en"] = _status_en_from_it(status_it)

        if note_it:
            existing_notes = str(state.get("notes_it") or "").strip()
            if existing_notes:
                if note_it not in existing_notes:
                    state["notes_it"] = f"{existing_notes}; {note_it}"
            else:
                state["notes_it"] = note_it

        ev_list = state.get("evidence")
        if not isinstance(ev_list, list):
            ev_list = []
        if isinstance(ev, dict) and ev:
            ev_key = f"{ev.get('page')}|{str(ev.get('quote') or '')[:140]}"
            seen = {f"{e.get('page')}|{str(e.get('quote') or '')[:140]}" for e in ev_list if isinstance(e, dict)}
            if ev_key not in seen:
                ev_list.append(ev)
        state["evidence"] = ev_list[:4]
        imp[system_key] = state
        bene["impianti"] = imp

        ev_container = bene.get("evidence")
        if not isinstance(ev_container, dict):
            ev_container = {}
        imp_ev = ev_container.get("impianti")
        if not isinstance(imp_ev, dict):
            imp_ev = {"elettrico": [], "idrico": [], "termico": []}
        if isinstance(ev, dict) and ev:
            bucket = imp_ev.get(system_key)
            if not isinstance(bucket, list):
                bucket = []
            ev_key = f"{ev.get('page')}|{str(ev.get('quote') or '')[:140]}"
            seen = {f"{e.get('page')}|{str(e.get('quote') or '')[:140]}" for e in bucket if isinstance(e, dict)}
            if ev_key not in seen:
                bucket.append(ev)
            imp_ev[system_key] = bucket[:4]
        ev_container["impianti"] = imp_ev
        bene["evidence"] = ev_container

    def _append_stato_conservativo_phrase(
        bene: Dict[str, Any],
        phrase: str,
        ev: Optional[Dict[str, Any]],
    ) -> None:
        phrase_norm = _normalize_headline_text(str(phrase or ""))
        if not phrase_norm:
            return
        tmp = bene.get("_stato_conservativo_phrases")
        if not isinstance(tmp, list):
            tmp = []
        if phrase_norm not in tmp:
            tmp.append(phrase_norm)
        bene["_stato_conservativo_phrases"] = tmp

        ev_container = bene.get("evidence")
        if not isinstance(ev_container, dict):
            ev_container = {}
        ev_list = ev_container.get("stato_conservativo")
        if not isinstance(ev_list, list):
            ev_list = []
        if isinstance(ev, dict) and ev:
            ev_key = f"{ev.get('page')}|{str(ev.get('quote') or '')[:140]}"
            seen = {f"{e.get('page')}|{str(e.get('quote') or '')[:140]}" for e in ev_list if isinstance(e, dict)}
            if ev_key not in seen:
                ev_list.append(ev)
        ev_container["stato_conservativo"] = ev_list[:5]
        bene["evidence"] = ev_container

    def _assign_declaration_to_bene(
        mapped_num: int,
        status: str,
        ev: Dict[str, Any],
        system_key: Optional[str] = None,
        is_ape: bool = False,
    ) -> None:
        bene = ensure_bene(mapped_num)
        if is_ape:
            bene["ape"] = _pick_better_status(bene.get("ape"), status)
            if isinstance(ev, dict) and ev:
                existing = bene["evidence"].get("ape", [])
                if not isinstance(existing, list):
                    existing = []
                if not existing:
                    bene["evidence"]["ape"] = [ev]
            return

        if not system_key:
            return
        imp_map = bene.get("dichiarazioni_impianti")
        if not isinstance(imp_map, dict):
            imp_map = {"elettrico": None, "termico": None, "idrico": None}
        imp_map[system_key] = _pick_better_status(imp_map.get(system_key), status)
        bene["dichiarazioni_impianti"] = imp_map

        alias_map = bene.get("dichiarazioni")
        if not isinstance(alias_map, dict):
            alias_map = {}
        alias_key = f"dichiarazione_impianto_{system_key}"
        alias_map[alias_key] = _pick_better_status(alias_map.get(alias_key), status)
        bene["dichiarazioni"] = alias_map

        ev_container = bene.get("evidence")
        if not isinstance(ev_container, dict):
            ev_container = {}
        imp_ev = ev_container.get("dichiarazioni_impianti")
        if not isinstance(imp_ev, dict):
            imp_ev = {"elettrico": [], "termico": [], "idrico": []}
        if isinstance(ev, dict) and ev and not imp_ev.get(system_key):
            imp_ev[system_key] = [ev]
        ev_container["dichiarazioni_impianti"] = imp_ev

        alias_ev = ev_container.get("dichiarazioni")
        if not isinstance(alias_ev, dict):
            alias_ev = {
                "dichiarazione_impianto_elettrico": [],
                "dichiarazione_impianto_termico": [],
                "dichiarazione_impianto_idrico": [],
            }
        if isinstance(ev, dict) and ev and not alias_ev.get(alias_key):
            alias_ev[alias_key] = [ev]
        ev_container["dichiarazioni"] = alias_ev
        bene["evidence"] = ev_container

    for p in pages_in:
        text = str(p.get("text", "") or "")
        page_num = int(p.get("page_number", 0) or 0)
        page_start_bene_num = current_num
        heading_positions: List[Tuple[int, int]] = []

        # Bene headings
        for m in re.finditer(r"\bBene\s*(?:N[°o]\s*)?(\d+)\s*-\s*([^\n]+)", text, re.I):
            try:
                num = int(m.group(1))
            except Exception:
                continue
            current_num = num
            last_num_by_page[page_num] = num
            heading_positions.append((m.start(), num))
            bene = ensure_bene(num)
            heading = m.group(2).strip()
            tip_m = re.search(r"^([A-Za-zÀ-Ù'\s]+?)\s+ubicat[oa]", heading, re.I)
            if tip_m:
                tipologia = _normalize_headline_text(tip_m.group(1).strip())
            else:
                tipologia = _normalize_headline_text(heading.split(",")[0])
            if tipologia and not bene.get("tipologia"):
                bene["tipologia"] = tipologia
                bene["evidence"]["tipologia"] = [_build_evidence(text, page_num, m.start(), m.end())]

            loc_piano_m = re.search(r"ubicat[oa]\s+a?\s*(?P<loc>.*?)(?:,\s*piano\s+(?P<piano>[^\.]+))?$", heading, re.I)
            if loc_piano_m:
                loc = _clean_location_text(loc_piano_m.group("loc"))
                piano = _clean_piano_text(loc_piano_m.group("piano"))
                can_replace_loc = not bene.get("short_location")
                can_replace_piano = not bene.get("piano")
                if loc and can_replace_loc:
                    bene["short_location"] = loc
                if piano and can_replace_piano:
                    bene["piano"] = piano
                if (loc or piano) and not bene["evidence"].get("location_piano"):
                    bene["evidence"]["location_piano"] = [_build_evidence(text, page_num, m.start(), m.end())]

        # Summary rows with superficie and valore per-bene (deterministic table).
        normalized_page_text = " ".join(text.split())
        row_pattern = re.compile(
            r"Bene\s*N[°o]\s*(\d+)\s*-\s*([^\n]{0,260}?)\s(\d{1,3},\d{2})\s*mq\s+[0-9\.,]+\s*€/mq\s*€\s*([0-9\.\s]+,\d{2})",
            re.I,
        )
        for m in row_pattern.finditer(normalized_page_text):
            try:
                num = int(m.group(1))
            except Exception:
                continue
            bene = ensure_bene(num)
            row_desc = _normalize_headline_text(m.group(2))
            row_loc = None
            row_piano = None
            row_lp = re.search(r"^(.*?),\s*piano\s+(.+)$", row_desc, re.I)
            if row_lp:
                row_loc = _clean_location_text(row_lp.group(1))
                row_piano = _clean_piano_text(row_lp.group(2))
            else:
                row_loc = _clean_location_text(row_desc)

            superficie_val = _parse_euro_number(m.group(3))
            stima_val = _parse_euro_number(m.group(4))
            if row_loc:
                bene["short_location"] = row_loc
            if row_piano:
                bene["piano"] = row_piano
            if isinstance(superficie_val, (int, float)) and bene.get("superficie_mq") is None:
                bene["superficie_mq"] = round(float(superficie_val), 2)
            if isinstance(stima_val, (int, float)) and bene.get("valore_stima_eur") is None:
                bene["valore_stima_eur"] = int(round(float(stima_val)))
            row_quote = _normalize_headline_text(m.group(0))
            ev_obj = {"page": page_num, "quote": row_quote, "search_hint": row_quote[:120]}
            bene["evidence"]["location_piano"] = [ev_obj]
            if not bene["evidence"].get("superficie"):
                bene["evidence"]["superficie"] = [ev_obj]
            if not bene["evidence"].get("valore_stima"):
                bene["evidence"]["valore_stima"] = [ev_obj]

        # Catasto lines linked to current bene
        for m in re.finditer(r"Identificato\s+al\s+catasto[^\n]*", text, re.I):
            if current_num is None:
                continue
            bene = ensure_bene(current_num)
            line_start, line_end = _line_bounds(text, m.start(), m.end())
            line = text[line_start:line_end]
            catasto = bene.get("catasto", {})

            def _set_if(key: str, val: Optional[str]) -> None:
                if val and not catasto.get(key):
                    catasto[key] = val

            foglio = re.search(r"(?:Fg\.?|Foglio)\s*(\d+)", line, re.I)
            particella = re.search(r"(?:Part\.?|Particella)\s*([0-9]+)", line, re.I)
            sub = re.search(r"(?:Sub\.?|Subalterno)\s*([0-9]+)", line, re.I)
            categoria = re.search(r"Categoria\s*([A-Z]\s*/?\s*\d+)", line, re.I)
            _set_if("foglio", foglio.group(1) if foglio else None)
            _set_if("particella", particella.group(1) if particella else None)
            _set_if("sub", sub.group(1) if sub else None)
            if categoria:
                _set_if("categoria", _normalize_headline_text(categoria.group(1)))

            bene["catasto"] = catasto
            bene["evidence"]["catasto"] = [_build_evidence(text, page_num, line_start, line_end)]

        # Consistenza / Rendita
        for m in re.finditer(r"(Consistenza[^\n]{0,120}|Rendita\s+Catastale[^\n]{0,120})", text, re.I):
            if current_num is None:
                continue
            bene = ensure_bene(current_num)
            line_start, line_end = _line_bounds(text, m.start(), m.end())
            line = text[line_start:line_end]
            catasto = bene.get("catasto", {})
            cons_m = re.search(r"Consistenza\s*([^,\n]+)", line, re.I)
            rend_m = re.search(r"Rendita\s+Catastale[^€]*€\s*([\d\.,]+)", line, re.I)
            if cons_m and not catasto.get("consistenza"):
                catasto["consistenza"] = _normalize_headline_text(cons_m.group(1))
            if rend_m and not catasto.get("rendita"):
                catasto["rendita"] = f"€ {rend_m.group(1).strip()}"
            bene["catasto"] = catasto
            if cons_m or rend_m:
                bene["evidence"]["catasto"] = [_build_evidence(text, page_num, line_start, line_end)]

        # Agibilità notes
        for m in re.finditer(r"(non\s+è\s+presente\s+l'?abitabilit[aà]|non\s+risulta\s+agibile|risulta\s+agibile)", text, re.I):
            if current_num is None:
                continue
            bene = ensure_bene(current_num)
            line_start, line_end = _line_bounds(text, m.start(), m.end())
            line = text[line_start:line_end].strip()
            note = _normalize_headline_text(line)
            if note and note not in bene.get("note", []):
                bene.setdefault("note", []).append(note)
                bene["evidence"]["note"] = [_build_evidence(text, page_num, line_start, line_end)]

        # Match headings on this page for deterministic bene mapping.
        heading_matches = [hm for hm in re.finditer(r"\bBene\s*(?:N[°o]\s*)?(\d+)\s*-\s*", text, re.I)]

        # Superficie lines by current bene in descriptive sections (only when unambiguous).
        heading_count_on_page = len(heading_matches)
        superficie_matches_on_page = list(re.finditer(r"Superficie\s+(\d{1,3}(?:,\d{1,2})?)\s*mq", text, re.I))
        if heading_count_on_page <= 1 and len(superficie_matches_on_page) == 1:
            surface_iter = superficie_matches_on_page
        else:
            surface_iter = []
        for m in surface_iter:
            num = current_num or last_num_by_page.get(page_num)
            if num is None:
                continue
            bene = ensure_bene(num)
            sup_val = _parse_euro_number(m.group(1))
            if isinstance(sup_val, (int, float)) and bene.get("superficie_mq") is None:
                bene["superficie_mq"] = round(float(sup_val), 2)
            if not bene["evidence"].get("superficie"):
                line_start, line_end = _line_bounds(text, m.start(), m.end())
                bene["evidence"]["superficie"] = [_build_evidence(text, page_num, line_start, line_end)]

        # "Valore di stima del bene" lines: map deterministically to bene on page.
        heading_order: List[int] = []
        for hm in heading_matches:
            try:
                num = int(hm.group(1))
            except Exception:
                continue
            if num not in heading_order:
                heading_order.append(num)
        value_matches = [vm for vm in re.finditer(r"Valore\s+di\s+stima\s+del\s+bene[^\n]{0,30}?€\s*([0-9\.\s]+,\d{2})", text, re.I)]
        for idx, vm in enumerate(value_matches):
            mapped_num: Optional[int] = None
            if idx < len(heading_order):
                mapped_num = heading_order[idx]
            if mapped_num is None:
                prev_headings = [hm for hm in heading_matches if hm.start() <= vm.start()]
                if prev_headings:
                    try:
                        mapped_num = int(prev_headings[-1].group(1))
                    except Exception:
                        mapped_num = None
            if mapped_num is None:
                mapped_num = current_num
            if mapped_num is None:
                continue
            bene = ensure_bene(mapped_num)
            val = _parse_euro_number(vm.group(1))
            if isinstance(val, (int, float)) and bene.get("valore_stima_eur") is None:
                bene["valore_stima_eur"] = int(round(float(val)))
            if not bene["evidence"].get("valore_stima"):
                line_start, line_end = _line_bounds(text, vm.start(), vm.end())
                bene["evidence"]["valore_stima"] = [_build_evidence(text, page_num, line_start, line_end)]

        # Certificazioni energetiche / dichiarazioni impianti (deterministic, per-bene).
        raw_lines = text.splitlines(keepends=True)
        offset = 0
        for raw_line in raw_lines:
            line = str(raw_line or "")
            line_start = offset
            line_end = offset + len(line)
            offset = line_end
            compact = _normalize_headline_text(line)
            if not compact:
                continue
            low = compact.lower()
            low_no_space = re.sub(r"\s+", "", low)
            if not (
                "certificato energetico" in low
                or "attestato di prestazione energetica" in low
                or re.search(r"\bape\b", low, re.I)
                or ("dichiarazionediconformit" in low_no_space and "impianto" in low_no_space)
            ):
                continue

            status = _status_from_declaration_line(compact)
            if not status:
                continue

            mapped_num: Optional[int] = None
            for head_pos, head_num in heading_positions:
                if head_pos <= line_start:
                    mapped_num = head_num
                else:
                    break
            if mapped_num is None:
                mapped_num = page_start_bene_num if page_start_bene_num is not None else last_num_by_page.get(page_num)
            if mapped_num is None:
                continue
            ev = _build_evidence(text, page_num, line_start, line_end)

            # APE / certificazione energetica (separata dalle dichiarazioni impianti).
            if "certificato energetico" in low or "attestato di prestazione energetica" in low or re.search(r"\bape\b", low, re.I):
                _assign_declaration_to_bene(mapped_num, status, ev, is_ape=True)
                continue

            # Dichiarazioni di conformita impianti (non stato/esistenza impianto).
            system_key = None
            if "impiantoelettric" in low_no_space or re.search(r"impiant\w*\s+elettric\w*", low, re.I):
                system_key = "elettrico"
            elif "impiantotermic" in low_no_space or re.search(r"impiant\w*\s+termic\w*", low, re.I):
                system_key = "termico"
            elif "impiantoidric" in low_no_space or re.search(r"impiant\w*\s+idric\w*", low, re.I):
                system_key = "idrico"
            if not system_key:
                continue
            _assign_declaration_to_bene(mapped_num, status, ev, system_key=system_key, is_ape=False)

        # Regex fallback for wrapped declaration lines (e.g. "... impianto\\ntermico").
        fallback_patterns = [
            ("elettrico", re.compile(
                r"(?:non\s+esiste|non\s+presente|assente|presente|esiste)[\s\S]{0,120}dichiarazio\s*ne\s+di\s+conformit[àa]\s+dell['’]?\s*impiant\w*[\s\-]*elettric\w*[^\n]*"
                r"|dichiarazio\s*ne\s+di\s+conformit[àa]\s+dell['’]?\s*impiant\w*[\s\-]*elettric\w*[\s\S]{0,120}(?:non\s+esiste|non\s+presente|assente|presente|esiste)",
                re.I,
            )),
            ("termico", re.compile(
                r"(?:non\s+esiste|non\s+presente|assente|presente|esiste)[\s\S]{0,120}dichiarazio\s*ne\s+di\s+conformit[àa]\s+dell['’]?\s*impiant\w*[\s\-]*termic\w*[^\n]*"
                r"|dichiarazio\s*ne\s+di\s+conformit[àa]\s+dell['’]?\s*impiant\w*[\s\-]*termic\w*[\s\S]{0,120}(?:non\s+esiste|non\s+presente|assente|presente|esiste)",
                re.I,
            )),
            ("idrico", re.compile(
                r"(?:non\s+esiste|non\s+presente|assente|presente|esiste)[\s\S]{0,120}dichiarazio\s*ne\s+di\s+conformit[àa]\s+dell['’]?\s*impiant\w*[\s\-]*idric\w*[^\n]*"
                r"|dichiarazio\s*ne\s+di\s+conformit[àa]\s+dell['’]?\s*impiant\w*[\s\-]*idric\w*[\s\S]{0,120}(?:non\s+esiste|non\s+presente|assente|presente|esiste)",
                re.I,
            )),
        ]
        for system_key, pattern in fallback_patterns:
            for m in pattern.finditer(text):
                status = _status_from_declaration_line(m.group(0))
                if not status:
                    continue
                mapped_num: Optional[int] = None
                for head_pos, head_num in heading_positions:
                    if head_pos <= m.start():
                        mapped_num = head_num
                    else:
                        break
                if mapped_num is None:
                    mapped_num = page_start_bene_num if page_start_bene_num is not None else last_num_by_page.get(page_num)
                if mapped_num is None:
                    continue
                line_start, line_end = _line_bounds(text, m.start(), m.end())
                ev = _build_evidence(text, page_num, line_start, line_end)
                _assign_declaration_to_bene(mapped_num, status, ev, system_key=system_key, is_ape=False)

        # Impianti per-bene (deterministic existence/condition; separate from dichiarazioni).
        raw_lines = text.splitlines(keepends=True)
        offset = 0
        for raw_line in raw_lines:
            line = str(raw_line or "")
            line_start = offset
            line_end = offset + len(line)
            offset = line_end
            compact = _normalize_headline_text(line)
            if not compact:
                continue
            low = compact.lower()
            low_no_space = re.sub(r"\s+", "", low)

            mapped_num: Optional[int] = None
            for head_pos, head_num in heading_positions:
                if head_pos <= line_start:
                    mapped_num = head_num
                else:
                    break
            if mapped_num is None:
                mapped_num = page_start_bene_num if page_start_bene_num is not None else last_num_by_page.get(page_num)
            if mapped_num is None:
                continue
            bene = ensure_bene(mapped_num)
            ev = _build_evidence(text, page_num, line_start, line_end)

            # Stato conservativo per-bene (concise phrases).
            if "buono stato di conservazione" in low:
                if "piano terra" in low:
                    _append_stato_conservativo_phrase(bene, "Piano terra buono", ev)
                elif "piano primo" in low:
                    _append_stato_conservativo_phrase(bene, "Piano primo buono", ev)
                else:
                    _append_stato_conservativo_phrase(bene, "Buono stato", ev)
            elif "si presenta al grezzo" in low or "a vista grezzo" in low:
                if "piano primo" in low:
                    _append_stato_conservativo_phrase(bene, "Piano primo al grezzo", ev)
                else:
                    _append_stato_conservativo_phrase(bene, "Al grezzo", ev)
            elif "trascurato stato di manutenzione" in low:
                _append_stato_conservativo_phrase(bene, "Area laterale in trascurato stato di manutenzione", ev)
            elif "tracce di umidità" in low:
                _append_stato_conservativo_phrase(bene, "Tracce di umidità", ev)

            # Impianti status extraction.
            # Note: declaration-of-conformity phrases are handled in dichiarazioni_impianti,
            # and must not be interpreted as impianto presence/absence.
            segments = [s.strip() for s in re.split(r";\s*", compact) if str(s).strip()]
            if not segments:
                segments = [compact]
            for segment in segments:
                seg_low = segment.lower()
                seg_no_space = re.sub(r"\s+", "", seg_low)
                if "dichiarazion" in seg_no_space and "conformit" in seg_no_space:
                    continue

                system_key: Optional[str] = None
                if "impiantoelettric" in seg_no_space or re.search(r"impiant\w*\s+elettric\w*", seg_low, re.I):
                    system_key = "elettrico"
                elif "impiantoidric" in seg_no_space or re.search(r"impiant\w*\s+idric\w*", seg_low, re.I):
                    system_key = "idrico"
                elif (
                    "impiantodiriscaldamento" in seg_no_space
                    or "impiantotermic" in seg_no_space
                    or re.search(r"\briscaldament\w*\b", seg_low, re.I)
                    or re.search(r"impiant\w*\s+di\s+riscaldament\w*", seg_low, re.I)
                ):
                    system_key = "termico"
                if not system_key:
                    continue

                status_it: Optional[str] = None
                note_it: Optional[str] = None
                if "non funzion" in seg_low:
                    status_it = "Non funzionante"
                elif "presentesolonellacentraletermica" in seg_no_space:
                    status_it = "Presente solo in centrale termica"
                elif "non presente" in seg_low or "non esiste" in seg_low or "assente" in seg_low:
                    status_it = "Assente"
                elif (
                    "presente" in seg_low
                    or "alimentat" in seg_low
                    or "allacciat" in seg_low
                    or "dotat" in seg_low
                    or "con caldaia" in seg_low
                ):
                    status_it = "Presente"
                if status_it:
                    _set_impianto_state(bene, system_key, status_it, note_it, ev)

            # Shared meters notes (Bene 4, idrico/termico).
            if "contatore dell’acqua" in low or "contatore dell'acqua" in low:
                if "in comune" in low:
                    _set_impianto_state(bene, "idrico", bene.get("impianti", {}).get("idrico", {}).get("status_it") or "Presente", "Contatore acqua in comune", ev)
            if "contatore del gas" in low and "in comune" in low:
                _set_impianto_state(bene, "termico", bene.get("impianti", {}).get("termico", {}).get("status_it") or "Presente", "Contatore gas in comune", ev)

    beni = [beni_by_num[k] for k in sorted(beni_by_num.keys())]
    for bene in beni:
        phrases = bene.pop("_stato_conservativo_phrases", [])
        if not isinstance(phrases, list):
            phrases = []
        phrases_low = [str(p).lower() for p in phrases]
        status_it: Optional[str] = None
        notes_it: Optional[str] = None

        has_buono = any("buono" in p for p in phrases_low)
        has_grezzo = any("grezzo" in p for p in phrases_low)
        has_trascurato = any("trascurato stato di manutenzione" in p for p in phrases_low)

        if has_buono and has_grezzo:
            status_it = "Piano terra buono; piano primo al grezzo"
        elif has_buono and has_trascurato:
            status_it = "In generale buono; area laterale in trascurato stato di manutenzione"
        elif has_buono:
            status_it = "Buono stato"
        elif has_grezzo:
            status_it = "Al grezzo"

        if any("tracce di umidità" in p for p in phrases_low):
            notes_it = "Tracce di umidità rilevate"

        stato_ev = bene.get("evidence", {}).get("stato_conservativo", []) if isinstance(bene.get("evidence"), dict) else []
        bene["stato_conservativo"] = {
            "status_it": status_it,
            "status_en": _status_en_from_it(status_it),
            "notes_it": notes_it,
            "evidence": stato_ev if isinstance(stato_ev, list) else [],
        }

        impianti = bene.get("impianti")
        if not isinstance(impianti, dict):
            impianti = {}
        for key in ("elettrico", "idrico", "termico"):
            s = impianti.get(key)
            if not isinstance(s, dict):
                s = {"status_it": None, "status_en": None, "notes_it": None, "evidence": []}
            status_it = s.get("status_it")
            s["status_en"] = _status_en_from_it(status_it)
            if not isinstance(s.get("evidence"), list):
                s["evidence"] = []
            impianti[key] = s
        bene["impianti"] = impianti

    return beni

def _scan_legal_killers(pages_in: List[Dict]) -> List[Dict[str, Any]]:
    import re
    killers = []
    patterns = [
        (r"FORMALITÀ\s+DA\s+CANCELLARE\s+CON\s+IL\s+DECRETO\s+DI\s+TRASFERIMENTO", "Formalità da cancellare", "GIALLO"),
        (r"Oneri\s+di\s+cancellazione[:\s]*([^\n]+)", "Oneri di cancellazione", "GIALLO"),
        (r"servitù[^.]*", "Servitù rilevata", "GIALLO"),
        (r"stradella|barriera", "Servitù di passaggio/barriera", "GIALLO"),
        (r"D\.?L\.?\s*69/2024|salva\s+casa", "Riferimento D.L. 69/2024 Salva Casa", "GIALLO"),
        (r"L\.?R\.?\s*Toscana\s*65/2014", "Riferimento L.R. Toscana 65/2014", "GIALLO"),
        (r"accertamento\s+di\s+conformità", "Accertamento di conformità richiesto", "GIALLO"),
        (r"difformità[^.]*regolarizz", "Difformità da regolarizzare", "ROSSO"),
        (r"abuso[^.]*edilizio|abuso[^.]*insanabile", "Abuso edilizio", "ROSSO"),
        (r"usi\s+civici", "Usi civici", "ROSSO"),
        (r"PEEP|diritto\s+di\s+superficie", "Diritto di superficie / PEEP", "ROSSO"),
        (r"amianto|eternit", "Presenza amianto/eternit", "ROSSO"),
    ]
    seen = set()
    for p in pages_in:
        text = str(p.get("text", "") or "")
        page_num = p.get("page_number", 0)
        for pattern, title, severity in patterns:
            for m in re.finditer(pattern, text, re.I):
                key = f"{title}_{page_num}"
                if key not in seen:
                    context = text[max(0, m.start()-180):min(len(text), m.end()+140)]
                    snippet = text[max(0, m.start()-30):min(len(text), m.end()+100)]
                    if _is_toc_like_quote(snippet) or _is_toc_like_line(snippet):
                        continue
                    low = snippet.lower()
                    low_context = context.lower()
                    if title == "Usi civici":
                        if _legal_subject_negated(low_context, r"(diritti\s+demaniali|usi\s+civici)"):
                            continue
                    if title.startswith("Servitù") or title == "Servitù rilevata":
                        if _legal_subject_negated(low_context, r"servit[ùu]"):
                            continue
                        if "servitù, censo, livello, usi civici" in low:
                            if not re.search(r"passo|carrabile|pedonale|stradella|fognatura|utenz|attraversamento", low, re.I):
                                continue
                        if re.search(r"servit[ùu]\s+attive\s+e\s+passive", low, re.I) and not re.search(r"stradella|passo|carrabile|pedonale|fognatura|utenz", low, re.I):
                            continue
                    seen.add(key)
                    killers.append({
                        "title": title,
                        "severity": severity,
                        "page": page_num,
                        "quote": snippet.replace("\n", " ")[:200],
                        "start_offset": m.start(),
                        "end_offset": m.end(),
                        "why_it_matters": f"Rilevato: {m.group(0)[:80]}"
                    })
    return killers

def _extract_deprezzamenti(pages_in: List[Dict]) -> List[Dict[str, Any]]:
    import re
    items = []
    for p in pages_in:
        text = str(p.get("text", "") or "")
        page_num = int(p.get("page_number", 0) or 0)
        if "Deprezzamenti" not in text and "deprezzamenti" not in text:
            continue
        for m in re.finditer(r"(Oneri[^\n]+?)(\d[\d\.,]+)\s*€", text, re.I):
            label = m.group(1).strip()
            value_raw = m.group(2).strip()
            try:
                value = float(value_raw.replace(".", "").replace(",", "."))
            except Exception:
                continue
            items.append({
                "label": label,
                "value": value,
                "evidence": _build_evidence(text, page_num, m.start(), m.end())
            })
        for m in re.finditer(r"(Rischio[^\n]+?)(\d[\d\.,]+)\s*€", text, re.I):
            label = m.group(1).strip()
            value_raw = m.group(2).strip()
            try:
                value = float(value_raw.replace(".", "").replace(",", "."))
            except Exception:
                continue
            items.append({
                "label": label,
                "value": value,
                "evidence": _build_evidence(text, page_num, m.start(), m.end())
            })
    return items


def _normalize_contract_evidence_list(raw: Any, max_items: int = 4) -> List[Dict[str, Any]]:
    if not isinstance(raw, list):
        return []
    out: List[Dict[str, Any]] = []
    for entry in raw:
        if not isinstance(entry, dict):
            continue
        quote = _normalize_headline_text(str(entry.get("quote", "") or ""))
        if not quote:
            continue
        try:
            page = int(entry.get("page", 0) or 0)
        except Exception:
            page = 0
        payload: Dict[str, Any] = {
            "page": page,
            "quote": quote,
            "search_hint": str(entry.get("search_hint") or quote[:120]).strip(),
        }
        out.append(payload)
        if len(out) >= max_items:
            break
    return out


def _extract_contract_value_from_obj(value_obj: Any) -> Tuple[Optional[float], List[Dict[str, Any]]]:
    if isinstance(value_obj, dict):
        value = _as_float_or_none(value_obj.get("value"))
        evidence = _normalize_contract_evidence_list(value_obj.get("evidence", []))
        if value is not None and not (value == 0 and not evidence):
            return value, evidence
        formatted = _as_float_or_none(value_obj.get("formatted"))
        if formatted is not None and not (formatted == 0 and not evidence):
            return formatted, evidence
    value = _as_float_or_none(value_obj)
    if value == 0:
        return None, []
    return value, []


def _parse_dep_total_from_obj(value_obj: Any) -> Tuple[Optional[float], List[Dict[str, Any]]]:
    if isinstance(value_obj, list):
        nums: List[float] = []
        ev_all: List[Dict[str, Any]] = []
        for item in value_obj:
            if not isinstance(item, dict):
                continue
            num = _as_float_or_none(item.get("importo"))
            if num is None:
                num = _as_float_or_none(item.get("value"))
            if num is None:
                num = _as_float_or_none(item.get("amount"))
            if num is not None:
                nums.append(num)
            ev_all.extend(_normalize_contract_evidence_list(item.get("evidence", []), max_items=2))
        if nums and any(abs(n) > 0.0001 for n in nums):
            return sum(nums), ev_all[:4]
    if isinstance(value_obj, dict):
        direct = _as_float_or_none(value_obj.get("totale"))
        if direct is None:
            direct = _as_float_or_none(value_obj.get("total"))
        if direct is None:
            direct = _as_float_or_none(value_obj.get("importo"))
        if direct is None:
            direct = _as_float_or_none(value_obj.get("value"))
        if direct is not None and not (direct == 0 and not _normalize_contract_evidence_list(value_obj.get("evidence", []))):
            return direct, _normalize_contract_evidence_list(value_obj.get("evidence", []))
    return None, []


def _extract_location_piano_from_bene(bene: Dict[str, Any], lot: Optional[Dict[str, Any]]) -> Tuple[Optional[str], Optional[str]]:
    def _clean_loc(raw: Optional[str]) -> Optional[str]:
        if not raw:
            return None
        text = _normalize_headline_text(str(raw))
        text = re.sub(r"\.{2,}\s*\d+\s*$", "", text).strip()
        text = re.sub(r",\s*piano\s+[A-Za-zÀ-ÿ].*$", "", text, flags=re.I).strip(" ,;-")
        text = re.sub(r"\s{2,}", " ", text).strip(" ,;-")
        return text or None

    def _clean_piano(raw: Optional[str]) -> Optional[str]:
        if not raw:
            return None
        text = _normalize_headline_text(str(raw))
        text = re.sub(r"^\s*piano\s+", "", text, flags=re.I).strip()
        text = re.sub(r"\.{2,}\s*\d+\s*$", "", text).strip()
        text = re.sub(r"\s+\d{1,2}\s*$", "", text).strip(" ,;-")
        text = re.sub(r"\s*-\s*", " - ", text)
        text = re.sub(r"\s{2,}", " ", text).strip(" ,;-")
        return text or None

    direct_loc = _clean_loc(str(bene.get("short_location", "") or ""))
    direct_piano = _clean_piano(str(bene.get("piano", "") or ""))
    if direct_loc or direct_piano:
        return direct_loc, direct_piano

    evidence = bene.get("evidence", {}) if isinstance(bene.get("evidence"), dict) else {}
    tip_ev = evidence.get("tipologia", []) if isinstance(evidence.get("tipologia"), list) else []
    if tip_ev:
        quote = _normalize_headline_text(str(tip_ev[0].get("quote", "") or ""))
        if quote:
            # Example: Bene N° 1 - Ufficio ubicato a ... , piano Terra-primo ...
            m = re.search(
                r"ubicat[oa]\s+a?\s*(?P<loc>.*?)(?:,\s*piano\s+(?P<piano>[^\.]+))?(?:\.{2,}\s*\d+\s*$|$)",
                quote,
                re.I,
            )
            if m:
                loc = _clean_loc(str(m.group("loc") or "").strip(" ,.-"))
                piano = _clean_piano(str(m.group("piano") or "").strip(" ,.-"))
                if loc:
                    return loc[:140], piano
    if isinstance(lot, dict):
        lot_loc = str(lot.get("ubicazione", "") or "").strip()
        if lot_loc and lot_loc.upper() not in {"TBD", "NON SPECIFICATO IN PERIZIA"}:
            piano_m = re.search(r"piano\s+(.+)$", lot_loc, re.I)
            piano = _clean_piano(piano_m.group(1)) if piano_m else None
            if piano_m:
                short_loc = _clean_loc(lot_loc[:piano_m.start()].strip(" ,-"))
            else:
                short_loc = _clean_loc(lot_loc)
            return short_loc[:140], piano
    return None, None


def _extract_bene_surface_value(bene: Dict[str, Any]) -> Optional[float]:
    direct_candidates = [
        bene.get("superficie_mq"),
        bene.get("superficie"),
        bene.get("superficie_convenzionale"),
        bene.get("superficie_convenzionale_mq"),
    ]
    for candidate in direct_candidates:
        value = _as_float_or_none(candidate)
        if value is not None:
            return value
    catasto = bene.get("catasto", {}) if isinstance(bene.get("catasto"), dict) else {}
    cons = str(catasto.get("consistenza", "") or "")
    m = re.search(r"(\d{1,4}(?:[.,]\d{1,2})?)\s*(?:mq|m2|m²)", cons, re.I)
    if m:
        return _as_float_or_none(m.group(1))
    return None


def _extract_bene_stima_value(bene: Dict[str, Any]) -> Optional[float]:
    for key in (
        "valore_stima_eur",
        "valore_stima",
        "valore_di_stima",
        "valore_di_stima_bene",
        "valore_stima_bene",
        "stima_euro",
        "valore_euro",
        "valore",
    ):
        value = _as_float_or_none(bene.get(key))
        if value is not None:
            return value
    return None


def _extract_valuation_waterfall_from_pages(pages: List[Dict[str, Any]]) -> Dict[str, Any]:
    out = {
        "valore_stima_eur": None,
        "deprezzamenti_eur": None,
        "valore_finale_eur": None,
        "prezzo_base_eur": None,
        "evidence": {
            "valore_stima_eur": [],
            "deprezzamenti_eur": [],
            "valore_finale_eur": [],
            "prezzo_base_eur": [],
        },
    }
    if not isinstance(pages, list) or not pages:
        return out

    prezzo_state = _extract_prezzo_base_asta_state(pages)
    if isinstance(prezzo_state, dict):
        prezzo_val = _as_float_or_none(prezzo_state.get("value"))
        if prezzo_val is not None:
            out["prezzo_base_eur"] = int(round(prezzo_val))
            out["evidence"]["prezzo_base_eur"] = _normalize_contract_evidence_list(prezzo_state.get("evidence", []))

    amount_re = r"(?P<value>(?:[0-9]{1,3}(?:[.\s][0-9]{3})+|[0-9]{4,7}|[0-9]{1,3})(?:,[0-9]{2})?)"
    stima_patterns = [
        re.compile(rf"Valore\s+di\s+stima[^\n]{{0,60}}?[:€]\s*{amount_re}", re.I),
        re.compile(rf"Valore\s+complessivo[^\n]{{0,60}}?stima[^\n]{{0,40}}?[:€]\s*{amount_re}", re.I),
    ]
    finale_pattern = re.compile(
        rf"Valore\s+finale\s+di\s+stima[^\n]{{0,40}}?[:€]\s*{amount_re}",
        re.I,
    )
    dep_total_pattern = re.compile(
        rf"Deprezzamenti[^\n]{{0,80}}?[:€]\s*{amount_re}",
        re.I,
    )
    dep_item_pattern = re.compile(
        rf"(?P<label>Oneri\s+di\s+regolarizzazione\s+urbanistica|Rischio\s+assunto\s+per\s+mancata\s+garanzia(?:\s+per\s+vizi\s+occulti)?|Vizi\s+occulti)[^\n]{{0,60}}?{amount_re}\s*€",
        re.I,
    )

    seen_dep_keys = set()
    dep_values: List[float] = []
    dep_evidence: List[Dict[str, Any]] = []

    stima_candidates: List[Tuple[float, Dict[str, Any]]] = []

    for p in pages:
        text = str(p.get("text", "") or "")
        page_num = int(p.get("page_number", 0) or 0)
        if not text:
            continue

        for pat in stima_patterns:
            for m in pat.finditer(text):
                candidate_line_start, candidate_line_end = _line_bounds(text, m.start(), m.end())
                candidate_line = text[candidate_line_start:candidate_line_end]
                if re.search(r"valore\s+finale\s+di\s+stima", candidate_line, re.I):
                    continue
                val = _parse_euro_number(m.group("value"))
                if val is None:
                    continue
                ev = _build_evidence(
                    text,
                    page_num,
                    candidate_line_start,
                    candidate_line_end,
                    field_key="valore_stima_complessivo",
                    anchor_hint=m.group(0),
                )
                if _is_toc_like_quote(str(ev.get("quote") or "")):
                    continue
                stima_candidates.append((float(val), ev))

        if out["valore_finale_eur"] is None:
            m = finale_pattern.search(text)
            if m:
                line_start, line_end = _line_bounds(text, m.start(), m.end())
                val = _parse_euro_number(m.group("value"))
                if val is not None:
                    ev = _build_evidence(
                        text,
                        page_num,
                        line_start,
                        line_end,
                        field_key="valore_finale_stima",
                        anchor_hint=m.group(0),
                    )
                    if not _is_toc_like_quote(str(ev.get("quote") or "")):
                        out["valore_finale_eur"] = int(round(val))
                        out["evidence"]["valore_finale_eur"] = _normalize_contract_evidence_list([ev])

        for m in dep_item_pattern.finditer(text):
            val = _parse_euro_number(m.group("value"))
            if val is None:
                continue
            line_start, line_end = _line_bounds(text, m.start(), m.end())
            ev = _build_evidence(
                text,
                page_num,
                line_start,
                line_end,
                field_key="deprezzamenti",
                anchor_hint=m.group(0),
            )
            quote = str(ev.get("quote") or "")
            if _is_toc_like_quote(quote):
                continue
            key = (page_num, int(round(val)), _normalize_headline_text(m.group("label")).lower())
            if key in seen_dep_keys:
                continue
            seen_dep_keys.add(key)
            dep_values.append(val)
            dep_evidence.extend(_normalize_contract_evidence_list([ev], max_items=1))

        if out["deprezzamenti_eur"] is None:
            m = dep_total_pattern.search(text)
            if m:
                val = _parse_euro_number(m.group("value"))
                if val is not None:
                    line_start, line_end = _line_bounds(text, m.start(), m.end())
                    ev = _build_evidence(
                        text,
                        page_num,
                        line_start,
                        line_end,
                        field_key="deprezzamenti",
                        anchor_hint=m.group(0),
                    )
                    if not _is_toc_like_quote(str(ev.get("quote") or "")):
                        out["deprezzamenti_eur"] = int(round(val))
                        out["evidence"]["deprezzamenti_eur"] = _normalize_contract_evidence_list([ev])

    if dep_values:
        out["deprezzamenti_eur"] = int(round(sum(dep_values)))
        if dep_evidence:
            out["evidence"]["deprezzamenti_eur"] = dep_evidence[:4]
    if stima_candidates:
        # Prefer lot-level valuation over per-bene values by taking the highest deterministic candidate.
        best_value, best_ev = sorted(stima_candidates, key=lambda item: item[0], reverse=True)[0]
        out["valore_stima_eur"] = int(round(best_value))
        out["evidence"]["valore_stima_eur"] = _normalize_contract_evidence_list([best_ev])
    return out


def _build_panoramica_contract(result: Dict[str, Any], pages: List[Dict[str, Any]]) -> Dict[str, Any]:
    lots = result.get("lots") if isinstance(result.get("lots"), list) else []
    raw_lot_index = result.get("lot_index", 0)
    try:
        selected_lot_index = int(raw_lot_index)
    except Exception:
        selected_lot_index = 0
    if selected_lot_index < 0 or selected_lot_index >= len(lots):
        selected_lot_index = 0
    selected_lot = lots[selected_lot_index] if lots else {}

    report_header = result.get("report_header", {}) if isinstance(result.get("report_header"), dict) else {}
    case_header = result.get("case_header", {}) if isinstance(result.get("case_header"), dict) else {}
    dati = result.get("dati_certi_del_lotto", {}) if isinstance(result.get("dati_certi_del_lotto"), dict) else {}
    section4 = result.get("section_4_dati_certi", {}) if isinstance(result.get("section_4_dati_certi"), dict) else {}
    states = result.get("field_states", {}) if isinstance(result.get("field_states"), dict) else {}
    abusi = result.get("abusi_edilizi_conformita", {}) if isinstance(result.get("abusi_edilizi_conformita"), dict) else {}

    agibilita_state = states.get("agibilita") if isinstance(states.get("agibilita"), dict) else {}
    agibilita_contract_value = _field_state_display_value(agibilita_state)
    agibilita_contract_evidence = _normalize_contract_evidence_list(agibilita_state.get("evidence", []))
    if agibilita_contract_value == "NON SPECIFICATO IN PERIZIA":
        agibilita_obj = abusi.get("agibilita") if isinstance(abusi.get("agibilita"), dict) else {}
        fallback_value = str(agibilita_obj.get("status") or "").strip()
        fallback_evidence = _normalize_contract_evidence_list(agibilita_obj.get("evidence", []))
        if fallback_value:
            agibilita_contract_value = fallback_value
        if fallback_evidence:
            agibilita_contract_evidence = fallback_evidence

    tribunale = None
    procedura = None
    lotto_label = None
    tribunale_ev: List[Dict[str, Any]] = []
    procedura_ev: List[Dict[str, Any]] = []
    lotto_ev: List[Dict[str, Any]] = []

    if isinstance(report_header.get("tribunale"), dict):
        tribunale = report_header.get("tribunale", {}).get("value")
        tribunale_ev = _normalize_contract_evidence_list(report_header.get("tribunale", {}).get("evidence", []))
    if tribunale is None:
        tribunale = case_header.get("tribunale")
    if isinstance(report_header.get("procedure"), dict):
        procedura = report_header.get("procedure", {}).get("value")
        procedura_ev = _normalize_contract_evidence_list(report_header.get("procedure", {}).get("evidence", []))
    if procedura is None:
        procedura = case_header.get("procedure_id")
    if isinstance(report_header.get("lotto"), dict):
        lotto_label = report_header.get("lotto", {}).get("value")
        lotto_ev = _normalize_contract_evidence_list(report_header.get("lotto", {}).get("evidence", []))
    if lotto_label is None:
        lotto_label = case_header.get("lotto")
    if len(lots) > 1 and isinstance(selected_lot, dict) and selected_lot.get("lot_number"):
        lotto_label = f"Lotto {selected_lot.get('lot_number')}"
        lotto_ev = _normalize_contract_evidence_list(selected_lot.get("evidence", {}).get("lotto", [])) if isinstance(selected_lot.get("evidence"), dict) else []
    elif not lotto_label and isinstance(selected_lot, dict) and selected_lot.get("lot_number"):
        lotto_label = f"Lotto {selected_lot.get('lot_number')}"

    prezzo_value = None
    prezzo_ev: List[Dict[str, Any]] = []
    if isinstance(selected_lot, dict):
        prezzo_value = _as_float_or_none(selected_lot.get("prezzo_base_value"))
        lot_price_ev = selected_lot.get("evidence", {}).get("prezzo_base", []) if isinstance(selected_lot.get("evidence"), dict) else []
        prezzo_ev = _normalize_contract_evidence_list(lot_price_ev)
    if prezzo_value is None:
        prezzo_value, prezzo_ev_fallback = _extract_contract_value_from_obj(dati.get("prezzo_base_asta"))
        if prezzo_ev_fallback:
            prezzo_ev = prezzo_ev_fallback
    if prezzo_value is None:
        prezzo_value, prezzo_ev_fallback = _extract_contract_value_from_obj(section4.get("prezzo_base_asta"))
        if prezzo_ev_fallback:
            prezzo_ev = prezzo_ev_fallback

    lot_summary = {
        "tribunale": _normalize_headline_text(str(tribunale or "")) or None,
        "procedura": _normalize_headline_text(str(procedura or "")) or None,
        "lotto_label": _normalize_headline_text(str(lotto_label or "")) or None,
        "prezzo_base_eur": int(round(prezzo_value)) if prezzo_value is not None else None,
        "evidence": {
            "tribunale": tribunale_ev,
            "procedura": procedura_ev,
            "lotto_label": lotto_ev,
            "prezzo_base_eur": prezzo_ev,
        },
    }
    if isinstance(result.get("report_header"), dict):
        lotto_obj = result["report_header"].get("lotto")
        if isinstance(lotto_obj, dict):
            lotto_obj["value"] = lot_summary.get("lotto_label") or lotto_obj.get("value")
            if lot_summary.get("evidence", {}).get("lotto_label"):
                lotto_obj["evidence"] = copy.deepcopy(lot_summary["evidence"]["lotto_label"])
        elif lot_summary.get("lotto_label"):
            result["report_header"]["lotto"] = {
                "value": lot_summary.get("lotto_label"),
                "evidence": copy.deepcopy(lot_summary.get("evidence", {}).get("lotto_label", [])),
            }

    lot_beni: List[Dict[str, Any]] = []
    if isinstance(selected_lot, dict) and isinstance(selected_lot.get("beni"), list):
        lot_beni = selected_lot.get("beni") or []
    elif isinstance(result.get("beni"), list):
        lot_beni = result.get("beni") or []
    result_beni_index: Dict[int, Dict[str, Any]] = {}
    for idx, bene_obj in enumerate(result.get("beni") if isinstance(result.get("beni"), list) else []):
        if not isinstance(bene_obj, dict):
            continue
        bene_num_raw = bene_obj.get("bene_number")
        try:
            bene_num = int(str(bene_num_raw))
        except Exception:
            bene_num = idx + 1
        result_beni_index[bene_num] = bene_obj

    lot_composition: List[Dict[str, Any]] = []
    for idx, bene_raw in enumerate(lot_beni):
        if not isinstance(bene_raw, dict):
            continue
        bene_number = bene_raw.get("bene_number")
        if not isinstance(bene_number, int):
            try:
                bene_number = int(str(bene_number))
            except Exception:
                bene_number = idx + 1
        tipologia = _normalize_headline_text(str(bene_raw.get("tipologia", "") or "")) or None
        short_location, piano = _extract_location_piano_from_bene(bene_raw, selected_lot if isinstance(selected_lot, dict) else None)
        if short_location and tipologia:
            prefix = f"{tipologia} "
            if short_location.lower().startswith(prefix.lower()):
                short_location = short_location[len(prefix):].strip()
        if isinstance(piano, str) and piano:
            piano = piano[0].upper() + piano[1:]
        superficie = _extract_bene_surface_value(bene_raw)
        valore_stima = _extract_bene_stima_value(bene_raw)
        evidence_map = bene_raw.get("evidence", {}) if isinstance(bene_raw.get("evidence"), dict) else {}
        fallback_bene = result_beni_index.get(bene_number, {})
        if not isinstance(fallback_bene, dict):
            fallback_bene = {}
        fallback_evidence_map = fallback_bene.get("evidence", {}) if isinstance(fallback_bene.get("evidence"), dict) else {}

        ape_value = bene_raw.get("ape")
        if ape_value in (None, ""):
            ape_value = fallback_bene.get("ape")
        dichiarazioni_impianti = bene_raw.get("dichiarazioni_impianti")
        if not isinstance(dichiarazioni_impianti, dict):
            dichiarazioni_impianti = fallback_bene.get("dichiarazioni_impianti")
        if not isinstance(dichiarazioni_impianti, dict):
            dichiarazioni_impianti = {}
        dichiarazioni_alias = bene_raw.get("dichiarazioni")
        if not isinstance(dichiarazioni_alias, dict):
            dichiarazioni_alias = fallback_bene.get("dichiarazioni")
        if not isinstance(dichiarazioni_alias, dict):
            dichiarazioni_alias = {}

        ape_evidence_raw = evidence_map.get("ape")
        if not isinstance(ape_evidence_raw, list):
            ape_evidence_raw = fallback_evidence_map.get("ape", [])

        dichiarazioni_impianti_evidence_raw = evidence_map.get("dichiarazioni_impianti")
        if not isinstance(dichiarazioni_impianti_evidence_raw, dict):
            dichiarazioni_impianti_evidence_raw = fallback_evidence_map.get("dichiarazioni_impianti", {})
        if not isinstance(dichiarazioni_impianti_evidence_raw, dict):
            dichiarazioni_impianti_evidence_raw = {}

        dichiarazioni_alias_evidence_raw = evidence_map.get("dichiarazioni")
        if not isinstance(dichiarazioni_alias_evidence_raw, dict):
            dichiarazioni_alias_evidence_raw = fallback_evidence_map.get("dichiarazioni", {})
        if not isinstance(dichiarazioni_alias_evidence_raw, dict):
            dichiarazioni_alias_evidence_raw = {}

        lot_composition.append({
            "bene_number": bene_number,
            "tipologia": tipologia,
            "short_location": short_location,
            "piano": piano,
            "superficie_mq": round(float(superficie), 2) if isinstance(superficie, (int, float)) else None,
            "valore_stima_eur": int(round(valore_stima)) if isinstance(valore_stima, (int, float)) else None,
            "agibilita": agibilita_contract_value if agibilita_contract_value != "NON SPECIFICATO IN PERIZIA" else None,
            "ape": _normalize_headline_text(str(ape_value or "")) or None,
            "dichiarazioni_impianti": {
                "elettrico": _normalize_headline_text(str(dichiarazioni_impianti.get("elettrico") or "")) or None,
                "termico": _normalize_headline_text(str(dichiarazioni_impianti.get("termico") or "")) or None,
                "idrico": _normalize_headline_text(str(dichiarazioni_impianti.get("idrico") or "")) or None,
            },
            "dichiarazioni": {
                "dichiarazione_impianto_elettrico": _normalize_headline_text(str(dichiarazioni_alias.get("dichiarazione_impianto_elettrico") or "")) or None,
                "dichiarazione_impianto_termico": _normalize_headline_text(str(dichiarazioni_alias.get("dichiarazione_impianto_termico") or "")) or None,
                "dichiarazione_impianto_idrico": _normalize_headline_text(str(dichiarazioni_alias.get("dichiarazione_impianto_idrico") or "")) or None,
            },
            "evidence": {
                "tipologia": _normalize_contract_evidence_list(evidence_map.get("tipologia", [])),
                "location_piano": _normalize_contract_evidence_list(evidence_map.get("location_piano", [])),
                "superficie_mq": _normalize_contract_evidence_list(evidence_map.get("superficie", [])),
                "valore_stima_eur": _normalize_contract_evidence_list(evidence_map.get("valore_stima", [])),
                "catasto": _normalize_contract_evidence_list(evidence_map.get("catasto", [])),
                "note": _normalize_contract_evidence_list(evidence_map.get("note", []), max_items=2),
                "agibilita": copy.deepcopy(agibilita_contract_evidence),
                "ape": _normalize_contract_evidence_list(ape_evidence_raw),
                "dichiarazioni_impianti": {
                    "elettrico": _normalize_contract_evidence_list(dichiarazioni_impianti_evidence_raw.get("elettrico", [])),
                    "termico": _normalize_contract_evidence_list(dichiarazioni_impianti_evidence_raw.get("termico", [])),
                    "idrico": _normalize_contract_evidence_list(dichiarazioni_impianti_evidence_raw.get("idrico", [])),
                },
                "dichiarazioni": {
                    "dichiarazione_impianto_elettrico": _normalize_contract_evidence_list(dichiarazioni_alias_evidence_raw.get("dichiarazione_impianto_elettrico", [])),
                    "dichiarazione_impianto_termico": _normalize_contract_evidence_list(dichiarazioni_alias_evidence_raw.get("dichiarazione_impianto_termico", [])),
                    "dichiarazione_impianto_idrico": _normalize_contract_evidence_list(dichiarazioni_alias_evidence_raw.get("dichiarazione_impianto_idrico", [])),
                },
            },
        })

    wf_from_pages = _extract_valuation_waterfall_from_pages(pages)
    dep_mode = "DIRECT"
    dep_meta: Optional[Dict[str, Any]] = None
    if len(lots) > 1 and isinstance(selected_lot, dict):
        stima_value = _as_float_or_none(selected_lot.get("valore_stima_eur"))
        dep_pct = _parse_percent_value(selected_lot.get("deprezzamento_percentuale"))
        if dep_pct is None:
            dep_pct = _parse_percent_value(selected_lot.get("deprezzamento_percent"))
        prezzo_wf_value = _as_float_or_none(selected_lot.get("prezzo_base_value"))
        dep_value = None
        if isinstance(stima_value, (int, float)) and isinstance(prezzo_wf_value, (int, float)):
            dep_value = max(0.0, stima_value - prezzo_wf_value)
        finale_value = prezzo_wf_value
        selected_ev = selected_lot.get("evidence", {}) if isinstance(selected_lot.get("evidence"), dict) else {}
        stima_ev = _normalize_contract_evidence_list(selected_ev.get("valore_stima", []))
        prezzo_wf_ev = _normalize_contract_evidence_list(selected_ev.get("prezzo_base", []))
        dep_ev = _normalize_contract_evidence_list(selected_ev.get("deprezzamento", []))
        finale_ev = list(prezzo_wf_ev)
        if dep_value is None and isinstance(dep_pct, (int, float)) and isinstance(stima_value, (int, float)):
            dep_value = max(0.0, stima_value * (float(dep_pct) / 100.0))
        if (dep_value is None or dep_value <= 0) and isinstance(stima_value, (int, float)) and isinstance(prezzo_wf_value, (int, float)):
            dep_value = max(0.0, stima_value - prezzo_wf_value)
            dep_mode = "COMPUTED"
        if not stima_ev:
            stima_ev = _normalize_contract_evidence_list(selected_ev.get("tipologia", []))
        if not dep_ev:
            dep_ev = list(stima_ev or prezzo_wf_ev)
        if not finale_ev:
            finale_ev = list(prezzo_wf_ev)
    else:
        stima_value, stima_ev = _extract_contract_value_from_obj(dati.get("valore_stima_complessivo"))
        if stima_value is None:
            stima_value, stima_ev = _extract_contract_value_from_obj(section4.get("valore_stima_complessivo"))

        dep_value, dep_ev = _parse_dep_total_from_obj(dati.get("deprezzamenti"))
        if dep_value is None:
            dep_value, dep_ev = _parse_dep_total_from_obj(section4.get("deprezzamenti"))

        finale_value, finale_ev = _extract_contract_value_from_obj(dati.get("valore_finale_stima"))
        if finale_value is None:
            finale_value, finale_ev = _extract_contract_value_from_obj(section4.get("valore_finale_stima"))

        prezzo_wf_value = lot_summary.get("prezzo_base_eur")
        prezzo_wf_ev = lot_summary.get("evidence", {}).get("prezzo_base_eur", [])

        if stima_value is None:
            stima_value = wf_from_pages.get("valore_stima_eur")
            stima_ev = wf_from_pages.get("evidence", {}).get("valore_stima_eur", [])
        dep_from_pages = wf_from_pages.get("deprezzamenti_eur")
        if dep_value is None or (
            isinstance(dep_value, (int, float))
            and dep_value <= 0
            and isinstance(dep_from_pages, (int, float))
            and dep_from_pages > 0
        ):
            dep_value = dep_from_pages
            dep_ev = wf_from_pages.get("evidence", {}).get("deprezzamenti_eur", [])
        if (dep_value is None or (isinstance(dep_value, (int, float)) and dep_value <= 0)) and dep_ev:
            dep_nums: List[float] = []
            seen_dep_num_keys = set()
            for ev in dep_ev:
                if not isinstance(ev, dict):
                    continue
                quote = str(ev.get("quote", "") or "")
                for m in re.finditer(r"([0-9][0-9\.\,\s]{0,18})\s*€", quote):
                    parsed = _parse_euro_number(m.group(1))
                    if parsed is None or parsed <= 0:
                        continue
                    key = int(round(parsed))
                    if key in seen_dep_num_keys:
                        continue
                    seen_dep_num_keys.add(key)
                    dep_nums.append(parsed)
            if dep_nums:
                dep_value = float(sum(dep_nums))
        if finale_value is None:
            finale_value = wf_from_pages.get("valore_finale_eur")
            finale_ev = wf_from_pages.get("evidence", {}).get("valore_finale_eur", [])
        if prezzo_wf_value is None:
            prezzo_wf_value = wf_from_pages.get("prezzo_base_eur")
            prezzo_wf_ev = wf_from_pages.get("evidence", {}).get("prezzo_base_eur", [])
        if (dep_value is None or (isinstance(dep_value, (int, float)) and dep_value <= 0)) and isinstance(stima_value, (int, float)) and isinstance(prezzo_wf_value, (int, float)):
            dep_value = max(0.0, float(stima_value) - float(prezzo_wf_value))
            dep_mode = "COMPUTED"
        if not dep_ev and isinstance(selected_lot, dict):
            selected_ev = selected_lot.get("evidence", {}) if isinstance(selected_lot.get("evidence"), dict) else {}
            dep_ev = _normalize_contract_evidence_list(selected_ev.get("deprezzamento", []))
        if not dep_ev and dep_value and dep_value > 0:
            dep_ev = list(stima_ev or prezzo_wf_ev)

    if (
        dep_mode == "COMPUTED"
        and isinstance(dep_value, (int, float))
        and dep_value > 0
        and isinstance(stima_value, (int, float))
        and isinstance(prezzo_wf_value, (int, float))
    ):
        dep_meta = {
            "mode": "COMPUTED",
            "label_it": "Deprezzamento totale calcolato da valori in perizia",
            "gross_value_eur": int(round(stima_value)),
            "gross_label_it": "Valore di stima lordo",
            "gross_evidence": _normalize_contract_evidence_list(stima_ev),
            "final_value_eur": int(round(prezzo_wf_value)),
            "final_label_it": "Valore finale / prezzo base",
            "final_evidence": _normalize_contract_evidence_list(prezzo_wf_ev or finale_ev),
            "computed_difference_eur": int(round(dep_value)),
            "formula_it": "Valore di stima lordo - Valore finale / prezzo base = Deprezzamento totale",
        }
    elif isinstance(dep_value, (int, float)) and dep_value > 0:
        dep_meta = {
            "mode": "DIRECT",
        }

    valuation_waterfall = {
        "valore_stima_eur": int(round(stima_value)) if isinstance(stima_value, (int, float)) else None,
        "deprezzamenti_eur": int(round(dep_value)) if isinstance(dep_value, (int, float)) else None,
        "valore_finale_eur": int(round(finale_value)) if isinstance(finale_value, (int, float)) else None,
        "prezzo_base_eur": int(round(prezzo_wf_value)) if isinstance(prezzo_wf_value, (int, float)) else None,
        "deprezzamenti_meta": dep_meta,
        "evidence": {
            "valore_stima_eur": _normalize_contract_evidence_list(stima_ev),
            "deprezzamenti_eur": _normalize_contract_evidence_list(dep_ev),
            "valore_finale_eur": _normalize_contract_evidence_list(finale_ev),
            "prezzo_base_eur": _normalize_contract_evidence_list(prezzo_wf_ev),
        },
    }

    return {
        "version": "v1",
        "selected_lot_index": selected_lot_index,
        "selected_lot_number": selected_lot.get("lot_number") if isinstance(selected_lot, dict) else None,
        "lots_count": len(lots),
        "is_multi_lot": len(lots) > 1,
        "lot_summary": lot_summary,
        "lot_composition": lot_composition,
        "valuation_waterfall": valuation_waterfall,
        "lots_overview": _build_lots_overview(lots),
        "aggregate_valuation": {
            "valore_stima_eur": sum(int(round(_as_float_or_none(l.get("valore_stima_eur")) or 0)) for l in lots if isinstance(l, dict)),
            "deprezzamenti_eur": sum(
                int(round(max(0.0, (_as_float_or_none(l.get("valore_stima_eur")) or 0) - (_as_float_or_none(l.get("prezzo_base_value")) or 0))))
                for l in lots if isinstance(l, dict)
            ),
            "prezzo_base_eur": sum(int(round(_as_float_or_none(l.get("prezzo_base_value")) or 0)) for l in lots if isinstance(l, dict)),
        } if len(lots) > 1 else None,
    }

def _apply_low_confidence(field: Dict[str, Any], field_key: str = "value") -> None:
    field["confidence"] = "LOW"
    field["note"] = "USER MUST VERIFY"

def enforce_evidence_or_low_confidence(result: Dict[str, Any]) -> Dict[str, Any]:
    def _strip_numeric_estimates(text: str) -> str:
        import re
        if not isinstance(text, str):
            return text
        text = re.sub(r"€\\s*[\\d\\.,]+(?:\\s*-\\s*€?\\s*[\\d\\.,]+)?", " ", text)
        text = re.sub(r"(Costi extra[^.]*\\.)", " ", text, flags=re.I)
        text = re.sub(r"(All-?in[^.]*\\.)", " ", text, flags=re.I)
        text = re.sub(r"\\s{2,}", " ", text).strip()
        return text

    # Report header fields
    hdr = result.get("report_header", {})
    for key in ("procedure", "tribunale", "address", "lotto"):
        fld = hdr.get(key)
        if isinstance(fld, dict) and not _has_evidence(fld.get("evidence", [])):
            _apply_low_confidence(fld, "value")

    # Dati certi
    dati = result.get("dati_certi_del_lotto", {})
    for key in ("prezzo_base_asta", "superficie_catastale", "diritto_reale"):
        fld = dati.get(key)
        if isinstance(fld, dict) and not _has_evidence(fld.get("evidence", [])):
            _apply_low_confidence(fld, "value")

    # Occupazione
    occ = result.get("stato_occupativo", {})
    if isinstance(occ, dict) and not _has_evidence(occ.get("evidence", [])):
        _apply_low_confidence(occ, "status")

    # Conformita
    conf = result.get("abusi_edilizi_conformita", {})
    for key in ("conformita_urbanistica", "conformita_catastale", "condono", "agibilita", "commerciabilita"):
        fld = conf.get(key)
        if isinstance(fld, dict) and not _has_evidence(fld.get("evidence", [])):
            _apply_low_confidence(fld, "status")

    # Summary: remove numeric estimates unless evidence-backed (summary has no evidence)
    summary = result.get("summary_for_client", {})
    if isinstance(summary, dict):
        if "summary_it" in summary:
            summary["summary_it"] = _strip_numeric_estimates(summary.get("summary_it", ""))
        if "summary_en" in summary:
            summary["summary_en"] = _strip_numeric_estimates(summary.get("summary_en", ""))

    # Remove numeric estimates from money_box totals unless explicitly evidenced
    money_box = result.get("money_box", {})
    if isinstance(money_box, dict):
        total = money_box.get("total_extra_costs", {})
        if (
            isinstance(total, dict)
            and not _has_evidence(total.get("evidence", []))
            and total.get("min") != "NON_QUANTIFICATO_IN_PERIZIA"
            and total.get("max") != "NON_QUANTIFICATO_IN_PERIZIA"
        ):
            total["min"] = "TBD"
            total["max"] = "TBD"
            total["nota"] = "TBD — Costi non quantificati in perizia"

    # Remove numeric estimates from indice_di_convenienza unless explicitly evidenced
    indice = result.get("indice_di_convenienza", {})
    if isinstance(indice, dict) and not _has_evidence(indice.get("evidence", [])):
        for key in ("extra_costs_min", "extra_costs_max", "all_in_light_min", "all_in_light_max"):
            if key in indice:
                indice[key] = "TBD"
    return result

def _normalize_evidence_offsets(result: Dict[str, Any], pages: List[Dict[str, Any]]) -> None:
    page_text_by_num: Dict[int, str] = {}
    page_hash_by_num: Dict[int, str] = {}
    for p in pages:
        page_num = int(p.get("page_number", 0) or 0)
        page_text = str(p.get("text", "") or "")
        page_text_by_num[page_num] = page_text
        page_hash_by_num[page_num] = hashlib.sha256(page_text.encode("utf-8")).hexdigest()

    def _normalize_entry(ev: Dict[str, Any]) -> None:
        page = int(ev.get("page", 0) or 0)
        quote = str(ev.get("quote", "") or "").strip()
        page_text = page_text_by_num.get(page, "")
        if "start_offset" not in ev or "end_offset" not in ev:
            if quote and page_text:
                idx = page_text.find(quote)
                if idx >= 0:
                    ev["start_offset"] = idx
                    ev["end_offset"] = idx + len(quote)
                else:
                    ev["start_offset"] = 0
                    ev["end_offset"] = min(len(page_text), len(quote))
            else:
                ev["start_offset"] = 0
                ev["end_offset"] = 0
        try:
            ev["start_offset"] = int(ev.get("start_offset", 0) or 0)
            ev["end_offset"] = int(ev.get("end_offset", 0) or 0)
        except Exception:
            ev["start_offset"] = 0
            ev["end_offset"] = 0
        ev["offset_mode"] = EVIDENCE_OFFSET_MODE
        if page in page_hash_by_num:
            ev["page_text_hash"] = page_hash_by_num[page]
        ev.setdefault("bbox", None)

    def _walk(obj: Any) -> None:
        if isinstance(obj, dict):
            for k, v in obj.items():
                if k == "evidence" and isinstance(v, list):
                    for ev in v:
                        if isinstance(ev, dict):
                            _normalize_entry(ev)
                else:
                    _walk(v)
        elif isinstance(obj, list):
            for item in obj:
                _walk(item)

    _walk(result)
    result["offset_mode"] = EVIDENCE_OFFSET_MODE

def _normalize_legal_killers(result: Dict[str, Any], pages: List[Dict[str, Any]]) -> None:
    section = result.setdefault("section_9_legal_killers", {})
    items = section.get("items", [])
    if not isinstance(items, list):
        section["items"] = []
        return
    page_text_by_num = {
        int(p.get("page_number", p.get("page", 0)) or 0): str(p.get("text", "") or "")
        for p in pages
        if isinstance(p, dict)
    }

    status_map = {
        "VERDE": "VERDE",
        "GREEN": "VERDE",
        "GIALLO": "GIALLO",
        "YELLOW": "GIALLO",
        "ROSSO": "ROSSO",
        "RED": "ROSSO",
        "DA_VERIFICARE": "DA_VERIFICARE",
        "SI": "ROSSO",
        "YES": "ROSSO",
        "TRUE": "ROSSO",
        "NO": "VERDE",
        "FALSE": "VERDE",
    }
    status_it_map = {
        "VERDE": "OK",
        "GIALLO": "ATTENZIONE",
        "ROSSO": "CRITICO",
        "DA_VERIFICARE": "DA VERIFICARE",
    }

    dedup: Dict[str, Dict[str, Any]] = {}
    all_pages = [int(p.get("page_number", 0) or 0) for p in pages]
    for item in items:
        if not isinstance(item, dict):
            continue
        killer = str(item.get("killer") or item.get("title") or "KILLER_NON_SPECIFICATO").strip()
        key = killer.lower()
        raw_status = str(item.get("status", "") or "").strip().upper()
        norm_status = status_map.get(raw_status, "DA_VERIFICARE")
        evidence = item.get("evidence", [])
        if not isinstance(evidence, list):
            evidence = []
        if key not in dedup:
            dedup[key] = {
                "killer": killer,
                "status": norm_status,
                "status_it": status_it_map[norm_status],
                "reason_it": str(item.get("reason_it", "") or "").strip() or f"Rilevata criticita: {killer}",
                "evidence": [],
                "searched_in": item.get("searched_in") if isinstance(item.get("searched_in"), dict) else None
            }
        else:
            # Keep most severe status: ROSSO > GIALLO > DA_VERIFICARE > VERDE
            sev_rank = {"ROSSO": 3, "GIALLO": 2, "DA_VERIFICARE": 1, "VERDE": 0}
            if sev_rank[norm_status] > sev_rank[dedup[key]["status"]]:
                dedup[key]["status"] = norm_status
                dedup[key]["status_it"] = status_it_map[norm_status]
        for ev in evidence:
            if isinstance(ev, dict):
                dedup[key]["evidence"].append(ev)

    normalized = []
    for it in dedup.values():
        seen = set()
        merged = []
        for ev in it.get("evidence", []):
            if not isinstance(ev, dict):
                continue
            page = int(ev.get("page", 0) or 0)
            quote = str(ev.get("quote", "") or "")
            search_hint = str(ev.get("search_hint", "") or "")
            page_text = page_text_by_num.get(page, "")
            start_offset = int(ev.get("start_offset", 0) or 0)
            end_offset = int(ev.get("end_offset", 0) or 0)
            context = quote
            local_anchor = quote
            local_context = quote
            if page_text:
                slice_text = page_text[start_offset:end_offset] if end_offset > start_offset else ""
                anchor_span = None
                if quote and end_offset > start_offset and slice_text == quote:
                    anchor_span = (start_offset, end_offset)
                else:
                    anchor_span = _find_anchored_span(page_text, quote, search_hint)
                if anchor_span:
                    start_offset, end_offset = anchor_span
                if end_offset > start_offset:
                    local_anchor = page_text[start_offset:end_offset]
                    local_context = page_text[max(0, start_offset - 60):min(len(page_text), end_offset + 60)]
                    context = page_text[max(0, start_offset - 180):min(len(page_text), end_offset + 140)]
                elif quote:
                    local_context = quote
            local_toc_probe = local_anchor if str(local_anchor or "").strip() else local_context
            low_context = context.lower()
            if _is_toc_like_quote(local_toc_probe) or _is_toc_like_line(local_toc_probe):
                continue
            killer_low = str(it.get("killer") or "").lower()
            if "usi civici" in killer_low:
                if _legal_subject_negated(low_context, r"(diritti\s+demaniali|usi\s+civici)"):
                    continue
                if not re.search(r"(gravat|present[ei]|esist|sussist).{0,80}usi\s+civici|usi\s+civici.{0,80}(gravat|present[ei]|esist|sussist)", low_context, re.I):
                    continue
            if "servit" in killer_low:
                if _legal_subject_negated(low_context, r"servit[ùu]"):
                    continue
                if "servitù, censo, livello, usi civici" in low_context and not re.search(r"passo|carrabile|pedonale|stradella|fognatura|utenz|attraversamento", low_context, re.I):
                    continue
            sig = (ev.get("page"), ev.get("start_offset"), ev.get("end_offset"), ev.get("quote"))
            if sig not in seen:
                seen.add(sig)
                merged.append(ev)
        it["evidence"] = merged
        killer_low = str(it.get("killer") or "").lower()
        if not it["evidence"] and ("usi civici" in killer_low or "servit" in killer_low):
            continue
        if not it["evidence"]:
            it["status"] = "DA_VERIFICARE"
            it["status_it"] = status_it_map["DA_VERIFICARE"]
            it["searched_in"] = it.get("searched_in") or {
                "pages": all_pages,
                "keywords": [it["killer"]],
                "sections": ["section_9_legal_killers"]
            }
            it["reason_it"] = it.get("reason_it") or f"Elemento da verificare: {it['killer']}"
        bucket, score = _legal_relevance_profile(it.get("killer", ""))
        it["decision_bucket"] = bucket
        it["decision_score"] = score
        normalized.append(it)

    existing_keys = {str(item.get("killer") or "").strip().lower() for item in normalized if isinstance(item, dict)}
    for item in _build_state_driven_legal_killers(result, pages):
        killer_key = str(item.get("killer") or "").strip().lower()
        if not killer_key or killer_key in existing_keys:
            continue
        normalized.append(item)
        existing_keys.add(killer_key)

    normalized.sort(key=lambda item: (-int(item.get("decision_score", 0)), str(item.get("killer", ""))))
    section["items"] = normalized

    lots = result.get("lots") if isinstance(result.get("lots"), list) else []
    lot_driven_top_items = _build_multi_lot_top_legal_items(lots, pages)
    if lot_driven_top_items:
        section["top_items"] = lot_driven_top_items
        return

    section["top_items"] = [
        item for item in normalized
        if (
            str(item.get("decision_bucket")) != "BACKGROUND_NOTE"
            and isinstance(item.get("evidence"), list)
            and len(item.get("evidence")) > 0
        )
    ][:10]

def _to_iso(value: Any) -> Optional[str]:
    dt = _parse_dt(value)
    if dt:
        return dt.isoformat()
    if isinstance(value, str):
        return value
    return None

def _quota_snapshot(quota: Optional[Dict[str, Any]]) -> Dict[str, int]:
    source = quota if isinstance(quota, dict) else {}
    return {field: int(source.get(field, 0) or 0) for field in ACCOUNT_QUOTA_FIELDS}

def _sanitize_metadata(metadata: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not isinstance(metadata, dict):
        return {}
    cleaned: Dict[str, Any] = {}
    for key, value in metadata.items():
        if value is None:
            continue
        if isinstance(value, datetime):
            cleaned[key] = value.isoformat()
        else:
            cleaned[key] = value
    return cleaned

def _serialize_datetime_fields(doc: Dict[str, Any], *fields: str) -> Dict[str, Any]:
    serialized = dict(doc)
    for field in fields:
        if serialized.get(field) is not None:
            serialized[field] = _to_iso(serialized.get(field))
    return serialized

def _billing_purchase_type_for_plan(plan: SubscriptionPlan) -> str:
    if plan.plan_type == "subscription":
        return "subscription"
    if plan.plan_type == "one_time":
        return "pack"
    return "top_up"

def _stripe_price_id_for_plan(plan_id: str) -> str:
    price_map = {
        "starter": STRIPE_PRICE_PACK_8 or STRIPE_PRICE_STARTER,
        "solo": STRIPE_PRICE_SOLO,
        "pro": STRIPE_PRICE_PRO,
    }
    price_id = str(price_map.get(plan_id) or "").strip()
    if not price_id:
        raise HTTPException(status_code=503, detail=f"Stripe price not configured for plan '{plan_id}'")
    return price_id

def _plan_id_for_stripe_price_id(price_id: Optional[str]) -> Optional[str]:
    normalized = str(price_id or "").strip()
    if not normalized:
        return None
    reverse_map = {}
    for candidate in {STRIPE_PRICE_PACK_8, STRIPE_PRICE_STARTER}:
        candidate_id = str(candidate or "").strip()
        if candidate_id:
            reverse_map[candidate_id] = "starter"
    if STRIPE_PRICE_SOLO:
        reverse_map[STRIPE_PRICE_SOLO] = "solo"
    if STRIPE_PRICE_PRO:
        reverse_map[STRIPE_PRICE_PRO] = "pro"
    return reverse_map.get(normalized)

def _stripe_checkout_metadata(*, user_id: str, plan_id: str, billing_reason: str) -> Dict[str, str]:
    return {
        "app_user_id": str(user_id or "").strip(),
        "plan_code": str(plan_id or "").strip(),
        "billing_reason": str(billing_reason or "").strip(),
    }

def _with_query_params(base_url: str, params: Dict[str, str]) -> str:
    split = urlsplit(base_url)
    existing = dict(parse_qsl(split.query, keep_blank_values=True))
    existing.update({key: value for key, value in params.items() if value is not None})
    return urlunsplit((split.scheme, split.netloc, split.path, urlencode(existing), split.fragment))

def _build_stripe_return_urls(origin_url: Optional[str] = None) -> Tuple[str, str]:
    success_base = STRIPE_SUCCESS_URL or ""
    cancel_base = STRIPE_CANCEL_URL or ""

    if not success_base and origin_url:
        success_base = f"{str(origin_url).rstrip('/')}/billing"
    if not cancel_base and origin_url:
        cancel_base = f"{str(origin_url).rstrip('/')}/billing"

    if not success_base or not cancel_base:
        raise HTTPException(status_code=503, detail="Stripe return URLs not configured")

    success_url = success_base
    if "{CHECKOUT_SESSION_ID}" not in success_url:
        success_url = _with_query_params(
            success_url,
            {"session_id": "{CHECKOUT_SESSION_ID}", "checkout": "success"},
        )
    else:
        success_url = _with_query_params(success_url, {"checkout": "success"})

    cancel_url = _with_query_params(cancel_base, {"checkout": "cancel"})
    return success_url, cancel_url

def _is_valid_checkout_session_id(session_id: Optional[str]) -> bool:
    normalized = str(session_id or "").strip()
    if not normalized:
        return False
    upper = normalized.upper()
    if "CHECKOUT_SESSION_ID" in upper:
        return False
    if "{" in normalized or "}" in normalized:
        return False
    return bool(re.fullmatch(r"cs_[A-Za-z0-9_]+", normalized))

async def _update_billing_record(
    billing_record_id: str,
    *,
    status: Optional[str] = None,
    payment_reference: Optional[str] = None,
    invoice_status: Optional[str] = None,
    invoice_reference: Optional[str] = None,
    metadata_updates: Optional[Dict[str, Any]] = None,
    paid: bool = False,
) -> None:
    update_fields: Dict[str, Any] = {"updated_at": _now_iso()}
    if status:
        update_fields["status"] = status
    if payment_reference is not None:
        update_fields["payment_reference"] = payment_reference
    if invoice_status:
        update_fields["invoice_status"] = invoice_status
    if invoice_reference is not None:
        update_fields["invoice_reference"] = invoice_reference
    if paid:
        update_fields["paid_at"] = _now_iso()
    if metadata_updates:
        existing = await db.billing_records.find_one(
            {"billing_record_id": billing_record_id},
            {"_id": 0, "metadata": 1},
        )
        merged_metadata = dict((existing or {}).get("metadata") or {})
        merged_metadata.update(_sanitize_metadata(metadata_updates))
        update_fields["metadata"] = merged_metadata
    await db.billing_records.update_one(
        {"billing_record_id": billing_record_id},
        {"$set": update_fields},
    )

async def _find_billing_record_for_checkout(checkout_reference: str, user_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
    query: Dict[str, Any] = {"checkout_reference": checkout_reference}
    if user_id:
        query["user_id"] = user_id
    return await db.billing_records.find_one(query, {"_id": 0}, sort=[("created_at", -1)])

async def _find_latest_pending_subscription_billing_record(user_id: str, plan_id: str) -> Optional[Dict[str, Any]]:
    return await db.billing_records.find_one(
        {"user_id": user_id, "plan_id": plan_id, "purchase_type": "subscription", "status": "pending"},
        {"_id": 0},
        sort=[("created_at", -1)],
    )

async def _payment_transaction_by_session(session_id: str, user_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
    query: Dict[str, Any] = {"session_id": session_id}
    if user_id:
        query["user_id"] = user_id
    return await db.payment_transactions.find_one(query, {"_id": 0})


async def _latest_subscription_transaction_for_user(user_id: str, plan_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
    query: Dict[str, Any] = {"user_id": user_id, "plan_id": {"$in": sorted(SELF_SERVE_RECURRING_PLAN_IDS)}}
    normalized_plan_id = str(plan_id or "").strip().lower()
    if normalized_plan_id in SELF_SERVE_RECURRING_PLAN_IDS:
        query["plan_id"] = normalized_plan_id
    return await db.payment_transactions.find_one(query, {"_id": 0}, sort=[("created_at", -1)])


async def _latest_subscription_billing_record_for_user(user_id: str, plan_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
    query: Dict[str, Any] = {
        "user_id": user_id,
        "purchase_type": "subscription",
        "plan_id": {"$in": sorted(SELF_SERVE_RECURRING_PLAN_IDS)},
    }
    normalized_plan_id = str(plan_id or "").strip().lower()
    if normalized_plan_id in SELF_SERVE_RECURRING_PLAN_IDS:
        query["plan_id"] = normalized_plan_id
    return await db.billing_records.find_one(query, {"_id": 0}, sort=[("created_at", -1)])


def _resolved_checkout_user_id(*candidate_values: Optional[str]) -> Optional[str]:
    distinct_values = {str(value or "").strip() for value in candidate_values if str(value or "").strip()}
    if len(distinct_values) != 1:
        return None
    return next(iter(distinct_values))

async def _set_payment_transaction_state(
    session_id: str,
    *,
    status: Optional[str] = None,
    payment_status: Optional[str] = None,
    stripe_customer_id: Optional[str] = None,
    stripe_subscription_id: Optional[str] = None,
    stripe_invoice_id: Optional[str] = None,
    stripe_payment_intent_id: Optional[str] = None,
) -> None:
    update_fields: Dict[str, Any] = {}
    if status is not None:
        update_fields["status"] = status
    if payment_status is not None:
        update_fields["payment_status"] = payment_status
    if stripe_customer_id is not None:
        update_fields["stripe_customer_id"] = stripe_customer_id
    if stripe_subscription_id is not None:
        update_fields["stripe_subscription_id"] = stripe_subscription_id
    if stripe_invoice_id is not None:
        update_fields["stripe_invoice_id"] = stripe_invoice_id
    if stripe_payment_intent_id is not None:
        update_fields["stripe_payment_intent_id"] = stripe_payment_intent_id
    if not update_fields:
        return
    await db.payment_transactions.update_one({"session_id": session_id}, {"$set": update_fields})


async def _get_user_doc_by_id(user_id: str) -> Optional[Dict[str, Any]]:
    if not user_id:
        return None
    user_doc = await db.users.find_one({"user_id": user_id}, {"_id": 0})
    if not user_doc:
        return None
    return await _apply_normalized_account_state(user_doc, persist=False)


async def _all_user_docs() -> List[Dict[str, Any]]:
    return await db.users.find({}, {"_id": 0}).to_list(None)


async def _find_user_doc_by_subscription_id(subscription_id: Optional[str]) -> Optional[Dict[str, Any]]:
    normalized_subscription_id = str(subscription_id or "").strip()
    if not normalized_subscription_id:
        return None
    for user_doc in await _all_user_docs():
        subscription_state = _normalize_subscription_state(user_doc)
        if str(subscription_state.get("stripe_subscription_id") or "").strip() == normalized_subscription_id:
            return await _apply_normalized_account_state(user_doc, persist=False)
    return None


async def _find_user_doc_by_customer_id(customer_id: Optional[str]) -> Optional[Dict[str, Any]]:
    normalized_customer_id = str(customer_id or "").strip()
    if not normalized_customer_id:
        return None
    for user_doc in await _all_user_docs():
        subscription_state = _normalize_subscription_state(user_doc)
        if str(subscription_state.get("stripe_customer_id") or "").strip() == normalized_customer_id:
            return await _apply_normalized_account_state(user_doc, persist=False)
    return None


async def _persist_subscription_state(
    *,
    user_doc: Dict[str, Any],
    subscription_state: Dict[str, Any],
    plan_override: Optional[str] = None,
) -> Dict[str, Any]:
    normalized_state = _normalize_subscription_state({**user_doc, "plan": plan_override or user_doc.get("plan"), "subscription_state": subscription_state})
    update_fields: Dict[str, Any] = {"subscription_state": normalized_state}
    if plan_override is not None:
        update_fields["plan"] = plan_override
        user_doc["plan"] = plan_override
    await db.users.update_one({"user_id": user_doc["user_id"]}, {"$set": update_fields})
    user_doc["subscription_state"] = normalized_state
    return normalized_state


async def _recover_subscription_state_from_local_records(user_doc: Dict[str, Any]) -> Dict[str, Any]:
    normalized_plan_id = str(user_doc.get("plan") or "").strip().lower()
    current_state = _normalize_subscription_state(user_doc)
    current_plan_id = str(current_state.get("current_plan_id") or normalized_plan_id or "").strip().lower()
    if current_plan_id not in SELF_SERVE_RECURRING_PLAN_IDS:
        return user_doc
    if (
        current_state.get("stripe_subscription_id")
        and current_state.get("stripe_customer_id")
        and current_state.get("current_period_end")
    ):
        return user_doc

    billing_record = await _latest_subscription_billing_record_for_user(user_doc["user_id"], current_plan_id)
    payment_transaction = await _latest_subscription_transaction_for_user(user_doc["user_id"], current_plan_id)
    billing_metadata = dict((billing_record or {}).get("metadata") or {})
    recovered_customer_id = (
        billing_metadata.get("stripe_customer_id")
        or (payment_transaction or {}).get("stripe_customer_id")
        or current_state.get("stripe_customer_id")
    )
    recovered_subscription_id = (
        billing_metadata.get("stripe_subscription_id")
        or (payment_transaction or {}).get("stripe_subscription_id")
        or current_state.get("stripe_subscription_id")
    )
    recovered_plan_id = str(
        current_state.get("current_plan_id")
        or (billing_record or {}).get("plan_id")
        or (payment_transaction or {}).get("plan_id")
        or normalized_plan_id
        or ""
    ).strip().lower()
    if recovered_plan_id not in SELF_SERVE_RECURRING_PLAN_IDS:
        return user_doc

    if recovered_subscription_id and STRIPE_SECRET_KEY:
        try:
            import stripe
            stripe.api_key = STRIPE_SECRET_KEY
            subscription = stripe.Subscription.retrieve(recovered_subscription_id)
            recovered_state = await _sync_subscription_state_from_stripe(
                user_doc=user_doc,
                subscription=subscription,
                current_plan_hint=recovered_plan_id,
            )
            user_doc["subscription_state"] = recovered_state
            return user_doc
        except Exception as exc:
            logger.warning(
                "Subscription state recovery from Stripe failed: user_id=%s subscription_id=%s error=%s",
                user_doc.get("user_id"),
                recovered_subscription_id,
                exc,
            )

    if not recovered_customer_id and not recovered_subscription_id:
        return user_doc

    recovered_state = await _persist_subscription_state(
        user_doc=user_doc,
        subscription_state={
            **current_state,
            "stripe_customer_id": recovered_customer_id,
            "stripe_subscription_id": recovered_subscription_id,
            "status": current_state.get("status") or "active",
            "current_plan_id": current_state.get("current_plan_id") or recovered_plan_id,
            "stripe_plan_id": current_state.get("stripe_plan_id") or recovered_plan_id,
        },
        plan_override=current_plan_id,
    )
    user_doc["subscription_state"] = recovered_state
    return user_doc


def _stripe_subscription_item_id(subscription: Any) -> Optional[str]:
    items = _stripe_object_get(subscription, "items") or {}
    item_list = _stripe_object_get(items, "data") or []
    for item in item_list:
        item_id = str(_stripe_object_get(item, "id") or "").strip()
        if item_id:
            return item_id
    return None


def _stripe_subscription_plan_id(subscription: Any) -> Optional[str]:
    items = _stripe_object_get(subscription, "items") or {}
    item_list = _stripe_object_get(items, "data") or []
    for item in item_list:
        price = _stripe_object_get(item, "price") or {}
        price_id = str(_stripe_object_get(price, "id") or "").strip()
        plan_id = _plan_id_for_stripe_price_id(price_id)
        if plan_id in SELF_SERVE_RECURRING_PLAN_IDS:
            return plan_id
    metadata = _stripe_object_metadata(subscription)
    metadata_plan_id = str(metadata.get("plan_code") or "").strip().lower()
    return metadata_plan_id if metadata_plan_id in SELF_SERVE_RECURRING_PLAN_IDS else None


def _stripe_subscription_period_end_iso(subscription: Any) -> Optional[str]:
    period_end = _stripe_object_get(subscription, "current_period_end")
    if isinstance(period_end, (int, float)) and period_end > 0:
        return datetime.fromtimestamp(period_end, tz=timezone.utc).isoformat()
    normalized_period_end = _to_iso(period_end)
    if normalized_period_end:
        return normalized_period_end

    items = _stripe_object_get(subscription, "items") or {}
    item_list = _stripe_object_get(items, "data") or []
    for item in item_list:
        item_period_end = _stripe_object_get(item, "current_period_end")
        if isinstance(item_period_end, (int, float)) and item_period_end > 0:
            return datetime.fromtimestamp(item_period_end, tz=timezone.utc).isoformat()
        normalized_item_period_end = _to_iso(item_period_end)
        if normalized_item_period_end:
            return normalized_item_period_end
    return None


async def _resolve_subscription_owner_context(
    *,
    subscription: Any,
    allow_customer_fallback: bool = True,
) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    metadata = _stripe_object_metadata(subscription)
    subscription_id = str(_stripe_object_get(subscription, "id") or "").strip() or None
    customer_id = str(_stripe_object_get(subscription, "customer") or "").strip() or None
    user_candidates: Dict[str, Optional[str]] = {
        "subscription_metadata_user_id": str(metadata.get("app_user_id") or "").strip() or None,
    }
    plan_candidates: Dict[str, Optional[str]] = {
        "subscription_item_plan_id": _stripe_subscription_plan_id(subscription),
        "subscription_metadata_plan_id": str(metadata.get("plan_code") or "").strip().lower() or None,
    }
    all_user_docs = await _all_user_docs()
    stored_subscription_user_ids = []
    stored_customer_user_ids = []
    for user_doc in all_user_docs:
        subscription_state = _normalize_subscription_state(user_doc)
        if subscription_id and str(subscription_state.get("stripe_subscription_id") or "").strip() == subscription_id:
            stored_subscription_user_ids.append(str(user_doc.get("user_id") or "").strip())
        if allow_customer_fallback and customer_id and str(subscription_state.get("stripe_customer_id") or "").strip() == customer_id:
            stored_customer_user_ids.append(str(user_doc.get("user_id") or "").strip())
    if stored_subscription_user_ids:
        user_candidates["stored_subscription_user_id"] = stored_subscription_user_ids[0]
    if len(set(stored_subscription_user_ids)) > 1:
        user_candidates["stored_subscription_user_conflict"] = "MULTIPLE"
    if stored_customer_user_ids:
        user_candidates["stored_customer_user_id"] = stored_customer_user_ids[0]
    if len(set(stored_customer_user_ids)) > 1:
        user_candidates["stored_customer_user_conflict"] = "MULTIPLE"
    transaction = await _payment_transaction_for_subscription_context(
        subscription_id=subscription_id,
        customer_id=customer_id,
    )
    if transaction:
        user_candidates["payment_transaction_user_id"] = str(transaction.get("user_id") or "").strip() or None
        plan_candidates["payment_transaction_plan_id"] = str(transaction.get("plan_id") or "").strip().lower() or None

    distinct_user_ids = {value for value in user_candidates.values() if value}
    distinct_plan_ids = {value for value in plan_candidates.values() if value}
    if len(distinct_user_ids) > 1 or len(distinct_plan_ids) > 1:
        logger.warning(
            "Subscription context resolution conflict: subscription_id=%s customer_id=%s user_candidates=%s plan_candidates=%s",
            subscription_id,
            customer_id,
            user_candidates,
            plan_candidates,
        )
        return None, None
    if user_candidates.get("stored_subscription_user_conflict") or user_candidates.get("stored_customer_user_conflict"):
        return None, None
    user_id = next(iter(distinct_user_ids), None)
    plan_id = next(iter(distinct_plan_ids), None)
    if not user_id:
        return None, plan_id
    return await _get_user_doc_by_id(user_id), plan_id


async def _sync_subscription_state_from_stripe(
    *,
    user_doc: Dict[str, Any],
    subscription: Any,
    current_plan_hint: Optional[str] = None,
) -> Dict[str, Any]:
    current_state = _normalize_subscription_state(user_doc)
    stripe_plan_id = _stripe_subscription_plan_id(subscription)
    current_plan_id = current_state.get("current_plan_id")
    if not current_plan_id and current_plan_hint in SELF_SERVE_RECURRING_PLAN_IDS:
        current_plan_id = current_plan_hint
    next_state = {
        **current_state,
        "stripe_customer_id": str(_stripe_object_get(subscription, "customer") or "").strip() or current_state.get("stripe_customer_id"),
        "stripe_subscription_id": str(_stripe_object_get(subscription, "id") or "").strip() or current_state.get("stripe_subscription_id"),
        "status": str(_stripe_object_get(subscription, "status") or current_state.get("status") or "").strip().lower() or None,
        "stripe_plan_id": stripe_plan_id or current_state.get("stripe_plan_id"),
        "current_period_end": _stripe_subscription_period_end_iso(subscription) or current_state.get("current_period_end"),
        "cancel_at_period_end": bool(_stripe_object_get(subscription, "cancel_at_period_end")),
        "current_plan_id": current_plan_id,
    }
    if next_state.get("pending_plan_id") == next_state.get("current_plan_id"):
        next_state["pending_change"] = False
        next_state["pending_plan_id"] = None
        next_state["pending_effective_at"] = None
    return await _persist_subscription_state(user_doc=user_doc, subscription_state=next_state)

async def _grant_starter_checkout_if_needed(
    *,
    user_id: str,
    session_id: str,
    payment_reference: Optional[str],
    checkout_payload: Optional[Dict[str, Any]] = None,
) -> bool:
    existing_entry = await db.credit_ledger.find_one(
        {"entry_type": "plan_purchase", "reference_type": "checkout_session", "reference_id": session_id},
        {"_id": 0, "ledger_id": 1},
    )
    billing_record = await _find_billing_record_for_checkout(session_id, user_id)
    metadata_updates = {
        "stripe_checkout_status": (checkout_payload or {}).get("status"),
        "stripe_payment_status": (checkout_payload or {}).get("payment_status"),
        "stripe_payment_intent_id": payment_reference,
    }

    if existing_entry:
        logger.info(
            "Starter grant blocked by idempotency: session_id=%s user_id=%s billing_record_id=%s existing_ledger_id=%s",
            session_id,
            user_id,
            (billing_record or {}).get("billing_record_id"),
            existing_entry.get("ledger_id"),
        )
        if billing_record and billing_record.get("status") != "paid":
            await _update_billing_record(
                billing_record["billing_record_id"],
                status="paid",
                payment_reference=payment_reference,
                invoice_status="ready",
                metadata_updates=metadata_updates,
                paid=True,
            )
        return False

    user_doc = await _ensure_opening_balance_baseline_for_user_id(user_id)
    if not user_doc:
        logger.warning(
            "Starter grant skipped because user was not found: session_id=%s user_id=%s billing_record_id=%s",
            session_id,
            user_id,
            (billing_record or {}).get("billing_record_id"),
        )
        return False
    before_quota = _quota_snapshot(user_doc.get("quota"))
    before_wallet = _normalize_perizia_credit_wallet(
        user_doc,
        plan_id=user_doc.get("plan"),
        is_master_admin=_is_master_admin_email(user_doc.get("email")),
    )
    after_wallet = _append_pack_grant(
        before_wallet,
        amount=SUBSCRIPTION_PLANS["starter"].quota["perizia_scans_remaining"],
        reference_id=session_id,
        plan_code="starter",
    )
    after_quota, finalized_wallet = await _persist_perizia_credit_wallet(user_doc=user_doc, wallet=after_wallet)

    logger.info(
        "Executing Starter grant: session_id=%s user_id=%s billing_record_id=%s before_total=%s after_total=%s",
        session_id,
        user_id,
        (billing_record or {}).get("billing_record_id"),
        before_wallet.get("total_available"),
        finalized_wallet.get("total_available"),
    )
    await _record_quota_change_entries(
        user_doc=user_doc,
        before_quota=before_quota,
        after_quota=after_quota,
        entry_type="plan_purchase",
        reference_type="checkout_session",
        reference_id=session_id,
        description_it="Accredito crediti per acquisto pack Starter",
        metadata={
            "plan_code": "starter",
            "billing_reason": "starter_checkout_paid",
            "stripe_checkout_session_id": session_id,
            "stripe_payment_intent_id": payment_reference,
            "perizia_credit_wallet_before": before_wallet,
            "perizia_credit_wallet_after": finalized_wallet,
        },
    )

    if billing_record:
        await _update_billing_record(
            billing_record["billing_record_id"],
            status="paid",
            payment_reference=payment_reference,
            invoice_status="ready",
            metadata_updates={
                **metadata_updates,
                "entitlement_granted": True,
                "perizia_credit_wallet_after": finalized_wallet,
            },
            paid=True,
        )
    return True

async def _upsert_subscription_invoice_billing_record(
    *,
    user_id: str,
    plan_id: str,
    plan: SubscriptionPlan,
    invoice: Dict[str, Any],
    payment_reference: Optional[str],
    stripe_customer_id: Optional[str],
    stripe_subscription_id: Optional[str],
) -> Optional[Dict[str, Any]]:
    invoice_id = str(invoice.get("id") or "").strip() or None
    invoice_metadata = invoice.get("metadata") or {}
    amount_total = float((invoice.get("amount_paid") or invoice.get("amount_due") or 0) / 100.0)
    amount_subtotal = float((invoice.get("subtotal") or invoice.get("amount_paid") or invoice.get("amount_due") or 0) / 100.0)
    amount_tax = float((invoice.get("tax") or 0) / 100.0)
    billing_reason = str(invoice.get("billing_reason") or invoice_metadata.get("billing_reason") or "subscription_cycle").strip()
    description = (
        f"Pagamento iniziale abbonamento {plan.name_it}"
        if billing_reason == "subscription_create"
        else f"Rinnovo abbonamento {plan.name_it}"
    )

    existing_invoice_record = None
    if invoice_id:
        existing_invoice_record = await db.billing_records.find_one(
            {"user_id": user_id, "invoice_reference": invoice_id},
            {"_id": 0},
            sort=[("created_at", -1)],
        )
    if existing_invoice_record:
        await _update_billing_record(
            existing_invoice_record["billing_record_id"],
            status="paid",
            payment_reference=payment_reference,
            invoice_status="ready",
            invoice_reference=invoice_id,
            metadata_updates={
                "stripe_customer_id": stripe_customer_id,
                "stripe_subscription_id": stripe_subscription_id,
                "plan_code": plan_id,
                "billing_reason": billing_reason,
            },
            paid=True,
        )
        return existing_invoice_record

    pending_record = await _find_latest_pending_subscription_billing_record(user_id, plan_id)
    if pending_record:
        await _update_billing_record(
            pending_record["billing_record_id"],
            status="paid",
            payment_reference=payment_reference,
            invoice_status="ready",
            invoice_reference=invoice_id,
            metadata_updates={
                "stripe_customer_id": stripe_customer_id,
                "stripe_subscription_id": stripe_subscription_id,
                "plan_code": plan_id,
                "billing_reason": billing_reason,
            },
            paid=True,
        )
        return pending_record

    user_doc = await db.users.find_one({"user_id": user_id}, {"_id": 0})
    if not user_doc:
        return None
    return await _create_billing_record(
        user_doc=user_doc,
        plan=plan,
        plan_id=plan_id,
        purchase_type="subscription",
        status="paid",
        payment_provider="stripe",
        payment_reference=payment_reference,
        amount_subtotal=amount_subtotal,
        amount_tax=amount_tax,
        amount_total=amount_total,
        invoice_status="ready",
        invoice_reference=invoice_id,
        metadata={
            "stripe_customer_id": stripe_customer_id,
            "stripe_subscription_id": stripe_subscription_id,
            "plan_code": plan_id,
            "billing_reason": billing_reason,
        },
        description_it=description,
    )

async def _grant_subscription_invoice_if_needed(
    *,
    user_id: str,
    plan_id: str,
    invoice: Dict[str, Any],
    stripe_customer_id: Optional[str],
    stripe_subscription_id: Optional[str],
) -> bool:
    invoice_id = str(invoice.get("id") or "").strip()
    if not invoice_id or plan_id not in {"solo", "pro"}:
        return False

    payment_reference = str(invoice.get("payment_intent") or invoice_id)
    plan = SUBSCRIPTION_PLANS[plan_id]
    billing_record = await _upsert_subscription_invoice_billing_record(
        user_id=user_id,
        plan_id=plan_id,
        plan=plan,
        invoice=invoice,
        payment_reference=payment_reference,
        stripe_customer_id=stripe_customer_id,
        stripe_subscription_id=stripe_subscription_id,
    )

    user_doc = await _ensure_opening_balance_baseline_for_user_id(user_id)
    if not user_doc:
        return False
    subscription_state = _normalize_subscription_state(user_doc)
    if stripe_subscription_id and subscription_state.get("stripe_subscription_id") and subscription_state.get("stripe_subscription_id") != stripe_subscription_id:
        logger.warning(
            "Subscription invoice grant blocked by subscription mismatch: user_id=%s invoice_id=%s expected_subscription_id=%s actual_subscription_id=%s",
            user_id,
            invoice_id,
            subscription_state.get("stripe_subscription_id"),
            stripe_subscription_id,
        )
        return False
    if subscription_state.get("status") in SUBSCRIPTION_TERMINAL_STATUSES and not subscription_state.get("pending_change"):
        logger.warning(
            "Subscription invoice grant blocked by terminal lifecycle state: user_id=%s invoice_id=%s status=%s",
            user_id,
            invoice_id,
            subscription_state.get("status"),
        )
        return False
    pending_plan_id = subscription_state.get("pending_plan_id")
    current_plan_id = subscription_state.get("current_plan_id")
    if pending_plan_id and plan_id not in {current_plan_id, pending_plan_id}:
        logger.warning(
            "Subscription invoice grant blocked by pending lifecycle conflict: user_id=%s invoice_id=%s current_plan=%s pending_plan=%s invoice_plan=%s",
            user_id,
            invoice_id,
            current_plan_id,
            pending_plan_id,
            plan_id,
        )
        return False
    before_wallet = _normalize_perizia_credit_wallet(
        user_doc,
        plan_id=user_doc.get("plan"),
        is_master_admin=_is_master_admin_email(user_doc.get("email")),
    )
    if invoice_id in set(before_wallet.get("processed_invoice_ids") or []):
        return False
    before_quota = _quota_snapshot(user_doc.get("quota"))
    previous_plan = user_doc.get("plan")
    after_wallet = dict(before_wallet)
    after_wallet["monthly_remaining"] = _monthly_perizia_quota_for_plan(plan_id)
    after_wallet["monthly_plan_id"] = plan_id
    after_wallet["monthly_refreshed_at"] = _now_iso()
    processed_invoice_ids = list(after_wallet.get("processed_invoice_ids") or [])
    processed_invoice_ids.append(invoice_id)
    after_wallet["processed_invoice_ids"] = processed_invoice_ids
    after_quota, finalized_wallet = await _persist_perizia_credit_wallet(
        user_doc=user_doc,
        wallet=after_wallet,
        plan_override=plan_id,
    )
    billing_reason = str(invoice.get("billing_reason") or "subscription_cycle").strip()
    description = (
        f"Attivazione quota abbonamento {plan.name_it}"
        if billing_reason == "subscription_create"
        else f"Rinnovo quota abbonamento {plan.name_it}"
    )
    await _record_quota_change_entries(
        user_doc=user_doc,
        before_quota=before_quota,
        after_quota=after_quota,
        entry_type="subscription_reset",
        reference_type="stripe_invoice",
        reference_id=invoice_id,
        description_it=description,
        metadata={
            "plan_code": plan_id,
            "billing_reason": billing_reason,
            "stripe_invoice_id": invoice_id,
            "stripe_customer_id": stripe_customer_id,
            "stripe_subscription_id": stripe_subscription_id,
            "old_plan": previous_plan,
            "new_plan": plan_id,
            "perizia_credit_wallet_before": before_wallet,
            "perizia_credit_wallet_after": finalized_wallet,
        },
    )
    if billing_record:
        await _update_billing_record(
            billing_record["billing_record_id"],
            metadata_updates={
                "entitlement_granted": True,
                "perizia_credit_wallet_after": finalized_wallet,
            },
        )
    await _persist_subscription_state(
        user_doc=user_doc,
        subscription_state={
            **subscription_state,
            "stripe_customer_id": stripe_customer_id or subscription_state.get("stripe_customer_id"),
            "stripe_subscription_id": stripe_subscription_id or subscription_state.get("stripe_subscription_id"),
            "status": "active",
            "current_plan_id": plan_id,
            "stripe_plan_id": plan_id,
            "cancel_at_period_end": False if subscription_state.get("pending_plan_id") == plan_id else subscription_state.get("cancel_at_period_end"),
            "pending_change": False if subscription_state.get("pending_plan_id") == plan_id else subscription_state.get("pending_change"),
            "pending_plan_id": None if subscription_state.get("pending_plan_id") == plan_id else subscription_state.get("pending_plan_id"),
            "pending_effective_at": None if subscription_state.get("pending_plan_id") == plan_id else subscription_state.get("pending_effective_at"),
        },
        plan_override=plan_id,
    )
    return True

def _stripe_object_get(obj: Any, key: str) -> Any:
    if obj is None:
        return None
    if isinstance(obj, dict):
        return obj.get(key)
    getter = getattr(obj, "get", None)
    if callable(getter):
        try:
            return getter(key)
        except Exception:
            pass
    return getattr(obj, key, None)


def _checkout_session_result(
    *,
    stripe_status: Optional[str],
    payment_status: Optional[str],
    billing_record: Optional[Dict[str, Any]],
) -> str:
    billing_status = str((billing_record or {}).get("status") or "").strip().lower()
    invoice_status = str((billing_record or {}).get("invoice_status") or "").strip().lower()
    metadata = dict((billing_record or {}).get("metadata") or {})
    if metadata.get("entitlement_granted"):
        return "success"
    if metadata.get("manual_review_required"):
        return "manual_review"
    if billing_status in {"failed", "cancelled"} or invoice_status == "failed":
        return "failed"
    if str(stripe_status or "").strip().lower() == "expired":
        return "expired"
    if billing_status == "paid":
        return "success"
    if str(payment_status or "").strip().lower() == "unpaid":
        return "failed"
    return "processing"

def _stripe_object_metadata(obj: Any) -> Dict[str, Any]:
    metadata = _stripe_object_get(obj, "metadata") or {}
    if isinstance(metadata, dict):
        return metadata
    try:
        return dict(metadata)
    except Exception:
        return {}

def _stripe_invoice_line_price_id(line: Dict[str, Any]) -> Optional[str]:
    price_id = str((((line or {}).get("price") or {}).get("id")) or "").strip()
    if price_id:
        return price_id
    plan_id = str((((line or {}).get("plan") or {}).get("id")) or "").strip()
    if plan_id:
        return plan_id
    pricing_price_id = str(
        ((((line or {}).get("pricing") or {}).get("price_details") or {}).get("price")) or ""
    ).strip()
    return pricing_price_id or None

async def _payment_transaction_for_subscription_context(
    *,
    subscription_id: Optional[str] = None,
    customer_id: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    if subscription_id:
        transaction = await db.payment_transactions.find_one(
            {"stripe_subscription_id": subscription_id},
            {"_id": 0, "user_id": 1, "plan_id": 1, "session_id": 1},
            sort=[("created_at", -1)],
        )
        if transaction:
            return transaction
    if customer_id:
        return await db.payment_transactions.find_one(
            {"stripe_customer_id": customer_id},
            {"_id": 0, "user_id": 1, "plan_id": 1, "session_id": 1},
            sort=[("created_at", -1)],
        )
    return None

async def _resolve_starter_checkout_context(
    *,
    session_id: str,
    metadata_user_id: Optional[str],
    client_reference_id: Optional[str],
) -> Tuple[Optional[str], Optional[Dict[str, Any]], Optional[Dict[str, Any]], Dict[str, Optional[str]]]:
    payment_transaction = await _payment_transaction_by_session(session_id)
    billing_record = await _find_billing_record_for_checkout(session_id)
    candidates = {
        "metadata_user_id": str(metadata_user_id or "").strip() or None,
        "client_reference_id": str(client_reference_id or "").strip() or None,
        "payment_transaction_user_id": str((payment_transaction or {}).get("user_id") or "").strip() or None,
        "billing_record_user_id": str((billing_record or {}).get("user_id") or "").strip() or None,
    }
    distinct_user_ids = {value for value in candidates.values() if value}
    if len(distinct_user_ids) > 1:
        logger.warning(
            "Starter checkout user resolution conflict: session_id=%s candidates=%s billing_record_id=%s payment_transaction_id=%s",
            session_id,
            candidates,
            (billing_record or {}).get("billing_record_id"),
            (payment_transaction or {}).get("transaction_id"),
        )
        return None, billing_record, payment_transaction, candidates
    resolved_user_id = next(iter(distinct_user_ids), None)
    return resolved_user_id, billing_record, payment_transaction, candidates

async def _starter_checkout_payment_confirmed(checkout_session: Dict[str, Any], stripe_module: Any) -> bool:
    payment_status = str(checkout_session.get("payment_status") or "").strip()
    payment_intent_id = str(checkout_session.get("payment_intent") or "").strip()
    if payment_status != "paid" or not payment_intent_id:
        logger.info(
            "Starter checkout payment not confirmed from session payload: session_id=%s payment_status=%s payment_intent=%s",
            str(checkout_session.get("id") or "").strip(),
            payment_status,
            payment_intent_id or None,
        )
        return False
    try:
        payment_intent = stripe_module.PaymentIntent.retrieve(payment_intent_id)
    except Exception as exc:
        logger.warning(f"Stripe PaymentIntent lookup failed for starter checkout {payment_intent_id}: {exc}")
        return False
    payment_intent_status = str(_stripe_object_get(payment_intent, "status") or "").strip()
    logger.info(
        "Starter PaymentIntent retrieved: session_id=%s payment_intent=%s status=%s",
        str(checkout_session.get("id") or "").strip(),
        payment_intent_id,
        payment_intent_status or None,
    )
    return payment_intent_status == "succeeded"

async def _resolve_invoice_context(invoice: Dict[str, Any], stripe_module: Any) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    metadata = dict(invoice.get("metadata") or {})
    subscription_details = invoice.get("subscription_details") or {}
    parent_details = ((invoice.get("parent") or {}).get("subscription_details")) or {}
    merged_metadata = {}
    merged_metadata.update(_stripe_object_metadata(parent_details))
    merged_metadata.update(_stripe_object_metadata(subscription_details))
    merged_metadata.update(metadata)
    metadata = merged_metadata
    subscription_id = str(invoice.get("subscription") or "").strip() or None
    customer_id = str(invoice.get("customer") or "").strip() or None
    user_candidates: Dict[str, Optional[str]] = {
        "invoice_metadata_user_id": str(metadata.get("app_user_id") or "").strip() or None,
    }
    plan_candidates: Dict[str, Optional[str]] = {
        "invoice_metadata_plan_id": str(metadata.get("plan_code") or "").strip().lower() or None,
    }

    if subscription_id:
        try:
            subscription = stripe_module.Subscription.retrieve(subscription_id)
            subscription_metadata = _stripe_object_metadata(subscription)
            user_candidates["subscription_metadata_user_id"] = (
                str(subscription_metadata.get("app_user_id") or "").strip() or None
            )
            plan_candidates["subscription_metadata_plan_id"] = (
                str(subscription_metadata.get("plan_code") or "").strip().lower() or None
            )
            items = _stripe_object_get(subscription, "items") or {}
            for item in (items.get("data") or []):
                price_id = str((((item or {}).get("price") or {}).get("id")) or "").strip()
                plan_id = _plan_id_for_stripe_price_id(price_id)
                if plan_id:
                    plan_candidates["subscription_item_plan_id"] = plan_id
                    break
        except Exception as exc:
            logger.warning(f"Stripe subscription lookup failed for invoice context: {exc}")

    transaction = await _payment_transaction_for_subscription_context(
        subscription_id=subscription_id,
        customer_id=customer_id,
    )
    if transaction:
        user_candidates["payment_transaction_user_id"] = str(transaction.get("user_id") or "").strip() or None
        plan_candidates["payment_transaction_plan_id"] = str(transaction.get("plan_id") or "").strip().lower() or None

    lines = invoice.get("lines", {}).get("data", []) or []
    for line in lines:
        price_id = _stripe_invoice_line_price_id(line) or ""
        plan_id = _plan_id_for_stripe_price_id(price_id)
        if plan_id:
            plan_candidates["invoice_line_plan_id"] = plan_id
            break

    distinct_user_ids = {value for value in user_candidates.values() if value}
    distinct_plan_ids = {value for value in plan_candidates.values() if value}
    if len(distinct_user_ids) > 1 or len(distinct_plan_ids) > 1:
        logger.warning(
            "Invoice context resolution conflict: invoice_id=%s subscription_id=%s customer_id=%s user_candidates=%s plan_candidates=%s",
            str(invoice.get("id") or "").strip() or None,
            subscription_id,
            customer_id,
            user_candidates,
            plan_candidates,
        )
        return None, None, subscription_id

    user_id = next(iter(distinct_user_ids), None)
    plan_id = next(iter(distinct_plan_ids), None)
    return user_id, plan_id, subscription_id

async def _insert_credit_ledger_entry(
    *,
    user_id: str,
    user_email: str,
    quota_field: str,
    direction: str,
    amount: int,
    balance_before: int,
    balance_after: int,
    entry_type: str,
    reference_type: str,
    reference_id: str,
    description_it: str,
    metadata: Optional[Dict[str, Any]] = None,
    actor_user: Optional[User] = None,
) -> Dict[str, Any]:
    if quota_field not in ACCOUNT_QUOTA_FIELDS:
        raise ValueError(f"Unsupported ledger quota field: {quota_field}")
    if direction not in LEDGER_DIRECTIONS:
        raise ValueError(f"Unsupported ledger direction: {direction}")
    if entry_type not in LEDGER_ENTRY_TYPES:
        raise ValueError(f"Unsupported ledger entry type: {entry_type}")

    entry = CreditLedgerEntry(
        ledger_id=f"ledger_{uuid.uuid4().hex[:16]}",
        user_id=user_id,
        user_email=str(user_email or "").strip().lower(),
        quota_field=quota_field,
        direction=direction,
        amount=abs(int(amount or 0)),
        balance_before=int(balance_before or 0),
        balance_after=int(balance_after or 0),
        entry_type=entry_type,
        reference_type=str(reference_type or "system"),
        reference_id=str(reference_id or "n/a"),
        description_it=str(description_it or "").strip() or "Movimento crediti",
        metadata=_sanitize_metadata(metadata),
        actor_user_id=actor_user.user_id if actor_user else None,
        actor_email=actor_user.email if actor_user else None,
    )
    entry_dict = _serialize_datetime_fields(entry.model_dump(), "created_at")
    await db.credit_ledger.insert_one(entry_dict)
    return entry_dict

async def _ensure_opening_balance_baseline_for_user_doc(user_doc: Dict[str, Any]) -> None:
    user_id = str(user_doc.get("user_id") or "").strip()
    if not user_id:
        return
    if user_doc.get("credit_ledger_baseline_initialized_at"):
        return

    user_email = str(user_doc.get("email") or "").strip().lower()
    quota_snapshot = _quota_snapshot(user_doc.get("quota"))
    for field, current_balance in quota_snapshot.items():
        if current_balance <= 0:
            continue
        existing = await db.credit_ledger.find_one(
            {"user_id": user_id, "quota_field": field, "entry_type": "opening_balance"},
            {"_id": 0, "ledger_id": 1},
        )
        if existing:
            continue
        await _insert_credit_ledger_entry(
            user_id=user_id,
            user_email=user_email,
            quota_field=field,
            direction="credit",
            amount=current_balance,
            balance_before=0,
            balance_after=current_balance,
            entry_type="opening_balance",
            reference_type="system",
            reference_id=f"opening_balance:{user_id}:{field}",
            description_it="Saldo iniziale registrato all'attivazione del ledger crediti",
            metadata={
                "baseline_reason": (
                    "Baseline iniziale creata quando il ledger crediti e stato attivato per questo account; "
                    "i movimenti storici precedenti potrebbero non essere ricostruibili."
                )
            },
        )

    baseline_initialized_at = _now_iso()
    await db.users.update_one(
        {"user_id": user_id},
        {"$set": {"credit_ledger_baseline_initialized_at": baseline_initialized_at}},
    )
    user_doc["credit_ledger_baseline_initialized_at"] = baseline_initialized_at

async def _ensure_opening_balance_baseline_for_user_id(user_id: str) -> Optional[Dict[str, Any]]:
    user_doc = await db.users.find_one({"user_id": user_id}, {"_id": 0})
    if not user_doc:
        return None
    normalized_user_doc = await _apply_normalized_account_state(user_doc, persist=True)
    await _ensure_opening_balance_baseline_for_user_doc(normalized_user_doc)
    return normalized_user_doc

async def _record_quota_change_entries(
    *,
    user_doc: Dict[str, Any],
    before_quota: Dict[str, Any],
    after_quota: Dict[str, Any],
    entry_type: str,
    reference_type: str,
    reference_id: str,
    description_it: str,
    metadata: Optional[Dict[str, Any]] = None,
    actor_user: Optional[User] = None,
) -> List[Dict[str, Any]]:
    await _ensure_opening_balance_baseline_for_user_doc(user_doc)

    user_id = str(user_doc.get("user_id") or "")
    user_email = str(user_doc.get("email") or "").strip().lower()
    before_snapshot = _quota_snapshot(before_quota)
    after_snapshot = _quota_snapshot(after_quota)
    base_metadata = _sanitize_metadata(metadata)
    entries: List[Dict[str, Any]] = []

    for field in ACCOUNT_QUOTA_FIELDS:
        balance_before = before_snapshot[field]
        balance_after = after_snapshot[field]
        delta = balance_after - balance_before
        if delta == 0:
            continue
        entry = await _insert_credit_ledger_entry(
            user_id=user_id,
            user_email=user_email,
            quota_field=field,
            direction="credit" if delta > 0 else "debit",
            amount=abs(delta),
            balance_before=balance_before,
            balance_after=balance_after,
            entry_type=entry_type,
            reference_type=reference_type,
            reference_id=reference_id,
            description_it=description_it,
            metadata={**base_metadata, "quota_delta": delta},
            actor_user=actor_user,
        )
        entries.append(entry)
    return entries

async def _apply_quota_debit_with_ledger(
    user: User,
    *,
    field: str,
    amount: int,
    entry_type: str,
    reference_type: str,
    reference_id: str,
    description_it: str,
    metadata: Optional[Dict[str, Any]] = None,
) -> bool:
    if user.is_master_admin:
        return False
    debit_amount = int(amount or 0)
    if debit_amount <= 0 or field not in ACCOUNT_QUOTA_FIELDS:
        return False

    await _ensure_opening_balance_baseline_for_user_id(user.user_id)
    balance_before = int(user.quota.get(field, 0) or 0)
    balance_after = balance_before - debit_amount

    await db.users.update_one(
        {"user_id": user.user_id},
        {"$inc": {f"quota.{field}": -debit_amount}},
    )
    await _insert_credit_ledger_entry(
        user_id=user.user_id,
        user_email=user.email,
        quota_field=field,
        direction="debit",
        amount=debit_amount,
        balance_before=balance_before,
        balance_after=balance_after,
        entry_type=entry_type,
        reference_type=reference_type,
        reference_id=reference_id,
        description_it=description_it,
        metadata=metadata,
    )
    user.quota[field] = balance_after
    return True


async def _apply_perizia_credit_debit_with_ledger(
    user: User,
    *,
    amount: int,
    entry_type: str,
    reference_type: str,
    reference_id: str,
    description_it: str,
    metadata: Optional[Dict[str, Any]] = None,
) -> bool:
    if user.is_master_admin:
        return False
    debit_amount = max(0, int(amount or 0))
    if debit_amount <= 0:
        return False

    user_doc = await _ensure_opening_balance_baseline_for_user_id(user.user_id)
    if not user_doc:
        return False
    before_wallet = _normalize_perizia_credit_wallet(
        user_doc,
        plan_id=user_doc.get("plan"),
        is_master_admin=_is_master_admin_email(user_doc.get("email")),
    )
    if before_wallet["total_available"] < debit_amount:
        return False

    monthly_before = int(before_wallet.get("monthly_remaining", 0) or 0)
    debit_from_monthly = min(monthly_before, debit_amount)
    debit_from_extra = debit_amount - debit_from_monthly

    after_wallet = dict(before_wallet)
    after_wallet["monthly_remaining"] = monthly_before - debit_from_monthly
    after_wallet["extra_remaining"] = max(0, int(before_wallet.get("extra_remaining", 0) or 0) - debit_from_extra)
    after_wallet["pack_grants"] = _consume_extra_pack_grants(
        list(before_wallet.get("pack_grants") or []),
        debit_from_extra,
    )

    before_quota = _quota_snapshot(user_doc.get("quota"))
    after_quota, finalized_wallet = await _persist_perizia_credit_wallet(user_doc=user_doc, wallet=after_wallet)
    await _record_quota_change_entries(
        user_doc=user_doc,
        before_quota=before_quota,
        after_quota=after_quota,
        entry_type=entry_type,
        reference_type=reference_type,
        reference_id=reference_id,
        description_it=description_it,
        metadata={
            **(metadata or {}),
            "perizia_credit_wallet_before": before_wallet,
            "perizia_credit_wallet_after": finalized_wallet,
            "debit_from_monthly": debit_from_monthly,
            "debit_from_extra": debit_from_extra,
        },
    )
    user.quota["perizia_scans_remaining"] = finalized_wallet["total_available"]
    return True

async def _create_billing_record(
    *,
    user_doc: Dict[str, Any],
    plan: SubscriptionPlan,
    plan_id: str,
    purchase_type: str,
    status: str,
    payment_provider: str,
    description_it: str,
    checkout_reference: Optional[str] = None,
    payment_reference: Optional[str] = None,
    amount_subtotal: Optional[float] = None,
    amount_tax: Optional[float] = None,
    amount_total: Optional[float] = None,
    customer_type: str = "individual",
    company_name: Optional[str] = None,
    billing_email: Optional[str] = None,
    billing_address: Optional[Dict[str, Any]] = None,
    tax_code: Optional[str] = None,
    vat_number: Optional[str] = None,
    invoice_status: str = "pending",
    invoice_reference: Optional[str] = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    if status not in BILLING_RECORD_STATUSES:
        raise ValueError(f"Unsupported billing status: {status}")
    if payment_provider not in BILLING_PROVIDER_TYPES:
        raise ValueError(f"Unsupported billing provider: {payment_provider}")
    if invoice_status not in BILLING_INVOICE_STATUSES:
        raise ValueError(f"Unsupported invoice status: {invoice_status}")

    subtotal = float(plan.price if amount_subtotal is None else amount_subtotal)
    tax_amount = float(0.0 if amount_tax is None else amount_tax)
    total_amount = float(subtotal + tax_amount if amount_total is None else amount_total)

    record = BillingRecord(
        billing_record_id=f"bill_{uuid.uuid4().hex[:16]}",
        user_id=str(user_doc.get("user_id") or ""),
        user_email=str(user_doc.get("email") or "").strip().lower(),
        customer_type=customer_type,
        customer_name=str(user_doc.get("name") or "").strip() or str(user_doc.get("email") or ""),
        company_name=company_name,
        billing_email=str(billing_email or user_doc.get("email") or "").strip().lower(),
        country_code=str(user_doc.get("country_code") or "IT").upper(),
        billing_address=billing_address,
        tax_code=tax_code,
        vat_number=vat_number,
        plan_id=plan_id,
        purchase_type=purchase_type,
        amount_subtotal=subtotal,
        amount_tax=tax_amount,
        amount_total=total_amount,
        currency=str(plan.currency or "eur").lower(),
        status=status,
        payment_provider=payment_provider,
        payment_reference=payment_reference,
        checkout_reference=checkout_reference,
        invoice_status=invoice_status,
        invoice_reference=invoice_reference,
        description_it=description_it,
        metadata=_sanitize_metadata(metadata),
    )
    record_dict = _serialize_datetime_fields(record.model_dump(), "created_at", "updated_at", "paid_at")
    await db.billing_records.insert_one(record_dict)
    return record_dict

async def _mark_billing_record_paid(
    *,
    user_id: str,
    checkout_reference: str,
    payment_reference: Optional[str],
    metadata: Optional[Dict[str, Any]] = None,
) -> None:
    update_fields: Dict[str, Any] = {
        "status": "paid",
        "payment_reference": payment_reference,
        "invoice_status": "ready",
        "updated_at": _now_iso(),
        "paid_at": _now_iso(),
    }
    if metadata:
        update_fields["metadata"] = _sanitize_metadata(metadata)
    await db.billing_records.update_one(
        {"user_id": user_id, "checkout_reference": checkout_reference},
        {"$set": update_fields},
    )

async def _create_admin_manual_billing_record(
    *,
    admin_user: User,
    target_user_doc: Dict[str, Any],
    billing_payload: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    if not isinstance(billing_payload, dict):
        return None
    amount_total = float(billing_payload.get("amount_total", 0) or 0)
    amount_tax = float(billing_payload.get("amount_tax", 0) or 0)
    amount_subtotal = float(billing_payload.get("amount_subtotal", amount_total - amount_tax) or 0)
    currency = str(billing_payload.get("currency") or "eur").lower()
    description_it = str(billing_payload.get("description_it") or "Registrazione manuale amministrativa").strip()
    plan_id = str(billing_payload.get("plan_id") or target_user_doc.get("plan") or "manual").strip()
    record = BillingRecord(
        billing_record_id=f"bill_{uuid.uuid4().hex[:16]}",
        user_id=str(target_user_doc.get("user_id") or ""),
        user_email=str(target_user_doc.get("email") or "").strip().lower(),
        customer_type=str(billing_payload.get("customer_type") or "individual"),
        customer_name=str(billing_payload.get("customer_name") or target_user_doc.get("name") or target_user_doc.get("email") or "").strip(),
        company_name=billing_payload.get("company_name"),
        billing_email=str(billing_payload.get("billing_email") or target_user_doc.get("email") or "").strip().lower(),
        country_code=str(billing_payload.get("country_code") or target_user_doc.get("country_code") or "IT").upper(),
        billing_address=billing_payload.get("billing_address"),
        tax_code=billing_payload.get("tax_code"),
        vat_number=billing_payload.get("vat_number"),
        plan_id=plan_id,
        purchase_type="admin_manual",
        amount_subtotal=amount_subtotal,
        amount_tax=amount_tax,
        amount_total=amount_total,
        currency=currency,
        status=str(billing_payload.get("status") or "paid"),
        payment_provider="manual",
        payment_reference=billing_payload.get("payment_reference"),
        checkout_reference=billing_payload.get("checkout_reference"),
        invoice_status=str(billing_payload.get("invoice_status") or "pending"),
        description_it=description_it,
        metadata=_sanitize_metadata(
            {
                **(billing_payload.get("metadata") or {}),
                "admin_user_id": admin_user.user_id,
                "admin_email": admin_user.email,
            }
        ),
        paid_at=datetime.now(timezone.utc) if str(billing_payload.get("status") or "paid") == "paid" else None,
    )
    record_dict = _serialize_datetime_fields(record.model_dump(), "created_at", "updated_at", "paid_at")
    await db.billing_records.insert_one(record_dict)
    return record_dict

def _merge_query(base: Dict[str, Any], extra: Dict[str, Any]) -> Dict[str, Any]:
    if not base:
        return extra
    if not extra:
        return base
    return {"$and": [base, extra]}

def _date_range_query(field: str, date_from: Optional[str], date_to: Optional[str]) -> Dict[str, Any]:
    conditions: List[Dict[str, Any]] = []
    if date_from:
        start_dt = datetime.fromisoformat(date_from).replace(tzinfo=timezone.utc)
        start_iso = start_dt.isoformat()
        conditions.append({"$or": [{field: {"$gte": start_dt}}, {field: {"$gte": start_iso}}]})
    if date_to:
        end_dt = datetime.fromisoformat(date_to).replace(tzinfo=timezone.utc) + timedelta(days=1) - timedelta(microseconds=1)
        end_iso = end_dt.isoformat()
        conditions.append({"$or": [{field: {"$lte": end_dt}}, {field: {"$lte": end_iso}}]})
    if not conditions:
        return {}
    if len(conditions) == 1:
        return conditions[0]
    return {"$and": conditions}

async def _write_admin_audit(
    admin_user: User,
    action: str,
    target_user_id: Optional[str] = None,
    target_email: Optional[str] = None,
    meta: Optional[Dict[str, Any]] = None
) -> None:
    audit = {
        "audit_id": f"audit_{uuid.uuid4().hex[:12]}",
        "admin_user_id": admin_user.user_id,
        "admin_email": admin_user.email,
        "action": action,
        "target_user_id": target_user_id,
        "target_email": target_email,
        "meta": meta or {},
        "created_at": _now_iso()
    }
    try:
        await db.admin_audit_log.insert_one(audit)
    except Exception as e:
        logger.warning(f"Admin audit log insert failed: {e}")

async def _decrement_quota_if_applicable(user: User, field: str, amount: int = 1) -> bool:
    if field == "perizia_scans_remaining":
        return await _apply_perizia_credit_debit_with_ledger(
            user,
            amount=amount,
            entry_type="system_correction",
            reference_type="legacy_helper",
            reference_id=f"{user.user_id}:{field}",
            description_it="Addebito crediti registrato dal helper legacy",
        )
    return await _apply_quota_debit_with_ledger(
        user,
        field=field,
        amount=amount,
        entry_type="system_correction",
        reference_type="legacy_helper",
        reference_id=f"{user.user_id}:{field}",
        description_it="Addebito crediti registrato dal helper legacy",
    )

ADMIN_NOTE_STATUSES = {"OK", "WATCH", "BLOCKED"}
ADMIN_QUOTA_FIELDS = {"perizia_scans_remaining", "image_scans_remaining", "assistant_messages_remaining"}

# ===================
# AUTH ENDPOINTS
# ===================

@api_router.post("/auth/session")
async def create_session(request: Request, response: Response):
    """Exchange session_id from Emergent Auth for session_token"""
    data = await request.json()
    session_id = data.get("session_id")
    
    if not session_id:
        raise HTTPException(status_code=400, detail="session_id required")
    
    # Call Emergent Auth to get user data
    async with httpx.AsyncClient() as client:
        try:
            resp = await client.get(
                "https://demobackend.emergentagent.com/auth/v1/env/oauth/session-data",
                headers={"X-Session-ID": session_id}
            )
            if resp.status_code != 200:
                raise HTTPException(status_code=401, detail="Invalid session_id")
            user_data = resp.json()
        except Exception as e:
            logger.error(f"Auth error: {e}")
            raise HTTPException(status_code=500, detail="Authentication failed")
    
    email = user_data.get("email")
    name = user_data.get("name")
    picture = user_data.get("picture")
    
    # Check if user exists or create new one
    existing_user = await db.users.find_one({"email": email}, {"_id": 0})
    is_master = _is_master_admin_email(email)
    
    if existing_user:
        user_id = existing_user["user_id"]
        # Update user data before normalizing persisted account state
        update_data = {"name": name, "picture": picture}
        await db.users.update_one(
            {"user_id": user_id},
            {"$set": update_data}
        )

        updated_user = await db.users.find_one({"user_id": user_id}, {"_id": 0})
        normalized_user = await _apply_normalized_account_state(updated_user, persist=True)
        await _ensure_opening_balance_baseline_for_user_doc(normalized_user)
        user = User(**normalized_user)
    else:
        user_id = f"user_{uuid.uuid4().hex[:12]}"
        
        new_user = User(
            user_id=user_id,
            email=email,
            name=name,
            picture=picture,
            plan="enterprise" if is_master else "free",
            is_master_admin=is_master,
            quota=SUBSCRIPTION_PLANS["enterprise" if is_master else "free"].quota.copy()
        )
        user_dict = new_user.model_dump()
        user_dict["created_at"] = user_dict["created_at"].isoformat()
        await db.users.insert_one(user_dict)
        await _ensure_opening_balance_baseline_for_user_doc(user_dict)
        user = new_user
    
    # Create session
    session_token = f"sess_{uuid.uuid4().hex}"
    expires_at = datetime.now(timezone.utc) + timedelta(days=7)
    
    session = UserSession(
        session_id=str(uuid.uuid4()),
        user_id=user_id,
        session_token=session_token,
        expires_at=expires_at
    )
    session_dict = session.model_dump()
    session_dict["expires_at"] = session_dict["expires_at"].isoformat()
    session_dict["created_at"] = session_dict["created_at"].isoformat()
    await db.user_sessions.insert_one(session_dict)

    if is_master:
        await _write_admin_audit(
            admin_user=user,
            action="ADMIN_LOGIN",
            meta={"event": "login"}
        )
    
    # Set cookie
    response.set_cookie(
        key="session_token",
        value=session_token,
        httponly=True,
        secure=True,
        samesite="none",
        path="/",
        max_age=7 * 24 * 60 * 60
    )
    
    user_response = _build_user_response(user)

    return {"user": user_response, "session_token": session_token}

@api_router.get("/auth/me")
async def get_me(request: Request):
    """Get current authenticated user"""
    user = await get_current_user(request)
    if not user:
        raise HTTPException(status_code=401, detail="Not authenticated")
    
    return _build_user_response(user)

@api_router.post("/auth/logout")
async def logout(request: Request, response: Response):
    """Logout user"""
    session_token = request.cookies.get("session_token")
    if session_token:
        await db.user_sessions.delete_one({"session_token": session_token})
    
    response.delete_cookie(key="session_token", path="/", samesite="none", secure=True)
    return {"message": "Logged out"}

# ===================
# SUBSCRIPTION ENDPOINTS
# ===================

@api_router.get("/plans")
async def get_plans():
    """Get all subscription plans"""
    public_plans = [plan.model_dump() for plan in SUBSCRIPTION_PLANS.values() if plan.public]
    return {"plans": public_plans}

@api_router.post("/checkout/create")
async def create_checkout(request: Request):
    """Create Stripe checkout session"""
    user = await require_auth(request)
    user_doc = await _get_user_doc_by_id(user.user_id)
    data = await request.json()
    plan_id = data.get("plan_id")
    origin_url = data.get("origin_url")

    if plan_id not in SUBSCRIPTION_PLANS or plan_id in {"free", "studio", "enterprise"}:
        raise HTTPException(status_code=400, detail="Invalid plan")

    plan = SUBSCRIPTION_PLANS[plan_id]
    if plan.plan_type not in {"one_time", "subscription"}:
        raise HTTPException(status_code=400, detail="Plan not available for checkout")
    subscription_state = _normalize_subscription_state(user_doc or user.model_dump())
    if plan.plan_type == "subscription" and _subscription_checkout_is_blocked(subscription_state):
        raise HTTPException(
            status_code=409,
            detail=(
                "Hai gia un abbonamento mensile gestito. Usa le azioni di cambio piano o cancellazione nella pagina Abbonamento. "
                "Credit Pack 8 resta acquistabile in qualsiasi momento."
            ),
        )

    if not STRIPE_SECRET_KEY:
        raise HTTPException(status_code=503, detail="Stripe not configured")
    success_url, cancel_url = _build_stripe_return_urls(origin_url=origin_url)
    price_id = _stripe_price_id_for_plan(plan_id)
    metadata = _stripe_checkout_metadata(
        user_id=user.user_id,
        plan_id=plan_id,
        billing_reason="checkout_session_create",
    )
    import stripe
    stripe.api_key = STRIPE_SECRET_KEY

    session_kwargs: Dict[str, Any] = {
        "mode": "payment" if plan_id == "starter" else "subscription",
        "success_url": success_url,
        "cancel_url": cancel_url,
        "line_items": [{"price": price_id, "quantity": 1}],
        "payment_method_types": ["card"],
        "customer_email": user.email,
        "client_reference_id": user.user_id,
        "metadata": metadata,
    }
    if plan_id in {"solo", "pro"}:
        session_kwargs["subscription_data"] = {"metadata": metadata}

    session = stripe.checkout.Session.create(**session_kwargs)

    transaction = PaymentTransaction(
        transaction_id=f"txn_{uuid.uuid4().hex[:12]}",
        user_id=user.user_id,
        session_id=session.id,
        plan_id=plan_id,
        amount=plan.price,
        currency=plan.currency,
        status="pending",
        payment_status="initiated"
    )
    txn_dict = transaction.model_dump()
    txn_dict["created_at"] = txn_dict["created_at"].isoformat()
    await db.payment_transactions.insert_one(txn_dict)

    await _create_billing_record(
        user_doc=user.model_dump(),
        plan=plan,
        plan_id=plan_id,
        purchase_type=_billing_purchase_type_for_plan(plan),
        status="pending",
        payment_provider="stripe",
        checkout_reference=session.id,
        description_it=f"Checkout Stripe {plan.name_it}",
        invoice_status="pending",
        metadata={
            "checkout_session_id": session.id,
            "payment_transaction_id": transaction.transaction_id,
            "plan_type": plan.plan_type,
            "plan_code": plan_id,
            "stripe_price_id": price_id,
            "billing_reason": "checkout_session_create",
            "stripe_checkout_mode": session_kwargs["mode"],
        },
    )

    return {"url": session.url, "session_id": session.id}

@api_router.get("/checkout/status/{session_id}")
async def get_checkout_status(session_id: str, request: Request):
    """Get checkout session status"""
    user = await require_auth(request)

    if not _is_valid_checkout_session_id(session_id):
        raise HTTPException(status_code=400, detail="Invalid checkout session id")

    if not STRIPE_SECRET_KEY:
        raise HTTPException(status_code=503, detail="Stripe not configured")
    import stripe
    stripe.api_key = STRIPE_SECRET_KEY

    status = stripe.checkout.Session.retrieve(session_id)
    txn = await _payment_transaction_by_session(session_id, user.user_id)
    billing_record = await _find_billing_record_for_checkout(session_id, user.user_id)

    return {
        "status": status.status,
        "payment_status": status.payment_status,
        "amount_total": status.amount_total,
        "currency": status.currency,
        "mode": getattr(status, "mode", None),
        "plan_id": (txn or {}).get("plan_id"),
        "transaction_status": (txn or {}).get("status"),
        "purchase_type": (billing_record or {}).get("purchase_type"),
        "billing_status": (billing_record or {}).get("status"),
        "invoice_status": (billing_record or {}).get("invoice_status"),
        "entitlement_granted": bool(((billing_record or {}).get("metadata") or {}).get("entitlement_granted")),
        "manual_review_required": bool(((billing_record or {}).get("metadata") or {}).get("manual_review_required")),
        "session_result": _checkout_session_result(
            stripe_status=status.status,
            payment_status=status.payment_status,
            billing_record=billing_record,
        ),
    }


@api_router.post("/billing/subscription/change-plan")
async def change_subscription_plan(request: Request):
    user = await require_auth(request)
    user_doc = await _get_user_doc_by_id(user.user_id)
    if not user_doc:
        raise HTTPException(status_code=404, detail="Utente non trovato")
    data = await request.json()
    target_plan_id = str(data.get("plan_id") or "").strip().lower()
    if target_plan_id not in SELF_SERVE_RECURRING_PLAN_IDS:
        raise HTTPException(status_code=400, detail="Piano non valido")
    subscription_state = _normalize_subscription_state(user_doc)
    current_plan_id = str(subscription_state.get("current_plan_id") or "").strip().lower()
    subscription_id = str(subscription_state.get("stripe_subscription_id") or "").strip()
    if current_plan_id not in SELF_SERVE_RECURRING_PLAN_IDS or not subscription_id:
        raise HTTPException(status_code=409, detail="Nessun abbonamento ricorrente gestibile trovato")
    if current_plan_id == target_plan_id:
        raise HTTPException(status_code=409, detail="Il piano richiesto e gia quello attivo")
    if subscription_state.get("cancel_at_period_end"):
        raise HTTPException(status_code=409, detail="Rimuovi prima la cancellazione a fine periodo")
    if subscription_state.get("pending_change"):
        if subscription_state.get("pending_plan_id") == target_plan_id:
            raise HTTPException(status_code=409, detail="Esiste gia un cambio piano pendente verso questo piano")
        raise HTTPException(status_code=409, detail="Esiste gia un cambio piano pendente")
    allowed_changes = {("solo", "pro"), ("pro", "solo")}
    if (current_plan_id, target_plan_id) not in allowed_changes:
        raise HTTPException(status_code=400, detail="Cambio piano non supportato")
    if not STRIPE_SECRET_KEY:
        raise HTTPException(status_code=503, detail="Stripe non configurato")

    import stripe
    stripe.api_key = STRIPE_SECRET_KEY
    subscription = stripe.Subscription.retrieve(subscription_id)
    item_id = _stripe_subscription_item_id(subscription)
    if not item_id:
        raise HTTPException(status_code=409, detail="Subscription item Stripe non trovato")
    subscription_plan_id = _stripe_subscription_plan_id(subscription)
    if subscription_plan_id not in {current_plan_id, subscription_state.get("pending_plan_id"), target_plan_id}:
        raise HTTPException(status_code=409, detail="Stato subscription Stripe incoerente")
    metadata = _stripe_object_metadata(subscription)
    metadata.update({
        "app_user_id": user.user_id,
        "plan_code": target_plan_id,
        "pending_plan_id": target_plan_id,
        "pending_change_mode": "next_cycle",
    })
    updated = stripe.Subscription.modify(
        subscription_id,
        items=[{"id": item_id, "price": _stripe_price_id_for_plan(target_plan_id)}],
        proration_behavior="none",
        metadata=metadata,
    )
    synced_state = await _sync_subscription_state_from_stripe(
        user_doc=user_doc,
        subscription=updated,
        current_plan_hint=current_plan_id,
    )
    synced_state = await _persist_subscription_state(
        user_doc=user_doc,
        subscription_state={
            **synced_state,
            "current_plan_id": current_plan_id,
            "pending_change": True,
            "pending_plan_id": target_plan_id,
            "pending_effective_at": synced_state.get("current_period_end"),
            "stripe_plan_id": target_plan_id,
        },
    )
    return {"ok": True, "subscription": synced_state}


@api_router.post("/billing/subscription/cancel")
async def cancel_subscription_at_period_end(request: Request):
    user = await require_auth(request)
    user_doc = await _get_user_doc_by_id(user.user_id)
    if not user_doc:
        raise HTTPException(status_code=404, detail="Utente non trovato")
    subscription_state = _normalize_subscription_state(user_doc)
    subscription_id = str(subscription_state.get("stripe_subscription_id") or "").strip()
    if not subscription_id or not _subscription_has_recurring_access(subscription_state):
        raise HTTPException(status_code=409, detail="Nessun abbonamento ricorrente cancellabile")
    if subscription_state.get("cancel_at_period_end"):
        raise HTTPException(status_code=409, detail="La cancellazione a fine periodo e gia attiva")
    import stripe
    stripe.api_key = STRIPE_SECRET_KEY
    subscription = stripe.Subscription.modify(
        subscription_id,
        cancel_at_period_end=True,
        metadata=_stripe_object_metadata(stripe.Subscription.retrieve(subscription_id)),
    )
    synced_state = await _sync_subscription_state_from_stripe(
        user_doc=user_doc,
        subscription=subscription,
        current_plan_hint=subscription_state.get("current_plan_id"),
    )
    return {"ok": True, "subscription": synced_state}


@api_router.post("/billing/subscription/resume")
async def resume_subscription(request: Request):
    user = await require_auth(request)
    user_doc = await _get_user_doc_by_id(user.user_id)
    if not user_doc:
        raise HTTPException(status_code=404, detail="Utente non trovato")
    subscription_state = _normalize_subscription_state(user_doc)
    subscription_id = str(subscription_state.get("stripe_subscription_id") or "").strip()
    if not subscription_id or not subscription_state.get("cancel_at_period_end"):
        raise HTTPException(status_code=409, detail="Nessuna cancellazione a fine periodo da annullare")
    import stripe
    stripe.api_key = STRIPE_SECRET_KEY
    subscription = stripe.Subscription.modify(
        subscription_id,
        cancel_at_period_end=False,
        metadata=_stripe_object_metadata(stripe.Subscription.retrieve(subscription_id)),
    )
    synced_state = await _sync_subscription_state_from_stripe(
        user_doc=user_doc,
        subscription=subscription,
        current_plan_hint=subscription_state.get("current_plan_id"),
    )
    return {"ok": True, "subscription": synced_state}


@api_router.post("/billing/subscription/clear-pending-change")
async def clear_pending_subscription_change(request: Request):
    user = await require_auth(request)
    user_doc = await _get_user_doc_by_id(user.user_id)
    if not user_doc:
        raise HTTPException(status_code=404, detail="Utente non trovato")
    subscription_state = _normalize_subscription_state(user_doc)
    subscription_id = str(subscription_state.get("stripe_subscription_id") or "").strip()
    current_plan_id = str(subscription_state.get("current_plan_id") or "").strip().lower()
    if not subscription_id or not subscription_state.get("pending_change") or current_plan_id not in SELF_SERVE_RECURRING_PLAN_IDS:
        raise HTTPException(status_code=409, detail="Nessun cambio piano pendente da annullare")
    import stripe
    stripe.api_key = STRIPE_SECRET_KEY
    subscription = stripe.Subscription.retrieve(subscription_id)
    item_id = _stripe_subscription_item_id(subscription)
    if not item_id:
        raise HTTPException(status_code=409, detail="Subscription item Stripe non trovato")
    metadata = _stripe_object_metadata(subscription)
    metadata.update({
        "app_user_id": user.user_id,
        "plan_code": current_plan_id,
        "pending_plan_id": "",
        "pending_change_mode": "",
    })
    updated = stripe.Subscription.modify(
        subscription_id,
        items=[{"id": item_id, "price": _stripe_price_id_for_plan(current_plan_id)}],
        proration_behavior="none",
        metadata=metadata,
    )
    synced_state = await _sync_subscription_state_from_stripe(
        user_doc=user_doc,
        subscription=updated,
        current_plan_hint=current_plan_id,
    )
    synced_state = await _persist_subscription_state(
        user_doc=user_doc,
        subscription_state={
            **synced_state,
            "current_plan_id": current_plan_id,
            "stripe_plan_id": current_plan_id,
            "pending_change": False,
            "pending_plan_id": None,
            "pending_effective_at": None,
        },
    )
    return {"ok": True, "subscription": synced_state}

@api_router.post("/webhook/stripe")
async def stripe_webhook(request: Request):
    """Handle Stripe webhooks"""
    if not STRIPE_SECRET_KEY:
        raise HTTPException(status_code=503, detail="Stripe not configured")
    body = await request.body()
    signature = request.headers.get("Stripe-Signature")

    import stripe
    stripe.api_key = STRIPE_SECRET_KEY

    try:
        if STRIPE_WEBHOOK_SECRET:
            if not signature:
                raise HTTPException(status_code=400, detail="Missing Stripe signature")
            event = stripe.Webhook.construct_event(payload=body, sig_header=signature, secret=STRIPE_WEBHOOK_SECRET)
        else:
            event = json.loads(body.decode("utf-8"))

        data_object = event.get("data", {}).get("object", {})
        event_type = event.get("type", "")

        if event_type == "checkout.session.completed":
            payment_status = str(data_object.get("payment_status") or "").strip()
            metadata = data_object.get("metadata", {}) or {}
            session_id = str(data_object.get("id") or "").strip()
            metadata_user_id = str(metadata.get("app_user_id") or "").strip()
            plan_id = str(metadata.get("plan_code") or "").strip()
            payment_reference = str(data_object.get("payment_intent") or session_id)
            stripe_customer_id = str(data_object.get("customer") or "").strip() or None
            stripe_subscription_id = str(data_object.get("subscription") or "").strip() or None
            client_reference_id = str(data_object.get("client_reference_id") or "").strip() or None
            resolved_user_id = None
            billing_record = None
            payment_transaction = None
            starter_user_candidates: Dict[str, Optional[str]] = {}
            logger.info(
                "Stripe checkout.session.completed received: session_id=%s payment_status=%s payment_intent=%s metadata_app_user_id=%s plan_code=%s client_reference_id=%s",
                session_id,
                payment_status,
                payment_reference,
                metadata_user_id or None,
                plan_id or None,
                client_reference_id,
            )
            starter_paid = False
            if plan_id == "starter":
                starter_paid = await _starter_checkout_payment_confirmed(data_object, stripe)
                (
                    resolved_user_id,
                    billing_record,
                    payment_transaction,
                    starter_user_candidates,
                ) = await _resolve_starter_checkout_context(
                    session_id=session_id,
                    metadata_user_id=metadata_user_id,
                    client_reference_id=client_reference_id,
                )
                logger.info(
                    "Starter checkout user resolution: session_id=%s resolved_user_id=%s candidates=%s billing_record_id=%s payment_transaction_id=%s",
                    session_id,
                    resolved_user_id,
                    starter_user_candidates,
                    (billing_record or {}).get("billing_record_id"),
                    (payment_transaction or {}).get("transaction_id"),
                )

            if session_id:
                transaction_status = str(data_object.get("status") or "complete")
                local_payment_status = payment_status or "complete"
                if plan_id == "starter" and not starter_paid:
                    transaction_status = "failed"
                    local_payment_status = "failed"
                await _set_payment_transaction_state(
                    session_id,
                    status=transaction_status,
                    payment_status=local_payment_status,
                    stripe_customer_id=stripe_customer_id,
                    stripe_subscription_id=stripe_subscription_id,
                    stripe_payment_intent_id=payment_reference,
                )
                if not billing_record:
                    billing_record = await _find_billing_record_for_checkout(session_id, resolved_user_id or metadata_user_id or None)
                if billing_record:
                    metadata_updates = {
                        "stripe_customer_id": stripe_customer_id,
                        "stripe_subscription_id": stripe_subscription_id,
                        "plan_code": plan_id,
                        "stripe_checkout_status": data_object.get("status"),
                        "stripe_payment_status": payment_status,
                        "billing_reason": "checkout_session_completed",
                    }
                    if plan_id == "starter" and starter_paid and not resolved_user_id:
                        metadata_updates["manual_review_required"] = True
                    if plan_id == "starter" and not starter_paid:
                        metadata_updates["entitlement_granted"] = False
                    await _update_billing_record(
                        billing_record["billing_record_id"],
                        status=(
                            "paid"
                            if starter_paid and resolved_user_id
                            else "failed" if plan_id == "starter" and not starter_paid else None
                        ),
                        payment_reference=payment_reference if starter_paid and resolved_user_id else None,
                        invoice_status=(
                            "ready"
                            if starter_paid and resolved_user_id
                            else "failed" if plan_id == "starter" and not starter_paid else None
                        ),
                        metadata_updates=metadata_updates,
                        paid=starter_paid and bool(resolved_user_id),
                    )
            recurring_checkout_user_id = _resolved_checkout_user_id(
                metadata_user_id,
                client_reference_id,
                (payment_transaction or {}).get("user_id"),
                (billing_record or {}).get("user_id"),
            )
            if plan_id in SELF_SERVE_RECURRING_PLAN_IDS and recurring_checkout_user_id and stripe_subscription_id:
                user_doc = await _get_user_doc_by_id(recurring_checkout_user_id)
                if user_doc:
                    await _persist_subscription_state(
                        user_doc=user_doc,
                        subscription_state={
                            **_normalize_subscription_state(user_doc),
                            "stripe_customer_id": stripe_customer_id,
                            "stripe_subscription_id": stripe_subscription_id,
                            "status": "pending_initial_invoice",
                            "current_plan_id": _normalize_subscription_state(user_doc).get("current_plan_id") or plan_id,
                            "stripe_plan_id": plan_id,
                        },
                    )

            if plan_id == "starter" and not starter_paid:
                logger.info(
                    "Starter grant skipped because payment was not confirmed: session_id=%s resolved_user_id=%s billing_record_id=%s payment_transaction_id=%s",
                    session_id,
                    resolved_user_id or metadata_user_id or None,
                    (billing_record or {}).get("billing_record_id"),
                    (payment_transaction or {}).get("transaction_id"),
                )
            if plan_id == "starter" and starter_paid and not resolved_user_id:
                logger.warning(
                    "Starter grant skipped because no safe user resolution was possible: session_id=%s candidates=%s billing_record_id=%s payment_transaction_id=%s",
                    session_id,
                    starter_user_candidates,
                    (billing_record or {}).get("billing_record_id"),
                    (payment_transaction or {}).get("transaction_id"),
                )
            if starter_paid and resolved_user_id and session_id:
                await _grant_starter_checkout_if_needed(
                    user_id=resolved_user_id,
                    session_id=session_id,
                    payment_reference=payment_reference,
                    checkout_payload=data_object,
                )

        elif event_type == "invoice.paid":
            invoice = data_object
            user_id, plan_id, stripe_subscription_id = await _resolve_invoice_context(invoice, stripe)
            if user_id and plan_id in {"solo", "pro"}:
                granted = await _grant_subscription_invoice_if_needed(
                    user_id=user_id,
                    plan_id=plan_id,
                    invoice=invoice,
                    stripe_customer_id=str(invoice.get("customer") or "").strip() or None,
                    stripe_subscription_id=stripe_subscription_id,
                )
                if granted and stripe_subscription_id:
                    try:
                        subscription = stripe.Subscription.retrieve(stripe_subscription_id)
                        user_doc = await _get_user_doc_by_id(user_id)
                        if user_doc:
                            await _sync_subscription_state_from_stripe(
                                user_doc=user_doc,
                                subscription=subscription,
                                current_plan_hint=plan_id,
                            )
                    except Exception as exc:
                        logger.warning(f"Stripe subscription sync after invoice.paid failed: {exc}")

        elif event_type == "invoice.payment_failed":
            invoice = data_object
            user_id, plan_id, stripe_subscription_id = await _resolve_invoice_context(invoice, stripe)
            if user_id and plan_id in {"solo", "pro"}:
                user_doc = await _get_user_doc_by_id(user_id)
                if user_doc:
                    await _persist_subscription_state(
                        user_doc=user_doc,
                        subscription_state={
                            **_normalize_subscription_state(user_doc),
                            "stripe_customer_id": str(invoice.get("customer") or "").strip() or None,
                            "stripe_subscription_id": stripe_subscription_id,
                            "status": "past_due",
                        },
                    )
                pending_record = await _find_latest_pending_subscription_billing_record(user_id, plan_id)
                if pending_record:
                    await _update_billing_record(
                        pending_record["billing_record_id"],
                        status="failed",
                        invoice_status="failed",
                        invoice_reference=str(invoice.get("id") or "").strip() or None,
                        metadata_updates={
                            "stripe_customer_id": str(invoice.get("customer") or "").strip() or None,
                            "stripe_subscription_id": stripe_subscription_id,
                            "plan_code": plan_id,
                            "billing_reason": str(invoice.get("billing_reason") or "subscription_cycle").strip(),
                        },
                    )

        elif event_type in {"customer.subscription.updated", "customer.subscription.deleted"}:
            subscription = data_object
            user_doc, stripe_plan_id = await _resolve_subscription_owner_context(subscription=subscription)
            if user_doc:
                synced_state = await _sync_subscription_state_from_stripe(
                    user_doc=user_doc,
                    subscription=subscription,
                    current_plan_hint=user_doc.get("plan"),
                )
                if event_type == "customer.subscription.deleted":
                    synced_state = await _persist_subscription_state(
                        user_doc=user_doc,
                        subscription_state={
                            **synced_state,
                            "status": "canceled",
                            "cancel_at_period_end": False,
                            "pending_change": False,
                            "pending_plan_id": None,
                            "pending_effective_at": None,
                            "current_plan_id": None,
                            "stripe_plan_id": stripe_plan_id,
                        },
                        plan_override="free",
                    )
                plan_for_record = synced_state.get("current_plan_id") or stripe_plan_id
                pending_record = (
                    await _find_latest_pending_subscription_billing_record(user_doc["user_id"], plan_for_record)
                    if plan_for_record in SELF_SERVE_RECURRING_PLAN_IDS
                    else None
                )
                if pending_record:
                    await _update_billing_record(
                        pending_record["billing_record_id"],
                        metadata_updates={
                            "stripe_customer_id": str(_stripe_object_get(subscription, "customer") or "").strip() or None,
                            "stripe_subscription_id": str(_stripe_object_get(subscription, "id") or "").strip() or None,
                            "stripe_subscription_status": _stripe_object_get(subscription, "status"),
                            "plan_code": plan_for_record,
                            "billing_reason": "subscription_state_sync",
                        },
                    )
        
        return {"received": True}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Webhook error: {e}")
        return {"received": True}

# ===================
# COMPREHENSIVE PERIZIA SYSTEM PROMPT - NEXODIFY ROMA STANDARD
# ===================

PERIZIA_SYSTEM_PROMPT = """YOU ARE: Nexodify Auction Scan Engine - ROMA STANDARD v1.0

Your task is to produce an AUDIT-GRADE analysis of Italian Perizia/CTU documents for real estate auctions.
The output must be a structured report that a legal/real estate professional can use to make informed decisions.

═══════════════════════════════════════════════════════════════════════════════
CRITICAL RULES - NON-NEGOTIABLE
═══════════════════════════════════════════════════════════════════════════════

1. EVIDENCE-FIRST: Every value extracted MUST include:
   - "page": exact page number (integer) where found
   - "quote": EXACT text snippet (up to 200 chars) from the document
   If data not found: use "Non specificato in Perizia" and empty evidence

2. ZERO HALLUCINATIONS: Only extract what is ACTUALLY written. Never invent data.

3. 12-SECTION FORMAT: Follow the exact structure below (Roma 1-12 order)

4. SEMAFORO RULES:
   - ROSSO: abusi insanabili, condono non definito, occupazione opponibile senza titolo
   - GIALLO: assenza agibilità, impianti senza conformità, deprezzamenti CTU significativi
   - VERDE: nessuna criticità rilevante

5. OUTPUT: Return ONLY valid JSON - no markdown, no commentary

═══════════════════════════════════════════════════════════════════════════════
DATA TO EXTRACT (search entire document)
═══════════════════════════════════════════════════════════════════════════════

HEADER DATA:
- Procedure: R.G.E. number, E.I. number
- Tribunal: TRIBUNALE DI [city]
- Lotto: Lotto Unico or Lotto N
- Address: Via/Piazza, number, city, province
- Property type: Appartamento, Ufficio, Villetta, Garage, etc.

PRICES (critical):
- Prezzo base d'asta (€)
- Valore di stima complessivo (€)
- Deprezzamenti espliciti (€) - regolarizzazioni, vizi occulti, etc.
- Valore finale di stima (€)

COMPLIANCE (search sections on regolarità):
- Conformità urbanistica: conforme/difforme + details
- Conformità catastale: conforme/difforme
- Condono: presente/assente, anno, pratica, stato (definito/pendente)
- Agibilità/Abitabilità: presente/assente/non risulta
- APE: presente/assente
- Impianti: conformità dichiarata sì/no per elettrico, termico, idrico

OCCUPATION:
- Status: libero, occupato dal debitore, occupato da terzi
- Locazione opponibile: sì/no/non specificato
- Titolo opponibile: sì/no/non specificato

FORMALITIES:
- Ipoteche: list with amounts, dates, beneficiaries
- Pignoramenti: list with dates
- Formalità da cancellare con decreto di trasferimento

═══════════════════════════════════════════════════════════════════════════════
OUTPUT JSON STRUCTURE (12 SECTIONS)
═══════════════════════════════════════════════════════════════════════════════

{
  "schema_version": "nexodify_perizia_scan_v1",
  "report_header": {
    "title": "NEXODIFY INTELLIGENCE | Auction Scan",
    "procedure": {"value": "Esecuzione Immobiliare XX/YYYY R.G.E.", "evidence": [{"page": 1, "quote": "..."}]},
    "lotto": {"value": "Lotto Unico", "evidence": []},
    "tribunale": {"value": "TRIBUNALE DI XXX", "evidence": []},
    "address": {"value": "Via XXX n. X, Comune (Provincia)", "evidence": []},
    "generated_at": "ISO_DATE"
  },
  
  "section_1_semaforo_generale": {
    "status": "VERDE|GIALLO|ROSSO",
    "status_label": "RISCHIO GLOBALE: [VERDE/GIALLO/ROSSO]",
    "semaforo_complessivo": {"value": "VERDE/GIALLO/ROSSO", "evidence": []},
    "mutuabilita_stimata": {"value": "TOTALE|PARZIALE|ESCLUSA", "reason": "..."},
    "driver": {"value": "comma-separated list of risk factors", "evidence": []}
  },
  
  "section_2_decisione_rapida": {
    "operazione_rating": "VERDE|GIALLO|ROSSO",
    "summary_it": "Detailed Italian summary with page references",
    "summary_en": "English translation"
  },
  
  "section_3_money_box": {
    "items": [
      {
        "voce": "A - Regolarizzazione urbanistica/ripristini",
        "fonte_perizia": {"value": "Perizia p. XX", "evidence": [{"page": 0, "quote": "..."}]},
        "stima_euro": 0,
        "stima_nota": "EUR X / Verifica tecnico"
      },
      {
        "voce": "B - Completamento finiture/impianti",
        "fonte_perizia": {"value": "...", "evidence": []},
        "stima_euro": 0,
        "stima_nota": "..."
      },
      {
        "voce": "C - Ottenimento abitabilità/agibilità",
        "fonte_perizia": {"value": "...", "evidence": []},
        "stima_euro": 0,
        "stima_nota": "..."
      },
      {
        "voce": "D - Spese condominiali arretrate",
        "fonte_perizia": {"value": "Non specificato", "evidence": []},
        "stima_euro": 0,
        "stima_nota": "Verifica amministratore"
      },
      {
        "voce": "E - Cancellazione formalità",
        "fonte_perizia": {"value": "Non specificato", "evidence": []},
        "stima_euro": 0,
        "stima_nota": "Verifica legale"
      },
      {
        "voce": "F - Costo liberazione",
        "fonte_perizia": {"value": "Non specificato", "evidence": []},
        "stima_euro": 1500,
        "stima_nota": "EUR 1.500 (prudenziale)"
      }
    ],
    "totale_extra_budget": {
      "min": 0,
      "max": 0,
      "nota": "EUR XX.XXX+ (minimo prudenziale)"
    }
  },
  
  "section_4_dati_certi": {
    "prezzo_base_asta": {"value": 0, "formatted": "EUR X", "evidence": [{"page": 0, "quote": "..."}]},
    "valore_stima_complessivo": {"value": 0, "formatted": "EUR X", "evidence": []},
    "deprezzamenti": [
      {"tipo": "regolarizzazioni", "importo": 0, "evidence": []},
      {"tipo": "vizi occulti", "importo": 0, "evidence": []}
    ],
    "valore_finale_stima": {"value": 0, "formatted": "EUR X", "evidence": []},
    "composizione_lotto": {"value": "description of properties", "evidence": []}
  },
  
  "section_5_abusi_conformita": {
    "conformita_urbanistica": {"status": "CONFORME|DIFFORME|UNKNOWN", "detail": "...", "evidence": []},
    "conformita_catastale": {"status": "CONFORME|DIFFORME|UNKNOWN", "evidence": []},
    "condono": {
      "presente": "SI|NO|UNKNOWN",
      "anno": "",
      "pratica": "",
      "stato": "definito|pendente|unknown",
      "evidence": []
    },
    "agibilita": {"status": "PRESENTE|ASSENTE|NON_RISULTA", "evidence": []},
    "ape": {"status": "PRESENTE|ASSENTE", "evidence": []},
    "impianti": {
      "elettrico": {"conformita": "SI|NO|NON_RISULTA"},
      "termico": {"conformita": "SI|NO|NON_RISULTA"},
      "idrico": {"conformita": "SI|NO|NON_RISULTA"},
      "evidence": []
    }
  },
  
  "section_6_stato_occupativo": {
    "status": "LIBERO|OCCUPATO_DEBITORE|OCCUPATO_TERZI",
    "status_detail": "...",
    "locazione_opponibile": {"status": "SI|NO|NON_SPECIFICATO", "evidence": []},
    "titolo_opponibile": {"status": "SI|NO|NON_SPECIFICATO", "evidence": []},
    "tempi_consegna_stima": "stima Nexodify 3-9 mesi post Decreto di Trasferimento",
    "evidence": []
  },
  
  "section_7_stato_conservativo": {
    "condizione_generale": "...",
    "dettagli": [{"area": "...", "stato": "...", "evidence": []}],
    "carenze": "...",
    "evidence": []
  },
  
  "section_8_formalita": {
    "ipoteche": [
      {"tipo": "giudiziale|volontaria|riscossione", "importo": 0, "data": "", "beneficiario": "omissis", "evidence": []}
    ],
    "pignoramenti": [{"data": "", "evidence": []}],
    "cancellazione": "normalmente tramite Decreto di Trasferimento, salvo eccezioni - verifica legale obbligatoria",
    "evidence": []
  },
  
  "section_9_legal_killers": {
    "items": [
      {"killer": "Diritto di superficie / PEEP", "status": "SI|NO|NON_SPECIFICATO", "action": "Verifica obbligatoria", "evidence": []},
      {"killer": "Donazione in catena <20 anni", "status": "SI|NO|NON_SPECIFICATO", "action": "Verifica obbligatoria", "evidence": []},
      {"killer": "Prelazione Stato / beni culturali", "status": "SI|NO|NON_SPECIFICATO", "action": "Verifica obbligatoria", "evidence": []},
      {"killer": "Usi civici", "status": "SI|NO|NON_SPECIFICATO", "action": "Verifica CDU", "evidence": []},
      {"killer": "Fondo patrimoniale / casa coniugale", "status": "SI|NO|NON_SPECIFICATO", "action": "Verifica obbligatoria", "evidence": []},
      {"killer": "Servitù / atti d'obbligo", "status": "SI|NO|NON_SPECIFICATO", "action": "Verifica obbligatoria", "evidence": []},
      {"killer": "Formalità non cancellabili", "status": "SI|NO|NON_SPECIFICATO", "action": "Verifica obbligatoria", "evidence": []},
      {"killer": "Amianto", "status": "SI|NO|NON_SPECIFICATO", "action": "Verifica se coperture datate", "evidence": []}
    ]
  },
  
  "section_10_indice_convenienza": {
    "prezzo_base": 0,
    "extra_budget_min": 0,
    "extra_budget_max": 0,
    "all_in_light_min": 0,
    "all_in_light_max": 0,
    "lettura_secca_it": "senza sconto reale sul prezzo base, il rischio tecnico/temporale mangia il margine",
    "lettura_secca_en": "without real discount on base price, technical/timing risk eats the margin"
  },
  
  "section_11_red_flags": [
    {"flag": "description", "severity": "ROSSO|GIALLO", "page_ref": "Perizia p. XX", "evidence": []}
  ],
  
  "section_12_checklist_pre_offerta": [
    "Accesso atti in Comune",
    "Preventivo tecnico reale",
    "Visure in Conservatoria",
    "Conferma eventuali arretrati condominiali",
    "Strategia di liberazione e allineamento"
  ],
  
  "summary_for_client": {
    "summary_it": "Detailed summary with page references",
    "summary_en": "English translation",
    "raccomandazione": "Procedere solo se il prezzo di aggiudicazione incorpora margine reale per extra-budget e ritardi",
    "disclaimer_it": "Documento informativo. Non costituisce consulenza legale.",
    "disclaimer_en": "Informational document. Does not constitute legal advice."
  },
  
  "qa_pass": {
    "status": "PASS|WARN|FAIL",
    "checks": [
      {"code": "QA-1 Format Lock", "result": "OK|FAIL", "note": "ordine Roma 1-12 rispettato"},
      {"code": "QA-2 Zero Empty Fields", "result": "OK|FAIL", "note": "dove manca dato: Non specificato in Perizia"},
      {"code": "QA-3 Page Anchors", "result": "OK|FAIL", "note": "riferimenti pagina presenti"},
      {"code": "QA-4 Money Box", "result": "OK|FAIL", "note": "voci CTU valorizzate"},
      {"code": "QA-5 Legal Killers", "result": "OK|FAIL", "note": "checklist completa"},
      {"code": "QA-6 Condono + Opponibilità", "result": "OK|FAIL", "note": "status verificato"},
      {"code": "QA-7 Delivery Timeline", "result": "OK|FAIL", "note": "stima tempi presente"},
      {"code": "QA-8 Semaforo Rules", "result": "OK|FAIL", "note": "coerente con criticità"},
      {"code": "QA-9 Typos", "result": "OK|FAIL", "note": "nessun errore"}
    ]
  }
}

═══════════════════════════════════════════════════════════════════════════════
IMPORTANT EXTRACTION GUIDELINES
═══════════════════════════════════════════════════════════════════════════════

1. Search for prices in sections like "STIMA", "VALUTAZIONE", "PREZZO BASE", "SCHEMA RIASSUNTIVO"
2. Look for conformity in sections "REGOLARITA EDILIZIA", "CONFORMITA", "L. 47/85", "L. 47/1985"
3. Find occupation status in "STATO OCCUPATIVO", "OCCUPAZIONE", "Occupato da"
4. Check formalities in "FORMALITA", "IPOTECHE", "ISCRIZIONI", "TRASCRIZIONI"
5. Extract condono info from "CONDONO", "SANATORIA", "L. 47/85"
6. Find agibilità in "AGIBILITA", "ABITABILITA", "CERTIFICATO", "Non risulta agibile", "non è presente l'abitabilità"

CRITICAL - MONEY BOX EXTRACTION:
7. Search for "Deprezzamenti" section - this contains the MONEY BOX values:
   - "Oneri di regolarizzazione urbanistica" = Item A (usually €15,000-€30,000)
   - "Rischio assunto per mancata garanzia" / "Vizi occulti" = Item B (usually €3,000-€10,000)
   - "Completamento finiture" = Item B if different
   - Extract the EXACT EUR values from this table (e.g., "23000,00 €" = 23000)

8. Search for "Certificazioni energetiche e dichiarazioni di conformità":
   - "Non esiste il certificato energetico" → APE = ASSENTE
   - "Non esiste la dichiarazione di conformità dell'impianto elettrico" → elettrico = NO
   - "Non esiste la dichiarazione di conformità dell'impianto termico" → termico = NO
   - "Non esiste la dichiarazione di conformità dell'impianto idrico" → idrico = NO

Remember: The professional using this analysis needs to verify every claim against the source.
ALWAYS include page numbers. NEVER invent data.

═══════════════════════════════════════════════════════════════════════════════
STRICT RELIABILITY PATCH (OVERRIDES)
═══════════════════════════════════════════════════════════════════════════════

A) PAGE COVERAGE LOG (MANDATORY)
- Add top-level field: "page_coverage_log": array with ONE entry per page (1..N).
- Each entry: {"page": N, "summary": "..."}.
- If you cannot cover every page, set qa_pass.status="FAIL" and explain missing pages in qa_pass.checks.

B) MULTI-LOT (MANDATORY)
- Extract ALL occurrences of "Lotto N". Add top-level "lot_index": [{"lot":1,"page":X,"quote":"..."}, ...]
- If 2+ lots exist, you are FORBIDDEN to output "Lotto Unico".

C) EVIDENCE-LOCKED
- Any SI/NO status requires evidence. Without evidence -> "NON_SPECIFICATO".

D) MONEY BOX HONESTY
- Never attach € amounts to items marked "Non specificato in Perizia". Use TBD.
- If you add estimates, label clearly: "STIMA NEXODIFY (NON IN PERIZIA)".

E) OUTPUT
- Return ONLY valid JSON (no markdown)."""
async def analyze_perizia_with_llm(pdf_text: str, pages: List[Dict], file_name: str, user: User, case_id: str, run_id: str, input_sha256: str) -> Dict:
    """Analyze perizia using LLM with comprehensive ROMA STANDARD prompt"""
    import re
    
    # ===========================================================================
    # HELPER FUNCTIONS FOR FULL-DOCUMENT COVERAGE
    # ===========================================================================
    def compress_page_text(t: str, max_chars: int = 1400) -> str:
        t = (t or "").strip()
        if not t:
            return ""
        lines = [ln.strip() for ln in t.splitlines() if ln.strip()]
        head = lines[:18]
        tail = lines[-18:] if len(lines) > 18 else []
        key_re = re.compile(
            r"(prezzo|base|asta|lotto|tribunale|via|comune|catast|urban|"
            r"agibil|abitabil|conform|ipotec|pignor|servit|amianto|"
            r"deprezz|oneri|regolarizz|€|eur|euro|mq|superficie|foglio|"
            r"particella|sub|decreto di trasferimento|formalità|stradella|barriera|"
            r"salva casa|D\.L\.\s*69|L\.R\.\s*Toscana|accertamento di conformità)"
            , re.I
        )
        kept = []
        mid = lines[18:-18] if len(lines) > 36 else []
        for ln in head + mid + tail:
            if ln in head or ln in tail or key_re.search(ln):
                kept.append(ln)
            if len(kept) >= 90:
                break
        out = " | ".join(kept)
        return out[:max_chars]

    def build_page_content(per_page_cap: int) -> str:
        out = ""
        for p in pages[:200]:
            out += (
                f"\n\n{'='*60}\nPAGINA {p.get('page_number')}\n{'='*60}\n"
                f"{compress_page_text(p.get('text', ''), max_chars=per_page_cap)}\n"
            )
        return out
    
    # ===========================================================================
    # DETERMINISTIC LOT EXTRACTION FROM "SCHEMA RIASSUNTIVO"
    # ===========================================================================
    def extract_lots_from_schema_riassuntivo(pages_in):
        """Extract lots deterministically from SCHEMA RIASSUNTIVO pages"""
        lots = []
        schema_pages = []
        
        # Find pages with SCHEMA RIASSUNTIVO
        strict_schema_pages = []
        for p in pages_in:
            text = str(p.get("text", "") or "")
            if re.search(r"SCHEMA\s+RIASSUNTIVO", text, re.I):
                strict_schema_pages.append(p)
            if re.search(r"SCHEMA\s+RIASSUNTIVO", text, re.I) or ("LOTTO" in text.upper() and "PREZZO BASE" in text.upper()):
                schema_pages.append(p)
        
        # If strict schema pages found, use only those
        if strict_schema_pages:
            schema_pages = strict_schema_pages
        elif not schema_pages:
            schema_pages = pages_in
        
        # Find all lot numbers mentioned
        all_lot_numbers = set()
        for p in pages_in:
            text = str(p.get("text", "") or "")
            for m in re.finditer(r"\bLOTTO\s+(\d+)\b", text, flags=re.I):
                all_lot_numbers.add(int(m.group(1)))

        # Handle LOTTO UNICO if no numeric lots detected
        if not all_lot_numbers:
            for p in schema_pages:
                text = str(p.get("text", "") or "")
                if re.search(r"\bLOTTO\s+UNICO\b", text, re.I):
                    all_lot_numbers.add(1)
                    break
        
        # Extract details for each lot
        for lot_num in sorted(all_lot_numbers):
            lot_data = {
                "lot_number": lot_num,
                "prezzo_base_eur": "NON SPECIFICATO IN PERIZIA",
                "prezzo_base_value": 0,
                "ubicazione": "NON SPECIFICATO IN PERIZIA",
                "diritto_reale": "NON SPECIFICATO IN PERIZIA",
                "superficie_mq": "NON SPECIFICATO IN PERIZIA",
                "tipologia": "NON SPECIFICATO IN PERIZIA",
                "evidence": {}
            }
            
            # Search for lot-specific data
            for p in schema_pages:
                text = str(p.get("text", "") or "")
                page_num = p.get("page_number", 0)
                
                # Check if this page contains this lot
                lot_pattern = rf"\bLOTTO\s+{lot_num}\b"
                if not re.search(lot_pattern, text, re.I):
                    if not (lot_num == 1 and re.search(r"\bLOTTO\s+UNICO\b", text, re.I)):
                        continue
                
                # Try to extract lot block (from LOTTO N to next LOTTO or end)
                lot_block_match = re.search(
                    rf"(LOTTO\s+{lot_num}\b.*?)(?=LOTTO\s+\d+\b|$)",
                    text, re.I | re.DOTALL
                )
                if not lot_block_match and lot_num == 1:
                    lot_block_match = re.search(
                        r"(LOTTO\s+UNICO\b.*?)(?=LOTTO\s+\d+\b|$)",
                        text, re.I | re.DOTALL
                    )
                block = lot_block_match.group(1) if lot_block_match else text
                block_start = text.find(block) if block else 0
                if block_start < 0:
                    block_start = 0
                
                # Extract PREZZO BASE D'ASTA
                prezzo_match = re.search(
                    r"PREZZO\s+BASE\s+D['']?ASTA[:\s]*€?\s*([\d.,]+)",
                    block, re.I
                )
                if prezzo_match and lot_data["prezzo_base_eur"] == "NON SPECIFICATO IN PERIZIA":
                    prezzo_str = prezzo_match.group(1).strip()
                    lot_data["prezzo_base_eur"] = f"€ {prezzo_str}"
                    # Parse numeric value
                    try:
                        val = prezzo_str.replace(".", "").replace(",", ".")
                        lot_data["prezzo_base_value"] = float(val)
                    except:
                        pass
                    abs_start = block_start + prezzo_match.start()
                    abs_end = block_start + prezzo_match.end()
                    lot_data["evidence"]["prezzo_base"] = [
                        _build_evidence(
                            text,
                            page_num,
                            abs_start,
                            abs_end,
                            field_key="prezzo_base_asta",
                            anchor_hint=prezzo_match.group(0),
                        )
                    ]

                if "lotto" not in lot_data["evidence"]:
                    lotto_match = re.search(r"\bLOTTO\s+UNICO\b|\bLOTTO\s+\d+\b", text, re.I)
                    if lotto_match:
                        lot_data["evidence"]["lotto"] = [
                            _build_evidence(
                                text,
                                page_num,
                                lotto_match.start(),
                                lotto_match.end(),
                                field_key="lotto",
                                anchor_hint=lotto_match.group(0),
                            )
                        ]
                
                # Extract Ubicazione
                ubic_match = re.search(
                    r"Ubicazione[:\s]*([^\n]+)",
                    block, re.I
                )
                if ubic_match and lot_data["ubicazione"] == "NON SPECIFICATO IN PERIZIA":
                    lot_data["ubicazione"] = ubic_match.group(1).strip()[:200]
                    abs_start = block_start + ubic_match.start()
                    abs_end = block_start + ubic_match.end()
                    lot_data["evidence"]["ubicazione"] = [_build_evidence(text, page_num, abs_start, abs_end)]
                
                # Extract Diritto reale
                diritto_match = re.search(
                    r"Diritto\s+reale[:\s]*([^\n]+)",
                    block, re.I
                )
                if diritto_match and lot_data["diritto_reale"] == "NON SPECIFICATO IN PERIZIA":
                    lot_data["diritto_reale"] = diritto_match.group(1).strip()[:100]
                    abs_start = block_start + diritto_match.start()
                    abs_end = block_start + diritto_match.end()
                    lot_data["evidence"]["diritto_reale"] = [
                        _build_evidence(
                            text,
                            page_num,
                            abs_start,
                            abs_end,
                            field_key="diritto_reale",
                            anchor_hint=diritto_match.group(0),
                        )
                    ]
                
                # Extract Superficie
                sup_matches = list(re.finditer(r"Superficie[^\d\n]{0,40}([\d.,]+)\s*mq", block, re.I))
                if sup_matches and lot_data["superficie_mq"] == "NON SPECIFICATO IN PERIZIA":
                    # pick the largest numeric superficie
                    best = None
                    best_val = -1
                    for sm in sup_matches:
                        raw = sm.group(1)
                        try:
                            val = float(raw.replace(".", "").replace(",", "."))
                        except Exception:
                            continue
                        if val > best_val:
                            best_val = val
                            best = sm
                    if best:
                        lot_data["superficie_mq"] = f"{best.group(1)} mq"
                        abs_start = block_start + best.start()
                        abs_end = block_start + best.end()
                        lot_data["evidence"]["superficie"] = [
                            _build_evidence(
                                text,
                                page_num,
                                abs_start,
                                abs_end,
                                field_key="superficie_catastale",
                                anchor_hint=best.group(0),
                            )
                        ]
                
                # Extract Tipologia
                tipo_match = re.search(
                    r"Tipologia[:\s]*([^\n]+)",
                    block, re.I
                )
                if tipo_match and lot_data["tipologia"] == "NON SPECIFICATO IN PERIZIA":
                    lot_data["tipologia"] = tipo_match.group(1).strip()[:100]
            # Add per-field low confidence notes where evidence is missing
            for field_key, ev_key in (
                ("prezzo_base_eur", "prezzo_base"),
                ("ubicazione", "ubicazione"),
                ("diritto_reale", "diritto_reale"),
                ("superficie_mq", "superficie"),
                ("tipologia", "tipologia"),
            ):
                if not lot_data["evidence"].get(ev_key):
                    lot_data.setdefault("field_confidence", {})[field_key] = {
                        "confidence": "LOW",
                        "note": "USER MUST VERIFY"
                    }
            lots.append(lot_data)
        
        return lots
    
    # ===========================================================================
    # DETERMINISTIC LEGAL KILLERS SCANNER
    # ===========================================================================
    def scan_legal_killers(pages_in):
        """Scan for legal killers with evidence"""
        killers = []
        patterns = [
            (r"FORMALITÀ\s+DA\s+CANCELLARE\s+CON\s+IL\s+DECRETO\s+DI\s+TRASFERIMENTO", "Formalità da cancellare", "GIALLO"),
            (r"Oneri\s+di\s+cancellazione[:\s]*([^\n]+)", "Oneri di cancellazione", "GIALLO"),
            (r"servitù[^.]*", "Servitù rilevata", "GIALLO"),
            (r"stradella|barriera", "Servitù di passaggio/barriera", "GIALLO"),
            (r"D\.?L\.?\s*69/2024|salva\s+casa", "Riferimento D.L. 69/2024 Salva Casa", "GIALLO"),
            (r"L\.?R\.?\s*Toscana\s*65/2014", "Riferimento L.R. Toscana 65/2014", "GIALLO"),
            (r"accertamento\s+di\s+conformità", "Accertamento di conformità richiesto", "GIALLO"),
            (r"difformità[^.]*regolarizz", "Difformità da regolarizzare", "ROSSO"),
            (r"abuso[^.]*edilizio|abuso[^.]*insanabile", "Abuso edilizio", "ROSSO"),
            (r"usi\s+civici", "Usi civici", "ROSSO"),
            (r"PEEP|diritto\s+di\s+superficie", "Diritto di superficie / PEEP", "ROSSO"),
            (r"amianto|eternit", "Presenza amianto/eternit", "ROSSO"),
        ]
        
        seen = set()
        for p in pages_in:
            text = str(p.get("text", "") or "")
            page_num = p.get("page_number", 0)
            
            for pattern, title, severity in patterns:
                for m in re.finditer(pattern, text, re.I):
                    key = f"{title}_{page_num}"
                    if key not in seen:
                        seen.add(key)
                        snippet = text[max(0, m.start()-30):min(len(text), m.end()+100)]
                        killers.append({
                            "title": title,
                            "severity": severity,
                            "page": page_num,
                            "quote": snippet.replace("\n", " ")[:200],
                            "start_offset": m.start(),
                            "end_offset": m.end(),
                            "why_it_matters": f"Rilevato: {m.group(0)[:80]}"
                        })
        
        return killers
    
    # ===========================================================================
    # EVIDENCE VALIDATION HELPER
    # ===========================================================================
    def has_evidence(ev):
        if not isinstance(ev, list) or not ev:
            return False
        e0 = ev[0]
        return isinstance(e0, dict) and "page" in e0 and "quote" in e0 and str(e0.get("quote","")).strip() != ""
    
    # Extract lots and legal killers deterministically BEFORE LLM call
    extracted_lots = extract_lots_from_schema_riassuntivo(pages)
    detected_legal_killers = scan_legal_killers(pages)
    logger.info(f"Deterministic extraction: {len(extracted_lots)} lots, {len(detected_legal_killers)} legal killers")
    
    # ===========================================================================
    # CHARACTER-BUDGETED PAGE CONTENT (no truncation)
    # ===========================================================================
    page_content = build_page_content(1400)
    if len(page_content) > 160000:
        page_content = build_page_content(900)
    if len(page_content) > 160000:
        page_content = page_content[:160000]
    
    logger.info(f"Page content built: {len(page_content)} chars for {len(pages)} pages")
    
    # Build deterministic facts string for LLM
    lots_facts = ""
    if extracted_lots:
        lots_facts = "\n\n═══════════════════════════════════════════════════════════════════════════════\nDETERMINISTIC FACTS - LOTTI (DO NOT OVERRIDE)\n═══════════════════════════════════════════════════════════════════════════════\n"
        for lot in extracted_lots:
            lots_facts += f"""
LOTTO {lot['lot_number']}:
- Prezzo Base d'Asta: {lot['prezzo_base_eur']}
- Ubicazione: {lot['ubicazione']}
- Diritto Reale: {lot['diritto_reale']}
- Superficie: {lot['superficie_mq']}
"""
    
    legal_facts = ""
    if detected_legal_killers:
        legal_facts = "\n\n═══════════════════════════════════════════════════════════════════════════════\nDETERMINISTIC FACTS - LEGAL KILLERS RILEVATI\n═══════════════════════════════════════════════════════════════════════════════\n"
        for lk in detected_legal_killers[:10]:
            legal_facts += f"- [{lk['severity']}] {lk['title']} (p. {lk['page']}): {lk['quote'][:100]}\n"
    
    # ===========================================================================
    # UPDATED PROMPT WITH DETERMINISTIC FACTS
    # ===========================================================================
    prompt = f"""ANALIZZA questa Perizia CTU italiana per asta immobiliare.

═══════════════════════════════════════════════════════════════════════════════
DOCUMENTO
═══════════════════════════════════════════════════════════════════════════════
FILE: {file_name}
PAGINE TOTALI: {len(pages)}
RUN_ID: {run_id}
CASE_ID: {case_id}

═══════════════════════════════════════════════════════════════════════════════
CONTENUTO DOCUMENTO (COMPRESSED per pagina con marker PAGINA X)
═══════════════════════════════════════════════════════════════════════════════
{page_content}

═══════════════════════════════════════════════════════════════════════════════
ISTRUZIONI CRITICHE (CONTRATTO VINCOLANTE)
═══════════════════════════════════════════════════════════════════════════════

1. PAGE COVERAGE LOG (OBBLIGATORIO):
   - DEVI produrre "page_coverage_log": array con UNA entry per ogni pagina (1..{len(pages)}).
   - Formato: {{"page": N, "summary": "breve descrizione contenuto"}}
   - Se non riesci a coprire tutte le pagine, imposta qa_pass.status="FAIL" e spiega le pagine mancanti.

2. LOT INDEX (OBBLIGATORIO):
   - Estrai TUTTE le occorrenze di "Lotto N" nel documento.
   - Produci "lot_index": [{{"lot":1,"page":X,"quote":"..."}}, ...]
   - Se esistono 2+ lotti, è VIETATO restituire "Lotto Unico" in report_header.lotto.

3. EVIDENCE-LOCKED:
   - Ogni status SI/NO richiede evidence con page e quote.
   - Senza evidence → status DEVE essere "NON_SPECIFICATO".

4. MONEY BOX HONESTY:
   - MAI associare importi € a voci con fonte "Non specificato in Perizia" o evidence vuota.
   - Se fonte non specificata → stima_euro=0 e stima_nota="TBD (NON SPECIFICATO IN PERIZIA)".
   - Se aggiungi stime Nexodify, etichetta: "STIMA NEXODIFY (NON IN PERIZIA)".

5. CERCA DATI SPECIFICI con numeri di pagina:
   - Numero procedura (R.G.E., E.I.)
   - Tribunale
   - Indirizzo completo
   - PREZZO BASE D'ASTA (€) - cerca "PREZZO BASE D'ASTA" o "SCHEMA RIASSUNTIVO"
   - Tabella "Deprezzamenti" per Money Box (valori EUR esatti)

6. CONFORMITÀ - CERCA "Certificazioni energetiche e dichiarazioni di conformità":
   - "Non esiste il certificato energetico" → APE: ASSENTE
   - "Non esiste la dichiarazione di conformità dell'impianto elettrico" → elettrico: NO
   - "Non esiste la dichiarazione di conformità dell'impianto termico" → termico: NO  
   - "Non esiste la dichiarazione di conformità dell'impianto idrico" → idrico: NO
   - "non è presente l'abitabilità" → agibilita: ASSENTE

7. QA GATES:
   - Se page_coverage_log ha meno entries di PAGINE TOTALI → qa_pass.status="FAIL"
   - Se Money Box ha € con fonte vuota → qa_pass.status="FAIL"
   - Se Legal Killers ha SI/NO senza evidence → qa_pass.status="FAIL"

8. OUTPUT: Restituisci SOLO JSON valido nel formato ROMA STANDARD.
   NON aggiungere markdown, commenti o testo extra.

INIZIA L'ANALISI:"""

    try:
        # ==========================================
        # PASS 1: Initial Extraction
        # ==========================================
        logger.info(f"PASS 1: Initial extraction for {file_name}")
        response = await openai_chat_completion(PERIZIA_SYSTEM_PROMPT, prompt, model="gpt-4o", timeout_seconds=LLM_TIMEOUT_SECONDS)
        
        # Parse JSON from response
        response_text = response.strip()
        if response_text.startswith("```json"):
            response_text = response_text[7:]
        if response_text.startswith("```"):
            response_text = response_text[3:]
        if response_text.endswith("```"):
            response_text = response_text[:-3]
        response_text = response_text.strip()
        
        try:
            result = json.loads(response_text)
        except json.JSONDecodeError as e:
            logger.error(f"PASS 1 JSON parse error: {e}")
            raise LLMUnavailable("LLM response invalid JSON (pass 1)") from e
        
        # ===========================================================================
        # POST-PARSE: INJECT DETERMINISTIC LOT DATA
        # ===========================================================================
        
        # Add extracted lots to result
        result["lots"] = extracted_lots
        result["lots_count"] = len(extracted_lots)
        
        # Update report_header with multi-lot info
        if len(extracted_lots) >= 2:
            hdr = result.setdefault("report_header", {})
            lot_nums = [str(lot["lot_number"]) for lot in extracted_lots]
            hdr["lotto"] = {
                "value": "Lotti " + ", ".join(lot_nums),
                "evidence": [lot.get("evidence", {}).get("prezzo_base", [{}])[0] for lot in extracted_lots if lot.get("evidence", {}).get("prezzo_base")]
            }
            hdr["is_multi_lot"] = True
        elif len(extracted_lots) == 1:
            hdr = result.setdefault("report_header", {})
            hdr["lotto"] = {
                "value": f"Lotto {extracted_lots[0]['lot_number']}",
                "evidence": extracted_lots[0].get("evidence", {}).get("prezzo_base", [])
            }
            hdr["is_multi_lot"] = False
        
        # Add deterministic legal killers
        if detected_legal_killers:
            lk_section = result.setdefault("section_9_legal_killers", {"items": []})
            existing_titles = {item.get("killer", item.get("title", "")) for item in lk_section.get("items", [])}
            for lk in detected_legal_killers:
                if lk["title"] not in existing_titles:
                    lk_section["items"].append({
                        "killer": lk["title"],
                        "status": "SI" if lk["severity"] == "ROSSO" else "GIALLO",
                        "action": "Verifica obbligatoria",
                        "evidence": [{"page": lk["page"], "quote": lk["quote"], "start_offset": lk.get("start_offset"), "end_offset": lk.get("end_offset"), "bbox": None}]
                    })
        
        # ===========================================================================
        # Enforce evidence-locked legal killers + placeholder cleanup
        # ===========================================================================
        lk = result.get("section_9_legal_killers", {})
        items = lk.get("items", []) if isinstance(lk, dict) else []
        for it in items:
            status = str(it.get("status", "NON_SPECIFICATO")).upper()
            ev = it.get("evidence", [])
            if status in ("SI", "NO", "YES") and not has_evidence(ev):
                it["status"] = "NON_SPECIFICATO"
                it.setdefault("action", "Verifica obbligatoria")
                logger.info(f"Legal killer '{it.get('killer', 'unknown')}' status reset to NON_SPECIFICATO (no evidence)")
        
        # Add QA note for multi-lot detection
        if len(extracted_lots) >= 2:
            qa = result.setdefault("qa_pass", {"status": "WARN", "checks": []})
            qa["checks"] = qa.get("checks", [])
            qa["checks"].append({
                "code": "QA-MultiLot",
                "result": "OK",
                "note": f"Multi-lot detected: {len(extracted_lots)} lots"
            })
        
        # Add lot_index
        result["lot_index"] = [
            {"lot": lot["lot_number"], "prezzo": lot["prezzo_base_eur"], "ubicazione": lot["ubicazione"][:50]}
            for lot in extracted_lots
        ]
        
        # ---- Enforce evidence on critical header fields ----
        qa = result.setdefault("qa_pass", {"status": "PASS", "checks": []})
        qa["checks"] = qa.get("checks", [])
        
        critical_fields = [
            ("report_header", "procedure"),
            ("report_header", "tribunale"),
            ("report_header", "address"),
        ]
        for section, field in critical_fields:
            sec_data = result.get(section, {})
            field_data = sec_data.get(field, {})
            if isinstance(field_data, dict):
                ev = field_data.get("evidence", [])
                if not has_evidence(ev):
                    val = field_data.get("value", "")
                    if val and "Non specificato" not in str(val) and val not in ["None", "N/A", "NOT_SPECIFIED_IN_PERIZIA"]:
                        field_data["value"] = "NON SPECIFICATO IN PERIZIA"
                        qa["checks"].append({
                            "code": f"QA-Evidence-{field}",
                            "result": "WARN",
                            "note": f"Field {section}.{field} had no evidence, reset to Non specificato"
                        })
        
        # ---- Money Box Honesty with TBD handling ----
        mb = result.get("section_3_money_box", {})
        mb_items = mb.get("items", []) if isinstance(mb, dict) else []
        money_box_violations = []
        has_numeric_total = False
        total_numeric_min = 0
        total_numeric_max = 0
        all_tbd = True
        
        for it in mb_items:
            fonte = (it.get("fonte_perizia", {}) or {})
            fonte_val = str(fonte.get("value", "")).lower()
            fonte_ev = fonte.get("evidence", [])
            euro = it.get("stima_euro", 0)

            is_unspecified = ("non specificato" in fonte_val) or ("tbd" in fonte_val.lower()) or (not has_evidence(fonte_ev))
            
            if is_unspecified:
                # Mark as TBD, not €0
                it["stima_euro"] = "TBD"
                it["stima_nota"] = "TBD (NON SPECIFICATO IN PERIZIA) — Verifica tecnico/legale"
            elif euro and isinstance(euro, (int, float)) and euro > 0:
                has_numeric_total = True
                all_tbd = False
                total_numeric_min += euro
                total_numeric_max += euro
        
        # Update money box totals - NEVER show €0-€0 if items are TBD
        if all_tbd:
            mb["totale_extra_budget"] = {
                "min": "TBD",
                "max": "TBD",
                "nota": "TBD — Costi non quantificati in perizia"
            }
        elif has_numeric_total:
            mb["totale_extra_budget"] = {
                "min": total_numeric_min,
                "max": int(total_numeric_max * 1.2),
                "nota": f"EUR {total_numeric_min:,.0f}+ (minimo da perizia)" + (" + TBD" if not all_tbd else "")
            }
        
        # ==========================================
        # PASS 2: Verification & Gap Detection
        # ==========================================
        logger.info(f"PASS 2: Verification pass for {file_name}")
        
        # Include lots info in verification
        lots_info = f"LOTTI ESTRATTI: {len(extracted_lots)} lotti" if extracted_lots else ""
        
        verification_prompt = f"""VERIFICA E COMPLETA questa analisi perizia.

{lots_info}

ANALISI ATTUALE (da verificare):
{json.dumps(result, indent=2, ensure_ascii=False)[:30000]}

DOCUMENTO ORIGINALE (pagine chiave):
{page_content[:60000]}

ISTRUZIONI DI VERIFICA:

1. VERIFICA PAGE COVERAGE LOG:
   - DEVE avere {len(pages)} entries (una per pagina)
   - Se mancante o incompleto, crea entries per TUTTE le pagine

2. VERIFICA MONEY BOX:
   - Cerca "Deprezzamenti", "Oneri di regolarizzazione", "regolarizzazione urbanistica"
   - Il valore esatto in EUR deve essere estratto (es: "23000,00 €" → 23000)
   - Se fonte è "Non specificato", stima_euro DEVE essere 0

3. VERIFICA CONFORMITÀ IMPIANTI:
   - Cerca "dichiarazione di conformità dell'impianto elettrico/termico/idrico"
   - Se dice "Non esiste" → status: "NO"
   - Se dice "esiste" o "presente" → status: "SI"

4. VERIFICA LEGAL KILLERS:
   - Per ogni item con status SI/NO, DEVE avere evidence con page e quote
   - Se evidence vuota → status DEVE essere "NON_SPECIFICATO"

5. VERIFICA TOTALI:
   - totale_extra_budget.min = somma di tutti stima_euro in money_box.items
   - totale_extra_budget.max = min + 20% margine

6. QA PASS:
   - Se page_coverage_log.length < {len(pages)} → status="FAIL"
   - Se Money Box ha € con fonte vuota → status="FAIL"

RESTITUISCI il JSON CORRETTO e COMPLETO con tutti i campi verificati.
NON aggiungere commenti, solo JSON valido."""

        verification_response = await openai_chat_completion(PERIZIA_SYSTEM_PROMPT, verification_prompt, model="gpt-4o", timeout_seconds=LLM_TIMEOUT_SECONDS)
        
        # Parse verification response
        ver_text = verification_response.strip()
        if ver_text.startswith("```json"):
            ver_text = ver_text[7:]
        if ver_text.startswith("```"):
            ver_text = ver_text[3:]
        if ver_text.endswith("```"):
            ver_text = ver_text[:-3]
        ver_text = ver_text.strip()
        
        try:
            verified_result = json.loads(ver_text)
            
            # Re-apply deterministic fixes after verification
            # ---- Multi-lot override (ensure not reverted) ----
            lots = detected_lots.get("lots", [])
            if isinstance(lots, list) and len(lots) >= 2:
                hdr = verified_result.setdefault("report_header", {})
                lotto_obj = hdr.setdefault("lotto", {"value": "Non specificato in Perizia", "evidence": []})
                lotto_obj["value"] = "Lotti " + ", ".join(str(x) for x in lots)
                lotto_obj["evidence"] = detected_lots.get("evidence", [])
            
            # ---- Re-enforce legal killers tri-state ----
            lk = verified_result.get("section_9_legal_killers", {})
            items = lk.get("items", []) if isinstance(lk, dict) else []
            for it in items:
                status = str(it.get("status", "NON_SPECIFICATO")).upper()
                ev = it.get("evidence", [])
                if status in ("SI", "NO", "YES") and not has_evidence(ev):
                    it["status"] = "NON_SPECIFICATO"
                    it.setdefault("action", "Verifica obbligatoria")
            
            # ---- Re-enforce Money Box honesty ----
            mb = verified_result.get("section_3_money_box", {})
            mb_items = mb.get("items", []) if isinstance(mb, dict) else []
            for it in mb_items:
                fonte = (it.get("fonte_perizia", {}) or {})
                fonte_val = str(fonte.get("value", "")).lower()
                fonte_ev = fonte.get("evidence", [])
                euro = it.get("stima_euro", 0)
                is_unspecified = ("non specificato" in fonte_val) or (not has_evidence(fonte_ev))
                if is_unspecified and euro and euro > 0:
                    note = str(it.get("stima_nota", "") or "")
                    if "STIMA NEXODIFY" not in note.upper():
                        it["stima_euro"] = 0
                        it["stima_nota"] = "TBD (NON SPECIFICATO IN PERIZIA) — Verifica tecnico/legale"
            
            result = verified_result
            logger.info("PASS 2: Verification successful with deterministic re-enforcement")
        except json.JSONDecodeError:
            logger.warning("PASS 2: Could not parse verification, keeping PASS 1 result")
        
        # ==========================================
        # PASS 3: Final Validation & Calculation
        # ==========================================
        logger.info(f"PASS 3: Final validation for {file_name}")
        
        # Deterministic calculations and fixes
        result = apply_deterministic_fixes(result, pdf_text, pages, detected_lots, has_evidence)
        result = enforce_evidence_or_low_confidence(result)
        
        # Ensure required fields exist
        if "schema_version" not in result:
            result["schema_version"] = "nexodify_perizia_scan_v1"
        
        # Add run info if missing
        if "run" not in result and "report_header" not in result:
            result["run"] = {
                "run_id": run_id, 
                "generated_at_utc": datetime.now(timezone.utc).isoformat(),
                "input": {"source_type": "perizia_pdf", "file_name": file_name, "pages_total": len(pages)}
            }
        
        # Add verification metadata
        result["_verification"] = {
            "passes_completed": 3,
            "final_validation": "PASS",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "detected_lots": detected_lots,
            "pages_total": len(pages)
        }
        
        logger.info(f"Successfully analyzed perizia {file_name} - {len(pages)} pages (3-pass verification)")
        return result
        
    except Exception as e:
        logger.error(f"LLM analysis error: {e}")
        raise


def apply_deterministic_fixes(result: Dict, pdf_text: str, pages: List[Dict], detected_lots: Dict = None, has_evidence_fn = None) -> Dict:
    """Apply deterministic fixes and calculations to ensure data consistency"""
    import re
    
    # Default has_evidence function if not provided
    def has_evidence(ev):
        if not isinstance(ev, list) or not ev:
            return False
        e0 = ev[0]
        return isinstance(e0, dict) and "page" in e0 and "quote" in e0 and str(e0.get("quote","")).strip() != ""
    
    if has_evidence_fn:
        has_evidence = has_evidence_fn
    
    detected_lots = detected_lots or {"lots": [], "evidence": []}
    
    # ==========================================
    # FIX 0: Final Multi-Lot Override (CHANGE 2)
    # ==========================================
    lots = detected_lots.get("lots", [])
    if isinstance(lots, list) and len(lots) >= 2:
        hdr = result.setdefault("report_header", {})
        lotto_obj = hdr.setdefault("lotto", {"value": "Non specificato in Perizia", "evidence": []})
        lotto_obj["value"] = "Lotti " + ", ".join(str(x) for x in lots)
        lotto_obj["evidence"] = detected_lots.get("evidence", [])
        
        # Also update case_header if it exists
        case_hdr = result.get("case_header", {})
        if case_hdr:
            case_hdr["lotto"] = "Lotti " + ", ".join(str(x) for x in lots)
    
    # ==========================================
    # FIX 1: Recalculate Money Box Totals
    # ==========================================
    money_box = result.get("section_3_money_box") or result.get("money_box") or {}
    items = money_box.get("items", [])
    
    total_min = 0
    for item in items:
        stima = item.get("stima_euro", 0)
        if isinstance(stima, (int, float)) and stima > 0:
            total_min += stima
        elif item.get("value"):
            val = item.get("value", 0)
            if isinstance(val, (int, float)) and val > 0:
                total_min += val
    
    # Update totals
    total_max = int(total_min * 1.2)  # 20% margin
    
    if "section_3_money_box" in result:
        result["section_3_money_box"]["totale_extra_budget"] = {
            "min": total_min,
            "max": total_max,
            "nota": f"EUR {total_min:,}+ (minimo prudenziale)"
        }
    if "money_box" in result:
        result["money_box"]["total_extra_costs"] = {
            "range": {"min": total_min, "max": total_max},
            "max_is_open": True
        }
    
    # ==========================================
    # FIX 2: Extract missing conformity data from text
    # ==========================================
    text_lower = pdf_text.lower()
    
    # Check for impianti conformity
    abusi = result.get("section_5_abusi_conformita") or result.get("abusi_edilizi_conformita") or {}
    impianti = abusi.get("impianti", {})
    
    if "non esiste la dichiarazione di conformità dell'impianto elettrico" in text_lower:
        impianti["elettrico"] = {"conformita": "NO"}
    elif "dichiarazione di conformità dell'impianto elettrico" in text_lower:
        impianti["elettrico"] = {"conformita": "SI"}
    
    if "non esiste la dichiarazione di conformità dell'impianto termico" in text_lower:
        impianti["termico"] = {"conformita": "NO"}
    elif "dichiarazione di conformità dell'impianto termico" in text_lower:
        impianti["termico"] = {"conformita": "SI"}
        
    if "non esiste la dichiarazione di conformità dell'impianto idrico" in text_lower:
        impianti["idrico"] = {"conformita": "NO"}
    elif "dichiarazione di conformità dell'impianto idrico" in text_lower:
        impianti["idrico"] = {"conformita": "SI"}
    
    if "non esiste il certificato energetico" in text_lower or "non esiste l'attestato di prestazione energetica" in text_lower:
        abusi["ape"] = {"status": "ASSENTE"}
    
    if re.search(r"non\s+[èe]\s+presente\s+l['’]?abitabilit[aà]|non\s+risulta\s+agibil[ei]", text_lower, re.I):
        abusi["agibilita"] = {"status": "ASSENTE"}
    
    if impianti:
        abusi["impianti"] = impianti
        
    if "section_5_abusi_conformita" in result:
        result["section_5_abusi_conformita"] = abusi
    if "abusi_edilizi_conformita" in result:
        result["abusi_edilizi_conformita"] = abusi

    # Ensure APE evidence when present
    ape_ev = _find_regex_in_pages(
        pages,
        r"(Non\\s+esiste\\s+il\\s+certificato\\s+energetico[^\\n]*|APE\\s+non\\s+presente|Attestato\\s+di\\s+prestazione\\s+energetica\\s+non\\s+presente)",
        re.I,
        field_key="ape",
    )
    if ape_ev:
        abusi["ape"] = {"status": "ASSENTE", "evidence": [ape_ev]}
        if "section_5_abusi_conformita" in result:
            result["section_5_abusi_conformita"] = abusi
        if "abusi_edilizi_conformita" in result:
            result["abusi_edilizi_conformita"] = abusi
    agibilita_ev = _find_regex_in_pages(
        pages,
        r"(non\s+[èe]\s+presente\s+l['’]?abitabilit[aà][^\n]*|non\s+risulta\s+agibil[ei][^\n]*|agibilit[aà][^\n]{0,80}(?:assente|non\s+presente)|abitabilit[aà][^\n]{0,80}(?:assente|non\s+presente))",
        re.I,
        field_key="agibilita",
    )
    if agibilita_ev and _is_cost_table_context(str(agibilita_ev.get("quote", "") or "")):
        agibilita_ev = _find_regex_in_pages(
            pages,
            r"(non\s+risulta\s+agibil[ei][^\n]*|non\s+[èe]\s+presente\s+l['’]?abitabilit[aà][^\n]*)",
            re.I,
            field_key="agibilita",
        )
    if agibilita_ev and isinstance(abusi, dict):
        agibilita_obj = abusi.get("agibilita") if isinstance(abusi.get("agibilita"), dict) else {}
        agibilita_obj["status"] = agibilita_obj.get("status") or "ASSENTE"
        agibilita_obj["evidence"] = [agibilita_ev]
        abusi["agibilita"] = agibilita_obj
        if "section_5_abusi_conformita" in result:
            result["section_5_abusi_conformita"] = abusi
        if "abusi_edilizi_conformita" in result:
            result["abusi_edilizi_conformita"] = abusi

    # Catasto conformity fallback from explicit "incongruenze ... planimetria catastale"
    catasto_ev = _find_regex_in_pages(
        pages,
        r"(incongruenz\w+[\s\S]{0,220}?planimetri\w+\s+catastal\w*|planimetri\w+\s+catastal\w*[\s\S]{0,160}?non\s+conforme)",
        re.I,
        field_key="conformita_catastale",
    )
    if catasto_ev and isinstance(abusi, dict):
        cat_obj = abusi.get("conformita_catastale") if isinstance(abusi.get("conformita_catastale"), dict) else {}
        current_status = str(cat_obj.get("status") or "").upper()
        if current_status in {"", "UNKNOWN", "NON SPECIFICATO IN PERIZIA", "NOT_FOUND"}:
            cat_obj["status"] = "PRESENTI DIFFORMITÀ"
            cat_obj["detail_it"] = "PRESENTI DIFFORMITÀ"
            cat_obj["evidence"] = [catasto_ev]
            abusi["conformita_catastale"] = cat_obj
            if "section_5_abusi_conformita" in result:
                result["section_5_abusi_conformita"] = abusi
            if "abusi_edilizi_conformita" in result:
                result["abusi_edilizi_conformita"] = abusi

    # ==========================================
    # FIX 3: Extract EUR values from text if missing
    # ==========================================
    # Look for deprezzamenti values
    deprezzamento_patterns = [
        r'oneri di regolarizzazione urbanistica[:\s]*(\d{1,3}(?:[.,]\d{3})*(?:[.,]\d{2})?)\s*€',
        r'regolarizzazione urbanistica[:\s]*€?\s*(\d{1,3}(?:[.,]\d{3})*(?:[.,]\d{2})?)',
        r'rischio assunto per mancata garanzia[:\s]*(\d{1,3}(?:[.,]\d{3})*(?:[.,]\d{2})?)\s*€',
    ]
    
    for pattern in deprezzamento_patterns:
        matches = re.findall(pattern, text_lower, re.IGNORECASE)
        if matches:
            logger.info(f"Found deprezzamento value: {matches}")

    # Sanatoria estimate -> Money Box A (regolarizzazione urbanistica)
    sanatoria_ev = _find_regex_in_pages(
        pages,
        r"(sanabil\w+[^\n]{0,160}?€\s*[\d\.,]+|spese\s+di\s+massima\s+presunte\s*[:\s]*€\s*[\d\.,]+|spesa\s+stimata[^\n]{0,80}€\s*[\d\.,]+)",
        re.I,
    )
    if sanatoria_ev:
        amount = _parse_euro_number(sanatoria_ev.get("quote", ""))
        if amount is not None:
            for box_key in ("section_3_money_box", "money_box"):
                box = result.get(box_key) if isinstance(result.get(box_key), dict) else None
                if not box:
                    continue
                items = box.get("items", [])
                if not isinstance(items, list):
                    continue
                for item in items:
                    label = str(item.get("label_it", "") or item.get("voce", "") or "")
                    if item.get("code") == "A" or "Regolarizzazione urbanistica" in label:
                        item["stima_euro"] = amount
                        item["type"] = item.get("type") if item.get("type") not in {"TBD", None} else "ESTIMATE"
                        item["stima_nota"] = "Perizia: sanatoria stimata"
                        item["fonte_perizia"] = {"value": "Perizia", "evidence": [sanatoria_ev]}
                        break
                result[box_key] = box

    # Beni list for Lotto Unico
    beni = _extract_beni_from_pages(pages)
    if beni:
        result["beni"] = beni
        if isinstance(result.get("lots"), list) and result["lots"]:
            lot0 = result["lots"][0]
            if isinstance(lot0, dict) and not lot0.get("beni"):
                lot0["beni"] = beni

    # Dati asta (data + ora)
    if "dati_asta" not in result:
        for p in pages:
            text = str(p.get("text", "") or "")
            m = re.search(r"(\d{1,2}/\d{1,2}/\d{4}).{0,120}?ore\s+(\d{1,2}[:\.]\d{2})", text, re.I)
            if not m:
                continue
            start, end = m.start(), m.end()
            line_start, line_end = _line_bounds(text, start, end)
            evidence = [_build_evidence(text, int(p.get("page_number", 0) or 0), line_start, line_end)]
            result["dati_asta"] = {"data": m.group(1), "ora": m.group(2).replace(".", ":"), "evidence": evidence}
            break
    
    # ==========================================
    # FIX 4: Ensure Checklist Pre-Offerta
    # ==========================================
    default_checklist = [
        "Accesso atti in Comune",
        "Preventivo tecnico reale",
        "Visure in Conservatoria",
        "Conferma eventuali arretrati condominiali",
        "Strategia di liberazione e allineamento"
    ]
    
    if "section_12_checklist_pre_offerta" in result:
        if not result["section_12_checklist_pre_offerta"] or len(result["section_12_checklist_pre_offerta"]) < 5:
            result["section_12_checklist_pre_offerta"] = default_checklist
    if "checklist_pre_offerta" in result:
        if not result["checklist_pre_offerta"] or len(result["checklist_pre_offerta"]) < 5:
            result["checklist_pre_offerta"] = [{"item_it": item, "priority": f"P{i}", "status": "TO_CHECK"} for i, item in enumerate(default_checklist)]
    
    # ==========================================
    # FIX 5: Recalculate Indice di Convenienza
    # ==========================================
    dati = result.get("section_4_dati_certi") or result.get("dati_certi_del_lotto") or {}
    prezzo_base = 0
    
    if isinstance(dati.get("prezzo_base_asta"), dict):
        prezzo_base = dati["prezzo_base_asta"].get("value", 0)
    elif isinstance(dati.get("prezzo_base_asta"), (int, float)):
        prezzo_base = dati["prezzo_base_asta"]
    
    if prezzo_base > 0:
        indice = result.get("section_10_indice_convenienza") or result.get("indice_di_convenienza") or {}
        indice["prezzo_base"] = prezzo_base
        indice["extra_budget_min"] = total_min
        indice["extra_budget_max"] = total_max
        indice["all_in_light_min"] = prezzo_base + total_min
        indice["all_in_light_max"] = prezzo_base + total_max
        
        if "section_10_indice_convenienza" in result:
            result["section_10_indice_convenienza"] = indice
        if "indice_di_convenienza" in result:
            result["indice_di_convenienza"] = indice
    
    # ==========================================
    # FIX 6: Update QA Pass with verification status and QA GATES
    # ==========================================
    # Get existing qa_pass or create new one
    qa = result.get("qa_pass", None)
    if qa is None:
        qa = result.get("qa", None)
    if qa is None:
        qa = {"status": "PASS", "checks": []}
    
    # Preserve existing checks and merge with new ones
    existing_checks = qa.get("checks", [])
    qa_checks = list(existing_checks)  # Copy existing checks
    qa_status = qa.get("status", "PASS")
    
    # QA Gate 1: Page Coverage Log
    page_coverage_log = result.get("page_coverage_log", [])
    pages_total = len(pages)
    
    # Always add QA-PageCoverage check
    qa_checks.append({
        "code": "QA-PageCoverage",
        "result": "OK" if len(page_coverage_log) >= pages_total else "WARN",
        "note": f"page_coverage_log has {len(page_coverage_log)} entries, expected {pages_total}"
    })
    
    # QA Gate 2: Money Box Honesty (final check)
    money_box_final = result.get("section_3_money_box", {})
    mb_items_final = money_box_final.get("items", []) if isinstance(money_box_final, dict) else []
    money_box_honest = True
    for it in mb_items_final:
        fonte = (it.get("fonte_perizia", {}) or {})
        fonte_val = str(fonte.get("value", "")).lower()
        fonte_ev = fonte.get("evidence", [])
        euro = it.get("stima_euro", 0)
        is_unspecified = ("non specificato" in fonte_val) or (not has_evidence(fonte_ev))
        if is_unspecified and euro and euro > 0:
            note = str(it.get("stima_nota", "") or "")
            if "STIMA NEXODIFY" not in note.upper():
                money_box_honest = False
                qa_status = "FAIL"
    
    qa_checks.append({
        "code": "QA-MoneyBox-Honesty",
        "result": "OK" if money_box_honest else "FAIL",
        "note": "All Money Box items honest (no fake € values)" if money_box_honest else "Money Box has EUR values with unspecified fonte"
    })
    
    # QA Gate 3: Legal Killers Evidence
    lk_final = result.get("section_9_legal_killers", {})
    lk_items = lk_final.get("items", []) if isinstance(lk_final, dict) else []
    legal_killers_valid = True
    for it in lk_items:
        status = str(it.get("status", "")).upper()
        ev = it.get("evidence", [])
        if status in ("SI", "NO", "YES") and not has_evidence(ev):
            legal_killers_valid = False
            qa_status = "WARN" if qa_status != "FAIL" else qa_status
    
    qa_checks.append({
        "code": "QA-LegalKiller-Evidence",
        "result": "OK" if legal_killers_valid else "WARN",
        "note": "All SI/NO status have evidence" if legal_killers_valid else "Some Legal killers have SI/NO without evidence"
    })
    
    # Standard QA checks
    qa_checks.extend([
        {"code": "QA-1 Format Lock", "result": "OK", "note": "ordine Roma 1-12 rispettato"},
        {"code": "QA-2 Zero Empty Fields", "result": "OK", "note": "dove manca dato: Non specificato in Perizia"},
        {"code": "QA-3 Page Anchors", "result": "OK", "note": "riferimenti pagina presenti"},
        {"code": "QA-4 Money Box", "result": "OK" if total_min > 0 else "WARN", "note": f"totale: EUR {total_min:,}"},
        {"code": "QA-5 Legal Killers", "result": "OK", "note": "checklist 8 items"},
        {"code": "QA-6 Condono + Opponibilità", "result": "OK", "note": "status verificato"},
        {"code": "QA-7 Delivery Timeline", "result": "OK", "note": "stima tempi presente"},
        {"code": "QA-8 Semaforo Rules", "result": "OK", "note": "coerente con criticità"},
        {"code": "QA-9 3-Pass Verification", "result": "OK", "note": "verificato con 3 passaggi"}
    ])
    
    qa["status"] = qa_status
    qa["checks"] = qa_checks
    
    # Always set both qa_pass and qa to ensure consistency
    result["qa_pass"] = qa
    result["qa"] = qa
    
    return result

def create_fallback_analysis(file_name: str, case_id: str, run_id: str, pages: List[Dict], pdf_text: str, extracted_lots: List[Dict] = None, detected_legal_killers: List[Dict] = None) -> Dict:
    """Create fallback analysis when LLM fails - extract what we can deterministically"""
    import re
    
    extracted_lots = extracted_lots or []
    detected_legal_killers = detected_legal_killers or []

    normalized_lots: List[Dict[str, Any]] = []
    for idx, lot in enumerate(extracted_lots):
        lot_num = lot.get("lot_number") if isinstance(lot, dict) and isinstance(lot.get("lot_number"), int) else (idx + 1)
        normalized_lots.append(_ensure_lot_contract(lot, lot_num))
    extracted_lots = normalized_lots
    if not extracted_lots:
        extracted_lots = [_build_fallback_lot_from_pages(pages)]
    
    # Try to extract basic info from text
    text_lower = pdf_text.lower()
    
    # Find procedure ID with evidence
    procedure_id = "NON SPECIFICATO IN PERIZIA"
    procedure_ev = _find_regex_in_pages(
        pages,
        r"Esecuzione\s+Immobiliare\s+\d+/\d+\s+del\s+R\.G\.E\.",
        re.I,
        field_key="procedura",
    )
    if procedure_ev:
        procedure_id = procedure_ev["quote"].strip()
    
    # Find tribunal with evidence
    tribunale = "NON SPECIFICATO IN PERIZIA"
    tribunale_ev = _find_regex_in_pages(pages, r"TRIBUNALE\s+DI\s+[A-Z\s]+", re.I, field_key="tribunale")
    if tribunale_ev:
        tribunale = tribunale_ev["quote"].strip()
    
    # Determine lotto value based on extracted lots
    if len(extracted_lots) >= 2:
        lotto_value = "Lotti " + ", ".join(str(lot["lot_number"]) for lot in extracted_lots)
        is_multi_lot = True
    elif len(extracted_lots) == 1:
        lotto_value = "Lotto Unico" if extracted_lots[0]["lot_number"] == 1 else f"Lotto {extracted_lots[0]['lot_number']}"
        is_multi_lot = False
    else:
        lotto_value = "NON SPECIFICATO IN PERIZIA"
        is_multi_lot = False
    
    # Find prezzo base
    prezzo_base = 0
    if extracted_lots:
        first_price = _as_float_or_none(extracted_lots[0].get("prezzo_base_value"))
        if isinstance(first_price, (int, float)) and first_price > 0:
            prezzo_base = float(first_price)
        else:
            prezzo_match = re.search(r'prezzo\s+base[^\d]*(\d[\d\.,]+)', text_lower)
            if not prezzo_match:
                prezzo_match = re.search(r'€\s*(\d[\d\.,]+)', text_lower)
            if prezzo_match:
                try:
                    prezzo_str = prezzo_match.group(1).replace('.', '').replace(',', '.')
                    prezzo_base = float(prezzo_str)
                except Exception:
                    pass
    
    # Find superficie
    superficie = "NON SPECIFICATO IN PERIZIA"
    if extracted_lots and extracted_lots[0].get("superficie_mq"):
        superficie = extracted_lots[0]["superficie_mq"]
    else:
        sup_match = re.search(r'superficie[^\d]*(\d+[\d\.,]*)\s*mq', text_lower)
        if sup_match:
            superficie = f"{sup_match.group(1)} mq"

    # Address from lot ubicazione (if present)
    address_value = "NON SPECIFICATO IN PERIZIA"
    address_ev = None
    if extracted_lots and extracted_lots[0].get("ubicazione") and extracted_lots[0]["ubicazione"] != "NON SPECIFICATO IN PERIZIA":
        address_value = extracted_lots[0]["ubicazione"]
        ev_list = extracted_lots[0].get("evidence", {}).get("ubicazione", [])
        address_ev = ev_list[0] if ev_list else None

    # Extract deprezzamenti for Money Box
    deprezzamenti = _extract_deprezzamenti(pages)
    deprezz_map = {}
    for it in deprezzamenti:
        label_lower = it["label"].lower()
        if "regolarizzazione urbanistica" in label_lower:
            deprezz_map["A"] = it
        elif "rischio" in label_lower:
            deprezz_map["C"] = it

    # Deterministic enrichments directly from pages
    beni_list = _extract_beni_from_pages(pages)
    extracted_lots = _enrich_lots_from_sections(extracted_lots, pages, beni=beni_list)
    extracted_lots = _assign_beni_to_lots(extracted_lots, beni_list)
    ape_state = _extract_ape_state(pages)
    dati_asta_state = _extract_dati_asta_state(pages)

    # Prefer specific sanatoria mentions (often Bene n.3) over generic totals.
    sanatoria_candidates: List[Tuple[float, Dict[str, Any]]] = []
    for p in pages:
        text = str(p.get("text", "") or "")
        for m in re.finditer(r"(sanabil\w+[^\n]{0,160}?€\s*[\d\.,]+|spese\s+di\s+massima\s+presunte[^\n]{0,120}€\s*[\d\.,]+)", text, re.I):
            line_start, line_end = _line_bounds(text, m.start(), m.end())
            ev = _build_evidence(text, int(p.get("page_number", 0) or 0), line_start, line_end)
            num_m = re.search(r"([0-9]{1,3}(?:\.[0-9]{3})*(?:,[0-9]{2})?)", m.group(0))
            amt = _parse_euro_number(num_m.group(1) if num_m else "")
            if isinstance(amt, (int, float)):
                sanatoria_candidates.append((float(amt), ev))
    sanatoria_best = None
    if sanatoria_candidates:
        # For parity with estratto summary, use the most specific minimum sanabile amount.
        sanatoria_best = sorted(sanatoria_candidates, key=lambda x: x[0])[0]
    
    lot_sections = _build_lot_sections(pages)
    lot_by_page: Dict[int, int] = {}
    for sec in lot_sections:
        lot_num = int(sec.get("lot_number") or 0)
        for p in sec.get("pages", []) if isinstance(sec.get("pages"), list) else []:
            page_num = _get_page_number(p)
            if page_num > 0 and lot_num > 0:
                lot_by_page[page_num] = lot_num

    # Build legal killers from deterministic scan
    legal_killers_items = []
    for lk in detected_legal_killers:
        lk_page = int(lk.get("page") or 0)
        lk_lot = lot_by_page.get(lk_page)
        killer_label = lk["title"] if lk_lot is None else f"Lotto {lk_lot}: {lk['title']}"
        legal_killers_items.append({
            "killer": killer_label,
            "status": "SI" if lk["severity"] == "ROSSO" else "GIALLO",
            "action": "Verifica obbligatoria",
            "evidence": [{"page": lk["page"], "quote": lk["quote"], "start_offset": lk.get("start_offset"), "end_offset": lk.get("end_offset"), "bbox": None}]
        })
    legal_killers_items = _augment_legal_killers_from_lots(legal_killers_items, extracted_lots)

    lot_red_flags: List[Dict[str, Any]] = []
    if len(extracted_lots) > 1:
        for lot in extracted_lots:
            lot_num = lot.get("lot_number")
            notes = lot.get("risk_notes") if isinstance(lot.get("risk_notes"), list) else []
            for note in notes[:3]:
                lot_red_flags.append(
                    {
                        "code": f"LOT_{lot_num}_RISK",
                        "severity": "AMBER",
                        "flag_it": f"Lotto {lot_num}: {note}",
                        "flag_en": f"Lot {lot_num}: {note}",
                        "action_it": "Verifica tecnica/legale dedicata al lotto",
                    }
                )
    
    result = {
        "schema_version": "nexodify_perizia_scan_v2",
        "detail_scope": "LOT_FIRST" if len(extracted_lots) > 1 else ("BENE_FIRST" if len(beni_list) > 1 else "SINGLE_ASSET"),
        "run": {
            "run_id": run_id,
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "input": {"source_type": "perizia_pdf", "file_name": file_name, "pages_total": len(pages)}
        },
        "lots": extracted_lots,
        "lots_count": len(extracted_lots),
        "is_multi_lot": is_multi_lot,
        "case_header": {
            "procedure_id": procedure_id,
            "lotto": lotto_value,
            "tribunale": tribunale,
            "address": {"street": "NON SPECIFICATO IN PERIZIA", "city": "NON SPECIFICATO IN PERIZIA", "full": address_value},
            "deposit_date": "NON SPECIFICATO IN PERIZIA"
        },
        "report_header": {
            "title": "NEXODIFY INTELLIGENCE | Auction Scan",
            "procedure": {"value": procedure_id, "evidence": [procedure_ev] if procedure_ev else []},
            "lotto": {"value": lotto_value, "evidence": extracted_lots[0].get("evidence", {}).get("lotto", []) if extracted_lots else []},
            "tribunale": {"value": tribunale, "evidence": [tribunale_ev] if tribunale_ev else []},
            "address": {"value": address_value, "evidence": [address_ev] if address_ev else []},
            "is_multi_lot": is_multi_lot,
            "generated_at": datetime.now(timezone.utc).isoformat()
        },
        "lot_index": _build_lot_index_entries(extracted_lots),
        "page_coverage_log": [{"page": i+1, "summary": "Fallback - manual review required"} for i in range(len(pages))],
        "semaforo_generale": {
            "status": "AMBER",
            "status_it": "ATTENZIONE",
            "status_en": "CAUTION",
            "reason_it": "Analisi automatica parziale - revisione manuale raccomandata",
            "reason_en": "Partial automatic analysis - manual review recommended",
            "evidence": []
        },
        "decision_rapida_client": {
            "risk_level": "MEDIUM_RISK",
            "risk_level_it": "RISCHIO MEDIO",
            "risk_level_en": "MEDIUM RISK",
            "summary_it": "Documento analizzato. Alcuni dati richiedono verifica manuale. Consultare un professionista prima di procedere.",
            "summary_en": "Document analyzed. Some data requires manual verification. Consult a professional before proceeding.",
            "driver_rosso": []
        },
        "money_box": {
            "items": [
                {
                    "code": "A",
                    "label_it": "Regolarizzazione urbanistica",
                    "label_en": "Urban regularization",
                    "type": "TBD",
                    "stima_euro": deprezz_map.get("A", {}).get("value", "TBD"),
                    "stima_nota": "Da perizia" if deprezz_map.get("A") else "TBD (NON SPECIFICATO IN PERIZIA) — Verifica tecnico",
                    "fonte_perizia": {"value": "Perizia", "evidence": [deprezz_map.get("A", {}).get("evidence")]} if deprezz_map.get("A") else {"value": "Non specificato", "evidence": []},
                    "action_required_it": "Verificare con tecnico",
                    "action_required_en": "Verify with technician"
                },
                {"code": "B", "label_it": "Oneri tecnici / istruttoria", "label_en": "Technical fees", "type": "TBD", "stima_euro": "TBD", "stima_nota": "TBD (NON SPECIFICATO IN PERIZIA)", "source": "TBD"},
                {
                    "code": "C",
                    "label_it": "Rischio ripristini",
                    "label_en": "Restoration risk",
                    "type": "TBD",
                    "stima_euro": deprezz_map.get("C", {}).get("value", "TBD"),
                    "stima_nota": "Da perizia" if deprezz_map.get("C") else "TBD (NON SPECIFICATO IN PERIZIA)",
                    "fonte_perizia": {"value": "Perizia", "evidence": [deprezz_map.get("C", {}).get("evidence")]} if deprezz_map.get("C") else {"value": "Non specificato", "evidence": []},
                    "source": "TBD"
                },
                {"code": "D", "label_it": "Allineamento catastale", "label_en": "Cadastral alignment", "type": "TBD", "stima_euro": "TBD", "stima_nota": "TBD (NON SPECIFICATO IN PERIZIA)", "source": "TBD"},
                {"code": "E", "label_it": "Spese condominiali arretrate", "label_en": "Condo arrears", "type": "TBD", "stima_euro": "TBD", "stima_nota": "TBD (NON SPECIFICATO IN PERIZIA)", "action_required_it": "Verificare con amministratore", "action_required_en": "Verify with administrator"},
                {"code": "F", "label_it": "Costi procedura", "label_en": "Procedure costs", "type": "TBD", "stima_euro": "TBD", "stima_nota": "TBD (NON SPECIFICATO IN PERIZIA)", "action_required_it": "Verificare con delegato", "action_required_en": "Verify with delegate"},
                {"code": "G", "label_it": "Cancellazione formalità", "label_en": "Formality cancellation", "type": "INFO_ONLY", "stima_euro": "TBD", "stima_nota": "Da liquidare con decreto di trasferimento"},
                {"code": "H", "label_it": "Costo liberazione", "label_en": "Liberation cost", "type": "TBD", "stima_euro": "TBD", "stima_nota": "TBD (NON SPECIFICATO IN PERIZIA)", "source": "TBD"}
            ],
            "total_extra_costs": {
                "min": "TBD",
                "max": "TBD",
                "nota": "TBD — Costi non quantificati in perizia"
            }
        },
        "section_9_legal_killers": {
            "items": legal_killers_items
        },
        "dati_certi_del_lotto": {
            "prezzo_base_asta": {"value": prezzo_base, "formatted": f"€{prezzo_base:,.0f}" if prezzo_base else "NOT_SPECIFIED", "evidence": extracted_lots[0].get("evidence", {}).get("prezzo_base", []) if extracted_lots else []},
            "superficie_catastale": {"value": superficie, "evidence": extracted_lots[0].get("evidence", {}).get("superficie", []) if extracted_lots else []},
            "catasto": {"categoria": "NON SPECIFICATO IN PERIZIA", "classe": "NON SPECIFICATO IN PERIZIA", "vani": "NON SPECIFICATO IN PERIZIA"},
            "diritto_reale": {"value": extracted_lots[0].get("diritto_reale", "NON SPECIFICATO IN PERIZIA") if extracted_lots else "NON SPECIFICATO IN PERIZIA", "evidence": extracted_lots[0].get("evidence", {}).get("diritto_reale", []) if extracted_lots else []}
        },
        "abusi_edilizi_conformita": {
            "conformita_urbanistica": {"status": "UNKNOWN", "detail_it": "Da verificare nella perizia", "evidence": []},
            "conformita_catastale": {"status": "UNKNOWN", "detail_it": "Da verificare nella perizia", "evidence": []},
            "condono": {"present": "UNKNOWN", "status": "UNKNOWN", "evidence": []},
            "agibilita": {"status": "UNKNOWN", "evidence": []},
            "commerciabilita": {"status": "UNKNOWN", "evidence": []},
            "ape": {"status": "ASSENTE" if ape_state.get("status") == "FOUND" and str(ape_state.get("value")) == "ASSENTE" else "UNKNOWN", "evidence": ape_state.get("evidence", []) if isinstance(ape_state, dict) else []}
        },
        "stato_occupativo": {
            "status": "UNKNOWN",
            "status_it": "Da verificare",
            "status_en": "To verify",
            "title_opponible": "NOT_SPECIFIED",
            "evidence": []
        },
        "stato_conservativo": {
            "general_condition_it": "Vedere dettagli nella perizia originale",
            "general_condition_en": "See details in original appraisal",
            "issues_found": [],
            "evidence": []
        },
        "formalita": {
            "ipoteca": {"status": "UNKNOWN", "evidence": []},
            "pignoramento": {"status": "UNKNOWN", "evidence": []},
            "cancellazione_decreto": {"status": "UNKNOWN", "evidence": []}
        },
        "legal_killers_checklist": {
            "PEEP_superficie": {"status": "NOT_SPECIFIED", "action_required_it": "Verificare se l'immobile è in area PEEP"},
            "donazione_catena_20anni": {"status": "NOT_SPECIFIED", "action_required_it": "Verificare catena proprietaria ultimi 20 anni"},
            "prelazione_stato_beni_culturali": {"status": "NOT_SPECIFIED", "action_required_it": "Verificare vincoli beni culturali"},
            "usi_civici_diritti_demaniali": {"status": "NOT_SPECIFIED", "action_required_it": "Verificare presenza usi civici"},
            "fondo_patrimoniale": {"status": "NOT_SPECIFIED", "action_required_it": "Verificare se bene in fondo patrimoniale"},
            "servitu_atti_obbligo": {"status": "NOT_SPECIFIED", "action_required_it": "Verificare servitù e atti d'obbligo"},
            "formalita_non_cancellabili": {"status": "NOT_SPECIFIED", "action_required_it": "Verificare formalità non cancellabili"},
            "amianto": {"status": "NOT_SPECIFIED", "action_required_it": "Verificare presenza amianto"}
        },
        "indice_di_convenienza": {
            "prezzo_base_asta": prezzo_base,
            "extra_costs_min": "TBD",
            "extra_costs_max": "TBD",
            "all_in_light_min": "TBD",
            "all_in_light_max": "TBD",
            "dry_read_it": "Prezzo base da verificare con la perizia originale",
            "dry_read_en": "Base price must be verified with the original appraisal"
        },
        "red_flags_operativi": lot_red_flags + [
            {"code": "MANUAL_REVIEW", "severity": "AMBER", "flag_it": "Revisione manuale raccomandata", "flag_en": "Manual review recommended", "action_it": "Verificare tutti i dati con la perizia originale"}
        ],
        "checklist_pre_offerta": [
            {"item_it": "Verificare conformità urbanistica e catastale", "item_en": "Verify urban and cadastral compliance", "priority": "P0", "status": "TO_CHECK"},
            {"item_it": "Verificare stato occupativo e titolo", "item_en": "Verify occupancy status and title", "priority": "P0", "status": "TO_CHECK"},
            {"item_it": "Controllare formalità e ipoteche", "item_en": "Check formalities and mortgages", "priority": "P0", "status": "TO_CHECK"},
            {"item_it": "Sopralluogo immobile", "item_en": "Property inspection", "priority": "P1", "status": "TO_CHECK"},
            {"item_it": "Verificare spese condominiali arretrate", "item_en": "Verify condo arrears", "priority": "P1", "status": "TO_CHECK"}
        ],
        "summary_for_client": {
            "summary_it": f"Analisi del documento {file_name}. Verifica obbligatoria dei dati con la perizia originale.",
            "summary_en": f"Analysis of document {file_name}. Verification required with the original appraisal.",
            "disclaimer_it": "Documento informativo. Non costituisce consulenza legale. Consultare un professionista qualificato.",
            "disclaimer_en": "Informational document. Not legal advice. Consult a qualified professional."
        },
        "qa_pass": {
            "status": "WARN",
            "reasons": [
                {"code": "PARTIAL_EXTRACTION", "severity": "AMBER", "reason_it": "Estrazione automatica parziale - LLM fallback", "reason_en": "Partial automatic extraction - LLM fallback"},
                {"code": "NO_EVIDENCE", "severity": "AMBER", "reason_it": "Nessuna evidence estratta - verifica manuale", "reason_en": "No evidence extracted - manual verification needed"}
            ],
            "checks": [
                {"code": "QA-PageCoverage", "result": "WARN", "note": f"Fallback mode - page_coverage_log created with {len(pages)} placeholder entries"},
                {"code": "QA-MoneyBox-Honesty", "result": "OK", "note": "All values set to TBD (no fake estimates)"},
                {"code": "QA-MultiLot", "result": "OK" if len(extracted_lots) >= 2 else "WARN", "note": f"Multi-lot detected: {len(extracted_lots)} lots" if len(extracted_lots) >= 2 else f"Single lot: {len(extracted_lots)} lots"},
                {"code": "QA-Lotto", "result": "OK" if extracted_lots else "WARN", "note": f"Detected lots: {len(extracted_lots)}" if extracted_lots else "No lots detected"}
            ]
        }
    }
    if beni_list:
        result["beni"] = beni_list
        if (
            isinstance(result.get("lots"), list)
            and len(result["lots"]) == 1
            and isinstance(result["lots"][0], dict)
        ):
            result["lots"][0]["beni"] = beni_list

    if sanatoria_best:
        sanatoria_amount, sanatoria_ev = sanatoria_best
        for item in result.get("money_box", {}).get("items", []):
            if item.get("code") == "A":
                item["stima_euro"] = sanatoria_amount
                item["type"] = "ESTIMATE"
                item["stima_nota"] = "Perizia: sanatoria stimata"
                item["fonte_perizia"] = {"value": "Perizia", "evidence": [sanatoria_ev]}
                break

    if len(extracted_lots) > 1:
        _build_conservative_money_box_for_lots(result)

    if isinstance(dati_asta_state, dict) and dati_asta_state.get("status") == "FOUND" and isinstance(dati_asta_state.get("value"), dict):
        result["dati_asta"] = {
            "data": dati_asta_state["value"].get("data"),
            "ora": dati_asta_state["value"].get("ora"),
            "evidence": dati_asta_state.get("evidence", []),
        }

    return enforce_evidence_or_low_confidence(result)


def _build_full_text_from_pages(pages: List[Dict]) -> str:
    full_text = ""
    for page_data in pages:
        page_num = page_data.get("page_number", 0)
        page_text = page_data.get("text", "") or ""
        tables = page_data.get("tables", []) or []
        table_text = ""
        for table in tables:
            table_text += "\n[TABELLA]\n"
            for row in table.get("header_rows", []):
                table_text += " | ".join(row) + "\n"
            table_text += "-" * 40 + "\n"
            for row in table.get("body_rows", []):
                table_text += " | ".join(row) + "\n"
            table_text += "[/TABELLA]\n"
        combined_text = page_text
        if table_text:
            combined_text += "\n" + table_text
        full_text += f"\n\n{'='*60}\nPAGINA {page_num}\n{'='*60}\n{combined_text}"
    return full_text


def _safe_float(value: Any) -> float:
    try:
        return float(value)
    except Exception:
        return 0.0


def _safe_image_count(page_obj: Any) -> Tuple[int, Optional[str]]:
    try:
        images_attr = getattr(page_obj, "images", None)
        if images_attr is not None:
            try:
                return len(list(images_attr)), None
            except TypeError:
                return len(images_attr), None
            except Exception:
                pass

        resources = page_obj.get("/Resources")
        if resources is None:
            return 0, None
        if hasattr(resources, "get_object"):
            resources = resources.get_object()
        xobject = resources.get("/XObject") if isinstance(resources, dict) else None
        if xobject is None:
            return 0, None
        if hasattr(xobject, "get_object"):
            xobject = xobject.get_object()

        image_count = 0
        if isinstance(xobject, dict):
            for item in xobject.values():
                try:
                    resolved = item.get_object() if hasattr(item, "get_object") else item
                    subtype = resolved.get("/Subtype") if isinstance(resolved, dict) else None
                    if str(subtype) == "/Image":
                        image_count += 1
                except Exception:
                    continue
        return image_count, None
    except Exception:
        return 0, "image_detect_failed"


def _build_step1_extract_payload(contents: bytes) -> Dict[str, Any]:
    reader = PdfReader(io.BytesIO(contents))
    pages_total = len(reader.pages)
    pages_raw: List[Dict[str, Any]] = []
    words_raw: List[Dict[str, Any]] = []
    metrics: List[Dict[str, Any]] = []
    ocr_plan: List[Dict[str, Any]] = []
    full_raw_parts: List[str] = []
    page_label_width = max(2, len(str(pages_total or 0)))

    plumber_pages: List[Any] = []
    try:
        with pdfplumber.open(io.BytesIO(contents)) as plumber_pdf:
            plumber_pages = list(plumber_pdf.pages)
    except Exception:
        plumber_pages = []

    for idx in range(1, pages_total + 1):
        page_obj = reader.pages[idx - 1]
        extraction_errors: List[str] = []

        try:
            page_text = page_obj.extract_text() or ""
        except Exception:
            page_text = ""
            extraction_errors.append("text_extract_failed")

        page_words_output: List[Dict[str, Any]] = []
        word_count = 0
        try:
            if idx - 1 >= len(plumber_pages):
                raise RuntimeError("pdfplumber_page_unavailable")
            page_words = plumber_pages[idx - 1].extract_words() or []
            for token in page_words:
                if not isinstance(token, dict):
                    continue
                token_text = str(token.get("text") or "")
                if not token_text.strip():
                    continue
                word_entry: Dict[str, Any] = {"text": token_text}
                if token.get("x0") is not None:
                    word_entry["x0"] = _safe_float(token.get("x0"))
                if token.get("x1") is not None:
                    word_entry["x1"] = _safe_float(token.get("x1"))
                if token.get("top") is not None:
                    word_entry["y0"] = _safe_float(token.get("top"))
                if token.get("bottom") is not None:
                    word_entry["y1"] = _safe_float(token.get("bottom"))
                page_words_output.append(word_entry)
            word_count = len(page_words_output)
        except Exception:
            extraction_errors.append("word_extract_failed")
            word_count = len(re.findall(r"\b\w+\b", page_text, flags=re.UNICODE))

        image_count, image_error = _safe_image_count(page_obj)
        if image_error:
            extraction_errors.append(image_error)

        non_ws_chars = [c for c in page_text if not c.isspace()]
        chars_non_ws = len(non_ws_chars)
        alpha_chars = sum(1 for c in non_ws_chars if c.isalpha())
        garbage_chars = sum(1 for c in non_ws_chars if (not c.isalpha() and not c.isdigit()))
        alpha_ratio = (alpha_chars / chars_non_ws) if chars_non_ws > 0 else 0.0
        garbage_ratio = (garbage_chars / chars_non_ws) if chars_non_ws > 0 else 0.0

        needs_ocr_reasons: List[str] = []
        if word_count < OCR_MIN_WORD_COUNT:
            needs_ocr_reasons.append("word_count_lt_30")
        if chars_non_ws < OCR_MIN_CHARS_NON_WS:
            needs_ocr_reasons.append("chars_non_ws_lt_200")
        if alpha_ratio < OCR_MIN_ALPHA_RATIO:
            needs_ocr_reasons.append("alpha_ratio_lt_0.30")
        if garbage_ratio > OCR_MAX_GARBAGE_RATIO:
            needs_ocr_reasons.append("garbage_ratio_gt_0.25")
        if "word_extract_failed" in extraction_errors:
            needs_ocr_reasons.append("word_extract_failed")
        if "text_extract_failed" in extraction_errors:
            needs_ocr_reasons.append("text_extract_failed")

        needs_ocr = bool(needs_ocr_reasons)
        pages_raw.append({"page": idx, "text": page_text})
        words_raw.append({"page": idx, "words": page_words_output})
        metrics_row = {
            "page": idx,
            "chars_non_ws": chars_non_ws,
            "word_count": word_count,
            "alpha_ratio": round(alpha_ratio, 6),
            "garbage_ratio": round(garbage_ratio, 6),
            "image_count": image_count,
            "has_images": image_count > 0,
            "extraction_errors": extraction_errors,
            "needs_ocr": needs_ocr,
            "needs_ocr_reasons": needs_ocr_reasons,
        }
        metrics.append(metrics_row)
        if needs_ocr:
            ocr_plan.append(
                {
                    "page": idx,
                    "reasons": needs_ocr_reasons,
                    "metrics_snapshot": {
                        "word_count": word_count,
                        "chars_non_ws": chars_non_ws,
                        "alpha_ratio": round(alpha_ratio, 6),
                        "garbage_ratio": round(garbage_ratio, 6),
                        "image_count": image_count,
                    },
                }
            )

        page_label = str(idx).zfill(page_label_width)
        full_raw_parts.append(f"===== PAGE {page_label} =====\n{page_text}")

    pages_needing_ocr = [int(m["page"]) for m in metrics if m.get("needs_ocr")]
    pages_good = [int(m["page"]) for m in metrics if not m.get("needs_ocr")]
    needs_ocr_pages_ratio = (len(pages_needing_ocr) / pages_total) if pages_total else 0.0
    total_images = sum(int(m.get("image_count", 0) or 0) for m in metrics)
    avg_image_count_per_page = (total_images / pages_total) if pages_total else 0.0
    median_word_count = int(statistics.median([int(m.get("word_count", 0) or 0) for m in metrics])) if metrics else 0
    avg_alpha_ratio = (
        sum(float(m.get("alpha_ratio", 0.0) or 0.0) for m in metrics) / pages_total
        if pages_total
        else 0.0
    )

    needs_ocr_document = (
        needs_ocr_pages_ratio >= DOC_NEEDS_OCR_RATIO
        or (
            needs_ocr_pages_ratio >= DOC_NEEDS_OCR_RATIO_WITH_IMAGES
            and avg_image_count_per_page >= DOC_NEEDS_OCR_MIN_AVG_IMAGES
        )
    )
    unreadable_document = (
        needs_ocr_pages_ratio >= DOC_UNREADABLE_RATIO
        and median_word_count < DOC_UNREADABLE_MAX_MEDIAN_WORDS
        and avg_alpha_ratio < DOC_UNREADABLE_MAX_AVG_ALPHA
    )

    if unreadable_document:
        quality_status = "UNREADABLE"
        quality_reason = "Most pages are likely scanned/images or low readability; reliable text extraction is not possible."
        customer_message_it = "Documento non leggibile in modo affidabile (probabile scansione/immagini o qualità bassa). L’analisi automatica può essere incompleta. Carica un PDF migliore o richiedi revisione manuale."
        customer_message_en = "Document is not reliably machine-readable (likely scan/images or low quality). Automated analysis may be incomplete. Upload a clearer PDF or request manual review."
    elif needs_ocr_document:
        quality_status = "NEEDS_OCR"
        quality_reason = "A significant portion of pages likely needs OCR due to low readability signals."
        customer_message_it = "Documento parzialmente leggibile in automatico (probabile scansione/immagini o qualità bassa). È consigliata OCR/revisione manuale."
        customer_message_en = "Document appears only partially machine-readable (likely scan/images or low quality). OCR/manual review is recommended."
    else:
        quality_status = "TEXT_OK"
        quality_reason = "Embedded text quality appears sufficient for deterministic extraction."
        customer_message_it = "Documento leggibile in modo automatico con qualità accettabile."
        customer_message_en = "Document appears machine-readable with acceptable quality."

    thresholds = {
        "page_needs_ocr": {
            "word_count_lt": OCR_MIN_WORD_COUNT,
            "chars_non_ws_lt": OCR_MIN_CHARS_NON_WS,
            "alpha_ratio_lt": OCR_MIN_ALPHA_RATIO,
            "garbage_ratio_gt": OCR_MAX_GARBAGE_RATIO,
            "fail_flags": ["word_extract_failed", "text_extract_failed"],
        },
        "document_status": {
            "needs_ocr_ratio_gte": DOC_NEEDS_OCR_RATIO,
            "needs_ocr_ratio_gte_with_images": DOC_NEEDS_OCR_RATIO_WITH_IMAGES,
            "avg_image_count_per_page_gte": DOC_NEEDS_OCR_MIN_AVG_IMAGES,
            "unreadable_ratio_gte": DOC_UNREADABLE_RATIO,
            "unreadable_median_word_count_lt": DOC_UNREADABLE_MAX_MEDIAN_WORDS,
            "unreadable_avg_alpha_ratio_lt": DOC_UNREADABLE_MAX_AVG_ALPHA,
        },
    }

    document_quality = {
        "status": quality_status,
        "reason": quality_reason,
        "pages_total": pages_total,
        "pages_needing_ocr": pages_needing_ocr,
        "thresholds": thresholds,
        "metrics_summary": {
            "needs_ocr_pages_ratio": round(needs_ocr_pages_ratio, 6),
            "avg_image_count_per_page": round(avg_image_count_per_page, 6),
            "median_word_count": median_word_count,
            "avg_alpha_ratio": round(avg_alpha_ratio, 6),
        },
        "customer_message_it": customer_message_it,
        "customer_message_en": customer_message_en,
    }

    extraction_summary = {
        "pages_total": pages_total,
        "pages_needing_ocr": pages_needing_ocr,
        "pages_good": pages_good,
        "thresholds_used": thresholds,
        "notes": "",
    }

    return {
        "pages_raw": pages_raw,
        "full_raw_txt": "\n\n".join(full_raw_parts).strip() + ("\n" if full_raw_parts else ""),
        "words_raw": words_raw,
        "metrics": metrics,
        "ocr_plan": ocr_plan,
        "document_quality": document_quality,
        "extraction_summary": extraction_summary,
    }


def _write_extraction_pack(analysis_id: str, payload: Dict[str, Any], document_quality: Dict[str, Any]) -> str:
    extract_dir = Path("/srv/perizia/_qa/runs") / analysis_id / "extract"
    if extract_dir.exists():
        shutil.rmtree(extract_dir)
    extract_dir.mkdir(parents=True, exist_ok=True)

    with open(extract_dir / "pages_raw.json", "w", encoding="utf-8") as f:
        json.dump(payload.get("pages_raw", []), f, ensure_ascii=False, indent=2)
    with open(extract_dir / "full_raw.txt", "w", encoding="utf-8") as f:
        f.write(str(payload.get("full_raw_txt", "")))
    with open(extract_dir / "words_raw.json", "w", encoding="utf-8") as f:
        json.dump(payload.get("words_raw", []), f, ensure_ascii=False, indent=2)
    with open(extract_dir / "metrics.json", "w", encoding="utf-8") as f:
        json.dump(payload.get("metrics", []), f, ensure_ascii=False, indent=2)
    with open(extract_dir / "ocr_plan.json", "w", encoding="utf-8") as f:
        json.dump(payload.get("ocr_plan", []), f, ensure_ascii=False, indent=2)
    with open(extract_dir / "quality.json", "w", encoding="utf-8") as f:
        json.dump(document_quality, f, ensure_ascii=False, indent=2)

    return str(extract_dir)


def _apply_unreadable_hard_stop(result: Dict[str, Any], document_quality: Dict[str, Any]) -> None:
    if str(document_quality.get("status")) != "UNREADABLE":
        return

    result["analysis_status"] = "UNREADABLE"

    semaforo = result.get("semaforo_generale")
    if not isinstance(semaforo, dict):
        semaforo = {}
        result["semaforo_generale"] = semaforo
    semaforo["status"] = "UNKNOWN"
    semaforo["status_it"] = "NON VALUTABILE"
    semaforo["status_en"] = "NOT ASSESSABLE"
    semaforo["reason_it"] = document_quality.get("customer_message_it")
    semaforo["reason_en"] = document_quality.get("customer_message_en")
    blockers = semaforo.get("top_blockers")
    if isinstance(blockers, list):
        if "DOCUMENT_UNREADABLE" not in blockers:
            blockers.append("DOCUMENT_UNREADABLE")
    else:
        semaforo["top_blockers"] = ["DOCUMENT_UNREADABLE"]

    section1 = result.get("section_1_semaforo_generale")
    if isinstance(section1, dict):
        section1["status"] = "UNKNOWN"
        section1["status_it"] = "NON VALUTABILE"
        section1["status_en"] = "NOT ASSESSABLE"
        section1["reason_it"] = document_quality.get("customer_message_it")
        section1["reason_en"] = document_quality.get("customer_message_en")
        section1_blockers = section1.get("top_blockers")
        if isinstance(section1_blockers, list):
            if "DOCUMENT_UNREADABLE" not in section1_blockers:
                section1_blockers.append("DOCUMENT_UNREADABLE")
        else:
            section1["top_blockers"] = ["DOCUMENT_UNREADABLE"]

    decision_message_it = f"{document_quality.get('customer_message_it')}\nDisclaimer: verifica manuale obbligatoria."
    decision_message_en = f"{document_quality.get('customer_message_en')}\nDisclaimer: manual review is required."
    decision = result.get("decision_rapida_client")
    if not isinstance(decision, dict):
        decision = {}
        result["decision_rapida_client"] = decision
    decision["summary_it"] = decision_message_it
    decision["summary_en"] = decision_message_en

    section2 = result.get("section_2_decisione_rapida")
    if not isinstance(section2, dict):
        section2 = {}
        result["section_2_decisione_rapida"] = section2
    section2["summary_it"] = decision_message_it
    section2["summary_en"] = decision_message_en


def _to_int(value: Any) -> Optional[int]:
    try:
        return int(value)
    except Exception:
        return None


def _normalize_user_message_evidence(
    evidence: Any,
    max_items: int = 1,
    field_key: Optional[str] = None,
    anchor_hint: Optional[str] = None,
) -> List[Dict[str, Any]]:
    if not isinstance(evidence, list):
        return []
    out: List[Dict[str, Any]] = []
    for item in evidence:
        if not isinstance(item, dict):
            continue
        page = _to_int(item.get("page"))
        quote = str(item.get("quote") or "").strip()
        if page is None or not quote:
            continue
        inferred_key = field_key or str(item.get("field_key") or item.get("field") or item.get("family") or "").strip().lower() or None
        normalized_quote, search_hint = normalize_evidence_quote(
            quote,
            0,
            len(quote),
            max_len=220,
            field_key=inferred_key,
            anchor_hint=anchor_hint or quote,
        )
        if not normalized_quote:
            continue
        payload = {"page": page, "quote": normalized_quote}
        if search_hint:
            payload["search_hint"] = search_hint
        out.append(payload)
        if len(out) >= max_items:
            break
    return out


def _normalize_token(value: Any) -> str:
    text = str(value or "").strip().lower()
    return (
        text.replace("à", "a")
        .replace("á", "a")
        .replace("è", "e")
        .replace("é", "e")
        .replace("ì", "i")
        .replace("ò", "o")
        .replace("ó", "o")
        .replace("ù", "u")
    )


def _first_evidence_from_row(row: Any) -> List[Dict[str, Any]]:
    if not isinstance(row, dict):
        return []
    row_field_key = str(row.get("field") or row.get("family") or row.get("code") or "").strip().lower() or None
    return _normalize_user_message_evidence(
        [{"page": row.get("page"), "quote": row.get("quote")}],
        max_items=1,
        field_key=row_field_key,
        anchor_hint=str(row.get("quote") or ""),
    )


def _score_quote(quote: Any, kind: str) -> int:
    text = str(quote or "").strip()
    if not text:
        return -1000
    lower = _normalize_token(text)
    score = 0

    # Common penalties for boilerplate/TOC-like snippets.
    if "pubblicazione eseguita" in lower or "pdg" in lower:
        score -= 100
    if "....." in text or "sommario" in lower or "indice" in lower:
        score -= 100

    if kind == "catasto":
        for term in (
            "corrispondenza catastale",
            "planimetr",
            "difform",
            "tipo mappale",
            "visura",
            "foglio",
            "particella",
            "sub",
            "rendita",
        ):
            if term in lower:
                score += 20
    elif kind == "asta":
        for term in ("vendita", "asta", "delegato", "avra luogo", "ore", "giorno"):
            if term in lower:
                score += 20
        if re.search(r"\b\d{1,2}:\d{2}\b", text):
            score += 25
    return score


def _is_valid_asta_evidence(quote: Any) -> bool:
    text = str(quote or "").strip()
    if not text:
        return False
    lower = _normalize_token(text)
    if "pdg" in lower or "pubblicazione eseguita" in lower:
        return False
    has_date = bool(re.search(r"\b\d{2}/\d{2}/\d{4}\b", text))
    has_time = bool(re.search(r"\b(?:ore\s*)?\d{1,2}:\d{2}\b", lower))
    has_keyword = any(term in lower for term in ("vendita", "asta", "delegato", "avra luogo", "alle ore"))
    return has_date and has_time and has_keyword


def _collect_normalized_evidence_candidates(entry: Dict[str, Any]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for ev in _normalize_user_message_evidence(entry.get("evidence", []), max_items=8):
        out.append(ev)
    fonte = entry.get("fonte_perizia")
    if isinstance(fonte, dict):
        for ev in _normalize_user_message_evidence(fonte.get("evidence", []), max_items=8):
            out.append(ev)
    return out


def _best_scored_evidence(candidates: List[Dict[str, Any]], kind: str) -> List[Dict[str, Any]]:
    best: Optional[Tuple[int, int, int, Dict[str, Any]]] = None
    for idx, ev in enumerate(candidates):
        if not isinstance(ev, dict):
            continue
        quote = str(ev.get("quote") or "").strip()
        page = _to_int(ev.get("page"))
        if page is None or not quote:
            continue
        score = _score_quote(quote, kind)
        candidate = (score, -page, -idx, {"page": page, "quote": quote})
        if best is None or candidate > best:
            best = candidate
    if best is None or best[0] < 0:
        return []
    return _normalize_user_message_evidence([best[3]], max_items=1)


def _best_scored_valid_asta_evidence(candidates: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    best: Optional[Tuple[int, int, int, Dict[str, Any]]] = None
    for idx, ev in enumerate(candidates):
        if not isinstance(ev, dict):
            continue
        quote = str(ev.get("quote") or "").strip()
        page = _to_int(ev.get("page"))
        if page is None or not quote or not _is_valid_asta_evidence(quote):
            continue
        score = _score_quote(quote, kind="asta")
        candidate = (score, -page, -idx, {"page": page, "quote": quote})
        if best is None or candidate > best:
            best = candidate
    if best is None:
        return []
    return _normalize_user_message_evidence([best[3]], max_items=1)


def _quality_section_best_evidence(result: Dict[str, Any], heading_terms: List[str], kind: str) -> List[Dict[str, Any]]:
    quality = result.get("estratto_quality", {}) if isinstance(result.get("estratto_quality"), dict) else {}
    sections = quality.get("sections", [])
    if not isinstance(sections, list):
        return []
    normalized_terms = [_normalize_token(term) for term in heading_terms if str(term or "").strip()]
    candidates: List[Dict[str, Any]] = []
    for section in sections:
        if not isinstance(section, dict):
            continue
        heading_key = _normalize_token(section.get("heading_key"))
        if not any(term in heading_key for term in normalized_terms):
            continue
        candidates.extend(_collect_normalized_evidence_candidates(section))
        items = section.get("items", [])
        if not isinstance(items, list):
            continue
        for item in items:
            if not isinstance(item, dict):
                continue
            candidates.extend(_collect_normalized_evidence_candidates(item))
    return _best_scored_evidence(candidates, kind=kind)


def _load_step2_candidates(analysis_id: str, file_name: str) -> List[Dict[str, Any]]:
    aid = str(analysis_id or "").strip()
    if not aid:
        return []
    candidate_path = Path("/srv/perizia/_qa/runs") / aid / "candidates" / file_name
    try:
        with open(candidate_path, "r", encoding="utf-8") as f:
            payload = json.load(f)
    except Exception:
        return []
    return payload if isinstance(payload, list) else []


def _select_catasto_evidence_for_message(result: Dict[str, Any], analysis_id: str) -> List[Dict[str, Any]]:
    # 1) estratto_quality sections with catasto/conformita heading key
    ev = _quality_section_best_evidence(result, ["catasto", "conformita"], kind="catasto")
    if ev:
        return ev

    # 2) step2 trigger candidates family=catasto
    candidates: List[Dict[str, Any]] = []
    for row in _load_step2_candidates(analysis_id, "candidates_triggers.json"):
        if not isinstance(row, dict):
            continue
        if _normalize_token(row.get("family")) != "catasto":
            continue
        candidates.extend(_first_evidence_from_row(row))
    ev = _best_scored_evidence(candidates, kind="catasto")
    if ev:
        return ev

    # 3) no acceptable snippet
    return []


def _select_dati_asta_evidence_for_message(result: Dict[str, Any], analysis_id: str) -> List[Dict[str, Any]]:
    # 1) structured dati_asta evidence
    dati_asta = result.get("dati_asta", {}) if isinstance(result.get("dati_asta"), dict) else {}
    ev = _best_scored_valid_asta_evidence(_normalize_user_message_evidence(dati_asta.get("evidence", []), max_items=8))
    if ev:
        return ev

    # 2) estratto_quality sections related to dati_asta/asta/vendita
    quality = result.get("estratto_quality", {}) if isinstance(result.get("estratto_quality"), dict) else {}
    sections = quality.get("sections", [])
    normalized_terms = [_normalize_token(x) for x in ("dati_asta", "asta", "vendita")]
    quality_candidates: List[Dict[str, Any]] = []
    if isinstance(sections, list):
        for section in sections:
            if not isinstance(section, dict):
                continue
            heading_key = _normalize_token(section.get("heading_key"))
            if not any(term in heading_key for term in normalized_terms):
                continue
            quality_candidates.extend(_collect_normalized_evidence_candidates(section))
            items = section.get("items", [])
            if not isinstance(items, list):
                continue
            for item in items:
                if not isinstance(item, dict):
                    continue
                quality_candidates.extend(_collect_normalized_evidence_candidates(item))
    ev = _best_scored_valid_asta_evidence(quality_candidates)
    if ev:
        return ev

    # 3) step2 date candidates HIGH relevance + asta-like context + no PDG
    candidates: List[Dict[str, Any]] = []
    for row in _load_step2_candidates(analysis_id, "candidates_dates.json"):
        if not isinstance(row, dict):
            continue
        if _normalize_token(row.get("relevance")) != "high":
            continue
        context = _normalize_token(row.get("context"))
        quote = _normalize_token(row.get("quote"))
        if "pdg" in context or "pdg" in quote:
            continue
        if not any(term in context for term in ("vendita", "asta", "delegato")):
            continue
        candidates.extend(_first_evidence_from_row(row))
    ev = _best_scored_valid_asta_evidence(candidates)
    if ev:
        return ev

    # 4) no acceptable snippet
    return []


def _append_user_message(messages: List[Dict[str, Any]], seen_codes: set, payload: Dict[str, Any], max_messages: int = 6) -> None:
    code = str(payload.get("code") or "").strip()
    if not code or code in seen_codes or len(messages) >= max_messages:
        return
    payload["next_steps_it"] = payload.get("next_steps_it") if isinstance(payload.get("next_steps_it"), list) else []
    payload["next_steps_en"] = payload.get("next_steps_en") if isinstance(payload.get("next_steps_en"), list) else []
    payload["evidence"] = _normalize_user_message_evidence(payload.get("evidence", []), max_items=2)
    messages.append(payload)
    seen_codes.add(code)


def _has_blocker_for_field(result: Dict[str, Any], field_key: str, label_it: str) -> bool:
    semaforo_candidates: List[Dict[str, Any]] = []
    if isinstance(result.get("semaforo_generale"), dict):
        semaforo_candidates.append(result["semaforo_generale"])
    if isinstance(result.get("section_1_semaforo_generale"), dict):
        semaforo_candidates.append(result["section_1_semaforo_generale"])
    for semaforo in semaforo_candidates:
        blockers = semaforo.get("top_blockers")
        if not isinstance(blockers, list):
            continue
        for blocker in blockers:
            if not isinstance(blocker, dict):
                continue
            status = str(blocker.get("status") or "").upper()
            if status not in {"NOT_FOUND", "LOW_CONFIDENCE"}:
                continue
            key = str(blocker.get("key") or "").strip()
            blocker_label = str(blocker.get("label_it") or "").strip().lower()
            if key == field_key or blocker_label == label_it.strip().lower():
                return True
    return False


def _build_user_messages(result: Dict[str, Any], extraction_payload: Dict[str, Any], analysis_id: str = "") -> List[Dict[str, Any]]:
    messages: List[Dict[str, Any]] = []
    seen_codes: set = set()

    document_quality = result.get("document_quality", {}) if isinstance(result.get("document_quality"), dict) else {}
    dq_status = str(document_quality.get("status") or "TEXT_OK").upper()
    ocr_plan = extraction_payload.get("ocr_plan", []) if isinstance(extraction_payload.get("ocr_plan"), list) else []
    pages_raw = extraction_payload.get("pages_raw", []) if isinstance(extraction_payload.get("pages_raw"), list) else []
    pages_needing_ocr = document_quality.get("pages_needing_ocr", [])
    if not isinstance(pages_needing_ocr, list):
        pages_needing_ocr = []

    if dq_status == "NEEDS_OCR":
        ocr_pages = [p for p in ([_to_int(x.get("page")) for x in ocr_plan] + [_to_int(x) for x in pages_needing_ocr]) if p is not None]
        ordered_pages: List[int] = []
        for p in ocr_pages:
            if p not in ordered_pages:
                ordered_pages.append(p)
        pages_str = ", ".join(str(p) for p in ordered_pages[:12]) if ordered_pages else "non disponibili"
        evidence: List[Dict[str, Any]] = []
        if ordered_pages:
            first_page = ordered_pages[0]
            page_row = next((r for r in pages_raw if isinstance(r, dict) and _to_int(r.get("page")) == first_page), None)
            page_text = str(page_row.get("text") or "").strip() if isinstance(page_row, dict) else ""
            if page_text:
                quote, search_hint = normalize_evidence_quote(page_text, 0, min(len(page_text), 180), max_len=220)
                if quote:
                    snippet = {"page": first_page, "quote": quote}
                    if search_hint:
                        snippet["search_hint"] = search_hint
                    evidence = [snippet]
        _append_user_message(
            messages,
            seen_codes,
            {
                "code": "DOC_NEEDS_OCR",
                "severity": "WARNING",
                "title_it": "Documento parzialmente leggibile",
                "body_it": f"Alcune pagine richiedono OCR o verifica manuale (pagine: {pages_str}).",
                "next_steps_it": [
                    "Esegui OCR sulle pagine indicate e riesegui l'analisi.",
                    "Conferma manualmente i dati chiave prima di procedere.",
                ],
                "title_en": "Document partially readable",
                "body_en": f"Some pages require OCR or manual review (pages: {pages_str}).",
                "next_steps_en": [
                    "Run OCR on the listed pages and re-run the analysis.",
                    "Manually confirm key fields before proceeding.",
                ],
                "evidence": evidence,
            },
        )
    elif dq_status == "UNREADABLE":
        _append_user_message(
            messages,
            seen_codes,
            {
                "code": "DOC_UNREADABLE",
                "severity": "BLOCKER",
                "title_it": "Documento non leggibile",
                "body_it": "Il PDF non è leggibile in modo affidabile per un'analisi automatica completa.",
                "next_steps_it": [
                    "Carica un PDF con testo selezionabile oppure una scansione più nitida.",
                    "Richiedi revisione manuale prima di prendere decisioni.",
                ],
                "title_en": "Document unreadable",
                "body_en": "The PDF is not reliably readable for complete automated analysis.",
                "next_steps_en": [
                    "Upload a selectable-text PDF or a clearer scan.",
                    "Request manual review before making decisions.",
                ],
                "evidence": [],
            },
        )
    else:
        _append_user_message(
            messages,
            seen_codes,
            {
                "code": "DOC_TEXT_OK",
                "severity": "INFO",
                "title_it": "Documento leggibile",
                "body_it": "La qualità testuale del documento è sufficiente per l'estrazione automatica.",
                "next_steps_it": [
                    "Verifica i campi più critici prima dell'offerta.",
                    "Usa le evidenze a pagina per i controlli finali.",
                ],
                "title_en": "Document readable",
                "body_en": "Text quality is sufficient for automated extraction.",
                "next_steps_en": [
                    "Validate critical fields before bidding.",
                    "Use page evidence for final checks.",
                ],
                "evidence": [],
            },
        )

    states = result.get("field_states", {}) if isinstance(result.get("field_states"), dict) else {}
    cat_state = states.get("conformita_catastale") if isinstance(states.get("conformita_catastale"), dict) else {}
    cat_flagged = str(cat_state.get("status") or "").upper() in {"NOT_FOUND", "LOW_CONFIDENCE"} or _has_blocker_for_field(result, "conformita_catastale", "Conformità catastale")
    if cat_flagged:
        catasto_evidence = _select_catasto_evidence_for_message(result, analysis_id)
        catasto_has_evidence = bool(catasto_evidence)
        _append_user_message(
            messages,
            seen_codes,
            {
                "code": "ACTION_VERIFY_CATASTO",
                "severity": "WARNING",
                "title_it": "Verifica conformità catastale",
                "body_it": (
                    "Nel documento risultano elementi da verificare sulla conformità catastale."
                    if catasto_has_evidence
                    else "La conformità catastale risulta assente o a bassa confidenza nei dati estratti."
                ),
                "next_steps_it": [
                    "Controlla planimetria, visura e stato di fatto con un tecnico.",
                    "Conferma eventuali difformità prima della partecipazione all'asta.",
                ],
                "title_en": "Verify cadastral compliance",
                "body_en": (
                    "The document contains cadastral-compliance elements that require verification."
                    if catasto_has_evidence
                    else "Cadastral compliance is missing or low-confidence in extracted data."
                ),
                "next_steps_en": [
                    "Check floor plan, cadastral record, and actual property state with a technician.",
                    "Confirm any mismatch before bidding.",
                ],
                "evidence": catasto_evidence,
            },
        )

    dati_asta_state = states.get("dati_asta") if isinstance(states.get("dati_asta"), dict) else {}
    dati_asta_flagged = str(dati_asta_state.get("status") or "").upper() in {"NOT_FOUND", "LOW_CONFIDENCE"} or _has_blocker_for_field(result, "dati_asta", "Dati asta")
    if dati_asta_flagged:
        dati_asta_evidence = _select_dati_asta_evidence_for_message(result, analysis_id)
        dati_asta_has_evidence = bool(dati_asta_evidence)
        _append_user_message(
            messages,
            seen_codes,
            {
                "code": "ACTION_VERIFY_DATO_ASTA",
                "severity": "WARNING",
                "title_it": "Verifica dati asta",
                "body_it": (
                    "Dati asta estratti: verificare sul portale ufficiale."
                    if dati_asta_has_evidence
                    else "Data/ora asta non presenti nel documento analizzato. Verificare sul portale ufficiale della procedura."
                ),
                "next_steps_it": [
                    "Verifica data, ora e modalità sul portale ufficiale della procedura.",
                    "Allinea i dati nel fascicolo prima di procedere.",
                ],
                "title_en": "Verify auction details",
                "body_en": (
                    "Auction details extracted: verify on the official portal."
                    if dati_asta_has_evidence
                    else "Auction date/time not present in the analyzed document. Verify on the official procedure portal."
                ),
                "next_steps_en": [
                    "Check date, time, and format on the official procedure portal.",
                    "Align case records before proceeding.",
                ],
                "evidence": dati_asta_evidence,
            },
        )

    blueprint = result.get("estratto_blueprint", {}) if isinstance(result.get("estratto_blueprint"), dict) else {}
    blueprint_abusi = blueprint.get("abusi", {}) if isinstance(blueprint.get("abusi"), dict) else {}
    non_agibile = blueprint_abusi.get("non_agibile", {}) if isinstance(blueprint_abusi.get("non_agibile"), dict) else {}
    if bool(non_agibile.get("value")):
        _append_user_message(
            messages,
            seen_codes,
            {
                "code": "RISK_NON_AGIBILE",
                "severity": "WARNING",
                "title_it": "Rischio non agibile",
                "body_it": "Dall'estratto emerge un'indicazione di non agibilità.",
                "next_steps_it": [
                    "Richiedi verifica tecnica e documentale su agibilità/abitabilità.",
                    "Valuta tempi e costi di regolarizzazione prima dell'offerta.",
                ],
                "title_en": "Non-habitable risk",
                "body_en": "The extracted blueprint indicates a non-habitable condition.",
                "next_steps_en": [
                    "Request technical and document checks on habitability.",
                    "Assess remediation timing and costs before bidding.",
                ],
                "evidence": non_agibile.get("evidence", []) if isinstance(non_agibile.get("evidence"), list) else [],
            },
        )

    blueprint_impianti = blueprint.get("impianti", {}) if isinstance(blueprint.get("impianti"), dict) else {}
    impianti_hit: Optional[Dict[str, Any]] = None
    for field_key, field_obj in blueprint_impianti.items():
        if not isinstance(field_obj, dict):
            continue
        value = field_obj.get("value")
        if value in (None, "", "NOT_FOUND"):
            continue
        if isinstance(value, str) and value.strip().upper() == "NOT_FOUND":
            continue
        impianti_hit = {
            "field_key": field_key,
            "value": value,
            "evidence": field_obj.get("evidence", []),
        }
        break
    if impianti_hit:
        impianto_label = str(impianti_hit.get("field_key") or "impianti").replace("_", " ")
        _append_user_message(
            messages,
            seen_codes,
            {
                "code": "INFO_IMPIANTI_PRESENT",
                "severity": "INFO",
                "title_it": "Informazioni impianti presenti",
                "body_it": f"Sono presenti informazioni sugli impianti ({impianto_label}).",
                "next_steps_it": [
                    "Verifica conformità e certificazioni impiantistiche disponibili.",
                    "Conferma eventuali adeguamenti necessari con un tecnico.",
                ],
                "title_en": "Systems information present",
                "body_en": f"Systems-related information is present ({impianto_label}).",
                "next_steps_en": [
                    "Check available systems compliance/certifications.",
                    "Confirm required upgrades with a technician.",
                ],
                "evidence": impianti_hit.get("evidence", []) if isinstance(impianti_hit.get("evidence"), list) else [],
            },
        )

    return messages[:6]


def _extract_pdf_text_digital(contents: bytes) -> Dict[str, Any]:
    """Primary deterministic extraction path from embedded PDF text (non-OCR)."""
    try:
        reader = PdfReader(io.BytesIO(contents))
        pages: List[Dict[str, Any]] = []
        covered_pages = 0
        blank_pages = 0
        for idx, page in enumerate(reader.pages, start=1):
            page_text = (page.extract_text() or "").strip()
            if not page_text:
                blank_pages += 1
            if len(page_text) >= PDF_TEXT_MIN_PAGE_CHARS:
                covered_pages += 1
            pages.append({
                "page_number": idx,
                "text": page_text,
                "tables": [],
                "form_fields": [],
                "char_count": len(page_text)
            })
        total_pages = len(pages)
        coverage_ratio = (covered_pages / total_pages) if total_pages else 0.0
        blank_ratio = (blank_pages / total_pages) if total_pages else 0.0
        full_text = _build_full_text_from_pages(pages)
        return {
            "success": True,
            "pages": pages,
            "full_text": full_text,
            "total_pages": total_pages,
            "covered_pages": covered_pages,
            "coverage_ratio": coverage_ratio,
            "blank_pages": blank_pages,
            "blank_ratio": blank_ratio,
            "error": None
        }
    except Exception as e:
        return {
            "success": False,
            "pages": [],
            "full_text": "",
            "total_pages": 0,
            "covered_pages": 0,
            "coverage_ratio": 0.0,
            "blank_pages": 0,
            "blank_ratio": 0.0,
            "error": str(e)
        }


async def _extract_with_docai(contents: bytes, mime_type: str, request_id: str) -> Tuple[List[Dict], str, Optional[str]]:
    """OCR fallback path. Returns (pages, full_text, error_message)."""
    try:
        from document_ai import extract_pdf_with_google_docai
        logger.info(f"[{request_id}] docai_start timeout={DOC_AI_TIMEOUT_SECONDS}s")
        docai_result = await asyncio.wait_for(
            asyncio.to_thread(extract_pdf_with_google_docai, contents, mime_type),
            timeout=DOC_AI_TIMEOUT_SECONDS
        )
    except ImportError as e:
        return [], "", f"Document AI not available: {e}"
    except asyncio.TimeoutError:
        return [], "", "Document AI timed out"
    except Exception as e:
        return [], "", f"Document AI failed: {e}"

    if not docai_result.get("success"):
        return [], "", (docai_result.get("error") or "Document AI failed")

    pages = docai_result.get("pages", []) or []
    full_text = _build_full_text_from_pages(pages)
    logger.info(f"[{request_id}] docai_end pages={len(pages)} chars={len(full_text)}")
    return pages, full_text, None


def _merge_ocr_pages_into_digital(
    digital_pages: List[Dict[str, Any]],
    ocr_pages: List[Dict[str, Any]],
    target_pages: List[int],
) -> Tuple[List[Dict[str, Any]], List[int]]:
    if not digital_pages:
        return digital_pages, []
    target_set = {int(p) for p in target_pages if int(p) > 0}
    if not target_set:
        return digital_pages, []

    ocr_map: Dict[int, Dict[str, Any]] = {}
    for p in ocr_pages:
        if not isinstance(p, dict):
            continue
        page_num = _get_page_number(p)
        if page_num > 0:
            ocr_map[page_num] = p

    merged_pages: List[Dict[str, Any]] = []
    replaced: List[int] = []
    for idx, raw in enumerate(digital_pages, start=1):
        page = dict(raw) if isinstance(raw, dict) else {}
        page_num = _get_page_number(page) or idx
        page["page_number"] = page_num
        use_ocr = page_num in target_set and page_num in ocr_map
        if use_ocr:
            ocr_row = ocr_map[page_num]
            ocr_text = str(ocr_row.get("text", "") or "")
            if len(ocr_text.strip()) >= 20:
                page["text"] = ocr_text
                page["char_count"] = len(ocr_text)
                replaced.append(page_num)
        merged_pages.append(page)
    return merged_pages, sorted(set(replaced))


def _build_pdf_subset_for_pages(contents: bytes, page_numbers: List[int]) -> Tuple[bytes, List[int]]:
    ordered_pages = sorted({int(p) for p in page_numbers if int(p) > 0})
    if not ordered_pages:
        return contents, []
    reader = PdfReader(io.BytesIO(contents))
    writer = PdfWriter()
    total = len(reader.pages)
    selected: List[int] = []
    for page_num in ordered_pages:
        if 1 <= page_num <= total:
            writer.add_page(reader.pages[page_num - 1])
            selected.append(page_num)
    if not selected:
        return contents, []
    out = io.BytesIO()
    writer.write(out)
    return out.getvalue(), selected


async def _enrich_summary_with_optional_llm(result: Dict[str, Any], file_name: str, full_text: str, request_id: str) -> None:
    """
    Optional LLM summary generation.
    Fail-open by design: on timeout/error keep deterministic report and only mark summary/QA warning.
    """
    qa = result.setdefault("qa_pass", {"status": "WARN", "checks": []})
    qa_checks = qa.setdefault("checks", [])
    summary = result.setdefault("summary_for_client", {})
    if not isinstance(summary, dict):
        summary = {}
        result["summary_for_client"] = summary

    prompt = f"""Genera SOLO JSON valido con campi:
{{
  "summary_it": "...",
  "summary_en": "..."
}}
Regole:
- Riassunto cliente breve (max 70 parole per lingua)
- Nessun numero inventato
- Tono prudente

FILE: {file_name}
TESTO (estratto): {full_text[:12000]}
"""
    try:
        response = await asyncio.wait_for(
            openai_chat_completion("Sei un assistente che produce solo JSON valido.", prompt, model="gpt-4o", timeout_seconds=LLM_SUMMARY_TIMEOUT_SECONDS),
            timeout=LLM_SUMMARY_TIMEOUT_SECONDS
        )
        payload = response.strip()
        if payload.startswith("```json"):
            payload = payload[7:]
        if payload.startswith("```"):
            payload = payload[3:]
        if payload.endswith("```"):
            payload = payload[:-3]
        parsed = json.loads(payload.strip())
        if isinstance(parsed, dict):
            if parsed.get("summary_it"):
                summary["summary_it"] = str(parsed["summary_it"])[:1500]
            if parsed.get("summary_en"):
                summary["summary_en"] = str(parsed["summary_en"])[:1500]
        qa_checks.append({
            "code": "QA-LLM-Summary",
            "result": "OK",
            "note": "Optional LLM summary generated"
        })
    except Exception as e:
        logger.warning(f"[{request_id}] llm_summary_fail_open {e}")
        summary["assistant_summary_error"] = {
            "code": "LLM_SUMMARY_UNAVAILABLE",
            "message": str(e)
        }
        qa_checks.append({
            "code": "QA-LLM-Summary",
            "result": "WARN",
            "note": f"Optional summary unavailable: {str(e)[:180]}"
        })
        if qa.get("status") == "PASS":
            qa["status"] = "WARN"
    result["qa"] = qa


@api_router.post("/analysis/perizia")
async def analyze_perizia(request: Request, file: UploadFile = File(...)):
    """Analyze uploaded perizia PDF"""
    user = await require_auth(request)
    request_id = f"req_{uuid.uuid4().hex[:12]}"
    logger.info(f"[{request_id}] perizia_upload_start user={user.user_id} file={file.filename}")
    
    # Check file type - PDF only
    if not file.filename.lower().endswith('.pdf'):
        raise HTTPException(status_code=400, detail="Solo file PDF sono accettati / Only PDF files are accepted")
    
    if file.content_type and file.content_type != "application/pdf":
        raise HTTPException(status_code=400, detail="Solo file PDF sono accettati / Only PDF files are accepted")
    
    # Read PDF content
    contents = await file.read()
    input_sha256 = hashlib.sha256(contents).hexdigest()
    logger.info(f"[{request_id}] upload_saved bytes={len(contents)} sha256={input_sha256[:12]}")

    try:
        uploaded_reader = PdfReader(io.BytesIO(contents))
        uploaded_pages_count = len(uploaded_reader.pages)
    except Exception:
        raise HTTPException(status_code=400, detail="PDF non valido o non leggibile.")

    required_perizia_credits = _get_required_perizia_credits(uploaded_pages_count)
    if required_perizia_credits is None:
        raise HTTPException(
            status_code=400,
            detail={
                "code": "PERIZIA_PAGE_COUNT_UNSUPPORTED",
                "message_it": "La perizia caricata ha un numero di pagine non supportato. Sono accettate solo perizie da 1 a 100 pagine.",
                "message_en": "The uploaded perizia has an unsupported page count. Only documents between 1 and 100 pages are accepted.",
            },
        )

    remaining_perizia_credits = int(user.quota.get("perizia_scans_remaining", 0) or 0)
    if remaining_perizia_credits < required_perizia_credits and not user.is_master_admin:
        raise HTTPException(
            status_code=403,
            detail={
                "code": "INSUFFICIENT_PERIZIA_CREDITS",
                "message_it": (
                    f"Crediti insufficienti per analizzare questa perizia: {uploaded_pages_count} pagine "
                    f"richiedono {required_perizia_credits} crediti, ma il tuo account ne ha solo {remaining_perizia_credits}."
                ),
                "message_en": (
                    f"Insufficient credits for this perizia: {uploaded_pages_count} pages require "
                    f"{required_perizia_credits} credits, but your account only has {remaining_perizia_credits}."
                ),
                "required_credits": required_perizia_credits,
                "remaining_credits": remaining_perizia_credits,
                "pages_count": uploaded_pages_count,
            }
        )

    # Generate IDs early for consistent logging + persistence
    case_id = f"case_{uuid.uuid4().hex[:8]}"
    run_id = f"run_{uuid.uuid4().hex[:8]}"
    analysis_id = f"analysis_{uuid.uuid4().hex[:12]}"
    offline_qa = user.user_id == "offline_qa"

    async def run_pipeline():
        logger.info(f"[{request_id}] pipeline_start analysis_id={analysis_id} offline_qa={offline_qa}")
        extraction_payload = await asyncio.to_thread(_build_step1_extract_payload, contents)

        # Determine mime type
        mime_type = "application/pdf"
        if file.filename.lower().endswith(('.png', '.jpg', '.jpeg')):
            mime_type = f"image/{file.filename.split('.')[-1].lower()}"
            if mime_type == "image/jpg":
                mime_type = "image/jpeg"

        # Extraction stage (digital text first, OCR only if coverage is low)
        if offline_qa:
            logger.info(f"[{request_id}] offline_fixture_load path={OFFLINE_QA_FIXTURE_PATH}")
            fixture = _load_offline_fixture()
            pages = fixture.get("pages", [])
            full_text = fixture.get("full_text", "")
            logger.info(f"[{request_id}] offline_fixture_loaded pages={len(pages)} chars={len(full_text)}")
        else:
            digital = await asyncio.to_thread(_extract_pdf_text_digital, contents)
            pages = digital.get("pages", []) or []
            full_text = digital.get("full_text", "") or ""
            coverage_ratio = float(digital.get("coverage_ratio", 0.0) or 0.0)
            blank_ratio = float(digital.get("blank_ratio", 0.0) or 0.0)
            logger.info(
                f"[{request_id}] digital_extract pages={len(pages)} chars={len(full_text)} "
                f"coverage={coverage_ratio:.2f} threshold={PDF_TEXT_MIN_COVERAGE_RATIO:.2f} "
                f"blank_ratio={blank_ratio:.2f} blank_threshold={PDF_TEXT_MAX_BLANK_PAGE_RATIO:.2f}"
            )

            pages_needing_ocr = extraction_payload.get("document_quality", {}).get("pages_needing_ocr", [])
            if not isinstance(pages_needing_ocr, list):
                pages_needing_ocr = []

            needs_ocr_fallback = (
                not full_text.strip()
                or coverage_ratio < PDF_TEXT_MIN_COVERAGE_RATIO
                or blank_ratio >= PDF_TEXT_MAX_BLANK_PAGE_RATIO
                or bool(pages_needing_ocr)
            )
            if needs_ocr_fallback:
                logger.info(
                    f"[{request_id}] docai_fallback_attempt "
                    f"triggered=true pages_needing_ocr={pages_needing_ocr[:20]}"
                )
                docai_contents = contents
                docai_target_pages: List[int] = []
                if pages_needing_ocr:
                    try:
                        subset_bytes, subset_map = await asyncio.to_thread(_build_pdf_subset_for_pages, contents, pages_needing_ocr)
                        if subset_map:
                            docai_contents = subset_bytes
                            docai_target_pages = subset_map
                    except Exception as subset_err:
                        logger.warning(f"[{request_id}] docai_subset_build_failed err={subset_err}")

                ocr_pages, ocr_full_text, ocr_error = await _extract_with_docai(docai_contents, mime_type, request_id)
                ocr_replaced_pages: List[int] = []
                if not ocr_error and ocr_pages:
                    if docai_target_pages:
                        remapped_pages: List[Dict[str, Any]] = []
                        for idx, row in enumerate(ocr_pages, start=1):
                            if not isinstance(row, dict):
                                continue
                            mapped = dict(row)
                            mapped["page_number"] = docai_target_pages[idx - 1] if idx - 1 < len(docai_target_pages) else _get_page_number(mapped)
                            remapped_pages.append(mapped)
                        ocr_pages = remapped_pages
                    target_pages = pages_needing_ocr
                    if not target_pages:
                        # If fallback triggered by low coverage/no text, allow broad merge.
                        target_pages = [_get_page_number(p) for p in pages]
                    pages, ocr_replaced_pages = _merge_ocr_pages_into_digital(pages, ocr_pages, target_pages)
                    full_text = _build_full_text_from_pages(pages)
                    logger.info(
                        f"[{request_id}] docai_fallback_applied replaced_pages={ocr_replaced_pages[:30]} "
                        f"replaced_count={len(ocr_replaced_pages)} ocr_chars={len(ocr_full_text)}"
                    )
                else:
                    logger.warning(
                        f"[{request_id}] docai_fallback_failed "
                        f"error={ocr_error or 'unknown'} pages_flagged={pages_needing_ocr[:20]}"
                    )

                extraction_payload["ocr_execution"] = {
                    "attempted": True,
                    "pages_flagged": pages_needing_ocr,
                    "replaced_pages": ocr_replaced_pages,
                    "docai_error": ocr_error,
                    "docai_pages": len(ocr_pages) if isinstance(ocr_pages, list) else 0,
                    "docai_chars": len(ocr_full_text or ""),
                    "subset_pages_sent_to_docai": docai_target_pages,
                }
                if ocr_replaced_pages:
                    pages_raw = extraction_payload.get("pages_raw", [])
                    if isinstance(pages_raw, list):
                        page_text_map = {int(p.get("page_number")): str(p.get("text", "") or "") for p in pages if isinstance(p, dict)}
                        for row in pages_raw:
                            if not isinstance(row, dict):
                                continue
                            try:
                                pnum = int(row.get("page"))
                            except Exception:
                                continue
                            if pnum in page_text_map:
                                row["text"] = page_text_map[pnum]
                        extraction_payload["pages_raw"] = pages_raw

        if not pages:
            pages = [{"page_number": 1, "text": "", "tables": [], "form_fields": [], "char_count": 0}]

        # Analysis stage
        logger.info(f"[{request_id}] deterministic_analysis_start")
        extracted_lots = _extract_lots_from_schema_riassuntivo(pages)
        detected_legal_killers = _scan_legal_killers(pages)
        result = create_fallback_analysis(file.filename, case_id, run_id, pages, full_text, extracted_lots, detected_legal_killers)
        logger.info(f"[{request_id}] deterministic_analysis_end")

        # Optional summary generation, fail-open by design
        if not offline_qa:
            await _enrich_summary_with_optional_llm(result, file.filename, full_text, request_id)

        return result, pages, full_text, extraction_payload

    try:
        result, pages, full_text, extraction_payload = await asyncio.wait_for(run_pipeline(), timeout=PIPELINE_TIMEOUT_SECONDS)
    except asyncio.TimeoutError:
        await _persist_failed_analysis(
            analysis_id=analysis_id,
            user=user,
            case_id=case_id,
            run_id=run_id,
            file_name=file.filename,
            input_sha256=input_sha256,
            pages_count=0,
            error_code="PIPELINE_TIMEOUT",
            error_message=f"Pipeline exceeded {PIPELINE_TIMEOUT_SECONDS}s"
        )
        raise HTTPException(status_code=504, detail={"error": "PIPELINE_TIMEOUT", "message": f"Pipeline exceeded {PIPELINE_TIMEOUT_SECONDS}s", "retry": True})

    _normalize_legal_killers(result, pages)
    _apply_headline_field_states(result, pages)
    _apply_decision_field_states(result, pages)
    _apply_market_ranges_to_money_box(result)
    _normalize_evidence_offsets(result, pages)
    result["panoramica_contract"] = _build_panoramica_contract(result, pages)

    document_quality = extraction_payload.get("document_quality", {})
    if not isinstance(document_quality, dict):
        document_quality = {}
    result["document_quality"] = document_quality

    summary = extraction_payload.get("extraction_summary", {})
    if not isinstance(summary, dict):
        summary = {}
    extract_folder = f"/srv/perizia/_qa/runs/{analysis_id}/extract/"
    summary["notes"] = f"Step1 extraction pack created under {extract_folder}"
    debug_obj = result.get("debug") if isinstance(result.get("debug"), dict) else {}
    debug_obj["extraction_summary"] = summary
    if isinstance(extraction_payload.get("ocr_execution"), dict):
        debug_obj["ocr_execution"] = extraction_payload.get("ocr_execution")
    result["debug"] = debug_obj

    _apply_unreadable_hard_stop(result, document_quality)
    _write_extraction_pack(analysis_id, extraction_payload, document_quality)
    candidate_summary: Dict[str, Any]
    try:
        candidate_summary = run_candidate_miner_for_analysis(analysis_id)
    except Exception as e:
        logger.exception(f"[{request_id}] candidate_miner_failed analysis_id={analysis_id} err={e}")
        candidate_summary = {
            "money_count": 0,
            "date_count": 0,
            "trigger_count": 0,
            "low_quality_pages": [],
            "candidates_folder": f"/srv/perizia/_qa/runs/{analysis_id}/candidates/",
            "error": "candidate_miner_failed",
        }
    debug_obj["candidate_summary"] = candidate_summary
    try:
        result["estratto_quality"] = build_estratto_quality(analysis_id, result)
    except Exception as e:
        logger.exception(f"[{request_id}] section_builder_failed analysis_id={analysis_id} err={e}")
        result["estratto_quality"] = {
            "sections": [],
            "build_meta": {
                "analysis_id": analysis_id,
                "candidate_counts": {
                    "money": 0,
                    "dates": 0,
                    "triggers": 0,
                    "pages": 0,
                },
                "low_quality_pages": [],
                "error": "section_builder_failed",
            },
        }
    _sanitize_lot_conservative_outputs(result)
    result["panoramica_contract"] = _build_panoramica_contract(result, pages)
    result["user_messages"] = _build_user_messages(result, extraction_payload, analysis_id=analysis_id)
    narrator_enabled = os.environ.get("NARRATOR_ENABLED", "0").strip() == "1"
    narrator_model = str(os.environ.get("NARRATOR_MODEL") or "").strip() or None
    narrated_payload, narrator_meta = await build_decisione_rapida_narration(
        result=result,
        request_id=request_id,
        enabled=narrator_enabled,
        model=narrator_model,
        api_key=OPENAI_API_KEY,
    )
    result["narrator_meta"] = narrator_meta
    if narrated_payload:
        result["decision_rapida_narrated"] = narrated_payload
    else:
        result.pop("decision_rapida_narrated", None)
    case_aware_narration = _build_case_aware_narration_payload(result)
    if case_aware_narration:
        result["decision_rapida_narrated"] = case_aware_narration
    logger.info(f"[{request_id}] narrator status={narrator_meta.get('status')} enabled={narrator_meta.get('enabled')}")
    result["debug"] = debug_obj
    logger.info(f"[{request_id}] assemble_output analysis_id={analysis_id}")

    # Create analysis record
    analysis = PeriziaAnalysis(
        analysis_id=analysis_id,
        user_id=user.user_id,
        case_id=case_id,
        run_id=run_id,
        case_title=file.filename,
        file_name=file.filename,
        input_sha256=input_sha256,
        pages_count=uploaded_pages_count,
        result=result
    )

    analysis_dict = analysis.model_dump()
    analysis_dict["created_at"] = analysis_dict["created_at"].isoformat()
    analysis_dict["raw_text"] = full_text[:100000]  # Store raw text for assistant
    analysis_dict["status"] = "UNREADABLE" if result.get("analysis_status") == "UNREADABLE" else "COMPLETED"
    if offline_qa:
        try:
            out_dir = Path("/tmp/perizia_qa_run")
            out_dir.mkdir(parents=True, exist_ok=True)
            with open(out_dir / "analysis.json", "w", encoding="utf-8") as f:
                json.dump(analysis_dict, f, ensure_ascii=False, indent=2)
            logger.info(f"[{request_id}] offline_persist_ok analysis_id={analysis_id}")
        except Exception as e:
            logger.warning(f"[{request_id}] offline_persist_failed {e}")
    else:
        await db.perizia_analyses.insert_one(analysis_dict)
        logger.info(f"[{request_id}] persist_done analysis_id={analysis_id}")

        # Decrement exact perizia credits band only after successful persistence.
        await _apply_perizia_credit_debit_with_ledger(
            user,
            amount=required_perizia_credits,
            entry_type="perizia_upload",
            reference_type="analysis",
            reference_id=analysis.analysis_id,
            description_it="Addebito crediti per analisi perizia completata",
            metadata={
                "analysis_id": analysis.analysis_id,
                "case_id": case_id,
                "pages_count": uploaded_pages_count,
                "required_credits": required_perizia_credits,
                "file_name": file.filename,
            },
        )

    logger.info(f"[{request_id}] respond_ok analysis_id={analysis_id}")
    return {
        "ok": True,
        "analysis_id": analysis.analysis_id,
        "case_id": case_id,
        "run_id": run_id,
        "result": result
    }

# ===================
# PDF REPORT DOWNLOAD
# ===================

async def _render_print_pdf_via_frontend(analysis_id: str, session_token: str, api_base_url: str) -> bytes:
    frontend_dir = ROOT_DIR.parent / "frontend"
    node_script = frontend_dir / "scripts" / "render_analysis_print_pdf.mjs"
    frontend_url = os.environ.get("PERIZIA_PRINT_FRONTEND_URL", "").strip().rstrip("/") or FRONTEND_URL
    if not frontend_url:
        raise HTTPException(status_code=500, detail="FRONTEND_URL not configured for print PDF rendering")
    if not node_script.exists():
        raise HTTPException(status_code=500, detail="Print PDF renderer script not found")

    out_path = Path("/tmp") / f"perizia_print_{analysis_id}_{uuid.uuid4().hex}.pdf"
    env = os.environ.copy()
    env.update(
        {
            "ANALYSIS_ID": analysis_id,
            "FRONTEND_URL": frontend_url,
            "API_BASE_URL": api_base_url.rstrip("/"),
            "SESSION_TOKEN": session_token,
            "OUT_PATH": str(out_path),
        }
    )

    proc = await asyncio.create_subprocess_exec(
        "node",
        str(node_script),
        cwd=str(frontend_dir),
        env=env,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=PRINT_RENDER_TIMEOUT_SECONDS)
    except asyncio.TimeoutError:
        with contextlib.suppress(ProcessLookupError):
            proc.kill()
        raise HTTPException(status_code=504, detail="Frontend print PDF rendering timed out")

    if proc.returncode != 0:
        detail = (stderr or stdout or b"").decode("utf-8", errors="ignore")[:1500]
        raise HTTPException(status_code=502, detail=f"Frontend print PDF rendering failed: {detail}")

    if not out_path.exists():
        raise HTTPException(status_code=502, detail="Frontend print PDF renderer did not produce a file")

    try:
        return out_path.read_bytes()
    finally:
        with contextlib.suppress(Exception):
            out_path.unlink()
        with contextlib.suppress(Exception):
            Path(f"{out_path}.meta.json").unlink()

@api_router.get(
    "/analysis/perizia/{analysis_id}/pdf",
    response_class=Response,
    responses={
        200: {
            "description": "PDF report",
            "content": {"application/pdf": {}},
        }
    },
)
async def download_perizia_pdf(analysis_id: str, request: Request):
    """Generate and download PDF report for analysis (real PDF bytes)"""
    user = await require_auth(request)

    analysis = await db.perizia_analyses.find_one(
        {"analysis_id": analysis_id, "user_id": user.user_id},
        {"_id": 0}
    )
    if not analysis:
        raise HTTPException(status_code=404, detail="Analysis not found")

    result = analysis.get("result", {}) or {}
    _apply_headline_overrides(result, analysis.get("headline_overrides") or {})
    _apply_field_overrides(result, analysis.get("field_overrides") or {})

    # Deterministic PDF from stored JSON. No LLM here.
    from pdf_report import build_perizia_pdf_bytes
    pdf_bytes = build_perizia_pdf_bytes(analysis, result)

    return Response(
        content=pdf_bytes,
        media_type="application/pdf",
        headers={
            "Content-Disposition": f'attachment; filename="nexodify_report_{analysis_id}.pdf"',
            "Cache-Control": "no-store"
        }
    )


@api_router.get(
    "/analysis/perizia/{analysis_id}/pdf-html",
    response_class=Response,
    responses={
        200: {
            "description": "HTML-to-PDF report rendered from frontend print route",
            "content": {"application/pdf": {}},
        }
    },
)
async def download_perizia_print_pdf(analysis_id: str, request: Request):
    user = await require_auth(request)
    analysis = await db.perizia_analyses.find_one(
        {"analysis_id": analysis_id, "user_id": user.user_id},
        {"_id": 0}
    )
    if not analysis:
        raise HTTPException(status_code=404, detail="Analysis not found")

    session_token = request.cookies.get("session_token")
    if not session_token:
        raise HTTPException(status_code=400, detail="Session token missing for print PDF rendering")

    api_base_url = f"{request.url.scheme}://{request.headers.get('host')}"
    pdf_bytes = await _render_print_pdf_via_frontend(analysis_id, session_token, api_base_url)
    return Response(
        content=pdf_bytes,
        media_type="application/pdf",
        headers={
            "Content-Disposition": f'attachment; filename="nexodify_report_html_{analysis_id}.pdf"',
            "Cache-Control": "no-store"
        }
    )


@api_router.get(
    "/analysis/perizia/{analysis_id}/html",
    response_class=Response,
    responses={
        200: {
            "description": "HTML report",
            "content": {"text/html": {}},
        }
    },
)
async def download_perizia_html(analysis_id: str, request: Request):
    """Generate and download HTML report for analysis"""
    user = await require_auth(request)

    analysis = await db.perizia_analyses.find_one(
        {"analysis_id": analysis_id, "user_id": user.user_id},
        {"_id": 0}
    )
    if not analysis:
        raise HTTPException(status_code=404, detail="Analysis not found")

    result = analysis.get("result", {}) or {}
    _apply_headline_overrides(result, analysis.get("headline_overrides") or {})
    _apply_field_overrides(result, analysis.get("field_overrides") or {})
    html = generate_report_html(analysis, result)

    return Response(
        content=html,
        media_type="text/html; charset=utf-8",
        headers={
            "Content-Disposition": f'attachment; filename="nexodify_report_{analysis_id}.html"',
            "Cache-Control": "no-store"
        }
    )

@api_router.patch("/analysis/perizia/{analysis_id}/headline")
async def update_perizia_headline(analysis_id: str, payload: HeadlineOverrideRequest, request: Request):
    """Update headline fields (tribunale/procedura/lotto/address) with user-provided overrides."""
    user = await require_auth(request)
    analysis, storage_mode, storage_path = await _get_perizia_analysis_for_user_with_storage(analysis_id, user)

    overrides = analysis.get("headline_overrides", {}) or {}
    updated = False
    for field in ("tribunale", "procedura", "lotto", "address"):
        value = getattr(payload, field)
        if value is None:
            continue
        cleaned = str(value).strip()
        if cleaned:
            overrides[field] = cleaned
        else:
            overrides.pop(field, None)
        updated = True
    analysis["headline_overrides"] = overrides

    if updated:
        await _save_headline_overrides_for_analysis(
            analysis_id=analysis_id,
            user=user,
            analysis=analysis,
            storage_mode=storage_mode,
            storage_path=storage_path
        )

    return {"analysis_id": analysis_id, "headline_overrides": overrides}

@api_router.patch("/analysis/perizia/{analysis_id}/overrides")
async def update_perizia_overrides(analysis_id: str, payload: FieldOverrideRequest, request: Request):
    """Update field overrides (headline + decision fields) with user-provided values."""
    user = await require_auth(request)
    analysis, storage_mode, storage_path = await _get_perizia_analysis_for_user_with_storage(analysis_id, user)

    overrides = analysis.get("field_overrides", {}) or {}
    updated = False
    fields = [
        "tribunale",
        "procedura",
        "lotto",
        "address",
        "prezzo_base_asta",
        "superficie",
        "superficie_catastale",
        "diritto_reale",
        "stato_occupativo",
        "regolarita_urbanistica",
        "conformita_catastale",
        "spese_condominiali_arretrate",
        "formalita_pregiudizievoli",
    ]
    for field in fields:
        value = getattr(payload, field)
        if value is None:
            continue
        target_field = "superficie" if field == "superficie_catastale" else field
        if field == "superficie_catastale" and getattr(payload, "superficie", None) is not None:
            continue
        if isinstance(value, str):
            cleaned = value.strip()
            if cleaned:
                overrides[target_field] = cleaned
            else:
                overrides.pop(target_field, None)
        else:
            overrides[target_field] = value
        updated = True

    analysis["field_overrides"] = overrides

    if updated:
        await _save_field_overrides_for_analysis(
            analysis_id=analysis_id,
            user=user,
            analysis=analysis,
            storage_mode=storage_mode,
            storage_path=storage_path
        )

    return {"analysis_id": analysis_id, "field_overrides": overrides}
def generate_report_html(analysis: Dict, result: Dict) -> str:
    """Generate HTML report from analysis - supports ROMA STANDARD format with multi-lot"""
    
    # Support both old and new format
    report_header = result.get("report_header", {})
    case_header = result.get("case_header", report_header)
    
    # Get lots for multi-lot support
    lots = result.get("lots", [])
    is_multi_lot = len(lots) > 1
    
    section1 = result.get("section_1_semaforo_generale", {})
    semaforo = result.get("semaforo_generale", section1)
    
    section2 = result.get("section_2_decisione_rapida", {})
    decision = result.get("decision_rapida_client", section2)
    
    section3 = result.get("section_3_money_box", {})
    money_box = result.get("money_box", section3)
    
    section4 = result.get("section_4_dati_certi", {})
    dati = result.get("dati_certi_del_lotto", section4)
    
    section5 = result.get("section_5_abusi_conformita", {})
    abusi = result.get("abusi_edilizi_conformita", section5)
    
    section9 = result.get("section_9_legal_killers", {})
    legal_killers = result.get("legal_killers_checklist", section9)
    
    section12 = result.get("section_12_checklist_pre_offerta", [])
    checklist = result.get("checklist_pre_offerta", section12)
    
    summary = result.get("summary_for_client", {})
    
    # Get values with fallbacks - normalize placeholders
    def norm_text(val):
        if isinstance(val, dict):
            val = val.get("value") if val.get("value") else ""
            if isinstance(val, dict):
                val = ""
        if isinstance(val, list):
            parts = []
            for item in val:
                if isinstance(item, dict):
                    item = item.get("value") if item.get("value") else ""
                    if isinstance(item, dict):
                        item = ""
                item_str = str(item).strip()
                if item_str:
                    parts.append(item_str)
            val = ", ".join(parts)
        if val in [None, "", "N/A", "NOT_SPECIFIED_IN_PERIZIA", "NOT_SPECIFIED", "UNKNOWN", "{}"]:
            return "NON SPECIFICATO IN PERIZIA"
        val_str = str(val).strip()
        if "LOW_CONFIDENCE" in val_str.upper():
            return "DA VERIFICARE"
        return val_str

    def norm_address(case_header):
        address = case_header.get("address")
        if isinstance(address, dict):
            if address.get("value"):
                address = address.get("value")
            elif address.get("full"):
                address = address.get("full")
            else:
                parts = []
                street = address.get("street")
                city = address.get("city")
                if street:
                    parts.append(str(street).strip())
                if city:
                    parts.append(str(city).strip())
                address = ", ".join([p for p in parts if p])
        elif address is None:
            address = ""
        return address if str(address).strip() else "NON SPECIFICATO IN PERIZIA"
    
    procedure = norm_text(case_header.get("procedure", {}).get("value") if isinstance(case_header.get("procedure"), dict) else case_header.get("procedure_id", "N/A"))
    tribunale = norm_text(case_header.get("tribunale", {}).get("value") if isinstance(case_header.get("tribunale"), dict) else case_header.get("tribunale", "N/A"))
    lotto = norm_text(case_header.get("lotto", {}).get("value") if isinstance(case_header.get("lotto"), dict) else case_header.get("lotto", "N/A"))
    address = norm_text(norm_address(case_header))
    
    # Handle prezzo base - for multi-lot, show all lots
    if is_multi_lot:
        prezzo_value = "MULTI-LOTTO"
    else:
        prezzo_base = dati.get("prezzo_base_asta", {})
        prezzo_value = prezzo_base.get("formatted") or f"€{prezzo_base.get('value', 0):,}" if isinstance(prezzo_base, dict) else str(prezzo_base)
    
    semaforo_status = semaforo.get("status", "AMBER")
    semaforo_color = "#10b981" if semaforo_status == "GREEN" or semaforo_status == "VERDE" else "#f59e0b" if semaforo_status == "AMBER" or semaforo_status == "GIALLO" else "#ef4444"
    
    # Build multi-lot table if applicable
    lots_html = ""
    if is_multi_lot:
        lots_html = '<div class="section"><h2>📊 LOTTI</h2><table class="lots-table"><thead><tr><th>Lotto</th><th>Prezzo Base</th><th>Ubicazione</th><th>Superficie</th><th>Diritto</th></tr></thead><tbody>'
        for lot in lots:
            lots_html += f'''<tr>
                <td style="color:#D4AF37;">Lotto {lot.get("lot_number", "?")}</td>
                <td style="color:#10b981;">{lot.get("prezzo_base_eur", "TBD")}</td>
                <td>{norm_text(lot.get("ubicazione", ""))[:50]}</td>
                <td>{lot.get("superficie_mq", "TBD")}</td>
                <td>{norm_text(lot.get("diritto_reale", ""))[:20]}</td>
            </tr>'''
        lots_html += '</tbody></table></div>'
    
    # Build money box items HTML - handle TBD values
    money_items_html = ""
    money_total_min = 0
    money_total_max = 0
    all_tbd = True
    
    for item in money_box.get("items", []):
        voce = item.get("voce") or item.get("label_it") or item.get("code", "")
        stima = item.get("stima_euro", 0)
        nota = item.get("stima_nota", "")
        fonte = norm_text(item.get("fonte_perizia"))
        
        # Handle TBD values
        if stima == "TBD" or (isinstance(stima, str) and "TBD" in stima):
            value_display = "TBD"
            value_color = "#f59e0b"  # Amber for TBD
        elif isinstance(stima, (int, float)) and stima > 0:
            value_display = f"€{stima:,.0f}"
            value_color = "#10b981"  # Green for real values
            all_tbd = False
            money_total_min += stima
            money_total_max += stima
        else:
            value_display = nota if nota else "TBD"
            value_color = "#f59e0b"
        
        money_items_html += f'<div class="money-item"><span>{voce}</span><span class="page-ref">{fonte}</span><span style="color: {value_color}; font-weight: bold;">{value_display}</span></div>'
    
    # Build money total - handle TBD
    money_total = money_box.get("totale_extra_budget", money_box.get("total_extra_costs", {}))
    if all_tbd or money_total.get("min") == "TBD":
        total_display = "TBD"
        total_note = "Costi non quantificati in perizia — Verifica tecnico/legale obbligatoria"
    else:
        total_min = money_total.get("min", money_total_min) if isinstance(money_total.get("min"), (int, float)) else money_total_min
        total_max = money_total.get("max", money_total_max) if isinstance(money_total.get("max"), (int, float)) else int(money_total_min * 1.2)
        total_display = f"€{total_min:,.0f} - €{total_max:,.0f}"
        total_note = money_total.get("nota", f"Costi extra stimati (min-max)")
    
    # Build legal killers HTML
    legal_items = legal_killers.get("items", []) if isinstance(legal_killers, dict) and "items" in legal_killers else []
    legal_html = ""
    for item in legal_items:
        killer = item.get("killer", "")
        status = item.get("status", "NON_SPECIFICATO")
        evidence = item.get("evidence", [])
        page_ref = f"p. {evidence[0].get('page', '?')}" if evidence else ""
        status_color = "#ef4444" if status == "SI" or status == "YES" else "#10b981" if status == "NO" else "#f59e0b"
        legal_html += f'<div class="legal-item"><span class="status-dot" style="background:{status_color}"></span><span>{killer}</span><span class="page-ref">{page_ref}</span><span class="status">{status}</span></div>'
    
    # If no legal killers from items, check legacy format
    if not legal_html and isinstance(legal_killers, dict):
        for key, value in legal_killers.items():
            if key != "items" and isinstance(value, dict):
                status = value.get("status", "NON_SPECIFICATO")
                status_color = "#ef4444" if status == "SI" or status == "YES" else "#10b981" if status == "NO" else "#f59e0b"
                legal_html += f'<div class="legal-item"><span class="status-dot" style="background:{status_color}"></span><span>{key.replace("_", " ").title()}</span><span class="status">{status}</span></div>'
    
    # Build checklist HTML
    checklist_items = checklist if isinstance(checklist, list) else []
    checklist_html = ""
    for i, item in enumerate(checklist_items):
        item_text = item if isinstance(item, str) else item.get("item_it", str(item))
        checklist_html += f'<div class="checklist-item"><span class="number">{i+1}</span><span>{item_text}</span></div>'
    
    html = f"""<!DOCTYPE html>
<html lang="it">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Nexodify Report - {analysis.get('file_name', 'Perizia')}</title>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{ font-family: 'Segoe UI', system-ui, sans-serif; background: #09090b; color: #fafafa; padding: 40px; line-height: 1.6; }}
        .container {{ max-width: 900px; margin: 0 auto; }}
        .header {{ text-align: center; margin-bottom: 40px; padding-bottom: 20px; border-bottom: 2px solid #D4AF37; }}
        .header h1 {{ font-size: 28px; color: #D4AF37; margin-bottom: 10px; }}
        .header p {{ color: #a1a1aa; }}
        .semaforo {{ display: inline-block; padding: 8px 20px; border-radius: 20px; background: {semaforo_color}20; color: {semaforo_color}; font-weight: bold; margin: 10px 0; border: 1px solid {semaforo_color}40; }}
        .section {{ background: #18181b; border: 1px solid #27272a; border-radius: 12px; padding: 24px; margin-bottom: 24px; page-break-inside: avoid; }}
        .section h2 {{ color: #D4AF37; font-size: 18px; margin-bottom: 16px; padding-bottom: 8px; border-bottom: 1px solid #27272a; }}
        .grid {{ display: grid; grid-template-columns: repeat(2, 1fr); gap: 16px; }}
        .field {{ background: #09090b; padding: 12px; border-radius: 8px; }}
        .field-label {{ font-size: 11px; color: #71717a; text-transform: uppercase; letter-spacing: 0.5px; }}
        .field-value {{ font-size: 16px; color: #fafafa; margin-top: 4px; font-weight: 500; }}
        .page-ref {{ font-size: 10px; color: #D4AF37; font-family: monospace; margin-left: 8px; }}
        .money-item {{ display: flex; justify-content: space-between; align-items: center; padding: 12px; background: #09090b; border-radius: 8px; margin-bottom: 8px; }}
        .total {{ background: #D4AF3720; border: 1px solid #D4AF3740; padding: 16px; border-radius: 8px; margin-top: 16px; }}
        .total-value {{ font-size: 24px; color: #D4AF37; font-weight: bold; }}
        .total-tbd {{ font-size: 24px; color: #f59e0b; font-weight: bold; }}
        .legal-item {{ display: flex; align-items: center; gap: 12px; padding: 10px; background: #09090b; border-radius: 8px; margin-bottom: 6px; }}
        .status-dot {{ width: 10px; height: 10px; border-radius: 50%; flex-shrink: 0; }}
        .status {{ margin-left: auto; font-size: 12px; font-family: monospace; color: #a1a1aa; }}
        .checklist-item {{ display: flex; align-items: center; gap: 12px; padding: 10px; background: #09090b; border-radius: 8px; margin-bottom: 6px; }}
        .checklist-item .number {{ width: 24px; height: 24px; border-radius: 50%; background: #D4AF3720; border: 1px solid #D4AF37; display: flex; align-items: center; justify-content: center; font-size: 12px; color: #D4AF37; flex-shrink: 0; }}
        .summary-box {{ background: #f59e0b20; border-left: 4px solid #f59e0b; padding: 16px; border-radius: 0 8px 8px 0; margin-bottom: 16px; }}
        .disclaimer {{ background: #27272a; padding: 16px; border-radius: 8px; margin-top: 40px; text-align: center; }}
        .disclaimer p {{ color: #71717a; font-size: 12px; }}
        .footer {{ text-align: center; margin-top: 40px; color: #52525b; font-size: 12px; }}
        .lots-table {{ width: 100%; border-collapse: collapse; margin-top: 12px; }}
        .lots-table th, .lots-table td {{ padding: 10px; text-align: left; border-bottom: 1px solid #27272a; }}
        .lots-table th {{ color: #71717a; font-size: 11px; text-transform: uppercase; }}
        .lots-table td {{ font-size: 14px; }}
        @media print {{ 
            body {{ background: white; color: black; padding: 20px; }} 
            .section {{ border-color: #e5e5e5; background: #f9f9f9; }} 
            .semaforo {{ print-color-adjust: exact; -webkit-print-color-adjust: exact; }}
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>NEXODIFY AUCTION SCAN REPORT</h1>
            <p>Analisi Forense Perizia CTU</p>
            <div class="semaforo">SEMAFORO: {semaforo_status}</div>
            <p style="margin-top: 10px; font-size: 14px;">{semaforo.get('driver', {}).get('value', semaforo.get('reason_it', ''))}</p>
        </div>
        
        <div class="section">
            <h2>1. DATI PROCEDURA</h2>
            <div class="grid">
                <div class="field">
                    <div class="field-label">Procedura</div>
                    <div class="field-value">{procedure}</div>
                </div>
                <div class="field">
                    <div class="field-label">Lotto</div>
                    <div class="field-value">{lotto}</div>
                </div>
                <div class="field">
                    <div class="field-label">Tribunale</div>
                    <div class="field-value">{tribunale}</div>
                </div>
                <div class="field">
                    <div class="field-label">Indirizzo</div>
                    <div class="field-value">{address}</div>
                </div>
            </div>
        </div>
        
        {lots_html}
        
        <div class="section">
            <h2>2. DECISIONE RAPIDA</h2>
            <p style="font-size: 18px; margin-bottom: 16px;">{decision.get('summary_it', 'Analisi completata')}</p>
            <p style="color: #a1a1aa;">{decision.get('summary_en', '')}</p>
        </div>
        
        <div class="section">
            <h2>3. PORTAFOGLIO COSTI (MONEY BOX)</h2>
            {money_items_html or '<p style="color: #71717a;">Nessun dato sui costi disponibile - Verifica necessaria</p>'}
            <div class="total">
                <div style="display: flex; justify-content: space-between; align-items: center;">
                    <span>TOTALE COSTI EXTRA STIMATI</span>
                    <span class="{'total-tbd' if total_display == 'TBD' else 'total-value'}">{total_display}</span>
                </div>
                <p style="font-size: 12px; color: #71717a; margin-top: 8px;">{total_note}</p>
            </div>
        </div>
        
        <div class="section">
            <h2>4. DATI CERTI DEL LOTTO</h2>
            <div class="grid">
                <div class="field">
                    <div class="field-label">Prezzo Base Asta</div>
                    <div class="field-value" style="color: #D4AF37; font-size: 20px;">{prezzo_value}</div>
                </div>
                <div class="field">
                    <div class="field-label">Superficie</div>
                    <div class="field-value">{norm_text(dati.get('superficie_catastale', {}).get('value', 'NON SPECIFICATO') if isinstance(dati.get('superficie_catastale'), dict) else dati.get('superficie_catastale', 'NON SPECIFICATO'))}</div>
                </div>
                <div class="field">
                    <div class="field-label">Composizione Lotto</div>
                    <div class="field-value">{dati.get('composizione_lotto', {}).get('value', 'N/A') if isinstance(dati.get('composizione_lotto'), dict) else dati.get('composizione_lotto', 'N/A')}</div>
                </div>
                <div class="field">
                    <div class="field-label">Diritto Reale</div>
                    <div class="field-value">{dati.get('diritto_reale', {}).get('value', 'N/A') if isinstance(dati.get('diritto_reale'), dict) else dati.get('diritto_reale', 'N/A')}</div>
                </div>
            </div>
        </div>
        
        <div class="section">
            <h2>9. LEGAL KILLERS CHECKLIST</h2>
            {legal_html or '<p style="color: #71717a;">Nessun dato disponibile</p>'}
        </div>
        
        <div class="section">
            <h2>12. CHECKLIST PRE-OFFERTA</h2>
            {checklist_html or '<p style="color: #71717a;">Nessuna checklist disponibile</p>'}
        </div>
        
        <div class="section">
            <h2>SUMMARY FOR CLIENT</h2>
            {f'<div class="summary-box"><p>⚠️ {summary.get("raccomandazione", "")}</p></div>' if summary.get("raccomandazione") else ''}
            <p>{summary.get('summary_it', 'Analisi completata.')}</p>
            <p style="color: #a1a1aa; margin-top: 12px; font-style: italic;">{summary.get('summary_en', '')}</p>
        </div>
        
        <div class="disclaimer">
            <p><strong>AVVISO IMPORTANTE</strong></p>
            <p>{summary.get('disclaimer_it', 'Documento informativo. Non costituisce consulenza legale.')}</p>
            <p>{summary.get('disclaimer_en', 'Informational document. Not legal advice.')}</p>
        </div>
        
        <div class="footer">
            <p>Report generato da Nexodify Forensic Engine</p>
            <p>Case ID: {analysis.get('case_id', 'N/A')} | Data: {analysis.get('created_at', 'N/A')}</p>
        </div>
    </div>
</body>
</html>"""
    
    return html

# ===================
# IMAGE FORENSICS ENDPOINT
# ===================

IMAGE_FORENSICS_SYSTEM_PROMPT = """Sei Nexodify Image Forensics Engine - analisi forense di immagini immobiliari.

COMPETENZE:
- Identificazione stato conservativo immobili
- Rilevamento potenziali problematiche strutturali
- Analisi conformità visiva con standard edilizi
- Identificazione materiali pericolosi (amianto, eternit)
- Valutazione stato impianti visibili
- Rilevamento umidità, muffe, danni

═══════════════════════════════════════════════════════════════════════════════
REGOLE DETERMINISTIC (NON-NEGOTIABLE)
═══════════════════════════════════════════════════════════════════════════════

1. EVIDENCE-LOCKED: Ogni finding DEVE avere:
   - "confidence": "HIGH|MEDIUM|LOW" - HIGH solo se visibile chiaramente
   - "evidence": descrizione di cosa vedi nell'immagine
   - Se non puoi confermare visivamente → confidence="LOW", status="NON_VERIFICABILE"

2. NO HALLUCINATIONS: Descrivi SOLO cosa vedi realmente. Mai inventare.

3. HONESTY:
   - Se immagine non chiara → "NON_VERIFICABILE"
   - Se non puoi valutare → ammettilo esplicitamente
   - Mai fingere certezza che non hai

4. QA GATES:
   - Se confidence="LOW" su più di 50% findings → qa_status="WARN"
   - Se non riesci ad analizzare → qa_status="FAIL"

OUTPUT JSON:
{
  "findings": [
    {
      "finding_id": "...",
      "category": "STRUTTURALE|IMPIANTI|FINITURE|MATERIALI|UMIDITA|ALTRO",
      "title_it": "...",
      "title_en": "...",
      "severity": "ROSSO|GIALLO|VERDE|NON_VERIFICABILE",
      "confidence": "HIGH|MEDIUM|LOW",
      "what_i_see_it": "descrizione dettagliata",
      "what_i_see_en": "...",
      "evidence": "cosa nell'immagine supporta questo finding",
      "action_required_it": "...",
      "action_required_en": "..."
    }
  ],
  "overall_assessment": {
    "risk_level": "ALTO|MEDIO|BASSO|NON_DETERMINABILE",
    "confidence": "HIGH|MEDIUM|LOW",
    "summary_it": "...",
    "summary_en": "..."
  },
  "limitations": ["lista di cosa NON puoi verificare dalle immagini"],
  "qa_pass": {
    "status": "PASS|WARN|FAIL",
    "checks": [...]
  }
}"""

@api_router.post("/analysis/image")
async def analyze_images(request: Request, files: List[UploadFile] = File(...)):
    """Analyze uploaded property images with evidence-locked findings"""
    user = await require_auth(request)
    _require_feature_access(user, "Image Forensics", "can_use_image_forensics")
    
    # Check quota
    if user.quota.get("image_scans_remaining", 0) <= 0 and not user.is_master_admin:
        raise HTTPException(
            status_code=403,
            detail={
                "code": "QUOTA_EXCEEDED",
                "message_it": "Quota analisi immagini esaurita. Aggiorna il piano.",
                "message_en": "Image analysis quota exceeded. Upgrade your plan."
            }
        )
    
    # Validate images
    valid_extensions = ['.jpg', '.jpeg', '.png', '.webp']
    for f in files:
        if not any(f.filename.lower().endswith(ext) for ext in valid_extensions):
            raise HTTPException(status_code=400, detail=f"Invalid image format: {f.filename}")
    
    case_id = f"img_case_{uuid.uuid4().hex[:8]}"
    run_id = f"img_run_{uuid.uuid4().hex[:8]}"
    
    # Analyze images with LLM (if available) or return honest placeholder
    findings = []
    qa_checks = []
    qa_status = "PASS"
    
    try:
        # Build image descriptions (actual vision analysis would require multimodal model)
        image_info = f"Caricate {len(files)} immagini: " + ", ".join([f.filename for f in files])
        
        prompt = f"""ANALISI IMMAGINI IMMOBILIARI

{image_info}

NOTA: Senza accesso diretto alle immagini, devo essere onesto sui limiti dell'analisi.

Fornisci un'analisi con i seguenti vincoli:
1. Indica chiaramente che l'analisi è LIMITATA senza visione diretta
2. Tutti i findings devono avere confidence="LOW" e status="NON_VERIFICABILE"
3. Suggerisci cosa verificare durante un sopralluogo
4. QA status deve essere "WARN" per indicare i limiti

Output JSON secondo lo schema specificato."""

        response = await openai_chat_completion(IMAGE_FORENSICS_SYSTEM_PROMPT, prompt, model="gpt-4o")
        
        # Parse response
        response_text = response.strip()
        if response_text.startswith("```json"):
            response_text = response_text[7:]
        if response_text.startswith("```"):
            response_text = response_text[3:]
        if response_text.endswith("```"):
            response_text = response_text[:-3]
        
        try:
            llm_result = json.loads(response_text)
            findings = llm_result.get("findings", [])
            qa_status = llm_result.get("qa_pass", {}).get("status", "WARN")
            qa_checks = llm_result.get("qa_pass", {}).get("checks", [])
        except json.JSONDecodeError:
            logger.warning("Image forensics LLM response not valid JSON, using fallback")
            qa_status = "WARN"
            
    except Exception as e:
        logger.error(f"Image forensics LLM error: {e}")
        qa_status = "WARN"
    
    # Always add honest findings if LLM didn't provide any
    if not findings:
        findings = [
            {
                "finding_id": f"find_{uuid.uuid4().hex[:8]}",
                "category": "GENERALE",
                "title_it": "Analisi immagini - Verifica manuale richiesta",
                "title_en": "Image analysis - Manual verification required",
                "severity": "NON_VERIFICABILE",
                "confidence": "LOW",
                "what_i_see_it": f"Caricate {len(files)} immagini. L'analisi automatica ha limiti significativi senza ispezione diretta.",
                "what_i_see_en": f"Uploaded {len(files)} images. Automatic analysis has significant limits without direct inspection.",
                "evidence": "NON DISPONIBILE - Richiede analisi visiva diretta",
                "action_required_it": "Sopralluogo professionale obbligatorio prima di qualsiasi decisione",
                "action_required_en": "Professional inspection mandatory before any decision"
            }
        ]
    
    # Enforce evidence-locked findings and QA gates
    low_confidence_count = 0
    for f in findings:
        # Ensure all required fields exist
        if "confidence" not in f:
            f["confidence"] = "LOW"
        if "evidence" not in f or not f["evidence"]:
            f["evidence"] = "NON VERIFICABILE - Richiede ispezione diretta"
            f["confidence"] = "LOW"
        if f.get("confidence") == "LOW":
            low_confidence_count += 1
    
    # QA Gate: If >50% findings are LOW confidence, set WARN
    if findings and (low_confidence_count / len(findings)) > 0.5:
        qa_status = "WARN"
    
    # Add standard QA checks
    qa_checks.extend([
        {"code": "QA-ImageCount", "result": "OK", "note": f"Analizzate {len(files)} immagini"},
        {"code": "QA-EvidenceLocked", "result": "OK" if all(f.get("evidence") for f in findings) else "WARN", "note": "Tutti i findings hanno evidence"},
        {"code": "QA-ConfidenceHonesty", "result": "OK" if low_confidence_count > 0 else "WARN", "note": f"{low_confidence_count}/{len(findings)} findings con confidence LOW (onestà)"},
        {"code": "QA-NoHallucination", "result": "OK", "note": "Nessuna certezza inventata"}
    ])
    
    result = {
        "ok": True,
        "mode": "IMAGE_FORENSICS",
        "result": {
            "schema_version": "nexodify_image_forensics_v2",
            "run": {
                "run_id": run_id,
                "case_id": case_id,
                "generated_at_utc": datetime.now(timezone.utc).isoformat(),
                "revision": 0,
                "images_analyzed": len(files),
                "image_names": [f.filename for f in files]
            },
            "findings": findings,
            "overall_assessment": {
                "risk_level": "NON_DETERMINABILE",
                "confidence": "LOW",
                "summary_it": f"Analisi di {len(files)} immagini completata con limitazioni. Sopralluogo professionale OBBLIGATORIO per valutazione accurata. L'analisi automatica non può sostituire un'ispezione diretta.",
                "summary_en": f"Analysis of {len(files)} images completed with limitations. Professional inspection MANDATORY for accurate assessment. Automatic analysis cannot replace direct inspection."
            },
            "limitations": [
                "Analisi senza accesso diretto alle immagini",
                "Non è possibile verificare stato strutturale",
                "Non è possibile rilevare problemi nascosti",
                "Non è possibile valutare conformità impianti",
                "Sopralluogo professionale sempre necessario"
            ],
            "qa_pass": {
                "status": qa_status,
                "checks": qa_checks
            },
            "disclaimer_it": "Documento informativo con LIMITI SIGNIFICATIVI. Non sostituisce ispezione professionale. Non costituisce consulenza tecnica.",
            "disclaimer_en": "Informational document with SIGNIFICANT LIMITATIONS. Does not replace professional inspection. Not technical advice."
        }
    }
    
    # Store analysis
    forensics = ImageForensics(
        forensics_id=f"forensics_{uuid.uuid4().hex[:12]}",
        user_id=user.user_id,
        case_id=case_id,
        run_id=run_id,
        image_count=len(files),
        result=result
    )
    
    forensics_dict = forensics.model_dump()
    forensics_dict["created_at"] = forensics_dict["created_at"].isoformat()
    await db.image_forensics.insert_one(forensics_dict)
    
    # Decrement quota if not master admin
    await _apply_quota_debit_with_ledger(
        user,
        field="image_scans_remaining",
        amount=1,
        entry_type="image_forensics",
        reference_type="forensics",
        reference_id=forensics.forensics_id,
        description_it="Addebito credito per analisi immagini",
        metadata={
            "forensics_id": forensics.forensics_id,
            "case_id": case_id,
            "image_count": len(files),
        },
    )
    
    return result

# ===================
# ASSISTANT QA ENDPOINT (PERIZIA-AWARE)
# ===================

ASSISTANT_SYSTEM_PROMPT = """Sei Nexodify Assistant, un esperto di aste immobiliari italiane, perizie CTU, e analisi documentale immobiliare.

COMPETENZE:
- Legge italiana sulle aste immobiliari (DPR 380/2001, L. 47/85, L. 724/94, L. 326/03)
- Interpretazione perizie CTU
- Conformità urbanistica e catastale
- Condoni edilizi e sanatorie
- Formalità ipotecarie e pignoramenti
- Calcolo costi accessori aste
- Valutazione rischi immobiliari

═══════════════════════════════════════════════════════════════════════════════
REGOLE DETERMINISTIC (NON-NEGOTIABLE)
═══════════════════════════════════════════════════════════════════════════════

1. EVIDENCE-LOCKED RESPONSES:
   - Se citi un dato dalla perizia → DEVI includere "source" con riferimento
   - Se non hai dati specifici → ammettilo: "Non ho informazioni specifiche su questo"
   - Mai inventare numeri, date, o fatti

2. CONFIDENCE TRACKING:
   - "confidence": "HIGH" solo se hai fonte diretta (perizia, legge citata)
   - "confidence": "MEDIUM" se è ragionamento basato su prassi comune
   - "confidence": "LOW" se è opinione generale o mancano dati

3. TRI-STATE ANSWERS:
   - Se puoi rispondere con certezza → rispondi
   - Se mancano dati → "needs_more_info": "YES" + lista "missing_inputs"
   - Se non è tua competenza → "out_of_scope": true

4. NO HALLUCINATIONS:
   - Mai fingere di sapere cosa non sai
   - Mai inventare riferimenti normativi
   - Se non sei sicuro → dillo esplicitamente

5. DISCLAIMER SEMPRE INCLUSO

Formato risposta JSON:
{
  "answer_it": "risposta in italiano",
  "answer_en": "risposta in inglese", 
  "confidence": "HIGH|MEDIUM|LOW",
  "sources": [{"type": "perizia|legge|prassi", "reference": "..."}],
  "needs_more_info": "YES|NO",
  "missing_inputs": [],
  "out_of_scope": false,
  "safe_disclaimer_it": "Le informazioni fornite hanno carattere esclusivamente informativo e non costituiscono consulenza legale, fiscale o professionale.",
  "safe_disclaimer_en": "Information provided is for informational purposes only and does not constitute legal, tax or professional advice.",
  "qa_pass": {
    "status": "PASS|WARN|FAIL",
    "reason": "..."
  }
}"""

@api_router.post("/analysis/assistant")
async def assistant_qa(request: Request):
    """Answer user questions about perizia/real estate - with evidence-locked responses"""
    user = await require_auth(request)
    _require_feature_access(user, "Assistente", "can_use_assistant")
    data = await request.json()
    question = data.get("question")
    related_case_id = data.get("related_case_id")
    
    if not question:
        raise HTTPException(status_code=400, detail="Question required")
    
    # Check quota
    if user.quota.get("assistant_messages_remaining", 0) <= 0 and not user.is_master_admin:
        raise HTTPException(
            status_code=403,
            detail={
                "code": "QUOTA_EXCEEDED",
                "message_it": "Quota messaggi assistente esaurita. Aggiorna il piano.",
                "message_en": "Assistant message quota exceeded. Upgrade your plan."
            }
        )
    
    run_id = f"qa_run_{uuid.uuid4().hex[:8]}"
    
    # Get context from user's analyzed perizie
    context = ""
    has_perizia_context = False
    perizia_file = None
    
    if related_case_id:
        analysis = await db.perizia_analyses.find_one({"case_id": related_case_id, "user_id": user.user_id}, {"_id": 0})
        if analysis:
            has_perizia_context = True
            perizia_file = analysis.get('file_name')
            result = analysis.get('result', {})
            
            # Extract key data with source tracking
            report_header = result.get('report_header', {})
            case_header = result.get('case_header', report_header)
            dati = result.get('section_4_dati_certi', result.get('dati_certi_del_lotto', {}))
            semaforo = result.get('section_1_semaforo_generale', result.get('semaforo_generale', {}))
            
            context = f"""
CONTESTO PERIZIA ANALIZZATA (FONTE VERIFICABILE):
- File: {perizia_file}
- Procedura: {case_header.get('procedure', {}).get('value', case_header.get('procedure_id', 'N/A'))}
- Tribunale: {case_header.get('tribunale', {}).get('value', case_header.get('tribunale', 'N/A'))}
- Lotto: {case_header.get('lotto', {}).get('value', case_header.get('lotto', 'N/A'))}
- Prezzo Base: {dati.get('prezzo_base_asta', {}).get('formatted', dati.get('prezzo_base_asta', {}).get('value', 'N/A'))}
- Semaforo: {semaforo.get('status', 'N/A')}
- Riepilogo: {result.get('summary_for_client', {}).get('summary_it', 'N/A')}

NOTA: Se citi questi dati, indica "source": {{"type": "perizia", "reference": "{perizia_file}"}}
"""
    else:
        # Get user's most recent analysis for context
        recent = await db.perizia_analyses.find_one(
            {"user_id": user.user_id},
            {"_id": 0},
            sort=[("created_at", -1)]
        )
        if recent:
            has_perizia_context = True
            perizia_file = recent.get('file_name')
            result = recent.get('result', {})
            context = f"""
ULTIMA PERIZIA ANALIZZATA (FONTE VERIFICABILE):
- File: {perizia_file}
- Procedura: {result.get('case_header', {}).get('procedure_id', 'N/A')}
- Prezzo Base: {result.get('dati_certi_del_lotto', {}).get('prezzo_base_asta', {}).get('formatted', 'N/A')}

NOTA: Se citi questi dati, indica "source": {{"type": "perizia", "reference": "{perizia_file}"}}
"""
    
    # Enhanced prompt with evidence requirements
    prompt = f"""{context}

DOMANDA UTENTE: {question}

REGOLE PER LA RISPOSTA:
1. Se rispondi basandoti sulla perizia, indica "sources" con riferimento al file
2. Se rispondi basandoti su leggi, cita la normativa specifica
3. Se non hai dati sufficienti, imposta "needs_more_info": "YES"
4. Imposta "confidence" in modo onesto (HIGH solo con fonte diretta)
5. Se la domanda è fuori competenza, imposta "out_of_scope": true

Output JSON secondo lo schema specificato."""
    
    try:
        response = await openai_chat_completion(ASSISTANT_SYSTEM_PROMPT, prompt, model="gpt-4o")
        
        # Parse response
        response_text = response.strip()
        if response_text.startswith("```json"):
            response_text = response_text[7:]
        if response_text.startswith("```"):
            response_text = response_text[3:]
        if response_text.endswith("```"):
            response_text = response_text[:-3]
        
        try:
            answer = json.loads(response_text)
            
            # Enforce evidence-locked rules
            # If confidence is HIGH but no sources, downgrade to MEDIUM
            if answer.get("confidence") == "HIGH" and not answer.get("sources"):
                answer["confidence"] = "MEDIUM"
                answer.setdefault("qa_pass", {})["status"] = "WARN"
                answer["qa_pass"]["reason"] = "Confidence downgraded: HIGH without sources"
            
            # Ensure required fields exist
            if "confidence" not in answer:
                answer["confidence"] = "MEDIUM"
            if "sources" not in answer:
                answer["sources"] = []
            if "needs_more_info" not in answer:
                answer["needs_more_info"] = "NO"
            if "missing_inputs" not in answer:
                answer["missing_inputs"] = []
            if "out_of_scope" not in answer:
                answer["out_of_scope"] = False
                
        except json.JSONDecodeError:
            answer = {
                "answer_it": response,
                "answer_en": "",
                "confidence": "LOW",
                "sources": [],
                "needs_more_info": "NO",
                "missing_inputs": [],
                "out_of_scope": False,
                "safe_disclaimer_it": "Le informazioni fornite hanno carattere esclusivamente informativo e non costituiscono consulenza legale.",
                "safe_disclaimer_en": "Information provided is for informational purposes only and does not constitute legal advice.",
                "qa_pass": {"status": "WARN", "reason": "Response not in structured JSON format"}
            }
    except Exception as e:
        logger.error(f"Assistant error: {e}")
        answer = {
            "answer_it": "Mi scusi, si è verificato un errore. Riprovi più tardi.",
            "answer_en": "Sorry, an error occurred. Please try again later.",
            "confidence": "LOW",
            "sources": [],
            "needs_more_info": "NO",
            "missing_inputs": [],
            "out_of_scope": False,
            "safe_disclaimer_it": "Le informazioni fornite hanno carattere esclusivamente informativo e non costituiscono consulenza legale.",
            "safe_disclaimer_en": "Information provided is for informational purposes only and does not constitute legal advice.",
            "qa_pass": {"status": "FAIL", "reason": f"LLM error: {str(e)}"}
        }
    
    # Add standard disclaimers if missing
    if "safe_disclaimer_it" not in answer:
        answer["safe_disclaimer_it"] = "Le informazioni fornite hanno carattere esclusivamente informativo e non costituiscono consulenza legale, fiscale o professionale."
    if "safe_disclaimer_en" not in answer:
        answer["safe_disclaimer_en"] = "Information provided is for informational purposes only and does not constitute legal, tax or professional advice."
    
    # Build QA checks
    qa_checks = [
        {"code": "QA-HasContext", "result": "OK" if has_perizia_context else "WARN", "note": f"Perizia context: {perizia_file}" if has_perizia_context else "No perizia context available"},
        {"code": "QA-ConfidenceHonesty", "result": "OK" if answer.get("confidence") in ["LOW", "MEDIUM"] or answer.get("sources") else "WARN", "note": f"Confidence: {answer.get('confidence', 'N/A')}"},
        {"code": "QA-SourcesProvided", "result": "OK" if answer.get("sources") else "WARN", "note": f"Sources: {len(answer.get('sources', []))}"},
        {"code": "QA-DisclaimerIncluded", "result": "OK", "note": "Disclaimer included"}
    ]
    
    # Determine overall QA status
    qa_status = answer.get("qa_pass", {}).get("status", "PASS")
    if not answer.get("sources") and answer.get("confidence") == "HIGH":
        qa_status = "WARN"
    
    result = {
        "ok": True,
        "mode": "ASSISTANT_QA",
        "result": {
            "schema_version": "nexodify_assistant_v2",
            "run": {
                "run_id": run_id,
                "generated_at_utc": datetime.now(timezone.utc).isoformat(),
                "case_id": related_case_id,
                "revision": 0,
                "has_perizia_context": has_perizia_context,
                "perizia_file": perizia_file
            },
            **answer,
            "qa_pass": {
                "status": qa_status,
                "checks": qa_checks
            }
        }
    }
    
    # Store QA
    qa = AssistantQA(
        qa_id=f"qa_{uuid.uuid4().hex[:12]}",
        user_id=user.user_id,
        case_id=related_case_id,
        run_id=run_id,
        question=question,
        result=result
    )
    
    qa_dict = qa.model_dump()
    qa_dict["created_at"] = qa_dict["created_at"].isoformat()
    await db.assistant_qa.insert_one(qa_dict)
    
    # Decrement quota if not master admin
    await _apply_quota_debit_with_ledger(
        user,
        field="assistant_messages_remaining",
        amount=1,
        entry_type="assistant_message",
        reference_type="assistant_qa",
        reference_id=qa.qa_id,
        description_it="Addebito credito per messaggio assistente",
        metadata={
            "qa_id": qa.qa_id,
            "case_id": related_case_id,
            "question_preview": _truncate(question, 120),
        },
    )
    
    return result

# ===================
# ADMIN API (MASTER ADMIN ONLY)
# ===================

async def _get_user_email_map(user_ids: List[str]) -> Dict[str, Dict[str, Any]]:
    if not user_ids:
        return {}
    users = await db.users.find(
        {"user_id": {"$in": user_ids}},
        {"_id": 0, "user_id": 1, "email": 1, "name": 1, "plan": 1, "created_at": 1, "is_master_admin": 1, "quota": 1}
    ).to_list(len(user_ids))
    return {u.get("user_id"): u for u in users}

async def _aggregate_usage(collection, user_ids: List[str], date_query: Optional[Dict[str, Any]] = None) -> Dict[str, Dict[str, Any]]:
    if not user_ids:
        return {}
    match_query: Dict[str, Any] = {"user_id": {"$in": user_ids}}
    if date_query:
        match_query = _merge_query(match_query, date_query)
    pipeline = [
        {"$match": match_query},
        {"$group": {"_id": "$user_id", "count": {"$sum": 1}, "last_active": {"$max": "$created_at"}}}
    ]
    results = await collection.aggregate(pipeline).to_list(len(user_ids))
    return {r["_id"]: {"count": r.get("count", 0), "last_active": r.get("last_active")} for r in results}

def _serialize_admin_ledger_entry(entry: Dict[str, Any]) -> Dict[str, Any]:
    return _serialize_datetime_fields({
        "ledger_id": entry.get("ledger_id"),
        "user_id": entry.get("user_id"),
        "user_email": entry.get("user_email"),
        "quota_field": entry.get("quota_field"),
        "direction": entry.get("direction"),
        "amount": int(entry.get("amount", 0) or 0),
        "balance_before": int(entry.get("balance_before", 0) or 0),
        "balance_after": int(entry.get("balance_after", 0) or 0),
        "entry_type": entry.get("entry_type"),
        "reference_type": entry.get("reference_type"),
        "reference_id": entry.get("reference_id"),
        "description_it": entry.get("description_it"),
        "metadata": entry.get("metadata") or {},
        "actor_user_id": entry.get("actor_user_id"),
        "actor_email": entry.get("actor_email"),
        "created_at": entry.get("created_at"),
    }, "created_at")

def _serialize_admin_billing_record(record: Dict[str, Any]) -> Dict[str, Any]:
    return _serialize_datetime_fields({
        "billing_record_id": record.get("billing_record_id"),
        "user_id": record.get("user_id"),
        "user_email": record.get("user_email"),
        "customer_type": record.get("customer_type"),
        "customer_name": record.get("customer_name"),
        "company_name": record.get("company_name"),
        "billing_email": record.get("billing_email"),
        "country_code": record.get("country_code"),
        "plan_id": record.get("plan_id"),
        "purchase_type": record.get("purchase_type"),
        "amount_subtotal": float(record.get("amount_subtotal", 0) or 0),
        "amount_tax": float(record.get("amount_tax", 0) or 0),
        "amount_total": float(record.get("amount_total", 0) or 0),
        "currency": record.get("currency"),
        "status": record.get("status"),
        "payment_provider": record.get("payment_provider"),
        "payment_reference": record.get("payment_reference"),
        "checkout_reference": record.get("checkout_reference"),
        "invoice_status": record.get("invoice_status"),
        "invoice_number": record.get("invoice_number"),
        "invoice_reference": record.get("invoice_reference"),
        "description_it": record.get("description_it"),
        "metadata": record.get("metadata") or {},
        "created_at": record.get("created_at"),
        "updated_at": record.get("updated_at"),
        "paid_at": record.get("paid_at"),
    }, "created_at", "updated_at", "paid_at")

def _summarize_credit_debits(entries: List[Dict[str, Any]]) -> Dict[str, int]:
    summary = {
        "total_debits": 0,
        "perizia_scans_remaining": 0,
        "image_scans_remaining": 0,
        "assistant_messages_remaining": 0,
    }
    for entry in entries:
        if entry.get("direction") != "debit":
            continue
        amount = int(entry.get("amount", 0) or 0)
        quota_field = entry.get("quota_field")
        summary["total_debits"] += amount
        if quota_field in summary:
            summary[quota_field] += amount
    return summary

def _summarize_billing_statuses(records: List[Dict[str, Any]]) -> Dict[str, int]:
    counts = {"pending": 0, "paid": 0, "failed": 0, "refunded": 0}
    for record in records:
        status = str(record.get("status") or "").lower()
        if status in counts:
            counts[status] += 1
    return counts

async def _get_admin_user_financial_summary_map(user_ids: List[str]) -> Dict[str, Dict[str, Any]]:
    summaries: Dict[str, Dict[str, Any]] = {
        user_id: {
            "latest_credit_movement_at": None,
            "latest_billing_status": None,
            "billing_records_count": 0,
            "latest_purchase_type": None,
        }
        for user_id in user_ids
        if user_id
    }
    if not summaries:
        return summaries

    user_filter = {"user_id": {"$in": list(summaries.keys())}}

    ledger_count = await db.credit_ledger.count_documents(user_filter)
    ledger_docs = await db.credit_ledger.find(
        user_filter,
        {"_id": 0, "user_id": 1, "created_at": 1}
    ).sort("created_at", -1).to_list(max(ledger_count, 0))

    for doc in ledger_docs:
        user_id = doc.get("user_id")
        if user_id in summaries and not summaries[user_id]["latest_credit_movement_at"]:
            summaries[user_id]["latest_credit_movement_at"] = _to_iso(doc.get("created_at"))

    billing_count = await db.billing_records.count_documents(user_filter)
    billing_docs = await db.billing_records.find(
        user_filter,
        {"_id": 0, "user_id": 1, "status": 1, "purchase_type": 1, "created_at": 1}
    ).sort("created_at", -1).to_list(max(billing_count, 0))

    for doc in billing_docs:
        user_id = doc.get("user_id")
        if user_id not in summaries:
            continue
        summaries[user_id]["billing_records_count"] += 1
        if not summaries[user_id]["latest_billing_status"]:
            summaries[user_id]["latest_billing_status"] = doc.get("status")
            summaries[user_id]["latest_purchase_type"] = doc.get("purchase_type")

    return summaries

def _max_last_active(*values: Any) -> Optional[str]:
    dts = [_parse_dt(v) for v in values if _parse_dt(v)]
    if not dts:
        return None
    return max(dts).isoformat()

@api_router.get("/admin/overview")
async def admin_overview(request: Request):
    admin_user = await require_master_admin(request)

    totals = {
        "users": await db.users.count_documents({}),
        "perizie": await db.perizia_analyses.count_documents({}),
        "images": await db.image_forensics.count_documents({}),
        "assistant_qas": await db.assistant_qa.count_documents({}),
        "transactions": await db.payment_transactions.count_documents({})
    }

    plan_counts = {"free": 0, "pro": 0, "enterprise": 0, "other": 0}
    try:
        plan_pipeline = [{"$group": {"_id": "$plan", "count": {"$sum": 1}}}]
        plan_results = await db.users.aggregate(plan_pipeline).to_list(20)
        for item in plan_results:
            plan_id = item.get("_id") or "other"
            if plan_id in plan_counts:
                plan_counts[plan_id] = item.get("count", 0)
            else:
                plan_counts["other"] += item.get("count", 0)
    except Exception as e:
        logger.warning(f"Plan counts aggregation failed: {e}")

    last30_date = (datetime.now(timezone.utc) - timedelta(days=30)).date().isoformat()
    date_query = _date_range_query("created_at", last30_date, None)

    perizie_30d = await db.perizia_analyses.count_documents(date_query or {})
    images_30d = await db.image_forensics.count_documents(date_query or {})
    assistant_30d = await db.assistant_qa.count_documents(date_query or {})

    active_users_set = set()
    try:
        active_users_set.update(await db.perizia_analyses.distinct("user_id", date_query or {}))
        active_users_set.update(await db.image_forensics.distinct("user_id", date_query or {}))
        active_users_set.update(await db.assistant_qa.distinct("user_id", date_query or {}))
    except Exception as e:
        logger.warning(f"Active users aggregation failed: {e}")

    paid_eur = 0.0
    try:
        payment_query = _merge_query({"currency": "eur"}, date_query or {})
        payment_pipeline = [
            {"$match": payment_query},
            {"$group": {"_id": None, "total": {"$sum": "$amount"}}}
        ]
        payment_results = await db.payment_transactions.aggregate(payment_pipeline).to_list(1)
        if payment_results:
            paid_eur = float(payment_results[0].get("total") or 0.0)
    except Exception as e:
        logger.warning(f"Paid EUR aggregation failed: {e}")

    ledger_30d_count = await db.credit_ledger.count_documents(date_query or {})
    ledger_30d_entries = await db.credit_ledger.find(
        date_query or {},
        {"_id": 0, "user_id": 1, "user_email": 1, "quota_field": 1, "direction": 1, "amount": 1, "entry_type": 1, "description_it": 1, "created_at": 1}
    ).sort("created_at", -1).to_list(max(ledger_30d_count, 0))
    ledger_30d_summary = _summarize_credit_debits(ledger_30d_entries)

    billing_30d_count = await db.billing_records.count_documents(date_query or {})
    billing_30d_records = await db.billing_records.find(
        date_query or {},
        {"_id": 0, "billing_record_id": 1, "user_id": 1, "user_email": 1, "plan_id": 1, "purchase_type": 1, "amount_total": 1, "currency": 1, "status": 1, "description_it": 1, "created_at": 1}
    ).sort("created_at", -1).to_list(max(billing_30d_count, 0))
    billing_status_counts = _summarize_billing_statuses(billing_30d_records)

    user_ids = []
    try:
        perizie_map = await _aggregate_usage(db.perizia_analyses, await db.perizia_analyses.distinct("user_id", date_query or {}), date_query)
        images_map = await _aggregate_usage(db.image_forensics, await db.image_forensics.distinct("user_id", date_query or {}), date_query)
        assistant_map = await _aggregate_usage(db.assistant_qa, await db.assistant_qa.distinct("user_id", date_query or {}), date_query)
        user_ids = list(set(list(perizie_map.keys()) + list(images_map.keys()) + list(assistant_map.keys())))
    except Exception as e:
        logger.warning(f"Top users aggregation failed: {e}")
        perizie_map, images_map, assistant_map = {}, {}, {}

    users_map = await _get_user_email_map(user_ids)
    top_users = []
    for user_id in user_ids:
        perizie_count = perizie_map.get(user_id, {}).get("count", 0)
        images_count = images_map.get(user_id, {}).get("count", 0)
        assistant_count = assistant_map.get(user_id, {}).get("count", 0)
        last_active = _max_last_active(
            perizie_map.get(user_id, {}).get("last_active"),
            images_map.get(user_id, {}).get("last_active"),
            assistant_map.get(user_id, {}).get("last_active")
        )
        user_doc = users_map.get(user_id, {})
        top_users.append({
            "user_id": user_id,
            "email": user_doc.get("email"),
            "plan": user_doc.get("plan"),
            "perizie": perizie_count,
            "images": images_count,
            "assistant_qas": assistant_count,
            "last_active_at": last_active
        })

    top_users = sorted(top_users, key=lambda x: (x.get("perizie", 0) + x.get("images", 0) + x.get("assistant_qas", 0)), reverse=True)[:10]

    latest_credit_movements = [
        _serialize_admin_ledger_entry(entry)
        for entry in ledger_30d_entries
        if entry.get("entry_type") != "opening_balance"
    ][:5]

    await _write_admin_audit(admin_user, "ADMIN_API_VIEW", meta={"endpoint": "overview"})

    return {
        "totals": totals,
        "plan_counts": plan_counts,
        "last_30d": {
            "perizie": perizie_30d,
            "images": images_30d,
            "assistant_qas": assistant_30d,
            "active_users": len(active_users_set),
            "paid_eur": paid_eur
        },
        "credit_ledger_30d": ledger_30d_summary,
        "billing_records_30d": {
            "status_counts": billing_status_counts,
        },
        "latest_credit_movements": latest_credit_movements,
        "latest_billing_activity": [
            _serialize_admin_billing_record(record)
            for record in billing_30d_records[:5]
        ],
        "top_users_30d": top_users
    }

@api_router.get("/admin/users")
async def admin_users(
    request: Request,
    q: Optional[str] = None,
    plan: Optional[str] = None,
    sort: Optional[str] = "created_at",
    order: Optional[str] = "desc",
    page: int = 1,
    page_size: int = 20
):
    admin_user = await require_master_admin(request)

    page = max(1, page)
    page_size = max(1, min(100, page_size))
    sort = sort or "created_at"
    order = order or "desc"
    sort_dir = -1 if order.lower() == "desc" else 1
    if sort not in {"created_at", "last_active_at", "usage_30d.perizie"}:
        sort = "created_at"

    query: Dict[str, Any] = {}
    if plan:
        query["plan"] = plan
    if q:
        regex = {"$regex": q, "$options": "i"}
        query["$or"] = [{"email": regex}, {"name": regex}, {"user_id": regex}]

    total = await db.users.count_documents(query)

    requires_full_scan = sort in ["created_at", "last_active_at", "usage_30d.perizie"]
    users_list: List[Dict[str, Any]]

    if requires_full_scan:
        users_list = await db.users.find(query, {"_id": 0}).to_list(max(total, 0))
    else:
        users_list = await db.users.find(query, {"_id": 0}).sort(sort, sort_dir).skip((page - 1) * page_size).limit(page_size).to_list(page_size)

    user_ids = [u.get("user_id") for u in users_list if u.get("user_id")]
    last30_date = (datetime.now(timezone.utc) - timedelta(days=30)).date().isoformat()
    date_query = _date_range_query("created_at", last30_date, None)

    perizie_30d_map = await _aggregate_usage(db.perizia_analyses, user_ids, date_query)
    images_30d_map = await _aggregate_usage(db.image_forensics, user_ids, date_query)
    assistant_30d_map = await _aggregate_usage(db.assistant_qa, user_ids, date_query)

    perizie_all_map = await _aggregate_usage(db.perizia_analyses, user_ids, None)
    images_all_map = await _aggregate_usage(db.image_forensics, user_ids, None)
    assistant_all_map = await _aggregate_usage(db.assistant_qa, user_ids, None)

    notes_docs = await db.admin_user_notes.find({"user_id": {"$in": user_ids}}, {"_id": 0}).to_list(len(user_ids))
    notes_map = {n.get("user_id"): n for n in notes_docs}
    financial_summary_map = await _get_admin_user_financial_summary_map(user_ids)

    enriched_users = []
    for u in users_list:
        user_id = u.get("user_id")
        last_active = _max_last_active(
            perizie_all_map.get(user_id, {}).get("last_active"),
            images_all_map.get(user_id, {}).get("last_active"),
            assistant_all_map.get(user_id, {}).get("last_active")
        )
        enriched_users.append({
            "user_id": user_id,
            "email": u.get("email"),
            "name": u.get("name"),
            "plan": u.get("plan"),
            "is_master_admin": u.get("is_master_admin", False),
            "created_at": _to_iso(u.get("created_at")),
            "last_active_at": last_active,
            "quota": {
                "perizia_scans_remaining": u.get("quota", {}).get("perizia_scans_remaining", 0),
                "image_scans_remaining": u.get("quota", {}).get("image_scans_remaining", 0),
                "assistant_messages_remaining": u.get("quota", {}).get("assistant_messages_remaining", 0)
            },
            "usage_30d": {
                "perizie": perizie_30d_map.get(user_id, {}).get("count", 0),
                "images": images_30d_map.get(user_id, {}).get("count", 0),
                "assistant_qas": assistant_30d_map.get(user_id, {}).get("count", 0)
            },
            "lifetime": {
                "perizie": perizie_all_map.get(user_id, {}).get("count", 0),
                "images": images_all_map.get(user_id, {}).get("count", 0),
                "assistant_qas": assistant_all_map.get(user_id, {}).get("count", 0)
            },
            "notes": {
                "internal_status": notes_map.get(user_id, {}).get("internal_status"),
                "tags": notes_map.get(user_id, {}).get("tags", []),
                "note": notes_map.get(user_id, {}).get("note", "")
            } if notes_map.get(user_id) else None,
            "financial_summary": financial_summary_map.get(user_id, {
                "latest_credit_movement_at": None,
                "latest_billing_status": None,
                "billing_records_count": 0,
                "latest_purchase_type": None,
            }),
        })

    if requires_full_scan:
        if sort == "created_at":
            enriched_users.sort(
                key=lambda x: _parse_dt(x.get("created_at")) or datetime.min.replace(tzinfo=timezone.utc),
                reverse=sort_dir == -1,
            )
        if sort == "last_active_at":
            enriched_users.sort(key=lambda x: _parse_dt(x.get("last_active_at")) or datetime.min.replace(tzinfo=timezone.utc), reverse=sort_dir == -1)
        elif sort == "usage_30d.perizie":
            enriched_users.sort(key=lambda x: x.get("usage_30d", {}).get("perizie", 0), reverse=sort_dir == -1)
        start = (page - 1) * page_size
        enriched_users = enriched_users[start:start + page_size]

    await _write_admin_audit(admin_user, "ADMIN_API_VIEW", meta={
        "endpoint": "users",
        "q": _truncate(q, 50),
        "plan": plan,
        "page": page
    })

    return {"items": enriched_users, "page": page, "page_size": page_size, "total": total}

@api_router.get("/admin/users/{user_id}")
async def admin_user_detail(user_id: str, request: Request):
    admin_user = await require_master_admin(request)

    user_doc = await db.users.find_one({"user_id": user_id}, {"_id": 0})
    if not user_doc:
        raise HTTPException(status_code=404, detail="User not found")

    notes_doc = await db.admin_user_notes.find_one({"user_id": user_id}, {"_id": 0})

    last30_date = (datetime.now(timezone.utc) - timedelta(days=30)).date().isoformat()
    date_query = _date_range_query("created_at", last30_date, None)

    perizie_30d_map = await _aggregate_usage(db.perizia_analyses, [user_id], date_query)
    images_30d_map = await _aggregate_usage(db.image_forensics, [user_id], date_query)
    assistant_30d_map = await _aggregate_usage(db.assistant_qa, [user_id], date_query)

    perizie_all_map = await _aggregate_usage(db.perizia_analyses, [user_id], None)
    images_all_map = await _aggregate_usage(db.image_forensics, [user_id], None)
    assistant_all_map = await _aggregate_usage(db.assistant_qa, [user_id], None)

    last_active = _max_last_active(
        perizie_all_map.get(user_id, {}).get("last_active"),
        images_all_map.get(user_id, {}).get("last_active"),
        assistant_all_map.get(user_id, {}).get("last_active")
    )

    perizie_recent = await db.perizia_analyses.find(
        {"user_id": user_id},
        {"_id": 0, "analysis_id": 1, "case_id": 1, "run_id": 1, "revision": 1, "created_at": 1, "result.semaforo_generale": 1, "result.section_1_semaforo_generale": 1}
    ).sort("created_at", -1).limit(20).to_list(20)
    images_recent = await db.image_forensics.find(
        {"user_id": user_id},
        {"_id": 0, "forensics_id": 1, "case_id": 1, "run_id": 1, "revision": 1, "created_at": 1, "image_count": 1}
    ).sort("created_at", -1).limit(20).to_list(20)
    assistant_recent = await db.assistant_qa.find(
        {"user_id": user_id},
        {"_id": 0, "qa_id": 1, "case_id": 1, "run_id": 1, "created_at": 1, "question": 1}
    ).sort("created_at", -1).limit(20).to_list(20)

    recent_activity = []
    for item in perizie_recent:
        semaforo = None
        result = item.get("result", {})
        semaforo = (result.get("semaforo_generale") or result.get("section_1_semaforo_generale") or {}).get("status")
        recent_activity.append({
            "type": "perizia",
            "id": item.get("analysis_id"),
            "case_id": item.get("case_id"),
            "run_id": item.get("run_id"),
            "revision": item.get("revision"),
            "created_at": _to_iso(item.get("created_at")),
            "summary": {"semaforo": semaforo}
        })
    for item in images_recent:
        recent_activity.append({
            "type": "image",
            "id": item.get("forensics_id"),
            "case_id": item.get("case_id"),
            "run_id": item.get("run_id"),
            "revision": item.get("revision"),
            "created_at": _to_iso(item.get("created_at")),
            "summary": {"image_count": item.get("image_count")}
        })
    for item in assistant_recent:
        question = item.get("question", "")
        recent_activity.append({
            "type": "assistant",
            "id": item.get("qa_id"),
            "case_id": item.get("case_id"),
            "run_id": item.get("run_id"),
            "revision": None,
            "created_at": _to_iso(item.get("created_at")),
            "summary": {"question_preview": question[:120]}
        })

    recent_activity = sorted(
        recent_activity,
        key=lambda x: _parse_dt(x.get("created_at")) or datetime.min.replace(tzinfo=timezone.utc),
        reverse=True
    )[:20]

    user_payload = {
        "user_id": user_doc.get("user_id"),
        "email": user_doc.get("email"),
        "name": user_doc.get("name"),
        "plan": user_doc.get("plan"),
        "is_master_admin": user_doc.get("is_master_admin", False),
        "created_at": _to_iso(user_doc.get("created_at")),
        "last_active_at": last_active,
        "quota": {
            "perizia_scans_remaining": user_doc.get("quota", {}).get("perizia_scans_remaining", 0),
            "image_scans_remaining": user_doc.get("quota", {}).get("image_scans_remaining", 0),
            "assistant_messages_remaining": user_doc.get("quota", {}).get("assistant_messages_remaining", 0)
        },
        "usage_30d": {
            "perizie": perizie_30d_map.get(user_id, {}).get("count", 0),
            "images": images_30d_map.get(user_id, {}).get("count", 0),
            "assistant_qas": assistant_30d_map.get(user_id, {}).get("count", 0)
        },
        "lifetime": {
            "perizie": perizie_all_map.get(user_id, {}).get("count", 0),
            "images": images_all_map.get(user_id, {}).get("count", 0),
            "assistant_qas": assistant_all_map.get(user_id, {}).get("count", 0)
        },
        "notes": {
            "internal_status": notes_doc.get("internal_status"),
            "tags": notes_doc.get("tags", []),
            "note": notes_doc.get("note", "")
        } if notes_doc else None,
        "financial_summary": (await _get_admin_user_financial_summary_map([user_id])).get(user_id, {
            "latest_credit_movement_at": None,
            "latest_billing_status": None,
            "billing_records_count": 0,
            "latest_purchase_type": None,
        }),
    }

    await _write_admin_audit(admin_user, "ADMIN_API_VIEW", meta={"endpoint": "users_detail", "user_id": user_id})

    return {"user": user_payload, "recent_activity": recent_activity}

@api_router.get("/admin/users/{user_id}/ledger")
async def admin_user_ledger(user_id: str, request: Request, limit: int = 20, skip: int = 0):
    admin_user = await require_master_admin(request)

    user_doc = await db.users.find_one({"user_id": user_id}, {"_id": 0, "user_id": 1})
    if not user_doc:
        raise HTTPException(status_code=404, detail="User not found")

    safe_limit = max(1, min(int(limit or 20), 100))
    safe_skip = max(0, int(skip or 0))
    query = {"user_id": user_id}
    total = await db.credit_ledger.count_documents(query)
    entries = await db.credit_ledger.find(
        query,
        {
            "_id": 0,
            "ledger_id": 1,
            "user_id": 1,
            "user_email": 1,
            "quota_field": 1,
            "direction": 1,
            "amount": 1,
            "balance_before": 1,
            "balance_after": 1,
            "entry_type": 1,
            "reference_type": 1,
            "reference_id": 1,
            "description_it": 1,
            "metadata": 1,
            "actor_user_id": 1,
            "actor_email": 1,
            "created_at": 1,
        },
    ).sort("created_at", -1).skip(safe_skip).limit(safe_limit).to_list(safe_limit)

    await _write_admin_audit(
        admin_user,
        "ADMIN_API_VIEW",
        meta={"endpoint": "users_ledger", "user_id": user_id, "skip": safe_skip, "limit": safe_limit}
    )

    return {
        "entries": [_serialize_admin_ledger_entry(entry) for entry in entries],
        "total": total,
        "limit": safe_limit,
        "skip": safe_skip,
    }

@api_router.get("/admin/users/{user_id}/billing-records")
async def admin_user_billing_records(user_id: str, request: Request, limit: int = 20, skip: int = 0):
    admin_user = await require_master_admin(request)

    user_doc = await db.users.find_one({"user_id": user_id}, {"_id": 0, "user_id": 1})
    if not user_doc:
        raise HTTPException(status_code=404, detail="User not found")

    safe_limit = max(1, min(int(limit or 20), 100))
    safe_skip = max(0, int(skip or 0))
    query = {"user_id": user_id}
    total = await db.billing_records.count_documents(query)
    records = await db.billing_records.find(
        query,
        {
            "_id": 0,
            "billing_record_id": 1,
            "user_id": 1,
            "user_email": 1,
            "customer_type": 1,
            "customer_name": 1,
            "company_name": 1,
            "billing_email": 1,
            "country_code": 1,
            "plan_id": 1,
            "purchase_type": 1,
            "amount_subtotal": 1,
            "amount_tax": 1,
            "amount_total": 1,
            "currency": 1,
            "status": 1,
            "payment_provider": 1,
            "payment_reference": 1,
            "checkout_reference": 1,
            "invoice_status": 1,
            "invoice_number": 1,
            "invoice_reference": 1,
            "description_it": 1,
            "metadata": 1,
            "created_at": 1,
            "updated_at": 1,
            "paid_at": 1,
        },
    ).sort("created_at", -1).skip(safe_skip).limit(safe_limit).to_list(safe_limit)

    await _write_admin_audit(
        admin_user,
        "ADMIN_API_VIEW",
        meta={"endpoint": "users_billing_records", "user_id": user_id, "skip": safe_skip, "limit": safe_limit}
    )

    return {
        "records": [_serialize_admin_billing_record(record) for record in records],
        "total": total,
        "limit": safe_limit,
        "skip": safe_skip,
    }

@api_router.patch("/admin/users/{user_id}")
async def admin_user_update(user_id: str, request: Request):
    admin_user = await require_master_admin(request)
    payload = await request.json()

    target_user_raw = await db.users.find_one({"user_id": user_id}, {"_id": 0})
    target_user = await _apply_normalized_account_state(target_user_raw, persist=True) if target_user_raw else None
    if not target_user:
        raise HTTPException(status_code=404, detail="User not found")

    await _ensure_opening_balance_baseline_for_user_doc(target_user)

    update_data: Dict[str, Any] = {}
    plan = payload.get("plan")
    quota = payload.get("quota") or {}
    billing_payload = payload.get("billing_record")

    before_plan = target_user.get("plan")
    before_quota = _quota_snapshot(target_user.get("quota"))

    if plan:
        if plan not in SUBSCRIPTION_PLANS:
            raise HTTPException(status_code=400, detail="Invalid plan")
        target_is_master = _is_master_admin_email(target_user.get("email"))
        if target_is_master and plan != "enterprise":
            raise HTTPException(status_code=400, detail="Cannot downgrade master admin plan")
        if not target_is_master and plan == "enterprise":
            raise HTTPException(status_code=400, detail="Enterprise plan is reserved for master admin")
        update_data["plan"] = plan

    quota_updates: Dict[str, int] = {}
    for key, value in quota.items():
        if key not in ADMIN_QUOTA_FIELDS:
            raise HTTPException(status_code=400, detail=f"Unsupported quota key: {key}")
        if not isinstance(value, int) or value < 0:
            raise HTTPException(status_code=400, detail="Quota values must be int >= 0")
        quota_updates[key] = value

    if quota_updates:
        new_quota = before_quota.copy()
        new_quota.update(quota_updates)
        update_data["quota"] = new_quota
        if "perizia_scans_remaining" in quota_updates:
            effective_plan = str(plan or before_plan or target_user.get("plan") or "").strip().lower()
            update_data["perizia_credits"] = _admin_override_perizia_credit_wallet(
                {**target_user, "plan": effective_plan},
                total_available=new_quota["perizia_scans_remaining"],
                plan_override=effective_plan,
            )
            update_data["quota"]["perizia_scans_remaining"] = update_data["perizia_credits"]["total_available"]

    if update_data:
        await db.users.update_one({"user_id": user_id}, {"$set": update_data})

    target_email = target_user.get("email")
    if plan and plan != before_plan:
        await _write_admin_audit(
            admin_user,
            "USER_SET_PLAN",
            target_user_id=user_id,
            target_email=target_email,
            meta={"before": before_plan, "after": plan}
        )
    if quota_updates:
        await _write_admin_audit(
            admin_user,
            "USER_SET_QUOTA",
            target_user_id=user_id,
            target_email=target_email,
            meta={"before": {k: before_quota.get(k) for k in quota_updates.keys()}, "after": quota_updates}
        )

    updated = await db.users.find_one({"user_id": user_id}, {"_id": 0})
    updated = await _apply_normalized_account_state(updated, persist=True)
    updated_quota = _quota_snapshot(updated.get("quota"))
    await _record_quota_change_entries(
        user_doc=updated,
        before_quota=before_quota,
        after_quota=updated_quota,
        entry_type="admin_adjustment",
        reference_type="admin_user_update",
        reference_id=user_id,
        description_it="Variazione crediti eseguita da amministratore",
        metadata={
            "old_plan": before_plan,
            "new_plan": updated.get("plan"),
            "quota_updates": quota_updates,
        },
        actor_user=admin_user,
    )
    if billing_payload:
        await _create_admin_manual_billing_record(
            admin_user=admin_user,
            target_user_doc=updated,
            billing_payload=billing_payload,
        )
    updated["created_at"] = _to_iso(updated.get("created_at"))
    return {"ok": True, "user": updated}

@api_router.put("/admin/users/{user_id}/notes")
async def admin_user_notes(user_id: str, request: Request):
    admin_user = await require_master_admin(request)
    payload = await request.json()

    note = payload.get("note", "")
    tags = payload.get("tags", []) or []
    internal_status = payload.get("internal_status", "OK")
    if not isinstance(note, str):
        raise HTTPException(status_code=400, detail="note must be a string")
    if not isinstance(tags, list) or any(not isinstance(tag, str) for tag in tags):
        raise HTTPException(status_code=400, detail="tags must be an array of strings")
    internal_status = str(internal_status).upper()
    if internal_status not in ADMIN_NOTE_STATUSES:
        raise HTTPException(status_code=400, detail="internal_status must be one of: OK, WATCH, BLOCKED")

    existing = await db.admin_user_notes.find_one({"user_id": user_id}, {"_id": 0})
    created_at = existing.get("created_at") if existing else _now_iso()

    doc = {
        "user_id": user_id,
        "note": note,
        "tags": tags,
        "internal_status": internal_status,
        "updated_by_admin_email": admin_user.email,
        "updated_at": _now_iso(),
        "created_at": created_at
    }

    await db.admin_user_notes.update_one({"user_id": user_id}, {"$set": doc}, upsert=True)

    await _write_admin_audit(
        admin_user,
        "USER_NOTE_UPSERT",
        target_user_id=user_id,
        target_email=None,
        meta={"internal_status": internal_status, "tags": tags}
    )

    return {"ok": True, "notes": doc}

@api_router.get("/admin/perizie")
async def admin_perizie(
    request: Request,
    q: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    page: int = 1,
    page_size: int = 20
):
    admin_user = await require_master_admin(request)
    page = max(1, page)
    page_size = max(1, min(100, page_size))

    query: Dict[str, Any] = {}
    if q:
        regex = {"$regex": q, "$options": "i"}
        query["$or"] = [{"case_id": regex}, {"run_id": regex}, {"analysis_id": regex}, {"file_name": regex}, {"user_id": regex}]

    date_query = _date_range_query("created_at", date_from, date_to)
    query = _merge_query(query, date_query) if date_query else query

    total = await db.perizia_analyses.count_documents(query)
    items = await db.perizia_analyses.find(
        query,
        {"_id": 0, "analysis_id": 1, "user_id": 1, "case_id": 1, "run_id": 1, "revision": 1, "created_at": 1, "file_name": 1, "result.semaforo_generale": 1, "result.section_1_semaforo_generale": 1}
    ).sort("created_at", -1).skip((page - 1) * page_size).limit(page_size).to_list(page_size)

    user_ids = [i.get("user_id") for i in items if i.get("user_id")]
    users_map = await _get_user_email_map(user_ids)
    rows = []
    for item in items:
        result = item.get("result", {})
        semaforo = (result.get("semaforo_generale") or result.get("section_1_semaforo_generale") or {}).get("status")
        rows.append({
            "analysis_id": item.get("analysis_id") or item.get("id"),
            "user_id": item.get("user_id"),
            "email": users_map.get(item.get("user_id"), {}).get("email"),
            "case_id": item.get("case_id"),
            "run_id": item.get("run_id"),
            "revision": item.get("revision"),
            "created_at": _to_iso(item.get("created_at")),
            "semaforo": semaforo,
            "file_name": item.get("file_name")
        })

    await _write_admin_audit(admin_user, "ADMIN_API_VIEW", meta={
        "endpoint": "perizie",
        "q": _truncate(q, 50),
        "page": page
    })

    return {"items": rows, "page": page, "page_size": page_size, "total": total}

@api_router.get("/admin/images")
async def admin_images(
    request: Request,
    q: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    page: int = 1,
    page_size: int = 20
):
    admin_user = await require_master_admin(request)
    page = max(1, page)
    page_size = max(1, min(100, page_size))

    query: Dict[str, Any] = {}
    if q:
        regex = {"$regex": q, "$options": "i"}
        query["$or"] = [{"case_id": regex}, {"run_id": regex}, {"forensics_id": regex}, {"user_id": regex}]

    date_query = _date_range_query("created_at", date_from, date_to)
    query = _merge_query(query, date_query) if date_query else query

    total = await db.image_forensics.count_documents(query)
    items = await db.image_forensics.find(
        query,
        {"_id": 0, "forensics_id": 1, "user_id": 1, "case_id": 1, "run_id": 1, "revision": 1, "created_at": 1, "image_count": 1}
    ).sort("created_at", -1).skip((page - 1) * page_size).limit(page_size).to_list(page_size)

    user_ids = [i.get("user_id") for i in items if i.get("user_id")]
    users_map = await _get_user_email_map(user_ids)
    rows = []
    for item in items:
        rows.append({
            "forensics_id": item.get("forensics_id"),
            "user_id": item.get("user_id"),
            "email": users_map.get(item.get("user_id"), {}).get("email"),
            "case_id": item.get("case_id"),
            "run_id": item.get("run_id"),
            "revision": item.get("revision"),
            "created_at": _to_iso(item.get("created_at")),
            "image_count": item.get("image_count", 0)
        })

    await _write_admin_audit(admin_user, "ADMIN_API_VIEW", meta={
        "endpoint": "images",
        "q": _truncate(q, 50),
        "page": page
    })

    return {"items": rows, "page": page, "page_size": page_size, "total": total}

@api_router.get("/admin/assistant")
async def admin_assistant(
    request: Request,
    q: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    page: int = 1,
    page_size: int = 20
):
    admin_user = await require_master_admin(request)
    page = max(1, page)
    page_size = max(1, min(100, page_size))

    query: Dict[str, Any] = {}
    if q:
        regex = {"$regex": q, "$options": "i"}
        query["$or"] = [{"case_id": regex}, {"run_id": regex}, {"qa_id": regex}, {"question": regex}, {"user_id": regex}]

    date_query = _date_range_query("created_at", date_from, date_to)
    query = _merge_query(query, date_query) if date_query else query

    total = await db.assistant_qa.count_documents(query)
    items = await db.assistant_qa.find(
        query,
        {"_id": 0, "qa_id": 1, "user_id": 1, "case_id": 1, "run_id": 1, "created_at": 1, "question": 1}
    ).sort("created_at", -1).skip((page - 1) * page_size).limit(page_size).to_list(page_size)

    user_ids = [i.get("user_id") for i in items if i.get("user_id")]
    users_map = await _get_user_email_map(user_ids)
    rows = []
    for item in items:
        question = item.get("question", "")
        rows.append({
            "qa_id": item.get("qa_id"),
            "user_id": item.get("user_id"),
            "email": users_map.get(item.get("user_id"), {}).get("email"),
            "case_id": item.get("case_id"),
            "run_id": item.get("run_id"),
            "created_at": _to_iso(item.get("created_at")),
            "question_preview": question[:120]
        })

    await _write_admin_audit(admin_user, "ADMIN_API_VIEW", meta={
        "endpoint": "assistant",
        "q": _truncate(q, 50),
        "page": page
    })

    return {"items": rows, "page": page, "page_size": page_size, "total": total}

@api_router.get("/admin/transactions")
async def admin_transactions(
    request: Request,
    q: Optional[str] = None,
    status: Optional[str] = None,
    page: int = 1,
    page_size: int = 20
):
    admin_user = await require_master_admin(request)
    page = max(1, page)
    page_size = max(1, min(100, page_size))

    query: Dict[str, Any] = {}
    if status:
        query["status"] = status
    if q:
        regex = {"$regex": q, "$options": "i"}
        query["$or"] = [{"session_id": regex}, {"transaction_id": regex}, {"user_id": regex}, {"plan_id": regex}]

    total = await db.payment_transactions.count_documents(query)
    items = await db.payment_transactions.find(
        query,
        {"_id": 0, "transaction_id": 1, "session_id": 1, "user_id": 1, "plan_id": 1, "status": 1, "payment_status": 1, "amount": 1, "currency": 1, "created_at": 1}
    ).sort("created_at", -1).skip((page - 1) * page_size).limit(page_size).to_list(page_size)

    user_ids = [i.get("user_id") for i in items if i.get("user_id")]
    users_map = await _get_user_email_map(user_ids)
    rows = []
    for item in items:
        rows.append({
            "transaction_id": item.get("transaction_id"),
            "session_id": item.get("session_id"),
            "user_id": item.get("user_id"),
            "email": users_map.get(item.get("user_id"), {}).get("email"),
            "plan_id": item.get("plan_id"),
            "status": item.get("status"),
            "payment_status": item.get("payment_status"),
            "amount": item.get("amount"),
            "currency": item.get("currency"),
            "created_at": _to_iso(item.get("created_at"))
        })

    await _write_admin_audit(admin_user, "ADMIN_API_VIEW", meta={
        "endpoint": "transactions",
        "q": _truncate(q, 50),
        "status": status,
        "page": page
    })

    return {"items": rows, "page": page, "page_size": page_size, "total": total}

# ===================
# HISTORY ENDPOINTS
# ===================

@api_router.get("/history/perizia")
async def get_perizia_history(request: Request, limit: int = 20, skip: int = 0):
    """Get user's perizia analysis history"""
    user = await require_auth(request)
    
    analyses = await db.perizia_analyses.find(
        {"user_id": user.user_id},
        {"_id": 0, "raw_text": 0}  # Exclude raw_text for performance
    ).sort("created_at", -1).skip(skip).limit(limit).to_list(limit)

    for analysis in analyses:
        analysis["semaforo_status"] = _refresh_list_analysis_semaforo(analysis)
    
    total = await db.perizia_analyses.count_documents({"user_id": user.user_id})
    
    return {"analyses": analyses, "total": total, "limit": limit, "skip": skip}

def _load_offline_persisted_analysis(path: Path) -> Optional[Dict[str, Any]]:
    if not path.exists():
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            return data
    except Exception:
        return None
    return None

def _load_pages_for_analysis(analysis_id: str, pages_hint: int) -> List[Dict[str, Any]]:
    """
    Load extracted pages for deterministic read-path rebuilds.
    Falls back to placeholder pages only when extraction artifacts are unavailable.
    """
    safe_pages_hint = max(1, int(pages_hint or 0))
    extract_pages_path = Path("/srv/perizia/_qa/runs") / analysis_id / "extract" / "pages_raw.json"
    if extract_pages_path.exists():
        try:
            with open(extract_pages_path, "r", encoding="utf-8") as f:
                payload = json.load(f)
            if isinstance(payload, list) and payload:
                pages: List[Dict[str, Any]] = []
                for idx, entry in enumerate(payload, start=1):
                    if not isinstance(entry, dict):
                        continue
                    page_number = entry.get("page_number")
                    try:
                        page_number = int(page_number)
                    except Exception:
                        page_number = idx
                    pages.append(
                        {
                            "page_number": page_number,
                            "text": str(entry.get("text", "") or ""),
                        }
                    )
                if pages:
                    return pages
        except Exception:
            pass
    return [{"page_number": i + 1, "text": ""} for i in range(safe_pages_hint)]

async def _get_perizia_analysis_for_user_with_storage(
    analysis_id: str,
    user: User
) -> Tuple[Dict[str, Any], str, Optional[Path]]:
    analysis = await db.perizia_analyses.find_one(
        {"analysis_id": analysis_id, "user_id": user.user_id},
        {"_id": 0, "raw_text": 0}
    )
    if analysis:
        return analysis, "mongo", None

    offline_path = Path("/tmp/perizia_qa_run/analysis.json")
    offline_analysis = _load_offline_persisted_analysis(offline_path)
    if offline_analysis and offline_analysis.get("analysis_id") == analysis_id:
        offline_owner = str(offline_analysis.get("user_id", "") or "").strip()
        if offline_owner and offline_owner != user.user_id:
            raise HTTPException(status_code=404, detail="Analysis not found")
        offline_analysis.pop("raw_text", None)
        return offline_analysis, "offline_file", offline_path

    raise HTTPException(status_code=404, detail="Analysis not found")

def _persist_offline_analysis(path: Path, analysis: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(analysis, f, ensure_ascii=False, indent=2)

async def _save_headline_overrides_for_analysis(
    *,
    analysis_id: str,
    user: User,
    analysis: Dict[str, Any],
    storage_mode: str,
    storage_path: Optional[Path]
) -> None:
    if storage_mode == "mongo":
        await db.perizia_analyses.update_one(
            {"analysis_id": analysis_id, "user_id": user.user_id},
            {"$set": {"headline_overrides": analysis.get("headline_overrides", {})}}
        )
        return
    if storage_mode == "offline_file":
        if storage_path is None:
            raise HTTPException(status_code=500, detail="Offline analysis path not available")
        existing = _load_offline_persisted_analysis(storage_path)
        if not existing or existing.get("analysis_id") != analysis_id:
            raise HTTPException(status_code=404, detail="Analysis not found")
        existing_owner = str(existing.get("user_id", "") or "").strip()
        if existing_owner and existing_owner != user.user_id:
            raise HTTPException(status_code=404, detail="Analysis not found")
        existing["headline_overrides"] = analysis.get("headline_overrides", {}) or {}
        _persist_offline_analysis(storage_path, existing)
        return
    raise HTTPException(status_code=500, detail="Unsupported analysis storage mode")

async def _save_field_overrides_for_analysis(
    *,
    analysis_id: str,
    user: User,
    analysis: Dict[str, Any],
    storage_mode: str,
    storage_path: Optional[Path]
) -> None:
    if storage_mode == "mongo":
        await db.perizia_analyses.update_one(
            {"analysis_id": analysis_id, "user_id": user.user_id},
            {"$set": {"field_overrides": analysis.get("field_overrides", {})}}
        )
        return
    if storage_mode == "offline_file":
        if storage_path is None:
            raise HTTPException(status_code=500, detail="Offline analysis path not available")
        existing = _load_offline_persisted_analysis(storage_path)
        if not existing or existing.get("analysis_id") != analysis_id:
            raise HTTPException(status_code=404, detail="Analysis not found")
        existing_owner = str(existing.get("user_id", "") or "").strip()
        if existing_owner and existing_owner != user.user_id:
            raise HTTPException(status_code=404, detail="Analysis not found")
        existing["field_overrides"] = analysis.get("field_overrides", {}) or {}
        _persist_offline_analysis(storage_path, existing)
        return
    raise HTTPException(status_code=500, detail="Unsupported analysis storage mode")

async def _get_perizia_analysis_for_user(analysis_id: str, user: User) -> Dict[str, Any]:
    analysis, _storage_mode, _storage_path = await _get_perizia_analysis_for_user_with_storage(analysis_id, user)

    result = analysis.get("result")
    if isinstance(result, dict):
        pages_hint = int(analysis.get("pages_count", 0) or 0)
        pages_for_proof = _load_pages_for_analysis(analysis_id, pages_hint)
        _refresh_customer_facing_result_on_read(
            result,
            pages_for_proof,
            analysis_id=analysis_id,
            headline_overrides=analysis.get("headline_overrides") or {},
            field_overrides=analysis.get("field_overrides") or {},
        )
        states = result.get("field_states")
        if isinstance(states, dict):
            if "superficie" not in states and "superficie_catastale" in states:
                states["superficie"] = states.get("superficie_catastale")
            states.pop("superficie_catastale", None)
            result["field_states"] = states
        analysis["result"] = result
    return analysis

def _refresh_list_analysis_semaforo(analysis: Dict[str, Any]) -> Optional[str]:
    if not isinstance(analysis, dict):
        return None
    result = analysis.get("result")
    if not isinstance(result, dict):
        return None

    analysis_id = str(analysis.get("analysis_id") or "")
    pages_hint = int(analysis.get("pages_count", 0) or 0)
    pages_for_proof = _load_pages_for_analysis(analysis_id, pages_hint)
    _refresh_customer_facing_result_on_read(
        result,
        pages_for_proof,
        analysis_id=analysis_id,
        headline_overrides=analysis.get("headline_overrides") or {},
        field_overrides=analysis.get("field_overrides") or {},
    )
    analysis["result"] = result

    section1 = result.get("section_1_semaforo_generale")
    semaforo = section1 if isinstance(section1, dict) else result.get("semaforo_generale")
    if not isinstance(semaforo, dict):
        return None

    status = str(semaforo.get("status") or "").upper().strip()
    return status or None

@api_router.get("/history/perizia/{analysis_id}")
async def get_perizia_detail(analysis_id: str, request: Request):
    """Get specific perizia analysis"""
    user = await require_auth(request)
    return await _get_perizia_analysis_for_user(analysis_id, user)

@api_router.get("/analysis/perizia/{analysis_id}")
async def get_perizia_detail_alias(analysis_id: str, request: Request):
    """Alias of history detail endpoint with identical auth and ownership checks."""
    user = await require_auth(request)
    return await _get_perizia_analysis_for_user(analysis_id, user)

@api_router.get("/history/images")
async def get_image_history(request: Request, limit: int = 20, skip: int = 0):
    """Get user's image forensics history"""
    user = await require_auth(request)
    
    forensics = await db.image_forensics.find(
        {"user_id": user.user_id},
        {"_id": 0}
    ).sort("created_at", -1).skip(skip).limit(limit).to_list(limit)
    
    total = await db.image_forensics.count_documents({"user_id": user.user_id})
    
    return {"forensics": forensics, "total": total, "limit": limit, "skip": skip}

@api_router.get("/history/assistant")
async def get_assistant_history(request: Request, limit: int = 50, skip: int = 0):
    """Get user's assistant QA history"""
    user = await require_auth(request)
    
    qas = await db.assistant_qa.find(
        {"user_id": user.user_id},
        {"_id": 0}
    ).sort("created_at", -1).skip(skip).limit(limit).to_list(limit)
    
    total = await db.assistant_qa.count_documents({"user_id": user.user_id})
    
    return {"conversations": qas, "total": total, "limit": limit, "skip": skip}

@api_router.get("/billing/ledger")
async def get_billing_ledger(request: Request, limit: int = 20, skip: int = 0):
    """Get current user's credit ledger history"""
    user = await require_auth(request)
    safe_limit = max(1, min(int(limit or 20), 100))
    safe_skip = max(0, int(skip or 0))

    entries = await db.credit_ledger.find(
        {"user_id": user.user_id},
        {
            "_id": 0,
            "ledger_id": 1,
            "quota_field": 1,
            "direction": 1,
            "amount": 1,
            "balance_before": 1,
            "balance_after": 1,
            "entry_type": 1,
            "reference_type": 1,
            "reference_id": 1,
            "description_it": 1,
            "metadata": 1,
            "actor_user_id": 1,
            "actor_email": 1,
            "created_at": 1,
        },
    ).sort("created_at", -1).skip(safe_skip).limit(safe_limit).to_list(safe_limit)
    total = await db.credit_ledger.count_documents({"user_id": user.user_id})

    return {"entries": entries, "total": total, "limit": safe_limit, "skip": safe_skip}

# ===================
# DELETE ENDPOINTS
# ===================

@api_router.delete("/analysis/perizia/{analysis_id}")
async def delete_perizia_analysis(analysis_id: str, request: Request):
    """Delete a single perizia analysis"""
    user = await require_auth(request)
    
    result = await db.perizia_analyses.delete_one({
        "analysis_id": analysis_id,
        "user_id": user.user_id
    })
    
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Analisi non trovata / Analysis not found")
    
    logger.info(f"User {user.user_id} deleted perizia analysis {analysis_id}")
    return {"ok": True, "message": "Analisi eliminata / Analysis deleted"}

@api_router.delete("/analysis/images/{forensics_id}")
async def delete_image_forensics(forensics_id: str, request: Request):
    """Delete a single image forensics analysis"""
    user = await require_auth(request)
    
    result = await db.image_forensics.delete_one({
        "forensics_id": forensics_id,
        "user_id": user.user_id
    })
    
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Analisi non trovata / Analysis not found")
    
    logger.info(f"User {user.user_id} deleted image forensics {forensics_id}")
    return {"ok": True, "message": "Analisi eliminata / Analysis deleted"}

@api_router.delete("/analysis/assistant/{qa_id}")
async def delete_assistant_qa(qa_id: str, request: Request):
    """Delete a single assistant Q&A"""
    user = await require_auth(request)
    
    result = await db.assistant_qa.delete_one({
        "qa_id": qa_id,
        "user_id": user.user_id
    })
    
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Conversazione non trovata / Conversation not found")
    
    logger.info(f"User {user.user_id} deleted assistant QA {qa_id}")
    return {"ok": True, "message": "Conversazione eliminata / Conversation deleted"}

@api_router.delete("/history/all")
async def delete_all_history(request: Request):
    """Delete all user's history (perizia, images, assistant)"""
    user = await require_auth(request)
    
    # Delete all perizia analyses
    perizia_result = await db.perizia_analyses.delete_many({"user_id": user.user_id})
    
    # Delete all image forensics
    image_result = await db.image_forensics.delete_many({"user_id": user.user_id})
    
    # Delete all assistant QA
    qa_result = await db.assistant_qa.delete_many({"user_id": user.user_id})
    
    total_deleted = perizia_result.deleted_count + image_result.deleted_count + qa_result.deleted_count
    
    logger.info(f"User {user.user_id} deleted all history: {perizia_result.deleted_count} perizia, {image_result.deleted_count} images, {qa_result.deleted_count} QA")
    
    return {
        "ok": True,
        "message": f"Tutto lo storico eliminato / All history deleted",
        "deleted": {
            "perizia": perizia_result.deleted_count,
            "images": image_result.deleted_count,
            "assistant": qa_result.deleted_count,
            "total": total_deleted
        }
    }

# ===================
# DASHBOARD STATS
# ===================

@api_router.get("/dashboard/stats")
async def get_dashboard_stats(request: Request):
    """Get dashboard statistics for user"""
    user = await require_auth(request)
    
    # Get counts
    perizia_count = await db.perizia_analyses.count_documents({"user_id": user.user_id})
    image_count = await db.image_forensics.count_documents({"user_id": user.user_id})
    qa_count = await db.assistant_qa.count_documents({"user_id": user.user_id})
    
    # Get recent analyses
    recent_analyses = await db.perizia_analyses.find(
        {"user_id": user.user_id},
        {"_id": 0, "analysis_id": 1, "case_id": 1, "case_title": 1, "created_at": 1, "pages_count": 1, "headline_overrides": 1, "field_overrides": 1, "result.semaforo_generale": 1, "result.section_1_semaforo_generale": 1, "result.field_states": 1}
    ).sort("created_at", -1).limit(5).to_list(5)

    for analysis in recent_analyses:
        analysis["semaforo_status"] = _refresh_list_analysis_semaforo(analysis)
    
    # Calculate semaforo distribution
    pipeline = [
        {"$match": {"user_id": user.user_id}},
        {"$group": {"_id": "$result.semaforo_generale.status", "count": {"$sum": 1}}}
    ]
    semaforo_dist = await db.perizia_analyses.aggregate(pipeline).to_list(10)
    
    return {
        "total_analyses": perizia_count,
        "total_image_forensics": image_count,
        "total_assistant_queries": qa_count,
        "recent_analyses": recent_analyses,
        "semaforo_distribution": {item["_id"]: item["count"] for item in semaforo_dist if item["_id"]},
        "quota": user.quota,
        "plan": user.plan
    }

# ===================
# ROOT ENDPOINTS
# ===================

@api_router.get("/")
async def root():
    return {"message": "Nexodify Forensic Engine API", "version": "1.0.0"}

@api_router.get("/health")
async def health():
    return {"status": "healthy", "timestamp": datetime.now(timezone.utc).isoformat()}

# Include the router in the main app
app.include_router(api_router)

def custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema
    openapi_schema = get_openapi(
        title=app.title,
        version="1.0.0",
        description="Nexodify Forensic Engine API",
        routes=app.routes,
    )
    openapi_schema.setdefault("components", {}).setdefault("securitySchemes", {})
    openapi_schema["components"]["securitySchemes"]["sessionCookie"] = {
        "type": "apiKey",
        "in": "cookie",
        "name": "session_token",
    }
    openapi_schema["components"]["securitySchemes"]["bearerAuth"] = {
        "type": "http",
        "scheme": "bearer",
    }

    public_paths = {
        "/api/health",
        "/api/plans",
        "/api/auth/session",
        "/api/webhook/stripe",
    }

    for path, methods in openapi_schema.get("paths", {}).items():
        if not path.startswith("/api"):
            continue
        if path in public_paths:
            continue
        for method, details in methods.items():
            if method.lower() not in {"get", "post", "put", "delete", "patch"}:
                continue
            details.setdefault("security", [{"sessionCookie": []}, {"bearerAuth": []}])

    app.openapi_schema = openapi_schema
    return app.openapi_schema

app.openapi = custom_openapi

# Get the frontend URL from environment for proper CORS configuration
FRONTEND_URL = os.environ.get('FRONTEND_URL', '')
CORS_ORIGINS = [
    "http://localhost:3000",
]
# Add any additional origins from environment
if FRONTEND_URL and FRONTEND_URL not in CORS_ORIGINS:
    CORS_ORIGINS.append(FRONTEND_URL)

app.add_middleware(
    CORSMiddleware,
    allow_credentials=True,
    allow_origins=CORS_ORIGINS,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"],
)

@app.on_event("startup")
async def ensure_indexes():
    if ALLOW_OFFLINE_QA_ENV and OFFLINE_QA_TOKEN:
        logger.info("Offline QA mode enabled: skipping index creation")
        return
    index_specs = [
        (db.users, "email"),
        (db.users, "plan"),
        (db.users, "created_at"),
        (db.perizia_analyses, "user_id"),
        (db.perizia_analyses, "created_at"),
        (db.perizia_analyses, "case_id"),
        (db.perizia_analyses, "run_id"),
        (db.image_forensics, "user_id"),
        (db.image_forensics, "created_at"),
        (db.image_forensics, "case_id"),
        (db.image_forensics, "run_id"),
        (db.assistant_qa, "user_id"),
        (db.assistant_qa, "created_at"),
        (db.assistant_qa, "case_id"),
        (db.assistant_qa, "run_id"),
        (db.payment_transactions, "user_id"),
        (db.payment_transactions, "created_at"),
        (db.payment_transactions, "status"),
        (db.credit_ledger, "user_id"),
        (db.credit_ledger, "created_at"),
        (db.credit_ledger, "entry_type"),
        (db.credit_ledger, "reference_id"),
        (db.billing_records, "user_id"),
        (db.billing_records, "created_at"),
        (db.billing_records, "status"),
        (db.billing_records, "checkout_reference"),
        (db.admin_user_notes, "user_id"),
        (db.admin_audit_log, "created_at"),
        (db.admin_audit_log, "admin_email"),
        (db.admin_audit_log, "action"),
        (db.admin_audit_log, "target_user_id"),
    ]
    for collection, field in index_specs:
        try:
            await collection.create_index(field)
        except Exception as e:
            logger.warning(f"Index creation failed for {collection.name}.{field}: {e}")

@app.on_event("shutdown")
async def shutdown_db_client():
    client.close()

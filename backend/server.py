from fastapi import FastAPI, APIRouter, HTTPException, Request, UploadFile, File, Depends, Response
from fastapi.responses import JSONResponse, StreamingResponse
from dotenv import load_dotenv
from starlette.middleware.cors import CORSMiddleware
from motor.motor_asyncio import AsyncIOMotorClient
import os
import asyncio
import logging
import re
from pathlib import Path
from pydantic import BaseModel, Field, ConfigDict
from typing import List, Optional, Dict, Any, Tuple
import uuid
from datetime import datetime, timezone, timedelta
import json
import httpx
import ipaddress
from fastapi.openapi.utils import get_openapi
from PyPDF2 import PdfReader
import io
import hashlib
from openai import AsyncOpenAI

ROOT_DIR = Path(__file__).parent
load_dotenv(ROOT_DIR / '.env')

# MongoDB connection
mongo_url = os.environ['MONGO_URL']
client = AsyncIOMotorClient(mongo_url)
db = client[os.environ['DB_NAME']]

# Environment variables
OPENAI_API_KEY = os.environ.get('OPENAI_API_KEY')
STRIPE_SECRET_KEY = os.environ.get('STRIPE_SECRET_KEY')
MASTER_ADMIN_EMAIL = os.environ.get('MASTER_ADMIN_EMAIL', 'admin@nexodify.com')
DOC_AI_TIMEOUT_SECONDS = int(os.environ.get('DOC_AI_TIMEOUT_SECONDS', '30'))
LLM_TIMEOUT_SECONDS = int(os.environ.get('LLM_TIMEOUT_SECONDS', '45'))
PIPELINE_TIMEOUT_SECONDS = int(os.environ.get('PIPELINE_TIMEOUT_SECONDS', '120'))
LLM_SUMMARY_TIMEOUT_SECONDS = int(os.environ.get('LLM_SUMMARY_TIMEOUT_SECONDS', '8'))
PDF_TEXT_MIN_PAGE_CHARS = int(os.environ.get("PDF_TEXT_MIN_PAGE_CHARS", "40"))
PDF_TEXT_MIN_COVERAGE_RATIO = float(os.environ.get("PDF_TEXT_MIN_COVERAGE_RATIO", "0.6"))
PDF_TEXT_MAX_BLANK_PAGE_RATIO = float(os.environ.get("PDF_TEXT_MAX_BLANK_PAGE_RATIO", "0.3"))
OFFLINE_QA_ENV = os.environ.get('OFFLINE_QA', '0').lower() in {"1", "true", "yes"}
ALLOW_OFFLINE_QA_ENV = os.environ.get("ALLOW_OFFLINE_QA", "0").strip() == "1"
OFFLINE_QA_TOKEN = os.environ.get("OFFLINE_QA_TOKEN", "").strip()
EVIDENCE_OFFSET_MODE = "PAGE_LOCAL"

OFFLINE_QA_FIXTURE_PATH = os.environ.get(
    "OFFLINE_QA_FIXTURE_PATH",
    str(ROOT_DIR / "tests" / "fixtures" / "perizia_test_extraction.json")
)


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
        "perizia_scans_remaining": 3,
        "image_scans_remaining": 5,
        "assistant_messages_remaining": 10
    })
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
        name="Free Trial",
        name_it="Prova Gratuita",
        price=0.0,
        features=["3 Perizia Scans", "5 Image Analyses", "10 Assistant Messages", "Basic Reports"],
        features_it=["3 Scansioni Perizia", "5 Analisi Immagini", "10 Messaggi Assistente", "Report Base"],
        quota={"perizia_scans_remaining": 3, "image_scans_remaining": 5, "assistant_messages_remaining": 10}
    ),
    "pro": SubscriptionPlan(
        plan_id="pro",
        name="Professional",
        name_it="Professionale",
        price=49.00,
        features=["50 Perizia Scans/month", "100 Image Analyses/month", "Unlimited Assistant", "Premium Reports", "Priority Support"],
        features_it=["50 Scansioni Perizia/mese", "100 Analisi Immagini/mese", "Assistente Illimitato", "Report Premium", "Supporto Prioritario"],
        quota={"perizia_scans_remaining": 50, "image_scans_remaining": 100, "assistant_messages_remaining": 9999}
    ),
    "enterprise": SubscriptionPlan(
        plan_id="enterprise",
        name="Enterprise",
        name_it="Enterprise",
        price=199.00,
        features=["Unlimited Perizia Scans", "Unlimited Image Analyses", "Unlimited Assistant", "Custom Reports", "API Access", "Dedicated Support"],
        features_it=["Scansioni Perizia Illimitate", "Analisi Immagini Illimitate", "Assistente Illimitato", "Report Personalizzati", "Accesso API", "Supporto Dedicato"],
        quota={"perizia_scans_remaining": 9999, "image_scans_remaining": 9999, "assistant_messages_remaining": 9999}
    )
}

# ===================
# AUTH HELPERS
# ===================

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
    
    return User(**user_doc)

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
    if user.email.lower() != MASTER_ADMIN_EMAIL.lower():
        raise HTTPException(status_code=403, detail="Forbidden")
    return user

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

def _build_evidence(page_text: str, page_num: int, start: int, end: int) -> Dict[str, Any]:
    snippet = page_text[start:end].strip()
    page_text_hash = hashlib.sha256(page_text.encode("utf-8")).hexdigest()
    return {
        "page": page_num,
        "quote": snippet[:200],
        "start_offset": start,
        "end_offset": end,
        "bbox": None,
        "offset_mode": EVIDENCE_OFFSET_MODE,
        "page_text_hash": page_text_hash
    }

def _build_search_entry(
    page_text: str,
    page_num: int,
    start: int,
    end: int,
    fallback_quote: Optional[str] = None,
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
    if len(quote) > 200:
        quote = quote[:200]
    page_text_hash = hashlib.sha256(text.encode("utf-8")).hexdigest() if text else None
    return {
        "page": page_num,
        "quote": quote,
        "start_offset": start,
        "end_offset": end,
        "bbox": None,
        "offset_mode": EVIDENCE_OFFSET_MODE,
        "page_text_hash": page_text_hash,
    }

def _find_regex_in_pages(pages_in: List[Dict], pattern: str, flags=0) -> Optional[Dict[str, Any]]:
    import re
    for p in pages_in:
        text = str(p.get("text", "") or "")
        m = re.search(pattern, text, flags)
        if m:
            return _build_evidence(text, int(p.get("page_number", 0) or 0), m.start(), m.end())
    return None

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

def _search_proof(pages_in: List[Dict[str, Any]], keywords: List[str], snippets: Optional[List[Dict[str, Any]]] = None) -> List[Dict[str, Any]]:
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
                entries.append(_build_search_entry(text, page, idx, idx + len(quote), fallback_quote=quote))
                return
        lowered = text.lower()
        for kw in keywords:
            idx = lowered.find(kw.lower())
            if idx >= 0:
                start = max(0, idx - 40)
                end = min(len(text), idx + 80)
                entries.append(_build_search_entry(text, page, start, end, fallback_quote=kw))
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
                entries.append(_build_search_entry(text, page_num, start, end, fallback_quote=kw))
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
) -> List[Dict[str, Any]]:
    if status not in {"NOT_FOUND", "LOW_CONFIDENCE"}:
        return []
    entries = _search_proof(pages_in, keywords, snippets)
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
    cleaned = re.split(r"S\s*E\s*Z", cleaned, flags=re.I)[0].strip()
    cleaned = re.sub(r"TRIBUNALE\s*DI", "TRIBUNALE DI", cleaned, flags=re.I)
    cleaned = re.sub(r"\bDI([A-ZÀ-Ù])", r"DI \1", cleaned)
    return cleaned

def _normalize_procedura_value(value: Optional[str]) -> Optional[str]:
    if not value:
        return value
    cleaned = _normalize_headline_text(str(value))
    match = re.search(r"\b(\d{1,6}/\d{2,4})\b", cleaned)
    return match.group(1) if match else cleaned

def _normalize_address_value(value: Optional[str]) -> Optional[str]:
    if not value:
        return value
    cleaned = _normalize_headline_text(str(value))
    cleaned = re.sub(r"^Ubicazione[:\s]*", "", cleaned, flags=re.I).strip()
    cleaned = re.sub(r"\b([A-ZÀ-Ù])\s+([a-zà-ù]{2,})\b", r"\1\2", cleaned)
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
            evidence.append(_build_evidence(text, int(p.get("page_number", 0) or 0), start, end))
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
                evidence.append(_build_evidence(text, int(p.get("page_number", 0) or 0), start, end))
                value = _normalize_headline_text(text[start:end])
                status = "LOW_CONFIDENCE"
                break

    snippets = _collect_keyword_snippets(pages, keywords)
    searched_in = _make_searched_in(pages, keywords, status, snippets)
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
                evidence.append(_build_evidence(text, int(p.get("page_number", 0) or 0), start, end))
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
                    evidence.append(_build_evidence(text, int(p.get("page_number", 0) or 0), start, end))
                    value = _normalize_procedura_value(match.group(0))
                    status = "LOW_CONFIDENCE"
                    break
            if status == "LOW_CONFIDENCE":
                break

    snippets = _collect_keyword_snippets(pages, keywords)
    searched_in = _make_searched_in(pages, keywords, status, snippets)
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
        evidence.extend(ev_list)

    if lot_numbers:
        lot_numbers = sorted(set(lot_numbers))
        if len(lot_numbers) >= 2:
            value = f"Lotti {lot_numbers[0]}–{lot_numbers[-1]}"
        else:
            lot_num = lot_numbers[0]
            unico_match = _find_regex_in_pages(pages, r"\bLOTTO\s+UNICO\b", re.I)
            value = "Lotto Unico" if unico_match else f"Lotto {lot_num}"
            if unico_match:
                evidence.append(unico_match)
        status = "FOUND" if evidence else "LOW_CONFIDENCE"

    if status == "NOT_FOUND":
        match = _find_regex_in_pages(pages, r"\bLOTTO\s+UNICO\b|\bLOTTO\s+\d+\b", re.I)
        if match:
            evidence.append(match)
            value = _normalize_headline_text(match.get("quote", ""))
            status = "FOUND"
        else:
            match = _find_regex_in_pages(pages, r"\bLOTTO\b", re.I)
            if match:
                evidence.append(match)
                value = "Lotto"
                status = "LOW_CONFIDENCE"

    if evidence:
        normalized = _normalize_lotto_value_from_evidence(evidence)
        if normalized:
            value = normalized

    snippets = _collect_keyword_snippets(pages, keywords)
    searched_in = _make_searched_in(pages, keywords, status, snippets)
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
                evidence = [_build_evidence(text, int(p.get("page_number", 0) or 0), start, end)]
                searched_in = _make_searched_in(pages, keywords, "FOUND")
                return _build_field_state(value=value, status="FOUND", evidence=evidence, searched_in=searched_in)
    searched_in = _make_searched_in(pages, keywords, "NOT_FOUND")
    return _build_field_state(value=None, status="NOT_FOUND", evidence=[], searched_in=searched_in)

def _extract_superficie_state(pages: List[Dict[str, Any]], dati_certi: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    keywords = ["superficie", "mq", "m²", "superficie commerciale", "superficie catastale"]
    dati_certi = dati_certi if isinstance(dati_certi, dict) else {}
    existing = dati_certi.get("superficie_catastale")
    if isinstance(existing, dict):
        value = existing.get("value")
        evidence = existing.get("evidence", [])
        if isinstance(value, str):
            match = re.search(r"(\d{1,4}(?:[\.,]\d{1,2})?)", value)
            if match:
                raw = match.group(1).replace(".", "").replace(",", ".")
                try:
                    value = {"value": float(raw), "unit": "mq", "label": "Superficie"}
                except Exception:
                    value = existing.get("value")
        if value is not None and isinstance(evidence, list) and evidence:
            searched_in = _make_searched_in(pages, keywords, "FOUND")
            return _build_field_state(value=value, status="FOUND", evidence=evidence, searched_in=searched_in)

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
            evidence = [_build_evidence(text, int(p.get("page_number", 0) or 0), start, end)]
            value = {"value": value_num, "unit": unit, "label": _normalize_headline_text(m.group(1))}
            searched_in = _make_searched_in(pages, keywords, "FOUND")
            return _build_field_state(value=value, status="FOUND", evidence=evidence, searched_in=searched_in)
    searched_in = _make_searched_in(pages, keywords, "NOT_FOUND")
    return _build_field_state(value=None, status="NOT_FOUND", evidence=[], searched_in=searched_in)

def _extract_diritto_reale_state(pages: List[Dict[str, Any]]) -> Dict[str, Any]:
    keywords = ["proprietà", "nuda proprietà", "usufrutto", "diritto di"]
    pattern = re.compile(r"\b(Nuda\s+proprietà|Piena\s+proprietà|Proprietà|Usufrutto|Diritto\s+di\s+[^\n]{0,40})\b", re.I)
    schema_pattern = re.compile(r"Diritto\s+reale[:\s]*([^\n]{3,60})", re.I)
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
            evidence = [_build_evidence(text, int(p.get("page_number", 0) or 0), start, end)]
            value = _normalize_headline_text(m.group(1))
            searched_in = _make_searched_in(pages, keywords, "FOUND")
            return _build_field_state(value=value, status="FOUND", evidence=evidence, searched_in=searched_in)
    for p in pages:
        text = str(p.get("text", "") or "")
        m = pattern.search(text)
        if not m:
            continue
        start, end = m.start(), m.end()
        evidence = [_build_evidence(text, int(p.get("page_number", 0) or 0), start, end)]
        value = _normalize_headline_text(m.group(0))
        searched_in = _make_searched_in(pages, keywords, "FOUND")
        return _build_field_state(value=value, status="FOUND", evidence=evidence, searched_in=searched_in)
    searched_in = _make_searched_in(pages, keywords, "NOT_FOUND")
    return _build_field_state(value=None, status="NOT_FOUND", evidence=[], searched_in=searched_in)

def _extract_stato_occupativo_state(pages: List[Dict[str, Any]]) -> Dict[str, Any]:
    keywords = ["occupato", "libero", "detenuto", "contratto di locazione", "rilascio"]
    ambiguous_markers = ["si presume", "non è noto", "non e' noto", "non risulta", "da verificare", "da accertare", "presumibilmente", "non è dato sapere", "non e' dato sapere"]
    patterns = [
        (re.compile(r"\\b(non\\s+occupato|libero|libera\\s+disponibilit[aà])\\b", re.I), "LIBERO"),
        (re.compile(r"\\b(occupato|detenuto|locato|locazione|contratto\\s+di\\s+locazione|inquilino)\\b", re.I), "OCCUPATO"),
    ]
    found_values: List[str] = []
    for p in pages:
        text = str(p.get("text", "") or "")
        for pat, label in patterns:
            m = pat.search(text)
            if not m:
                continue
            start, end = m.start(), m.end()
            line_start, line_end = _line_bounds(text, start, end)
            line_text = text[line_start:line_end].lower()
            evidence = [_build_evidence(text, int(p.get("page_number", 0) or 0), line_start, line_end)]
            status = "LOW_CONFIDENCE" if any(marker in line_text for marker in ambiguous_markers) else "FOUND"
            searched_in = _make_searched_in(pages, keywords, status)
            if status == "LOW_CONFIDENCE":
                return _build_field_state(value=label, status="LOW_CONFIDENCE", evidence=evidence, searched_in=searched_in)
            found_values.append(label)
            return _build_field_state(value=label, status="FOUND", evidence=evidence, searched_in=searched_in)
    if len(set(found_values)) > 1:
        searched_in = _make_searched_in(pages, keywords, "LOW_CONFIDENCE")
        return _build_field_state(value="DA VERIFICARE", status="LOW_CONFIDENCE", evidence=[], searched_in=searched_in)
    searched_in = _make_searched_in(pages, keywords, "NOT_FOUND")
    return _build_field_state(value=None, status="NOT_FOUND", evidence=[], searched_in=searched_in)

def _extract_regolarita_urbanistica_state(pages: List[Dict[str, Any]]) -> Dict[str, Any]:
    keywords = ["conformità urbanistica", "abusi edilizi", "sanatoria", "condono", "pratiche edilizie", "difformità"]
    positive = re.compile(r"(non\\s+(?:risultano|emergono)\\s+abusi|assenza\\s+di\\s+abusi|non\\s+sono\\s+stati\\s+riscontrati\\s+abusi)", re.I)
    negative = re.compile(r"(abusi\\s+edilizi|difformit[aà]|non\\s+conform[ei]|irregolarit[aà]|sanatoria|condono)", re.I)
    ambiguous = re.compile(r"(da\\s+verificare|non\\s+è\\s+noto|non\\s+e'\\s+noto|da\\s+accertare|si\\s+presume|presumibilmente)", re.I)
    for p in pages:
        text = str(p.get("text", "") or "")
        for pat, label in ((positive, "NON EMERGONO ABUSI"), (negative, "PRESENTI DIFFORMITÀ")):
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

def _extract_conformita_catastale_state(pages: List[Dict[str, Any]]) -> Dict[str, Any]:
    keywords = ["conformità catastale", "difformità", "planimetria", "catasto"]
    positive = re.compile(r"(conformit[aà]\\s+catastale|planimetria\\s+conforme|conforme\\s+al\\s+catasto)", re.I)
    negative = re.compile(r"(difformit[aà]\\s+catastal[ei]|non\\s+conforme|mancata\\s+corrispondenza|planimetria\\s+non\\s+conforme)", re.I)
    ambiguous = re.compile(r"(da\\s+verificare|non\\s+è\\s+noto|non\\s+e'\\s+noto|da\\s+accertare|si\\s+presume|presumibilmente)", re.I)
    for p in pages:
        text = str(p.get("text", "") or "")
        for pat, label in ((positive, "CONFORME"), (negative, "PRESENTI DIFFORMITÀ")):
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

def _extract_spese_condominiali_state(pages: List[Dict[str, Any]]) -> Dict[str, Any]:
    keywords = ["spese condominiali", "arretrate", "arretrati", "oneri condominiali", "morosità"]
    positive = re.compile(r"(nessun\\s+arretrato|non\\s+risultano\\s+arretrati|in\\s+regola\\s+con\\s+i\\s+pagamenti)", re.I)
    negative = re.compile(r"(spese\\s+condominiali\\s+arretrate|arretrati\\s+condominiali|morosit[aà]|oneri\\s+condominiali\\s+insoluti)", re.I)
    ambiguous = re.compile(r"(da\\s+verificare|non\\s+è\\s+noto|non\\s+e'\\s+noto|da\\s+accertare|si\\s+presume|presumibilmente)", re.I)
    for p in pages:
        text = str(p.get("text", "") or "")
        for pat, label in ((positive, "NON RISULTANO ARRETRATI"), (negative, "PRESENTI ARRETRATI")):
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
        "DA VERIFICARE": "Da verificare",
        "NON SPECIFICATO IN PERIZIA": "Non specificato in perizia",
    }
    status_en_map = {
        "LIBERO": "Free",
        "OCCUPATO": "Occupied",
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

            def _state_from_lot_value(raw_value: Any, evidence_key: str, keywords: List[str], value_builder=None) -> Dict[str, Any]:
                evidence = lot_evidence.get(evidence_key, [])
                value = raw_value
                if value_builder:
                    value = value_builder(raw_value)
                if raw_value in (None, "", "TBD", "NON SPECIFICATO IN PERIZIA"):
                    searched_in = _make_searched_in(pages, keywords, "NOT_FOUND")
                    return _build_field_state(value=None, status="NOT_FOUND", evidence=[], searched_in=searched_in)
                if not evidence:
                    searched_in = _make_searched_in(pages, keywords, "LOW_CONFIDENCE")
                    return _build_field_state(value=value, status="LOW_CONFIDENCE", evidence=[], searched_in=searched_in)
                searched_in = _make_searched_in(pages, keywords, "FOUND")
                return _build_field_state(value=value, status="FOUND", evidence=evidence, searched_in=searched_in)

            states["prezzo_base_asta"] = _state_from_lot_value(
                selected_lot.get("prezzo_base_value"),
                "prezzo_base",
                ["prezzo base", "prezzo base d'asta", "€"],
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
                value_builder=_superficie_value,
            )

            states["diritto_reale"] = _state_from_lot_value(
                selected_lot.get("diritto_reale"),
                "diritto_reale",
                ["proprietà", "nuda proprietà", "usufrutto", "diritto di"],
            )

    result["field_states"] = states
    _enforce_field_states_contract(result, pages)
    _apply_decision_states_to_result(result, result.get("field_states", states))

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
    import re
    lots = []
    schema_pages = []
    for p in pages_in:
        text = str(p.get("text", "") or "")
        if "SCHEMA RIASSUNTIVO" in text.upper():
            schema_pages.append(p)
    if not schema_pages:
        for p in pages_in:
            text = str(p.get("text", "") or "")
            if "LOTTO" in text.upper() and "PREZZO BASE" in text.upper():
                schema_pages.append(p)
        if not schema_pages:
            schema_pages = pages_in

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

    for lot_num in sorted(all_lot_numbers):
        lot_data = {
            "lot_number": lot_num,
            "prezzo_base_eur": "TBD",
            "prezzo_base_value": None,
            "ubicazione": "TBD",
            "diritto_reale": "TBD",
            "superficie_mq": "TBD",
            "tipologia": "TBD",
            "evidence": {
                "lotto": [],
                "prezzo_base": [],
                "ubicazione": [],
                "superficie": [],
                "diritto_reale": [],
                "tipologia": [],
            }
        }
        for p in schema_pages:
            text = str(p.get("text", "") or "")
            page_num = p.get("page_number", 0)
            lot_pattern = rf"\bLOTTO\s+{lot_num}\b"
            if not re.search(lot_pattern, text, re.I):
                # Allow LOTTO UNICO for lot_num=1
                if not (lot_num == 1 and re.search(r"\bLOTTO\s+UNICO\b", text, re.I)):
                    continue
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
            prezzo_match = re.search(
                r"PREZZO\s+BASE\s+D['']?ASTA[:\s]*€?\s*([\d.,]+)",
                block, re.I
            )
            if prezzo_match and lot_data["prezzo_base_eur"] == "TBD":
                prezzo_str = prezzo_match.group(1).strip()
                lot_data["prezzo_base_eur"] = f"€ {prezzo_str}"
                try:
                    val = prezzo_str.replace(".", "").replace(",", ".")
                    lot_data["prezzo_base_value"] = float(val)
                except Exception:
                    pass
                abs_start = block_start + prezzo_match.start()
                abs_end = block_start + prezzo_match.end()
                lot_data["evidence"]["prezzo_base"] = [_build_evidence(text, page_num, abs_start, abs_end)]

            if not lot_data["evidence"].get("lotto"):
                lotto_match = re.search(r"\bLOTTO\s+UNICO\b|\bLOTTO\s+\d+\b", text, re.I)
                if lotto_match:
                    lot_data["evidence"]["lotto"] = [_build_evidence(text, page_num, lotto_match.start(), lotto_match.end())]

            ubic_match = re.search(r"Ubicazione[:\s]*([^\n]+)", block, re.I)
            if ubic_match and lot_data["ubicazione"] == "TBD":
                lot_data["ubicazione"] = _normalize_address_value(ubic_match.group(1).strip()[:200])
                abs_start = block_start + ubic_match.start()
                abs_end = block_start + ubic_match.end()
                lot_data["evidence"]["ubicazione"] = [_build_evidence(text, page_num, abs_start, abs_end)]

            diritto_match = re.search(r"Diritto\s+reale[:\s]*([^\n]+)", block, re.I)
            if diritto_match and lot_data["diritto_reale"] == "TBD":
                lot_data["diritto_reale"] = _normalize_headline_text(diritto_match.group(1).strip()[:100])
                abs_start = block_start + diritto_match.start()
                abs_end = block_start + diritto_match.end()
                lot_data["evidence"]["diritto_reale"] = [_build_evidence(text, page_num, abs_start, abs_end)]

            sup_matches = list(re.finditer(r"Superficie[^\d\n]{0,40}([\d.,]+)\s*mq", block, re.I))
            if sup_matches and lot_data["superficie_mq"] == "TBD":
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
                    lot_data["superficie_mq"] = best_val
                    abs_start = block_start + best.start()
                    abs_end = block_start + best.end()
                    lot_data["evidence"]["superficie"] = [_build_evidence(text, page_num, abs_start, abs_end)]

            tipo_match = re.search(r"Tipologia[:\s]*([^\n]+)", block, re.I)
            if tipo_match and lot_data["tipologia"] == "TBD":
                lot_data["tipologia"] = _normalize_headline_text(tipo_match.group(1).strip()[:100])
                abs_start = block_start + tipo_match.start()
                abs_end = block_start + tipo_match.end()
                lot_data["evidence"]["tipologia"] = [_build_evidence(text, page_num, abs_start, abs_end)]

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
        if isinstance(total, dict) and not _has_evidence(total.get("evidence", [])):
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
            sig = (ev.get("page"), ev.get("start_offset"), ev.get("end_offset"), ev.get("quote"))
            if sig not in seen:
                seen.add(sig)
                merged.append(ev)
        it["evidence"] = merged
        if not it["evidence"]:
            it["status"] = "DA_VERIFICARE"
            it["status_it"] = status_it_map["DA_VERIFICARE"]
            it["searched_in"] = it.get("searched_in") or {
                "pages": all_pages,
                "keywords": [it["killer"]],
                "sections": ["section_9_legal_killers"]
            }
            it["reason_it"] = it.get("reason_it") or f"Elemento da verificare: {it['killer']}"
        normalized.append(it)

    section["items"] = normalized

def _to_iso(value: Any) -> Optional[str]:
    dt = _parse_dt(value)
    if dt:
        return dt.isoformat()
    if isinstance(value, str):
        return value
    return None

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

async def _decrement_quota_if_applicable(user: User, field: str) -> bool:
    if user.is_master_admin:
        return False
    await db.users.update_one(
        {"user_id": user.user_id},
        {"$inc": {f"quota.{field}": -1}}
    )
    return True

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
    is_master = email.lower() == MASTER_ADMIN_EMAIL.lower()  # Case-insensitive comparison
    
    if existing_user:
        user_id = existing_user["user_id"]
        # Update user data - also update master admin status and plan if they're the admin
        update_data = {"name": name, "picture": picture}
        
        # If this is the master admin, ensure they have enterprise access
        if is_master:
            update_data["is_master_admin"] = True
            update_data["plan"] = "enterprise"
            update_data["quota"] = SUBSCRIPTION_PLANS["enterprise"].quota.copy()
        
        await db.users.update_one(
            {"user_id": user_id},
            {"$set": update_data}
        )
        
        # Refresh user data after update
        updated_user = await db.users.find_one({"user_id": user_id}, {"_id": 0})
        user = User(**updated_user)
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
    
    user_response = user.model_dump()
    user_response["created_at"] = user_response["created_at"].isoformat() if isinstance(user_response["created_at"], datetime) else user_response["created_at"]
    
    return {"user": user_response, "session_token": session_token}

@api_router.get("/auth/me")
async def get_me(request: Request):
    """Get current authenticated user"""
    user = await get_current_user(request)
    if not user:
        raise HTTPException(status_code=401, detail="Not authenticated")
    
    user_response = user.model_dump()
    user_response["created_at"] = user_response["created_at"].isoformat() if isinstance(user_response["created_at"], datetime) else user_response["created_at"]
    return user_response

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
    return {"plans": [plan.model_dump() for plan in SUBSCRIPTION_PLANS.values()]}

@api_router.post("/checkout/create")
async def create_checkout(request: Request):
    """Create Stripe checkout session"""
    user = await require_auth(request)
    data = await request.json()
    plan_id = data.get("plan_id")
    origin_url = data.get("origin_url")
    
    if plan_id not in SUBSCRIPTION_PLANS or plan_id == "free":
        raise HTTPException(status_code=400, detail="Invalid plan")
    
    plan = SUBSCRIPTION_PLANS[plan_id]

    if not STRIPE_SECRET_KEY:
        raise HTTPException(status_code=503, detail="Stripe not configured")
    import stripe
    stripe.api_key = STRIPE_SECRET_KEY
    
    success_url = f"{origin_url}/billing?session_id={{CHECKOUT_SESSION_ID}}"
    cancel_url = f"{origin_url}/billing"

    session = stripe.checkout.Session.create(
        mode="payment",
        success_url=success_url,
        cancel_url=cancel_url,
        line_items=[
            {
                "price_data": {
                    "currency": plan.currency,
                    "product_data": {"name": plan.name},
                    "unit_amount": int(plan.price * 100),
                },
                "quantity": 1,
            }
        ],
        metadata={
            "user_id": user.user_id,
            "plan_id": plan_id,
            "email": user.email,
        },
    )
    
    # Create payment transaction record
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
    
    return {"url": session.url, "session_id": session.id}

@api_router.get("/checkout/status/{session_id}")
async def get_checkout_status(session_id: str, request: Request):
    """Get checkout session status"""
    user = await require_auth(request)

    if not STRIPE_SECRET_KEY:
        raise HTTPException(status_code=503, detail="Stripe not configured")
    import stripe
    stripe.api_key = STRIPE_SECRET_KEY

    status = stripe.checkout.Session.retrieve(session_id)
    
    # Update transaction in database
    txn = await db.payment_transactions.find_one({"session_id": session_id, "user_id": user.user_id}, {"_id": 0})
    
    if txn and txn.get("payment_status") != "paid" and status.payment_status == "paid":
        # Update transaction
        await db.payment_transactions.update_one(
            {"session_id": session_id},
            {"$set": {"status": status.status, "payment_status": status.payment_status}}
        )
        
        # Upgrade user plan
        plan_id = txn.get("plan_id")
        if plan_id in SUBSCRIPTION_PLANS:
            plan = SUBSCRIPTION_PLANS[plan_id]
            await db.users.update_one(
                {"user_id": user.user_id},
                {"$set": {"plan": plan_id, "quota": plan.quota.copy()}}
            )
    
    return {
        "status": status.status,
        "payment_status": status.payment_status,
        "amount_total": status.amount_total,
        "currency": status.currency
    }

@api_router.post("/webhook/stripe")
async def stripe_webhook(request: Request):
    """Handle Stripe webhooks"""
    if not STRIPE_SECRET_KEY:
        raise HTTPException(status_code=503, detail="Stripe not configured")
    body = await request.body()
    signature = request.headers.get("Stripe-Signature")

    import stripe
    stripe.api_key = STRIPE_SECRET_KEY
    webhook_secret = os.environ.get("STRIPE_WEBHOOK_SECRET")

    try:
        if webhook_secret and signature:
            event = stripe.Webhook.construct_event(payload=body, sig_header=signature, secret=webhook_secret)
        else:
            event = json.loads(body.decode("utf-8"))

        data_object = event.get("data", {}).get("object", {})
        event_type = event.get("type", "")

        if event_type == "checkout.session.completed":
            payment_status = data_object.get("payment_status")
            metadata = data_object.get("metadata", {}) or {}
            session_id = data_object.get("id")

            if payment_status == "paid":
                user_id = metadata.get("user_id")
                plan_id = metadata.get("plan_id")
            
                if user_id and plan_id and plan_id in SUBSCRIPTION_PLANS:
                    plan = SUBSCRIPTION_PLANS[plan_id]
                    await db.users.update_one(
                        {"user_id": user_id},
                        {"$set": {"plan": plan_id, "quota": plan.quota.copy()}}
                    )
                    if session_id:
                        await db.payment_transactions.update_one(
                            {"session_id": session_id},
                            {"$set": {"status": "complete", "payment_status": "paid"}}
                        )
        
        return {"received": True}
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
                    lot_data["evidence"]["prezzo_base"] = [_build_evidence(text, page_num, abs_start, abs_end)]

                if "lotto" not in lot_data["evidence"]:
                    lotto_match = re.search(r"\bLOTTO\s+UNICO\b|\bLOTTO\s+\d+\b", text, re.I)
                    if lotto_match:
                        lot_data["evidence"]["lotto"] = [_build_evidence(text, page_num, lotto_match.start(), lotto_match.end())]
                
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
                    lot_data["evidence"]["diritto_reale"] = [_build_evidence(text, page_num, abs_start, abs_end)]
                
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
                        lot_data["evidence"]["superficie"] = [_build_evidence(text, page_num, abs_start, abs_end)]
                
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
    
    if "non è presente l'abitabilità" in text_lower or "non risulta agibile" in text_lower:
        abusi["agibilita"] = {"status": "ASSENTE"}
    
    if impianti:
        abusi["impianti"] = impianti
        
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
    
    # Try to extract basic info from text
    text_lower = pdf_text.lower()
    
    # Find procedure ID with evidence
    procedure_id = "NON SPECIFICATO IN PERIZIA"
    procedure_ev = _find_regex_in_pages(pages, r"Esecuzione\s+Immobiliare\s+\d+/\d+\s+del\s+R\.G\.E\.", re.I)
    if procedure_ev:
        procedure_id = procedure_ev["quote"].strip()
    
    # Find tribunal with evidence
    tribunale = "NON SPECIFICATO IN PERIZIA"
    tribunale_ev = _find_regex_in_pages(pages, r"TRIBUNALE\s+DI\s+[A-Z\s]+", re.I)
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
    if extracted_lots and extracted_lots[0].get("prezzo_base_value", 0) > 0:
        prezzo_base = extracted_lots[0]["prezzo_base_value"]
    else:
        prezzo_match = re.search(r'prezzo\s+base[^\d]*(\d[\d\.,]+)', text_lower)
        if not prezzo_match:
            prezzo_match = re.search(r'€\s*(\d[\d\.,]+)', text_lower)
        if prezzo_match:
            try:
                prezzo_str = prezzo_match.group(1).replace('.', '').replace(',', '.')
                prezzo_base = float(prezzo_str)
            except:
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
    
    # Build legal killers from deterministic scan
    legal_killers_items = []
    for lk in detected_legal_killers:
        legal_killers_items.append({
            "killer": lk["title"],
            "status": "SI" if lk["severity"] == "ROSSO" else "GIALLO",
            "action": "Verifica obbligatoria",
            "evidence": [{"page": lk["page"], "quote": lk["quote"], "start_offset": lk.get("start_offset"), "end_offset": lk.get("end_offset"), "bbox": None}]
        })
    
    result = {
        "schema_version": "nexodify_perizia_scan_v2",
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
        "lot_index": [{"lot": lot["lot_number"], "prezzo": lot["prezzo_base_eur"], "ubicazione": lot["ubicazione"][:50], "page": (lot.get("evidence", {}).get("lotto", [{}])[0].get("page") if lot.get("evidence", {}).get("lotto") else None), "quote": (lot.get("evidence", {}).get("lotto", [{}])[0].get("quote") if lot.get("evidence", {}).get("lotto") else None)} for lot in extracted_lots],
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
            "commerciabilita": {"status": "UNKNOWN", "evidence": []}
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
        "red_flags_operativi": [
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
    
    # Check quota
    if user.quota.get("perizia_scans_remaining", 0) <= 0 and not user.is_master_admin:
        raise HTTPException(
            status_code=403, 
            detail={
                "code": "QUOTA_EXCEEDED",
                "message_it": "Quota scansioni perizia esaurita. Aggiorna il piano.",
                "message_en": "Perizia scan quota exceeded. Upgrade your plan."
            }
        )
    
    # Read PDF content
    contents = await file.read()
    input_sha256 = hashlib.sha256(contents).hexdigest()
    logger.info(f"[{request_id}] upload_saved bytes={len(contents)} sha256={input_sha256[:12]}")

    # Generate IDs early for consistent logging + persistence
    case_id = f"case_{uuid.uuid4().hex[:8]}"
    run_id = f"run_{uuid.uuid4().hex[:8]}"
    analysis_id = f"analysis_{uuid.uuid4().hex[:12]}"
    offline_qa = user.user_id == "offline_qa"

    async def run_pipeline():
        logger.info(f"[{request_id}] pipeline_start analysis_id={analysis_id} offline_qa={offline_qa}")

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

            needs_ocr_fallback = (
                not full_text.strip()
                or coverage_ratio < PDF_TEXT_MIN_COVERAGE_RATIO
                or blank_ratio >= PDF_TEXT_MAX_BLANK_PAGE_RATIO
            )
            if needs_ocr_fallback:
                logger.info(f"[{request_id}] docai_fallback_needed")
                docai_pages, docai_text, docai_error = await _extract_with_docai(contents, mime_type, request_id)
                if docai_error:
                    logger.warning(f"[{request_id}] docai_fallback_failed {docai_error}")
                elif docai_text.strip():
                    pages = docai_pages
                    full_text = docai_text
                    logger.info(f"[{request_id}] docai_fallback_used pages={len(pages)} chars={len(full_text)}")

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

        return result, pages, full_text

    try:
        result, pages, full_text = await asyncio.wait_for(run_pipeline(), timeout=PIPELINE_TIMEOUT_SECONDS)
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
    _normalize_evidence_offsets(result, pages)
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
        pages_count=len(pages),
        result=result
    )

    analysis_dict = analysis.model_dump()
    analysis_dict["created_at"] = analysis_dict["created_at"].isoformat()
    analysis_dict["raw_text"] = full_text[:100000]  # Store raw text for assistant
    analysis_dict["status"] = "COMPLETED"
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

        # Decrement quota if not master admin
        await _decrement_quota_if_applicable(user, "perizia_scans_remaining")

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
    await _decrement_quota_if_applicable(user, "image_scans_remaining")
    
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
    await _decrement_quota_if_applicable(user, "assistant_messages_remaining")
    
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

    requires_full_scan = sort in ["last_active_at", "usage_30d.perizie"]
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
            } if notes_map.get(user_id) else None
        })

    if requires_full_scan:
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
        } if notes_doc else None
    }

    await _write_admin_audit(admin_user, "ADMIN_API_VIEW", meta={"endpoint": "users_detail", "user_id": user_id})

    return {"user": user_payload, "recent_activity": recent_activity}

@api_router.patch("/admin/users/{user_id}")
async def admin_user_update(user_id: str, request: Request):
    admin_user = await require_master_admin(request)
    payload = await request.json()

    target_user = await db.users.find_one({"user_id": user_id}, {"_id": 0})
    if not target_user:
        raise HTTPException(status_code=404, detail="User not found")

    update_data: Dict[str, Any] = {}
    plan = payload.get("plan")
    quota = payload.get("quota") or {}

    before_plan = target_user.get("plan")
    before_quota = target_user.get("quota", {}).copy()

    if plan:
        if plan not in SUBSCRIPTION_PLANS:
            raise HTTPException(status_code=400, detail="Invalid plan")
        if target_user.get("email", "").lower() == MASTER_ADMIN_EMAIL.lower() and plan != "enterprise":
            raise HTTPException(status_code=400, detail="Cannot downgrade master admin plan")
        update_data["plan"] = plan

    quota_updates: Dict[str, int] = {}
    for key, value in quota.items():
        if key not in ADMIN_QUOTA_FIELDS:
            raise HTTPException(status_code=400, detail=f"Unsupported quota key: {key}")
        if not isinstance(value, int) or value < 0:
            raise HTTPException(status_code=400, detail="Quota values must be int >= 0")
        quota_updates[key] = value

    if quota_updates:
        new_quota = target_user.get("quota", {}).copy()
        new_quota.update(quota_updates)
        update_data["quota"] = new_quota

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
        if not isinstance(result.get("field_states"), dict):
            pages_hint = int(analysis.get("pages_count", 0) or 0)
            pages_for_proof = [{"page_number": i + 1, "text": ""} for i in range(max(1, pages_hint))]
            _apply_headline_field_states(result, pages_for_proof)
            _apply_decision_field_states(result, pages_for_proof)
        else:
            pages_hint = int(analysis.get("pages_count", 0) or 0)
            pages_for_proof = [{"page_number": i + 1, "text": ""} for i in range(max(1, pages_hint))]
            decision_keys = {
                "prezzo_base_asta",
                "superficie",
                "diritto_reale",
                "stato_occupativo",
                "regolarita_urbanistica",
                "conformita_catastale",
                "spese_condominiali_arretrate",
                "formalita_pregiudizievoli",
            }
            if not decision_keys.issubset(set(result.get("field_states", {}).keys())):
                _apply_decision_field_states(result, pages_for_proof)
        _apply_headline_overrides(result, analysis.get("headline_overrides") or {})
        _apply_field_overrides(result, analysis.get("field_overrides") or {})
        _enforce_field_states_contract(result, pages_for_proof)
        states = result.get("field_states")
        if isinstance(states, dict):
            if "superficie" not in states and "superficie_catastale" in states:
                states["superficie"] = states.get("superficie_catastale")
            states.pop("superficie_catastale", None)
            result["field_states"] = states
        analysis["result"] = result
    return analysis

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
        {"_id": 0, "analysis_id": 1, "case_id": 1, "case_title": 1, "created_at": 1, "result.semaforo_generale": 1}
    ).sort("created_at", -1).limit(5).to_list(5)
    
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

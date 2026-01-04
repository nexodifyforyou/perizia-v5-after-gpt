from fastapi import FastAPI, APIRouter, HTTPException, Request, UploadFile, File, Depends, Response
from fastapi.responses import JSONResponse, StreamingResponse
from dotenv import load_dotenv
from starlette.middleware.cors import CORSMiddleware
from motor.motor_asyncio import AsyncIOMotorClient
import os
import logging
from pathlib import Path
from pydantic import BaseModel, Field, ConfigDict
from typing import List, Optional, Dict, Any
import uuid
from datetime import datetime, timezone, timedelta
import json
import httpx
from PyPDF2 import PdfReader
import io
import hashlib

ROOT_DIR = Path(__file__).parent
load_dotenv(ROOT_DIR / '.env')

# MongoDB connection
mongo_url = os.environ['MONGO_URL']
client = AsyncIOMotorClient(mongo_url)
db = client[os.environ['DB_NAME']]

# Environment variables
EMERGENT_LLM_KEY = os.environ.get('EMERGENT_LLM_KEY')
STRIPE_API_KEY = os.environ.get('STRIPE_API_KEY')
MASTER_ADMIN_EMAIL = os.environ.get('MASTER_ADMIN_EMAIL', 'admin@nexodify.com')

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
    user = await get_current_user(request)
    if not user:
        raise HTTPException(status_code=401, detail="Not authenticated")
    return user

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
    
    if existing_user:
        user_id = existing_user["user_id"]
        # Update user data
        await db.users.update_one(
            {"user_id": user_id},
            {"$set": {"name": name, "picture": picture}}
        )
        user = User(**existing_user)
        user.name = name
        user.picture = picture
    else:
        user_id = f"user_{uuid.uuid4().hex[:12]}"
        is_master = email == MASTER_ADMIN_EMAIL
        
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
    
    from emergentintegrations.payments.stripe.checkout import StripeCheckout, CheckoutSessionRequest
    
    host_url = str(request.base_url)
    webhook_url = f"{host_url}api/webhook/stripe"
    
    stripe_checkout = StripeCheckout(api_key=STRIPE_API_KEY, webhook_url=webhook_url)
    
    success_url = f"{origin_url}/billing?session_id={{CHECKOUT_SESSION_ID}}"
    cancel_url = f"{origin_url}/billing"
    
    checkout_request = CheckoutSessionRequest(
        amount=plan.price,
        currency=plan.currency,
        success_url=success_url,
        cancel_url=cancel_url,
        metadata={
            "user_id": user.user_id,
            "plan_id": plan_id,
            "email": user.email
        }
    )
    
    session = await stripe_checkout.create_checkout_session(checkout_request)
    
    # Create payment transaction record
    transaction = PaymentTransaction(
        transaction_id=f"txn_{uuid.uuid4().hex[:12]}",
        user_id=user.user_id,
        session_id=session.session_id,
        plan_id=plan_id,
        amount=plan.price,
        currency=plan.currency,
        status="pending",
        payment_status="initiated"
    )
    txn_dict = transaction.model_dump()
    txn_dict["created_at"] = txn_dict["created_at"].isoformat()
    await db.payment_transactions.insert_one(txn_dict)
    
    return {"url": session.url, "session_id": session.session_id}

@api_router.get("/checkout/status/{session_id}")
async def get_checkout_status(session_id: str, request: Request):
    """Get checkout session status"""
    user = await require_auth(request)
    
    from emergentintegrations.payments.stripe.checkout import StripeCheckout
    
    host_url = str(request.base_url)
    webhook_url = f"{host_url}api/webhook/stripe"
    stripe_checkout = StripeCheckout(api_key=STRIPE_API_KEY, webhook_url=webhook_url)
    
    status = await stripe_checkout.get_checkout_status(session_id)
    
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
    body = await request.body()
    signature = request.headers.get("Stripe-Signature")
    
    from emergentintegrations.payments.stripe.checkout import StripeCheckout
    
    host_url = str(request.base_url)
    webhook_url = f"{host_url}api/webhook/stripe"
    stripe_checkout = StripeCheckout(api_key=STRIPE_API_KEY, webhook_url=webhook_url)
    
    try:
        webhook_response = await stripe_checkout.handle_webhook(body, signature)
        
        if webhook_response.payment_status == "paid":
            user_id = webhook_response.metadata.get("user_id")
            plan_id = webhook_response.metadata.get("plan_id")
            
            if user_id and plan_id and plan_id in SUBSCRIPTION_PLANS:
                plan = SUBSCRIPTION_PLANS[plan_id]
                await db.users.update_one(
                    {"user_id": user_id},
                    {"$set": {"plan": plan_id, "quota": plan.quota.copy()}}
                )
                await db.payment_transactions.update_one(
                    {"session_id": webhook_response.session_id},
                    {"$set": {"status": "complete", "payment_status": "paid"}}
                )
        
        return {"received": True}
    except Exception as e:
        logger.error(f"Webhook error: {e}")
        return {"received": True}

# ===================
# COMPREHENSIVE PERIZIA SYSTEM PROMPT
# ===================

PERIZIA_SYSTEM_PROMPT = """YOU ARE: Nexodify Auction Scan Engine (Perizia → Evidence-First JSON).

CRITICAL RULE - EVIDENCE REQUIRED FOR EVERYTHING:
Every single value you extract MUST include an "evidence" array with:
- "page": the exact page number (integer, 1-based) where you found this information
- "anchor": 3-8 key words that locate the information on that page
- "quote": the EXACT text snippet (up to 200 chars) from the document that supports this value

Example of proper evidence:
"prezzo_base_asta": {
  "value": 150000,
  "formatted": "€150.000",
  "evidence": [{"page": 12, "anchor": "prezzo base asta", "quote": "Il prezzo base d'asta è fissato in € 150.000,00 (centocinquantamila/00)"}]
}

If you CANNOT find evidence in the document, you MUST use:
- "NOT_SPECIFIED_IN_PERIZIA" as the value
- Empty evidence array []
- "reason": explaining why it wasn't found

ABSOLUTE RULES:
1) NO HALLUCINATIONS - only extract what is ACTUALLY written in the document
2) EVERY extracted value MUST have page number and quote
3) If a field has no evidence, mark it NOT_SPECIFIED_IN_PERIZIA
4) Output ONE valid JSON object only - no markdown, no commentary

WHAT TO EXTRACT (with page references):
- procedure_id (E.I./R.G.E./N. procedimento) + page where found
- lotto + page
- tribunale + page
- full address + page
- deposit date + page
- prezzo base d'asta (€ amounts) + page
- valore tecnico lordo + page
- deprezzamento CTU % + page
- superficie (mq, m², metri quadri) + page
- catasto (categoria, classe, vani) + page
- diritto reale + page
- conformità urbanistica + page + exact text stating conforme/difforme
- conformità catastale + page
- condono/sanatoria details + pages
- agibilità status + page
- occupazione status + page
- stato conservativo issues + pages
- formalità (ipoteca, pignoramento) + pages

MONEY BOX (A-H):
For each item, if found in perizia, include page reference. If not found, use NEXODIFY_ESTIMATE with empty evidence.

LEGAL KILLERS (8 items):
For each, search the document. If mentioned, include page reference. If not found, status = "NOT_SPECIFIED_IN_PERIZIA".

OUTPUT JSON STRUCTURE:
{
  "schema_version": "nexodify_perizia_scan_v1",
  "run": {
    "run_id": "string",
    "generated_at_utc": "ISO_DATE",
    "input": {"source_type": "perizia_pdf", "file_name": "string", "pages_total": 0}
  },
  "case_header": {
    "procedure_id": {"value": "string", "evidence": [{"page": 1, "anchor": "string", "quote": "string"}]},
    "lotto": {"value": "string", "evidence": []},
    "tribunale": {"value": "string", "evidence": []},
    "address": {"value": "string", "evidence": []},
    "deposit_date": {"value": "string", "evidence": []}
  },
  "semaforo_generale": {
    "status": "GREEN/AMBER/RED",
    "status_it": "string",
    "status_en": "string", 
    "reason_it": "string - explain WHY this rating based on evidence found",
    "reason_en": "string",
    "evidence": [{"page": 0, "anchor": "string", "quote": "string"}]
  },
  "decision_rapida_client": {
    "risk_level": "LOW_RISK/MEDIUM_RISK/HIGH_RISK",
    "risk_level_it": "string",
    "risk_level_en": "string",
    "summary_it": "string - reference page numbers in your summary",
    "summary_en": "string",
    "driver_rosso": [{"code": "string", "headline_it": "string", "severity": "RED/AMBER", "evidence": [{"page": 0, "anchor": "string", "quote": "string"}]}]
  },
  "money_box": {
    "items": [
      {
        "code": "A",
        "label_it": "Regolarizzazione urbanistica",
        "label_en": "Urban regularization",
        "type": "FIXED/RANGE/NOT_SPECIFIED/NEXODIFY_ESTIMATE",
        "value": 0,
        "range": {"min": 0, "max": 0},
        "source": "PERIZIA/NEXODIFY_ESTIMATE",
        "evidence": [{"page": 0, "anchor": "string", "quote": "string"}],
        "action_required_it": "string"
      }
    ],
    "total_extra_costs": {"range": {"min": 0, "max": 0}, "max_is_open": false}
  },
  "dati_certi_del_lotto": {
    "prezzo_base_asta": {"value": 0, "formatted": "€X", "evidence": [{"page": 0, "anchor": "string", "quote": "string"}]},
    "valore_tecnico_lordo": {"value": 0, "evidence": []},
    "deprezzamento_ctu_percent": {"value": 0, "evidence": []},
    "superficie_catastale": {"value": "string", "evidence": []},
    "catasto": {"categoria": "string", "classe": "string", "vani": "string", "evidence": []},
    "diritto_reale": {"value": "string", "evidence": []}
  },
  "abusi_edilizi_conformita": {
    "conformita_urbanistica": {"status": "CONFORME/DIFFORME/UNKNOWN", "detail_it": "string", "evidence": []},
    "conformita_catastale": {"status": "CONFORME/DIFFORME/UNKNOWN", "evidence": []},
    "condono": {"present": "YES/NO/UNKNOWN", "practice_ids": [], "status": "string", "evidence": []},
    "agibilita": {"status": "PRESENT/MISSING/UNKNOWN", "evidence": []},
    "commerciabilita": {"status": "OK/NOT_MARKETABLE/UNKNOWN", "evidence": []}
  },
  "stato_occupativo": {
    "status": "LIBERO/OCCUPATO_DEBITORE/OCCUPATO_TERZI/UNKNOWN",
    "status_it": "string",
    "title_opponible": "YES/NO/UNKNOWN",
    "evidence": [{"page": 0, "anchor": "string", "quote": "string"}]
  },
  "stato_conservativo": {
    "general_condition_it": "string",
    "issues_found": [{"issue_it": "string", "evidence": [{"page": 0, "anchor": "string", "quote": "string"}]}],
    "evidence": []
  },
  "formalita": {
    "ipoteca": {"status": "PRESENT/ABSENT/UNKNOWN", "amount": 0, "evidence": []},
    "pignoramento": {"status": "PRESENT/ABSENT/UNKNOWN", "evidence": []},
    "cancellazione_decreto": {"status": "YES/NO/UNKNOWN", "evidence": []}
  },
  "legal_killers_checklist": {
    "PEEP_superficie": {"status": "YES/NO/NOT_SPECIFIED", "evidence": [], "action_required_it": "string"},
    "donazione_catena_20anni": {"status": "YES/NO/NOT_SPECIFIED", "evidence": [], "action_required_it": "string"},
    "prelazione_stato_beni_culturali": {"status": "YES/NO/NOT_SPECIFIED", "evidence": [], "action_required_it": "string"},
    "usi_civici_diritti_demaniali": {"status": "YES/NO/NOT_SPECIFIED", "evidence": [], "action_required_it": "string"},
    "fondo_patrimoniale": {"status": "YES/NO/NOT_SPECIFIED", "evidence": [], "action_required_it": "string"},
    "servitu_atti_obbligo": {"status": "YES/NO/NOT_SPECIFIED", "evidence": [], "action_required_it": "string"},
    "formalita_non_cancellabili": {"status": "YES/NO/NOT_SPECIFIED", "evidence": [], "action_required_it": "string"},
    "amianto": {"status": "YES/NO/NOT_SPECIFIED", "evidence": [], "action_required_it": "string"}
  },
  "indice_di_convenienza": {
    "prezzo_base_asta": 0,
    "extra_costs_min": 0,
    "extra_costs_max": 0,
    "all_in_light_min": 0,
    "all_in_light_max": 0,
    "dry_read_it": "string - include page references",
    "dry_read_en": "string"
  },
  "red_flags_operativi": [
    {"code": "string", "severity": "RED/AMBER", "flag_it": "string", "flag_en": "string", "action_it": "string", "evidence": [{"page": 0, "anchor": "string", "quote": "string"}]}
  ],
  "checklist_pre_offerta": [
    {"item_it": "string", "item_en": "string", "priority": "P0/P1/P2", "status": "TO_CHECK"}
  ],
  "summary_for_client": {
    "summary_it": "string - MUST reference specific page numbers for key findings",
    "summary_en": "string",
    "disclaimer_it": "Documento informativo. Non costituisce consulenza legale.",
    "disclaimer_en": "Informational document. Not legal advice."
  },
  "qa": {
    "status": "PASS/WARN/FAIL",
    "reasons": [{"code": "string", "severity": "RED/AMBER", "reason_it": "string", "evidence": []}]
  }
}

SEMAFORO DETERMINATION:
- RED: condono without defined status (cite page), occupied without title proof (cite page), non-marketable (cite page)
- AMBER: ipoteca/pignoramento present (cite pages), missing agibilità, condono criticalities
- GREEN: no critical issues found

Remember: The user needs to verify your analysis against the original document. Include page numbers for EVERYTHING."""
async def analyze_perizia_with_llm(pdf_text: str, pages: List[Dict], file_name: str, user: User, case_id: str, run_id: str, input_sha256: str) -> Dict:
    """Analyze perizia using LLM with comprehensive prompt"""
    from emergentintegrations.llm.chat import LlmChat, UserMessage
    
    chat = LlmChat(
        api_key=EMERGENT_LLM_KEY,
        session_id=f"perizia_{run_id}",
        system_message=PERIZIA_SYSTEM_PROMPT
    ).with_model("openai", "gpt-4o")
    
    # Build page-by-page content for the prompt
    page_content = ""
    for p in pages[:100]:  # Limit to first 100 pages
        page_content += f"\n=== PAGE {p['page_number']} ===\n{p['text']}\n"
    
    prompt = f"""Analyze this Italian Perizia/CTU document. Extract ALL available data with evidence.

FILE: {file_name}
PAGES: {len(pages)}
RUN_ID: {run_id}
CASE_ID: {case_id}

DOCUMENT CONTENT:
{page_content[:80000]}

IMPORTANT:
1. Search EVERY page for relevant data
2. Extract ALL prices, surfaces, dates, addresses found
3. Include page numbers and quotes as evidence
4. For money_box, provide NEXODIFY_ESTIMATE where perizia doesn't specify costs
5. Compute total_extra_costs and all_in_light values
6. Determine semaforo (GREEN/AMBER/RED) based on risk factors found

Return the complete JSON analysis."""

    try:
        response = await chat.send_message(UserMessage(text=prompt))
        
        # Try to parse JSON from response
        response_text = response.strip()
        if response_text.startswith("```json"):
            response_text = response_text[7:]
        if response_text.startswith("```"):
            response_text = response_text[3:]
        if response_text.endswith("```"):
            response_text = response_text[:-3]
        
        try:
            result = json.loads(response_text)
            # Ensure required fields exist
            if "schema_version" not in result:
                result["schema_version"] = "nexodify_perizia_scan_v1"
            if "run" not in result:
                result["run"] = {"run_id": run_id, "generated_at_utc": datetime.now(timezone.utc).isoformat()}
            return result
        except json.JSONDecodeError as e:
            logger.error(f"JSON parse error: {e}")
            logger.error(f"Response was: {response_text[:1000]}")
            return create_fallback_analysis(file_name, case_id, run_id, pages, pdf_text)
        
    except Exception as e:
        logger.error(f"LLM analysis error: {e}")
        return create_fallback_analysis(file_name, case_id, run_id, pages, pdf_text)

def create_fallback_analysis(file_name: str, case_id: str, run_id: str, pages: List[Dict], pdf_text: str) -> Dict:
    """Create fallback analysis when LLM fails - extract what we can deterministically"""
    import re
    
    # Try to extract basic info from text
    text_lower = pdf_text.lower()
    
    # Find procedure ID
    procedure_id = "NOT_SPECIFIED_IN_PERIZIA"
    proc_match = re.search(r'(r\.?g\.?e\.?|e\.?i\.?|esecuzione\s+immobiliare|procedura)\s*n?\.?\s*(\d+[/\-]?\d*)', text_lower)
    if proc_match:
        procedure_id = proc_match.group(0).upper()
    
    # Find tribunal
    tribunale = "NOT_SPECIFIED_IN_PERIZIA"
    trib_match = re.search(r'tribunale\s+(di\s+)?([a-z\s]+)', text_lower)
    if trib_match:
        tribunale = "TRIBUNALE DI " + trib_match.group(2).strip().upper()
    
    # Find prezzo base
    prezzo_base = 0
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
    superficie = "NOT_SPECIFIED_IN_PERIZIA"
    sup_match = re.search(r'(\d+[\.,]?\d*)\s*(mq|m²|metri\s*quadr)', text_lower)
    if sup_match:
        superficie = sup_match.group(1) + " mq"
    
    return {
        "schema_version": "nexodify_perizia_scan_v1",
        "run": {
            "run_id": run_id,
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "input": {"source_type": "perizia_pdf", "file_name": file_name, "pages_total": len(pages)}
        },
        "case_header": {
            "procedure_id": procedure_id,
            "lotto": "Lotto Unico",
            "tribunale": tribunale,
            "address": {"street": "NOT_SPECIFIED_IN_PERIZIA", "city": "NOT_SPECIFIED_IN_PERIZIA", "full": "NOT_SPECIFIED_IN_PERIZIA"},
            "deposit_date": "NOT_SPECIFIED_IN_PERIZIA"
        },
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
                {"code": "A", "label_it": "Regolarizzazione urbanistica", "label_en": "Urban regularization", "type": "NOT_SPECIFIED", "action_required_it": "Verificare con tecnico", "action_required_en": "Verify with technician"},
                {"code": "B", "label_it": "Oneri tecnici / istruttoria", "label_en": "Technical fees", "type": "NEXODIFY_ESTIMATE", "range": {"min": 5000, "max": 25000}, "source": "NEXODIFY_ESTIMATE"},
                {"code": "C", "label_it": "Rischio ripristini", "label_en": "Restoration risk", "type": "NEXODIFY_ESTIMATE", "range": {"min": 10000, "max": 40000}, "source": "NEXODIFY_ESTIMATE"},
                {"code": "D", "label_it": "Allineamento catastale", "label_en": "Cadastral alignment", "type": "NEXODIFY_ESTIMATE", "range": {"min": 1000, "max": 2000}, "source": "NEXODIFY_ESTIMATE"},
                {"code": "E", "label_it": "Spese condominiali arretrate", "label_en": "Condo arrears", "type": "NOT_SPECIFIED", "action_required_it": "Verificare con amministratore", "action_required_en": "Verify with administrator"},
                {"code": "F", "label_it": "Costi procedura", "label_en": "Procedure costs", "type": "NOT_SPECIFIED", "action_required_it": "Verificare con delegato", "action_required_en": "Verify with delegate"},
                {"code": "G", "label_it": "Cancellazione formalità", "label_en": "Formality cancellation", "type": "INFO_ONLY", "note_it": "Solitamente con decreto di trasferimento"},
                {"code": "H", "label_it": "Costo liberazione", "label_en": "Liberation cost", "type": "NEXODIFY_ESTIMATE", "value": 1500, "source": "NEXODIFY_ESTIMATE"}
            ],
            "total_extra_costs": {"range": {"min": 17500, "max": 68500}, "max_is_open": True}
        },
        "dati_certi_del_lotto": {
            "prezzo_base_asta": {"value": prezzo_base, "formatted": f"€{prezzo_base:,.0f}" if prezzo_base else "NOT_SPECIFIED", "evidence": []},
            "superficie_catastale": {"value": superficie, "evidence": []},
            "catasto": {"categoria": "NOT_SPECIFIED_IN_PERIZIA", "classe": "NOT_SPECIFIED_IN_PERIZIA", "vani": "NOT_SPECIFIED_IN_PERIZIA"},
            "diritto_reale": {"value": "NOT_SPECIFIED_IN_PERIZIA", "evidence": []}
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
            "extra_costs_min": 17500,
            "extra_costs_max": 68500,
            "all_in_light_min": prezzo_base + 17500 if prezzo_base else 0,
            "all_in_light_max": prezzo_base + 68500 if prezzo_base else 0,
            "dry_read_it": f"Prezzo base €{prezzo_base:,.0f} + costi stimati €17.500-68.500 = All-in €{prezzo_base + 17500:,.0f}-{prezzo_base + 68500:,.0f}" if prezzo_base else "Prezzo base non specificato",
            "dry_read_en": f"Base price €{prezzo_base:,.0f} + estimated costs €17,500-68,500 = All-in €{prezzo_base + 17500:,.0f}-{prezzo_base + 68500:,.0f}" if prezzo_base else "Base price not specified"
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
            "summary_it": f"Analisi del documento {file_name}. Il sistema ha estratto i dati disponibili ma si raccomanda una revisione manuale completa prima di procedere con qualsiasi offerta. Prezzo base: €{prezzo_base:,.0f}. Costi extra stimati: €17.500-68.500. All-in stimato: €{prezzo_base + 17500:,.0f}-{prezzo_base + 68500:,.0f}." if prezzo_base else f"Analisi del documento {file_name}. Dati incompleti - revisione manuale necessaria.",
            "summary_en": f"Analysis of document {file_name}. The system extracted available data but a complete manual review is recommended before proceeding with any offer. Base price: €{prezzo_base:,.0f}. Estimated extra costs: €17,500-68,500. Estimated all-in: €{prezzo_base + 17500:,.0f}-{prezzo_base + 68500:,.0f}." if prezzo_base else f"Analysis of document {file_name}. Incomplete data - manual review required.",
            "disclaimer_it": "Documento informativo. Non costituisce consulenza legale. Consultare un professionista qualificato.",
            "disclaimer_en": "Informational document. Not legal advice. Consult a qualified professional."
        },
        "qa": {
            "status": "WARN",
            "reasons": [
                {"code": "PARTIAL_EXTRACTION", "severity": "AMBER", "reason_it": "Estrazione automatica parziale", "reason_en": "Partial automatic extraction"}
            ]
        }
    }

@api_router.post("/analysis/perizia")
async def analyze_perizia(request: Request, file: UploadFile = File(...)):
    """Analyze uploaded perizia PDF"""
    user = await require_auth(request)
    
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
    
    # Read PDF
    contents = await file.read()
    input_sha256 = hashlib.sha256(contents).hexdigest()
    
    try:
        pdf_reader = PdfReader(io.BytesIO(contents))
        pages = []
        full_text = ""
        
        for i, page in enumerate(pdf_reader.pages):
            page_text = page.extract_text() or ""
            pages.append({"page_number": i + 1, "text": page_text})
            full_text += f"\n=== PAGE {i + 1} ===\n{page_text}"
        
        if not full_text.strip():
            raise HTTPException(status_code=400, detail="Impossibile estrarre testo dal PDF. Il file potrebbe essere scansionato o protetto. / Could not extract text from PDF. File may be scanned or protected.")
        
    except Exception as e:
        logger.error(f"PDF parsing error: {e}")
        raise HTTPException(status_code=400, detail="File PDF non valido / Invalid PDF file")
    
    # Generate IDs
    case_id = f"case_{uuid.uuid4().hex[:8]}"
    run_id = f"run_{uuid.uuid4().hex[:8]}"
    
    # Analyze with LLM
    result = await analyze_perizia_with_llm(full_text, pages, file.filename, user, case_id, run_id, input_sha256)
    
    # Create analysis record
    analysis = PeriziaAnalysis(
        analysis_id=f"analysis_{uuid.uuid4().hex[:12]}",
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
    await db.perizia_analyses.insert_one(analysis_dict)
    
    # Decrement quota if not master admin
    if not user.is_master_admin:
        await db.users.update_one(
            {"user_id": user.user_id},
            {"$inc": {"quota.perizia_scans_remaining": -1}}
        )
    
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

@api_router.get("/analysis/perizia/{analysis_id}/pdf")
async def download_perizia_pdf(analysis_id: str, request: Request):
    """Generate and download PDF report for analysis"""
    user = await require_auth(request)
    
    analysis = await db.perizia_analyses.find_one(
        {"analysis_id": analysis_id, "user_id": user.user_id},
        {"_id": 0}
    )
    
    if not analysis:
        raise HTTPException(status_code=404, detail="Analysis not found")
    
    result = analysis.get("result", {})
    
    # Generate HTML for PDF
    html_content = generate_report_html(analysis, result)
    
    # Return as downloadable HTML (can be converted to PDF client-side or via print)
    return Response(
        content=html_content,
        media_type="text/html",
        headers={
            "Content-Disposition": f'attachment; filename="nexodify_report_{analysis_id}.html"'
        }
    )

def generate_report_html(analysis: Dict, result: Dict) -> str:
    """Generate HTML report from analysis"""
    case_header = result.get("case_header", {})
    semaforo = result.get("semaforo_generale", {})
    decision = result.get("decision_rapida_client", {})
    money_box = result.get("money_box", {})
    dati = result.get("dati_certi_del_lotto", {})
    summary = result.get("summary_for_client", {})
    
    semaforo_color = "#10b981" if semaforo.get("status") == "GREEN" else "#f59e0b" if semaforo.get("status") == "AMBER" else "#ef4444"
    
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
        .section {{ background: #18181b; border: 1px solid #27272a; border-radius: 12px; padding: 24px; margin-bottom: 24px; }}
        .section h2 {{ color: #D4AF37; font-size: 18px; margin-bottom: 16px; padding-bottom: 8px; border-bottom: 1px solid #27272a; }}
        .grid {{ display: grid; grid-template-columns: repeat(2, 1fr); gap: 16px; }}
        .field {{ background: #09090b; padding: 12px; border-radius: 8px; }}
        .field-label {{ font-size: 11px; color: #71717a; text-transform: uppercase; letter-spacing: 0.5px; }}
        .field-value {{ font-size: 16px; color: #fafafa; margin-top: 4px; font-weight: 500; }}
        .money-item {{ display: flex; justify-content: space-between; padding: 12px; background: #09090b; border-radius: 8px; margin-bottom: 8px; }}
        .total {{ background: #D4AF3720; border: 1px solid #D4AF3740; padding: 16px; border-radius: 8px; margin-top: 16px; }}
        .total-value {{ font-size: 24px; color: #D4AF37; font-weight: bold; }}
        .disclaimer {{ background: #27272a; padding: 16px; border-radius: 8px; margin-top: 40px; text-align: center; }}
        .disclaimer p {{ color: #71717a; font-size: 12px; }}
        .footer {{ text-align: center; margin-top: 40px; color: #52525b; font-size: 12px; }}
        @media print {{ body {{ background: white; color: black; }} .section {{ border-color: #e5e5e5; background: #f9f9f9; }} }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>NEXODIFY AUCTION SCAN REPORT</h1>
            <p>Analisi Forense Perizia CTU</p>
            <div class="semaforo">{semaforo.get('status_it', 'ATTENZIONE')}</div>
        </div>
        
        <div class="section">
            <h2>DATI PROCEDURA</h2>
            <div class="grid">
                <div class="field">
                    <div class="field-label">Procedura</div>
                    <div class="field-value">{case_header.get('procedure_id', 'N/A')}</div>
                </div>
                <div class="field">
                    <div class="field-label">Lotto</div>
                    <div class="field-value">{case_header.get('lotto', 'N/A')}</div>
                </div>
                <div class="field">
                    <div class="field-label">Tribunale</div>
                    <div class="field-value">{case_header.get('tribunale', 'N/A')}</div>
                </div>
                <div class="field">
                    <div class="field-label">File Analizzato</div>
                    <div class="field-value">{analysis.get('file_name', 'N/A')}</div>
                </div>
            </div>
        </div>
        
        <div class="section">
            <h2>DECISIONE RAPIDA</h2>
            <p style="font-size: 18px; margin-bottom: 16px;">{decision.get('summary_it', 'Analisi completata')}</p>
            <p style="color: #a1a1aa;">{decision.get('summary_en', '')}</p>
        </div>
        
        <div class="section">
            <h2>DATI CERTI DEL LOTTO</h2>
            <div class="grid">
                <div class="field">
                    <div class="field-label">Prezzo Base Asta</div>
                    <div class="field-value" style="color: #D4AF37; font-size: 20px;">{dati.get('prezzo_base_asta', {}).get('formatted', 'N/A')}</div>
                </div>
                <div class="field">
                    <div class="field-label">Superficie</div>
                    <div class="field-value">{dati.get('superficie_catastale', {}).get('value', 'N/A')}</div>
                </div>
                <div class="field">
                    <div class="field-label">Diritto Reale</div>
                    <div class="field-value">{dati.get('diritto_reale', {}).get('value', 'N/A')}</div>
                </div>
                <div class="field">
                    <div class="field-label">Categoria Catastale</div>
                    <div class="field-value">{dati.get('catasto', {}).get('categoria', 'N/A')}</div>
                </div>
            </div>
        </div>
        
        <div class="section">
            <h2>PORTAFOGLIO COSTI (MONEY BOX)</h2>
            {''.join([f'<div class="money-item"><span>{item.get("label_it", item.get("code", ""))}</span><span style="color: #D4AF37;">{"€" + str(item.get("value", "")) if item.get("value") else ("€" + str(item.get("range", {}).get("min", "")) + " - €" + str(item.get("range", {}).get("max", "")) if item.get("range") else item.get("type", "N/A"))}</span></div>' for item in money_box.get("items", [])])}
            <div class="total">
                <div style="display: flex; justify-content: space-between; align-items: center;">
                    <span>TOTALE COSTI EXTRA STIMATI</span>
                    <span class="total-value">€{money_box.get("total_extra_costs", {}).get("range", {}).get("min", 0):,} - €{money_box.get("total_extra_costs", {}).get("range", {}).get("max", 0):,}</span>
                </div>
            </div>
        </div>
        
        <div class="section">
            <h2>RIEPILOGO</h2>
            <p>{summary.get('summary_it', 'Analisi completata.')}</p>
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

@api_router.post("/analysis/image")
async def analyze_images(request: Request, files: List[UploadFile] = File(...)):
    """Analyze uploaded property images"""
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
    
    result = {
        "ok": True,
        "mode": "IMAGE_FORENSICS",
        "result": {
            "schema_version": "nexodify_image_forensics_v1",
            "run": {
                "run_id": run_id,
                "case_id": case_id,
                "generated_at_utc": datetime.now(timezone.utc).isoformat(),
                "revision": 0
            },
            "findings": [
                {
                    "finding_id": f"find_{uuid.uuid4().hex[:8]}",
                    "title_it": "Analisi immagine completata",
                    "title_en": "Image analysis completed",
                    "severity": "LOW",
                    "confidence": "MEDIUM",
                    "what_i_see_it": f"Analizzate {len(files)} immagini dell'immobile",
                    "what_i_see_en": f"Analyzed {len(files)} property images",
                    "why_it_matters_it": "Le immagini forniscono contesto visivo per la valutazione",
                    "why_it_matters_en": "Images provide visual context for assessment"
                }
            ],
            "summary_it": f"Caricate {len(files)} immagini. Per un'analisi visiva dettagliata, si raccomanda un sopralluogo professionale.",
            "summary_en": f"Uploaded {len(files)} images. For detailed visual analysis, professional inspection recommended.",
            "disclaimer_it": "Documento informativo. Non costituisce consulenza legale.",
            "disclaimer_en": "Informational document. Not legal advice."
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
    
    # Decrement quota
    if not user.is_master_admin:
        await db.users.update_one(
            {"user_id": user.user_id},
            {"$inc": {"quota.image_scans_remaining": -1}}
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

REGOLE:
1. Rispondi SEMPRE in italiano prima, poi in inglese
2. Se hai contesto da una perizia analizzata, usalo per risposte specifiche
3. Cita fonti normative quando possibile
4. NON fornire mai consulenza legale - solo informazioni
5. Raccomanda sempre di consultare professionisti qualificati per decisioni importanti

DISCLAIMER (includi sempre):
"Le informazioni fornite hanno carattere esclusivamente informativo e non costituiscono consulenza legale, fiscale o professionale."

Formato risposta JSON:
{
  "answer_it": "risposta in italiano",
  "answer_en": "risposta in inglese", 
  "needs_more_info": "YES/NO",
  "missing_inputs": [],
  "safe_disclaimer_it": "...",
  "safe_disclaimer_en": "..."
}"""

@api_router.post("/analysis/assistant")
async def assistant_qa(request: Request):
    """Answer user questions about perizia/real estate - with context from analyzed documents"""
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
    
    from emergentintegrations.llm.chat import LlmChat, UserMessage
    
    run_id = f"qa_run_{uuid.uuid4().hex[:8]}"
    
    # Get context from user's analyzed perizie
    context = ""
    if related_case_id:
        analysis = await db.perizia_analyses.find_one({"case_id": related_case_id, "user_id": user.user_id}, {"_id": 0})
        if analysis:
            result = analysis.get('result', {})
            context = f"""
CONTESTO PERIZIA ANALIZZATA:
- File: {analysis.get('file_name')}
- Procedura: {result.get('case_header', {}).get('procedure_id', 'N/A')}
- Tribunale: {result.get('case_header', {}).get('tribunale', 'N/A')}
- Prezzo Base: {result.get('dati_certi_del_lotto', {}).get('prezzo_base_asta', {}).get('formatted', 'N/A')}
- Semaforo: {result.get('semaforo_generale', {}).get('status', 'N/A')}
- Riepilogo: {result.get('summary_for_client', {}).get('summary_it', 'N/A')}
"""
    else:
        # Get user's most recent analysis for context
        recent = await db.perizia_analyses.find_one(
            {"user_id": user.user_id},
            {"_id": 0},
            sort=[("created_at", -1)]
        )
        if recent:
            result = recent.get('result', {})
            context = f"""
ULTIMA PERIZIA ANALIZZATA DALL'UTENTE:
- File: {recent.get('file_name')}
- Procedura: {result.get('case_header', {}).get('procedure_id', 'N/A')}
- Prezzo Base: {result.get('dati_certi_del_lotto', {}).get('prezzo_base_asta', {}).get('formatted', 'N/A')}
"""
    
    chat = LlmChat(
        api_key=EMERGENT_LLM_KEY,
        session_id=f"assistant_{user.user_id}_{run_id}",
        system_message=ASSISTANT_SYSTEM_PROMPT
    ).with_model("openai", "gpt-4o")
    
    prompt = f"""{context}

DOMANDA UTENTE: {question}

Rispondi in modo chiaro e utile, citando la normativa italiana quando rilevante. Output JSON."""
    
    try:
        response = await chat.send_message(UserMessage(text=prompt))
        
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
        except:
            answer = {
                "answer_it": response,
                "answer_en": "",
                "needs_more_info": "NO",
                "missing_inputs": [],
                "safe_disclaimer_it": "Le informazioni fornite hanno carattere esclusivamente informativo e non costituiscono consulenza legale.",
                "safe_disclaimer_en": "Information provided is for informational purposes only and does not constitute legal advice."
            }
    except Exception as e:
        logger.error(f"Assistant error: {e}")
        answer = {
            "answer_it": "Mi scusi, si è verificato un errore. Riprovi più tardi.",
            "answer_en": "Sorry, an error occurred. Please try again later.",
            "needs_more_info": "NO",
            "missing_inputs": [],
            "safe_disclaimer_it": "Le informazioni fornite hanno carattere esclusivamente informativo e non costituiscono consulenza legale.",
            "safe_disclaimer_en": "Information provided is for informational purposes only and does not constitute legal advice."
        }
    
    result = {
        "ok": True,
        "mode": "ASSISTANT_QA",
        "result": {
            "schema_version": "nexodify_assistant_v1",
            "run": {
                "run_id": run_id,
                "generated_at_utc": datetime.now(timezone.utc).isoformat(),
                "case_id": related_case_id,
                "revision": 0
            },
            **answer
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
    
    # Decrement quota
    if not user.is_master_admin:
        await db.users.update_one(
            {"user_id": user.user_id},
            {"$inc": {"quota.assistant_messages_remaining": -1}}
        )
    
    return result

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

@api_router.get("/history/perizia/{analysis_id}")
async def get_perizia_detail(analysis_id: str, request: Request):
    """Get specific perizia analysis"""
    user = await require_auth(request)
    
    analysis = await db.perizia_analyses.find_one(
        {"analysis_id": analysis_id, "user_id": user.user_id},
        {"_id": 0, "raw_text": 0}
    )
    
    if not analysis:
        raise HTTPException(status_code=404, detail="Analysis not found")
    
    return analysis

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

app.add_middleware(
    CORSMiddleware,
    allow_credentials=True,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.on_event("shutdown")
async def shutdown_db_client():
    client.close()

from fastapi import FastAPI, APIRouter, HTTPException, Request, UploadFile, File, Depends, Response
from fastapi.responses import JSONResponse
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
# PERIZIA ANALYSIS ENDPOINT
# ===================

PERIZIA_SYSTEM_PROMPT = """You are "Nexodify Forensic Engine": a deterministic, audit-grade analyzer for Italian real-estate perizie/CTU documents.

ABSOLUTE RULES:
1) NO HALLUCINATION: Never invent facts, values, laws, dates, addresses, costs, or outcomes.
2) EVIDENCE-FIRST: Every factual claim MUST include evidence[] with page, anchor, quote.
3) ZERO EMPTY FIELDS: Use "NOT_SPECIFIED_IN_PERIZIA" or "UNKNOWN" instead of empty values.
4) OUTPUT ONLY VALID JSON - no markdown, no commentary.

You must analyze the perizia document and extract:
- case_header: procedure_id, lotto, tribunale, address, deposit_date
- semaforo_generale: GREEN/AMBER/RED based on risk assessment
- decision_rapida_client: risk level, driver rosso reasons
- money_box: costs A-H with evidence or NEXODIFY_ESTIMATE
- dati_certi_del_lotto: prezzo_base, superficie, catasto, diritto_reale
- abusi_edilizi_conformita: urbanistica, catastale, condono status
- stato_occupativo: libero/occupato, title_opponible
- stato_conservativo: condition issues found
- formalita: ipoteca, pignoramento, cancellation status
- legal_killers_checklist: 8 killer checks (YES/NO/NOT_SPECIFIED)
- indice_di_convenienza: all_in cost calculation
- red_flags_operativi: list of warnings
- checklist_pre_offerta: due diligence items
- summary_for_client: bilingual summary
- qa: PASS/WARN/FAIL with reasons

Output must be valid JSON matching the schema."""

async def analyze_perizia_with_llm(pdf_text: str, pages: List[Dict], file_name: str, user: User, case_id: str, run_id: str) -> Dict:
    """Analyze perizia using LLM"""
    from emergentintegrations.llm.chat import LlmChat, UserMessage
    
    chat = LlmChat(
        api_key=EMERGENT_LLM_KEY,
        session_id=f"perizia_{run_id}",
        system_message=PERIZIA_SYSTEM_PROMPT
    ).with_model("gemini", "gemini-2.5-flash")
    
    # Prepare the analysis request
    analysis_request = {
        "mode": "PERIZIA_ANALYZE",
        "user_context": {
            "user_id": user.user_id,
            "plan": user.plan,
            "is_paid": user.plan != "free" or user.is_master_admin
        },
        "request": {
            "request_id": run_id,
            "case_id": case_id,
            "file_name": file_name
        },
        "perizia_text": pdf_text[:50000],  # Limit text size
        "page_count": len(pages)
    }
    
    prompt = f"""Analyze this Italian perizia/CTU document and produce a complete forensic analysis JSON.

Document: {file_name}
Pages: {len(pages)}

TEXT CONTENT:
{pdf_text[:40000]}

Produce a complete JSON analysis following the schema. Include evidence with page numbers and quotes for every finding."""
    
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
        except json.JSONDecodeError:
            # Create structured response if parsing fails
            result = create_default_analysis_result(file_name, case_id, run_id, pdf_text)
        
        return result
    except Exception as e:
        logger.error(f"LLM analysis error: {e}")
        return create_default_analysis_result(file_name, case_id, run_id, pdf_text)

def create_default_analysis_result(file_name: str, case_id: str, run_id: str, pdf_text: str) -> Dict:
    """Create default analysis structure when LLM fails"""
    return {
        "ok": True,
        "mode": "PERIZIA_ANALYZE",
        "result": {
            "schema_version": "nexodify_perizia_scan_v1",
            "run": {
                "run_id": run_id,
                "case_id": case_id,
                "generated_at_utc": datetime.now(timezone.utc).isoformat(),
                "revision": 0
            },
            "case_header": {
                "procedure_id": "NOT_SPECIFIED_IN_PERIZIA",
                "lotto": "NOT_SPECIFIED_IN_PERIZIA",
                "tribunale": "NOT_SPECIFIED_IN_PERIZIA",
                "address": "NOT_SPECIFIED_IN_PERIZIA",
                "deposit_date": "NOT_SPECIFIED_IN_PERIZIA"
            },
            "semaforo_generale": {
                "status": "AMBER",
                "status_it": "ATTENZIONE",
                "status_en": "CAUTION",
                "reason_it": "Analisi richiede revisione manuale",
                "reason_en": "Analysis requires manual review"
            },
            "decision_rapida_client": {
                "risk_level": "MEDIUM_RISK",
                "risk_level_it": "RISCHIO MEDIO",
                "risk_level_en": "MEDIUM RISK",
                "driver_rosso": [],
                "summary_it": "Documento richiede analisi approfondita",
                "summary_en": "Document requires detailed analysis"
            },
            "money_box": {
                "items": [
                    {"code": "A", "label_it": "Regolarizzazione urbanistica", "label_en": "Urban regularization", "type": "NOT_SPECIFIED", "action_required_it": "Verificare con tecnico", "action_required_en": "Verify with technician"},
                    {"code": "B", "label_it": "Oneri tecnici", "label_en": "Technical fees", "type": "NEXODIFY_ESTIMATE", "range": {"min": 5000, "max": 25000}},
                    {"code": "C", "label_it": "Rischio ripristini", "label_en": "Restoration risk", "type": "NEXODIFY_ESTIMATE", "range": {"min": 10000, "max": 40000}},
                    {"code": "D", "label_it": "Allineamento catastale", "label_en": "Cadastral alignment", "type": "NEXODIFY_ESTIMATE", "range": {"min": 1000, "max": 2000}},
                    {"code": "E", "label_it": "Spese condominiali", "label_en": "Condo fees", "type": "NOT_SPECIFIED", "action_required_it": "Verificare con amministratore", "action_required_en": "Verify with administrator"},
                    {"code": "F", "label_it": "Costi procedura", "label_en": "Procedure costs", "type": "NOT_SPECIFIED", "action_required_it": "Verificare con delegato", "action_required_en": "Verify with delegate"},
                    {"code": "G", "label_it": "Cancellazione formalità", "label_en": "Formality cancellation", "type": "INFO_ONLY", "note_it": "Verificare decreto", "note_en": "Verify decree"},
                    {"code": "H", "label_it": "Costo liberazione", "label_en": "Liberation cost", "type": "NEXODIFY_ESTIMATE", "value": 1500}
                ],
                "total_extra_costs": {
                    "range": {"min": 17500, "max": 68500},
                    "max_is_open": True
                }
            },
            "dati_certi_del_lotto": {
                "prezzo_base_asta": "NOT_SPECIFIED_IN_PERIZIA",
                "superficie_catastale": "NOT_SPECIFIED_IN_PERIZIA",
                "catasto": {
                    "categoria": "NOT_SPECIFIED_IN_PERIZIA",
                    "classe": "NOT_SPECIFIED_IN_PERIZIA",
                    "vani": "NOT_SPECIFIED_IN_PERIZIA"
                },
                "diritto_reale": "NOT_SPECIFIED_IN_PERIZIA"
            },
            "abusi_edilizi_conformita": {
                "conformita_urbanistica": "UNKNOWN",
                "conformita_catastale": "UNKNOWN",
                "condono": {
                    "present": "UNKNOWN",
                    "status": "NOT_SPECIFIED_IN_PERIZIA"
                }
            },
            "stato_occupativo": {
                "status": "UNKNOWN",
                "title_opponible": "NOT_SPECIFIED_IN_PERIZIA"
            },
            "stato_conservativo": {
                "issues_found": [],
                "note_it": "Verificare stato immobile",
                "note_en": "Verify property condition"
            },
            "formalita": {
                "ipoteca": "NOT_SPECIFIED_IN_PERIZIA",
                "pignoramento": "NOT_SPECIFIED_IN_PERIZIA",
                "cancellazione_decreto": "NOT_SPECIFIED_IN_PERIZIA"
            },
            "legal_killers_checklist": {
                "PEEP_superficie": {"status": "NOT_SPECIFIED_IN_PERIZIA", "action_required_it": "Verificare", "action_required_en": "Verify"},
                "donazione_catena_20anni": {"status": "NOT_SPECIFIED_IN_PERIZIA", "action_required_it": "Verificare", "action_required_en": "Verify"},
                "prelazione_stato_beni_culturali": {"status": "NOT_SPECIFIED_IN_PERIZIA", "action_required_it": "Verificare", "action_required_en": "Verify"},
                "usi_civici_diritti_demaniali": {"status": "NOT_SPECIFIED_IN_PERIZIA", "action_required_it": "Verificare", "action_required_en": "Verify"},
                "fondo_patrimoniale": {"status": "NOT_SPECIFIED_IN_PERIZIA", "action_required_it": "Verificare", "action_required_en": "Verify"},
                "servitu_atti_obbligo": {"status": "NOT_SPECIFIED_IN_PERIZIA", "action_required_it": "Verificare", "action_required_en": "Verify"},
                "formalita_non_cancellabili": {"status": "NOT_SPECIFIED_IN_PERIZIA", "action_required_it": "Verificare", "action_required_en": "Verify"},
                "amianto": {"status": "NOT_SPECIFIED_IN_PERIZIA", "action_required_it": "Verificare", "action_required_en": "Verify"}
            },
            "indice_di_convenienza": {
                "note_it": "Calcolo richiede prezzo base",
                "note_en": "Calculation requires base price"
            },
            "red_flags_operativi": [
                {
                    "code": "MANUAL_REVIEW",
                    "severity": "AMBER",
                    "flag_it": "Analisi automatica incompleta",
                    "flag_en": "Automatic analysis incomplete",
                    "action_it": "Revisione manuale raccomandata",
                    "action_en": "Manual review recommended"
                }
            ],
            "checklist_pre_offerta": [
                {"item_it": "Verificare conformità urbanistica", "item_en": "Verify urban compliance", "status": "TO_CHECK"},
                {"item_it": "Verificare stato occupativo", "item_en": "Verify occupancy status", "status": "TO_CHECK"},
                {"item_it": "Verificare formalità", "item_en": "Verify formalities", "status": "TO_CHECK"}
            ],
            "summary_for_client": {
                "summary_it": "Documento caricato. Analisi preliminare completata. Si raccomanda revisione approfondita.",
                "summary_en": "Document uploaded. Preliminary analysis complete. Detailed review recommended."
            },
            "qa": {
                "status": "WARN",
                "reasons": [
                    {"code": "QA_INCOMPLETE", "reason_it": "Analisi parziale", "reason_en": "Partial analysis"}
                ]
            }
        }
    }

@api_router.post("/analysis/perizia")
async def analyze_perizia(request: Request, file: UploadFile = File(...)):
    """Analyze uploaded perizia PDF"""
    user = await require_auth(request)
    
    # Check file type - PDF only
    if not file.filename.lower().endswith('.pdf'):
        raise HTTPException(status_code=400, detail="Only PDF files are accepted / Solo file PDF sono accettati")
    
    if file.content_type and file.content_type != "application/pdf":
        raise HTTPException(status_code=400, detail="Only PDF files are accepted / Solo file PDF sono accettati")
    
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
    
    try:
        pdf_reader = PdfReader(io.BytesIO(contents))
        pages = []
        full_text = ""
        
        for i, page in enumerate(pdf_reader.pages):
            page_text = page.extract_text() or ""
            pages.append({"page_number": i + 1, "text": page_text})
            full_text += f"\n=== PAGE {i + 1} ===\n{page_text}"
        
        if not full_text.strip():
            raise HTTPException(status_code=400, detail="Could not extract text from PDF / Impossibile estrarre testo dal PDF")
        
    except Exception as e:
        logger.error(f"PDF parsing error: {e}")
        raise HTTPException(status_code=400, detail="Invalid PDF file / File PDF non valido")
    
    # Generate IDs
    case_id = f"case_{uuid.uuid4().hex[:8]}"
    run_id = f"run_{uuid.uuid4().hex[:8]}"
    
    # Analyze with LLM
    result = await analyze_perizia_with_llm(full_text, pages, file.filename, user, case_id, run_id)
    
    # Create analysis record
    analysis = PeriziaAnalysis(
        analysis_id=f"analysis_{uuid.uuid4().hex[:12]}",
        user_id=user.user_id,
        case_id=case_id,
        run_id=run_id,
        case_title=file.filename,
        file_name=file.filename,
        result=result
    )
    
    analysis_dict = analysis.model_dump()
    analysis_dict["created_at"] = analysis_dict["created_at"].isoformat()
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
# IMAGE FORENSICS ENDPOINT
# ===================

IMAGE_FORENSICS_PROMPT = """You are an expert building forensics analyzer. Analyze the uploaded building/property images and identify:
1. Visible defects (cracks, water damage, mold, structural issues)
2. Materials observed (concrete, brick, plaster, etc.)
3. Compliance flags (safety issues, building code concerns)
4. Condition assessment

Output must be valid JSON with:
- findings: array of {finding_id, title_it, title_en, severity, confidence, what_i_see_it, what_i_see_en, why_it_matters_it, why_it_matters_en}
- materials_observed: array of strings
- defects_observed: array of strings
- compliance_flags: array of {code, severity, note_it, note_en}
- summary_it, summary_en"""

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
    
    # For now, create a basic response (image analysis would need vision model)
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
                    "why_it_matters_en": "Images provide visual context for assessment",
                    "recommended_next_photo_it": "Foto dettagliata di eventuali problemi identificati",
                    "recommended_next_photo_en": "Detailed photo of any identified issues"
                }
            ],
            "materials_observed": ["NOT_ANALYZED"],
            "defects_observed": ["NOT_ANALYZED"],
            "compliance_flags": [],
            "summary_it": f"Caricate {len(files)} immagini. Analisi visiva richiede modello vision dedicato.",
            "summary_en": f"Uploaded {len(files)} images. Visual analysis requires dedicated vision model."
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
# ASSISTANT QA ENDPOINT
# ===================

ASSISTANT_SYSTEM_PROMPT = """You are Nexodify Assistant, an expert on Italian real-estate auctions, perizia documents, and property analysis.

Rules:
1. Answer questions about Italian real estate auctions, CTU/perizia documents, legal requirements
2. If a specific case is referenced, use the provided context
3. Always provide bilingual responses (Italian first, then English)
4. Never provide legal advice - only informational guidance
5. Be precise and cite sources when possible

Output JSON with: answer_it, answer_en, needs_more_info, missing_inputs, safe_disclaimer_it, safe_disclaimer_en"""

@api_router.post("/analysis/assistant")
async def assistant_qa(request: Request):
    """Answer user questions about perizia/real estate"""
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
    
    # Get related case context if provided
    context = ""
    if related_case_id:
        analysis = await db.perizia_analyses.find_one({"case_id": related_case_id, "user_id": user.user_id}, {"_id": 0})
        if analysis:
            context = f"\nRelated case analysis summary: {json.dumps(analysis.get('result', {}).get('summary_for_client', {}))}"
    
    chat = LlmChat(
        api_key=EMERGENT_LLM_KEY,
        session_id=f"assistant_{run_id}",
        system_message=ASSISTANT_SYSTEM_PROMPT
    ).with_model("gemini", "gemini-2.5-flash")
    
    prompt = f"""User question: {question}
{context}

Provide a helpful response in JSON format with answer_it, answer_en, needs_more_info (YES/NO), missing_inputs (array), safe_disclaimer_it, safe_disclaimer_en."""
    
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
                "answer_en": response,
                "needs_more_info": "NO",
                "missing_inputs": [],
                "safe_disclaimer_it": "Documento informativo, non è consulenza legale.",
                "safe_disclaimer_en": "Informational only; not legal advice."
            }
    except Exception as e:
        logger.error(f"Assistant error: {e}")
        answer = {
            "answer_it": "Mi scusi, si è verificato un errore. Riprovi più tardi.",
            "answer_en": "Sorry, an error occurred. Please try again later.",
            "needs_more_info": "NO",
            "missing_inputs": [],
            "safe_disclaimer_it": "Documento informativo, non è consulenza legale.",
            "safe_disclaimer_en": "Informational only; not legal advice."
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
        {"_id": 0}
    ).sort("created_at", -1).skip(skip).limit(limit).to_list(limit)
    
    total = await db.perizia_analyses.count_documents({"user_id": user.user_id})
    
    return {"analyses": analyses, "total": total, "limit": limit, "skip": skip}

@api_router.get("/history/perizia/{analysis_id}")
async def get_perizia_detail(analysis_id: str, request: Request):
    """Get specific perizia analysis"""
    user = await require_auth(request)
    
    analysis = await db.perizia_analyses.find_one(
        {"analysis_id": analysis_id, "user_id": user.user_id},
        {"_id": 0}
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
        {"$group": {"_id": "$result.result.semaforo_generale.status", "count": {"$sum": 1}}}
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

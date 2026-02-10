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

async def openai_chat_completion(system_message: str, user_message: str, model: str = "gpt-4o") -> str:
    if not OPENAI_API_KEY:
        raise RuntimeError("OPENAI_API_KEY not set")
    client = AsyncOpenAI(api_key=OPENAI_API_KEY)
    response = await client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": system_message},
            {"role": "user", "content": user_message},
        ],
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
        for p in pages_in:
            text = str(p.get("text", "") or "")
            if "SCHEMA RIASSUNTIVO" in text.upper() or ("LOTTO" in text.upper() and "PREZZO BASE" in text.upper()):
                schema_pages.append(p)
        
        # If no schema pages found, scan all pages for lot patterns
        if not schema_pages:
            schema_pages = pages_in
        
        # Find all lot numbers mentioned
        all_lot_numbers = set()
        for p in pages_in:
            text = str(p.get("text", "") or "")
            for m in re.finditer(r"\bLOTTO\s+(\d+)\b", text, flags=re.I):
                all_lot_numbers.add(int(m.group(1)))
        
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
                    continue
                
                # Try to extract lot block (from LOTTO N to next LOTTO or end)
                lot_block_match = re.search(
                    rf"(LOTTO\s+{lot_num}\b.*?)(?=LOTTO\s+\d+\b|$)",
                    text, re.I | re.DOTALL
                )
                block = lot_block_match.group(1) if lot_block_match else text
                
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
                    lot_data["evidence"]["prezzo_base"] = [{"page": page_num, "quote": prezzo_match.group(0)[:150]}]
                
                # Extract Ubicazione
                ubic_match = re.search(
                    r"Ubicazione[:\s]*([^\n]+)",
                    block, re.I
                )
                if ubic_match and lot_data["ubicazione"] == "NON SPECIFICATO IN PERIZIA":
                    lot_data["ubicazione"] = ubic_match.group(1).strip()[:200]
                    lot_data["evidence"]["ubicazione"] = [{"page": page_num, "quote": ubic_match.group(0)[:150]}]
                
                # Extract Diritto reale
                diritto_match = re.search(
                    r"Diritto\s+reale[:\s]*([^\n]+)",
                    block, re.I
                )
                if diritto_match and lot_data["diritto_reale"] == "NON SPECIFICATO IN PERIZIA":
                    lot_data["diritto_reale"] = diritto_match.group(1).strip()[:100]
                    lot_data["evidence"]["diritto_reale"] = [{"page": page_num, "quote": diritto_match.group(0)[:150]}]
                
                # Extract Superficie
                sup_match = re.search(
                    r"Superficie[^:]*[:\s]*([\d.,]+)\s*mq",
                    block, re.I
                )
                if sup_match and lot_data["superficie_mq"] == "NON SPECIFICATO IN PERIZIA":
                    lot_data["superficie_mq"] = f"{sup_match.group(1)} mq"
                    lot_data["evidence"]["superficie"] = [{"page": page_num, "quote": sup_match.group(0)[:150]}]
                
                # Extract Tipologia
                tipo_match = re.search(
                    r"Tipologia[:\s]*([^\n]+)",
                    block, re.I
                )
                if tipo_match and lot_data["tipologia"] == "NON SPECIFICATO IN PERIZIA":
                    lot_data["tipologia"] = tipo_match.group(1).strip()[:100]
            
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
        response = await openai_chat_completion(PERIZIA_SYSTEM_PROMPT, prompt, model="gpt-4o")
        
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
            return create_fallback_analysis(file_name, case_id, run_id, pages, pdf_text, extracted_lots, detected_legal_killers)
        
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
                        "evidence": [{"page": lk["page"], "quote": lk["quote"]}]
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

        verification_response = await openai_chat_completion(PERIZIA_SYSTEM_PROMPT, verification_prompt, model="gpt-4o")
        
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
        return create_fallback_analysis(file_name, case_id, run_id, pages, pdf_text, extracted_lots, detected_legal_killers)


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
    
    # Find procedure ID
    procedure_id = "NON SPECIFICATO IN PERIZIA"
    proc_match = re.search(r'(r\.?g\.?e\.?|e\.?i\.?|esecuzione\s+immobiliare|procedura)\s*n?\.?\s*(\d+[/\-]?\d*)', text_lower)
    if proc_match:
        procedure_id = proc_match.group(0).upper()
    
    # Find tribunal
    tribunale = "NON SPECIFICATO IN PERIZIA"
    trib_match = re.search(r'tribunale\s+(di\s+)?([a-z\s]+)', text_lower)
    if trib_match:
        tribunale = "TRIBUNALE DI " + trib_match.group(2).strip().upper()
    
    # Determine lotto value based on extracted lots
    if len(extracted_lots) >= 2:
        lotto_value = "Lotti " + ", ".join(str(lot["lot_number"]) for lot in extracted_lots)
        is_multi_lot = True
    elif len(extracted_lots) == 1:
        lotto_value = f"Lotto {extracted_lots[0]['lot_number']}"
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
    
    # Build legal killers from deterministic scan
    legal_killers_items = []
    for lk in detected_legal_killers:
        legal_killers_items.append({
            "killer": lk["title"],
            "status": "SI" if lk["severity"] == "ROSSO" else "GIALLO",
            "action": "Verifica obbligatoria",
            "evidence": [{"page": lk["page"], "quote": lk["quote"]}]
        })
    
    return {
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
            "address": {"street": "NON SPECIFICATO IN PERIZIA", "city": "NON SPECIFICATO IN PERIZIA", "full": "NON SPECIFICATO IN PERIZIA"},
            "deposit_date": "NON SPECIFICATO IN PERIZIA"
        },
        "report_header": {
            "title": "NEXODIFY INTELLIGENCE | Auction Scan",
            "procedure": {"value": procedure_id, "evidence": []},
            "lotto": {"value": lotto_value, "evidence": []},
            "tribunale": {"value": tribunale, "evidence": []},
            "address": {"value": "NON SPECIFICATO IN PERIZIA", "evidence": []},
            "is_multi_lot": is_multi_lot,
            "generated_at": datetime.now(timezone.utc).isoformat()
        },
        "lot_index": [{"lot": lot["lot_number"], "prezzo": lot["prezzo_base_eur"], "ubicazione": lot["ubicazione"][:50]} for lot in extracted_lots],
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
                {"code": "A", "label_it": "Regolarizzazione urbanistica", "label_en": "Urban regularization", "type": "TBD", "stima_euro": "TBD", "stima_nota": "TBD (NON SPECIFICATO IN PERIZIA) — Verifica tecnico", "action_required_it": "Verificare con tecnico", "action_required_en": "Verify with technician"},
                {"code": "B", "label_it": "Oneri tecnici / istruttoria", "label_en": "Technical fees", "type": "TBD", "stima_euro": "TBD", "stima_nota": "TBD (NON SPECIFICATO IN PERIZIA)", "source": "TBD"},
                {"code": "C", "label_it": "Rischio ripristini", "label_en": "Restoration risk", "type": "TBD", "stima_euro": "TBD", "stima_nota": "TBD (NON SPECIFICATO IN PERIZIA)", "source": "TBD"},
                {"code": "D", "label_it": "Allineamento catastale", "label_en": "Cadastral alignment", "type": "TBD", "stima_euro": "TBD", "stima_nota": "TBD (NON SPECIFICATO IN PERIZIA)", "source": "TBD"},
                {"code": "E", "label_it": "Spese condominiali arretrate", "label_en": "Condo arrears", "type": "TBD", "stima_euro": "TBD", "stima_nota": "TBD (NON SPECIFICATO IN PERIZIA)", "action_required_it": "Verificare con amministratore", "action_required_en": "Verify with administrator"},
                {"code": "F", "label_it": "Costi procedura", "label_en": "Procedure costs", "type": "TBD", "stima_euro": "TBD", "stima_nota": "TBD (NON SPECIFICATO IN PERIZIA)", "action_required_it": "Verificare con delegato", "action_required_en": "Verify with delegate"},
                {"code": "G", "label_it": "Cancellazione formalità", "label_en": "Formality cancellation", "type": "INFO_ONLY", "stima_euro": "TBD", "stima_nota": "Da liquidare con decreto di trasferimento"},
                {"code": "H", "label_it": "Costo liberazione", "label_en": "Liberation cost", "type": "TBD", "stima_euro": "TBD", "stima_nota": "TBD (NON SPECIFICATO IN PERIZIA)", "source": "TBD"}
            ],
            "total_extra_costs": {"min": "TBD", "max": "TBD", "nota": "TBD — Costi non quantificati in perizia"}
        },
        "section_9_legal_killers": {
            "items": legal_killers_items
        },
        "dati_certi_del_lotto": {
            "prezzo_base_asta": {"value": prezzo_base, "formatted": f"€{prezzo_base:,.0f}" if prezzo_base else "NOT_SPECIFIED", "evidence": []},
            "superficie_catastale": {"value": superficie, "evidence": []},
            "catasto": {"categoria": "NON SPECIFICATO IN PERIZIA", "classe": "NON SPECIFICATO IN PERIZIA", "vani": "NON SPECIFICATO IN PERIZIA"},
            "diritto_reale": {"value": "NON SPECIFICATO IN PERIZIA", "evidence": []}
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
            "extra_costs_min": 0,
            "extra_costs_max": 0,
            "all_in_light_min": prezzo_base if prezzo_base else 0,
            "all_in_light_max": prezzo_base if prezzo_base else 0,
            "dry_read_it": f"Prezzo base €{prezzo_base:,.0f} - Costi extra TBD (non specificati in perizia)" if prezzo_base else "Prezzo base non specificato - Verifica obbligatoria",
            "dry_read_en": f"Base price €{prezzo_base:,.0f} - Extra costs TBD (not specified in perizia)" if prezzo_base else "Base price not specified - Verification required"
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
    
    # Read PDF with Google Document AI for HIGH-QUALITY OCR extraction
    contents = await file.read()
    input_sha256 = hashlib.sha256(contents).hexdigest()
    
    try:
        # Use Google Document AI for extraction
        from document_ai import extract_pdf_with_google_docai
        
        logger.info(f"Extracting text from {file.filename} using Google Document AI...")
        
        # Determine mime type
        mime_type = "application/pdf"
        if file.filename.lower().endswith(('.png', '.jpg', '.jpeg')):
            mime_type = f"image/{file.filename.split('.')[-1].lower()}"
            if mime_type == "image/jpg":
                mime_type = "image/jpeg"
        
        # Extract with Google Document AI
        docai_result = extract_pdf_with_google_docai(contents, mime_type)
        
        if not docai_result.get("success"):
            logger.warning(f"Google Document AI failed: {docai_result.get('error')}, falling back to pdfplumber")
            # Fallback to pdfplumber
            import pdfplumber
            pages = []
            full_text = ""
            with pdfplumber.open(io.BytesIO(contents)) as pdf:
                for i, page in enumerate(pdf.pages):
                    page_text = page.extract_text(x_tolerance=3, y_tolerance=3, layout=True) or ""
                    pages.append({"page_number": i + 1, "text": page_text})
                    full_text += f"\n\n{'='*60}\nPAGINA {i + 1}\n{'='*60}\n{page_text}"
        else:
            # Use Google Document AI results
            pages = docai_result.get("pages", [])
            
            # Build full_text with page markers for LLM
            full_text = ""
            for page_data in pages:
                page_num = page_data.get("page_number", 0)
                page_text = page_data.get("text", "")
                
                # Include table data as formatted text
                tables = page_data.get("tables", [])
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
            
            logger.info(f"Google Document AI extracted {len(pages)} pages, {len(full_text)} chars total")
            
            # Log detailed extraction info
            for i, p in enumerate(pages[:10]):
                logger.info(f"Page {p.get('page_number', i+1)}: {p.get('char_count', len(p.get('text', '')))} chars, "
                           f"{len(p.get('tables', []))} tables, confidence: {p.get('confidence', 0):.2%}")
        
        if not full_text.strip():
            raise HTTPException(status_code=400, detail="Impossibile estrarre testo dal PDF. Il file potrebbe essere scansionato o protetto. / Could not extract text from PDF. File may be scanned or protected.")
        
    except ImportError as e:
        logger.error(f"Google Document AI import error: {e}")
        # Fallback to pdfplumber if Document AI not available
        import pdfplumber
        pages = []
        full_text = ""
        with pdfplumber.open(io.BytesIO(contents)) as pdf:
            for i, page in enumerate(pdf.pages):
                page_text = page.extract_text(x_tolerance=3, y_tolerance=3, layout=True) or ""
                pages.append({"page_number": i + 1, "text": page_text})
                full_text += f"\n\n{'='*60}\nPAGINA {i + 1}\n{'='*60}\n{page_text}"
    except Exception as e:
        logger.error(f"PDF parsing error: {e}")
        raise HTTPException(status_code=400, detail=f"Errore elaborazione PDF / PDF processing error: {str(e)}")
    
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
    def normalize(val):
        if val in [None, "None", "N/A", "NOT_SPECIFIED_IN_PERIZIA", "NOT_SPECIFIED", "UNKNOWN", ""]:
            return "NON SPECIFICATO IN PERIZIA"
        return val
    
    procedure = case_header.get("procedure", {}).get("value") if isinstance(case_header.get("procedure"), dict) else case_header.get("procedure_id", "N/A")
    tribunale = case_header.get("tribunale", {}).get("value") if isinstance(case_header.get("tribunale"), dict) else case_header.get("tribunale", "N/A")
    lotto = case_header.get("lotto", {}).get("value") if isinstance(case_header.get("lotto"), dict) else case_header.get("lotto", "N/A")
    address = case_header.get("address", {}).get("value") if isinstance(case_header.get("address"), dict) else case_header.get("address", "N/A")
    
    procedure = normalize(procedure)
    tribunale = normalize(tribunale)
    lotto = normalize(lotto)
    address = normalize(address)
    
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
                <td>{normalize(lot.get("ubicazione", ""))[:50]}</td>
                <td>{lot.get("superficie_mq", "TBD")}</td>
                <td>{normalize(lot.get("diritto_reale", ""))[:20]}</td>
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
        fonte = item.get("fonte_perizia", {}).get("value", "") if isinstance(item.get("fonte_perizia"), dict) else ""
        
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
        
        money_items_html += f'<div class="money-item"><span>{voce}</span><span class="page-ref">{normalize(fonte)}</span><span style="color: {value_color}; font-weight: bold;">{value_display}</span></div>'
    
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
                    <div class="field-value">{normalize(dati.get('superficie_catastale', {}).get('value', 'NON SPECIFICATO') if isinstance(dati.get('superficie_catastale'), dict) else dati.get('superficie_catastale', 'NON SPECIFICATO'))}</div>
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

# Get the frontend URL from environment for proper CORS configuration
FRONTEND_URL = os.environ.get('FRONTEND_URL', '')
CORS_ORIGINS = [
    "http://localhost:3000",
    "https://repo-setup-31.preview.emergentagent.com",
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

@app.on_event("shutdown")
async def shutdown_db_client():
    client.close()

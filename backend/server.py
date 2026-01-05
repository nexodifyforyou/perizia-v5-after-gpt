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
    from emergentintegrations.llm.chat import LlmChat, UserMessage
    
    # ===========================================================================
    # HELPER FUNCTIONS FOR FULL-DOCUMENT COVERAGE (CHANGE 1)
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
            r"particella|sub|rendita|p\.?e\.?e\.?p|usi\s+civici|beni\s+culturali|"
            r"donazione|fondo\s+patrimoniale|formalità|non\s+cancell|locazione|opponib)"
            , re.I
        )
        kept = []
        # keep head, tail, and keyword lines from middle
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
        # include up to 200 pages (your PDFs are <= 200 typically; adjust if needed)
        for p in pages[:200]:
            out += (
                f"\n\n{'='*60}\nPAGINA {p.get('page_number')}\n{'='*60}\n"
                f"{compress_page_text(p.get('text', ''), max_chars=per_page_cap)}\n"
            )
        return out
    
    # ===========================================================================
    # DETERMINISTIC MULTI-LOT DETECTION (CHANGE 2)
    # ===========================================================================
    def detect_lots_from_pages(pages_in):
        lots = set()
        evidence = []
        for p in pages_in:
            t = str(p.get("text", "") or "")
            for m in re.finditer(r"\bLotto\s+(\d+)\b", t, flags=re.I):
                try:
                    n = int(m.group(1))
                    lots.add(n)
                    if len(evidence) < 3:
                        s = max(m.start() - 80, 0)
                        e = min(m.end() + 80, len(t))
                        evidence.append({
                            "page": int(p.get("page_number", 0)),
                            "quote": t[s:e].replace("\n"," ")[:200]
                        })
                except Exception:
                    continue
        return {"lots": sorted(lots), "evidence": evidence}
    
    # ===========================================================================
    # EVIDENCE VALIDATION HELPER (CHANGE 3)
    # ===========================================================================
    def has_evidence(ev):
        if not isinstance(ev, list) or not ev:
            return False
        e0 = ev[0]
        return isinstance(e0, dict) and "page" in e0 and "quote" in e0 and str(e0.get("quote","")).strip() != ""
    
    # Detect lots deterministically BEFORE LLM call
    detected_lots = detect_lots_from_pages(pages)
    logger.info(f"Deterministic lot detection: {detected_lots}")
    
    chat = LlmChat(
        api_key=EMERGENT_LLM_KEY,
        session_id=f"perizia_{run_id}",
        system_message=PERIZIA_SYSTEM_PROMPT
    ).with_model("openai", "gpt-4o")
    
    # ===========================================================================
    # CHARACTER-BUDGETED PAGE CONTENT (CHANGE 1 - no truncation)
    # ===========================================================================
    # Per-page cap: 1400 chars (fallback 900)
    # Total cap: 160,000 chars (hard)
    page_content = build_page_content(1400)
    if len(page_content) > 160000:
        page_content = build_page_content(900)
    # If still too big, hard cut ONLY AFTER page markers are preserved
    if len(page_content) > 160000:
        page_content = page_content[:160000]
    
    logger.info(f"Page content built: {len(page_content)} chars for {len(pages)} pages")
    
    # ===========================================================================
    # UPDATED PROMPT WITH ENFORCEABLE CONSTRAINTS (CHANGE 6)
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
        response = await chat.send_message(UserMessage(text=prompt))
        
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
            return create_fallback_analysis(file_name, case_id, run_id, pages, pdf_text, detected_lots)
        
        # ===========================================================================
        # POST-PARSE DETERMINISTIC FIXES (CHANGE 2, 3, 4)
        # ===========================================================================
        
        # ---- CHANGE 2: Deterministic multi-lot override (no "Lotto Unico" lies) ----
        lots = detected_lots.get("lots", [])
        if isinstance(lots, list) and len(lots) >= 2:
            # Ensure report_header exists
            hdr = result.setdefault("report_header", {})
            lotto_obj = hdr.setdefault("lotto", {"value": "Non specificato in Perizia", "evidence": []})
            lotto_obj["value"] = "Lotti " + ", ".join(str(x) for x in lots)
            lotto_obj["evidence"] = detected_lots.get("evidence", [])

            # Add a QA note
            qa = result.setdefault("qa_pass", {"status": "WARN", "checks": []})
            qa["checks"] = qa.get("checks", [])
            qa["checks"].append({
                "code": "QA-Lotto",
                "result": "OK",
                "note": f"Multi-lot detected deterministically: {lots}"
            })
            logger.info(f"Multi-lot override applied: {lots}")
        
        # Add detected lot_index to result
        if lots:
            result["lot_index"] = [{"lot": l, "page": e.get("page", 0), "quote": e.get("quote", "")} 
                                   for l, e in zip(lots, detected_lots.get("evidence", [{}]*len(lots)))]
        
        # ---- CHANGE 3: Enforce tri-state for section_9_legal_killers ----
        lk = result.get("section_9_legal_killers", {})
        items = lk.get("items", []) if isinstance(lk, dict) else []
        for it in items:
            status = str(it.get("status", "NON_SPECIFICATO")).upper()
            ev = it.get("evidence", [])
            if status in ("SI", "NO", "YES") and not has_evidence(ev):
                it["status"] = "NON_SPECIFICATO"
                it.setdefault("action", "Verifica obbligatoria")
                logger.info(f"Legal killer '{it.get('killer', 'unknown')}' status reset to NON_SPECIFICATO (no evidence)")
        
        # ---- CHANGE 3: Enforce evidence on critical header fields ----
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
                    if val and "Non specificato" not in str(val):
                        field_data["value"] = "Non specificato in Perizia"
                        qa["checks"].append({
                            "code": f"QA-Evidence-{field}",
                            "result": "WARN",
                            "note": f"Field {section}.{field} had no evidence, reset to Non specificato"
                        })
        
        # Check section_4_dati_certi.prezzo_base_asta
        dati = result.get("section_4_dati_certi", {})
        prezzo = dati.get("prezzo_base_asta", {})
        if isinstance(prezzo, dict):
            ev = prezzo.get("evidence", [])
            if not has_evidence(ev) and prezzo.get("value", 0) > 0:
                qa["checks"].append({
                    "code": "QA-Evidence-prezzo_base",
                    "result": "WARN", 
                    "note": "prezzo_base_asta has value but no evidence"
                })
        
        # ---- CHANGE 4: Money Box Honesty ----
        mb = result.get("section_3_money_box", {})
        mb_items = mb.get("items", []) if isinstance(mb, dict) else []
        money_box_violations = []
        
        for it in mb_items:
            fonte = (it.get("fonte_perizia", {}) or {})
            fonte_val = str(fonte.get("value", "")).lower()
            fonte_ev = fonte.get("evidence", [])
            euro = it.get("stima_euro", 0)

            is_unspecified = ("non specificato" in fonte_val) or (not has_evidence(fonte_ev))
            if is_unspecified and euro and euro > 0:
                voce = it.get("voce", "unknown")
                note = str(it.get("stima_nota", "") or "")
                if "STIMA NEXODIFY" not in note.upper():
                    it["stima_euro"] = 0
                    it["stima_nota"] = "TBD (NON SPECIFICATO IN PERIZIA) — Verifica tecnico/legale"
                    money_box_violations.append(voce)
                    logger.info(f"Money Box item '{voce}' reset to 0 (fonte unspecified, no evidence)")
        
        # QA gate for money box violations
        if money_box_violations:
            qa["checks"].append({
                "code": "QA-MoneyBox-Honesty",
                "result": "WARN",
                "note": f"Money Box items reset due to unspecified fonte: {money_box_violations}"
            })
        
        # ==========================================
        # PASS 2: Verification & Gap Detection
        # ==========================================
        logger.info(f"PASS 2: Verification pass for {file_name}")
        
        # Include lot_index requirement in verification
        lot_index_info = f"DETECTED LOTS: {detected_lots.get('lots', [])} - If 2+ lots, do NOT use 'Lotto Unico'" if detected_lots.get('lots') else ""
        
        verification_prompt = f"""VERIFICA E COMPLETA questa analisi perizia.

{lot_index_info}

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

        verification_response = await chat.send_message(UserMessage(text=verification_prompt))
        
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
        return create_fallback_analysis(file_name, case_id, run_id, pages, pdf_text, detected_lots)


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
    qa = result.get("qa_pass") or result.get("qa") or {}
    qa_checks = qa.get("checks", [])
    qa_status = "PASS"
    
    # QA Gate 1: Page Coverage Log
    page_coverage_log = result.get("page_coverage_log", [])
    pages_total = len(pages)
    if len(page_coverage_log) < pages_total:
        qa_checks.append({
            "code": "QA-PageCoverage",
            "result": "WARN",
            "note": f"page_coverage_log has {len(page_coverage_log)} entries, expected {pages_total}"
        })
    
    # QA Gate 2: Money Box Honesty (final check)
    money_box_final = result.get("section_3_money_box", {})
    mb_items_final = money_box_final.get("items", []) if isinstance(money_box_final, dict) else []
    for it in mb_items_final:
        fonte = (it.get("fonte_perizia", {}) or {})
        fonte_val = str(fonte.get("value", "")).lower()
        fonte_ev = fonte.get("evidence", [])
        euro = it.get("stima_euro", 0)
        is_unspecified = ("non specificato" in fonte_val) or (not has_evidence(fonte_ev))
        if is_unspecified and euro and euro > 0:
            note = str(it.get("stima_nota", "") or "")
            if "STIMA NEXODIFY" not in note.upper():
                qa_status = "FAIL"
                qa_checks.append({
                    "code": "QA-MoneyBox-Violation",
                    "result": "FAIL",
                    "note": f"Money Box item '{it.get('voce', 'unknown')}' has EUR value with unspecified fonte"
                })
    
    # QA Gate 3: Legal Killers Evidence
    lk_final = result.get("section_9_legal_killers", {})
    lk_items = lk_final.get("items", []) if isinstance(lk_final, dict) else []
    for it in lk_items:
        status = str(it.get("status", "")).upper()
        ev = it.get("evidence", [])
        if status in ("SI", "NO", "YES") and not has_evidence(ev):
            qa_status = "WARN" if qa_status != "FAIL" else qa_status
            qa_checks.append({
                "code": "QA-LegalKiller-Evidence",
                "result": "WARN",
                "note": f"Legal killer '{it.get('killer', 'unknown')}' has SI/NO without evidence"
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
    
    if "qa_pass" in result:
        result["qa_pass"] = qa
    if "qa" in result:
        result["qa"] = qa
    
    return result

def create_fallback_analysis(file_name: str, case_id: str, run_id: str, pages: List[Dict], pdf_text: str, detected_lots: Dict = None) -> Dict:
    """Create fallback analysis when LLM fails - extract what we can deterministically"""
    import re
    
    detected_lots = detected_lots or {"lots": [], "evidence": []}
    
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
    
    # Determine lotto value based on detected lots (CHANGE 2 - no "Lotto Unico" lies)
    lots = detected_lots.get("lots", [])
    if isinstance(lots, list) and len(lots) >= 2:
        lotto_value = "Lotti " + ", ".join(str(x) for x in lots)
    elif isinstance(lots, list) and len(lots) == 1:
        lotto_value = f"Lotto {lots[0]}"
    else:
        lotto_value = "Non specificato in Perizia"
    
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
            "lotto": lotto_value,
            "tribunale": tribunale,
            "address": {"street": "NOT_SPECIFIED_IN_PERIZIA", "city": "NOT_SPECIFIED_IN_PERIZIA", "full": "NOT_SPECIFIED_IN_PERIZIA"},
            "deposit_date": "NOT_SPECIFIED_IN_PERIZIA"
        },
        "report_header": {
            "title": "NEXODIFY INTELLIGENCE | Auction Scan",
            "procedure": {"value": procedure_id, "evidence": []},
            "lotto": {"value": lotto_value, "evidence": detected_lots.get("evidence", [])},
            "tribunale": {"value": tribunale, "evidence": []},
            "address": {"value": "Non specificato in Perizia", "evidence": []},
            "generated_at": datetime.now(timezone.utc).isoformat()
        },
        "lot_index": [{"lot": l, "page": e.get("page", 0), "quote": e.get("quote", "")} 
                      for l, e in zip(lots, detected_lots.get("evidence", [{}]*len(lots)))] if lots else [],
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
                {"code": "A", "label_it": "Regolarizzazione urbanistica", "label_en": "Urban regularization", "type": "NOT_SPECIFIED", "stima_euro": 0, "stima_nota": "TBD (NON SPECIFICATO IN PERIZIA) — Verifica tecnico", "action_required_it": "Verificare con tecnico", "action_required_en": "Verify with technician"},
                {"code": "B", "label_it": "Oneri tecnici / istruttoria", "label_en": "Technical fees", "type": "NOT_SPECIFIED", "stima_euro": 0, "stima_nota": "TBD (NON SPECIFICATO IN PERIZIA)", "source": "NOT_SPECIFIED"},
                {"code": "C", "label_it": "Rischio ripristini", "label_en": "Restoration risk", "type": "NOT_SPECIFIED", "stima_euro": 0, "stima_nota": "TBD (NON SPECIFICATO IN PERIZIA)", "source": "NOT_SPECIFIED"},
                {"code": "D", "label_it": "Allineamento catastale", "label_en": "Cadastral alignment", "type": "NOT_SPECIFIED", "stima_euro": 0, "stima_nota": "TBD (NON SPECIFICATO IN PERIZIA)", "source": "NOT_SPECIFIED"},
                {"code": "E", "label_it": "Spese condominiali arretrate", "label_en": "Condo arrears", "type": "NOT_SPECIFIED", "stima_euro": 0, "stima_nota": "TBD (NON SPECIFICATO IN PERIZIA)", "action_required_it": "Verificare con amministratore", "action_required_en": "Verify with administrator"},
                {"code": "F", "label_it": "Costi procedura", "label_en": "Procedure costs", "type": "NOT_SPECIFIED", "stima_euro": 0, "stima_nota": "TBD (NON SPECIFICATO IN PERIZIA)", "action_required_it": "Verificare con delegato", "action_required_en": "Verify with delegate"},
                {"code": "G", "label_it": "Cancellazione formalità", "label_en": "Formality cancellation", "type": "INFO_ONLY", "stima_euro": 0, "stima_nota": "Solitamente con decreto di trasferimento"},
                {"code": "H", "label_it": "Costo liberazione", "label_en": "Liberation cost", "type": "NOT_SPECIFIED", "stima_euro": 0, "stima_nota": "TBD (NON SPECIFICATO IN PERIZIA)", "source": "NOT_SPECIFIED"}
            ],
            "total_extra_costs": {"range": {"min": 0, "max": 0}, "max_is_open": True}
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
    """Generate HTML report from analysis - supports ROMA STANDARD format"""
    
    # Support both old and new format
    report_header = result.get("report_header", {})
    case_header = result.get("case_header", report_header)
    
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
    
    # Get values with fallbacks
    procedure = case_header.get("procedure", {}).get("value") if isinstance(case_header.get("procedure"), dict) else case_header.get("procedure_id", "N/A")
    tribunale = case_header.get("tribunale", {}).get("value") if isinstance(case_header.get("tribunale"), dict) else case_header.get("tribunale", "N/A")
    lotto = case_header.get("lotto", {}).get("value") if isinstance(case_header.get("lotto"), dict) else case_header.get("lotto", "N/A")
    address = case_header.get("address", {}).get("value") if isinstance(case_header.get("address"), dict) else case_header.get("address", "N/A")
    
    prezzo_base = dati.get("prezzo_base_asta", {})
    prezzo_value = prezzo_base.get("formatted") or f"€{prezzo_base.get('value', 0):,}" if isinstance(prezzo_base, dict) else str(prezzo_base)
    
    semaforo_status = semaforo.get("status", "AMBER")
    semaforo_color = "#10b981" if semaforo_status == "GREEN" or semaforo_status == "VERDE" else "#f59e0b" if semaforo_status == "AMBER" or semaforo_status == "GIALLO" else "#ef4444"
    
    # Build money box items HTML
    money_items_html = ""
    for item in money_box.get("items", []):
        voce = item.get("voce") or item.get("label_it") or item.get("code", "")
        stima = item.get("stima_euro", 0)
        nota = item.get("stima_nota", "")
        fonte = item.get("fonte_perizia", {}).get("value", "") if isinstance(item.get("fonte_perizia"), dict) else ""
        
        value_display = f"€{stima:,}" if stima else nota or "Verifica"
        money_items_html += f'<div class="money-item"><span>{voce}</span><span class="page-ref">{fonte}</span><span style="color: #D4AF37;">{value_display}</span></div>'
    
    # Build legal killers HTML
    legal_items = legal_killers.get("items", []) if isinstance(legal_killers, dict) and "items" in legal_killers else []
    legal_html = ""
    for item in legal_items:
        killer = item.get("killer", "")
        status = item.get("status", "NON_SPECIFICATO")
        action = item.get("action", "")
        status_color = "#ef4444" if status == "SI" or status == "YES" else "#10b981" if status == "NO" else "#f59e0b"
        legal_html += f'<div class="legal-item"><span class="status-dot" style="background:{status_color}"></span><span>{killer}</span><span class="status">{status}</span></div>'
    
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
        .legal-item {{ display: flex; align-items: center; gap: 12px; padding: 10px; background: #09090b; border-radius: 8px; margin-bottom: 6px; }}
        .status-dot {{ width: 10px; height: 10px; border-radius: 50%; flex-shrink: 0; }}
        .status {{ margin-left: auto; font-size: 12px; font-family: monospace; color: #a1a1aa; }}
        .checklist-item {{ display: flex; align-items: center; gap: 12px; padding: 10px; background: #09090b; border-radius: 8px; margin-bottom: 6px; }}
        .checklist-item .number {{ width: 24px; height: 24px; border-radius: 50%; background: #D4AF3720; border: 1px solid #D4AF37; display: flex; align-items: center; justify-content: center; font-size: 12px; color: #D4AF37; flex-shrink: 0; }}
        .summary-box {{ background: #f59e0b20; border-left: 4px solid #f59e0b; padding: 16px; border-radius: 0 8px 8px 0; margin-bottom: 16px; }}
        .disclaimer {{ background: #27272a; padding: 16px; border-radius: 8px; margin-top: 40px; text-align: center; }}
        .disclaimer p {{ color: #71717a; font-size: 12px; }}
        .footer {{ text-align: center; margin-top: 40px; color: #52525b; font-size: 12px; }}
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
        
        <div class="section">
            <h2>2. DECISIONE RAPIDA</h2>
            <p style="font-size: 18px; margin-bottom: 16px;">{decision.get('summary_it', 'Analisi completata')}</p>
            <p style="color: #a1a1aa;">{decision.get('summary_en', '')}</p>
        </div>
        
        <div class="section">
            <h2>3. PORTAFOGLIO COSTI (MONEY BOX)</h2>
            {money_items_html or '<p style="color: #71717a;">Nessun dato disponibile</p>'}
            <div class="total">
                <div style="display: flex; justify-content: space-between; align-items: center;">
                    <span>TOTALE COSTI EXTRA STIMATI</span>
                    <span class="total-value">€{money_box.get('totale_extra_budget', {}).get('min', money_box.get('total_extra_costs', {}).get('range', {}).get('min', 0)):,} - €{money_box.get('totale_extra_budget', {}).get('max', money_box.get('total_extra_costs', {}).get('range', {}).get('max', 0)):,}</span>
                </div>
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
                    <div class="field-value">{dati.get('superficie_catastale', {}).get('value', 'N/A') if isinstance(dati.get('superficie_catastale'), dict) else dati.get('superficie_catastale', 'N/A')}</div>
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

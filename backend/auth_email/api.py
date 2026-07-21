"""Public OTP endpoints: request a code, verify a code.

Both are unauthenticated and both are enumeration-safe: the response to a valid
request is byte-identical whether the address belongs to the owner, an active
beta tester, an existing Google customer or nobody at all. Nothing about account
state is returned before verification succeeds.

Verification takes ``challenge_id`` + code rather than email + code, so a
guesser cannot spray codes at an address without first holding the identifier
issued to that browser.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional

from fastapi import APIRouter, HTTPException, Request, Response
from pydantic import BaseModel, ConfigDict

from . import challenges, config, identity, ratelimit, sender as sender_module

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/auth/email", tags=["auth"])

# Public, pre-authentication capability probe. Separate router because it is
# scoped to "what can this deployment authenticate with", not to email OTP
# specifically, and a future provider would answer here too.
capabilities_router = APIRouter(prefix="/auth", tags=["auth"])

REQUEST_CODE_PATH = "/api/auth/email/request-code"
VERIFY_CODE_PATH = "/api/auth/email/verify-code"
CAPABILITIES_PATH = "/api/auth/capabilities"

# Customer-facing copy. Deliberately uniform: a caller must not be able to tell
# an unknown address from a known one, or a provider outage from a bad code.
MSG_CODE_SENT = "Se l'indirizzo email è valido, riceverai a breve un codice di accesso."
MSG_INVALID_EMAIL = "Inserisci un indirizzo email valido."
MSG_INVALID_CODE = "Il codice non è valido o è scaduto. Richiedine uno nuovo."
MSG_RATE_LIMITED = "Troppi tentativi. Attendi qualche minuto prima di riprovare."
MSG_DELIVERY_UNAVAILABLE = "Al momento non è possibile inviare il codice. Riprova più tardi."


class RequestCodePayload(BaseModel):
    model_config = ConfigDict(extra="ignore")
    email: str = ""


class VerifyCodePayload(BaseModel):
    model_config = ConfigDict(extra="ignore")
    challenge_id: str = ""
    code: str = ""


def _client_ip(request: Request) -> Optional[str]:
    forwarded = request.headers.get("x-forwarded-for") or ""
    if forwarded:
        return forwarded.split(",")[0].strip() or None
    return request.client.host if request.client else None


def _user_agent(request: Request) -> Optional[str]:
    return request.headers.get("user-agent")


async def _preflight_or_503() -> None:
    """Fail closed when a security prerequisite is missing.

    The operator gets the specific reason in the logs; the customer gets the
    same generic delivery message they would see during a provider outage.
    """
    import server  # type: ignore  # lazy: avoid circular import

    index_ready = await identity.unique_index_ready(server.db)
    result = config.preflight(index_ready=index_ready)
    if not result.ok:
        logger.warning("auth_email preflight refused: %s", result.reason_summary)
        raise HTTPException(status_code=503, detail=MSG_DELIVERY_UNAVAILABLE)


@capabilities_router.get("/capabilities")
async def capabilities() -> Dict[str, Any]:
    """Tell an unauthenticated client which login methods it may offer.

    Deliberately answers with the *same* preflight the OTP endpoints enforce,
    not with the raw ``AUTH_EMAIL_ENABLED`` value. A deployment where the flag
    is on but the pepper is missing would 503 on every request, so advertising
    the option there would put a permanently broken button in front of users.
    The capability is true only when a code would actually be sent.

    The payload carries booleans and nothing else: no provider name, no sender
    address, no key material, and no preflight reasons — those stay in the
    server log, because "sender_domain_not_verified" tells an attacker about
    deployment state they have no business knowing.

    Google is reported unconditionally: it is configured independently of this
    module and must never be hidden by an email-auth problem.
    """
    import server  # type: ignore  # lazy: avoid circular import

    try:
        index_ready = await identity.unique_index_ready(server.db)
        email_ok = config.preflight(index_ready=index_ready).ok
    except Exception as exc:  # pragma: no cover - defensive
        # Fail closed: an unreachable database hides the email option rather
        # than offering one that cannot work.
        logger.warning("auth_email capabilities probe failed: %s", exc)
        email_ok = False

    return {"email_otp_enabled": bool(email_ok), "google_enabled": True}


@router.post("/request-code")
async def request_code(request: Request, payload: RequestCodePayload) -> Dict[str, Any]:
    await _preflight_or_503()

    normalized = challenges.normalize_email(payload.email)
    if not challenges.is_valid_email(normalized):
        # Syntactic rejection only — this reveals nothing about registration.
        raise HTTPException(status_code=400, detail=MSG_INVALID_EMAIL)

    client_ip = _client_ip(request)

    if not await ratelimit.check_ip_hourly(client_ip):
        raise HTTPException(status_code=429, detail=MSG_RATE_LIMITED)

    allowed, retry_after = await ratelimit.check_cooldown(normalized)
    if not allowed:
        raise HTTPException(status_code=429, detail=MSG_RATE_LIMITED)

    if not await ratelimit.check_email_hourly(normalized):
        raise HTTPException(status_code=429, detail=MSG_RATE_LIMITED)

    active_sender = sender_module.get_sender()
    if active_sender is None:
        logger.warning("auth_email sender unavailable for provider=%s", config.provider_name())
        raise HTTPException(status_code=503, detail=MSG_DELIVERY_UNAVAILABLE)

    # Release an abandoned-but-expired slot so a legitimate retry is not blocked
    # until the TTL monitor happens to run.
    await challenges.expire_if_stale(normalized)

    challenge, code, error = await challenges.create(
        normalized_email=normalized,
        request_ip=client_ip,
        user_agent=_user_agent(request),
    )
    if error == "ACTIVE_CHALLENGE_EXISTS" or challenge is None:
        # A concurrent request won the active slot. Answer exactly as the
        # cooldown path does, so the loser learns nothing from the difference.
        raise HTTPException(status_code=429, detail=MSG_RATE_LIMITED)

    challenge_id = challenge["challenge_id"]
    await challenges.mark_send_pending(challenge_id, provider=active_sender.name)

    # The plaintext code exists only in this call frame. Any same-code retry has
    # to happen here, inside the sender, because nothing recoverable is stored.
    result = await active_sender.send_login_code(
        to=normalized,
        code=code,
        idempotency_key=challenge["idempotency_key"],
        ttl_seconds=config.code_ttl_seconds(),
    )
    await challenges.record_send_result(challenge_id, result)

    if result.definitive_failure:
        # The provider will never accept this message; the challenge is already
        # terminal and unverifiable. Never claim a delivery that did not happen.
        logger.warning(
            "auth_email delivery refused challenge=%s reason=%s",
            challenge_id,
            result.failure_category,
        )
        raise HTTPException(status_code=502, detail=MSG_DELIVERY_UNAVAILABLE)

    # OK or ambiguous: the message may well have arrived, so the challenge stays
    # verifiable and the customer sees the normal message.
    return {
        "challenge_id": challenge_id,
        "expires_in": config.code_ttl_seconds(),
        "resend_available_in": config.resend_cooldown_seconds(),
        "message": MSG_CODE_SENT,
    }


@router.post("/verify-code")
async def verify_code(
    request: Request, response: Response, payload: VerifyCodePayload
) -> Dict[str, Any]:
    await _preflight_or_503()

    import server  # type: ignore  # lazy: avoid circular import

    challenge_id = str(payload.challenge_id or "").strip()
    code = str(payload.code or "").strip()

    if not challenge_id or not code.isdigit() or len(code) != 6:
        raise HTTPException(status_code=400, detail=MSG_INVALID_CODE)

    outcome, claimed = await challenges.consume(challenge_id, code)

    if outcome == challenges.RESULT_LOCKED:
        raise HTTPException(status_code=429, detail=MSG_RATE_LIMITED)
    if outcome != challenges.RESULT_OK or not claimed:
        raise HTTPException(status_code=400, detail=MSG_INVALID_CODE)

    verified_email = claimed.get("normalized_email") or ""

    # From here the flow is identical to Google: the same chokepoint resolves or
    # creates the user, links any pending beta membership, and mints the same
    # session cookie. Authorization is recomputed per request from the email, so
    # owner and beta status follow the account, not the login method.
    user, session_token = await server._create_local_login(
        email=verified_email,
        name=None,
        picture=None,
        response=response,
        auth_method=identity.METHOD_EMAIL_OTP,
        email_verified=True,
    )

    return {
        "user": server._build_user_response(user),
        "session_token": session_token,
    }

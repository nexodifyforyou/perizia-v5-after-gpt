"""Pure-function tests: normalization, validation, hashing, templates, config."""

import os
import sys

import pytest

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from auth_email import challenges, config, templates  # noqa: E402
from auth_email import identity  # noqa: E402
from tests.auth_email_helpers import TEST_PEPPER, apply_test_env  # noqa: E402


# ---------------------------------------------------------------------------
# Normalization — conservative by contract
# ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    "raw,expected",
    [
        ("  Mario.Rossi@Example.IT  ", "mario.rossi@example.it"),
        ("UTENTE@EXAMPLE-MS365.ONMICROSOFT.COM", "utente@example-ms365.onmicrosoft.com"),
        ("name@example.com", "name@example.com"),
    ],
)
def test_normalization_trims_and_lowercases(raw, expected):
    assert challenges.normalize_email(raw) == expected


def test_plus_addressing_and_dots_are_preserved():
    """name@ and name+beta@ must remain distinct identities."""
    plain = challenges.normalize_email("name@example.com")
    plussed = challenges.normalize_email("name+beta@example.com")
    dotted = challenges.normalize_email("n.a.m.e@example.com")

    assert plussed == "name+beta@example.com"
    assert plain != plussed
    assert dotted == "n.a.m.e@example.com"
    assert dotted != plain


def test_gmail_specific_rules_are_not_applied():
    """No dot-stripping, no plus-stripping, even for gmail.com."""
    assert challenges.normalize_email("a.b+tag@gmail.com") == "a.b+tag@gmail.com"


def test_identity_and_challenge_normalization_agree():
    """One rule across the codebase, or a membership and a user could diverge."""
    from beta_program.store import normalize_beta_email

    for raw in ("  A.B@Example.IT ", "x+y@corp.example.com"):
        assert (
            challenges.normalize_email(raw)
            == identity.normalize_email(raw)
            == normalize_beta_email(raw)
        )


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    "value",
    [
        "user@example.com",
        "mario.rossi@studio-example.it",
        "utente@example-ms365.onmicrosoft.com",
        "name+beta@example.co.uk",
        "info@pec-example.aruba.it",
    ],
)
def test_valid_business_addresses_accepted(value):
    """Any provider works: Microsoft, Aruba, custom corporate, Google alike."""
    assert challenges.is_valid_email(value) is True


@pytest.mark.parametrize(
    "value",
    [
        "",
        "   ",
        "no-at-sign",
        "two@@example.com",
        "user@",
        "@example.com",
        "user@example",
        "user@.com",
        "user..name@example.com",
        "user@exa mple.com",
        "user\n@example.com",
        "user\x00@example.com",
        "a" * 250 + "@example.com",
    ],
)
def test_invalid_addresses_rejected(value):
    assert challenges.is_valid_email(value) is False


def test_control_characters_rejected():
    assert challenges.is_valid_email("user@exam\rple.com") is False


# ---------------------------------------------------------------------------
# Code generation and hashing
# ---------------------------------------------------------------------------
def test_generated_code_is_six_digits():
    for _ in range(200):
        code = challenges.generate_code()
        assert len(code) == 6
        assert code.isdigit()


def test_generated_codes_vary():
    """Not a randomness proof; catches a constant or badly seeded generator."""
    seen = {challenges.generate_code() for _ in range(200)}
    assert len(seen) > 50


def test_hash_requires_pepper(monkeypatch):
    monkeypatch.setenv("AUTH_EMAIL_CODE_PEPPER", "")
    with pytest.raises(RuntimeError):
        challenges.hash_code("123456", "salt")


def test_hash_rejects_short_pepper(monkeypatch):
    monkeypatch.setenv("AUTH_EMAIL_CODE_PEPPER", "tooshort")
    with pytest.raises(RuntimeError):
        challenges.hash_code("123456", "salt")


def test_hash_is_deterministic_and_salt_dependent(monkeypatch):
    monkeypatch.setenv("AUTH_EMAIL_CODE_PEPPER", TEST_PEPPER)
    a = challenges.hash_code("123456", "salt-a")
    b = challenges.hash_code("123456", "salt-a")
    c = challenges.hash_code("123456", "salt-b")
    assert a == b
    assert a != c


def test_hash_does_not_contain_plaintext(monkeypatch):
    monkeypatch.setenv("AUTH_EMAIL_CODE_PEPPER", TEST_PEPPER)
    digest = challenges.hash_code("123456", "salt")
    assert "123456" not in digest


def test_hash_depends_on_pepper(monkeypatch):
    """Two deployments with different pepper must not share valid hashes."""
    monkeypatch.setenv("AUTH_EMAIL_CODE_PEPPER", TEST_PEPPER)
    first = challenges.hash_code("123456", "salt")
    monkeypatch.setenv("AUTH_EMAIL_CODE_PEPPER", TEST_PEPPER + "-different")
    assert challenges.hash_code("123456", "salt") != first


def test_verify_code_hash_matches_and_rejects(monkeypatch):
    monkeypatch.setenv("AUTH_EMAIL_CODE_PEPPER", TEST_PEPPER)
    digest = challenges.hash_code("654321", "s")
    assert challenges.verify_code_hash("654321", "s", digest) is True
    assert challenges.verify_code_hash("654320", "s", digest) is False


def test_idempotency_key_derives_from_challenge_id_only(monkeypatch):
    monkeypatch.setenv("AUTH_EMAIL_CODE_PEPPER", TEST_PEPPER)
    key = challenges.build_idempotency_key("aec_abc123")
    assert key == challenges.build_idempotency_key("aec_abc123")
    assert key != challenges.build_idempotency_key("aec_other")
    # The key must not leak the code or the address.
    assert "aec_abc123" not in key


def test_identifier_hash_is_one_way(monkeypatch):
    monkeypatch.setenv("AUTH_EMAIL_CODE_PEPPER", TEST_PEPPER)
    hashed = challenges.hash_identifier("203.0.113.10")
    assert hashed and "203.0.113.10" not in hashed
    assert challenges.hash_identifier(None) is None


# ---------------------------------------------------------------------------
# Template — must carry no account information
# ---------------------------------------------------------------------------
def test_subject_matches_specification():
    assert templates.SUBJECT == "Il tuo codice di accesso a Perizia Scan"


def test_body_contains_code_and_expiry():
    text = templates.render_text("123456", 600)
    assert "123456" in text
    assert "10 minuti" in text
    assert "una sola volta" in text


def test_body_discloses_no_account_state():
    """No beta status, credits, reports, or hint that the account exists."""
    for body in (templates.render_text("123456", 600), templates.render_html("123456", 600)):
        lowered = body.lower()
        for forbidden in (
            "beta",
            "credit",
            "perizia scan pro",
            "abbonamento",
            "report",
            "admin",
            "quota",
            "saldo",
        ):
            assert forbidden not in lowered


def test_body_tells_recipient_not_to_reply():
    assert "non accetta risposte" in templates.render_text("123456", 600)


def test_body_uses_correctly_accented_italian():
    """Guards against ASCII-stripped copy reaching a real recipient."""
    text = templates.render_text("123456", 600)
    assert "può essere utilizzato" in text
    assert "Questo messaggio è inviato" in text
    # The bare forms would be misspelt Italian.
    assert "puo essere" not in text
    assert "messaggio e inviato" not in text


def test_html_body_escapes_accents_for_mail_clients():
    html = templates.render_html("123456", 600)
    assert "pu&ograve;" in html
    assert "&egrave;" in html


def test_customer_facing_messages_use_accented_italian():
    """The API copy is shown verbatim in the UI, so it must be correct Italian."""
    from auth_email import api as auth_api

    assert auth_api.MSG_INVALID_CODE == (
        "Il codice non è valido o è scaduto. Richiedine uno nuovo."
    )
    assert auth_api.MSG_DELIVERY_UNAVAILABLE == (
        "Al momento non è possibile inviare il codice. Riprova più tardi."
    )
    assert auth_api.MSG_RATE_LIMITED == (
        "Troppi tentativi. Attendi qualche minuto prima di riprovare."
    )
    for message in (
        auth_api.MSG_CODE_SENT,
        auth_api.MSG_INVALID_EMAIL,
        auth_api.MSG_INVALID_CODE,
        auth_api.MSG_RATE_LIMITED,
        auth_api.MSG_DELIVERY_UNAVAILABLE,
    ):
        # No message may disclose account state.
        lowered = message.lower()
        for forbidden in ("beta", "admin", "registrat", "google", "esiste"):
            assert forbidden not in lowered


def test_html_body_contains_code():
    assert "123456" in templates.render_html("123456", 600)


# ---------------------------------------------------------------------------
# Config preflight — fail closed
# ---------------------------------------------------------------------------
def test_preflight_passes_with_full_config(monkeypatch):
    apply_test_env(monkeypatch)
    assert config.preflight(index_ready=True).ok is True


def test_preflight_fails_without_index(monkeypatch):
    apply_test_env(monkeypatch)
    result = config.preflight(index_ready=False)
    assert result.ok is False
    assert "normalized_email_unique_index_missing" in result.reasons


def test_preflight_fails_without_pepper(monkeypatch):
    apply_test_env(monkeypatch, AUTH_EMAIL_CODE_PEPPER="")
    result = config.preflight(index_ready=True)
    assert result.ok is False
    assert "pepper_missing_or_too_short" in result.reasons


def test_preflight_fails_with_short_pepper(monkeypatch):
    apply_test_env(monkeypatch, AUTH_EMAIL_CODE_PEPPER="short")
    assert config.preflight(index_ready=True).ok is False


def test_preflight_fails_when_disabled(monkeypatch):
    apply_test_env(monkeypatch, AUTH_EMAIL_ENABLED="false")
    result = config.preflight(index_ready=True)
    assert result.ok is False
    assert "feature_disabled" in result.reasons


def test_preflight_resend_requires_key_and_verified_domain(monkeypatch):
    apply_test_env(
        monkeypatch,
        AUTH_EMAIL_PROVIDER="resend",
        RESEND_API_KEY="",
        AUTH_EMAIL_SENDER_DOMAIN_VERIFIED="false",
    )
    result = config.preflight(index_ready=True)
    assert result.ok is False
    assert "resend_api_key_missing" in result.reasons
    assert "sender_domain_not_verified" in result.reasons


def test_preflight_resend_ok_when_fully_configured(monkeypatch):
    apply_test_env(
        monkeypatch,
        AUTH_EMAIL_PROVIDER="resend",
        RESEND_API_KEY="re_test_key_not_real",
        AUTH_EMAIL_SENDER_DOMAIN_VERIFIED="true",
    )
    assert config.preflight(index_ready=True).ok is True


def test_preflight_fails_without_from_address(monkeypatch):
    apply_test_env(monkeypatch, AUTH_EMAIL_FROM="")
    result = config.preflight(index_ready=True)
    assert result.ok is False
    assert "from_address_missing" in result.reasons


def test_expiry_and_purge_are_separate_settings(monkeypatch):
    """Authentication validity and data retention are different boundaries."""
    apply_test_env(monkeypatch)
    assert config.code_ttl_seconds() == 600
    assert config.purge_after_seconds() == 172800
    assert config.purge_after_seconds() > config.code_ttl_seconds()

import os
import sys
import json
import copy
from datetime import datetime, timezone

import httpx
import pytest

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import server as server
from test_admin import FakeDB, _seed_session


@pytest.fixture()
def fake_db(monkeypatch):
    fake_db = FakeDB()
    monkeypatch.setattr(server, "db", fake_db)
    server.MASTER_ADMIN_EMAIL = "admin@nexodify.com"
    server.STRIPE_SECRET_KEY = "sk_test"
    server.STRIPE_WEBHOOK_SECRET = "whsec_test"
    server.STRIPE_PRICE_STARTER = "price_starter_env"
    server.STRIPE_PRICE_PACK_8 = "price_pack_8_env"
    server.STRIPE_PRICE_SOLO = "price_solo_env"
    server.STRIPE_PRICE_PRO = "price_pro_env"
    server.STRIPE_SUCCESS_URL = "http://frontend.test/billing"
    server.STRIPE_CANCEL_URL = "http://frontend.test/billing"
    return fake_db


def _seed_pending_checkout(fake_db, *, user_id: str, plan_id: str, session_id: str, transaction_id: str, billing_record_id: str):
    fake_db.payment_transactions.items.append({
        "transaction_id": transaction_id,
        "user_id": user_id,
        "session_id": session_id,
        "plan_id": plan_id,
        "amount": 49.0,
        "currency": "eur",
        "status": "pending",
        "payment_status": "initiated",
        "created_at": datetime.now(timezone.utc).isoformat(),
    })
    fake_db.billing_records.items.append({
        "billing_record_id": billing_record_id,
        "user_id": user_id,
        "user_email": "checkout@example.com",
        "customer_type": "individual",
        "customer_name": "Checkout User",
        "billing_email": "checkout@example.com",
        "country_code": "IT",
        "plan_id": plan_id,
        "purchase_type": "pack" if plan_id == "starter" else "subscription",
        "amount_subtotal": 49.0,
        "amount_tax": 0.0,
        "amount_total": 49.0,
        "currency": "eur",
        "status": "pending",
        "payment_provider": "stripe",
        "payment_reference": None,
        "checkout_reference": session_id,
        "invoice_status": "pending",
        "invoice_number": None,
        "invoice_reference": None,
        "description_it": f"Checkout Stripe {plan_id}",
        "metadata": {
            "checkout_session_id": session_id,
            "payment_transaction_id": transaction_id,
            "plan_code": plan_id,
        },
        "created_at": datetime.now(timezone.utc).isoformat(),
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "paid_at": None,
    })


def _user_doc(fake_db, user_id="user_checkout"):
    return next(item for item in fake_db.users.items if item["user_id"] == user_id)


def _perizia_wallet(fake_db, user_id="user_checkout"):
    return _user_doc(fake_db, user_id).get("perizia_credits") or {}


def _subscription_state(fake_db, user_id="user_checkout"):
    return _user_doc(fake_db, user_id).get("subscription_state") or {}


def _seed_default_checkout_user(fake_db, *, plan="free", credits=4):
    return _seed_session(fake_db, {
        "user_id": "user_checkout",
        "email": "checkout@example.com",
        "name": "Checkout User",
        "plan": plan,
        "is_master_admin": False,
        "quota": {
            "perizia_scans_remaining": credits,
            "image_scans_remaining": 0,
            "assistant_messages_remaining": 0,
        },
    })


def _ledger_entries(fake_db, *, entry_type=None, reference_type=None, reference_id=None, user_id=None):
    entries = list(fake_db.credit_ledger.items)
    if entry_type is not None:
        entries = [item for item in entries if item["entry_type"] == entry_type]
    if reference_type is not None:
        entries = [item for item in entries if item["reference_type"] == reference_type]
    if reference_id is not None:
        entries = [item for item in entries if item["reference_id"] == reference_id]
    if user_id is not None:
        entries = [item for item in entries if item["user_id"] == user_id]
    return entries


def _billing_records(fake_db, *, user_id=None, plan_id=None, checkout_reference=None, invoice_reference=None):
    records = list(fake_db.billing_records.items)
    if user_id is not None:
        records = [item for item in records if item["user_id"] == user_id]
    if plan_id is not None:
        records = [item for item in records if item["plan_id"] == plan_id]
    if checkout_reference is not None:
        records = [item for item in records if item["checkout_reference"] == checkout_reference]
    if invoice_reference is not None:
        records = [item for item in records if item["invoice_reference"] == invoice_reference]
    return records


def _assert_wallet_invariants(wallet, *, user_doc=None):
    assert wallet["total_available"] == wallet["monthly_remaining"] + wallet["extra_remaining"]
    assert wallet["extra_remaining"] == sum(int(item.get("amount_remaining", 0) or 0) for item in wallet["pack_grants"])
    assert wallet["monthly_remaining"] >= 0
    assert wallet["extra_remaining"] >= 0
    assert wallet["total_available"] >= 0
    assert len(wallet["processed_invoice_ids"]) == len(set(wallet["processed_invoice_ids"]))
    for grant in wallet["pack_grants"]:
        assert grant["amount_granted"] >= 0
        assert grant["amount_remaining"] >= 0
        assert grant["amount_remaining"] <= grant["amount_granted"]
    if user_doc is not None:
        assert user_doc["quota"]["perizia_scans_remaining"] == wallet["total_available"]
        monthly_cap = server._monthly_perizia_quota_for_plan(user_doc.get("plan"))
        if user_doc.get("plan") in {"solo", "pro", "studio"}:
            assert wallet["monthly_remaining"] <= monthly_cap


def _make_wallet(*, monthly_remaining=0, extra_grants=None, plan_id="free", processed_invoice_ids=None, monthly_refreshed_at=None):
    extra_grants = extra_grants or []
    wallet = {
        "monthly_remaining": monthly_remaining,
        "extra_remaining": sum(int(item["amount_remaining"]) for item in extra_grants),
        "monthly_plan_id": plan_id if plan_id in {"solo", "pro"} else None,
        "monthly_refreshed_at": monthly_refreshed_at,
        "pack_grants": list(extra_grants),
        "processed_invoice_ids": list(processed_invoice_ids or []),
    }
    return server._finalize_perizia_credit_wallet(wallet, plan_id=plan_id, is_master_admin=False)


def _extra_grant(*, amount, grant_id, source="manual_seed", reference_id=None, expires_at=None, granted_at="2026-01-01T00:00:00+00:00"):
    return server._make_pack_grant(
        amount=amount,
        amount_remaining=amount,
        source=source,
        plan_code="starter",
        reference_id=reference_id,
        grant_id=grant_id,
        granted_at=granted_at,
        expires_at=expires_at,
    )


def _assert_wallet_exact(fake_db, *, user_id="user_checkout", monthly_remaining, extra_remaining, total_available, monthly_plan_id, processed_invoice_ids, pack_grants):
    user_doc = _user_doc(fake_db, user_id)
    wallet = _perizia_wallet(fake_db, user_id)
    assert wallet["monthly_remaining"] == monthly_remaining
    assert wallet["extra_remaining"] == extra_remaining
    assert wallet["total_available"] == total_available
    assert wallet["monthly_plan_id"] == monthly_plan_id
    assert wallet["processed_invoice_ids"] == processed_invoice_ids
    assert len(wallet["pack_grants"]) == len(pack_grants)
    for grant, expected in zip(wallet["pack_grants"], pack_grants):
        for key, value in expected.items():
            assert grant[key] == value
    _assert_wallet_invariants(wallet, user_doc=user_doc)
    return wallet


def _assert_no_unexpected_ledger_entries(fake_db, *, allowed_entry_types, user_id=None):
    entries = _ledger_entries(fake_db, user_id=user_id)
    unexpected = [item for item in entries if item["entry_type"] not in set(allowed_entry_types)]
    assert unexpected == []


def _snapshot_state(fake_db):
    return {
        "users": copy.deepcopy(fake_db.users.items),
        "payment_transactions": copy.deepcopy(fake_db.payment_transactions.items),
        "billing_records": copy.deepcopy(fake_db.billing_records.items),
        "credit_ledger": copy.deepcopy(fake_db.credit_ledger.items),
    }


def _assert_state_unchanged(fake_db, snapshot):
    assert _snapshot_state(fake_db) == snapshot


def _assert_checkout_side_effects_absent(fake_db, snapshot):
    current = _snapshot_state(fake_db)
    assert current["payment_transactions"] == snapshot["payment_transactions"]
    assert current["billing_records"] == snapshot["billing_records"]


def _stripe_event(event_type, obj):
    return {"type": event_type, "data": {"object": obj}}


@pytest.mark.anyio
async def test_create_checkout_uses_env_price_ids_modes_and_card_only(fake_db, monkeypatch):
    captured = []

    class FakeStripeSessionApi:
        @staticmethod
        def create(**kwargs):
            captured.append(kwargs)
            session_id = f"cs_test_{len(captured)}"
            return type("CheckoutSession", (), {"id": session_id, "url": f"https://stripe.test/{session_id}"})()

    class FakeStripeModule:
        api_key = None
        checkout = type("CheckoutNamespace", (), {"Session": FakeStripeSessionApi})

    monkeypatch.setitem(sys.modules, "stripe", FakeStripeModule)

    session_token = _seed_default_checkout_user(fake_db)

    transport = httpx.ASGITransport(app=server.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        pack_response = await client.post(
            "/api/checkout/create",
            json={"plan_id": "starter", "origin_url": "http://frontend.test"},
            headers={"Authorization": f"Bearer {session_token}"},
        )
        solo_response = await client.post(
            "/api/checkout/create",
            json={"plan_id": "solo", "origin_url": "http://frontend.test"},
            headers={"Authorization": f"Bearer {session_token}"},
        )

    assert pack_response.status_code == 200
    assert solo_response.status_code == 200
    assert captured[0]["mode"] == "payment"
    assert captured[0]["line_items"] == [{"price": "price_pack_8_env", "quantity": 1}]
    assert captured[0]["payment_method_types"] == ["card"]
    assert captured[0]["metadata"]["plan_code"] == "starter"
    assert "subscription_data" not in captured[0]
    assert captured[1]["mode"] == "subscription"
    assert captured[1]["line_items"] == [{"price": "price_solo_env", "quantity": 1}]
    assert captured[1]["payment_method_types"] == ["card"]
    assert captured[1]["subscription_data"]["metadata"]["app_user_id"] == "user_checkout"


@pytest.mark.anyio
async def test_active_recurring_plan_guard_blocks_new_subscription_but_allows_pack(fake_db, monkeypatch):
    captured = []

    class FakeStripeSessionApi:
        @staticmethod
        def create(**kwargs):
            captured.append(kwargs)
            return type("CheckoutSession", (), {"id": "cs_pack", "url": "https://stripe.test/cs_pack"})()

    class FakeStripeModule:
        api_key = None
        checkout = type("CheckoutNamespace", (), {"Session": FakeStripeSessionApi})

    monkeypatch.setitem(sys.modules, "stripe", FakeStripeModule)

    session_token = _seed_default_checkout_user(fake_db, plan="pro", credits=99)

    transport = httpx.ASGITransport(app=server.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        solo_response = await client.post(
            "/api/checkout/create",
            json={"plan_id": "solo", "origin_url": "http://frontend.test"},
            headers={"Authorization": f"Bearer {session_token}"},
        )
        pack_response = await client.post(
            "/api/checkout/create",
            json={"plan_id": "starter", "origin_url": "http://frontend.test"},
            headers={"Authorization": f"Bearer {session_token}"},
        )

    assert solo_response.status_code == 409
    assert "abbonamento mensile gestito" in solo_response.json()["detail"]
    assert pack_response.status_code == 200
    assert len(captured) == 1
    assert captured[0]["metadata"]["plan_code"] == "starter"


@pytest.mark.anyio
async def test_checkout_status_is_read_only_and_session_specific(fake_db, monkeypatch):
    class FakeStripeSessionApi:
        @staticmethod
        def retrieve(session_id):
            if session_id == "cs_test_success":
                return type("CheckoutStatus", (), {
                    "status": "complete",
                    "payment_status": "paid",
                    "amount_total": 4900,
                    "currency": "eur",
                    "mode": "payment",
                })()
            return type("CheckoutStatus", (), {
                "status": "complete",
                "payment_status": "paid",
                "amount_total": 4900,
                "currency": "eur",
                "mode": "payment",
            })()

    class FakeStripeModule:
        api_key = None
        checkout = type("CheckoutNamespace", (), {"Session": FakeStripeSessionApi})

    monkeypatch.setitem(sys.modules, "stripe", FakeStripeModule)

    session_token = _seed_default_checkout_user(fake_db)
    _seed_pending_checkout(fake_db, user_id="user_checkout", plan_id="starter", session_id="cs_test_success", transaction_id="txn_success", billing_record_id="bill_success")
    _seed_pending_checkout(fake_db, user_id="user_checkout", plan_id="starter", session_id="cs_test_failed", transaction_id="txn_failed", billing_record_id="bill_failed")
    fake_db.billing_records.items[0]["status"] = "paid"
    fake_db.billing_records.items[0]["invoice_status"] = "ready"
    fake_db.billing_records.items[0]["metadata"]["entitlement_granted"] = True
    fake_db.billing_records.items[1]["status"] = "failed"
    fake_db.billing_records.items[1]["invoice_status"] = "failed"

    transport = httpx.ASGITransport(app=server.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        success_response = await client.get("/api/checkout/status/cs_test_success", headers={"Authorization": f"Bearer {session_token}"})
        failed_response = await client.get("/api/checkout/status/cs_test_failed", headers={"Authorization": f"Bearer {session_token}"})

    assert success_response.status_code == 200
    assert failed_response.status_code == 200
    assert success_response.json()["session_result"] == "success"
    assert failed_response.json()["session_result"] == "failed"
    assert _user_doc(fake_db)["quota"]["perizia_scans_remaining"] == 4
    non_baseline_entries = [item for item in fake_db.credit_ledger.items if item["entry_type"] != "opening_balance"]
    assert non_baseline_entries == []


@pytest.mark.anyio
async def test_starter_webhook_does_not_grant_when_payment_intent_not_succeeded(fake_db, monkeypatch):
    next_event = {}

    class FakeWebhook:
        @staticmethod
        def construct_event(payload, sig_header, secret):
            assert payload == b"{}"
            assert sig_header == "sig_test"
            assert secret == "whsec_test"
            return next_event

    class FakePaymentIntentApi:
        @staticmethod
        def retrieve(payment_intent_id):
            assert payment_intent_id == "pi_starter_auth"
            return {"id": payment_intent_id, "status": "requires_action"}

    class FakeStripeModule:
        api_key = None
        Webhook = FakeWebhook
        PaymentIntent = FakePaymentIntentApi

    monkeypatch.setitem(sys.modules, "stripe", FakeStripeModule)

    _seed_default_checkout_user(fake_db)
    _seed_pending_checkout(
        fake_db,
        user_id="user_checkout",
        plan_id="starter",
        session_id="cs_test_starter_auth",
        transaction_id="txn_starter_auth",
        billing_record_id="bill_starter_auth",
    )

    next_event = {
        "type": "checkout.session.completed",
        "data": {
            "object": {
                "id": "cs_test_starter_auth",
                "status": "complete",
                "payment_status": "paid",
                "payment_intent": "pi_starter_auth",
                "metadata": {
                    "app_user_id": "user_checkout",
                    "plan_code": "starter",
                    "billing_reason": "checkout_session_create",
                },
            }
        },
    }

    transport = httpx.ASGITransport(app=server.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post("/api/webhook/stripe", content=b"{}", headers={"Stripe-Signature": "sig_test"})

    assert response.status_code == 200
    assert _user_doc(fake_db)["quota"]["perizia_scans_remaining"] == 4
    assert [item for item in fake_db.credit_ledger.items if item["entry_type"] == "plan_purchase"] == []
    assert fake_db.billing_records.items[0]["status"] == "failed"
    assert fake_db.billing_records.items[0]["invoice_status"] == "failed"


@pytest.mark.anyio
async def test_pack_purchase_increases_extra_bucket_and_is_idempotent(fake_db, monkeypatch):
    next_event = {}

    class FakeWebhook:
        @staticmethod
        def construct_event(payload, sig_header, secret):
            assert payload == b"{}"
            assert sig_header == "sig_test"
            assert secret == "whsec_test"
            return next_event

    class FakePaymentIntentApi:
        @staticmethod
        def retrieve(payment_intent_id):
            assert payment_intent_id == "pi_starter"
            return {"id": payment_intent_id, "status": "succeeded"}

    class FakeStripeModule:
        api_key = None
        Webhook = FakeWebhook
        PaymentIntent = FakePaymentIntentApi

    monkeypatch.setitem(sys.modules, "stripe", FakeStripeModule)

    _seed_default_checkout_user(fake_db)
    _seed_pending_checkout(
        fake_db,
        user_id="user_checkout",
        plan_id="starter",
        session_id="cs_test_starter",
        transaction_id="txn_starter",
        billing_record_id="bill_starter",
    )

    next_event = {
        "type": "checkout.session.completed",
        "data": {
            "object": {
                "id": "cs_test_starter",
                "status": "complete",
                "payment_status": "paid",
                "payment_intent": "pi_starter",
                "metadata": {
                    "app_user_id": "user_checkout",
                    "plan_code": "starter",
                    "billing_reason": "checkout_session_create",
                },
            }
        },
    }

    transport = httpx.ASGITransport(app=server.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        first = await client.post("/api/webhook/stripe", content=b"{}", headers={"Stripe-Signature": "sig_test"})
        second = await client.post("/api/webhook/stripe", content=b"{}", headers={"Stripe-Signature": "sig_test"})

    wallet = _perizia_wallet(fake_db)
    assert first.status_code == 200
    assert second.status_code == 200
    assert _user_doc(fake_db)["quota"]["perizia_scans_remaining"] == 12
    assert wallet["monthly_remaining"] == 0
    assert wallet["extra_remaining"] == 12
    assert wallet["total_available"] == 12
    assert len(wallet["pack_grants"]) == 2
    assert wallet["pack_grants"][0]["source"] == "legacy_migration"
    assert wallet["pack_grants"][0]["amount_remaining"] == 4
    assert wallet["pack_grants"][1]["reference_id"] == "cs_test_starter"
    assert wallet["pack_grants"][1]["amount_remaining"] == 8
    assert wallet["pack_grants"][1]["expires_at"] is not None
    ledger_entries = [item for item in fake_db.credit_ledger.items if item["entry_type"] == "plan_purchase"]
    assert len(ledger_entries) == 1
    assert ledger_entries[0]["reference_id"] == "cs_test_starter"
    assert fake_db.billing_records.items[0]["status"] == "paid"
    _assert_wallet_invariants(wallet, user_doc=_user_doc(fake_db))


@pytest.mark.anyio
async def test_starter_webhook_skips_when_user_resolution_conflicts(fake_db, monkeypatch):
    next_event = {}

    class FakeWebhook:
        @staticmethod
        def construct_event(payload, sig_header, secret):
            assert payload == b"{}"
            assert sig_header == "sig_test"
            assert secret == "whsec_test"
            return next_event

    class FakePaymentIntentApi:
        @staticmethod
        def retrieve(payment_intent_id):
            assert payment_intent_id == "pi_starter_conflict"
            return {"id": payment_intent_id, "status": "succeeded"}

    class FakeStripeModule:
        api_key = None
        Webhook = FakeWebhook
        PaymentIntent = FakePaymentIntentApi

    monkeypatch.setitem(sys.modules, "stripe", FakeStripeModule)

    _seed_default_checkout_user(fake_db)
    _seed_session(fake_db, {
        "user_id": "user_other",
        "email": "other@example.com",
        "name": "Other User",
        "plan": "free",
        "is_master_admin": False,
        "quota": {
            "perizia_scans_remaining": 4,
            "image_scans_remaining": 0,
            "assistant_messages_remaining": 0,
        },
    }, session_token="sess_other")
    _seed_pending_checkout(
        fake_db,
        user_id="user_checkout",
        plan_id="starter",
        session_id="cs_test_starter_conflict",
        transaction_id="txn_starter_conflict",
        billing_record_id="bill_starter_conflict",
    )

    next_event = {
        "type": "checkout.session.completed",
        "data": {
            "object": {
                "id": "cs_test_starter_conflict",
                "status": "complete",
                "payment_status": "paid",
                "payment_intent": "pi_starter_conflict",
                "client_reference_id": "user_checkout",
                "metadata": {
                    "app_user_id": "user_other",
                    "plan_code": "starter",
                    "billing_reason": "checkout_session_create",
                },
            }
        },
    }

    transport = httpx.ASGITransport(app=server.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post("/api/webhook/stripe", content=b"{}", headers={"Stripe-Signature": "sig_test"})

    assert response.status_code == 200
    users = {item["user_id"]: item for item in fake_db.users.items}
    assert users["user_checkout"]["quota"]["perizia_scans_remaining"] == 4
    assert users["user_other"]["quota"]["perizia_scans_remaining"] == 4
    assert [item for item in fake_db.credit_ledger.items if item["entry_type"] == "plan_purchase"] == []
    assert fake_db.billing_records.items[0]["status"] == "pending"
    assert fake_db.billing_records.items[0]["metadata"]["manual_review_required"] is True


@pytest.mark.anyio
async def test_old_successful_pack_session_is_not_confused_with_new_failed_session(fake_db, monkeypatch):
    next_event = {}

    class FakeWebhook:
        @staticmethod
        def construct_event(payload, sig_header, secret):
            assert payload == b"{}"
            assert sig_header == "sig_test"
            assert secret == "whsec_test"
            return next_event

    class FakePaymentIntentApi:
        @staticmethod
        def retrieve(payment_intent_id):
            statuses = {
                "pi_starter_old_success": "succeeded",
                "pi_starter_new_failed": "requires_action",
            }
            return {"id": payment_intent_id, "status": statuses[payment_intent_id]}

    class FakeStripeModule:
        api_key = None
        Webhook = FakeWebhook
        PaymentIntent = FakePaymentIntentApi

    monkeypatch.setitem(sys.modules, "stripe", FakeStripeModule)

    _seed_default_checkout_user(fake_db)
    _seed_pending_checkout(fake_db, user_id="user_checkout", plan_id="starter", session_id="cs_test_starter_old_success", transaction_id="txn_starter_old_success", billing_record_id="bill_starter_old_success")
    _seed_pending_checkout(fake_db, user_id="user_checkout", plan_id="starter", session_id="cs_test_starter_new_failed", transaction_id="txn_starter_new_failed", billing_record_id="bill_starter_new_failed")

    transport = httpx.ASGITransport(app=server.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        next_event = {
            "type": "checkout.session.completed",
            "data": {"object": {
                "id": "cs_test_starter_old_success",
                "status": "complete",
                "payment_status": "paid",
                "payment_intent": "pi_starter_old_success",
                "client_reference_id": "user_checkout",
                "metadata": {"app_user_id": "user_checkout", "plan_code": "starter", "billing_reason": "checkout_session_create"},
            }},
        }
        first = await client.post("/api/webhook/stripe", content=b"{}", headers={"Stripe-Signature": "sig_test"})

        next_event = {
            "type": "checkout.session.completed",
            "data": {"object": {
                "id": "cs_test_starter_new_failed",
                "status": "complete",
                "payment_status": "paid",
                "payment_intent": "pi_starter_new_failed",
                "client_reference_id": "user_checkout",
                "metadata": {"app_user_id": "user_checkout", "plan_code": "starter", "billing_reason": "checkout_session_create"},
            }},
        }
        second = await client.post("/api/webhook/stripe", content=b"{}", headers={"Stripe-Signature": "sig_test"})

    assert first.status_code == 200
    assert second.status_code == 200
    assert _user_doc(fake_db)["quota"]["perizia_scans_remaining"] == 12
    ledger_entries = [item for item in fake_db.credit_ledger.items if item["entry_type"] == "plan_purchase"]
    assert len(ledger_entries) == 1
    assert ledger_entries[0]["reference_id"] == "cs_test_starter_old_success"
    billing_by_session = {item["checkout_reference"]: item for item in fake_db.billing_records.items}
    assert billing_by_session["cs_test_starter_old_success"]["status"] == "paid"
    assert billing_by_session["cs_test_starter_new_failed"]["status"] == "failed"


@pytest.mark.anyio
async def test_monthly_subscription_grant_refreshes_monthly_bucket_and_preserves_extra(fake_db, monkeypatch):
    next_event = {}

    class FakeWebhook:
        @staticmethod
        def construct_event(payload, sig_header, secret):
            assert sig_header == "sig_test"
            assert secret == "whsec_test"
            return next_event

    class FakeSubscriptionApi:
        @staticmethod
        def retrieve(subscription_id):
            assert subscription_id == "sub_test_solo"
            return type("Subscription", (), {"metadata": {"app_user_id": "user_checkout", "plan_code": "solo"}})()

    class FakeStripeModule:
        api_key = None
        Webhook = FakeWebhook
        Subscription = FakeSubscriptionApi

    monkeypatch.setitem(sys.modules, "stripe", FakeStripeModule)

    _seed_default_checkout_user(fake_db, credits=4)
    _seed_pending_checkout(fake_db, user_id="user_checkout", plan_id="solo", session_id="cs_test_solo", transaction_id="txn_solo", billing_record_id="bill_solo")

    transport = httpx.ASGITransport(app=server.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        next_event = {
            "type": "checkout.session.completed",
            "data": {"object": {
                "id": "cs_test_solo",
                "status": "complete",
                "payment_status": "paid",
                "subscription": "sub_test_solo",
                "customer": "cus_test_solo",
                "metadata": {"app_user_id": "user_checkout", "plan_code": "solo", "billing_reason": "checkout_session_create"},
            }},
        }
        checkout_completed = await client.post("/api/webhook/stripe", content=b"{}", headers={"Stripe-Signature": "sig_test"})

        next_event = {
            "type": "invoice.paid",
            "data": {"object": {
                "id": "in_test_solo_1",
                "subscription": "sub_test_solo",
                "customer": "cus_test_solo",
                "payment_intent": "pi_invoice_solo",
                "billing_reason": "subscription_create",
                "amount_paid": 4900,
                "subtotal": 4900,
                "tax": 0,
                "metadata": {"app_user_id": "user_checkout", "plan_code": "solo", "billing_reason": "subscription_create"},
                "lines": {"data": [{"price": {"id": "price_solo_env"}}]},
            }},
        }
        first_invoice = await client.post("/api/webhook/stripe", content=b"{}", headers={"Stripe-Signature": "sig_test"})
        second_invoice = await client.post("/api/webhook/stripe", content=b"{}", headers={"Stripe-Signature": "sig_test"})

    wallet = _perizia_wallet(fake_db)
    assert checkout_completed.status_code == 200
    assert first_invoice.status_code == 200
    assert second_invoice.status_code == 200
    assert _user_doc(fake_db)["plan"] == "solo"
    assert _user_doc(fake_db)["quota"]["perizia_scans_remaining"] == 32
    assert wallet["monthly_remaining"] == 28
    assert wallet["extra_remaining"] == 4
    assert wallet["total_available"] == 32
    subscription_entries = [item for item in fake_db.credit_ledger.items if item["entry_type"] == "subscription_reset"]
    assert len(subscription_entries) == 1
    assert subscription_entries[0]["reference_id"] == "in_test_solo_1"
    assert fake_db.billing_records.items[0]["status"] == "paid"
    assert fake_db.billing_records.items[0]["invoice_reference"] == "in_test_solo_1"


@pytest.mark.anyio
async def test_free_user_pack_then_solo_activation_preserves_extra_and_is_idempotent(fake_db, monkeypatch):
    next_event = {}

    class FakeWebhook:
        @staticmethod
        def construct_event(payload, sig_header, secret):
            assert sig_header == "sig_test"
            assert secret == "whsec_test"
            return next_event

    class FakePaymentIntentApi:
        @staticmethod
        def retrieve(payment_intent_id):
            assert payment_intent_id == "pi_pack_then_solo"
            return {"id": payment_intent_id, "status": "succeeded"}

    class FakeSubscriptionApi:
        @staticmethod
        def retrieve(subscription_id):
            assert subscription_id == "sub_pack_then_solo"
            return type("Subscription", (), {"metadata": {"app_user_id": "user_checkout", "plan_code": "solo"}})()

    class FakeStripeModule:
        api_key = None
        Webhook = FakeWebhook
        PaymentIntent = FakePaymentIntentApi
        Subscription = FakeSubscriptionApi

    monkeypatch.setitem(sys.modules, "stripe", FakeStripeModule)

    _seed_default_checkout_user(fake_db, plan="free", credits=4)
    initial_user = _user_doc(fake_db)
    initial_normalized = await server._apply_normalized_account_state(initial_user, persist=True)
    initial_wallet = initial_normalized["perizia_credits"]

    _seed_pending_checkout(
        fake_db,
        user_id="user_checkout",
        plan_id="starter",
        session_id="cs_pack_then_solo_pack",
        transaction_id="txn_pack_then_solo_pack",
        billing_record_id="bill_pack_then_solo_pack",
    )
    _seed_pending_checkout(
        fake_db,
        user_id="user_checkout",
        plan_id="solo",
        session_id="cs_pack_then_solo_solo",
        transaction_id="txn_pack_then_solo_solo",
        billing_record_id="bill_pack_then_solo_solo",
    )

    transport = httpx.ASGITransport(app=server.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        next_event = {
            "type": "checkout.session.completed",
            "data": {"object": {
                "id": "cs_pack_then_solo_pack",
                "status": "complete",
                "payment_status": "paid",
                "payment_intent": "pi_pack_then_solo",
                "metadata": {
                    "app_user_id": "user_checkout",
                    "plan_code": "starter",
                    "billing_reason": "checkout_session_create",
                },
            }},
        }
        first_pack = await client.post("/api/webhook/stripe", content=b"{}", headers={"Stripe-Signature": "sig_test"})
        second_pack = await client.post("/api/webhook/stripe", content=b"{}", headers={"Stripe-Signature": "sig_test"})

        next_event = {
            "type": "checkout.session.completed",
            "data": {"object": {
                "id": "cs_pack_then_solo_solo",
                "status": "complete",
                "payment_status": "paid",
                "subscription": "sub_pack_then_solo",
                "customer": "cus_pack_then_solo",
                "metadata": {
                    "app_user_id": "user_checkout",
                    "plan_code": "solo",
                    "billing_reason": "checkout_session_create",
                },
            }},
        }
        checkout_completed = await client.post("/api/webhook/stripe", content=b"{}", headers={"Stripe-Signature": "sig_test"})

        pack_wallet = dict(_perizia_wallet(fake_db))
        print("PACK_WALLET=" + json.dumps(pack_wallet, sort_keys=True))

        next_event = {
            "type": "invoice.paid",
            "data": {"object": {
                "id": "in_pack_then_solo_1",
                "subscription": "sub_pack_then_solo",
                "customer": "cus_pack_then_solo",
                "payment_intent": "pi_invoice_pack_then_solo",
                "billing_reason": "subscription_create",
                "amount_paid": 4900,
                "subtotal": 4900,
                "tax": 0,
                "metadata": {
                    "app_user_id": "user_checkout",
                    "plan_code": "solo",
                    "billing_reason": "subscription_create",
                },
                "lines": {"data": [{"price": {"id": "price_solo_env"}}]},
            }},
        }
        first_invoice = await client.post("/api/webhook/stripe", content=b"{}", headers={"Stripe-Signature": "sig_test"})
        second_invoice = await client.post("/api/webhook/stripe", content=b"{}", headers={"Stripe-Signature": "sig_test"})

    solo_wallet = _perizia_wallet(fake_db)
    print("SOLO_WALLET=" + json.dumps(solo_wallet, sort_keys=True))

    assert first_pack.status_code == 200
    assert second_pack.status_code == 200
    assert checkout_completed.status_code == 200
    assert first_invoice.status_code == 200
    assert second_invoice.status_code == 200

    assert initial_wallet["monthly_remaining"] == 0
    assert initial_wallet["extra_remaining"] == 4
    assert initial_wallet["total_available"] == 4

    assert pack_wallet["monthly_remaining"] == 0
    assert pack_wallet["extra_remaining"] == 12
    assert pack_wallet["total_available"] == 12
    assert len([item for item in fake_db.credit_ledger.items if item["entry_type"] == "plan_purchase"]) == 1

    assert _user_doc(fake_db)["plan"] == "solo"
    assert _user_doc(fake_db)["quota"]["perizia_scans_remaining"] == 40
    assert solo_wallet["monthly_remaining"] == 28
    assert solo_wallet["extra_remaining"] == 12
    assert solo_wallet["total_available"] == 40
    assert solo_wallet["processed_invoice_ids"] == ["in_pack_then_solo_1"]
    assert len([item for item in fake_db.credit_ledger.items if item["entry_type"] == "subscription_reset"]) == 1


@pytest.mark.anyio
async def test_invoice_paid_uses_transaction_fallback_when_invoice_context_metadata_is_missing(fake_db, monkeypatch):
    next_event = {}

    class FakeWebhook:
        @staticmethod
        def construct_event(payload, sig_header, secret):
            assert sig_header == "sig_test"
            assert secret == "whsec_test"
            return next_event

    class FakeSubscriptionApi:
        @staticmethod
        def retrieve(subscription_id):
            assert subscription_id == "sub_test_solo_fallback"
            return type("Subscription", (), {"metadata": {}})()

    class FakeStripeModule:
        api_key = None
        Webhook = FakeWebhook
        Subscription = FakeSubscriptionApi

    monkeypatch.setitem(sys.modules, "stripe", FakeStripeModule)

    _seed_default_checkout_user(fake_db, credits=4)
    _seed_pending_checkout(fake_db, user_id="user_checkout", plan_id="solo", session_id="cs_test_solo_fallback", transaction_id="txn_solo_fallback", billing_record_id="bill_solo_fallback")

    transport = httpx.ASGITransport(app=server.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        next_event = {
            "type": "checkout.session.completed",
            "data": {"object": {
                "id": "cs_test_solo_fallback",
                "status": "complete",
                "payment_status": "paid",
                "subscription": "sub_test_solo_fallback",
                "customer": "cus_test_solo_fallback",
                "metadata": {"app_user_id": "user_checkout", "plan_code": "solo", "billing_reason": "checkout_session_create"},
            }},
        }
        checkout_completed = await client.post("/api/webhook/stripe", content=b"{}", headers={"Stripe-Signature": "sig_test"})

        next_event = {
            "type": "invoice.paid",
            "data": {"object": {
                "id": "in_test_solo_fallback_1",
                "subscription": "sub_test_solo_fallback",
                "customer": "cus_test_solo_fallback",
                "payment_intent": "pi_invoice_solo_fallback",
                "billing_reason": "subscription_create",
                "amount_paid": 4900,
                "subtotal": 4900,
                "tax": 0,
                "metadata": {},
                "lines": {"data": []},
            }},
        }
        invoice_paid = await client.post("/api/webhook/stripe", content=b"{}", headers={"Stripe-Signature": "sig_test"})

    wallet = _perizia_wallet(fake_db)
    assert checkout_completed.status_code == 200
    assert invoice_paid.status_code == 200
    assert _user_doc(fake_db)["plan"] == "solo"
    assert wallet["monthly_remaining"] == 28
    assert wallet["extra_remaining"] == 4
    assert wallet["total_available"] == 32
    subscription_entries = [item for item in fake_db.credit_ledger.items if item["entry_type"] == "subscription_reset"]
    assert len(subscription_entries) == 1
    assert subscription_entries[0]["reference_id"] == "in_test_solo_fallback_1"


@pytest.mark.anyio
async def test_subscription_refresh_preserves_extra_packs_and_prevents_99_becomes_28_regression(fake_db, monkeypatch):
    next_event = {}

    class FakeWebhook:
        @staticmethod
        def construct_event(payload, sig_header, secret):
            return next_event

    class FakeSubscriptionApi:
        @staticmethod
        def retrieve(subscription_id):
            return type("Subscription", (), {"metadata": {"app_user_id": "user_checkout", "plan_code": "solo"}})()

    class FakeStripeModule:
        api_key = None
        Webhook = FakeWebhook
        Subscription = FakeSubscriptionApi

    monkeypatch.setitem(sys.modules, "stripe", FakeStripeModule)

    _seed_default_checkout_user(fake_db, plan="free", credits=99)

    next_event = {
        "type": "invoice.paid",
        "data": {"object": {
            "id": "in_test_solo_99",
            "subscription": "sub_test_solo_99",
            "customer": "cus_test_solo_99",
            "payment_intent": "pi_invoice_solo_99",
            "billing_reason": "subscription_create",
            "amount_paid": 4900,
            "subtotal": 4900,
            "tax": 0,
            "metadata": {"app_user_id": "user_checkout", "plan_code": "solo", "billing_reason": "subscription_create"},
            "lines": {"data": [{"price": {"id": "price_solo_env"}}]},
        }},
    }

    transport = httpx.ASGITransport(app=server.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post("/api/webhook/stripe", content=b"{}", headers={"Stripe-Signature": "sig_test"})

    wallet = _perizia_wallet(fake_db)
    assert response.status_code == 200
    assert _user_doc(fake_db)["plan"] == "solo"
    assert wallet["monthly_remaining"] == 28
    assert wallet["extra_remaining"] == 99
    assert wallet["total_available"] == 127
    assert _user_doc(fake_db)["quota"]["perizia_scans_remaining"] == 127


@pytest.mark.anyio
async def test_pro_subscription_refresh_preserves_legacy_extra_balance(fake_db, monkeypatch):
    next_event = {}

    class FakeWebhook:
        @staticmethod
        def construct_event(payload, sig_header, secret):
            return next_event

    class FakeSubscriptionApi:
        @staticmethod
        def retrieve(subscription_id):
            return type("Subscription", (), {"metadata": {"app_user_id": "user_checkout", "plan_code": "pro"}})()

    class FakeStripeModule:
        api_key = None
        Webhook = FakeWebhook
        Subscription = FakeSubscriptionApi

    monkeypatch.setitem(sys.modules, "stripe", FakeStripeModule)

    _seed_default_checkout_user(fake_db, plan="pro", credits=99)

    next_event = {
        "type": "invoice.paid",
        "data": {"object": {
            "id": "in_test_pro_refresh",
            "subscription": "sub_test_pro_refresh",
            "customer": "cus_test_pro_refresh",
            "payment_intent": "pi_invoice_pro_refresh",
            "billing_reason": "subscription_cycle",
            "amount_paid": 12900,
            "subtotal": 12900,
            "tax": 0,
            "metadata": {"app_user_id": "user_checkout", "plan_code": "pro", "billing_reason": "subscription_cycle"},
            "lines": {"data": [{"price": {"id": "price_pro_env"}}]},
        }},
    }

    transport = httpx.ASGITransport(app=server.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post("/api/webhook/stripe", content=b"{}", headers={"Stripe-Signature": "sig_test"})

    wallet = _perizia_wallet(fake_db)
    assert response.status_code == 200
    assert wallet["monthly_remaining"] == 84
    assert wallet["extra_remaining"] == 15
    assert wallet["total_available"] == 99


@pytest.mark.anyio
async def test_replayed_invoice_paid_does_not_create_duplicate_paid_billing_record(fake_db, monkeypatch):
    next_event = {}

    class FakeWebhook:
        @staticmethod
        def construct_event(payload, sig_header, secret):
            return next_event

    class FakeSubscriptionApi:
        @staticmethod
        def retrieve(subscription_id):
            return type("Subscription", (), {"metadata": {"app_user_id": "user_checkout", "plan_code": "pro"}})()

    class FakeStripeModule:
        api_key = None
        Webhook = FakeWebhook
        Subscription = FakeSubscriptionApi

    monkeypatch.setitem(sys.modules, "stripe", FakeStripeModule)

    _seed_default_checkout_user(fake_db)

    next_event = {
        "type": "invoice.paid",
        "data": {"object": {
            "id": "in_test_pro_renewal_1",
            "subscription": "sub_test_pro",
            "customer": "cus_test_pro",
            "payment_intent": "pi_invoice_pro",
            "billing_reason": "subscription_cycle",
            "amount_paid": 14900,
            "subtotal": 14900,
            "tax": 0,
            "metadata": {"app_user_id": "user_checkout", "plan_code": "pro", "billing_reason": "subscription_cycle"},
            "lines": {"data": [{"price": {"id": "price_pro_env"}}]},
        }},
    }

    transport = httpx.ASGITransport(app=server.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        first_invoice = await client.post("/api/webhook/stripe", content=b"{}", headers={"Stripe-Signature": "sig_test"})
        second_invoice = await client.post("/api/webhook/stripe", content=b"{}", headers={"Stripe-Signature": "sig_test"})

    wallet = _perizia_wallet(fake_db)
    assert first_invoice.status_code == 200
    assert second_invoice.status_code == 200
    assert _user_doc(fake_db)["plan"] == "pro"
    assert wallet["monthly_remaining"] == 84
    assert wallet["extra_remaining"] == 4
    assert wallet["total_available"] == 88
    subscription_entries = [item for item in fake_db.credit_ledger.items if item["entry_type"] == "subscription_reset"]
    assert len(subscription_entries) == 1
    assert subscription_entries[0]["reference_id"] == "in_test_pro_renewal_1"
    assert len(fake_db.billing_records.items) == 1
    assert fake_db.billing_records.items[0]["status"] == "paid"
    assert fake_db.billing_records.items[0]["invoice_reference"] == "in_test_pro_renewal_1"


@pytest.mark.anyio
async def test_consumption_order_uses_monthly_before_extra_pack(fake_db):
    _seed_default_checkout_user(fake_db, plan="solo", credits=32)
    user_doc = _user_doc(fake_db)
    normalized = await server._apply_normalized_account_state(user_doc, persist=True)
    user = server.User(**normalized)

    debited = await server._apply_perizia_credit_debit_with_ledger(
        user,
        amount=30,
        entry_type="perizia_upload",
        reference_type="analysis",
        reference_id="analysis_consumption_order",
        description_it="Debit test",
    )

    wallet = _perizia_wallet(fake_db)
    assert debited is True
    assert wallet["monthly_remaining"] == 0
    assert wallet["extra_remaining"] == 2
    assert wallet["total_available"] == 2
    latest_debit = [item for item in fake_db.credit_ledger.items if item["entry_type"] == "perizia_upload"][-1]
    assert latest_debit["amount"] == 30
    assert latest_debit["metadata"]["debit_from_monthly"] == 28
    assert latest_debit["metadata"]["debit_from_extra"] == 2


@pytest.mark.parametrize(
    ("plan", "legacy_credits", "expected_monthly", "expected_extra", "expected_monthly_plan_id"),
    [
        ("free", 4, 0, 4, None),
        ("free", 0, 0, 0, None),
        ("solo", 28, 28, 0, "solo"),
        ("solo", 91, 28, 63, "solo"),
        ("pro", 91, 84, 7, "pro"),
        ("pro", 40, 40, 0, "pro"),
        ("starter", 12, 0, 12, None),
    ],
)
def test_legacy_wallet_migration_matrix_is_exact(plan, legacy_credits, expected_monthly, expected_extra, expected_monthly_plan_id):
    user_doc = {
        "user_id": f"user_{plan}_{legacy_credits}",
        "email": f"{plan}_{legacy_credits}@example.com",
        "plan": plan,
        "quota": {
            "perizia_scans_remaining": legacy_credits,
            "image_scans_remaining": 0,
            "assistant_messages_remaining": 0,
        },
        "created_at": "2026-01-01T00:00:00+00:00",
    }

    wallet = server._normalize_perizia_credit_wallet(user_doc, plan_id=plan, is_master_admin=False)

    assert wallet == {
        "version": 1,
        "monthly_remaining": expected_monthly,
        "extra_remaining": expected_extra,
        "total_available": legacy_credits,
        "monthly_plan_id": expected_monthly_plan_id,
        "monthly_refreshed_at": None,
        "pack_expiry_enforced": False,
        "pack_validity_days": 365,
        "pack_grants": []
        if expected_extra == 0
        else [
            {
                "grant_id": wallet["pack_grants"][0]["grant_id"],
                "source": "legacy_migration",
                "plan_code": "starter",
                "reference_id": user_doc["user_id"],
                "amount_granted": expected_extra,
                "amount_remaining": expected_extra,
                "granted_at": "2026-01-01T00:00:00+00:00",
                "expires_at": None,
            }
        ],
        "processed_invoice_ids": [],
    }
    _assert_wallet_invariants(wallet)


@pytest.mark.anyio
async def test_pack_purchase_adds_only_to_extra_when_user_already_has_extra(fake_db, monkeypatch):
    next_event = {}

    class FakeWebhook:
        @staticmethod
        def construct_event(payload, sig_header, secret):
            return next_event

    class FakePaymentIntentApi:
        @staticmethod
        def retrieve(payment_intent_id):
            return {"id": payment_intent_id, "status": "succeeded"}

    class FakeStripeModule:
        api_key = None
        Webhook = FakeWebhook
        PaymentIntent = FakePaymentIntentApi

    monkeypatch.setitem(sys.modules, "stripe", FakeStripeModule)

    _seed_default_checkout_user(fake_db, plan="free", credits=12)
    user_doc = _user_doc(fake_db)
    normalized = await server._apply_normalized_account_state(user_doc, persist=True)
    before_wallet = normalized["perizia_credits"]
    _seed_pending_checkout(fake_db, user_id="user_checkout", plan_id="starter", session_id="cs_pack_existing_extra", transaction_id="txn_pack_existing_extra", billing_record_id="bill_pack_existing_extra")

    next_event = {
        "type": "checkout.session.completed",
        "data": {"object": {
            "id": "cs_pack_existing_extra",
            "status": "complete",
            "payment_status": "paid",
            "payment_intent": "pi_pack_existing_extra",
            "metadata": {"app_user_id": "user_checkout", "plan_code": "starter", "billing_reason": "checkout_session_create"},
        }},
    }

    transport = httpx.ASGITransport(app=server.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post("/api/webhook/stripe", content=b"{}", headers={"Stripe-Signature": "sig_test"})

    user_doc = _user_doc(fake_db)
    wallet = _perizia_wallet(fake_db)
    assert response.status_code == 200
    assert before_wallet["monthly_remaining"] == 0
    assert before_wallet["extra_remaining"] == 12
    assert wallet["monthly_remaining"] == 0
    assert wallet["extra_remaining"] == 20
    assert wallet["total_available"] == 20
    assert len(_ledger_entries(fake_db, entry_type="plan_purchase", reference_id="cs_pack_existing_extra")) == 1
    assert _billing_records(fake_db, checkout_reference="cs_pack_existing_extra")[0]["status"] == "paid"
    _assert_wallet_invariants(wallet, user_doc=user_doc)


@pytest.mark.anyio
async def test_pack_purchase_adds_only_to_extra_when_user_already_has_monthly(fake_db, monkeypatch):
    next_event = {}

    class FakeWebhook:
        @staticmethod
        def construct_event(payload, sig_header, secret):
            return next_event

    class FakePaymentIntentApi:
        @staticmethod
        def retrieve(payment_intent_id):
            return {"id": payment_intent_id, "status": "succeeded"}

    class FakeStripeModule:
        api_key = None
        Webhook = FakeWebhook
        PaymentIntent = FakePaymentIntentApi

    monkeypatch.setitem(sys.modules, "stripe", FakeStripeModule)

    _seed_default_checkout_user(fake_db, plan="solo", credits=28)
    user_doc = _user_doc(fake_db)
    normalized = await server._apply_normalized_account_state(user_doc, persist=True)
    before_wallet = normalized["perizia_credits"]
    _seed_pending_checkout(fake_db, user_id="user_checkout", plan_id="starter", session_id="cs_pack_existing_monthly", transaction_id="txn_pack_existing_monthly", billing_record_id="bill_pack_existing_monthly")

    next_event = {
        "type": "checkout.session.completed",
        "data": {"object": {
            "id": "cs_pack_existing_monthly",
            "status": "complete",
            "payment_status": "paid",
            "payment_intent": "pi_pack_existing_monthly",
            "metadata": {"app_user_id": "user_checkout", "plan_code": "starter", "billing_reason": "checkout_session_create"},
        }},
    }

    transport = httpx.ASGITransport(app=server.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post("/api/webhook/stripe", content=b"{}", headers={"Stripe-Signature": "sig_test"})

    user_doc = _user_doc(fake_db)
    wallet = _perizia_wallet(fake_db)
    assert response.status_code == 200
    assert before_wallet["monthly_remaining"] == 28
    assert before_wallet["extra_remaining"] == 0
    assert wallet["monthly_remaining"] == 28
    assert wallet["extra_remaining"] == 8
    assert wallet["total_available"] == 36
    assert wallet["monthly_plan_id"] == "solo"
    assert len(_ledger_entries(fake_db, entry_type="plan_purchase", reference_id="cs_pack_existing_monthly")) == 1
    assert _billing_records(fake_db, checkout_reference="cs_pack_existing_monthly")[0]["status"] == "paid"
    _assert_wallet_invariants(wallet, user_doc=user_doc)


@pytest.mark.anyio
async def test_checkout_session_completed_for_subscription_does_not_grant_or_mutate_wallet(fake_db, monkeypatch):
    next_event = {}

    class FakeWebhook:
        @staticmethod
        def construct_event(payload, sig_header, secret):
            return next_event

    class FakeStripeModule:
        api_key = None
        Webhook = FakeWebhook

    monkeypatch.setitem(sys.modules, "stripe", FakeStripeModule)

    _seed_default_checkout_user(fake_db, plan="free", credits=4)
    initial_user = _user_doc(fake_db)
    initial_normalized = await server._apply_normalized_account_state(initial_user, persist=True)
    initial_wallet = dict(initial_normalized["perizia_credits"])
    _seed_pending_checkout(fake_db, user_id="user_checkout", plan_id="solo", session_id="cs_subscription_only", transaction_id="txn_subscription_only", billing_record_id="bill_subscription_only")

    next_event = {
        "type": "checkout.session.completed",
        "data": {"object": {
            "id": "cs_subscription_only",
            "status": "complete",
            "payment_status": "paid",
            "subscription": "sub_subscription_only",
            "customer": "cus_subscription_only",
            "metadata": {"app_user_id": "user_checkout", "plan_code": "solo", "billing_reason": "checkout_session_create"},
        }},
    }

    transport = httpx.ASGITransport(app=server.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post("/api/webhook/stripe", content=b"{}", headers={"Stripe-Signature": "sig_test"})

    user_doc = _user_doc(fake_db)
    wallet = _perizia_wallet(fake_db)
    assert response.status_code == 200
    assert user_doc["plan"] == "free"
    assert wallet == initial_wallet
    assert _billing_records(fake_db, checkout_reference="cs_subscription_only")[0]["status"] == "pending"
    assert _billing_records(fake_db, checkout_reference="cs_subscription_only")[0]["invoice_reference"] is None
    assert _ledger_entries(fake_db, entry_type="subscription_reset") == []
    _assert_wallet_invariants(wallet, user_doc=user_doc)


@pytest.mark.anyio
async def test_invoice_payment_failed_marks_billing_failed_without_grant_or_wallet_mutation(fake_db, monkeypatch):
    next_event = {}

    class FakeWebhook:
        @staticmethod
        def construct_event(payload, sig_header, secret):
            return next_event

    class FakeSubscriptionApi:
        @staticmethod
        def retrieve(subscription_id):
            return type("Subscription", (), {"metadata": {"app_user_id": "user_checkout", "plan_code": "solo"}})()

    class FakeStripeModule:
        api_key = None
        Webhook = FakeWebhook
        Subscription = FakeSubscriptionApi

    monkeypatch.setitem(sys.modules, "stripe", FakeStripeModule)

    _seed_default_checkout_user(fake_db, plan="free", credits=12)
    user_doc = _user_doc(fake_db)
    normalized = await server._apply_normalized_account_state(user_doc, persist=True)
    initial_wallet = dict(normalized["perizia_credits"])
    _seed_pending_checkout(fake_db, user_id="user_checkout", plan_id="solo", session_id="cs_failed_invoice", transaction_id="txn_failed_invoice", billing_record_id="bill_failed_invoice")

    next_event = {
        "type": "invoice.payment_failed",
        "data": {"object": {
            "id": "in_failed_invoice",
            "subscription": "sub_failed_invoice",
            "customer": "cus_failed_invoice",
            "billing_reason": "subscription_create",
            "metadata": {"app_user_id": "user_checkout", "plan_code": "solo", "billing_reason": "subscription_create"},
        }},
    }

    transport = httpx.ASGITransport(app=server.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post("/api/webhook/stripe", content=b"{}", headers={"Stripe-Signature": "sig_test"})

    user_doc = _user_doc(fake_db)
    wallet = _perizia_wallet(fake_db)
    billing = _billing_records(fake_db, checkout_reference="cs_failed_invoice")[0]
    assert response.status_code == 200
    assert user_doc["plan"] == "free"
    assert wallet == initial_wallet
    assert billing["status"] == "failed"
    assert billing["invoice_status"] == "failed"
    assert billing["invoice_reference"] == "in_failed_invoice"
    assert _ledger_entries(fake_db, entry_type="subscription_reset") == []
    _assert_wallet_invariants(wallet, user_doc=user_doc)


@pytest.mark.parametrize("event_type", ["customer.subscription.updated", "customer.subscription.deleted"])
@pytest.mark.anyio
async def test_subscription_lifecycle_events_do_not_grant_or_wipe_wallet(event_type, fake_db, monkeypatch):
    next_event = {}

    class FakeWebhook:
        @staticmethod
        def construct_event(payload, sig_header, secret):
            return next_event

    class FakeStripeModule:
        api_key = None
        Webhook = FakeWebhook

    monkeypatch.setitem(sys.modules, "stripe", FakeStripeModule)

    _seed_default_checkout_user(fake_db, plan="free", credits=12)
    user_doc = _user_doc(fake_db)
    normalized = await server._apply_normalized_account_state(user_doc, persist=True)
    initial_wallet = dict(normalized["perizia_credits"])
    _seed_pending_checkout(fake_db, user_id="user_checkout", plan_id="solo", session_id=f"cs_{event_type}", transaction_id=f"txn_{event_type}", billing_record_id=f"bill_{event_type}")

    next_event = {
        "type": event_type,
        "data": {"object": {
            "id": f"sub_{event_type}",
            "customer": f"cus_{event_type}",
            "metadata": {"app_user_id": "user_checkout", "plan_code": "solo"},
        }},
    }

    transport = httpx.ASGITransport(app=server.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post("/api/webhook/stripe", content=b"{}", headers={"Stripe-Signature": "sig_test"})

    user_doc = _user_doc(fake_db)
    wallet = _perizia_wallet(fake_db)
    assert response.status_code == 200
    assert wallet == initial_wallet
    assert user_doc["plan"] == "free"
    assert _ledger_entries(fake_db, entry_type="subscription_reset") == []
    _assert_wallet_invariants(wallet, user_doc=user_doc)


@pytest.mark.anyio
async def test_invoice_paid_conflicting_metadata_and_transaction_context_fails_closed(fake_db, monkeypatch):
    next_event = {}

    class FakeWebhook:
        @staticmethod
        def construct_event(payload, sig_header, secret):
            return next_event

    class FakeSubscriptionApi:
        @staticmethod
        def retrieve(subscription_id):
            return type("Subscription", (), {"metadata": {"app_user_id": "user_checkout", "plan_code": "solo"}})()

    class FakeStripeModule:
        api_key = None
        Webhook = FakeWebhook
        Subscription = FakeSubscriptionApi

    monkeypatch.setitem(sys.modules, "stripe", FakeStripeModule)

    _seed_default_checkout_user(fake_db, plan="free", credits=4)
    _seed_session(fake_db, {
        "user_id": "user_other",
        "email": "other@example.com",
        "name": "Other User",
        "plan": "free",
        "is_master_admin": False,
        "quota": {"perizia_scans_remaining": 4, "image_scans_remaining": 0, "assistant_messages_remaining": 0},
    }, session_token="sess_other")
    _seed_pending_checkout(fake_db, user_id="user_checkout", plan_id="solo", session_id="cs_conflict_invoice", transaction_id="txn_conflict_invoice", billing_record_id="bill_conflict_invoice")
    fake_db.payment_transactions.items[0]["stripe_subscription_id"] = "sub_conflict_invoice"
    fake_db.payment_transactions.items[0]["stripe_customer_id"] = "cus_conflict_invoice"

    next_event = {
        "type": "invoice.paid",
        "data": {"object": {
            "id": "in_conflict_invoice",
            "subscription": "sub_conflict_invoice",
            "customer": "cus_conflict_invoice",
            "payment_intent": "pi_conflict_invoice",
            "billing_reason": "subscription_create",
            "metadata": {"app_user_id": "user_other", "plan_code": "solo", "billing_reason": "subscription_create"},
            "lines": {"data": [{"price": {"id": "price_solo_env"}}]},
        }},
    }

    transport = httpx.ASGITransport(app=server.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post("/api/webhook/stripe", content=b"{}", headers={"Stripe-Signature": "sig_test"})

    checkout_user = _user_doc(fake_db, "user_checkout")
    other_user = _user_doc(fake_db, "user_other")
    assert response.status_code == 200
    assert checkout_user["plan"] == "free"
    assert other_user["plan"] == "free"
    assert checkout_user["quota"]["perizia_scans_remaining"] == 4
    assert other_user["quota"]["perizia_scans_remaining"] == 4
    assert _ledger_entries(fake_db, entry_type="subscription_reset") == []


@pytest.mark.anyio
async def test_invoice_paid_preserves_extra_for_pro_refresh_and_never_reduces_it(fake_db, monkeypatch):
    next_event = {}

    class FakeWebhook:
        @staticmethod
        def construct_event(payload, sig_header, secret):
            return next_event

    class FakeSubscriptionApi:
        @staticmethod
        def retrieve(subscription_id):
            return type("Subscription", (), {"metadata": {"app_user_id": "user_checkout", "plan_code": "pro"}})()

    class FakeStripeModule:
        api_key = None
        Webhook = FakeWebhook
        Subscription = FakeSubscriptionApi

    monkeypatch.setitem(sys.modules, "stripe", FakeStripeModule)

    _seed_default_checkout_user(fake_db, plan="free", credits=91)
    user_doc = _user_doc(fake_db)
    normalized = await server._apply_normalized_account_state(user_doc, persist=True)
    before_wallet = dict(normalized["perizia_credits"])

    next_event = {
        "type": "invoice.paid",
        "data": {"object": {
            "id": "in_pro_preserve_extra",
            "subscription": "sub_pro_preserve_extra",
            "customer": "cus_pro_preserve_extra",
            "payment_intent": "pi_pro_preserve_extra",
            "billing_reason": "subscription_create",
            "metadata": {"app_user_id": "user_checkout", "plan_code": "pro", "billing_reason": "subscription_create"},
            "lines": {"data": [{"price": {"id": "price_pro_env"}}]},
        }},
    }

    transport = httpx.ASGITransport(app=server.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post("/api/webhook/stripe", content=b"{}", headers={"Stripe-Signature": "sig_test"})

    user_doc = _user_doc(fake_db)
    wallet = _perizia_wallet(fake_db)
    assert response.status_code == 200
    assert before_wallet["monthly_remaining"] == 0
    assert before_wallet["extra_remaining"] == 91
    assert wallet["monthly_remaining"] == 84
    assert wallet["extra_remaining"] == 91
    assert wallet["total_available"] == 175
    assert wallet["extra_remaining"] >= before_wallet["extra_remaining"]
    assert _ledger_entries(fake_db, entry_type="subscription_reset", reference_id="in_pro_preserve_extra")[0]["metadata"]["perizia_credit_wallet_before"]["extra_remaining"] == 91
    assert _ledger_entries(fake_db, entry_type="subscription_reset", reference_id="in_pro_preserve_extra")[0]["metadata"]["perizia_credit_wallet_after"]["extra_remaining"] == 91
    _assert_wallet_invariants(wallet, user_doc=user_doc)


@pytest.mark.anyio
async def test_pro_user_cannot_buy_solo_but_can_still_buy_pack_and_free_user_can_buy_pro(fake_db, monkeypatch):
    captured = []

    class FakeStripeSessionApi:
        @staticmethod
        def create(**kwargs):
            captured.append(kwargs)
            return type("CheckoutSession", (), {"id": f"cs_{len(captured)}", "url": f"https://stripe.test/{len(captured)}"})()

    class FakeStripeModule:
        api_key = None
        checkout = type("CheckoutNamespace", (), {"Session": FakeStripeSessionApi})

    monkeypatch.setitem(sys.modules, "stripe", FakeStripeModule)

    pro_token = _seed_default_checkout_user(fake_db, plan="pro", credits=91)
    free_token = _seed_session(fake_db, {
        "user_id": "user_free_two",
        "email": "free2@example.com",
        "name": "Free Two",
        "plan": "free",
        "is_master_admin": False,
        "quota": {"perizia_scans_remaining": 4, "image_scans_remaining": 0, "assistant_messages_remaining": 0},
    }, session_token="sess_free_two")

    transport = httpx.ASGITransport(app=server.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        blocked = await client.post("/api/checkout/create", json={"plan_id": "solo", "origin_url": "http://frontend.test"}, headers={"Authorization": f"Bearer {pro_token}"})
        allowed_pack = await client.post("/api/checkout/create", json={"plan_id": "starter", "origin_url": "http://frontend.test"}, headers={"Authorization": f"Bearer {pro_token}"})
        allowed_pro = await client.post("/api/checkout/create", json={"plan_id": "pro", "origin_url": "http://frontend.test"}, headers={"Authorization": f"Bearer {free_token}"})
        blocked_studio = await client.post("/api/checkout/create", json={"plan_id": "studio", "origin_url": "http://frontend.test"}, headers={"Authorization": f"Bearer {free_token}"})

    assert blocked.status_code == 409
    assert blocked.json()["detail"] == (
        "Hai gia un abbonamento mensile gestito. Usa le azioni di cambio piano o cancellazione nella pagina Abbonamento. "
        "Credit Pack 8 resta acquistabile in qualsiasi momento."
    )
    assert allowed_pack.status_code == 200
    assert allowed_pro.status_code == 200
    assert blocked_studio.status_code == 400
    assert blocked_studio.json()["detail"] == "Invalid plan"
    assert [item["metadata"]["plan_code"] for item in captured] == ["starter", "pro"]


@pytest.mark.anyio
async def test_solo_user_cannot_buy_pro_but_can_still_buy_pack(fake_db, monkeypatch):
    captured = []

    class FakeStripeSessionApi:
        @staticmethod
        def create(**kwargs):
            captured.append(kwargs)
            return type("CheckoutSession", (), {"id": f"cs_solo_guard_{len(captured)}", "url": "https://stripe.test/solo-guard"})()

    class FakeStripeModule:
        api_key = None
        checkout = type("CheckoutNamespace", (), {"Session": FakeStripeSessionApi})

    monkeypatch.setitem(sys.modules, "stripe", FakeStripeModule)

    solo_token = _seed_default_checkout_user(fake_db, plan="solo", credits=40)

    transport = httpx.ASGITransport(app=server.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        blocked = await client.post("/api/checkout/create", json={"plan_id": "pro", "origin_url": "http://frontend.test"}, headers={"Authorization": f"Bearer {solo_token}"})
        allowed_pack = await client.post("/api/checkout/create", json={"plan_id": "starter", "origin_url": "http://frontend.test"}, headers={"Authorization": f"Bearer {solo_token}"})

    assert blocked.status_code == 409
    assert blocked.json()["detail"] == (
        "Hai gia un abbonamento mensile gestito. Usa le azioni di cambio piano o cancellazione nella pagina Abbonamento. "
        "Credit Pack 8 resta acquistabile in qualsiasi momento."
    )
    assert allowed_pack.status_code == 200
    assert len(captured) == 1
    assert captured[0]["metadata"]["plan_code"] == "starter"


@pytest.mark.parametrize(
    ("wallet", "debit_amount", "expected_monthly", "expected_extra"),
    [
        (_make_wallet(monthly_remaining=28, extra_grants=[_extra_grant(amount=12, grant_id="g1")], plan_id="solo"), 4, 24, 12),
        (_make_wallet(monthly_remaining=3, extra_grants=[_extra_grant(amount=12, grant_id="g1")], plan_id="solo"), 4, 0, 11),
        (_make_wallet(monthly_remaining=0, extra_grants=[_extra_grant(amount=12, grant_id="g1")], plan_id="free"), 7, 0, 5),
        (_make_wallet(monthly_remaining=28, extra_grants=[_extra_grant(amount=12, grant_id="g1")], plan_id="solo"), 40, 0, 0),
    ],
)
@pytest.mark.anyio
async def test_debit_order_matrix_preserves_wallet_invariants(fake_db, wallet, debit_amount, expected_monthly, expected_extra):
    _seed_default_checkout_user(fake_db, plan="solo", credits=wallet["total_available"])
    user_doc = _user_doc(fake_db)
    user_doc["plan"] = wallet["monthly_plan_id"] or "free"
    user_doc["perizia_credits"] = wallet
    user_doc["quota"]["perizia_scans_remaining"] = wallet["total_available"]
    user = server.User(**await server._apply_normalized_account_state(user_doc, persist=True))

    debited = await server._apply_perizia_credit_debit_with_ledger(
        user,
        amount=debit_amount,
        entry_type="perizia_upload",
        reference_type="analysis",
        reference_id=f"analysis_{debit_amount}",
        description_it="Debit matrix test",
    )

    wallet_after = _perizia_wallet(fake_db)
    latest_debit = _ledger_entries(fake_db, entry_type="perizia_upload")[-1]
    assert debited is True
    assert wallet_after["monthly_remaining"] == expected_monthly
    assert wallet_after["extra_remaining"] == expected_extra
    assert wallet_after["total_available"] == expected_monthly + expected_extra
    assert latest_debit["metadata"]["debit_from_monthly"] + latest_debit["metadata"]["debit_from_extra"] == debit_amount
    _assert_wallet_invariants(wallet_after, user_doc=_user_doc(fake_db))


@pytest.mark.anyio
async def test_insufficient_funds_debit_is_noop_and_preserves_wallet_exactly(fake_db):
    wallet = _make_wallet(monthly_remaining=3, extra_grants=[_extra_grant(amount=2, grant_id="g_small")], plan_id="solo")
    _seed_default_checkout_user(fake_db, plan="solo", credits=wallet["total_available"])
    user_doc = _user_doc(fake_db)
    user_doc["perizia_credits"] = wallet
    user_doc["quota"]["perizia_scans_remaining"] = wallet["total_available"]
    user = server.User(**await server._apply_normalized_account_state(user_doc, persist=True))
    before_wallet = dict(_perizia_wallet(fake_db))
    baseline_perizia_uploads = list(_ledger_entries(fake_db, entry_type="perizia_upload"))

    debited = await server._apply_perizia_credit_debit_with_ledger(
        user,
        amount=7,
        entry_type="perizia_upload",
        reference_type="analysis",
        reference_id="analysis_insufficient",
        description_it="Insufficient debit test",
    )

    after_wallet = _perizia_wallet(fake_db)
    assert debited is False
    assert after_wallet == before_wallet
    assert _ledger_entries(fake_db, entry_type="perizia_upload") == baseline_perizia_uploads
    _assert_wallet_invariants(after_wallet, user_doc=_user_doc(fake_db))


@pytest.mark.parametrize(
    ("plan", "legacy_credits", "expected_monthly", "expected_extra", "expected_monthly_plan_id"),
    [
        ("free", 999, 0, 999, None),
        ("solo", 0, 0, 0, "solo"),
        ("solo", 1000, 28, 972, "solo"),
        ("pro", 84, 84, 0, "pro"),
        ("pro", 1000, 84, 916, "pro"),
        ("studio", 40, 40, 0, "studio"),
        ("studio", 210, 210, 0, "studio"),
        ("studio", 1000, 210, 790, "studio"),
        ("starter", 999, 0, 999, None),
    ],
)
def test_legacy_wallet_migration_torture_matrix_is_exact(plan, legacy_credits, expected_monthly, expected_extra, expected_monthly_plan_id):
    user_doc = {
        "user_id": f"user_{plan}_{legacy_credits}",
        "email": f"{plan}_{legacy_credits}@example.com",
        "plan": plan,
        "quota": {
            "perizia_scans_remaining": legacy_credits,
            "image_scans_remaining": 0,
            "assistant_messages_remaining": 0,
        },
        "created_at": "2026-01-01T00:00:00+00:00",
    }

    wallet = server._normalize_perizia_credit_wallet(user_doc, plan_id=plan, is_master_admin=False)

    assert wallet["monthly_remaining"] == expected_monthly
    assert wallet["extra_remaining"] == expected_extra
    assert wallet["total_available"] == expected_monthly + expected_extra
    assert wallet["monthly_plan_id"] == expected_monthly_plan_id
    if expected_extra:
        assert len(wallet["pack_grants"]) == 1
        assert wallet["pack_grants"][0]["source"] == "legacy_migration"
        assert wallet["pack_grants"][0]["reference_id"] == user_doc["user_id"]
        assert wallet["pack_grants"][0]["amount_remaining"] == expected_extra
    else:
        assert wallet["pack_grants"] == []
    _assert_wallet_invariants(wallet)


@pytest.mark.parametrize(
    ("raw_wallet", "plan_id", "expected_monthly", "expected_extra", "expected_sources", "expected_processed_invoice_ids"),
    [
        (
            {"monthly_remaining": 99, "extra_remaining": 0, "pack_grants": [], "processed_invoice_ids": ["in_a", "in_a", "in_b"]},
            "solo",
            28,
            71,
            ["normalized_monthly_spillover"],
            ["in_a", "in_b"],
        ),
        (
            {"monthly_remaining": 5, "extra_remaining": 0, "pack_grants": [], "processed_invoice_ids": ["in_x", "", None, "in_x"]},
            "free",
            0,
            5,
            ["non_recurring_monthly_rollover"],
            ["in_x"],
        ),
        (
            {"monthly_remaining": -4, "extra_remaining": 11, "pack_grants": [{"grant_id": "g_bad", "amount_granted": 2, "amount_remaining": -9}]},
            "free",
            0,
            11,
            ["unknown", "wallet_spillover_recovery"],
            [],
        ),
    ],
)
def test_corrupt_wallet_normalization_repairs_predictably(raw_wallet, plan_id, expected_monthly, expected_extra, expected_sources, expected_processed_invoice_ids):
    wallet = server._finalize_perizia_credit_wallet(raw_wallet, plan_id=plan_id, is_master_admin=False)
    assert wallet["monthly_remaining"] == expected_monthly
    assert wallet["extra_remaining"] == expected_extra
    assert wallet["total_available"] == expected_monthly + expected_extra
    assert [item["source"] for item in wallet["pack_grants"]] == expected_sources
    assert wallet["processed_invoice_ids"] == expected_processed_invoice_ids
    _assert_wallet_invariants(wallet)


@pytest.mark.anyio
async def test_pack_replay_ten_times_cannot_mint_credits_twice(fake_db, monkeypatch):
    next_event = {}

    class FakeWebhook:
        @staticmethod
        def construct_event(payload, sig_header, secret):
            return next_event

    class FakePaymentIntentApi:
        @staticmethod
        def retrieve(payment_intent_id):
            return {"id": payment_intent_id, "status": "succeeded"}

    class FakeStripeModule:
        api_key = None
        Webhook = FakeWebhook
        PaymentIntent = FakePaymentIntentApi

    monkeypatch.setitem(sys.modules, "stripe", FakeStripeModule)

    _seed_default_checkout_user(fake_db, plan="free", credits=4)
    _seed_pending_checkout(fake_db, user_id="user_checkout", plan_id="starter", session_id="cs_pack_replay_10", transaction_id="txn_pack_replay_10", billing_record_id="bill_pack_replay_10")
    next_event = _stripe_event("checkout.session.completed", {
        "id": "cs_pack_replay_10",
        "status": "complete",
        "payment_status": "paid",
        "payment_intent": "pi_pack_replay_10",
        "metadata": {"app_user_id": "user_checkout", "plan_code": "starter", "billing_reason": "checkout_session_create"},
    })

    transport = httpx.ASGITransport(app=server.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        for _ in range(10):
            response = await client.post("/api/webhook/stripe", content=b"{}", headers={"Stripe-Signature": "sig_test"})
            assert response.status_code == 200

    wallet = _assert_wallet_exact(
        fake_db,
        monthly_remaining=0,
        extra_remaining=12,
        total_available=12,
        monthly_plan_id=None,
        processed_invoice_ids=[],
        pack_grants=[
            {"source": "legacy_migration", "reference_id": "user_checkout", "amount_remaining": 4},
            {"reference_id": "cs_pack_replay_10", "amount_granted": 8, "amount_remaining": 8, "source": "stripe_checkout", "plan_code": "starter"},
        ],
    )
    assert len(_ledger_entries(fake_db, entry_type="plan_purchase", reference_id="cs_pack_replay_10")) == 1
    billing = _billing_records(fake_db, checkout_reference="cs_pack_replay_10")[0]
    assert billing["status"] == "paid"
    assert billing["payment_reference"] == "pi_pack_replay_10"
    assert billing["metadata"]["entitlement_granted"] is True
    assert wallet["pack_grants"][1]["expires_at"] is not None


@pytest.mark.parametrize(
    ("session_object", "payment_intent_behavior"),
    [
        (
            {
                "id": "cs_pack_missing_intent",
                "status": "complete",
                "payment_status": "paid",
                "metadata": {"app_user_id": "user_checkout", "plan_code": "starter", "billing_reason": "checkout_session_create"},
            },
            "unused",
        ),
        (
            {
                "id": "cs_pack_requires_action",
                "status": "complete",
                "payment_status": "paid",
                "payment_intent": "pi_pack_requires_action",
                "metadata": {"app_user_id": "user_checkout", "plan_code": "starter", "billing_reason": "checkout_session_create"},
            },
            "requires_action",
        ),
        (
            {
                "id": "cs_pack_lookup_throws",
                "status": "complete",
                "payment_status": "paid",
                "payment_intent": "pi_pack_lookup_throws",
                "metadata": {"app_user_id": "user_checkout", "plan_code": "starter", "billing_reason": "checkout_session_create"},
            },
            "throws",
        ),
    ],
)
@pytest.mark.anyio
async def test_pack_failure_paths_do_not_grant_paid_state_or_credits(fake_db, monkeypatch, session_object, payment_intent_behavior):
    next_event = {}

    class FakeWebhook:
        @staticmethod
        def construct_event(payload, sig_header, secret):
            return next_event

    class FakePaymentIntentApi:
        @staticmethod
        def retrieve(payment_intent_id):
            if payment_intent_behavior == "throws":
                raise RuntimeError("boom")
            return {"id": payment_intent_id, "status": payment_intent_behavior}

    class FakeStripeModule:
        api_key = None
        Webhook = FakeWebhook
        PaymentIntent = FakePaymentIntentApi

    monkeypatch.setitem(sys.modules, "stripe", FakeStripeModule)

    _seed_default_checkout_user(fake_db)
    await server._apply_normalized_account_state(_user_doc(fake_db), persist=True)
    _seed_pending_checkout(fake_db, user_id="user_checkout", plan_id="starter", session_id=session_object["id"], transaction_id=f"txn_{session_object['id']}", billing_record_id=f"bill_{session_object['id']}")
    before = _snapshot_state(fake_db)
    next_event = _stripe_event("checkout.session.completed", session_object)

    transport = httpx.ASGITransport(app=server.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post("/api/webhook/stripe", content=b"{}", headers={"Stripe-Signature": "sig_test"})

    assert response.status_code == 200
    wallet = _perizia_wallet(fake_db)
    assert wallet["total_available"] == 4
    assert _ledger_entries(fake_db, entry_type="plan_purchase") == []
    billing = _billing_records(fake_db, checkout_reference=session_object["id"])[0]
    txn = next(item for item in fake_db.payment_transactions.items if item["session_id"] == session_object["id"])
    assert billing["status"] == "failed"
    assert billing["invoice_status"] == "failed"
    assert billing["payment_reference"] is None
    assert billing["metadata"].get("entitlement_granted") in {False, None}
    assert txn["status"] == "failed"
    assert txn["payment_status"] == "failed"
    assert len(fake_db.credit_ledger.items) == len(before["credit_ledger"])
    _assert_wallet_invariants(wallet, user_doc=_user_doc(fake_db))


@pytest.mark.anyio
async def test_blocked_recurring_checkout_creates_no_transaction_or_billing_junk(fake_db, monkeypatch):
    class FakeStripeSessionApi:
        @staticmethod
        def create(**kwargs):
            raise AssertionError("Stripe should not be called for blocked recurring checkout")

    class FakeStripeModule:
        api_key = None
        checkout = type("CheckoutNamespace", (), {"Session": FakeStripeSessionApi})

    monkeypatch.setitem(sys.modules, "stripe", FakeStripeModule)

    solo_token = _seed_default_checkout_user(fake_db, plan="solo", credits=40)
    before = _snapshot_state(fake_db)

    transport = httpx.ASGITransport(app=server.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        blocked = await client.post(
            "/api/checkout/create",
            json={"plan_id": "pro", "origin_url": "http://frontend.test"},
            headers={"Authorization": f"Bearer {solo_token}"},
        )

    assert blocked.status_code == 409
    assert blocked.json()["detail"] == (
        "Hai gia un abbonamento mensile gestito. Usa le azioni di cambio piano o cancellazione nella pagina Abbonamento. "
        "Credit Pack 8 resta acquistabile in qualsiasi momento."
    )
    _assert_checkout_side_effects_absent(fake_db, before)


@pytest.mark.anyio
async def test_checkout_status_stays_read_only_across_repeated_reads_after_pack_success(fake_db, monkeypatch):
    class FakeStripeSessionApi:
        @staticmethod
        def retrieve(session_id):
            return type("CheckoutStatus", (), {
                "status": "complete",
                "payment_status": "paid",
                "amount_total": 4900,
                "currency": "eur",
                "mode": "payment",
            })()

    class FakeWebhook:
        @staticmethod
        def construct_event(payload, sig_header, secret):
            return _stripe_event("checkout.session.completed", {
                "id": "cs_read_only_pack",
                "status": "complete",
                "payment_status": "paid",
                "payment_intent": "pi_read_only_pack",
                "metadata": {"app_user_id": "user_checkout", "plan_code": "starter", "billing_reason": "checkout_session_create"},
            })

    class FakePaymentIntentApi:
        @staticmethod
        def retrieve(payment_intent_id):
            return {"id": payment_intent_id, "status": "succeeded"}

    class FakeStripeModule:
        api_key = None
        checkout = type("CheckoutNamespace", (), {"Session": FakeStripeSessionApi})
        Webhook = FakeWebhook
        PaymentIntent = FakePaymentIntentApi

    monkeypatch.setitem(sys.modules, "stripe", FakeStripeModule)

    token = _seed_default_checkout_user(fake_db)
    _seed_pending_checkout(fake_db, user_id="user_checkout", plan_id="starter", session_id="cs_read_only_pack", transaction_id="txn_read_only_pack", billing_record_id="bill_read_only_pack")

    transport = httpx.ASGITransport(app=server.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        webhook = await client.post("/api/webhook/stripe", content=b"{}", headers={"Stripe-Signature": "sig_test"})
        assert webhook.status_code == 200
        baseline = _snapshot_state(fake_db)
        for _ in range(3):
            response = await client.get("/api/checkout/status/cs_read_only_pack", headers={"Authorization": f"Bearer {token}"})
            assert response.status_code == 200
            assert response.json()["session_result"] == "success"
            _assert_state_unchanged(fake_db, baseline)


@pytest.mark.anyio
async def test_invoice_paid_conflict_between_metadata_and_transaction_user_fails_closed(fake_db, monkeypatch):
    next_event = {}

    class FakeWebhook:
        @staticmethod
        def construct_event(payload, sig_header, secret):
            return next_event

    class FakeSubscriptionApi:
        @staticmethod
        def retrieve(subscription_id):
            return type("Subscription", (), {"metadata": {"app_user_id": "user_other", "plan_code": "solo"}})()

    class FakeStripeModule:
        api_key = None
        Webhook = FakeWebhook
        Subscription = FakeSubscriptionApi

    monkeypatch.setitem(sys.modules, "stripe", FakeStripeModule)

    _seed_default_checkout_user(fake_db, plan="free", credits=4)
    _seed_session(fake_db, {
        "user_id": "user_other",
        "email": "other@example.com",
        "name": "Other User",
        "plan": "free",
        "is_master_admin": False,
        "quota": {"perizia_scans_remaining": 4, "image_scans_remaining": 0, "assistant_messages_remaining": 0},
    }, session_token="sess_other")
    _seed_pending_checkout(fake_db, user_id="user_checkout", plan_id="solo", session_id="cs_conflict_txn_user", transaction_id="txn_conflict_txn_user", billing_record_id="bill_conflict_txn_user")
    fake_db.payment_transactions.items[0]["stripe_subscription_id"] = "sub_conflict_txn_user"
    fake_db.payment_transactions.items[0]["stripe_customer_id"] = "cus_conflict_txn_user"

    next_event = _stripe_event("invoice.paid", {
        "id": "in_conflict_txn_user",
        "subscription": "sub_conflict_txn_user",
        "customer": "cus_conflict_txn_user",
        "payment_intent": "pi_conflict_txn_user",
        "billing_reason": "subscription_create",
        "metadata": {"app_user_id": "user_other", "plan_code": "solo", "billing_reason": "subscription_create"},
        "lines": {"data": [{"price": {"id": "price_solo_env"}}]},
    })

    transport = httpx.ASGITransport(app=server.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post("/api/webhook/stripe", content=b"{}", headers={"Stripe-Signature": "sig_test"})

    assert response.status_code == 200
    assert _user_doc(fake_db, "user_checkout")["plan"] == "free"
    assert _user_doc(fake_db, "user_other")["plan"] == "free"
    assert _user_doc(fake_db, "user_checkout")["quota"]["perizia_scans_remaining"] == 4
    assert _user_doc(fake_db, "user_other")["quota"]["perizia_scans_remaining"] == 4
    assert _ledger_entries(fake_db, entry_type="subscription_reset") == []
    paid_bills = [item for item in fake_db.billing_records.items if item["status"] == "paid"]
    assert paid_bills == []


@pytest.mark.anyio
async def test_invoice_paid_conflict_between_metadata_and_customer_mapping_fails_closed(fake_db, monkeypatch):
    next_event = {}

    class FakeWebhook:
        @staticmethod
        def construct_event(payload, sig_header, secret):
            return next_event

    class FakeSubscriptionApi:
        @staticmethod
        def retrieve(subscription_id):
            return type("Subscription", (), {"metadata": {}})()

    class FakeStripeModule:
        api_key = None
        Webhook = FakeWebhook
        Subscription = FakeSubscriptionApi

    monkeypatch.setitem(sys.modules, "stripe", FakeStripeModule)

    _seed_default_checkout_user(fake_db, plan="free", credits=4)
    _seed_session(fake_db, {
        "user_id": "user_other",
        "email": "other@example.com",
        "name": "Other User",
        "plan": "free",
        "is_master_admin": False,
        "quota": {"perizia_scans_remaining": 4, "image_scans_remaining": 0, "assistant_messages_remaining": 0},
    }, session_token="sess_other")
    _seed_pending_checkout(fake_db, user_id="user_checkout", plan_id="solo", session_id="cs_conflict_customer_map", transaction_id="txn_conflict_customer_map", billing_record_id="bill_conflict_customer_map")
    fake_db.payment_transactions.items[0]["stripe_customer_id"] = "cus_conflict_customer_map"

    next_event = _stripe_event("invoice.paid", {
        "id": "in_conflict_customer_map",
        "subscription": "sub_conflict_customer_map",
        "customer": "cus_conflict_customer_map",
        "payment_intent": "pi_conflict_customer_map",
        "billing_reason": "subscription_create",
        "metadata": {"app_user_id": "user_other", "plan_code": "solo", "billing_reason": "subscription_create"},
        "lines": {"data": [{"price": {"id": "price_solo_env"}}]},
    })

    transport = httpx.ASGITransport(app=server.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post("/api/webhook/stripe", content=b"{}", headers={"Stripe-Signature": "sig_test"})

    assert response.status_code == 200
    assert _user_doc(fake_db, "user_checkout")["plan"] == "free"
    assert _user_doc(fake_db, "user_other")["plan"] == "free"
    assert _ledger_entries(fake_db, entry_type="subscription_reset") == []
    paid_bills = [item for item in fake_db.billing_records.items if item["status"] == "paid"]
    assert paid_bills == []


@pytest.mark.anyio
async def test_debit_sequence_consumes_monthly_then_multiple_pack_grants_in_order(fake_db):
    wallet = _make_wallet(
        monthly_remaining=5,
        extra_grants=[
            _extra_grant(amount=3, grant_id="g_early", expires_at="2026-06-01T00:00:00+00:00"),
            _extra_grant(amount=4, grant_id="g_late", expires_at="2026-09-01T00:00:00+00:00"),
        ],
        plan_id="solo",
        processed_invoice_ids=["in_keep"],
    )
    _seed_default_checkout_user(fake_db, plan="solo", credits=wallet["total_available"])
    user_doc = _user_doc(fake_db)
    user_doc["perizia_credits"] = wallet
    user_doc["quota"]["perizia_scans_remaining"] = wallet["total_available"]
    user = server.User(**await server._apply_normalized_account_state(user_doc, persist=True))

    first = await server._apply_perizia_credit_debit_with_ledger(
        user,
        amount=7,
        entry_type="perizia_upload",
        reference_type="analysis",
        reference_id="analysis_seq_1",
        description_it="Hostile debit one",
    )
    wallet_after_first = _assert_wallet_exact(
        fake_db,
        monthly_remaining=0,
        extra_remaining=5,
        total_available=5,
        monthly_plan_id="solo",
        processed_invoice_ids=["in_keep"],
        pack_grants=[
            {"grant_id": "g_early", "amount_remaining": 1},
            {"grant_id": "g_late", "amount_remaining": 4},
        ],
    )

    second = await server._apply_perizia_credit_debit_with_ledger(
        user,
        amount=4,
        entry_type="perizia_upload",
        reference_type="analysis",
        reference_id="analysis_seq_2",
        description_it="Hostile debit two",
    )

    assert first is True
    assert second is True
    _assert_wallet_exact(
        fake_db,
        monthly_remaining=0,
        extra_remaining=1,
        total_available=1,
        monthly_plan_id="solo",
        processed_invoice_ids=["in_keep"],
        pack_grants=[
            {"grant_id": "g_early", "amount_remaining": 0},
            {"grant_id": "g_late", "amount_remaining": 1},
        ],
    )
    debit_entries = _ledger_entries(fake_db, entry_type="perizia_upload")
    assert [entry["reference_id"] for entry in debit_entries[-2:]] == ["analysis_seq_1", "analysis_seq_2"]
    assert wallet_after_first["processed_invoice_ids"] == ["in_keep"]


@pytest.mark.anyio
async def test_hostile_sequence_pack_refresh_replays_failures_reads_and_debits(fake_db, monkeypatch):
    next_event = {}

    class FakeWebhook:
        @staticmethod
        def construct_event(payload, sig_header, secret):
            return next_event

    class FakePaymentIntentApi:
        @staticmethod
        def retrieve(payment_intent_id):
            statuses = {
                "pi_seq_pack_ok": "succeeded",
                "pi_seq_pack_fail": "requires_action",
            }
            return {"id": payment_intent_id, "status": statuses[payment_intent_id]}

    class FakeSubscriptionApi:
        @staticmethod
        def retrieve(subscription_id):
            return type("Subscription", (), {"metadata": {"app_user_id": "user_checkout", "plan_code": "solo"}})()

    class FakeStripeSessionApi:
        @staticmethod
        def retrieve(session_id):
            return type("CheckoutStatus", (), {
                "status": "complete",
                "payment_status": "paid",
                "amount_total": 4900,
                "currency": "eur",
                "mode": "payment" if "pack" in session_id else "subscription",
            })()

    class FakeStripeModule:
        api_key = None
        Webhook = FakeWebhook
        PaymentIntent = FakePaymentIntentApi
        Subscription = FakeSubscriptionApi
        checkout = type("CheckoutNamespace", (), {"Session": FakeStripeSessionApi})

    monkeypatch.setitem(sys.modules, "stripe", FakeStripeModule)

    token = _seed_default_checkout_user(fake_db, plan="free", credits=4)
    _seed_pending_checkout(fake_db, user_id="user_checkout", plan_id="starter", session_id="cs_seq_pack_ok", transaction_id="txn_seq_pack_ok", billing_record_id="bill_seq_pack_ok")
    _seed_pending_checkout(fake_db, user_id="user_checkout", plan_id="starter", session_id="cs_seq_pack_fail", transaction_id="txn_seq_pack_fail", billing_record_id="bill_seq_pack_fail")
    _seed_pending_checkout(fake_db, user_id="user_checkout", plan_id="solo", session_id="cs_seq_solo", transaction_id="txn_seq_solo", billing_record_id="bill_seq_solo")

    transport = httpx.ASGITransport(app=server.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        next_event = _stripe_event("checkout.session.completed", {
            "id": "cs_seq_pack_ok",
            "status": "complete",
            "payment_status": "paid",
            "payment_intent": "pi_seq_pack_ok",
            "metadata": {"app_user_id": "user_checkout", "plan_code": "starter", "billing_reason": "checkout_session_create"},
        })
        assert (await client.post("/api/webhook/stripe", content=b"{}", headers={"Stripe-Signature": "sig_test"})).status_code == 200
        _assert_wallet_exact(
            fake_db,
            monthly_remaining=0,
            extra_remaining=12,
            total_available=12,
            monthly_plan_id=None,
            processed_invoice_ids=[],
            pack_grants=[
                {"source": "legacy_migration", "reference_id": "user_checkout", "amount_remaining": 4},
                {"reference_id": "cs_seq_pack_ok", "amount_remaining": 8},
            ],
        )

        next_event = _stripe_event("checkout.session.completed", {
            "id": "cs_seq_pack_fail",
            "status": "complete",
            "payment_status": "paid",
            "payment_intent": "pi_seq_pack_fail",
            "metadata": {"app_user_id": "user_checkout", "plan_code": "starter", "billing_reason": "checkout_session_create"},
        })
        assert (await client.post("/api/webhook/stripe", content=b"{}", headers={"Stripe-Signature": "sig_test"})).status_code == 200
        _assert_wallet_exact(
            fake_db,
            monthly_remaining=0,
            extra_remaining=12,
            total_available=12,
            monthly_plan_id=None,
            processed_invoice_ids=[],
            pack_grants=[
                {"source": "legacy_migration", "reference_id": "user_checkout", "amount_remaining": 4},
                {"reference_id": "cs_seq_pack_ok", "amount_remaining": 8},
            ],
        )

        next_event = _stripe_event("checkout.session.completed", {
            "id": "cs_seq_solo",
            "status": "complete",
            "payment_status": "paid",
            "subscription": "sub_seq_solo",
            "customer": "cus_seq_solo",
            "metadata": {"app_user_id": "user_checkout", "plan_code": "solo", "billing_reason": "checkout_session_create"},
        })
        assert (await client.post("/api/webhook/stripe", content=b"{}", headers={"Stripe-Signature": "sig_test"})).status_code == 200
        before_reads = _snapshot_state(fake_db)
        for _ in range(2):
            response = await client.get("/api/checkout/status/cs_seq_solo", headers={"Authorization": f"Bearer {token}"})
            assert response.status_code == 200
            assert response.json()["session_result"] == "processing"
            _assert_state_unchanged(fake_db, before_reads)

        next_event = _stripe_event("invoice.paid", {
            "id": "in_seq_solo_1",
            "subscription": "sub_seq_solo",
            "customer": "cus_seq_solo",
            "payment_intent": "pi_seq_invoice_1",
            "billing_reason": "subscription_create",
            "metadata": {"app_user_id": "user_checkout", "plan_code": "solo", "billing_reason": "subscription_create"},
            "lines": {"data": [{"price": {"id": "price_solo_env"}}]},
        })
        assert (await client.post("/api/webhook/stripe", content=b"{}", headers={"Stripe-Signature": "sig_test"})).status_code == 200
        _assert_wallet_exact(
            fake_db,
            monthly_remaining=28,
            extra_remaining=12,
            total_available=40,
            monthly_plan_id="solo",
            processed_invoice_ids=["in_seq_solo_1"],
            pack_grants=[{"reference_id": "user_checkout", "amount_remaining": 4}, {"reference_id": "cs_seq_pack_ok", "amount_remaining": 8}],
        )

        assert (await client.post("/api/webhook/stripe", content=b"{}", headers={"Stripe-Signature": "sig_test"})).status_code == 200
        _assert_wallet_exact(
            fake_db,
            monthly_remaining=28,
            extra_remaining=12,
            total_available=40,
            monthly_plan_id="solo",
            processed_invoice_ids=["in_seq_solo_1"],
            pack_grants=[{"reference_id": "user_checkout", "amount_remaining": 4}, {"reference_id": "cs_seq_pack_ok", "amount_remaining": 8}],
        )

        user = server.User(**await server._apply_normalized_account_state(_user_doc(fake_db), persist=True))
        assert await server._apply_perizia_credit_debit_with_ledger(
            user,
            amount=31,
            entry_type="perizia_upload",
            reference_type="analysis",
            reference_id="analysis_seq_final",
            description_it="Hostile sequence debit",
        ) is True
        _assert_wallet_exact(
            fake_db,
            monthly_remaining=0,
            extra_remaining=9,
            total_available=9,
            monthly_plan_id="solo",
            processed_invoice_ids=["in_seq_solo_1"],
            pack_grants=[{"reference_id": "cs_seq_pack_ok", "amount_remaining": 5}, {"reference_id": "user_checkout", "amount_remaining": 4}],
        )

        next_event = _stripe_event("invoice.payment_failed", {
            "id": "in_seq_solo_late_fail",
            "subscription": "sub_seq_solo",
            "customer": "cus_seq_solo",
            "billing_reason": "subscription_cycle",
            "metadata": {"app_user_id": "user_checkout", "plan_code": "solo", "billing_reason": "subscription_cycle"},
        })
        assert (await client.post("/api/webhook/stripe", content=b"{}", headers={"Stripe-Signature": "sig_test"})).status_code == 200
        _assert_wallet_exact(
            fake_db,
            monthly_remaining=0,
            extra_remaining=9,
            total_available=9,
            monthly_plan_id="solo",
            processed_invoice_ids=["in_seq_solo_1"],
            pack_grants=[{"reference_id": "cs_seq_pack_ok", "amount_remaining": 5}, {"reference_id": "user_checkout", "amount_remaining": 4}],
        )

    assert len(_ledger_entries(fake_db, entry_type="plan_purchase", user_id="user_checkout")) == 1
    assert len(_ledger_entries(fake_db, entry_type="subscription_reset", user_id="user_checkout")) == 1
    assert len(_ledger_entries(fake_db, entry_type="perizia_upload", user_id="user_checkout")) == 1


@pytest.mark.anyio
async def test_subscription_lifecycle_state_is_exposed_and_synced(fake_db, monkeypatch):
    next_event = {}

    class FakeWebhook:
        @staticmethod
        def construct_event(payload, sig_header, secret):
            return next_event

    class FakeSubscriptionApi:
        subscriptions = {
            "sub_state_solo": {
                "id": "sub_state_solo",
                "customer": "cus_state_solo",
                "status": "active",
                "cancel_at_period_end": False,
                "current_period_end": 1775000000,
                "metadata": {"app_user_id": "user_checkout", "plan_code": "solo"},
                "items": {"data": [{"id": "si_state_solo", "price": {"id": "price_solo_env"}}]},
            }
        }

        @staticmethod
        def retrieve(subscription_id):
            return copy.deepcopy(FakeSubscriptionApi.subscriptions[subscription_id])

    class FakeStripeModule:
        api_key = None
        Webhook = FakeWebhook
        Subscription = FakeSubscriptionApi

    monkeypatch.setitem(sys.modules, "stripe", FakeStripeModule)
    token = _seed_default_checkout_user(fake_db, plan="free", credits=4)
    _seed_pending_checkout(fake_db, user_id="user_checkout", plan_id="solo", session_id="cs_state_solo", transaction_id="txn_state_solo", billing_record_id="bill_state_solo")

    transport = httpx.ASGITransport(app=server.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        next_event = _stripe_event("checkout.session.completed", {
            "id": "cs_state_solo",
            "status": "complete",
            "payment_status": "paid",
            "subscription": "sub_state_solo",
            "customer": "cus_state_solo",
            "metadata": {"app_user_id": "user_checkout", "plan_code": "solo", "billing_reason": "checkout_session_create"},
        })
        assert (await client.post("/api/webhook/stripe", content=b"{}", headers={"Stripe-Signature": "sig_test"})).status_code == 200

        next_event = _stripe_event("invoice.paid", {
            "id": "in_state_solo_1",
            "subscription": "sub_state_solo",
            "customer": "cus_state_solo",
            "payment_intent": "pi_state_solo_1",
            "billing_reason": "subscription_create",
            "metadata": {"app_user_id": "user_checkout", "plan_code": "solo"},
            "lines": {"data": [{"price": {"id": "price_solo_env"}}]},
        })
        assert (await client.post("/api/webhook/stripe", content=b"{}", headers={"Stripe-Signature": "sig_test"})).status_code == 200

        me = await client.get("/api/auth/me", headers={"Authorization": f"Bearer {token}"})

    assert me.status_code == 200
    state = me.json()["account"]["subscription"]
    assert state["current_plan_id"] == "solo"
    assert state["stripe_customer_id"] == "cus_state_solo"
    assert state["stripe_subscription_id"] == "sub_state_solo"
    assert state["status"] == "active"
    assert state["cancel_at_period_end"] is False
    assert state["pending_change"] is False
    assert state["current_period_end"] is not None


@pytest.mark.anyio
async def test_change_plan_upgrade_and_downgrade_schedule_next_cycle_without_duplicate_subscription(fake_db, monkeypatch):
    next_event = {}

    class FakeSubscriptionApi:
        subscriptions = {
            "sub_plan_change": {
                "id": "sub_plan_change",
                "customer": "cus_plan_change",
                "status": "active",
                "cancel_at_period_end": False,
                "current_period_end": 1775000000,
                "metadata": {"app_user_id": "user_checkout", "plan_code": "solo"},
                "items": {"data": [{"id": "si_plan_change", "price": {"id": "price_solo_env"}}]},
            }
        }
        modify_calls = []

        @staticmethod
        def retrieve(subscription_id):
            return copy.deepcopy(FakeSubscriptionApi.subscriptions[subscription_id])

        @staticmethod
        def modify(subscription_id, **kwargs):
            FakeSubscriptionApi.modify_calls.append((subscription_id, copy.deepcopy(kwargs)))
            subscription = FakeSubscriptionApi.subscriptions[subscription_id]
            if "items" in kwargs:
                subscription["items"]["data"][0]["price"]["id"] = kwargs["items"][0]["price"]
            if "cancel_at_period_end" in kwargs:
                subscription["cancel_at_period_end"] = kwargs["cancel_at_period_end"]
            if "metadata" in kwargs:
                subscription["metadata"] = dict(kwargs["metadata"])
            return copy.deepcopy(subscription)

    class FakeStripeModule:
        api_key = None
        Subscription = FakeSubscriptionApi

    monkeypatch.setitem(sys.modules, "stripe", FakeStripeModule)
    token = _seed_default_checkout_user(fake_db, plan="solo", credits=40)
    fake_db.users.items[0]["subscription_state"] = {
        "stripe_customer_id": "cus_plan_change",
        "stripe_subscription_id": "sub_plan_change",
        "status": "active",
        "current_plan_id": "solo",
        "stripe_plan_id": "solo",
        "current_period_end": "2026-04-30T00:00:00+00:00",
        "cancel_at_period_end": False,
        "pending_change": False,
        "pending_plan_id": None,
    }

    transport = httpx.ASGITransport(app=server.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        upgrade = await client.post(
            "/api/billing/subscription/change-plan",
            json={"plan_id": "pro"},
            headers={"Authorization": f"Bearer {token}"},
        )
        duplicate = await client.post(
            "/api/billing/subscription/change-plan",
            json={"plan_id": "solo"},
            headers={"Authorization": f"Bearer {token}"},
        )

        next_event = _stripe_event("invoice.paid", {
            "id": "in_plan_change_pro_1",
            "subscription": "sub_plan_change",
            "customer": "cus_plan_change",
            "payment_intent": "pi_plan_change_pro_1",
            "billing_reason": "subscription_cycle",
            "metadata": {"app_user_id": "user_checkout", "plan_code": "pro"},
            "lines": {"data": [{"price": {"id": "price_pro_env"}}]},
        })
        class FakeWebhook:
            @staticmethod
            def construct_event(payload, sig_header, secret):
                return next_event
        FakeStripeModule.Webhook = FakeWebhook
        invoice_paid = await client.post("/api/webhook/stripe", content=b"{}", headers={"Stripe-Signature": "sig_test"})

    assert upgrade.status_code == 200
    assert duplicate.status_code == 409
    assert invoice_paid.status_code == 200
    wallet = _perizia_wallet(fake_db)
    state = _subscription_state(fake_db)
    assert len(FakeSubscriptionApi.modify_calls) == 1
    assert FakeSubscriptionApi.modify_calls[0][1]["items"][0]["price"] == "price_pro_env"
    assert wallet["monthly_remaining"] == 84
    assert wallet["extra_remaining"] == 12
    assert _user_doc(fake_db)["plan"] == "pro"
    assert state["current_plan_id"] == "pro"
    assert state["pending_change"] is False
    assert state["pending_plan_id"] is None
    assert state["stripe_subscription_id"] == "sub_plan_change"


@pytest.mark.anyio
async def test_downgrade_request_preserves_current_cycle_credits_until_next_invoice(fake_db, monkeypatch):
    class FakeSubscriptionApi:
        subscriptions = {
            "sub_downgrade": {
                "id": "sub_downgrade",
                "customer": "cus_downgrade",
                "status": "active",
                "cancel_at_period_end": False,
                "current_period_end": 1775000000,
                "metadata": {"app_user_id": "user_checkout", "plan_code": "pro"},
                "items": {"data": [{"id": "si_downgrade", "price": {"id": "price_pro_env"}}]},
            }
        }

        @staticmethod
        def retrieve(subscription_id):
            return copy.deepcopy(FakeSubscriptionApi.subscriptions[subscription_id])

        @staticmethod
        def modify(subscription_id, **kwargs):
            subscription = FakeSubscriptionApi.subscriptions[subscription_id]
            if "items" in kwargs:
                subscription["items"]["data"][0]["price"]["id"] = kwargs["items"][0]["price"]
            if "metadata" in kwargs:
                subscription["metadata"] = dict(kwargs["metadata"])
            return copy.deepcopy(subscription)

    class FakeWebhook:
        @staticmethod
        def construct_event(payload, sig_header, secret):
            return next_event

    class FakeStripeModule:
        api_key = None
        Subscription = FakeSubscriptionApi
        Webhook = FakeWebhook

    monkeypatch.setitem(sys.modules, "stripe", FakeStripeModule)
    token = _seed_default_checkout_user(fake_db, plan="pro", credits=84)
    fake_db.users.items[0]["subscription_state"] = {
        "stripe_customer_id": "cus_downgrade",
        "stripe_subscription_id": "sub_downgrade",
        "status": "active",
        "current_plan_id": "pro",
        "stripe_plan_id": "pro",
        "current_period_end": "2026-04-30T00:00:00+00:00",
        "cancel_at_period_end": False,
        "pending_change": False,
        "pending_plan_id": None,
    }
    next_event = {}

    transport = httpx.ASGITransport(app=server.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        downgrade = await client.post(
            "/api/billing/subscription/change-plan",
            json={"plan_id": "solo"},
            headers={"Authorization": f"Bearer {token}"},
        )
        before_invoice_wallet = _perizia_wallet(fake_db)

        next_event = _stripe_event("invoice.paid", {
            "id": "in_downgrade_solo_1",
            "subscription": "sub_downgrade",
            "customer": "cus_downgrade",
            "payment_intent": "pi_downgrade_solo_1",
            "billing_reason": "subscription_cycle",
            "metadata": {"app_user_id": "user_checkout", "plan_code": "solo"},
            "lines": {"data": [{"price": {"id": "price_solo_env"}}]},
        })
        invoice_paid = await client.post("/api/webhook/stripe", content=b"{}", headers={"Stripe-Signature": "sig_test"})

    assert downgrade.status_code == 200
    assert invoice_paid.status_code == 200
    assert before_invoice_wallet["monthly_remaining"] == 84
    assert _perizia_wallet(fake_db)["monthly_remaining"] == 28
    assert _subscription_state(fake_db)["current_plan_id"] == "solo"


@pytest.mark.anyio
async def test_cancel_at_period_end_and_resume_preserve_credits_and_block_new_refresh_after_delete(fake_db, monkeypatch):
    next_event = {}

    class FakeWebhook:
        @staticmethod
        def construct_event(payload, sig_header, secret):
            return next_event

    class FakeSubscriptionApi:
        subscriptions = {
            "sub_cancel": {
                "id": "sub_cancel",
                "customer": "cus_cancel",
                "status": "active",
                "cancel_at_period_end": False,
                "current_period_end": 1775000000,
                "metadata": {"app_user_id": "user_checkout", "plan_code": "solo"},
                "items": {"data": [{"id": "si_cancel", "price": {"id": "price_solo_env"}}]},
            }
        }

        @staticmethod
        def retrieve(subscription_id):
            return copy.deepcopy(FakeSubscriptionApi.subscriptions[subscription_id])

        @staticmethod
        def modify(subscription_id, **kwargs):
            subscription = FakeSubscriptionApi.subscriptions[subscription_id]
            if "cancel_at_period_end" in kwargs:
                subscription["cancel_at_period_end"] = kwargs["cancel_at_period_end"]
            if "metadata" in kwargs:
                subscription["metadata"] = dict(kwargs["metadata"])
            return copy.deepcopy(subscription)

    class FakeStripeModule:
        api_key = None
        Webhook = FakeWebhook
        Subscription = FakeSubscriptionApi

    monkeypatch.setitem(sys.modules, "stripe", FakeStripeModule)
    token = _seed_default_checkout_user(fake_db, plan="solo", credits=35)
    fake_db.users.items[0]["perizia_credits"] = _make_wallet(
        monthly_remaining=28,
        extra_grants=[_extra_grant(amount=7, grant_id="g_extra_cancel")],
        plan_id="solo",
        processed_invoice_ids=["in_old"],
    )
    fake_db.users.items[0]["quota"]["perizia_scans_remaining"] = 35
    fake_db.users.items[0]["subscription_state"] = {
        "stripe_customer_id": "cus_cancel",
        "stripe_subscription_id": "sub_cancel",
        "status": "active",
        "current_plan_id": "solo",
        "stripe_plan_id": "solo",
        "current_period_end": "2026-04-30T00:00:00+00:00",
        "cancel_at_period_end": False,
        "pending_change": False,
        "pending_plan_id": None,
    }

    transport = httpx.ASGITransport(app=server.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        cancel = await client.post("/api/billing/subscription/cancel", headers={"Authorization": f"Bearer {token}"})
        resume = await client.post("/api/billing/subscription/resume", headers={"Authorization": f"Bearer {token}"})
        cancel_again = await client.post("/api/billing/subscription/cancel", headers={"Authorization": f"Bearer {token}"})

        next_event = _stripe_event("customer.subscription.deleted", {
            "id": "sub_cancel",
            "customer": "cus_cancel",
            "status": "canceled",
            "cancel_at_period_end": False,
            "current_period_end": 1775000000,
            "metadata": {"app_user_id": "user_checkout", "plan_code": "solo"},
            "items": {"data": [{"id": "si_cancel", "price": {"id": "price_solo_env"}}]},
        })
        deleted = await client.post("/api/webhook/stripe", content=b"{}", headers={"Stripe-Signature": "sig_test"})

        next_event = _stripe_event("invoice.paid", {
            "id": "in_cancel_after_delete",
            "subscription": "sub_cancel",
            "customer": "cus_cancel",
            "payment_intent": "pi_cancel_after_delete",
            "billing_reason": "subscription_cycle",
            "metadata": {"app_user_id": "user_checkout", "plan_code": "solo"},
            "lines": {"data": [{"price": {"id": "price_solo_env"}}]},
        })
        after_delete_invoice = await client.post("/api/webhook/stripe", content=b"{}", headers={"Stripe-Signature": "sig_test"})

    assert cancel.status_code == 200
    assert resume.status_code == 200
    assert cancel_again.status_code == 200
    assert deleted.status_code == 200
    assert after_delete_invoice.status_code == 200
    state = _subscription_state(fake_db)
    wallet = _perizia_wallet(fake_db)
    assert state["status"] == "canceled"
    assert _user_doc(fake_db)["plan"] == "free"
    assert wallet["extra_remaining"] >= 7
    assert "in_cancel_after_delete" not in wallet["processed_invoice_ids"]


@pytest.mark.anyio
async def test_subscription_update_conflict_fails_closed_and_does_not_mutate_wrong_user(fake_db, monkeypatch):
    next_event = {}

    class FakeWebhook:
        @staticmethod
        def construct_event(payload, sig_header, secret):
            return next_event

    class FakeStripeModule:
        api_key = None
        Webhook = FakeWebhook

    monkeypatch.setitem(sys.modules, "stripe", FakeStripeModule)
    _seed_default_checkout_user(fake_db, plan="solo", credits=28)
    _seed_session(fake_db, {
        "user_id": "user_other",
        "email": "other@example.com",
        "name": "Other",
        "plan": "free",
        "is_master_admin": False,
        "quota": {"perizia_scans_remaining": 4, "image_scans_remaining": 0, "assistant_messages_remaining": 0},
    }, session_token="sess_other")
    _user_doc(fake_db)["subscription_state"] = {
        "stripe_customer_id": "cus_conflict",
        "stripe_subscription_id": "sub_conflict",
        "status": "active",
        "current_plan_id": "solo",
        "stripe_plan_id": "solo",
        "current_period_end": "2026-04-30T00:00:00+00:00",
        "cancel_at_period_end": False,
        "pending_change": False,
        "pending_plan_id": None,
    }
    _user_doc(fake_db, "user_other")["subscription_state"] = {
        "stripe_customer_id": "cus_conflict",
        "stripe_subscription_id": "sub_other",
        "status": "active",
        "current_plan_id": "pro",
        "stripe_plan_id": "pro",
        "current_period_end": "2026-04-30T00:00:00+00:00",
        "cancel_at_period_end": False,
        "pending_change": False,
        "pending_plan_id": None,
    }
    before = _snapshot_state(fake_db)

    transport = httpx.ASGITransport(app=server.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        next_event = _stripe_event("customer.subscription.updated", {
            "id": "sub_conflict",
            "customer": "cus_conflict",
            "status": "active",
            "cancel_at_period_end": False,
            "current_period_end": 1775000000,
            "metadata": {"app_user_id": "user_checkout", "plan_code": "solo"},
            "items": {"data": [{"id": "si_conflict", "price": {"id": "price_solo_env"}}]},
        })
        response = await client.post("/api/webhook/stripe", content=b"{}", headers={"Stripe-Signature": "sig_test"})

    assert response.status_code == 200
    assert _snapshot_state(fake_db) == before

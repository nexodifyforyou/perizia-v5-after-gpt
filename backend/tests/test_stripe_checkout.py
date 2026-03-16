import os
import sys
from datetime import datetime, timezone

import pytest
import httpx

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


@pytest.mark.anyio
async def test_create_checkout_uses_env_price_ids_and_modes(fake_db, monkeypatch):
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

    session_token = _seed_session(fake_db, {
        "user_id": "user_checkout",
        "email": "checkout@example.com",
        "name": "Checkout User",
        "plan": "free",
        "is_master_admin": False,
        "quota": {
            "perizia_scans_remaining": 4,
            "image_scans_remaining": 0,
            "assistant_messages_remaining": 0,
        },
    })

    transport = httpx.ASGITransport(app=server.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        starter_response = await client.post(
            "/api/checkout/create",
            json={"plan_id": "starter", "origin_url": "http://frontend.test"},
            headers={"Authorization": f"Bearer {session_token}"},
        )
        solo_response = await client.post(
            "/api/checkout/create",
            json={"plan_id": "solo", "origin_url": "http://frontend.test"},
            headers={"Authorization": f"Bearer {session_token}"},
        )

    assert starter_response.status_code == 200
    assert solo_response.status_code == 200
    assert captured[0]["mode"] == "payment"
    assert captured[0]["line_items"] == [{"price": "price_starter_env", "quantity": 1}]
    assert captured[0]["metadata"]["plan_code"] == "starter"
    assert "subscription_data" not in captured[0]
    assert captured[1]["mode"] == "subscription"
    assert captured[1]["line_items"] == [{"price": "price_solo_env", "quantity": 1}]
    assert captured[1]["metadata"]["plan_code"] == "solo"
    assert captured[1]["subscription_data"]["metadata"]["app_user_id"] == "user_checkout"


@pytest.mark.anyio
async def test_checkout_status_is_read_only(fake_db, monkeypatch):
    class FakeStripeSessionApi:
        @staticmethod
        def retrieve(session_id):
            assert session_id == "cs_test_readonly"
            return type(
                "CheckoutStatus",
                (),
                {
                    "status": "complete",
                    "payment_status": "paid",
                    "amount_total": 4900,
                    "currency": "eur",
                    "mode": "subscription",
                },
            )()

    class FakeStripeModule:
        api_key = None
        checkout = type("CheckoutNamespace", (), {"Session": FakeStripeSessionApi})

    monkeypatch.setitem(sys.modules, "stripe", FakeStripeModule)

    session_token = _seed_session(fake_db, {
        "user_id": "user_checkout",
        "email": "checkout@example.com",
        "name": "Checkout User",
        "plan": "free",
        "is_master_admin": False,
        "quota": {
            "perizia_scans_remaining": 4,
            "image_scans_remaining": 0,
            "assistant_messages_remaining": 0,
        },
    })
    _seed_pending_checkout(
        fake_db,
        user_id="user_checkout",
        plan_id="solo",
        session_id="cs_test_readonly",
        transaction_id="txn_readonly",
        billing_record_id="bill_readonly",
    )

    transport = httpx.ASGITransport(app=server.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get(
            "/api/checkout/status/cs_test_readonly",
            headers={"Authorization": f"Bearer {session_token}"},
        )

    assert response.status_code == 200
    assert response.json()["billing_status"] == "pending"
    assert fake_db.users.items[0]["quota"]["perizia_scans_remaining"] == 4
    non_baseline_entries = [item for item in fake_db.credit_ledger.items if item["entry_type"] != "opening_balance"]
    assert non_baseline_entries == []
    assert fake_db.billing_records.items[0]["status"] == "pending"


@pytest.mark.anyio
async def test_starter_webhook_is_idempotent(fake_db, monkeypatch):
    next_event = {}

    class FakeWebhook:
        @staticmethod
        def construct_event(payload, sig_header, secret):
            assert payload == b"{}"
            assert sig_header == "sig_test"
            assert secret == "whsec_test"
            return next_event

    class FakeStripeModule:
        api_key = None
        Webhook = FakeWebhook

    monkeypatch.setitem(sys.modules, "stripe", FakeStripeModule)

    _seed_session(fake_db, {
        "user_id": "user_checkout",
        "email": "checkout@example.com",
        "name": "Checkout User",
        "plan": "free",
        "is_master_admin": False,
        "quota": {
            "perizia_scans_remaining": 4,
            "image_scans_remaining": 0,
            "assistant_messages_remaining": 0,
        },
    })
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

    assert first.status_code == 200
    assert second.status_code == 200
    assert fake_db.users.items[0]["quota"]["perizia_scans_remaining"] == 12
    ledger_entries = [item for item in fake_db.credit_ledger.items if item["entry_type"] == "plan_purchase"]
    assert len(ledger_entries) == 1
    assert ledger_entries[0]["reference_id"] == "cs_test_starter"
    assert fake_db.billing_records.items[0]["status"] == "paid"


@pytest.mark.anyio
async def test_subscription_grant_is_driven_by_invoice_paid(fake_db, monkeypatch):
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
            return type(
                "Subscription",
                (),
                {
                    "metadata": {
                        "app_user_id": "user_checkout",
                        "plan_code": "solo",
                    }
                },
            )()

    class FakeStripeModule:
        api_key = None
        Webhook = FakeWebhook
        Subscription = FakeSubscriptionApi

    monkeypatch.setitem(sys.modules, "stripe", FakeStripeModule)

    _seed_session(fake_db, {
        "user_id": "user_checkout",
        "email": "checkout@example.com",
        "name": "Checkout User",
        "plan": "free",
        "is_master_admin": False,
        "quota": {
            "perizia_scans_remaining": 4,
            "image_scans_remaining": 0,
            "assistant_messages_remaining": 0,
        },
    })
    _seed_pending_checkout(
        fake_db,
        user_id="user_checkout",
        plan_id="solo",
        session_id="cs_test_solo",
        transaction_id="txn_solo",
        billing_record_id="bill_solo",
    )

    transport = httpx.ASGITransport(app=server.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        next_event = {
            "type": "checkout.session.completed",
            "data": {
                "object": {
                    "id": "cs_test_solo",
                    "status": "complete",
                    "payment_status": "paid",
                    "subscription": "sub_test_solo",
                    "customer": "cus_test_solo",
                    "metadata": {
                        "app_user_id": "user_checkout",
                        "plan_code": "solo",
                        "billing_reason": "checkout_session_create",
                    },
                }
            },
        }
        checkout_completed = await client.post("/api/webhook/stripe", content=b"{}", headers={"Stripe-Signature": "sig_test"})

        assert checkout_completed.status_code == 200
        assert fake_db.users.items[0]["plan"] == "free"
        assert fake_db.users.items[0]["quota"]["perizia_scans_remaining"] == 4
        assert [item for item in fake_db.credit_ledger.items if item["entry_type"] == "subscription_reset"] == []
        assert fake_db.billing_records.items[0]["status"] == "pending"
        assert fake_db.billing_records.items[0]["invoice_reference"] is None

        next_event = {
            "type": "invoice.paid",
            "data": {
                "object": {
                    "id": "in_test_solo_1",
                    "subscription": "sub_test_solo",
                    "customer": "cus_test_solo",
                    "payment_intent": "pi_invoice_solo",
                    "billing_reason": "subscription_create",
                    "amount_paid": 4900,
                    "subtotal": 4900,
                    "tax": 0,
                    "metadata": {
                        "app_user_id": "user_checkout",
                        "plan_code": "solo",
                        "billing_reason": "subscription_create",
                    },
                    "lines": {
                        "data": [
                            {
                                "price": {"id": "price_solo_env"},
                            }
                        ]
                    },
                }
            },
        }
        first_invoice = await client.post("/api/webhook/stripe", content=b"{}", headers={"Stripe-Signature": "sig_test"})
        second_invoice = await client.post("/api/webhook/stripe", content=b"{}", headers={"Stripe-Signature": "sig_test"})

    assert first_invoice.status_code == 200
    assert second_invoice.status_code == 200
    assert fake_db.users.items[0]["plan"] == "solo"
    assert fake_db.users.items[0]["quota"]["perizia_scans_remaining"] == 28
    subscription_entries = [item for item in fake_db.credit_ledger.items if item["entry_type"] == "subscription_reset"]
    assert len(subscription_entries) == 1
    assert subscription_entries[0]["reference_type"] == "stripe_invoice"
    assert subscription_entries[0]["reference_id"] == "in_test_solo_1"
    assert fake_db.billing_records.items[0]["status"] == "paid"
    assert fake_db.billing_records.items[0]["invoice_reference"] == "in_test_solo_1"


@pytest.mark.anyio
async def test_replayed_invoice_paid_does_not_create_duplicate_paid_billing_record(fake_db, monkeypatch):
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
            assert subscription_id == "sub_test_pro"
            return type(
                "Subscription",
                (),
                {
                    "metadata": {
                        "app_user_id": "user_checkout",
                        "plan_code": "pro",
                    }
                },
            )()

    class FakeStripeModule:
        api_key = None
        Webhook = FakeWebhook
        Subscription = FakeSubscriptionApi

    monkeypatch.setitem(sys.modules, "stripe", FakeStripeModule)

    _seed_session(fake_db, {
        "user_id": "user_checkout",
        "email": "checkout@example.com",
        "name": "Checkout User",
        "plan": "free",
        "is_master_admin": False,
        "quota": {
            "perizia_scans_remaining": 4,
            "image_scans_remaining": 0,
            "assistant_messages_remaining": 0,
        },
    })

    next_event = {
        "type": "invoice.paid",
        "data": {
            "object": {
                "id": "in_test_pro_renewal_1",
                "subscription": "sub_test_pro",
                "customer": "cus_test_pro",
                "payment_intent": "pi_invoice_pro",
                "billing_reason": "subscription_cycle",
                "amount_paid": 14900,
                "subtotal": 14900,
                "tax": 0,
                "metadata": {
                    "app_user_id": "user_checkout",
                    "plan_code": "pro",
                    "billing_reason": "subscription_cycle",
                },
                "lines": {
                    "data": [
                        {
                            "price": {"id": "price_pro_env"},
                        }
                    ]
                },
            }
        },
    }

    transport = httpx.ASGITransport(app=server.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        first_invoice = await client.post("/api/webhook/stripe", content=b"{}", headers={"Stripe-Signature": "sig_test"})
        second_invoice = await client.post("/api/webhook/stripe", content=b"{}", headers={"Stripe-Signature": "sig_test"})

    assert first_invoice.status_code == 200
    assert second_invoice.status_code == 200
    assert fake_db.users.items[0]["plan"] == "pro"
    assert fake_db.users.items[0]["quota"]["perizia_scans_remaining"] == server.SUBSCRIPTION_PLANS["pro"].quota["perizia_scans_remaining"]
    subscription_entries = [item for item in fake_db.credit_ledger.items if item["entry_type"] == "subscription_reset"]
    assert len(subscription_entries) == 1
    assert subscription_entries[0]["reference_id"] == "in_test_pro_renewal_1"
    assert len(fake_db.billing_records.items) == 1
    assert fake_db.billing_records.items[0]["status"] == "paid"
    assert fake_db.billing_records.items[0]["invoice_reference"] == "in_test_pro_renewal_1"

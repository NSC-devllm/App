from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
from decimal import Decimal

import pytest
from fastapi.testclient import TestClient
from starlette.requests import Request

from src.api import main as api_main


@contextmanager
def _fake_session_scope():
    yield object()


def _allow_admin(request: Request):
    request.state.actor_id = "actor-1"
    request.state.actor_roles = ["ADMIN_READ"]
    return None


@pytest.fixture()
def client(monkeypatch: pytest.MonkeyPatch) -> TestClient:
    api_main.app.dependency_overrides[api_main.require_admin_read] = _allow_admin
    monkeypatch.setattr(api_main, "session_scope", _fake_session_scope)
    with TestClient(api_main.app) as client:
        yield client
    api_main.app.dependency_overrides = {}


def test_get_admin_tx_success(
    monkeypatch: pytest.MonkeyPatch, client: TestClient
) -> None:
    expected = {
        "tx_id": "tx-123",
        "event_time": datetime(2026, 2, 5, 1, 0, 0, tzinfo=timezone.utc).isoformat(),
        "entry_type": "PAYMENT",
        "amount": str(Decimal("10.00")),
        "amount_signed": str(Decimal("-10.00")),
        "status": "SETTLED",
        "status_group": "SETTLED",
        "sender_wallet_id": "wallet-1",
        "receiver_wallet_id": "wallet-2",
        "related": {"related_id": "po-1", "related_type": "PAYMENT_ORDER"},
        "paired_tx_id": "tx-456",
        "merchant_name": "Shop",
        "pairing_status": "COMPLETE",
        "data_lag_sec": 0,
    }

    recorded: dict[str, object] = {}
    sentinel = object()

    def fake_fetch(session, tx_id):
        assert tx_id == "tx-123"
        return sentinel

    def fake_build(context):
        assert context is sentinel
        return expected

    def fake_record(session, fields):
        recorded.update(fields)

    monkeypatch.setattr(api_main, "fetch_admin_tx_context", fake_fetch)
    monkeypatch.setattr(api_main, "build_admin_tx_response", fake_build)
    monkeypatch.setattr(api_main, "record_admin_audit", fake_record)

    response = client.get("/admin/tx/tx-123")

    assert response.status_code == 200
    payload = response.json()
    assert payload["tx_id"] == "tx-123"
    assert payload["pairing_status"] == "COMPLETE"
    assert recorded["result"] == "FOUND"
    assert recorded["status_code"] == 200
    assert recorded["resource_id"] == "tx-123"
    assert recorded["result_count"] == 1


def test_get_admin_tx_404(monkeypatch: pytest.MonkeyPatch, client: TestClient) -> None:
    recorded: dict[str, object] = {}

    def fake_fetch(session, tx_id):
        assert tx_id == "missing-tx"
        return None

    def fake_record(session, fields):
        recorded.update(fields)

    monkeypatch.setattr(api_main, "fetch_admin_tx_context", fake_fetch)
    monkeypatch.setattr(api_main, "record_admin_audit", fake_record)

    response = client.get("/admin/tx/missing-tx")

    assert response.status_code == 404
    assert response.json()["detail"] == "tx_id not found"
    assert recorded["result"] == "NOT_FOUND"
    assert recorded["status_code"] == 404
    assert recorded["resource_id"] == "missing-tx"
    assert recorded["result_count"] == 0


def test_get_admin_tx_auth_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    api_main.app.dependency_overrides = {}
    monkeypatch.setenv("AUTH_MODE", "oidc")

    with TestClient(api_main.app) as client:
        response = client.get("/admin/tx/tx-123")

    assert response.status_code == 401
    assert response.json()["detail"] == "Authorization header missing"


def test_get_admin_tx_with_audit_role_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """ADMIN_AUDIT 단독 역할로도 조회 가능 (DEC-215 옵션 C)."""

    def _allow_audit(request: Request):
        request.state.actor_id = "auditor-1"
        request.state.actor_roles = ["ADMIN_AUDIT"]
        return None

    api_main.app.dependency_overrides[api_main.require_admin_read] = _allow_audit
    monkeypatch.setattr(api_main, "session_scope", _fake_session_scope)

    expected = {
        "tx_id": "tx-999",
        "event_time": datetime(2026, 2, 5, 1, 0, 0, tzinfo=timezone.utc).isoformat(),
        "entry_type": "PAYMENT",
        "amount": str(Decimal("5.00")),
        "amount_signed": None,
        "status": "SETTLED",
        "status_group": "SETTLED",
        "sender_wallet_id": "wallet-a",
        "receiver_wallet_id": None,
        "related": None,
        "paired_tx_id": None,
        "merchant_name": None,
        "pairing_status": "UNKNOWN",
        "data_lag_sec": 0,
    }
    sentinel = object()
    monkeypatch.setattr(api_main, "fetch_admin_tx_context", lambda s, t: sentinel)
    monkeypatch.setattr(api_main, "build_admin_tx_response", lambda c: expected)
    monkeypatch.setattr(api_main, "record_admin_audit", lambda s, f: None)

    with TestClient(api_main.app) as client:
        response = client.get("/admin/tx/tx-999")

    api_main.app.dependency_overrides = {}
    assert response.status_code == 200
    assert response.json()["tx_id"] == "tx-999"


def test_get_admin_tx_forbidden_no_matching_role(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """ADMIN_READ/ADMIN_AUDIT 둘 다 없으면 403 (DEC-215)."""
    api_main.app.dependency_overrides = {}
    monkeypatch.setenv("AUTH_MODE", "oidc")

    monkeypatch.setattr(
        "src.api.auth._decode_token",
        lambda token: {"roles": ["VIEWER"], "sub": "user-no-access"},
    )

    with TestClient(api_main.app) as client:
        response = client.get(
            "/admin/tx/tx-123",
            headers={"Authorization": "Bearer fake-token"},
        )

    assert response.status_code == 403
    assert response.json()["detail"] == "Forbidden"


# ========== GET /admin/payment-orders/{order_id} ==========


def test_get_admin_order_success(
    monkeypatch: pytest.MonkeyPatch, client: TestClient
) -> None:
    from src.api.schemas import AdminOrderResponse, PairingStatus

    recorded: dict[str, object] = {}
    sentinel = object()

    fake_response = AdminOrderResponse(
        order={
            "order_id": "po-123",
            "user_id": "user-1",
            "merchant_name": "Shop",
            "amount": Decimal("10.00"),
            "status": "SETTLED",
            "status_group": "SUCCESS",
            "created_at": datetime(2026, 2, 5, 1, 0, 0, tzinfo=timezone.utc),
        },
        ledger_entries=[],
        pairing_status=PairingStatus.UNKNOWN,
    )

    class FakeContext:
        ledger_entries = ["entry1", "entry2"]

    def fake_fetch(session, order_id):
        assert order_id == "po-123"
        return FakeContext()

    def fake_build(context):
        return fake_response

    def fake_record(session, fields):
        recorded.update(fields)

    monkeypatch.setattr(api_main, "fetch_admin_order_context", fake_fetch)
    monkeypatch.setattr(api_main, "build_admin_order_response", fake_build)
    monkeypatch.setattr(api_main, "record_admin_audit", fake_record)

    response = client.get("/admin/payment-orders/po-123")

    assert response.status_code == 200
    payload = response.json()
    assert payload["order"]["order_id"] == "po-123"
    assert recorded["result"] == "FOUND"
    assert recorded["status_code"] == 200
    assert recorded["resource_id"] == "po-123"
    assert recorded["resource_type"] == "PAYMENT_ORDER"
    assert recorded["action"] == "ADMIN_ORDER_LOOKUP"
    assert recorded["result_count"] == 2


def test_get_admin_order_404(
    monkeypatch: pytest.MonkeyPatch, client: TestClient
) -> None:
    recorded: dict[str, object] = {}

    def fake_fetch(session, order_id):
        return None

    def fake_record(session, fields):
        recorded.update(fields)

    monkeypatch.setattr(api_main, "fetch_admin_order_context", fake_fetch)
    monkeypatch.setattr(api_main, "record_admin_audit", fake_record)

    response = client.get("/admin/payment-orders/missing-po")

    assert response.status_code == 404
    assert response.json()["detail"] == "order_id not found"
    assert recorded["result"] == "NOT_FOUND"
    assert recorded["status_code"] == 404
    assert recorded["result_count"] == 0


# ========== GET /admin/wallets/{wallet_id}/tx ==========


def test_get_admin_wallet_tx_success(
    monkeypatch: pytest.MonkeyPatch, client: TestClient
) -> None:
    from src.api.schemas import AdminWalletTxResponse

    recorded: dict[str, object] = {}
    sentinel_entries = [object(), object()]

    fake_response = AdminWalletTxResponse(
        wallet_id="wallet-A",
        entries=[],
        count=2,
    )

    def fake_fetch(session, wallet_id, *, from_time=None, to_time=None, limit=20):
        assert wallet_id == "wallet-A"
        return sentinel_entries

    def fake_build(wallet_id, entries):
        return fake_response

    def fake_record(session, fields):
        recorded.update(fields)

    monkeypatch.setattr(api_main, "fetch_admin_wallet_tx", fake_fetch)
    monkeypatch.setattr(api_main, "build_admin_wallet_tx_response", fake_build)
    monkeypatch.setattr(api_main, "record_admin_audit", fake_record)

    response = client.get("/admin/wallets/wallet-A/tx")

    assert response.status_code == 200
    payload = response.json()
    assert payload["wallet_id"] == "wallet-A"
    assert recorded["result"] == "FOUND"
    assert recorded["resource_type"] == "WALLET"
    assert recorded["resource_id"] == "wallet-A"
    assert recorded["action"] == "ADMIN_WALLET_TX_LOOKUP"
    assert recorded["result_count"] == 2


def test_get_admin_wallet_tx_empty(
    monkeypatch: pytest.MonkeyPatch, client: TestClient
) -> None:
    from src.api.schemas import AdminWalletTxResponse

    recorded: dict[str, object] = {}

    fake_response = AdminWalletTxResponse(
        wallet_id="wallet-X",
        entries=[],
        count=0,
    )

    def fake_fetch(session, wallet_id, *, from_time=None, to_time=None, limit=20):
        return []

    def fake_build(wallet_id, entries):
        return fake_response

    def fake_record(session, fields):
        recorded.update(fields)

    monkeypatch.setattr(api_main, "fetch_admin_wallet_tx", fake_fetch)
    monkeypatch.setattr(api_main, "build_admin_wallet_tx_response", fake_build)
    monkeypatch.setattr(api_main, "record_admin_audit", fake_record)

    response = client.get("/admin/wallets/wallet-X/tx")

    assert response.status_code == 200
    payload = response.json()
    assert payload["count"] == 0
    assert recorded["result"] == "EMPTY"
    assert recorded["result_count"] == 0


def test_get_admin_wallet_tx_query_params(
    monkeypatch: pytest.MonkeyPatch, client: TestClient
) -> None:
    from src.api.schemas import AdminWalletTxResponse

    captured: dict[str, object] = {}

    def fake_fetch(session, wallet_id, *, from_time=None, to_time=None, limit=20):
        captured["from_time"] = from_time
        captured["to_time"] = to_time
        captured["limit"] = limit
        return [object()]

    fake_response = AdminWalletTxResponse(
        wallet_id="wallet-A",
        entries=[],
        count=1,
    )

    def fake_build(wallet_id, entries):
        return fake_response

    monkeypatch.setattr(api_main, "fetch_admin_wallet_tx", fake_fetch)
    monkeypatch.setattr(api_main, "build_admin_wallet_tx_response", fake_build)
    monkeypatch.setattr(api_main, "record_admin_audit", lambda s, f: None)

    response = client.get(
        "/admin/wallets/wallet-A/tx?from=2026-01-01T00:00:00Z&to=2026-02-01T00:00:00Z&limit=50"
    )

    assert response.status_code == 200
    assert captured["from_time"] is not None
    assert captured["to_time"] is not None
    assert captured["limit"] == 50


def test_get_admin_wallet_tx_limit_exceeds_max(
    monkeypatch: pytest.MonkeyPatch, client: TestClient
) -> None:
    monkeypatch.setattr(api_main, "record_admin_audit", lambda s, f: None)

    response = client.get("/admin/wallets/wallet-A/tx?limit=200")

    assert response.status_code == 422

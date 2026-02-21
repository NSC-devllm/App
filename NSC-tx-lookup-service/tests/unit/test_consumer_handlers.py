from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
from decimal import Decimal
from types import SimpleNamespace

import pytest

from src.consumer import main as consumer_main
from src.consumer.events import EventValidationError
from src.consumer.metrics import PairingMetrics, VersionMissingCounter
from src.consumer.pairing import PairingSnapshot


@contextmanager
def _fake_session_scope():
    yield object()


def _config():
    return SimpleNamespace(
        ledger_topic="ledger.entry.upserted",
        payment_order_topic="payment.order.upserted",
    )


def test_handle_payload_ledger_topic(
    monkeypatch: pytest.MonkeyPatch, transaction_ledger_happy_rows
):
    config = _config()
    monkeypatch.setattr(consumer_main, "load_config", lambda: config)
    monkeypatch.setattr(consumer_main, "session_scope", _fake_session_scope)

    calls: dict[str, object] = {}

    def fake_upsert(session, event):
        calls["event"] = event
        snapshot = PairingSnapshot(
            payment_order_id="po-1",
            payment_tx_id=event.tx_id,
            receive_tx_id=None,
            payer_wallet_id=event.wallet_id,
            payee_wallet_id=None,
            amount=Decimal("10.00"),
            status=None,
            event_time=datetime.now(timezone.utc),
            complete=False,
            incomplete_age_sec=1.0,
        )
        return True, snapshot

    monkeypatch.setattr(consumer_main, "upsert_ledger_entry", fake_upsert)

    counter = VersionMissingCounter(log_every=1)
    pairing_metrics = PairingMetrics(log_every=1)

    payload = transaction_ledger_happy_rows[0]
    consumer_main._handle_payload(
        config.ledger_topic, payload, counter, pairing_metrics
    )
    consumer_main._handle_payload(
        config.ledger_topic, payload, counter, pairing_metrics
    )

    assert calls["event"].tx_id == payload["tx_id"]
    assert counter.processed == 2
    assert counter.missing_version == 2
    assert pairing_metrics.total == 2
    assert pairing_metrics.incomplete == 2


def test_handle_payload_payment_order_topic(
    monkeypatch: pytest.MonkeyPatch, payment_orders_happy_rows
):
    config = _config()
    monkeypatch.setattr(consumer_main, "load_config", lambda: config)
    monkeypatch.setattr(consumer_main, "session_scope", _fake_session_scope)

    calls: dict[str, object] = {}

    def fake_upsert(session, event):
        calls["event"] = event
        return False

    monkeypatch.setattr(consumer_main, "upsert_payment_order", fake_upsert)

    counter = VersionMissingCounter(log_every=1)
    pairing_metrics = PairingMetrics(log_every=1)

    payload = payment_orders_happy_rows[0]
    consumer_main._handle_payload(
        config.payment_order_topic, payload, counter, pairing_metrics
    )

    assert calls["event"].order_id == payload["order_id"]
    assert counter.processed == 1
    assert counter.missing_version == 0
    assert pairing_metrics.total == 0


def test_handle_payload_unknown_topic(monkeypatch: pytest.MonkeyPatch):
    config = _config()
    monkeypatch.setattr(consumer_main, "load_config", lambda: config)
    monkeypatch.setattr(consumer_main, "session_scope", _fake_session_scope)

    counter = VersionMissingCounter()
    pairing_metrics = PairingMetrics()

    with pytest.raises(EventValidationError):
        consumer_main._handle_payload("unknown-topic", {}, counter, pairing_metrics)


def test_handle_payload_invalid_payload(monkeypatch: pytest.MonkeyPatch):
    config = _config()
    monkeypatch.setattr(consumer_main, "load_config", lambda: config)
    monkeypatch.setattr(consumer_main, "session_scope", _fake_session_scope)

    counter = VersionMissingCounter()
    pairing_metrics = PairingMetrics()

    with pytest.raises(EventValidationError):
        consumer_main._handle_payload(
            config.ledger_topic, {"wallet_id": "w1"}, counter, pairing_metrics
        )

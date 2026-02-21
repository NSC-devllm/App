from __future__ import annotations

import json
import time
from datetime import datetime, timezone

import pytest

pytestmark = pytest.mark.e2e

from confluent_kafka import Producer
from fastapi.testclient import TestClient

from src.api.main import app
from src.common.config import load_config
from src.common.kafka import build_kafka_client_config
from src.consumer.main import run_consumer


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def test_admin_tx_end_to_end(monkeypatch):
    suffix = int(time.time())
    ledger_topic = f"ledger.entry.upserted.e2e.{suffix}"
    payment_topic = f"payment.order.upserted.e2e.{suffix}"
    group_id = f"bo-sync-consumer-e2e-{suffix}"

    monkeypatch.setenv("LEDGER_TOPIC", ledger_topic)
    monkeypatch.setenv("PAYMENT_ORDER_TOPIC", payment_topic)
    monkeypatch.setenv("KAFKA_GROUP_ID", group_id)
    monkeypatch.setenv("CONSUMER_OFFSET_RESET", "earliest")
    monkeypatch.setenv("AUTH_MODE", "disabled")

    config = load_config()
    producer = Producer(build_kafka_client_config(config))

    related_id = f"po-e2e-{suffix}"
    payment_tx = f"tx-e2e-pay-{suffix}"
    receive_tx = f"tx-e2e-rec-{suffix}"
    now = _iso_now()

    ledger_payment = {
        "tx_id": payment_tx,
        "wallet_id": "wallet-pay",
        "entry_type": "PAYMENT",
        "amount": "10.00",
        "amount_signed": "-10.00",
        "related_id": related_id,
        "related_type": "PAYMENT_ORDER",
        "event_time": now,
        "created_at": now,
        "updated_at": now,
        "version": 1,
    }
    ledger_receive = {
        "tx_id": receive_tx,
        "wallet_id": "wallet-rec",
        "entry_type": "RECEIVE",
        "amount": "10.00",
        "amount_signed": "10.00",
        "related_id": related_id,
        "related_type": "PAYMENT_ORDER",
        "event_time": now,
        "created_at": now,
        "updated_at": now,
        "version": 1,
    }
    payment_order = {
        "order_id": related_id,
        "user_id": "user-e2e",
        "merchant_name": "MERCHANT-E2E",
        "amount": "10.00",
        "status": "SETTLED",
        "created_at": now,
        "updated_at": now,
        "version": 1,
    }

    producer.produce(
        ledger_topic,
        key=ledger_payment["tx_id"],
        value=json.dumps(ledger_payment).encode("utf-8"),
    )
    producer.produce(
        ledger_topic,
        key=ledger_receive["tx_id"],
        value=json.dumps(ledger_receive).encode("utf-8"),
    )
    producer.produce(
        payment_topic,
        key=payment_order["order_id"],
        value=json.dumps(payment_order).encode("utf-8"),
    )
    producer.flush()

    run_consumer(max_messages=3, max_idle_seconds=10)

    client = TestClient(app)
    response = client.get(f"/admin/tx/{payment_tx}")
    assert response.status_code == 200
    payload = response.json()
    assert payload["tx_id"] == payment_tx
    assert payload["paired_tx_id"] == receive_tx
    assert payload["pairing_status"] == "COMPLETE"

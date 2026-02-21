from __future__ import annotations

import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from confluent_kafka import Producer
from sqlalchemy import select

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

from src.common.config import load_config
from src.common.kafka import build_kafka_client_config
from src.common.logging import configure_logging
from src.consumer.main import run_consumer
from src.db.models import PaymentLedgerPair
from src.db.session import session_scope


def publish_events(order_id: str, payment_tx: str, receive_tx: str) -> None:
    config = load_config()
    producer = Producer(build_kafka_client_config(config))
    now = datetime.now(timezone.utc).isoformat()

    ledger_payment = {
        "tx_id": payment_tx,
        "wallet_id": "wallet-e2e-1",
        "entry_type": "PAYMENT",
        "amount": "15.00",
        "amount_signed": "-15.00",
        "related_id": order_id,
        "related_type": "PAYMENT_ORDER",
        "event_time": now,
        "created_at": now,
        "updated_at": now,
        "version": 1,
    }

    ledger_receive = {
        "tx_id": receive_tx,
        "wallet_id": "wallet-e2e-2",
        "entry_type": "RECEIVE",
        "amount": "15.00",
        "amount_signed": "15.00",
        "related_id": order_id,
        "related_type": "PAYMENT_ORDER",
        "event_time": now,
        "created_at": now,
        "updated_at": now,
        "version": 1,
    }

    payment_order = {
        "order_id": order_id,
        "user_id": "user-e2e-1",
        "merchant_name": "MERCHANT-E2E",
        "amount": "15.00",
        "status": "SETTLED",
        "created_at": now,
        "updated_at": now,
        "version": 1,
    }

    producer.produce(
        config.ledger_topic,
        key=ledger_payment["tx_id"],
        value=json.dumps(ledger_payment).encode("utf-8"),
    )
    producer.produce(
        config.ledger_topic,
        key=ledger_receive["tx_id"],
        value=json.dumps(ledger_receive).encode("utf-8"),
    )
    producer.produce(
        config.payment_order_topic,
        key=payment_order["order_id"],
        value=json.dumps(payment_order).encode("utf-8"),
    )
    producer.flush()


def assert_pairing(order_id: str) -> None:
    with session_scope() as session:
        pair = session.execute(
            select(PaymentLedgerPair).where(
                PaymentLedgerPair.payment_order_id == order_id
            )
        ).scalar_one_or_none()
        if not pair:
            raise SystemExit("Pair row not found")
        payment_tx_id = pair.payment_tx_id
        receive_tx_id = pair.receive_tx_id

    if not payment_tx_id or not receive_tx_id:
        raise SystemExit("Pair row incomplete")


def main() -> None:
    configure_logging()

    suffix = int(time.time())
    order_id = f"po-e2e-{suffix}"
    payment_tx = f"tx-pay-e2e-{suffix}"
    receive_tx = f"tx-rec-e2e-{suffix}"

    os.environ["KAFKA_GROUP_ID"] = f"bo-sync-consumer-e2e-{suffix}"
    os.environ["LEDGER_TOPIC"] = f"ledger.entry.upserted.e2e.{suffix}"
    os.environ["PAYMENT_ORDER_TOPIC"] = f"payment.order.upserted.e2e.{suffix}"
    os.environ["CONSUMER_OFFSET_RESET"] = "earliest"

    publish_events(order_id, payment_tx, receive_tx)
    run_consumer(max_messages=3, max_idle_seconds=10)
    assert_pairing(order_id)

    print("E2E pairing check passed")


if __name__ == "__main__":
    main()

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

from confluent_kafka import Producer

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

from src.common.config import load_config
from src.common.kafka import build_kafka_client_config


def publish() -> None:
    config = load_config()
    producer = Producer(build_kafka_client_config(config))

    now = datetime.now(timezone.utc).isoformat()

    ledger_payment = {
        "tx_id": "tx-payment-002",
        "wallet_id": "wallet-001",
        "entry_type": "PAYMENT",
        "amount": "12000.00",
        "amount_signed": "-12000.00",
        "related_id": "po-002",
        "related_type": "PAYMENT_ORDER",
        "event_time": now,
        "created_at": now,
        "updated_at": now,
        "version": 2,
    }

    ledger_receive = {
        "tx_id": "tx-receive-002",
        "wallet_id": "wallet-002",
        "entry_type": "RECEIVE",
        "amount": "12000.00",
        "amount_signed": "12000.00",
        "related_id": "po-002",
        "related_type": "PAYMENT_ORDER",
        "event_time": now,
        "created_at": now,
        "updated_at": now,
        "version": 2,
    }

    payment_order = {
        "order_id": "po-002",
        "user_id": "user-002",
        "merchant_name": "MERCHANT-002",
        "amount": "12000.00",
        "status": "SETTLED",
        "created_at": now,
        "updated_at": now,
        "version": 2,
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


if __name__ == "__main__":
    publish()

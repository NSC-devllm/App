from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

from src.db.models import LedgerEntry, PaymentLedgerPair, PaymentOrder
from src.db.session import session_scope
from src.db.upsert import latest_wins_upsert


def seed() -> None:
    now = datetime.now(timezone.utc)

    ledger_payment = {
        "tx_id": "tx-payment-001",
        "wallet_id": "wallet-001",
        "entry_type": "PAYMENT",
        "amount": 10000.00,
        "amount_signed": -10000.00,
        "related_id": "po-001",
        "related_type": "PAYMENT_ORDER",
        "event_time": now,
        "created_at": now,
        "updated_at": now,
        "source_version": 1,
        "ingested_at": now,
    }

    ledger_receive = {
        "tx_id": "tx-receive-001",
        "wallet_id": "wallet-002",
        "entry_type": "RECEIVE",
        "amount": 10000.00,
        "amount_signed": 10000.00,
        "related_id": "po-001",
        "related_type": "PAYMENT_ORDER",
        "event_time": now,
        "created_at": now,
        "updated_at": now,
        "source_version": 1,
        "ingested_at": now,
    }

    payment_order = {
        "order_id": "po-001",
        "user_id": "user-001",
        "merchant_name": "MERCHANT-001",
        "amount": 10000.00,
        "status": "SETTLED",
        "created_at": now,
        "updated_at": now,
        "source_version": 1,
        "ingested_at": now,
    }

    pair = {
        "payment_order_id": "po-001",
        "payment_tx_id": "tx-payment-001",
        "receive_tx_id": "tx-receive-001",
        "payer_wallet_id": "wallet-001",
        "payee_wallet_id": "wallet-002",
        "amount": 10000.00,
        "status": "SETTLED",
        "event_time": now,
        "updated_at": now,
        "ingested_at": now,
    }

    with session_scope() as session:
        session.execute(
            latest_wins_upsert(LedgerEntry.__table__, ledger_payment, ["tx_id"])
        )
        session.execute(
            latest_wins_upsert(LedgerEntry.__table__, ledger_receive, ["tx_id"])
        )
        session.execute(
            latest_wins_upsert(PaymentOrder.__table__, payment_order, ["order_id"])
        )
        session.execute(
            latest_wins_upsert(PaymentLedgerPair.__table__, pair, ["payment_order_id"])
        )


if __name__ == "__main__":
    seed()

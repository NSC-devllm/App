from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

from src.consumer.pairing import (
    LedgerEntrySnapshot,
    PaymentOrderSnapshot,
    compute_pairing,
    should_update_pair,
)


def _dt(value: str) -> datetime:
    text = value.replace("Z", "+00:00")
    return datetime.fromisoformat(text).astimezone(timezone.utc)


def test_compute_pairing_incomplete_payment_only() -> None:
    entries = [
        LedgerEntrySnapshot(
            tx_id="tx-1",
            wallet_id="wallet-1",
            entry_type="PAYMENT",
            amount=Decimal("10.00"),
            event_time=_dt("2026-02-05T01:00:00Z"),
        )
    ]

    snapshot = compute_pairing(
        payment_order_id="po-1",
        entries=entries,
        payment_order=None,
        now=_dt("2026-02-05T01:00:05Z"),
    )

    assert snapshot.complete is False
    assert snapshot.payment_tx_id == "tx-1"
    assert snapshot.receive_tx_id is None
    assert snapshot.incomplete_age_sec == 5.0


def test_compute_pairing_complete_pair() -> None:
    entries = [
        LedgerEntrySnapshot(
            tx_id="tx-pay",
            wallet_id="wallet-pay",
            entry_type="PAYMENT",
            amount=Decimal("10.00"),
            event_time=_dt("2026-02-05T01:00:00Z"),
        ),
        LedgerEntrySnapshot(
            tx_id="tx-rec",
            wallet_id="wallet-rec",
            entry_type="RECEIVE",
            amount=Decimal("10.00"),
            event_time=_dt("2026-02-05T01:00:03Z"),
        ),
    ]

    snapshot = compute_pairing(
        payment_order_id="po-1",
        entries=entries,
        payment_order=PaymentOrderSnapshot(
            order_id="po-1", amount=Decimal("10.00"), status="SETTLED"
        ),
        now=_dt("2026-02-05T01:00:10Z"),
    )

    assert snapshot.complete is True
    assert snapshot.payment_tx_id == "tx-pay"
    assert snapshot.receive_tx_id == "tx-rec"
    assert snapshot.incomplete_age_sec is None
    assert snapshot.status == "SETTLED"


def test_should_update_pair_prevents_regression() -> None:
    assert should_update_pair(existing_complete=True, new_complete=False) is False
    assert should_update_pair(existing_complete=True, new_complete=True) is True
    assert should_update_pair(existing_complete=False, new_complete=True) is True


def test_compute_pairing_receive_only() -> None:
    entries = [
        LedgerEntrySnapshot(
            tx_id="tx-rec",
            wallet_id="wallet-rec",
            entry_type="RECEIVE",
            amount=Decimal("20.00"),
            event_time=_dt("2026-02-05T01:00:00Z"),
        )
    ]

    snapshot = compute_pairing(
        payment_order_id="po-2",
        entries=entries,
        payment_order=None,
        now=_dt("2026-02-05T01:00:10Z"),
    )

    assert snapshot.complete is False
    assert snapshot.payment_tx_id is None
    assert snapshot.receive_tx_id == "tx-rec"
    assert snapshot.payee_wallet_id == "wallet-rec"
    assert snapshot.payer_wallet_id is None
    assert snapshot.incomplete_age_sec == 10.0


def test_compute_pairing_amount_from_payment_entry() -> None:
    entries = [
        LedgerEntrySnapshot(
            tx_id="tx-pay",
            wallet_id="wallet-pay",
            entry_type="PAYMENT",
            amount=Decimal("50.00"),
            event_time=_dt("2026-02-05T01:00:00Z"),
        )
    ]

    snapshot = compute_pairing(
        payment_order_id="po-3",
        entries=entries,
        payment_order=None,
        now=_dt("2026-02-05T01:00:00Z"),
    )

    assert snapshot.amount == Decimal("50.00")
    assert snapshot.status is None

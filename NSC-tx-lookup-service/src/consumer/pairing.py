from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


@dataclass(frozen=True)
class LedgerEntrySnapshot:
    tx_id: str
    wallet_id: str
    entry_type: str
    amount: Decimal
    event_time: datetime


@dataclass(frozen=True)
class PaymentOrderSnapshot:
    order_id: str
    amount: Decimal
    status: str


@dataclass
class PairingSnapshot:
    payment_order_id: str
    payment_tx_id: str | None
    receive_tx_id: str | None
    payer_wallet_id: str | None
    payee_wallet_id: str | None
    amount: Decimal | None
    status: str | None
    event_time: datetime | None
    complete: bool
    incomplete_age_sec: float | None
    skipped: bool = False


def _as_snapshot(entry: Any) -> LedgerEntrySnapshot:
    return LedgerEntrySnapshot(
        tx_id=entry.tx_id,
        wallet_id=entry.wallet_id,
        entry_type=entry.entry_type,
        amount=entry.amount,
        event_time=entry.event_time,
    )


def compute_pairing(
    payment_order_id: str,
    entries: list[LedgerEntrySnapshot],
    payment_order: PaymentOrderSnapshot | None,
    now: datetime | None = None,
) -> PairingSnapshot:
    now = now or datetime.now(timezone.utc)

    payment_entry = next((e for e in entries if e.entry_type == "PAYMENT"), None)
    receive_entry = next((e for e in entries if e.entry_type == "RECEIVE"), None)

    complete = payment_entry is not None and receive_entry is not None

    event_time = None
    for entry in entries:
        if event_time is None or entry.event_time > event_time:
            event_time = entry.event_time

    incomplete_age_sec = None
    if not complete and event_time is not None:
        incomplete_age_sec = max(0.0, (now - event_time).total_seconds())

    amount = None
    status = None
    if payment_order:
        amount = payment_order.amount
        status = payment_order.status
    else:
        if payment_entry:
            amount = payment_entry.amount
        elif receive_entry:
            amount = receive_entry.amount

    return PairingSnapshot(
        payment_order_id=payment_order_id,
        payment_tx_id=payment_entry.tx_id if payment_entry else None,
        receive_tx_id=receive_entry.tx_id if receive_entry else None,
        payer_wallet_id=payment_entry.wallet_id if payment_entry else None,
        payee_wallet_id=receive_entry.wallet_id if receive_entry else None,
        amount=amount,
        status=status,
        event_time=event_time,
        complete=complete,
        incomplete_age_sec=incomplete_age_sec,
    )


def should_update_pair(existing_complete: bool, new_complete: bool) -> bool:
    if existing_complete and not new_complete:
        return False
    return True


def update_pairing_for_related_id(
    session: Session,
    related_id: str,
) -> PairingSnapshot | None:
    from sqlalchemy import select

    from src.db.models import LedgerEntry, PaymentLedgerPair, PaymentOrder
    from src.db.upsert import latest_wins_upsert

    ledger_entries = (
        session.execute(select(LedgerEntry).where(LedgerEntry.related_id == related_id))
        .scalars()
        .all()
    )
    if not ledger_entries:
        return None

    payment_order = session.execute(
        select(PaymentOrder).where(PaymentOrder.order_id == related_id)
    ).scalar_one_or_none()

    payment_snapshot = (
        PaymentOrderSnapshot(
            order_id=payment_order.order_id,
            amount=payment_order.amount,
            status=payment_order.status,
        )
        if payment_order
        else None
    )

    snapshot = compute_pairing(
        related_id,
        [_as_snapshot(entry) for entry in ledger_entries],
        payment_snapshot,
    )

    existing_pair = session.execute(
        select(PaymentLedgerPair).where(
            PaymentLedgerPair.payment_order_id == related_id
        )
    ).scalar_one_or_none()

    existing_complete = bool(
        existing_pair
        and existing_pair.payment_tx_id is not None
        and existing_pair.receive_tx_id is not None
    )

    if not should_update_pair(existing_complete, snapshot.complete):
        snapshot.skipped = True
        return snapshot

    ingested_at = datetime.now(timezone.utc)
    values = {
        "payment_order_id": snapshot.payment_order_id,
        "payment_tx_id": snapshot.payment_tx_id,
        "receive_tx_id": snapshot.receive_tx_id,
        "payer_wallet_id": snapshot.payer_wallet_id,
        "payee_wallet_id": snapshot.payee_wallet_id,
        "amount": snapshot.amount,
        "status": snapshot.status,
        "event_time": snapshot.event_time,
        "updated_at": ingested_at,
        "ingested_at": ingested_at,
    }
    stmt = latest_wins_upsert(PaymentLedgerPair.__table__, values, ["payment_order_id"])
    session.execute(stmt)
    return snapshot

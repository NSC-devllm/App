from __future__ import annotations

from datetime import datetime, timezone

from typing import Any

from src.api.schemas import (
    AdminOrderResponse,
    AdminTxResponse,
    AdminWalletTxResponse,
    LedgerEntryItem,
    PairingStatus,
    PaymentOrderDetail,
    RelatedInfo,
)
from src.db.admin_tx import AdminOrderContext, AdminTxContext


def _resolve_related_type(context: AdminTxContext) -> str:
    if context.ledger_entry.related_type:
        return context.ledger_entry.related_type
    if context.payment_order:
        return "PAYMENT_ORDER"
    return "UNKNOWN"


def _resolve_pairing(
    context: AdminTxContext,
) -> tuple[PairingStatus, str | None, str | None, str | None]:
    ledger = context.ledger_entry
    related_type = _resolve_related_type(context)

    if not ledger.related_id or related_type != "PAYMENT_ORDER":
        return PairingStatus.UNKNOWN, None, None, None

    payment_tx_id = None
    receive_tx_id = None
    payer_wallet_id = None
    payee_wallet_id = None

    if context.payment_pair:
        payment_tx_id = context.payment_pair.payment_tx_id
        receive_tx_id = context.payment_pair.receive_tx_id
        payer_wallet_id = context.payment_pair.payer_wallet_id
        payee_wallet_id = context.payment_pair.payee_wallet_id

    if context.peer_entry:
        if context.peer_entry.entry_type == "PAYMENT":
            payment_tx_id = payment_tx_id or context.peer_entry.tx_id
            payer_wallet_id = payer_wallet_id or context.peer_entry.wallet_id
        elif context.peer_entry.entry_type == "RECEIVE":
            receive_tx_id = receive_tx_id or context.peer_entry.tx_id
            payee_wallet_id = payee_wallet_id or context.peer_entry.wallet_id

    if ledger.entry_type == "PAYMENT":
        payment_tx_id = payment_tx_id or ledger.tx_id
        payer_wallet_id = payer_wallet_id or ledger.wallet_id
    elif ledger.entry_type == "RECEIVE":
        receive_tx_id = receive_tx_id or ledger.tx_id
        payee_wallet_id = payee_wallet_id or ledger.wallet_id

    complete = bool(payment_tx_id and receive_tx_id)
    pairing_status = PairingStatus.COMPLETE if complete else PairingStatus.INCOMPLETE

    paired_tx_id = None
    if ledger.entry_type == "PAYMENT":
        paired_tx_id = receive_tx_id
    elif ledger.entry_type == "RECEIVE":
        paired_tx_id = payment_tx_id

    return pairing_status, paired_tx_id, payer_wallet_id, payee_wallet_id


def _compute_data_lag_sec(context: AdminTxContext) -> int | None:
    ledger = context.ledger_entry
    base_time = max(ledger.ingested_at, ledger.event_time)
    if not base_time:
        return None
    lag = datetime.now(timezone.utc) - base_time
    return max(0, int(lag.total_seconds()))


def _resolve_status_group(status: str | None) -> str:
    if not status:
        return "UNKNOWN"
    normalized = status.strip().upper()
    if normalized in {"SETTLED", "COMPLETED", "SUCCESS", "SUCCEEDED", "PAID"}:
        return "SUCCESS"
    if normalized in {"FAILED", "CANCELLED", "CANCELED", "REJECTED", "DECLINED"}:
        return "FAIL"
    if normalized in {"CREATED", "PENDING", "PROCESSING", "AUTHORIZED"}:
        return "IN_PROGRESS"
    return "UNKNOWN"


def build_admin_tx_response(context: AdminTxContext) -> AdminTxResponse:
    ledger = context.ledger_entry

    pairing_status, paired_tx_id, sender_wallet_id, receiver_wallet_id = (
        _resolve_pairing(context)
    )

    related = None
    if ledger.related_id:
        related = RelatedInfo(
            related_id=ledger.related_id, related_type=_resolve_related_type(context)
        )

    status = context.payment_order.status if context.payment_order else None
    merchant_name = (
        context.payment_order.merchant_name if context.payment_order else None
    )

    return AdminTxResponse(
        tx_id=ledger.tx_id,
        event_time=ledger.event_time,
        entry_type=ledger.entry_type,
        amount=ledger.amount,
        amount_signed=ledger.amount_signed,
        status=status,
        status_group=_resolve_status_group(status),
        sender_wallet_id=sender_wallet_id,
        receiver_wallet_id=receiver_wallet_id,
        related=related,
        paired_tx_id=paired_tx_id,
        merchant_name=merchant_name,
        pairing_status=pairing_status,
        data_lag_sec=_compute_data_lag_sec(context),
    )


def _compute_entry_data_lag_sec(entry: Any) -> int | None:
    ingested = getattr(entry, "ingested_at", None)
    event = getattr(entry, "event_time", None)
    candidates = [t for t in (ingested, event) if t is not None]
    if not candidates:
        return None
    base_time = max(candidates)
    lag = datetime.now(timezone.utc) - base_time
    return max(0, int(lag.total_seconds()))


def _build_ledger_entry_item(
    entry: Any,
    *,
    payment_pair: Any | None = None,
    payment_order: Any | None = None,
) -> LedgerEntryItem:
    status = payment_order.status if payment_order else None

    paired_tx_id = None
    pairing_status = PairingStatus.UNKNOWN

    if entry.related_id:
        if payment_pair and payment_pair.payment_tx_id and payment_pair.receive_tx_id:
            pairing_status = PairingStatus.COMPLETE
            if entry.entry_type == "PAYMENT":
                paired_tx_id = payment_pair.receive_tx_id
            elif entry.entry_type == "RECEIVE":
                paired_tx_id = payment_pair.payment_tx_id
        elif payment_pair or entry.related_type in (None, "PAYMENT_ORDER"):
            pairing_status = PairingStatus.INCOMPLETE

    return LedgerEntryItem(
        tx_id=entry.tx_id,
        event_time=entry.event_time,
        entry_type=entry.entry_type,
        amount=entry.amount,
        amount_signed=entry.amount_signed,
        wallet_id=entry.wallet_id,
        status=status,
        status_group=_resolve_status_group(status),
        paired_tx_id=paired_tx_id,
        pairing_status=pairing_status,
        data_lag_sec=_compute_entry_data_lag_sec(entry),
    )


def build_admin_order_response(context: AdminOrderContext) -> AdminOrderResponse:
    order = context.payment_order

    order_detail = PaymentOrderDetail(
        order_id=order.order_id,
        user_id=order.user_id,
        merchant_name=order.merchant_name,
        amount=order.amount,
        status=order.status,
        status_group=_resolve_status_group(order.status),
        created_at=order.created_at,
    )

    items = [
        _build_ledger_entry_item(
            e,
            payment_pair=context.payment_pair,
            payment_order=order,
        )
        for e in context.ledger_entries
    ]

    if (
        context.payment_pair
        and context.payment_pair.payment_tx_id
        and context.payment_pair.receive_tx_id
    ):
        overall_pairing = PairingStatus.COMPLETE
    elif context.ledger_entries:
        overall_pairing = PairingStatus.INCOMPLETE
    else:
        overall_pairing = PairingStatus.UNKNOWN

    return AdminOrderResponse(
        order=order_detail,
        ledger_entries=items,
        pairing_status=overall_pairing,
    )


def build_admin_wallet_tx_response(
    wallet_id: str,
    entries: list[Any],
) -> AdminWalletTxResponse:
    items = [_build_ledger_entry_item(e) for e in entries]
    return AdminWalletTxResponse(
        wallet_id=wallet_id,
        entries=items,
        count=len(items),
    )

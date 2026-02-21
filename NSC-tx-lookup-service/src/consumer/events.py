from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Mapping


class EventValidationError(ValueError):
    pass


def _parse_decimal(value: Any, field: str) -> Decimal:
    if value is None:
        raise EventValidationError(f"{field} is required")
    try:
        return Decimal(str(value))
    except Exception as exc:
        raise EventValidationError(f"{field} must be numeric (got {value!r})") from exc


def _parse_datetime(value: Any, field: str) -> datetime:
    if value is None:
        raise EventValidationError(f"{field} is required")
    if isinstance(value, datetime):
        return (
            value.astimezone(timezone.utc)
            if value.tzinfo
            else value.replace(tzinfo=timezone.utc)
        )
    if not isinstance(value, str):
        raise EventValidationError(f"{field} must be ISO-8601 string (got {value!r})")
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise EventValidationError(
            f"{field} must be ISO-8601 string (got {value!r})"
        ) from exc
    return (
        parsed.astimezone(timezone.utc)
        if parsed.tzinfo
        else parsed.replace(tzinfo=timezone.utc)
    )


def _parse_optional_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    return _parse_datetime(value, "updated_at")


def _parse_optional_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except Exception as exc:
        raise EventValidationError(f"version must be int (got {value!r})") from exc


@dataclass(frozen=True)
class LedgerEntryUpserted:
    tx_id: str
    wallet_id: str
    entry_type: str
    amount: Decimal
    amount_signed: Decimal | None
    related_id: str | None
    related_type: str | None
    event_time: datetime
    created_at: datetime
    updated_at: datetime | None
    source_version: int | None

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "LedgerEntryUpserted":
        tx_id = str(payload.get("tx_id") or "").strip()
        wallet_id = str(payload.get("wallet_id") or "").strip()
        entry_type = str(payload.get("entry_type") or payload.get("type") or "").strip()
        if not tx_id:
            raise EventValidationError("tx_id is required")
        if not wallet_id:
            raise EventValidationError("wallet_id is required")
        if not entry_type:
            raise EventValidationError("entry_type is required")

        amount = _parse_decimal(payload.get("amount"), "amount")
        amount_signed = payload.get("amount_signed")
        parsed_amount_signed = (
            _parse_decimal(amount_signed, "amount_signed")
            if amount_signed is not None
            else None
        )

        related_id = payload.get("related_id")
        related_type = payload.get("related_type")

        event_time_value = (
            payload.get("event_time")
            or payload.get("source_created_at")
            or payload.get("created_at")
        )
        event_time = _parse_datetime(event_time_value, "event_time")
        created_value = (
            payload.get("source_created_at")
            or payload.get("created_at")
            or payload.get("event_time")
        )
        created_at = _parse_datetime(created_value, "created_at")

        updated_at = _parse_optional_datetime(payload.get("updated_at"))
        version_value = payload.get("version") or payload.get("source_version")
        source_version = _parse_optional_int(version_value)

        return cls(
            tx_id=tx_id,
            wallet_id=wallet_id,
            entry_type=entry_type,
            amount=amount,
            amount_signed=parsed_amount_signed,
            related_id=str(related_id).strip() if related_id is not None else None,
            related_type=(
                str(related_type).strip() if related_type is not None else None
            ),
            event_time=event_time,
            created_at=created_at,
            updated_at=updated_at,
            source_version=source_version,
        )


@dataclass(frozen=True)
class PaymentOrderUpserted:
    order_id: str
    user_id: str | None
    merchant_name: str | None
    amount: Decimal
    status: str
    created_at: datetime
    updated_at: datetime | None
    source_version: int | None

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "PaymentOrderUpserted":
        order_id = str(payload.get("order_id") or "").strip()
        status = str(payload.get("status") or "").strip()
        if not order_id:
            raise EventValidationError("order_id is required")
        if not status:
            raise EventValidationError("status is required")

        amount = _parse_decimal(payload.get("amount"), "amount")

        created_at = _parse_datetime(payload.get("created_at"), "created_at")
        updated_at = _parse_optional_datetime(payload.get("updated_at"))
        version_value = payload.get("version") or payload.get("source_version")
        source_version = _parse_optional_int(version_value)

        user_id = payload.get("user_id")
        merchant_name = payload.get("merchant_name")

        return cls(
            order_id=order_id,
            user_id=str(user_id).strip() if user_id is not None else None,
            merchant_name=(
                str(merchant_name).strip() if merchant_name is not None else None
            ),
            amount=amount,
            status=status,
            created_at=created_at,
            updated_at=updated_at,
            source_version=source_version,
        )

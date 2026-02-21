from __future__ import annotations

from datetime import datetime, timezone

import pytest

from src.consumer.events import (
    EventValidationError,
    LedgerEntryUpserted,
    PaymentOrderUpserted,
    _parse_datetime,
    _parse_decimal,
    _parse_optional_int,
)


def test_ledger_entry_from_dict_happy(transaction_ledger_happy_rows):
    payload = transaction_ledger_happy_rows[0]
    event = LedgerEntryUpserted.from_dict(payload)

    assert event.tx_id == payload["tx_id"]
    assert event.wallet_id == payload["wallet_id"]
    assert event.entry_type == payload["type"]
    assert event.amount is not None
    assert event.event_time is not None


def test_ledger_entry_missing_tx_id_raises():
    with pytest.raises(EventValidationError):
        LedgerEntryUpserted.from_dict(
            {
                "wallet_id": "wallet-1",
                "entry_type": "PAYMENT",
                "amount": "1.00",
                "event_time": "2026-02-05T01:00:00Z",
                "created_at": "2026-02-05T01:00:00Z",
            }
        )


def test_payment_order_from_dict_happy(payment_orders_happy_rows):
    payload = payment_orders_happy_rows[0]
    event = PaymentOrderUpserted.from_dict(payload)

    assert event.order_id == payload["order_id"]
    assert event.status == payload["status"]
    assert event.amount is not None


def test_payment_order_missing_status_raises():
    with pytest.raises(EventValidationError):
        PaymentOrderUpserted.from_dict(
            {
                "order_id": "order-1",
                "amount": "10.00",
                "created_at": "2026-02-05T01:00:00Z",
            }
        )


def test_ledger_entry_edge_empty_strings_raise(transaction_ledger_edge_rows):
    payload = transaction_ledger_edge_rows[0]
    with pytest.raises(EventValidationError):
        LedgerEntryUpserted.from_dict(payload)


def test_ledger_entry_error_none_tx_id_raises(transaction_ledger_error_rows):
    payload = transaction_ledger_error_rows[0]
    with pytest.raises(EventValidationError):
        LedgerEntryUpserted.from_dict(payload)


def test_payment_order_edge_empty_strings_raise(payment_orders_edge_rows):
    payload = payment_orders_edge_rows[0]
    with pytest.raises(EventValidationError):
        PaymentOrderUpserted.from_dict(payload)


def test_payment_order_error_none_order_id_raises(payment_orders_error_rows):
    payload = payment_orders_error_rows[0]
    with pytest.raises(EventValidationError):
        PaymentOrderUpserted.from_dict(payload)


# --- Parser helper tests ---


def test_parse_decimal_none_raises():
    with pytest.raises(EventValidationError, match="amount is required"):
        _parse_decimal(None, "amount")


def test_parse_decimal_invalid_raises():
    with pytest.raises(EventValidationError, match="amount must be numeric"):
        _parse_decimal("not-a-number", "amount")


def test_parse_datetime_none_raises():
    with pytest.raises(EventValidationError, match="event_time is required"):
        _parse_datetime(None, "event_time")


def test_parse_datetime_with_datetime_object():
    dt = datetime(2026, 2, 5, 1, 0, 0, tzinfo=timezone.utc)
    result = _parse_datetime(dt, "event_time")
    assert result == dt


def test_parse_datetime_naive_gets_utc():
    dt = datetime(2026, 2, 5, 1, 0, 0)
    result = _parse_datetime(dt, "event_time")
    assert result.tzinfo == timezone.utc


def test_parse_datetime_non_string_raises():
    with pytest.raises(EventValidationError, match="ISO-8601"):
        _parse_datetime(12345, "event_time")


def test_parse_datetime_invalid_string_raises():
    with pytest.raises(EventValidationError, match="ISO-8601"):
        _parse_datetime("not-a-date", "event_time")


def test_parse_optional_int_none():
    assert _parse_optional_int(None) is None


def test_parse_optional_int_valid():
    assert _parse_optional_int("42") == 42
    assert _parse_optional_int(7) == 7


def test_parse_optional_int_invalid_raises():
    with pytest.raises(EventValidationError, match="version must be int"):
        _parse_optional_int("abc")


# --- Field alias tests ---


def test_ledger_entry_uses_source_created_at_alias():
    payload = {
        "tx_id": "tx-alias",
        "wallet_id": "wallet-1",
        "entry_type": "PAYMENT",
        "amount": "10.00",
        "source_created_at": "2026-02-05T01:00:00Z",
    }
    event = LedgerEntryUpserted.from_dict(payload)
    assert event.tx_id == "tx-alias"
    assert event.event_time is not None
    assert event.created_at is not None


def test_ledger_entry_with_version_and_updated_at():
    payload = {
        "tx_id": "tx-full",
        "wallet_id": "wallet-1",
        "entry_type": "PAYMENT",
        "amount": "10.00",
        "amount_signed": "-10.00",
        "related_id": "po-1",
        "related_type": "PAYMENT_ORDER",
        "event_time": "2026-02-05T01:00:00Z",
        "created_at": "2026-02-05T01:00:00Z",
        "updated_at": "2026-02-05T01:00:01Z",
        "version": 5,
    }
    event = LedgerEntryUpserted.from_dict(payload)
    assert event.source_version == 5
    assert event.updated_at is not None
    assert event.amount_signed is not None
    assert event.related_id == "po-1"


def test_payment_order_with_all_fields():
    payload = {
        "order_id": "order-full",
        "user_id": "user-1",
        "merchant_name": "Shop",
        "amount": "100.00",
        "status": "SETTLED",
        "created_at": "2026-02-05T01:00:00Z",
        "updated_at": "2026-02-05T01:00:01Z",
        "source_version": 3,
    }
    event = PaymentOrderUpserted.from_dict(payload)
    assert event.user_id == "user-1"
    assert event.merchant_name == "Shop"
    assert event.source_version == 3
    assert event.updated_at is not None

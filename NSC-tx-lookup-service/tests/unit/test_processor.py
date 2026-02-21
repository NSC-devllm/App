from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import MagicMock, patch

from src.consumer.events import LedgerEntryUpserted, PaymentOrderUpserted
from src.consumer.processor import upsert_ledger_entry, upsert_payment_order


def _make_ledger_event(**overrides) -> LedgerEntryUpserted:
    defaults = {
        "tx_id": "tx-1",
        "wallet_id": "wallet-1",
        "entry_type": "PAYMENT",
        "amount": Decimal("10.00"),
        "amount_signed": Decimal("-10.00"),
        "related_id": "po-1",
        "related_type": "PAYMENT_ORDER",
        "event_time": datetime(2026, 2, 5, 1, 0, 0, tzinfo=timezone.utc),
        "created_at": datetime(2026, 2, 5, 1, 0, 0, tzinfo=timezone.utc),
        "updated_at": datetime(2026, 2, 5, 1, 0, 1, tzinfo=timezone.utc),
        "source_version": 1,
    }
    defaults.update(overrides)
    return LedgerEntryUpserted(**defaults)


def _make_payment_order_event(**overrides) -> PaymentOrderUpserted:
    defaults = {
        "order_id": "order-1",
        "user_id": "user-1",
        "merchant_name": "Shop",
        "amount": Decimal("10.00"),
        "status": "SETTLED",
        "created_at": datetime(2026, 2, 5, 1, 0, 0, tzinfo=timezone.utc),
        "updated_at": datetime(2026, 2, 5, 1, 0, 1, tzinfo=timezone.utc),
        "source_version": 1,
    }
    defaults.update(overrides)
    return PaymentOrderUpserted(**defaults)


@patch("src.consumer.processor.update_pairing_for_related_id")
@patch("src.db.upsert.latest_wins_upsert")
def test_upsert_ledger_entry_with_version(mock_upsert, mock_pairing):
    session = MagicMock()
    mock_upsert.return_value = "stmt"
    mock_pairing.return_value = None

    event = _make_ledger_event(source_version=1, updated_at=None)
    missing_version, snapshot = upsert_ledger_entry(session, event)

    assert missing_version is False
    mock_upsert.assert_called_once()
    mock_pairing.assert_called_once_with(session, "po-1")


@patch("src.consumer.processor.update_pairing_for_related_id")
@patch("src.db.upsert.latest_wins_upsert")
def test_upsert_ledger_entry_missing_version(mock_upsert, mock_pairing):
    session = MagicMock()
    mock_upsert.return_value = "stmt"
    mock_pairing.return_value = None

    event = _make_ledger_event(source_version=None, updated_at=None)
    missing_version, snapshot = upsert_ledger_entry(session, event)

    assert missing_version is True


@patch("src.consumer.processor.update_pairing_for_related_id")
@patch("src.db.upsert.latest_wins_upsert")
def test_upsert_ledger_entry_no_related_id(mock_upsert, mock_pairing):
    session = MagicMock()
    mock_upsert.return_value = "stmt"

    event = _make_ledger_event(related_id=None)
    missing_version, snapshot = upsert_ledger_entry(session, event)

    assert snapshot is None
    mock_pairing.assert_not_called()


@patch("src.consumer.processor.update_pairing_for_related_id")
@patch("src.db.upsert.latest_wins_upsert")
def test_upsert_ledger_entry_skips_pairing_for_non_payment_order(
    mock_upsert, mock_pairing
):
    """DEC-216: related_type not in {None, PAYMENT_ORDER} must skip pairing."""
    session = MagicMock()
    mock_upsert.return_value = "stmt"

    event = _make_ledger_event(related_type="WITHDRAWAL", related_id="w-001")
    _, snapshot = upsert_ledger_entry(session, event)

    assert snapshot is None
    mock_pairing.assert_not_called()


@patch("src.consumer.processor.update_pairing_for_related_id")
@patch("src.db.upsert.latest_wins_upsert")
def test_upsert_ledger_entry_pairs_when_related_type_none(mock_upsert, mock_pairing):
    """DEC-216: related_type=None with related_id should still trigger pairing."""
    session = MagicMock()
    mock_upsert.return_value = "stmt"
    mock_pairing.return_value = None

    event = _make_ledger_event(related_type=None, related_id="po-1")
    upsert_ledger_entry(session, event)

    mock_pairing.assert_called_once_with(session, "po-1")


@patch("src.consumer.processor.update_pairing_for_related_id")
@patch("src.db.upsert.latest_wins_upsert")
def test_upsert_ledger_entry_pairs_when_related_type_payment_order(
    mock_upsert, mock_pairing
):
    """DEC-216: related_type=PAYMENT_ORDER should trigger pairing."""
    session = MagicMock()
    mock_upsert.return_value = "stmt"
    mock_pairing.return_value = None

    event = _make_ledger_event(related_type="PAYMENT_ORDER", related_id="po-1")
    upsert_ledger_entry(session, event)

    mock_pairing.assert_called_once_with(session, "po-1")


@patch("src.db.upsert.latest_wins_upsert")
def test_upsert_payment_order(mock_upsert):
    session = MagicMock()
    mock_upsert.return_value = "stmt"

    event = _make_payment_order_event()
    missing = upsert_payment_order(session, event)

    assert missing is False
    session.execute.assert_called_once_with("stmt")


@patch("src.db.upsert.latest_wins_upsert")
def test_upsert_payment_order_missing_version(mock_upsert):
    session = MagicMock()
    mock_upsert.return_value = "stmt"

    event = _make_payment_order_event(source_version=None, updated_at=None)
    missing = upsert_payment_order(session, event)

    assert missing is True

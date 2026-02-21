from __future__ import annotations

from datetime import datetime, timedelta, timezone

from src.consumer.metrics import FreshnessTracker


def test_freshness_tracker_keeps_latest_event_time():
    tracker = FreshnessTracker()
    older = datetime.now(timezone.utc) - timedelta(seconds=10)
    newer = datetime.now(timezone.utc)

    tracker.record("ledger.entry.upserted", older)
    tracker.record("ledger.entry.upserted", newer)

    assert tracker.last_event_time["ledger.entry.upserted"] == newer


def test_freshness_tracker_ignores_older_out_of_order():
    tracker = FreshnessTracker()
    newer = datetime.now(timezone.utc)
    older = newer - timedelta(seconds=30)

    tracker.record("payment.order.upserted", newer)
    tracker.record("payment.order.upserted", older)

    assert tracker.last_event_time["payment.order.upserted"] == newer


def test_freshness_tracker_skips_none():
    tracker = FreshnessTracker()

    tracker.record("ledger.entry.upserted", None)

    assert tracker.last_event_time == {}

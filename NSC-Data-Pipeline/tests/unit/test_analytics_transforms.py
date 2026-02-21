from __future__ import annotations

import hashlib
from datetime import date
from decimal import Decimal

from src.transforms import analytics


def test_transform_order_events_records_combines_sources() -> None:
    orders = [
        {
            "order_id": 101,
            "user_id": "user-1",
            "total_amount": "120.00",
            "status": "CREATED",
            "created_at": "2026-02-01T01:00:00Z",
        }
    ]
    payment_orders = [
        {
            "order_id": "101",
            "user_id": "user-1",
            "merchant_name": "CoffeeLab",
            "amount": "120.00",
            "status": "PAID",
            "created_at": "2026-02-01T01:05:00Z",
        }
    ]

    result = analytics.transform_order_events_records(
        orders, payment_orders, run_id="run-analytics"
    )

    assert len(result.records) == 2
    sources = {record["order_source"] for record in result.records}
    assert sources == {"ORDERS", "PAYMENT_ORDERS"}
    assert all(record["order_ref"] == "101" for record in result.records)
    assert all(
        record["event_date_kst"] == date(2026, 2, 1) for record in result.records
    )
    assert result.bad_records == []


def test_transform_order_items_records_validates_required_keys() -> None:
    records = [
        {"item_id": 1, "order_id": 101, "product_id": 10, "quantity": 2},
        {"item_id": 2, "order_id": 101},
    ]

    result = analytics.transform_order_items_records(records, run_id="run-analytics")

    assert len(result.records) == 1
    assert len(result.bad_records) == 1
    assert result.records[0]["order_ref"] == "101"
    bad_record = result.bad_records[0]
    assert "detected_date_kst" in bad_record
    assert isinstance(bad_record["detected_date_kst"], date)


def test_transform_products_records_parses_decimal() -> None:
    records = [
        {"product_id": 10, "category": "COFFEE", "price_krw": "2500.50"},
    ]

    result = analytics.transform_products_records(records, run_id="run-analytics")

    assert len(result.records) == 1
    assert result.records[0]["price_krw"] == Decimal("2500.50")


def test_build_fact_payment_anonymized_anonymizes_and_derives_category() -> None:
    order_events = [
        {
            "order_ref": "101",
            "order_source": "PAYMENT_ORDERS",
            "user_id": "user-1",
            "merchant_name": "CoffeeLab",
            "amount": "120.00",
            "status": "PAID",
            "event_time": "2026-02-01T01:05:00Z",
            "event_date_kst": date(2026, 2, 1),
        },
        {
            "order_ref": "101",
            "order_source": "ORDERS",
            "user_id": "user-1",
            "event_time": "2026-02-01T01:00:00Z",
            "event_date_kst": date(2026, 2, 1),
        },
    ]
    order_items = [
        {
            "item_id": 1,
            "order_ref": "101",
            "product_id": 10,
            "quantity": 1,
            "price_at_purchase": "10.00",
        },
        {
            "item_id": 2,
            "order_ref": "101",
            "product_id": 20,
            "quantity": 3,
            "price_at_purchase": "5.00",
        },
    ]
    products = [
        {"product_id": 10, "category": "BEANS"},
        {"product_id": 20, "category": "BREW"},
    ]

    rows = analytics.build_fact_payment_anonymized(
        order_events,
        order_items,
        products,
        run_id="run-analytics",
        salt="pepper",
    )

    assert len(rows) == 1
    row = rows[0]
    expected_key = hashlib.sha256("user-1pepper".encode("utf-8")).hexdigest()
    assert row["user_key"] == expected_key
    assert row["category"] == "BREW"
    assert row["date_kst"] == date(2026, 2, 1)

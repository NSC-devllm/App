from datetime import datetime, timezone
from decimal import Decimal

import pytest


@pytest.fixture
def payment_orders_happy_rows():
    return [
        {
            "order_id": "order_1001",
            "user_id": "user_1",
            "merchant_name": "Shop A",
            "amount": Decimal("100.00"),
            "status": "OK",
            "created_at": datetime(2026, 2, 1, 0, 0, 0, tzinfo=timezone.utc),
        },
    ]


@pytest.fixture
def payment_orders_edge_rows():
    return [
        {
            "order_id": "",
            "user_id": "",
            "merchant_name": "",
            "amount": Decimal("0.00"),
            "status": "",
            "created_at": datetime(2026, 2, 1, 23, 59, 59, tzinfo=timezone.utc),
        },
    ]


@pytest.fixture
def payment_orders_error_rows():
    return [
        {
            "order_id": None,
            "user_id": "user_1",
            "merchant_name": "Shop A",
            "amount": Decimal("100.00"),
            "status": "OK",
            "created_at": datetime(2026, 2, 1, 0, 0, 0, tzinfo=timezone.utc),
        },
        {
            "order_id": "order_1001",
            "user_id": "user_1",
            "merchant_name": "Shop A",
            "amount": Decimal("100.00"),
            "status": "OK",
            "created_at": datetime(2026, 2, 1, 0, 0, 0, tzinfo=timezone.utc),
        },
    ]

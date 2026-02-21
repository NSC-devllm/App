from datetime import datetime, timezone
from decimal import Decimal

import pytest


@pytest.fixture
def transaction_ledger_happy_rows():
    return [
        {
            "tx_id": "tx_1",
            "wallet_id": "user_1",
            "type": "CHARGE",
            "amount": Decimal("100.00"),
            "related_id": "related_id_1",
            "created_at": datetime(2026, 2, 1, 0, 0, 0, tzinfo=timezone.utc),
        },
    ]


@pytest.fixture
def transaction_ledger_edge_rows():
    return [
        {
            "tx_id": "",
            "wallet_id": "",
            "type": "",
            "amount": Decimal("0.00"),
            "related_id": "",
            "created_at": datetime(2026, 2, 1, 23, 59, 59, tzinfo=timezone.utc),
        },
    ]


@pytest.fixture
def transaction_ledger_error_rows():
    return [
        {
            "tx_id": None,
            "wallet_id": "user_1",
            "type": "CHARGE",
            "amount": Decimal("100.00"),
            "related_id": "related_id_1",
            "created_at": datetime(2026, 2, 1, 0, 0, 0, tzinfo=timezone.utc),
        },
        {
            "tx_id": "tx_1",
            "wallet_id": "user_1",
            "type": "CHARGE",
            "amount": Decimal("100.00"),
            "related_id": "related_id_1",
            "created_at": datetime(2026, 2, 1, 0, 0, 0, tzinfo=timezone.utc),
        },
    ]

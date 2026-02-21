from __future__ import annotations

import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

pytest_plugins = [
    "tests.unit.fixtures.payment_orders_fixtures",
    "tests.unit.fixtures.transaction_ledger_fixtures",
]

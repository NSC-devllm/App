from __future__ import annotations

import sys
from pathlib import Path

from fastapi.testclient import TestClient

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

from scripts.seed_local_db import seed
from src.api.main import app


def main() -> None:
    seed()
    client = TestClient(app)

    response = client.get("/admin/tx/tx-payment-001")
    if response.status_code != 200:
        raise SystemExit(f"Expected 200, got {response.status_code}: {response.text}")

    payload = response.json()
    if payload.get("tx_id") != "tx-payment-001":
        raise SystemExit("Unexpected tx_id in response")

    response_missing = client.get("/admin/tx/tx-missing")
    if response_missing.status_code != 404:
        raise SystemExit(f"Expected 404, got {response_missing.status_code}")

    print("Admin API smoke test passed")


if __name__ == "__main__":
    main()

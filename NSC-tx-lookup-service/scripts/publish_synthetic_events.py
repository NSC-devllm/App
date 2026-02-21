from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

from confluent_kafka import Producer

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

from src.common.config import load_config
from src.common.kafka import build_kafka_client_config


def _iso(value: datetime) -> str:
    return value.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _ledger_event(
    *,
    tx_id: str,
    wallet_id: str,
    entry_type: str,
    amount: str,
    related_id: str,
    event_time: datetime,
    version: int,
) -> dict[str, Any]:
    signed = amount if entry_type == "RECEIVE" else f"-{amount}"
    ts = _iso(event_time)
    return {
        "tx_id": tx_id,
        "wallet_id": wallet_id,
        "entry_type": entry_type,
        "amount": amount,
        "amount_signed": signed,
        "related_id": related_id,
        "related_type": "PAYMENT_ORDER",
        "event_time": ts,
        "created_at": ts,
        "updated_at": ts,
        "version": version,
    }


def _payment_order_event(
    *,
    order_id: str,
    amount: str,
    event_time: datetime,
    version: int,
) -> dict[str, Any]:
    ts = _iso(event_time)
    return {
        "order_id": order_id,
        "user_id": "user-synthetic",
        "merchant_name": "MERCHANT-SYNTHETIC",
        "amount": amount,
        "status": "SETTLED",
        "created_at": ts,
        "updated_at": ts,
        "version": version,
    }


def _build_scenario_records(
    scenario: str,
    run_id: str,
    now: datetime,
) -> tuple[list[tuple[str, str, dict[str, Any]]], dict[str, str]]:
    order_id = f"po-{run_id}"
    payment_tx = f"tx-pay-{run_id}"
    receive_tx = f"tx-rec-{run_id}"
    metadata = {
        "order_id": order_id,
        "payment_tx_id": payment_tx,
        "receive_tx_id": receive_tx,
    }

    if scenario == "happy":
        records = [
            (
                "ledger",
                payment_tx,
                _ledger_event(
                    tx_id=payment_tx,
                    wallet_id="wallet-payer",
                    entry_type="PAYMENT",
                    amount="100.00",
                    related_id=order_id,
                    event_time=now,
                    version=1,
                ),
            ),
            (
                "ledger",
                receive_tx,
                _ledger_event(
                    tx_id=receive_tx,
                    wallet_id="wallet-payee",
                    entry_type="RECEIVE",
                    amount="100.00",
                    related_id=order_id,
                    event_time=now,
                    version=1,
                ),
            ),
            (
                "payment",
                order_id,
                _payment_order_event(
                    order_id=order_id,
                    amount="100.00",
                    event_time=now,
                    version=1,
                ),
            ),
        ]
        return records, metadata

    if scenario == "duplicate":
        base, metadata = _build_scenario_records("happy", run_id, now)
        return base + base, metadata

    if scenario == "out_of_order":
        newer = now
        older = now - timedelta(minutes=3)
        records = [
            (
                "ledger",
                payment_tx,
                _ledger_event(
                    tx_id=payment_tx,
                    wallet_id="wallet-payer",
                    entry_type="PAYMENT",
                    amount="200.00",
                    related_id=order_id,
                    event_time=newer,
                    version=2,
                ),
            ),
            (
                "ledger",
                receive_tx,
                _ledger_event(
                    tx_id=receive_tx,
                    wallet_id="wallet-payee",
                    entry_type="RECEIVE",
                    amount="200.00",
                    related_id=order_id,
                    event_time=newer,
                    version=2,
                ),
            ),
            (
                "payment",
                order_id,
                _payment_order_event(
                    order_id=order_id,
                    amount="200.00",
                    event_time=newer,
                    version=2,
                ),
            ),
            (
                "ledger",
                payment_tx,
                _ledger_event(
                    tx_id=payment_tx,
                    wallet_id="wallet-payer",
                    entry_type="PAYMENT",
                    amount="150.00",
                    related_id=order_id,
                    event_time=older,
                    version=1,
                ),
            ),
            (
                "ledger",
                receive_tx,
                _ledger_event(
                    tx_id=receive_tx,
                    wallet_id="wallet-payee",
                    entry_type="RECEIVE",
                    amount="150.00",
                    related_id=order_id,
                    event_time=older,
                    version=1,
                ),
            ),
            (
                "payment",
                order_id,
                _payment_order_event(
                    order_id=order_id,
                    amount="150.00",
                    event_time=older,
                    version=1,
                ),
            ),
        ]
        return records, metadata

    if scenario == "error":
        records = [
            (
                "ledger",
                f"tx-invalid-ledger-{run_id}",
                {
                    "tx_id": f"tx-invalid-ledger-{run_id}",
                    "entry_type": "PAYMENT",
                    "amount": "not-a-number",
                    "related_id": order_id,
                    "created_at": _iso(now),
                },
            ),
            (
                "payment",
                f"po-invalid-{run_id}",
                {
                    "order_id": f"po-invalid-{run_id}",
                    "amount": "100.00",
                    "created_at": _iso(now),
                },
            ),
        ]
        return records, metadata

    raise ValueError(f"Unsupported scenario: {scenario}")


def publish_scenario(scenario: str, run_id: str) -> dict[str, str]:
    config = load_config()
    producer = Producer(build_kafka_client_config(config))
    records, metadata = _build_scenario_records(
        scenario=scenario,
        run_id=run_id,
        now=datetime.now(timezone.utc),
    )

    for stream, key, payload in records:
        topic = (
            config.ledger_topic if stream == "ledger" else config.payment_order_topic
        )
        producer.produce(
            topic,
            key=key,
            value=json.dumps(payload, ensure_ascii=True).encode("utf-8"),
        )
    producer.flush()
    return metadata


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Publish deterministic synthetic event scenarios."
    )
    parser.add_argument(
        "--scenario",
        required=True,
        choices=("happy", "duplicate", "out_of_order", "error"),
        help="Scenario name to publish.",
    )
    parser.add_argument(
        "--run-id",
        required=True,
        help="Stable run identifier to make tx/order IDs deterministic.",
    )
    args = parser.parse_args(list(argv) if argv is not None else None)

    metadata = publish_scenario(args.scenario, args.run_id)
    print(
        json.dumps(
            {
                "scenario": args.scenario,
                "run_id": args.run_id,
                **metadata,
            },
            ensure_ascii=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

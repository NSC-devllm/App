from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable, Mapping

from src.common.rules import RuleDefinition, select_rule
from src.common.time_utils import date_kst, now_utc, to_utc

ENTRY_TYPE_SIGN: dict[str, int] = {
    "CHARGE": 1,
    "WITHDRAW": -1,
    "PAYMENT": -1,
    "RECEIVE": 1,
    "REFUND_OUT": -1,
    "REFUND_IN": 1,
    "HOLD": 0,
    "RELEASE": 0,
    "MINT": 1,
    "BURN": -1,
}

ALLOWED_ENTRY_TYPES = frozenset(ENTRY_TYPE_SIGN.keys())


@dataclass(frozen=True)
class TransformResult:
    records: list[dict[str, Any]]
    bad_records: list[dict[str, Any]]


def _parse_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return to_utc(value)
    if isinstance(value, str):
        normalized = value.replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(normalized)
        except ValueError:
            return None
        return to_utc(parsed)
    return None


def _parse_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def derive_snapshot_ts(
    *, source_extracted_at: Any | None, ingested_at: Any | None
) -> datetime | None:
    return _parse_datetime(source_extracted_at) or _parse_datetime(ingested_at)


def derive_balance_total(
    balance_available: Decimal | None, balance_frozen: Decimal | None
) -> Decimal | None:
    if balance_available is None or balance_frozen is None:
        return None
    return balance_available + balance_frozen


def derive_amount_signed(
    entry_type: str | None, amount: Decimal | str | int | float | None
) -> Decimal | None:
    if not entry_type:
        return None
    sign = ENTRY_TYPE_SIGN.get(entry_type)
    if sign is None:
        return None
    amount_value = _parse_decimal(amount)
    if amount_value is None:
        return None
    amount_abs = abs(amount_value)
    return amount_abs * Decimal(sign)


def build_bad_record(
    record: Mapping[str, Any],
    *,
    reason: str,
    source_table: str,
    run_id: str,
    rule_id: str | None = None,
) -> dict[str, Any]:
    detected_at = now_utc()
    return {
        "detected_date_kst": date_kst(detected_at),
        "source_table": source_table,
        "reason": reason,
        "record_json": json.dumps(record, default=str, ensure_ascii=True),
        "run_id": run_id,
        "rule_id": rule_id,
        "detected_at": detected_at,
    }


def calculate_bad_records_rate(valid_count: int, bad_count: int) -> float:
    total = valid_count + bad_count
    if total == 0:
        return 0.0
    return bad_count / total


def _resolve_fail_threshold(rule: RuleDefinition | None) -> float | None:
    if rule is None:
        return None
    if rule.threshold is not None:
        return float(rule.threshold)
    if rule.severity_map and "fail" in rule.severity_map:
        return float(rule.severity_map["fail"])
    return None


def enforce_bad_records_rate(
    *,
    valid_count: int,
    bad_count: int,
    rule: RuleDefinition | None,
) -> float:
    rate = calculate_bad_records_rate(valid_count, bad_count)
    threshold = _resolve_fail_threshold(rule)
    if threshold is not None and rate > threshold:
        rule_id = rule.rule_id if rule else "unknown"
        raise RuntimeError(
            f"bad_records_rate {rate:.4f} exceeded threshold {threshold} "
            f"(rule_id={rule_id})"
        )
    return rate


def resolve_allowed_values(rule: RuleDefinition | None) -> set[str] | None:
    if rule is None or not rule.allowed_values:
        return None
    return {value for value in rule.allowed_values}


def build_status_lookup(
    payment_orders: Iterable[Mapping[str, Any]],
    *,
    key_field: str = "order_id",
    status_field: str = "status",
) -> dict[str, str]:
    lookup: dict[str, str] = {}
    for record in payment_orders:
        order_id = record.get(key_field)
        status = record.get(status_field)
        if order_id is None or status is None:
            continue
        lookup[str(order_id)] = str(status)
    return lookup


def transform_wallet_snapshot_records(
    records: Iterable[Mapping[str, Any]],
    *,
    run_id: str,
    rule_id: str | None = None,
) -> TransformResult:
    valid: list[dict[str, Any]] = []
    bad: list[dict[str, Any]] = []

    for record in records:
        user_id = record.get("user_id")
        if not user_id:
            bad.append(
                build_bad_record(
                    record,
                    reason="missing_user_id",
                    source_table="silver.wallet_snapshot",
                    run_id=run_id,
                    rule_id=rule_id,
                )
            )
            continue

        balance_available = _parse_decimal(record.get("balance"))
        balance_frozen = _parse_decimal(record.get("frozen_amount"))
        if balance_available is None or balance_frozen is None:
            bad.append(
                build_bad_record(
                    record,
                    reason="invalid_balance",
                    source_table="silver.wallet_snapshot",
                    run_id=run_id,
                    rule_id=rule_id,
                )
            )
            continue

        if balance_available < 0 or balance_frozen < 0:
            bad.append(
                build_bad_record(
                    record,
                    reason="negative_balance",
                    source_table="silver.wallet_snapshot",
                    run_id=run_id,
                    rule_id=rule_id,
                )
            )
            continue

        snapshot_ts = derive_snapshot_ts(
            source_extracted_at=record.get("source_extracted_at"),
            ingested_at=record.get("ingested_at"),
        )
        if snapshot_ts is None:
            bad.append(
                build_bad_record(
                    record,
                    reason="missing_snapshot_ts",
                    source_table="silver.wallet_snapshot",
                    run_id=run_id,
                    rule_id=rule_id,
                )
            )
            continue

        balance_total = derive_balance_total(balance_available, balance_frozen)
        if balance_total is None:
            bad.append(
                build_bad_record(
                    record,
                    reason="invalid_balance_total",
                    source_table="silver.wallet_snapshot",
                    run_id=run_id,
                    rule_id=rule_id,
                )
            )
            continue

        valid.append(
            {
                "snapshot_ts": snapshot_ts,
                "snapshot_date_kst": date_kst(snapshot_ts),
                "user_id": str(user_id),
                "balance_available": balance_available,
                "balance_frozen": balance_frozen,
                "balance_total": balance_total,
                "source_updated_at": _parse_datetime(record.get("updated_at")),
                "run_id": run_id,
                "rule_id": rule_id,
            }
        )

    return TransformResult(records=valid, bad_records=bad)


def transform_ledger_entries_records(
    records: Iterable[Mapping[str, Any]],
    *,
    run_id: str,
    rule_id: str | None = None,
    allowed_entry_types: set[str] | None = None,
    allowed_statuses: set[str] | None = None,
    status_lookup: Mapping[str, str] | None = None,
) -> TransformResult:
    valid: list[dict[str, Any]] = []
    bad: list[dict[str, Any]] = []

    for record in records:
        tx_id = record.get("tx_id")
        wallet_id = record.get("wallet_id")
        entry_type = record.get("type") or record.get("entry_type")

        if not tx_id or not wallet_id:
            bad.append(
                build_bad_record(
                    record,
                    reason="missing_tx_or_wallet",
                    source_table="silver.ledger_entries",
                    run_id=run_id,
                    rule_id=rule_id,
                )
            )
            continue

        if not entry_type:
            bad.append(
                build_bad_record(
                    record,
                    reason="missing_entry_type",
                    source_table="silver.ledger_entries",
                    run_id=run_id,
                    rule_id=rule_id,
                )
            )
            continue

        if allowed_entry_types is not None and entry_type not in allowed_entry_types:
            bad.append(
                build_bad_record(
                    record,
                    reason="invalid_entry_type",
                    source_table="silver.ledger_entries",
                    run_id=run_id,
                    rule_id=rule_id,
                )
            )
            continue

        amount = _parse_decimal(record.get("amount"))
        if amount is None:
            bad.append(
                build_bad_record(
                    record,
                    reason="invalid_amount",
                    source_table="silver.ledger_entries",
                    run_id=run_id,
                    rule_id=rule_id,
                )
            )
            continue

        if amount <= 0:
            bad.append(
                build_bad_record(
                    record,
                    reason="non_positive_amount",
                    source_table="silver.ledger_entries",
                    run_id=run_id,
                    rule_id=rule_id,
                )
            )
            continue

        amount_signed = derive_amount_signed(entry_type, amount)
        if amount_signed is None:
            bad.append(
                build_bad_record(
                    record,
                    reason="amount_signed_unavailable",
                    source_table="silver.ledger_entries",
                    run_id=run_id,
                    rule_id=rule_id,
                )
            )
            continue

        event_time = _parse_datetime(
            record.get("created_at") or record.get("event_time")
        )
        if event_time is None:
            bad.append(
                build_bad_record(
                    record,
                    reason="missing_event_time",
                    source_table="silver.ledger_entries",
                    run_id=run_id,
                    rule_id=rule_id,
                )
            )
            continue

        related_id_value = record.get("related_id")
        related_id = str(related_id_value) if related_id_value is not None else None
        status = record.get("status")
        if status_lookup is not None and related_id in status_lookup:
            status = status_lookup[related_id]

        if allowed_statuses is not None and status is not None:
            if status not in allowed_statuses:
                bad.append(
                    build_bad_record(
                        record,
                        reason="invalid_status",
                        source_table="silver.ledger_entries",
                        run_id=run_id,
                        rule_id=rule_id,
                    )
                )
                continue

        valid.append(
            {
                "tx_id": str(tx_id),
                "wallet_id": str(wallet_id),
                "event_time": event_time,
                "event_date_kst": date_kst(event_time),
                "entry_type": str(entry_type),
                "amount": amount,
                "amount_signed": amount_signed,
                "related_id": related_id,
                "related_type": record.get("related_type"),
                "status": status,
                "created_at": _parse_datetime(record.get("created_at")),
                "run_id": run_id,
                "rule_id": rule_id,
            }
        )

    return TransformResult(records=valid, bad_records=bad)


def transform_wallet_snapshot_with_rules(
    records: Iterable[Mapping[str, Any]],
    *,
    run_id: str,
    rules: Iterable[RuleDefinition],
) -> tuple[TransformResult, float]:
    bad_rate_rule = select_rule(rules, domain="silver", metric="bad_records_rate")
    rule_id = bad_rate_rule.rule_id if bad_rate_rule else None
    result = transform_wallet_snapshot_records(records, run_id=run_id, rule_id=rule_id)
    bad_rate = enforce_bad_records_rate(
        valid_count=len(result.records),
        bad_count=len(result.bad_records),
        rule=bad_rate_rule,
    )
    return result, bad_rate


def transform_ledger_entries_with_rules(
    records: Iterable[Mapping[str, Any]],
    *,
    run_id: str,
    rules: Iterable[RuleDefinition],
    status_lookup: Mapping[str, str] | None = None,
) -> tuple[TransformResult, float]:
    entry_type_rule = select_rule(rules, domain="silver", metric="entry_type_allowed")
    status_rule = select_rule(rules, domain="silver", metric="payment_status_allowed")
    bad_rate_rule = select_rule(rules, domain="silver", metric="bad_records_rate")

    allowed_entry_types = resolve_allowed_values(entry_type_rule)
    allowed_statuses = resolve_allowed_values(status_rule)
    rule_id = entry_type_rule.rule_id if entry_type_rule else None

    result = transform_ledger_entries_records(
        records,
        run_id=run_id,
        rule_id=rule_id,
        allowed_entry_types=allowed_entry_types,
        allowed_statuses=allowed_statuses,
        status_lookup=status_lookup,
    )

    bad_rate = enforce_bad_records_rate(
        valid_count=len(result.records),
        bad_count=len(result.bad_records),
        rule=bad_rate_rule,
    )
    return result, bad_rate

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable, Mapping

from src.common.contracts import TableContract, get_contract
from src.common.rules import RuleDefinition, select_rule
from src.common.time_utils import date_kst, now_utc, to_utc

SEVERITY_WARN = "WARN"
SEVERITY_CRITICAL = "CRITICAL"

TAG_SOURCE_STALE = "SOURCE_STALE"
TAG_EVENT_DROP = "EVENT_DROP_SUSPECTED"
TAG_DUP = "DUP_SUSPECTED"
TAG_CONTRACT = "CONTRACT_VIOLATION"


@dataclass(frozen=True)
class DQTableConfig:
    source_table: str
    window_start_ts: datetime
    window_end_ts: datetime
    dup_key_fields: tuple[str, ...] | None = None
    freshness_fields: tuple[str, ...] = ("ingested_at", "source_updated_at")
    dup_rule_metric: str | None = None


@dataclass(frozen=True)
class DQStatusOutput:
    dq_status: dict[str, Any]
    exceptions: list[dict[str, Any]]
    zero_window_count: int


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


def _is_missing(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str) and value.strip() == "":
        return True
    return False


def _calculate_rate(numerator: int, denominator: int) -> float:
    if denominator == 0:
        return 0.0
    return numerator / denominator


def compute_freshness_sec(
    records: Iterable[Mapping[str, Any]],
    *,
    now_ts: datetime,
    timestamp_fields: Iterable[str],
) -> float | None:
    max_ts: datetime | None = None
    for record in records:
        for field in timestamp_fields:
            ts = _parse_datetime(record.get(field))
            if ts is None:
                continue
            if max_ts is None or ts > max_ts:
                max_ts = ts
    if max_ts is None:
        return None
    return float((now_ts - max_ts).total_seconds())


def compute_duplicate_rate(
    records: Iterable[Mapping[str, Any]],
    *,
    key_fields: Iterable[str],
) -> float:
    key_list = [field for field in key_fields if field]
    if not key_list:
        return 0.0
    seen: set[tuple[Any, ...]] = set()
    total = 0
    duplicates = 0
    for record in records:
        key = tuple(record.get(field) for field in key_list)
        if any(_is_missing(value) for value in key):
            continue
        total += 1
        if key in seen:
            duplicates += 1
        else:
            seen.add(key)
    return _calculate_rate(duplicates, total)


def compute_bad_records_rate(
    records: Iterable[Mapping[str, Any]],
    *,
    contract: TableContract,
    allowed_entry_types: set[str] | None = None,
    allowed_statuses: set[str] | None = None,
) -> float:
    total = 0
    bad = 0
    for record in records:
        total += 1
        if not _record_meets_contract(
            record,
            contract,
            allowed_entry_types=allowed_entry_types,
            allowed_statuses=allowed_statuses,
        ):
            bad += 1
    return _calculate_rate(bad, total)


def _record_meets_contract(
    record: Mapping[str, Any],
    contract: TableContract,
    *,
    allowed_entry_types: set[str] | None = None,
    allowed_statuses: set[str] | None = None,
) -> bool:
    for column in contract.required_columns:
        if _is_missing(record.get(column)):
            return False

    if contract.name == "bronze.user_wallets_raw":
        balance = _parse_decimal(record.get("balance"))
        frozen = _parse_decimal(record.get("frozen_amount"))
        if balance is None or frozen is None:
            return False
        if balance < 0 or frozen < 0:
            return False

    if contract.name == "bronze.transaction_ledger_raw":
        amount = _parse_decimal(record.get("amount"))
        if amount is None or amount <= 0:
            return False
        entry_type = record.get("type")
        if allowed_entry_types is not None and entry_type not in allowed_entry_types:
            return False

    if contract.name == "bronze.payment_orders_raw":
        amount = _parse_decimal(record.get("amount"))
        if amount is None or amount <= 0:
            return False
        status = record.get("status")
        if allowed_statuses is not None and status is not None:
            if status not in allowed_statuses:
                return False

    return True


def _severity_rank(severity: str | None) -> int:
    if severity == SEVERITY_CRITICAL:
        return 2
    if severity == SEVERITY_WARN:
        return 1
    return 0


def _evaluate_severity(
    value: float | int | None,
    rule: RuleDefinition | None,
) -> str | None:
    if rule is None or value is None:
        return None
    severity_map = rule.severity_map or {}
    if "crit" in severity_map and value >= severity_map["crit"]:
        return SEVERITY_CRITICAL
    if "warn" in severity_map and value >= severity_map["warn"]:
        return SEVERITY_WARN
    if rule.threshold is not None and value >= rule.threshold:
        return SEVERITY_CRITICAL
    return None


def _pick_overall_status(
    assessments: list[tuple[str, str | None]],
) -> tuple[str | None, str | None]:
    priority = {
        TAG_SOURCE_STALE: 0,
        TAG_DUP: 1,
        TAG_EVENT_DROP: 2,
        TAG_CONTRACT: 3,
    }
    best_tag = None
    best_severity = None
    best_rank = -1
    best_priority = 999
    for tag, severity in assessments:
        if severity is None:
            continue
        rank = _severity_rank(severity)
        tag_priority = priority.get(tag, 999)
        if rank > best_rank or (rank == best_rank and tag_priority < best_priority):
            best_rank = rank
            best_priority = tag_priority
            best_tag = tag
            best_severity = severity
    return best_tag, best_severity


def _build_exception(
    *,
    dq_tag: str,
    severity: str,
    metric: str,
    metric_value: float | int | None,
    config: DQTableConfig,
    run_id: str,
    rule_id: str | None,
    generated_at: datetime,
) -> dict[str, Any]:
    return {
        "date_kst": date_kst(config.window_end_ts),
        "domain": "dq",
        "exception_type": dq_tag,
        "severity": severity,
        "source_table": config.source_table,
        "window_start_ts": to_utc(config.window_start_ts),
        "window_end_ts": to_utc(config.window_end_ts),
        "metric": metric,
        "metric_value": Decimal(str(metric_value))
        if metric_value is not None
        else None,
        "message": json.dumps({"metric": metric, "value": metric_value}),
        "run_id": run_id,
        "rule_id": rule_id,
        "generated_at": generated_at,
    }


def build_dq_status(
    records: Iterable[Mapping[str, Any]],
    *,
    config: DQTableConfig,
    run_id: str,
    rules: Iterable[RuleDefinition],
    previous_zero_windows: int = 0,
    now_ts: datetime | None = None,
) -> DQStatusOutput:
    now_ts = to_utc(now_ts) if now_ts else now_utc()
    contract = get_contract(config.source_table)
    records_list = list(records)

    entry_type_rule = select_rule(rules, domain="silver", metric="entry_type_allowed")
    status_rule = select_rule(rules, domain="silver", metric="payment_status_allowed")
    allowed_entry_types = (
        set(entry_type_rule.allowed_values)
        if entry_type_rule and entry_type_rule.allowed_values
        else None
    )
    allowed_statuses = (
        set(status_rule.allowed_values)
        if status_rule and status_rule.allowed_values
        else None
    )

    freshness_rule = select_rule(rules, domain="dq", metric="freshness_sec")
    dup_rule = (
        select_rule(rules, domain="dq", metric=config.dup_rule_metric)
        if config.dup_rule_metric
        else None
    )
    completeness_rule = select_rule(
        rules, domain="dq", metric="completeness_zero_windows"
    )
    contract_rule = select_rule(rules, domain="dq", metric="contract_bad_records_rate")

    event_count = len(records_list)
    zero_windows = previous_zero_windows + 1 if event_count == 0 else 0

    freshness_sec = compute_freshness_sec(
        records_list, now_ts=now_ts, timestamp_fields=config.freshness_fields
    )
    dup_rate = (
        compute_duplicate_rate(records_list, key_fields=config.dup_key_fields)
        if config.dup_key_fields
        else None
    )
    bad_records_rate = compute_bad_records_rate(
        records_list,
        contract=contract,
        allowed_entry_types=allowed_entry_types,
        allowed_statuses=allowed_statuses,
    )

    freshness_severity = _evaluate_severity(freshness_sec, freshness_rule)
    dup_severity = _evaluate_severity(dup_rate, dup_rule)
    completeness_severity = _evaluate_severity(zero_windows, completeness_rule)
    contract_severity = _evaluate_severity(bad_records_rate, contract_rule)

    assessments = [
        (TAG_SOURCE_STALE, freshness_severity),
        (TAG_DUP, dup_severity),
        (TAG_EVENT_DROP, completeness_severity),
        (TAG_CONTRACT, contract_severity),
    ]

    dq_tag, severity = _pick_overall_status(assessments)
    generated_at = now_utc()

    dq_status = {
        "source_table": config.source_table,
        "window_start_ts": to_utc(config.window_start_ts),
        "window_end_ts": to_utc(config.window_end_ts),
        "date_kst": date_kst(config.window_end_ts),
        "freshness_sec": int(freshness_sec) if freshness_sec is not None else None,
        "event_count": event_count,
        "dup_rate": Decimal(str(dup_rate)) if dup_rate is not None else None,
        "bad_records_rate": Decimal(str(bad_records_rate))
        if bad_records_rate is not None
        else None,
        "dq_tag": dq_tag,
        "severity": severity,
        "run_id": run_id,
        "rule_id": dq_tag
        and _resolve_rule_id(
            dq_tag,
            freshness_rule=freshness_rule,
            dup_rule=dup_rule,
            completeness_rule=completeness_rule,
            contract_rule=contract_rule,
        ),
        "generated_at": generated_at,
    }

    exceptions: list[dict[str, Any]] = []
    if freshness_severity:
        exceptions.append(
            _build_exception(
                dq_tag=TAG_SOURCE_STALE,
                severity=freshness_severity,
                metric="freshness_sec",
                metric_value=freshness_sec,
                config=config,
                run_id=run_id,
                rule_id=freshness_rule.rule_id if freshness_rule else None,
                generated_at=generated_at,
            )
        )
    if dup_severity:
        exceptions.append(
            _build_exception(
                dq_tag=TAG_DUP,
                severity=dup_severity,
                metric=config.dup_rule_metric or "dup_rate",
                metric_value=dup_rate,
                config=config,
                run_id=run_id,
                rule_id=dup_rule.rule_id if dup_rule else None,
                generated_at=generated_at,
            )
        )
    if completeness_severity:
        exceptions.append(
            _build_exception(
                dq_tag=TAG_EVENT_DROP,
                severity=completeness_severity,
                metric="completeness_zero_windows",
                metric_value=zero_windows,
                config=config,
                run_id=run_id,
                rule_id=completeness_rule.rule_id if completeness_rule else None,
                generated_at=generated_at,
            )
        )
    if contract_severity:
        exceptions.append(
            _build_exception(
                dq_tag=TAG_CONTRACT,
                severity=contract_severity,
                metric="contract_bad_records_rate",
                metric_value=bad_records_rate,
                config=config,
                run_id=run_id,
                rule_id=contract_rule.rule_id if contract_rule else None,
                generated_at=generated_at,
            )
        )

    return DQStatusOutput(
        dq_status=dq_status,
        exceptions=exceptions,
        zero_window_count=zero_windows,
    )


def _resolve_rule_id(
    dq_tag: str,
    *,
    freshness_rule: RuleDefinition | None,
    dup_rule: RuleDefinition | None,
    completeness_rule: RuleDefinition | None,
    contract_rule: RuleDefinition | None,
) -> str | None:
    if dq_tag == TAG_SOURCE_STALE and freshness_rule:
        return freshness_rule.rule_id
    if dq_tag == TAG_DUP and dup_rule:
        return dup_rule.rule_id
    if dq_tag == TAG_EVENT_DROP and completeness_rule:
        return completeness_rule.rule_id
    if dq_tag == TAG_CONTRACT and contract_rule:
        return contract_rule.rule_id
    return None

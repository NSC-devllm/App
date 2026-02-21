from __future__ import annotations

import json
from datetime import datetime
from decimal import Decimal
from typing import Any, Iterable

from pyspark import StorageLevel
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.common.contracts import get_contract
from src.common.rules import RuleDefinition, select_rule
from src.common.time_utils import date_kst, now_utc, to_utc
from src.io.spark_safety import safe_collect
from src.transforms.dq_guardrail import (
    SEVERITY_CRITICAL,
    SEVERITY_WARN,
    TAG_CONTRACT,
    TAG_DUP,
    TAG_EVENT_DROP,
    TAG_SOURCE_STALE,
    DQStatusOutput,
    DQTableConfig,
)

_METRIC_COLLECT_MAX_ROWS = 1


def _ensure_columns(df: DataFrame, columns: Iterable[str]) -> DataFrame:
    aligned = df
    for column in columns:
        if column not in aligned.columns:
            aligned = aligned.withColumn(column, F.lit(None))
    return aligned


def _non_missing_expr(column) -> F.Column:
    return column.isNotNull() & (F.trim(column.cast("string")) != F.lit(""))


def _and_all(expressions: list[F.Column]) -> F.Column:
    if not expressions:
        return F.lit(True)
    combined = expressions[0]
    for expression in expressions[1:]:
        combined = combined & expression
    return combined


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


def _compute_freshness_sec(
    df: DataFrame,
    *,
    timestamp_fields: tuple[str, ...],
    now_ts: datetime,
    context: str,
) -> float | None:
    present_fields = [field for field in timestamp_fields if field in df.columns]
    if not present_fields:
        return None

    metric_df = df.agg(
        *[
            F.max(F.col(field).cast("timestamp")).alias(field)
            for field in present_fields
        ]
    )
    metric_rows = safe_collect(
        metric_df,
        max_rows=_METRIC_COLLECT_MAX_ROWS,
        context=context,
    )
    if not metric_rows:
        return None

    payload = metric_rows[0].asDict(recursive=True)
    max_ts: datetime | None = None
    for field in present_fields:
        value = payload.get(field)
        if value is None:
            continue
        timestamp_utc = to_utc(value)
        if max_ts is None or timestamp_utc > max_ts:
            max_ts = timestamp_utc

    if max_ts is None:
        return None
    return float((now_ts - max_ts).total_seconds())


def _compute_duplicate_rate(
    df: DataFrame,
    *,
    key_fields: tuple[str, ...] | None,
) -> float | None:
    if not key_fields:
        return None

    keys = [field for field in key_fields if field]
    if not keys:
        return 0.0

    valid_expr = _and_all([_non_missing_expr(F.col(field)) for field in keys])
    valid_keys_df = df.filter(valid_expr).select(*[F.col(field) for field in keys])
    metrics_df = valid_keys_df.agg(
        F.count(F.lit(1)).alias("total_count"),
        F.countDistinct(F.struct(*[F.col(field) for field in keys])).alias(
            "distinct_count"
        ),
    )
    metrics_rows = safe_collect(
        metrics_df,
        max_rows=_METRIC_COLLECT_MAX_ROWS,
        context=f"pipeline_a_spark:dup:{','.join(keys)}",
    )
    if not metrics_rows:
        return 0.0

    payload = metrics_rows[0].asDict(recursive=True)
    total_count = int(payload.get("total_count") or 0)
    if total_count == 0:
        return 0.0

    distinct_count = int(payload.get("distinct_count") or 0)
    duplicate_count = total_count - distinct_count
    return duplicate_count / total_count


def _valid_record_expr(
    df: DataFrame,
    *,
    source_table: str,
    required_columns: set[str],
    allowed_entry_types: set[str] | None,
    allowed_statuses: set[str] | None,
) -> F.Column:
    required_expr = _and_all(
        [_non_missing_expr(F.col(column)) for column in sorted(required_columns)]
    )

    true_expr = F.lit(True)

    if source_table == "bronze.user_wallets_raw":
        zero = F.lit("0").cast("decimal(38,18)")
        balance = F.col("balance").cast("decimal(38,18)")
        frozen = F.col("frozen_amount").cast("decimal(38,18)")
        return (
            required_expr
            & balance.isNotNull()
            & frozen.isNotNull()
            & (balance >= zero)
            & (frozen >= zero)
        )

    if source_table == "bronze.transaction_ledger_raw":
        amount = F.col("amount").cast("decimal(38,18)")
        valid_expr = required_expr & amount.isNotNull() & (amount > F.lit("0"))
        if allowed_entry_types is not None:
            valid_expr = valid_expr & F.col("type").isin(sorted(allowed_entry_types))
        return valid_expr

    if source_table == "bronze.payment_orders_raw":
        amount = F.col("amount").cast("decimal(38,18)")
        valid_expr = required_expr & amount.isNotNull() & (amount > F.lit("0"))
        if allowed_statuses is not None:
            valid_expr = valid_expr & (
                F.col("status").isNull()
                | F.col("status").isin(sorted(allowed_statuses))
            )
        return valid_expr

    return required_expr & true_expr


def build_dq_status_spark(
    records_df: DataFrame,
    *,
    config: DQTableConfig,
    run_id: str,
    rules: Iterable[RuleDefinition],
    previous_zero_windows: int = 0,
    now_ts: datetime | None = None,
) -> DQStatusOutput:
    now_ts = to_utc(now_ts) if now_ts else now_utc()

    contract = get_contract(config.source_table)

    required_columns = set(contract.required_columns)
    needed_columns = set(required_columns)
    needed_columns.update(config.freshness_fields)
    if config.dup_key_fields:
        needed_columns.update(config.dup_key_fields)

    if config.source_table == "bronze.user_wallets_raw":
        needed_columns.update(("balance", "frozen_amount"))
    elif config.source_table == "bronze.transaction_ledger_raw":
        needed_columns.update(("type", "amount"))
    elif config.source_table == "bronze.payment_orders_raw":
        needed_columns.update(("amount", "status"))

    prepared_df = _ensure_columns(records_df, needed_columns).persist(
        StorageLevel.MEMORY_AND_DISK
    )
    try:
        entry_type_rule = select_rule(
            rules, domain="silver", metric="entry_type_allowed"
        )
        status_rule = select_rule(
            rules, domain="silver", metric="payment_status_allowed"
        )

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
        contract_rule = select_rule(
            rules, domain="dq", metric="contract_bad_records_rate"
        )

        freshness_sec = _compute_freshness_sec(
            prepared_df,
            timestamp_fields=config.freshness_fields,
            now_ts=now_ts,
            context=f"pipeline_a_spark:freshness:{config.source_table}",
        )
        dup_rate = _compute_duplicate_rate(
            prepared_df, key_fields=config.dup_key_fields
        )

        valid_expr = _valid_record_expr(
            prepared_df,
            source_table=config.source_table,
            required_columns=required_columns,
            allowed_entry_types=allowed_entry_types,
            allowed_statuses=allowed_statuses,
        )
        count_metrics_df = prepared_df.agg(
            F.count(F.lit(1)).alias("event_count"),
            F.sum(F.when(valid_expr, F.lit(1)).otherwise(F.lit(0))).alias(
                "valid_count"
            ),
        )
        count_metrics_rows = safe_collect(
            count_metrics_df,
            max_rows=_METRIC_COLLECT_MAX_ROWS,
            context=f"pipeline_a_spark:counts:{config.source_table}",
        )
        count_payload = (
            count_metrics_rows[0].asDict(recursive=True) if count_metrics_rows else {}
        )
        event_count = int(count_payload.get("event_count") or 0)
        valid_count = int(count_payload.get("valid_count") or 0)
        invalid_count = max(event_count - valid_count, 0)
        zero_windows = previous_zero_windows + 1 if event_count == 0 else 0
        bad_records_rate = (invalid_count / event_count) if event_count else 0.0
    finally:
        prepared_df.unpersist()

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
        "bad_records_rate": Decimal(str(bad_records_rate)),
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

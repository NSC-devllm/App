from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Iterable

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from src.common.rules import RuleDefinition, select_rule
from src.transforms.ledger_controls import (
    DEFAULT_FAILED_STATUSES,
    DEFAULT_REFUNDED_STATUSES,
    EXCEPTION_RECON,
    EXCEPTION_SUPPLY,
    SEVERITY_CRITICAL,
    SEVERITY_WARN,
    SUPPLY_ENTRY_SIGNS,
    TAG_EVENT_DROP,
    TAG_SOURCE_STALE,
)

KST_TIMEZONE = "Asia/Seoul"


@F.udf(returnType=StringType())
def _serialize_recon_message(
    user_id,
    delta_balance_total,
    net_flow_total,
    drift_abs,
):
    payload = {
        "user_id": str(user_id),
        "delta_balance_total": str(delta_balance_total),
        "net_flow_total": str(net_flow_total),
        "drift_abs": str(drift_abs),
    }
    return json.dumps(payload, default=str, ensure_ascii=True)


@F.udf(returnType=StringType())
def _serialize_supply_message(
    issued_supply,
    wallet_total_balance,
    diff_amount,
):
    payload = {
        "issued_supply": str(issued_supply),
        "wallet_total_balance": str(wallet_total_balance),
        "diff_amount": str(diff_amount),
    }
    return json.dumps(payload, default=str, ensure_ascii=True)


@dataclass(frozen=True)
class PipelineBSparkOutputs:
    recon_df: DataFrame | None
    supply_df: DataFrame | None
    ops_failure_df: DataFrame | None
    ops_refund_df: DataFrame | None
    pairing_quality_df: DataFrame | None
    admin_tx_search_df: DataFrame | None
    exception_df: DataFrame | None


def _date_kst_expr(column) -> F.Column:
    return F.to_date(F.from_utc_timestamp(column, KST_TIMEZONE))


def _non_blank_expr(column) -> F.Column:
    return column.isNotNull() & (F.trim(column.cast("string")) != F.lit(""))


def _ensure_columns(df: DataFrame, columns: tuple[str, ...]) -> DataFrame:
    aligned = df
    for column in columns:
        if column not in aligned.columns:
            aligned = aligned.withColumn(column, F.lit(None))
    return aligned


def _decimal_literal(value: Decimal | int | float, *, scale: int = 6) -> F.Column:
    return F.lit(str(value)).cast(f"decimal(38,{scale})")


def _to_decimal(value: float | int | Decimal | None) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def _resolve_threshold(rule: RuleDefinition | None) -> Decimal | None:
    if rule is None:
        return None
    if rule.threshold is not None:
        return _to_decimal(rule.threshold)
    if rule.severity_map and "crit" in rule.severity_map:
        return _to_decimal(rule.severity_map["crit"])
    return None


def _build_severity_expr(
    metric_column,
    *,
    rule: RuleDefinition | None,
) -> F.Column:
    if rule is None:
        return F.lit(None).cast("string")

    checks: list[tuple[F.Column, str]] = []
    if rule.severity_map and "crit" in rule.severity_map:
        crit = _to_decimal(rule.severity_map["crit"])
        if crit is not None:
            checks.append(
                (
                    metric_column > _decimal_literal(crit, scale=6),
                    SEVERITY_CRITICAL,
                )
            )
    if rule.severity_map and "warn" in rule.severity_map:
        warn = _to_decimal(rule.severity_map["warn"])
        if warn is not None:
            checks.append(
                (
                    metric_column > _decimal_literal(warn, scale=6),
                    SEVERITY_WARN,
                )
            )
    if rule.threshold is not None:
        threshold = _to_decimal(rule.threshold)
        if threshold is not None:
            checks.append(
                (
                    metric_column > _decimal_literal(threshold, scale=6),
                    SEVERITY_CRITICAL,
                )
            )

    if not checks:
        return F.lit(None).cast("string")

    expr = None
    for condition, label in checks:
        if expr is None:
            expr = F.when(condition, F.lit(label))
        else:
            expr = expr.when(condition, F.lit(label))
    return expr.otherwise(F.lit(None).cast("string"))


def _build_target_dates_df(spark, target_dates: Iterable[date]) -> DataFrame:
    unique_dates = sorted({target_date for target_date in target_dates if target_date})
    if not unique_dates:
        raise ValueError("target_dates must not be empty")
    return spark.createDataFrame(
        [{"date_kst": target_date} for target_date in unique_dates],
        schema="date_kst date",
    )


def _build_dq_tags_by_date_df(
    *,
    target_dates_df: DataFrame,
    dq_status_df: DataFrame | None,
) -> DataFrame:
    if dq_status_df is None:
        return target_dates_df.select(
            "date_kst",
            F.lit(None).cast("string").alias("dq_tag"),
        )

    tagged = (
        dq_status_df.select(
            F.col("date_kst").cast("date").alias("date_kst"),
            F.col("dq_tag").cast("string").alias("dq_tag"),
        )
        .filter(F.col("date_kst").isNotNull() & _non_blank_expr(F.col("dq_tag")))
        .join(target_dates_df, on="date_kst", how="inner")
    )

    resolved = tagged.groupBy("date_kst").agg(
        F.max(
            F.when(F.col("dq_tag") == F.lit(TAG_SOURCE_STALE), F.lit(1)).otherwise(
                F.lit(0)
            )
        ).alias("has_source_stale"),
        F.max(
            F.when(F.col("dq_tag") == F.lit(TAG_EVENT_DROP), F.lit(1)).otherwise(
                F.lit(0)
            )
        ).alias("has_event_drop"),
        F.min("dq_tag").alias("fallback_tag"),
    )

    return target_dates_df.join(
        resolved.select(
            "date_kst",
            F.when(F.col("has_source_stale") == F.lit(1), F.lit(TAG_SOURCE_STALE))
            .when(F.col("has_event_drop") == F.lit(1), F.lit(TAG_EVENT_DROP))
            .otherwise(F.col("fallback_tag"))
            .alias("dq_tag"),
        ),
        on="date_kst",
        how="left",
    )


def _build_wallet_bounds_df(
    *,
    wallet_snapshot_df: DataFrame,
    target_dates_df: DataFrame,
) -> DataFrame:
    prepared = (
        wallet_snapshot_df.select(
            F.col("snapshot_ts").cast("timestamp").alias("snapshot_ts"),
            F.col("user_id").cast("string").alias("user_id"),
            F.col("balance_total").cast("decimal(38,2)").alias("balance_total"),
        )
        .filter(
            F.col("snapshot_ts").isNotNull()
            & _non_blank_expr(F.col("user_id"))
            & F.col("balance_total").isNotNull()
        )
        .withColumn("date_kst", _date_kst_expr(F.col("snapshot_ts")))
        .join(target_dates_df, on="date_kst", how="inner")
    )

    min_window = Window.partitionBy("date_kst", "user_id").orderBy(
        F.col("snapshot_ts").asc()
    )
    max_window = Window.partitionBy("date_kst", "user_id").orderBy(
        F.col("snapshot_ts").desc()
    )

    start_rows = (
        prepared.withColumn("rn", F.row_number().over(min_window))
        .filter(F.col("rn") == F.lit(1))
        .select(
            "date_kst",
            "user_id",
            F.col("balance_total").alias("start_balance"),
        )
    )
    end_rows = (
        prepared.withColumn("rn", F.row_number().over(max_window))
        .filter(F.col("rn") == F.lit(1))
        .select(
            "date_kst",
            "user_id",
            F.col("balance_total").alias("end_balance"),
        )
    )
    return start_rows.join(end_rows, on=["date_kst", "user_id"], how="inner")


def _build_ledger_base_df(ledger_entries_df: DataFrame) -> DataFrame:
    ledger_entries_df = _ensure_columns(
        ledger_entries_df,
        (
            "tx_id",
            "wallet_id",
            "entry_type",
            "type",
            "amount",
            "amount_signed",
            "related_id",
            "related_type",
            "merchant_name",
            "status",
            "event_time",
            "event_date_kst",
        ),
    )
    event_time = F.col("event_time").cast("timestamp")
    event_date_kst = F.col("event_date_kst").cast("date")
    return (
        ledger_entries_df.select(
            F.col("tx_id").cast("string").alias("tx_id"),
            F.col("wallet_id").cast("string").alias("wallet_id"),
            F.coalesce(
                F.col("entry_type").cast("string"),
                F.col("type").cast("string"),
            ).alias("entry_type"),
            F.col("amount").cast("decimal(38,2)").alias("amount"),
            F.col("amount_signed").cast("decimal(38,2)").alias("amount_signed"),
            F.when(
                F.col("related_id").isNotNull(),
                F.col("related_id").cast("string"),
            ).alias("related_id"),
            F.col("related_type").cast("string").alias("related_type"),
            F.col("merchant_name").cast("string").alias("merchant_name"),
            F.col("status").cast("string").alias("status"),
            event_time.alias("event_time"),
            event_date_kst.alias("event_date_kst"),
        )
        .withColumn(
            "date_kst",
            F.when(F.col("event_date_kst").isNotNull(), F.col("event_date_kst"))
            .otherwise(_date_kst_expr(F.col("event_time")))
            .cast("date"),
        )
        .filter(F.col("date_kst").isNotNull())
    )


def _build_payment_orders_base_df(payment_orders_df: DataFrame) -> DataFrame:
    payment_orders_df = _ensure_columns(
        payment_orders_df,
        ("order_id", "merchant_name", "status", "created_at", "event_time"),
    )
    created_ts = F.coalesce(
        F.col("created_at").cast("timestamp"),
        F.col("event_time").cast("timestamp"),
    )
    return payment_orders_df.select(
        F.col("order_id").cast("string").alias("order_id"),
        F.col("merchant_name").cast("string").alias("merchant_name"),
        F.col("status").cast("string").alias("status"),
        created_ts.alias("created_at"),
    ).withColumn("date_kst", _date_kst_expr(F.col("created_at")).cast("date"))


def _build_recon_outputs(
    *,
    target_dates_df: DataFrame,
    wallet_bounds_df: DataFrame,
    ledger_base_df: DataFrame,
    dq_tags_by_date_df: DataFrame,
    run_id: str,
    recon_rule: RuleDefinition | None,
) -> tuple[DataFrame, DataFrame]:
    net_flow_df = (
        ledger_base_df.join(target_dates_df, on="date_kst", how="inner")
        .filter(
            _non_blank_expr(F.col("wallet_id")) & F.col("amount_signed").isNotNull()
        )
        .groupBy("date_kst", "wallet_id")
        .agg(F.sum("amount_signed").cast("decimal(38,2)").alias("net_flow_total"))
    )

    recon_df = (
        wallet_bounds_df.join(
            net_flow_df,
            (wallet_bounds_df["date_kst"] == net_flow_df["date_kst"])
            & (wallet_bounds_df["user_id"] == net_flow_df["wallet_id"]),
            how="left",
        )
        .drop(net_flow_df["date_kst"])
        .drop("wallet_id")
        .withColumn(
            "net_flow_total",
            F.coalesce(
                F.col("net_flow_total"),
                _decimal_literal(Decimal("0"), scale=2),
            ).cast("decimal(38,2)"),
        )
        .withColumn(
            "delta_balance_total",
            (F.col("end_balance") - F.col("start_balance")).cast("decimal(38,2)"),
        )
        .withColumn(
            "drift_abs",
            F.abs(F.col("delta_balance_total") - F.col("net_flow_total")).cast(
                "decimal(38,2)"
            ),
        )
        .withColumn(
            "drift_pct",
            F.when(
                F.col("net_flow_total") != _decimal_literal(Decimal("0"), scale=2),
                (
                    F.col("drift_abs").cast("decimal(38,6)")
                    / F.abs(F.col("net_flow_total")).cast("decimal(38,6)")
                ).cast("decimal(38,6)"),
            ),
        )
        .join(dq_tags_by_date_df, on="date_kst", how="left")
        .select(
            "date_kst",
            F.col("user_id").cast("string").alias("user_id"),
            F.col("delta_balance_total")
            .cast("decimal(38,2)")
            .alias("delta_balance_total"),
            F.col("net_flow_total").cast("decimal(38,2)").alias("net_flow_total"),
            F.col("drift_abs").cast("decimal(38,2)").alias("drift_abs"),
            F.col("drift_pct").cast("decimal(38,6)").alias("drift_pct"),
            F.col("dq_tag").cast("string").alias("dq_tag"),
            F.lit(run_id).alias("run_id"),
            F.lit(recon_rule.rule_id if recon_rule else None)
            .cast("string")
            .alias("rule_id"),
        )
    )

    severity_expr = _build_severity_expr(
        F.col("drift_abs").cast("decimal(38,6)"),
        rule=recon_rule,
    )
    recon_exceptions_df = (
        recon_df.withColumn("severity", severity_expr)
        .filter(F.col("severity").isNotNull())
        .withColumn(
            "message",
            _serialize_recon_message(
                F.col("user_id"),
                F.col("delta_balance_total"),
                F.col("net_flow_total"),
                F.col("drift_abs"),
            ),
        )
        .select(
            F.col("date_kst").cast("date").alias("date_kst"),
            F.lit("ledger").alias("domain"),
            F.lit(EXCEPTION_RECON).alias("exception_type"),
            F.col("severity").cast("string").alias("severity"),
            F.lit(None).cast("string").alias("source_table"),
            F.lit(None).cast("timestamp").alias("window_start_ts"),
            F.lit(None).cast("timestamp").alias("window_end_ts"),
            F.lit("drift_abs").alias("metric"),
            F.col("drift_abs").cast("decimal(38,6)").alias("metric_value"),
            F.col("message").cast("string").alias("message"),
            F.lit(run_id).alias("run_id"),
            F.lit(recon_rule.rule_id if recon_rule else None)
            .cast("string")
            .alias("rule_id"),
            F.current_timestamp().alias("generated_at"),
        )
    )
    return recon_df, recon_exceptions_df


def _build_supply_outputs(
    *,
    target_dates_df: DataFrame,
    wallet_bounds_df: DataFrame,
    ledger_base_df: DataFrame,
    run_id: str,
    supply_rule: RuleDefinition | None,
) -> tuple[DataFrame, DataFrame]:
    wallet_total_df = wallet_bounds_df.groupBy("date_kst").agg(
        F.sum("end_balance").cast("decimal(38,2)").alias("wallet_total_balance")
    )

    sign_map = F.create_map(
        *[
            item
            for key, value in SUPPLY_ENTRY_SIGNS.items()
            for item in (F.lit(key), F.lit(value))
        ]
    )
    supply_daily_df = (
        ledger_base_df.withColumn("entry_sign", sign_map[F.col("entry_type")])
        .filter(F.col("entry_sign").isNotNull() & F.col("amount").isNotNull())
        .withColumn(
            "signed_amount",
            (F.col("amount") * F.col("entry_sign").cast("decimal(38,2)")).cast(
                "decimal(38,2)"
            ),
        )
        .groupBy("date_kst")
        .agg(F.sum("signed_amount").cast("decimal(38,2)").alias("daily_signed_amount"))
    )

    issued_supply_df = (
        target_dates_df.alias("target")
        .join(
            supply_daily_df.alias("daily"),
            F.col("daily.date_kst") <= F.col("target.date_kst"),
            how="left",
        )
        .groupBy(F.col("target.date_kst").alias("date_kst"))
        .agg(
            F.sum("daily.daily_signed_amount")
            .cast("decimal(38,2)")
            .alias("issued_supply")
        )
    )

    threshold = _resolve_threshold(supply_rule)
    threshold_expr = (
        _decimal_literal(threshold, scale=6) if threshold is not None else None
    )

    supply_df = (
        target_dates_df.join(issued_supply_df, on="date_kst", how="left")
        .join(wallet_total_df, on="date_kst", how="left")
        .withColumn(
            "issued_supply",
            F.coalesce(
                F.col("issued_supply"),
                _decimal_literal(Decimal("0"), scale=2),
            ).cast("decimal(38,2)"),
        )
        .withColumn(
            "wallet_total_balance",
            F.coalesce(
                F.col("wallet_total_balance"),
                _decimal_literal(Decimal("0"), scale=2),
            ).cast("decimal(38,2)"),
        )
        .withColumn(
            "diff_amount",
            (F.col("issued_supply") - F.col("wallet_total_balance")).cast(
                "decimal(38,2)"
            ),
        )
        .withColumn("diff_abs", F.abs(F.col("diff_amount")).cast("decimal(38,6)"))
        .withColumn(
            "is_ok",
            (
                F.col("diff_abs") <= threshold_expr
                if threshold_expr is not None
                else F.col("diff_abs") == _decimal_literal(Decimal("0"), scale=6)
            ),
        )
        .select(
            F.col("date_kst").cast("date").alias("date_kst"),
            F.col("issued_supply").cast("decimal(38,2)").alias("issued_supply"),
            F.col("wallet_total_balance")
            .cast("decimal(38,2)")
            .alias("wallet_total_balance"),
            F.col("diff_amount").cast("decimal(38,2)").alias("diff_amount"),
            F.col("is_ok").cast("boolean").alias("is_ok"),
            F.lit(run_id).alias("run_id"),
            F.lit(supply_rule.rule_id if supply_rule else None)
            .cast("string")
            .alias("rule_id"),
            F.col("diff_abs").cast("decimal(38,6)").alias("_diff_abs"),
        )
    )

    severity_expr = _build_severity_expr(F.col("_diff_abs"), rule=supply_rule)
    supply_exceptions_df = (
        supply_df.withColumn("severity", severity_expr)
        .filter(F.col("severity").isNotNull())
        .withColumn(
            "message",
            _serialize_supply_message(
                F.col("issued_supply"),
                F.col("wallet_total_balance"),
                F.col("diff_amount"),
            ),
        )
        .select(
            F.col("date_kst").cast("date").alias("date_kst"),
            F.lit("ledger").alias("domain"),
            F.lit(EXCEPTION_SUPPLY).alias("exception_type"),
            F.col("severity").cast("string").alias("severity"),
            F.lit(None).cast("string").alias("source_table"),
            F.lit(None).cast("timestamp").alias("window_start_ts"),
            F.lit(None).cast("timestamp").alias("window_end_ts"),
            F.lit("supply_diff_abs").alias("metric"),
            F.col("_diff_abs").cast("decimal(38,6)").alias("metric_value"),
            F.col("message").cast("string").alias("message"),
            F.lit(run_id).alias("run_id"),
            F.lit(supply_rule.rule_id if supply_rule else None)
            .cast("string")
            .alias("rule_id"),
            F.current_timestamp().alias("generated_at"),
        )
    )

    return supply_df.drop("_diff_abs"), supply_exceptions_df


def _build_ops_status_daily_df(
    *,
    payment_orders_base_df: DataFrame,
    target_dates_df: DataFrame,
    statuses: set[str],
    count_column: str,
    rate_column: str,
    run_id: str,
) -> DataFrame:
    base = payment_orders_base_df.join(target_dates_df, on="date_kst", how="inner")
    status_set = sorted({str(status) for status in statuses})
    grouped = base.groupBy("date_kst", "merchant_name").agg(
        F.count(F.lit(1)).cast("bigint").alias("total_cnt"),
        F.sum(F.when(F.col("status").isin(status_set), F.lit(1)).otherwise(F.lit(0)))
        .cast("bigint")
        .alias(count_column),
    )
    return grouped.select(
        F.col("date_kst").cast("date").alias("date_kst"),
        F.col("merchant_name").cast("string").alias("merchant_name"),
        F.col("total_cnt").cast("bigint").alias("total_cnt"),
        F.col(count_column).cast("bigint").alias(count_column),
        F.when(
            F.col("total_cnt") > F.lit(0),
            (
                F.col(count_column).cast("decimal(38,6)")
                / F.col("total_cnt").cast("decimal(38,6)")
            ).cast("decimal(38,6)"),
        )
        .otherwise(_decimal_literal(Decimal("0"), scale=6))
        .alias(rate_column),
        F.lit(run_id).alias("run_id"),
        F.lit(None).cast("string").alias("rule_id"),
    )


def _build_pairing_quality_df(
    *,
    ledger_base_df: DataFrame,
    target_dates_df: DataFrame,
    payment_orders_base_df: DataFrame | None,
    run_id: str,
) -> DataFrame:
    entries = ledger_base_df.join(target_dates_df, on="date_kst", how="inner")
    entry_counts = entries.groupBy("date_kst").agg(
        F.count(F.lit(1)).cast("bigint").alias("entry_cnt")
    )
    null_related_counts = entries.groupBy("date_kst").agg(
        F.sum(
            F.when(
                F.col("related_id").isNull() | (F.col("related_id") == F.lit("")),
                F.lit(1),
            ).otherwise(F.lit(0))
        )
        .cast("bigint")
        .alias("null_related_cnt")
    )

    related_groups = (
        entries.filter(F.col("related_id").isNotNull())
        .groupBy("date_kst", "related_id")
        .agg(
            F.count(F.lit(1)).cast("bigint").alias("group_cnt"),
            F.countDistinct(
                F.when(_non_blank_expr(F.col("wallet_id")), F.col("wallet_id"))
            )
            .cast("bigint")
            .alias("wallet_cnt"),
        )
    )
    group_totals = related_groups.groupBy("date_kst").agg(
        F.count(F.lit(1)).cast("bigint").alias("total_group_cnt")
    )
    pair_candidates = (
        related_groups.filter(
            (F.col("group_cnt") == F.lit(2)) & (F.col("wallet_cnt") == F.lit(2))
        )
        .groupBy("date_kst")
        .agg(F.count(F.lit(1)).cast("bigint").alias("pair_candidate_group_cnt"))
    )

    joined_groups = None
    if payment_orders_base_df is not None:
        order_ids = payment_orders_base_df.select(
            F.col("order_id").cast("string").alias("order_id")
        ).filter(F.col("order_id").isNotNull())
        joined_groups = (
            related_groups.join(
                order_ids,
                related_groups["related_id"] == order_ids["order_id"],
                how="inner",
            )
            .groupBy(related_groups["date_kst"])
            .agg(F.count(F.lit(1)).cast("bigint").alias("joined_group_cnt"))
            .withColumnRenamed("date_kst", "joined_date_kst")
        )

    joined_col = (
        F.col("joined_group_cnt")
        if joined_groups is not None
        else F.lit(0).cast("bigint")
    )

    base = (
        target_dates_df.join(entry_counts, on="date_kst", how="left")
        .join(null_related_counts, on="date_kst", how="left")
        .join(group_totals, on="date_kst", how="left")
        .join(pair_candidates, on="date_kst", how="left")
    )
    if joined_groups is not None:
        base = base.join(
            joined_groups,
            base["date_kst"] == joined_groups["joined_date_kst"],
            how="left",
        ).drop("joined_date_kst")

    return base.select(
        F.col("date_kst").cast("date").alias("date_kst"),
        F.coalesce(F.col("entry_cnt"), F.lit(0)).cast("bigint").alias("entry_cnt"),
        F.when(
            F.coalesce(F.col("entry_cnt"), F.lit(0)) > F.lit(0),
            (
                F.coalesce(F.col("null_related_cnt"), F.lit(0)).cast("decimal(38,6)")
                / F.coalesce(F.col("entry_cnt"), F.lit(0)).cast("decimal(38,6)")
            ).cast("decimal(38,6)"),
        )
        .otherwise(_decimal_literal(Decimal("0"), scale=6))
        .alias("related_id_null_rate"),
        F.when(
            F.coalesce(F.col("total_group_cnt"), F.lit(0)) > F.lit(0),
            (
                F.coalesce(F.col("pair_candidate_group_cnt"), F.lit(0)).cast(
                    "decimal(38,6)"
                )
                / F.coalesce(F.col("total_group_cnt"), F.lit(0)).cast("decimal(38,6)")
            ).cast("decimal(38,6)"),
        )
        .otherwise(_decimal_literal(Decimal("0"), scale=6))
        .alias("pair_candidate_rate"),
        F.when(
            F.coalesce(F.col("total_group_cnt"), F.lit(0)) > F.lit(0),
            (
                joined_col.cast("decimal(38,6)")
                / F.coalesce(F.col("total_group_cnt"), F.lit(0)).cast("decimal(38,6)")
            ).cast("decimal(38,6)"),
        )
        .otherwise(_decimal_literal(Decimal("0"), scale=6))
        .alias("join_payment_orders_rate"),
        F.lit(run_id).alias("run_id"),
        F.lit(None).cast("string").alias("rule_id"),
    )


def _build_admin_tx_search_df(
    *,
    ledger_base_df: DataFrame,
    target_dates_df: DataFrame,
    run_id: str,
) -> DataFrame:
    base = (
        ledger_base_df.join(target_dates_df, on="date_kst", how="inner")
        .filter(_non_blank_expr(F.col("tx_id")) & _non_blank_expr(F.col("wallet_id")))
        .select(
            F.col("date_kst").cast("date").alias("event_date_kst"),
            F.col("tx_id").cast("string").alias("tx_id"),
            F.col("wallet_id").cast("string").alias("wallet_id"),
            F.col("entry_type").cast("string").alias("entry_type"),
            F.col("amount").cast("decimal(38,2)").alias("amount"),
            F.col("amount_signed").cast("decimal(38,2)").alias("amount_signed"),
            F.col("event_time").cast("timestamp").alias("event_time"),
            F.col("related_id").cast("string").alias("related_id"),
            F.col("related_type").cast("string").alias("related_type"),
            F.col("merchant_name").cast("string").alias("merchant_name"),
            F.col("status").cast("string").alias("payment_status"),
        )
    )

    pair_groups = (
        base.filter(_non_blank_expr(F.col("related_id")))
        .groupBy("event_date_kst", "related_id")
        .agg(F.collect_set("tx_id").alias("tx_ids"))
        .filter(F.size("tx_ids") == F.lit(2))
        .select(
            "event_date_kst",
            F.col("tx_ids").getItem(0).alias("tx_id_left"),
            F.col("tx_ids").getItem(1).alias("tx_id_right"),
        )
    )
    pair_lookup = pair_groups.select(
        "event_date_kst",
        F.col("tx_id_left").alias("tx_id"),
        F.col("tx_id_right").alias("paired_tx_id"),
    ).unionByName(
        pair_groups.select(
            "event_date_kst",
            F.col("tx_id_right").alias("tx_id"),
            F.col("tx_id_left").alias("paired_tx_id"),
        )
    )

    return base.join(
        pair_lookup,
        on=["event_date_kst", "tx_id"],
        how="left",
    ).select(
        F.col("event_date_kst").cast("date").alias("event_date_kst"),
        F.col("tx_id").cast("string").alias("tx_id"),
        F.col("wallet_id").cast("string").alias("wallet_id"),
        F.col("entry_type").cast("string").alias("entry_type"),
        F.col("amount").cast("decimal(38,2)").alias("amount"),
        F.col("amount_signed").cast("decimal(38,2)").alias("amount_signed"),
        F.col("event_time").cast("timestamp").alias("event_time"),
        F.col("related_id").cast("string").alias("related_id"),
        F.col("related_type").cast("string").alias("related_type"),
        F.col("merchant_name").cast("string").alias("merchant_name"),
        F.col("payment_status").cast("string").alias("payment_status"),
        F.col("paired_tx_id").cast("string").alias("paired_tx_id"),
        F.lit(run_id).alias("run_id"),
        F.lit(None).cast("string").alias("rule_id"),
    )


def transform_pipeline_b_tables_spark(
    *,
    wallet_snapshot_df: DataFrame,
    ledger_entries_df: DataFrame,
    payment_orders_df: DataFrame | None,
    dq_status_df: DataFrame | None,
    target_dates: Iterable[date],
    run_id: str,
    rules: Iterable[RuleDefinition],
    run_recon: bool,
    run_supply_ops: bool,
) -> PipelineBSparkOutputs:
    target_dates = list(target_dates)
    if not target_dates:
        raise ValueError("target_dates must not be empty")
    if run_supply_ops and payment_orders_df is None:
        raise ValueError("payment_orders_df is required when run_supply_ops=True")

    spark = wallet_snapshot_df.sparkSession
    target_dates_df = _build_target_dates_df(spark, target_dates)
    dq_tags_by_date_df = _build_dq_tags_by_date_df(
        target_dates_df=target_dates_df,
        dq_status_df=dq_status_df,
    )
    wallet_bounds_df = _build_wallet_bounds_df(
        wallet_snapshot_df=wallet_snapshot_df,
        target_dates_df=target_dates_df,
    )
    ledger_base_df = _build_ledger_base_df(ledger_entries_df)

    recon_rule = select_rule(rules, domain="ledger", metric="drift_abs")
    supply_rule = select_rule(rules, domain="ledger", metric="supply_diff_abs")

    recon_df = None
    recon_exceptions_df = None
    if run_recon:
        recon_df, recon_exceptions_df = _build_recon_outputs(
            target_dates_df=target_dates_df,
            wallet_bounds_df=wallet_bounds_df,
            ledger_base_df=ledger_base_df,
            dq_tags_by_date_df=dq_tags_by_date_df,
            run_id=run_id,
            recon_rule=recon_rule,
        )

    supply_df = None
    supply_exceptions_df = None
    ops_failure_df = None
    ops_refund_df = None
    pairing_quality_df = None
    admin_tx_search_df = None
    if run_supply_ops:
        payment_orders_base_df = _build_payment_orders_base_df(payment_orders_df)
        supply_df, supply_exceptions_df = _build_supply_outputs(
            target_dates_df=target_dates_df,
            wallet_bounds_df=wallet_bounds_df,
            ledger_base_df=ledger_base_df,
            run_id=run_id,
            supply_rule=supply_rule,
        )
        ops_failure_df = _build_ops_status_daily_df(
            payment_orders_base_df=payment_orders_base_df,
            target_dates_df=target_dates_df,
            statuses=set(DEFAULT_FAILED_STATUSES),
            count_column="failed_cnt",
            rate_column="failure_rate",
            run_id=run_id,
        )
        ops_refund_df = _build_ops_status_daily_df(
            payment_orders_base_df=payment_orders_base_df,
            target_dates_df=target_dates_df,
            statuses=set(DEFAULT_REFUNDED_STATUSES),
            count_column="refunded_cnt",
            rate_column="refund_rate",
            run_id=run_id,
        )
        pairing_quality_df = _build_pairing_quality_df(
            ledger_base_df=ledger_base_df,
            target_dates_df=target_dates_df,
            payment_orders_base_df=payment_orders_base_df,
            run_id=run_id,
        )
        admin_tx_search_df = _build_admin_tx_search_df(
            ledger_base_df=ledger_base_df,
            target_dates_df=target_dates_df,
            run_id=run_id,
        )

    exception_parts = [
        df for df in (recon_exceptions_df, supply_exceptions_df) if df is not None
    ]
    exception_df = None
    if exception_parts:
        exception_df = exception_parts[0]
        for df in exception_parts[1:]:
            exception_df = exception_df.unionByName(df)

    return PipelineBSparkOutputs(
        recon_df=recon_df,
        supply_df=supply_df,
        ops_failure_df=ops_failure_df,
        ops_refund_df=ops_refund_df,
        pairing_quality_df=pairing_quality_df,
        admin_tx_search_df=admin_tx_search_df,
        exception_df=exception_df,
    )

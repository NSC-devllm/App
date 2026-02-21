from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from src.transforms.silver_controls import ENTRY_TYPE_SIGN

KST_TIMEZONE = "Asia/Seoul"


@dataclass(frozen=True)
class SilverSparkOutputs:
    wallet_snapshot_df: DataFrame
    ledger_entries_df: DataFrame
    order_events_df: DataFrame
    order_items_df: DataFrame
    products_df: DataFrame
    bad_records_df: DataFrame
    wallet_valid_count: int
    wallet_bad_count: int
    ledger_valid_count: int
    ledger_bad_count: int


def _date_kst_expr(column) -> F.Column:
    return F.to_date(F.from_utc_timestamp(column, KST_TIMEZONE))


def _is_missing_expr(column) -> F.Column:
    return column.isNull() | (F.trim(column.cast("string")) == F.lit(""))


def _null_if_blank(column) -> F.Column:
    return F.when(_is_missing_expr(column), F.lit(None)).otherwise(column)


def _build_bad_records_df(
    source_df: DataFrame,
    *,
    reason_column: str,
    source_columns: tuple[str, ...],
    source_table: str,
    run_id: str,
    rule_id: str | None,
) -> DataFrame:
    detected_at = F.current_timestamp()
    record_json = F.to_json(F.struct(*[F.col(column) for column in source_columns]))
    return source_df.filter(F.col(reason_column).isNotNull()).select(
        _date_kst_expr(detected_at).alias("detected_date_kst"),
        F.lit(source_table).alias("source_table"),
        F.col(reason_column).alias("reason"),
        record_json.alias("record_json"),
        F.lit(run_id).alias("run_id"),
        F.lit(rule_id).cast("string").alias("rule_id"),
        detected_at.alias("detected_at"),
    )


def _ensure_columns(df: DataFrame, columns: tuple[str, ...]) -> DataFrame:
    aligned = df
    for column in columns:
        if column not in aligned.columns:
            aligned = aligned.withColumn(column, F.lit(None))
    return aligned


def _build_wallet_snapshot_outputs(
    wallet_df: DataFrame,
    *,
    run_id: str,
    rule_id: str | None,
) -> tuple[DataFrame, DataFrame]:
    wallet_df = _ensure_columns(
        wallet_df,
        (
            "user_id",
            "balance",
            "frozen_amount",
            "updated_at",
            "source_extracted_at",
            "ingested_at",
        ),
    )
    source_columns = tuple(wallet_df.columns)
    evaluated = (
        wallet_df.withColumn("user_id_raw", _null_if_blank(F.col("user_id")))
        .withColumn("balance_available_raw", F.col("balance").cast("decimal(38,2)"))
        .withColumn("balance_frozen_raw", F.col("frozen_amount").cast("decimal(38,2)"))
        .withColumn(
            "snapshot_ts_raw",
            F.coalesce(
                F.col("source_extracted_at").cast("timestamp"),
                F.col("ingested_at").cast("timestamp"),
            ),
        )
        .withColumn(
            "bad_reason",
            F.when(F.col("user_id_raw").isNull(), F.lit("missing_user_id"))
            .when(
                F.col("balance_available_raw").isNull()
                | F.col("balance_frozen_raw").isNull(),
                F.lit("invalid_balance"),
            )
            .when(
                (F.col("balance_available_raw") < F.lit(0))
                | (F.col("balance_frozen_raw") < F.lit(0)),
                F.lit("negative_balance"),
            )
            .when(F.col("snapshot_ts_raw").isNull(), F.lit("missing_snapshot_ts")),
        )
    )

    valid_df = evaluated.filter(F.col("bad_reason").isNull()).select(
        F.col("snapshot_ts_raw").alias("snapshot_ts"),
        _date_kst_expr(F.col("snapshot_ts_raw")).alias("snapshot_date_kst"),
        F.col("user_id_raw").cast("string").alias("user_id"),
        F.col("balance_available_raw").alias("balance_available"),
        F.col("balance_frozen_raw").alias("balance_frozen"),
        (F.col("balance_available_raw") + F.col("balance_frozen_raw")).alias(
            "balance_total"
        ),
        F.col("updated_at").cast("timestamp").alias("source_updated_at"),
        F.lit(run_id).alias("run_id"),
        F.lit(rule_id).cast("string").alias("rule_id"),
    )
    bad_df = _build_bad_records_df(
        evaluated,
        reason_column="bad_reason",
        source_columns=source_columns,
        source_table="silver.wallet_snapshot",
        run_id=run_id,
        rule_id=rule_id,
    )
    return valid_df, bad_df


def _build_status_lookup_df(payment_orders_df: DataFrame) -> DataFrame:
    payment_orders_df = _ensure_columns(
        payment_orders_df,
        ("order_id", "status", "created_at", "ingested_at"),
    )
    lookup_base = payment_orders_df.select(
        F.col("order_id").cast("string").alias("lookup_order_id"),
        F.col("status").cast("string").alias("lookup_status"),
        F.col("created_at").cast("timestamp").alias("lookup_created_at"),
        F.col("ingested_at").cast("timestamp").alias("lookup_ingested_at"),
    ).filter(F.col("lookup_order_id").isNotNull() & F.col("lookup_status").isNotNull())
    row_num = F.row_number().over(
        Window.partitionBy("lookup_order_id").orderBy(
            F.col("lookup_created_at").desc_nulls_last(),
            F.col("lookup_ingested_at").desc_nulls_last(),
            F.col("lookup_status").desc(),
        )
    )
    return (
        lookup_base.withColumn("rn", row_num)
        .filter(F.col("rn") == F.lit(1))
        .select("lookup_order_id", "lookup_status")
    )


def _build_ledger_entries_outputs(
    ledger_df: DataFrame,
    *,
    payment_orders_df: DataFrame,
    run_id: str,
    rule_id: str | None,
    allowed_entry_types: set[str] | None,
    allowed_statuses: set[str] | None,
) -> tuple[DataFrame, DataFrame]:
    ledger_df = _ensure_columns(
        ledger_df,
        (
            "tx_id",
            "wallet_id",
            "type",
            "entry_type",
            "amount",
            "related_id",
            "related_type",
            "status",
            "created_at",
            "event_time",
        ),
    )
    source_columns = tuple(ledger_df.columns)
    entry_sign_map = F.create_map(
        *[
            item
            for key, value in ENTRY_TYPE_SIGN.items()
            for item in (F.lit(key), F.lit(value))
        ]
    )
    entry_type_values = sorted(allowed_entry_types) if allowed_entry_types else []
    status_values = sorted(allowed_statuses) if allowed_statuses else []
    status_lookup_df = _build_status_lookup_df(payment_orders_df)

    evaluated = (
        ledger_df.withColumn("tx_id_raw", _null_if_blank(F.col("tx_id")))
        .withColumn("wallet_id_raw", _null_if_blank(F.col("wallet_id")))
        .withColumn(
            "entry_type_raw",
            F.coalesce(
                _null_if_blank(F.col("type")), _null_if_blank(F.col("entry_type"))
            ),
        )
        .withColumn("amount_raw", F.col("amount").cast("decimal(38,2)"))
        .withColumn("amount_abs_raw", F.abs(F.col("amount_raw")))
        .withColumn("entry_sign_raw", entry_sign_map[F.col("entry_type_raw")])
        .withColumn(
            "event_time_raw",
            F.coalesce(
                F.col("created_at").cast("timestamp"),
                F.col("event_time").cast("timestamp"),
            ),
        )
        .withColumn(
            "related_id_raw",
            F.when(F.col("related_id").isNotNull(), F.col("related_id").cast("string")),
        )
        .join(
            status_lookup_df,
            F.col("related_id_raw") == F.col("lookup_order_id"),
            how="left",
        )
        .withColumn(
            "status_raw",
            F.when(
                F.col("lookup_status").isNotNull(), F.col("lookup_status")
            ).otherwise(F.col("status").cast("string")),
        )
        .withColumn(
            "bad_reason",
            F.when(
                F.col("tx_id_raw").isNull() | F.col("wallet_id_raw").isNull(),
                F.lit("missing_tx_or_wallet"),
            )
            .when(F.col("entry_type_raw").isNull(), F.lit("missing_entry_type"))
            .when(
                F.lit(bool(entry_type_values))
                & (~F.col("entry_type_raw").isin(entry_type_values)),
                F.lit("invalid_entry_type"),
            )
            .when(F.col("amount_raw").isNull(), F.lit("invalid_amount"))
            .when(F.col("amount_raw") <= F.lit(0), F.lit("non_positive_amount"))
            .when(F.col("entry_sign_raw").isNull(), F.lit("amount_signed_unavailable"))
            .when(F.col("event_time_raw").isNull(), F.lit("missing_event_time"))
            .when(
                F.lit(bool(status_values))
                & F.col("status_raw").isNotNull()
                & (~F.col("status_raw").isin(status_values)),
                F.lit("invalid_status"),
            ),
        )
    )

    valid_df = evaluated.filter(F.col("bad_reason").isNull()).select(
        F.col("tx_id_raw").cast("string").alias("tx_id"),
        F.col("wallet_id_raw").cast("string").alias("wallet_id"),
        F.col("event_time_raw").alias("event_time"),
        _date_kst_expr(F.col("event_time_raw")).alias("event_date_kst"),
        F.col("entry_type_raw").cast("string").alias("entry_type"),
        F.col("amount_raw").alias("amount"),
        (F.col("amount_abs_raw") * F.col("entry_sign_raw").cast("decimal(38,2)"))
        .cast("decimal(38,2)")
        .alias("amount_signed"),
        F.col("related_id_raw").alias("related_id"),
        F.col("related_type").cast("string").alias("related_type"),
        F.col("status_raw").alias("status"),
        F.col("created_at").cast("timestamp").alias("created_at"),
        F.lit(run_id).alias("run_id"),
        F.lit(rule_id).cast("string").alias("rule_id"),
    )
    bad_df = _build_bad_records_df(
        evaluated,
        reason_column="bad_reason",
        source_columns=source_columns,
        source_table="silver.ledger_entries",
        run_id=run_id,
        rule_id=rule_id,
    )
    return valid_df, bad_df


def _build_order_events_outputs(
    *,
    orders_df: DataFrame,
    payment_orders_df: DataFrame,
    run_id: str,
) -> tuple[DataFrame, DataFrame]:
    orders_df = _ensure_columns(
        orders_df,
        ("order_id", "user_id", "total_amount", "status", "created_at", "event_time"),
    )
    payment_orders_df = _ensure_columns(
        payment_orders_df,
        (
            "order_id",
            "user_id",
            "merchant_name",
            "amount",
            "status",
            "created_at",
            "event_time",
        ),
    )
    order_source_columns = tuple(orders_df.columns)
    payment_source_columns = tuple(payment_orders_df.columns)

    orders_eval = (
        orders_df.withColumn("order_id_raw", _null_if_blank(F.col("order_id")))
        .withColumn(
            "event_time_raw",
            F.coalesce(
                F.col("created_at").cast("timestamp"),
                F.col("event_time").cast("timestamp"),
            ),
        )
        .withColumn(
            "bad_reason",
            F.when(F.col("order_id_raw").isNull(), F.lit("missing_order_id")),
        )
    )
    payment_eval = (
        payment_orders_df.withColumn("order_id_raw", _null_if_blank(F.col("order_id")))
        .withColumn(
            "event_time_raw",
            F.coalesce(
                F.col("created_at").cast("timestamp"),
                F.col("event_time").cast("timestamp"),
            ),
        )
        .withColumn(
            "bad_reason",
            F.when(F.col("order_id_raw").isNull(), F.lit("missing_order_id")),
        )
    )

    orders_valid = orders_eval.filter(F.col("bad_reason").isNull()).select(
        F.col("order_id_raw").cast("string").alias("order_ref"),
        F.lit("ORDERS").alias("order_source"),
        F.col("user_id").cast("string").alias("user_id"),
        F.lit(None).cast("string").alias("merchant_name"),
        F.col("total_amount").cast("decimal(38,2)").alias("amount"),
        F.col("status").cast("string").alias("status"),
        F.col("event_time_raw").alias("event_time"),
        _date_kst_expr(F.col("event_time_raw")).alias("event_date_kst"),
        F.lit(run_id).alias("run_id"),
    )
    payment_valid = payment_eval.filter(F.col("bad_reason").isNull()).select(
        F.col("order_id_raw").cast("string").alias("order_ref"),
        F.lit("PAYMENT_ORDERS").alias("order_source"),
        F.col("user_id").cast("string").alias("user_id"),
        F.col("merchant_name").cast("string").alias("merchant_name"),
        F.col("amount").cast("decimal(38,2)").alias("amount"),
        F.col("status").cast("string").alias("status"),
        F.col("event_time_raw").alias("event_time"),
        _date_kst_expr(F.col("event_time_raw")).alias("event_date_kst"),
        F.lit(run_id).alias("run_id"),
    )
    valid_df = orders_valid.unionByName(payment_valid)

    orders_bad = _build_bad_records_df(
        orders_eval,
        reason_column="bad_reason",
        source_columns=order_source_columns,
        source_table="silver.order_events",
        run_id=run_id,
        rule_id=None,
    )
    payment_bad = _build_bad_records_df(
        payment_eval,
        reason_column="bad_reason",
        source_columns=payment_source_columns,
        source_table="silver.order_events",
        run_id=run_id,
        rule_id=None,
    )
    bad_df = orders_bad.unionByName(payment_bad)
    return valid_df, bad_df


def _parse_int_column(column) -> F.Column:
    return F.when(
        F.lower(F.trim(column.cast("string"))).isin("true", "false"),
        F.lit(None),
    ).otherwise(column.cast("bigint"))


def _build_order_items_outputs(
    order_items_df: DataFrame,
    *,
    run_id: str,
) -> tuple[DataFrame, DataFrame]:
    order_items_df = _ensure_columns(
        order_items_df,
        ("item_id", "order_id", "product_id", "quantity", "price_at_purchase"),
    )
    source_columns = tuple(order_items_df.columns)
    evaluated = (
        order_items_df.withColumn("item_id_raw", _parse_int_column(F.col("item_id")))
        .withColumn("order_id_raw", _parse_int_column(F.col("order_id")))
        .withColumn("product_id_raw", _parse_int_column(F.col("product_id")))
        .withColumn(
            "bad_reason",
            F.when(
                F.col("item_id_raw").isNull()
                | F.col("order_id_raw").isNull()
                | F.col("product_id_raw").isNull(),
                F.lit("missing_required_keys"),
            ),
        )
    )

    valid_df = evaluated.filter(F.col("bad_reason").isNull()).select(
        F.col("item_id_raw").cast("bigint").alias("item_id"),
        F.col("order_id_raw").cast("bigint").alias("order_id"),
        F.col("order_id_raw").cast("string").alias("order_ref"),
        F.col("product_id_raw").cast("bigint").alias("product_id"),
        F.col("quantity").cast("int").alias("quantity"),
        F.col("price_at_purchase").cast("decimal(38,2)").alias("price_at_purchase"),
        F.lit(run_id).alias("run_id"),
    )
    bad_df = _build_bad_records_df(
        evaluated,
        reason_column="bad_reason",
        source_columns=source_columns,
        source_table="silver.order_items",
        run_id=run_id,
        rule_id=None,
    )
    return valid_df, bad_df


def _build_products_outputs(
    products_df: DataFrame,
    *,
    run_id: str,
) -> tuple[DataFrame, DataFrame]:
    products_df = _ensure_columns(
        products_df,
        ("product_id", "product_name", "category", "price_krw", "is_display"),
    )
    source_columns = tuple(products_df.columns)
    evaluated = products_df.withColumn(
        "product_id_raw", _parse_int_column(F.col("product_id"))
    ).withColumn(
        "bad_reason",
        F.when(F.col("product_id_raw").isNull(), F.lit("missing_product_id")),
    )

    valid_df = evaluated.filter(F.col("bad_reason").isNull()).select(
        F.col("product_id_raw").cast("bigint").alias("product_id"),
        F.col("product_name").cast("string").alias("product_name"),
        F.col("category").cast("string").alias("category"),
        F.col("price_krw").cast("decimal(38,2)").alias("price_krw"),
        F.col("is_display").cast("boolean").alias("is_display"),
        F.lit(run_id).alias("run_id"),
    )
    bad_df = _build_bad_records_df(
        evaluated,
        reason_column="bad_reason",
        source_columns=source_columns,
        source_table="silver.products",
        run_id=run_id,
        rule_id=None,
    )
    return valid_df, bad_df


def transform_silver_tables_spark(
    *,
    wallet_df: DataFrame,
    ledger_df: DataFrame,
    payment_orders_df: DataFrame,
    orders_df: DataFrame,
    order_items_df: DataFrame,
    products_df: DataFrame,
    run_id: str,
    bad_rate_rule_id: str | None,
    entry_type_rule_id: str | None,
    allowed_entry_types: set[str] | None,
    allowed_statuses: set[str] | None,
) -> SilverSparkOutputs:
    wallet_valid_df, wallet_bad_df = _build_wallet_snapshot_outputs(
        wallet_df,
        run_id=run_id,
        rule_id=bad_rate_rule_id,
    )
    ledger_valid_df, ledger_bad_df = _build_ledger_entries_outputs(
        ledger_df,
        payment_orders_df=payment_orders_df,
        run_id=run_id,
        rule_id=entry_type_rule_id,
        allowed_entry_types=allowed_entry_types,
        allowed_statuses=allowed_statuses,
    )
    order_events_df, order_events_bad_df = _build_order_events_outputs(
        orders_df=orders_df,
        payment_orders_df=payment_orders_df,
        run_id=run_id,
    )
    order_items_valid_df, order_items_bad_df = _build_order_items_outputs(
        order_items_df,
        run_id=run_id,
    )
    products_valid_df, products_bad_df = _build_products_outputs(
        products_df,
        run_id=run_id,
    )
    bad_records_df = (
        wallet_bad_df.unionByName(ledger_bad_df)
        .unionByName(order_events_bad_df)
        .unionByName(order_items_bad_df)
        .unionByName(products_bad_df)
    )

    return SilverSparkOutputs(
        wallet_snapshot_df=wallet_valid_df,
        ledger_entries_df=ledger_valid_df,
        order_events_df=order_events_df,
        order_items_df=order_items_valid_df,
        products_df=products_valid_df,
        bad_records_df=bad_records_df,
        wallet_valid_count=wallet_valid_df.count(),
        wallet_bad_count=wallet_bad_df.count(),
        ledger_valid_count=ledger_valid_df.count(),
        ledger_bad_count=ledger_bad_df.count(),
    )

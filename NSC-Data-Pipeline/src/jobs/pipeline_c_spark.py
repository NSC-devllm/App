from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

KST_TIMEZONE = "Asia/Seoul"


def _ensure_columns(df: DataFrame, columns: tuple[str, ...]) -> DataFrame:
    aligned = df
    for column in columns:
        if column not in aligned.columns:
            aligned = aligned.withColumn(column, F.lit(None))
    return aligned


def _parse_int_or_null(column) -> F.Column:
    return F.when(
        F.lower(F.trim(column.cast("string"))).isin("true", "false"),
        F.lit(None),
    ).otherwise(column.cast("bigint"))


def _date_kst_expr(column) -> F.Column:
    return F.to_date(F.from_utc_timestamp(column, KST_TIMEZONE))


def transform_pipeline_c_fact_spark(
    order_events_df: DataFrame,
    order_items_df: DataFrame,
    products_df: DataFrame,
    *,
    run_id: str,
    salt: str,
    order_source_filter: str | None = "PAYMENT_ORDERS",
) -> DataFrame:
    if not salt:
        raise ValueError("salt is required for user_key anonymization")

    order_events_df = _ensure_columns(
        order_events_df,
        (
            "order_ref",
            "order_source",
            "user_id",
            "merchant_name",
            "amount",
            "status",
            "event_time",
            "event_date_kst",
        ),
    )
    order_items_df = _ensure_columns(
        order_items_df,
        (
            "item_id",
            "order_ref",
            "product_id",
            "quantity",
            "price_at_purchase",
        ),
    )
    products_df = _ensure_columns(products_df, ("product_id", "category"))

    products_lookup_df = products_df.select(
        F.col("product_id").cast("string").alias("product_id"),
        F.col("category").cast("string").alias("category"),
    ).filter(F.col("product_id").isNotNull() & F.col("category").isNotNull())

    order_items_prepared = order_items_df.select(
        F.col("order_ref").cast("string").alias("order_ref"),
        F.col("product_id").cast("string").alias("product_id"),
        F.coalesce(
            F.col("price_at_purchase").cast("decimal(38,2)"),
            F.lit("0").cast("decimal(38,2)"),
        ).alias("price_at_purchase"),
        F.coalesce(
            _parse_int_or_null(F.col("quantity")),
            F.lit(1),
        )
        .cast("bigint")
        .alias("quantity"),
        _parse_int_or_null(F.col("item_id")).alias("item_id"),
    ).filter(
        F.col("order_ref").isNotNull()
        & (F.col("order_ref") != F.lit(""))
        & F.col("product_id").isNotNull()
    )

    category_candidates_df = (
        order_items_prepared.join(products_lookup_df, on="product_id", how="inner")
        .withColumn(
            "line_amount",
            (F.col("price_at_purchase") * F.col("quantity").cast("decimal(38,2)")).cast(
                "decimal(38,2)"
            ),
        )
        .withColumn(
            "item_id_null",
            F.when(F.col("item_id").isNull(), F.lit(1)).otherwise(F.lit(0)),
        )
    )

    category_rank_window = Window.partitionBy("order_ref").orderBy(
        F.col("line_amount").desc(),
        F.col("item_id_null").asc(),
        F.col("item_id").asc(),
        F.col("product_id").asc(),
        F.col("category").asc(),
    )
    categories_by_order_df = (
        category_candidates_df.withColumn(
            "rn", F.row_number().over(category_rank_window)
        )
        .filter(F.col("rn") == F.lit(1))
        .select(
            F.col("order_ref").cast("string").alias("order_ref"),
            F.col("category").cast("string").alias("category"),
        )
    )

    events_df = order_events_df.select(
        F.col("order_ref").cast("string").alias("order_ref"),
        F.col("order_source").cast("string").alias("order_source"),
        F.col("user_id").cast("string").alias("user_id"),
        F.col("merchant_name").cast("string").alias("merchant_name"),
        F.col("amount").cast("decimal(38,2)").alias("amount"),
        F.col("status").cast("string").alias("status"),
        F.col("event_time").cast("timestamp").alias("event_time"),
        F.col("event_date_kst").cast("date").alias("event_date_kst"),
    )
    if order_source_filter:
        events_df = events_df.filter(
            F.col("order_source") == F.lit(str(order_source_filter))
        )

    filtered_events_df = (
        events_df.filter(
            F.col("order_ref").isNotNull()
            & (F.col("order_ref") != F.lit(""))
            & F.col("user_id").isNotNull()
            & (F.col("user_id") != F.lit(""))
        )
        .withColumn(
            "date_kst",
            F.coalesce(
                F.col("event_date_kst"),
                _date_kst_expr(F.col("event_time")),
            ),
        )
        .filter(F.col("date_kst").isNotNull())
    )

    return filtered_events_df.join(
        categories_by_order_df,
        on="order_ref",
        how="left",
    ).select(
        F.col("date_kst").cast("date").alias("date_kst"),
        F.sha2(F.concat(F.col("user_id"), F.lit(salt)), 256).alias("user_key"),
        F.col("merchant_name").cast("string").alias("merchant_name"),
        F.col("amount").cast("decimal(38,2)").alias("amount"),
        F.col("status").cast("string").alias("status"),
        F.col("category").cast("string").alias("category"),
        F.lit(run_id).alias("run_id"),
    )

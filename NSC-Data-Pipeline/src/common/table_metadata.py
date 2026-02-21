from __future__ import annotations

SILVER_MERGE_KEYS: dict[str, tuple[str, ...]] = {
    "silver.wallet_snapshot": ("snapshot_ts", "user_id"),
    "silver.ledger_entries": ("tx_id", "wallet_id"),
    "silver.order_events": ("order_ref", "order_source"),
    "silver.order_items": ("item_id",),
    "silver.products": ("product_id",),
}

SILVER_PARTITION_COLUMNS: dict[str, tuple[str, ...]] = {
    "silver.wallet_snapshot": ("snapshot_date_kst",),
    "silver.ledger_entries": ("event_date_kst",),
    "silver.order_events": ("event_date_kst",),
    "silver.order_items": (),
    "silver.products": (),
    "silver.bad_records": ("detected_date_kst",),
    "silver.dq_status": ("date_kst",),
}

GOLD_MERGE_KEYS: dict[str, tuple[str, ...]] = {
    "gold.recon_daily_snapshot_flow": ("date_kst", "user_id"),
    "gold.ledger_supply_balance_daily": ("date_kst",),
    "gold.exception_ledger": (
        "date_kst",
        "domain",
        "exception_type",
        "run_id",
        "metric",
        "message",
    ),
    "gold.pipeline_state": ("pipeline_name",),
    "gold.ops_payment_failure_daily": ("date_kst", "merchant_name"),
    "gold.ops_payment_refund_daily": ("date_kst", "merchant_name"),
    "gold.ops_ledger_pairing_quality_daily": ("date_kst",),
    "gold.admin_tx_search": ("event_date_kst", "tx_id"),
    "gold.fact_payment_anonymized": (),
    "gold.dim_rule_scd2": ("rule_id",),
}

GOLD_PARTITION_COLUMNS: dict[str, tuple[str, ...]] = {
    "gold.recon_daily_snapshot_flow": ("date_kst",),
    "gold.ledger_supply_balance_daily": ("date_kst",),
    "gold.exception_ledger": ("date_kst",),
    "gold.pipeline_state": (),
    "gold.ops_payment_failure_daily": ("date_kst",),
    "gold.ops_payment_refund_daily": ("date_kst",),
    "gold.ops_ledger_pairing_quality_daily": ("date_kst",),
    "gold.admin_tx_search": ("event_date_kst",),
    "gold.fact_payment_anonymized": ("date_kst",),
    "gold.dim_rule_scd2": (),
}

GOLD_WRITE_STRATEGY: dict[str, str] = {
    "gold.recon_daily_snapshot_flow": "merge",
    "gold.ledger_supply_balance_daily": "merge",
    "gold.exception_ledger": "merge",
    "gold.pipeline_state": "merge",
    "gold.ops_payment_failure_daily": "merge",
    "gold.ops_payment_refund_daily": "merge",
    "gold.ops_ledger_pairing_quality_daily": "merge",
    "gold.admin_tx_search": "merge",
    "gold.fact_payment_anonymized": "overwrite_partitions",
    "gold.dim_rule_scd2": "merge",
}

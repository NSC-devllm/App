from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Mapping


class ContractValidationError(ValueError):
    pass


@dataclass(frozen=True)
class ContractColumn:
    name: str
    data_type: str
    required: bool = False


@dataclass(frozen=True)
class TableContract:
    name: str
    columns: tuple[ContractColumn, ...]
    description: str | None = None

    @property
    def column_names(self) -> tuple[str, ...]:
        return tuple(column.name for column in self.columns)

    @property
    def required_columns(self) -> set[str]:
        return {column.name for column in self.columns if column.required}

    @property
    def cast_map(self) -> Mapping[str, str]:
        return {column.name: column.data_type for column in self.columns}


META_COLUMNS = (
    ContractColumn("ingested_at", "timestamp", False),
    ContractColumn("source_extracted_at", "timestamp", False),
    ContractColumn("batch_id", "string", False),
    ContractColumn("source_system", "string", False),
)

BRONZE_CONTRACTS: dict[str, TableContract] = {
    "bronze.user_wallets_raw": TableContract(
        name="bronze.user_wallets_raw",
        columns=(
            ContractColumn("user_id", "string", True),
            ContractColumn("balance", "decimal(18,2)", True),
            ContractColumn("frozen_amount", "decimal(18,2)", True),
            ContractColumn("updated_at", "timestamp", False),
            *META_COLUMNS,
        ),
        description="Raw user wallet balances.",
    ),
    "bronze.transaction_ledger_raw": TableContract(
        name="bronze.transaction_ledger_raw",
        columns=(
            ContractColumn("tx_id", "string", True),
            ContractColumn("wallet_id", "string", True),
            ContractColumn("type", "string", True),
            ContractColumn("amount", "decimal(18,2)", True),
            ContractColumn("related_id", "string", False),
            ContractColumn("created_at", "timestamp", False),
            *META_COLUMNS,
        ),
        description="Raw ledger events.",
    ),
    "bronze.payment_orders_raw": TableContract(
        name="bronze.payment_orders_raw",
        columns=(
            ContractColumn("order_id", "string", True),
            ContractColumn("user_id", "string", False),
            ContractColumn("merchant_name", "string", False),
            ContractColumn("amount", "decimal(18,2)", True),
            ContractColumn("status", "string", False),
            ContractColumn("created_at", "timestamp", False),
            *META_COLUMNS,
        ),
        description="Raw payment orders.",
    ),
    "bronze.orders_raw": TableContract(
        name="bronze.orders_raw",
        columns=(
            ContractColumn("order_id", "bigint", True),
            ContractColumn("user_id", "string", False),
            ContractColumn("total_amount", "decimal(18,2)", False),
            ContractColumn("status", "string", False),
            ContractColumn("created_at", "timestamp", False),
            *META_COLUMNS,
        ),
        description="Raw commerce orders.",
    ),
    "bronze.order_items_raw": TableContract(
        name="bronze.order_items_raw",
        columns=(
            ContractColumn("item_id", "bigint", True),
            ContractColumn("order_id", "bigint", True),
            ContractColumn("product_id", "bigint", True),
            ContractColumn("quantity", "int", False),
            ContractColumn("price_at_purchase", "decimal(18,2)", False),
            *META_COLUMNS,
        ),
        description="Raw commerce order items.",
    ),
    "bronze.products_raw": TableContract(
        name="bronze.products_raw",
        columns=(
            ContractColumn("product_id", "bigint", True),
            ContractColumn("product_name", "string", False),
            ContractColumn("price_krw", "decimal(18,2)", False),
            ContractColumn("stock_quantity", "int", False),
            ContractColumn("category", "string", False),
            ContractColumn("is_display", "boolean", False),
            *META_COLUMNS,
        ),
        description="Raw products catalog.",
    ),
}

SILVER_CONTRACTS: dict[str, TableContract] = {
    "silver.wallet_snapshot": TableContract(
        name="silver.wallet_snapshot",
        columns=(
            ContractColumn("snapshot_ts", "timestamp", True),
            ContractColumn("snapshot_date_kst", "date", True),
            ContractColumn("user_id", "string", True),
            ContractColumn("balance_available", "decimal(38,2)", True),
            ContractColumn("balance_frozen", "decimal(38,2)", True),
            ContractColumn("balance_total", "decimal(38,2)", True),
            ContractColumn("source_updated_at", "timestamp", False),
            ContractColumn("run_id", "string", True),
            ContractColumn("rule_id", "string", False),
        ),
        description="Point-in-time wallet balance snapshots.",
    ),
    "silver.ledger_entries": TableContract(
        name="silver.ledger_entries",
        columns=(
            ContractColumn("tx_id", "string", True),
            ContractColumn("wallet_id", "string", True),
            ContractColumn("event_time", "timestamp", True),
            ContractColumn("event_date_kst", "date", True),
            ContractColumn("entry_type", "string", True),
            ContractColumn("amount", "decimal(38,2)", True),
            ContractColumn("amount_signed", "decimal(38,2)", True),
            ContractColumn("related_id", "string", False),
            ContractColumn("related_type", "string", False),
            ContractColumn("status", "string", False),
            ContractColumn("created_at", "timestamp", False),
            ContractColumn("run_id", "string", True),
            ContractColumn("rule_id", "string", False),
        ),
        description="Standardized ledger entries for reconciliation.",
    ),
    "silver.order_events": TableContract(
        name="silver.order_events",
        columns=(
            ContractColumn("order_ref", "string", True),
            ContractColumn("order_source", "string", True),
            ContractColumn("user_id", "string", False),
            ContractColumn("merchant_name", "string", False),
            ContractColumn("amount", "decimal(38,2)", False),
            ContractColumn("status", "string", False),
            ContractColumn("event_time", "timestamp", False),
            ContractColumn("event_date_kst", "date", False),
            ContractColumn("run_id", "string", True),
        ),
        description="Normalized order/payment events for analytics.",
    ),
    "silver.order_items": TableContract(
        name="silver.order_items",
        columns=(
            ContractColumn("item_id", "bigint", True),
            ContractColumn("order_id", "bigint", True),
            ContractColumn("order_ref", "string", True),
            ContractColumn("product_id", "bigint", True),
            ContractColumn("quantity", "int", False),
            ContractColumn("price_at_purchase", "decimal(38,2)", False),
            ContractColumn("run_id", "string", True),
        ),
        description="Order line items for analytics.",
    ),
    "silver.products": TableContract(
        name="silver.products",
        columns=(
            ContractColumn("product_id", "bigint", True),
            ContractColumn("product_name", "string", False),
            ContractColumn("category", "string", False),
            ContractColumn("price_krw", "decimal(38,2)", False),
            ContractColumn("is_display", "boolean", False),
            ContractColumn("run_id", "string", True),
        ),
        description="Product dimension for analytics.",
    ),
    "silver.bad_records": TableContract(
        name="silver.bad_records",
        columns=(
            ContractColumn("detected_date_kst", "date", True),
            ContractColumn("source_table", "string", True),
            ContractColumn("reason", "string", True),
            ContractColumn("record_json", "string", True),
            ContractColumn("run_id", "string", True),
            ContractColumn("rule_id", "string", False),
            ContractColumn("detected_at", "timestamp", True),
        ),
        description="Quarantined bad records from silver transforms.",
    ),
    "silver.dq_status": TableContract(
        name="silver.dq_status",
        columns=(
            ContractColumn("source_table", "string", True),
            ContractColumn("window_start_ts", "timestamp", True),
            ContractColumn("window_end_ts", "timestamp", True),
            ContractColumn("date_kst", "date", True),
            ContractColumn("freshness_sec", "bigint", False),
            ContractColumn("event_count", "bigint", True),
            ContractColumn("dup_rate", "decimal(38,6)", False),
            ContractColumn("bad_records_rate", "decimal(38,6)", False),
            ContractColumn("dq_tag", "string", False),
            ContractColumn("severity", "string", False),
            ContractColumn("run_id", "string", True),
            ContractColumn("rule_id", "string", False),
            ContractColumn("generated_at", "timestamp", True),
        ),
        description="Guardrail data quality status per window.",
    ),
}

GOLD_CONTRACTS: dict[str, TableContract] = {
    "gold.recon_daily_snapshot_flow": TableContract(
        name="gold.recon_daily_snapshot_flow",
        columns=(
            ContractColumn("date_kst", "date", True),
            ContractColumn("user_id", "string", True),
            ContractColumn("delta_balance_total", "decimal(38,2)", True),
            ContractColumn("net_flow_total", "decimal(38,2)", True),
            ContractColumn("drift_abs", "decimal(38,2)", True),
            ContractColumn("drift_pct", "decimal(38,6)", False),
            ContractColumn("dq_tag", "string", False),
            ContractColumn("run_id", "string", True),
            ContractColumn("rule_id", "string", False),
        ),
        description="Daily reconciliation results.",
    ),
    "gold.ledger_supply_balance_daily": TableContract(
        name="gold.ledger_supply_balance_daily",
        columns=(
            ContractColumn("date_kst", "date", True),
            ContractColumn("issued_supply", "decimal(38,2)", True),
            ContractColumn("wallet_total_balance", "decimal(38,2)", True),
            ContractColumn("diff_amount", "decimal(38,2)", True),
            ContractColumn("is_ok", "boolean", True),
            ContractColumn("run_id", "string", True),
            ContractColumn("rule_id", "string", False),
        ),
        description="Supply vs wallet balance check.",
    ),
    "gold.fact_payment_anonymized": TableContract(
        name="gold.fact_payment_anonymized",
        columns=(
            ContractColumn("date_kst", "date", True),
            ContractColumn("user_key", "string", True),
            ContractColumn("merchant_name", "string", False),
            ContractColumn("amount", "decimal(38,2)", False),
            ContractColumn("status", "string", False),
            ContractColumn("category", "string", False),
            ContractColumn("run_id", "string", True),
        ),
        description="Anonymized payment facts for analytics.",
    ),
    "gold.admin_tx_search": TableContract(
        name="gold.admin_tx_search",
        columns=(
            ContractColumn("event_date_kst", "date", True),
            ContractColumn("tx_id", "string", True),
            ContractColumn("wallet_id", "string", True),
            ContractColumn("entry_type", "string", True),
            ContractColumn("amount", "decimal(38,2)", True),
            ContractColumn("amount_signed", "decimal(38,2)", False),
            ContractColumn("event_time", "timestamp", True),
            ContractColumn("related_id", "string", False),
            ContractColumn("related_type", "string", False),
            ContractColumn("merchant_name", "string", False),
            ContractColumn("payment_status", "string", False),
            ContractColumn("paired_tx_id", "string", False),
            ContractColumn("run_id", "string", True),
            ContractColumn("rule_id", "string", False),
        ),
        description="Batch index for admin tx search.",
    ),
    "gold.ops_payment_failure_daily": TableContract(
        name="gold.ops_payment_failure_daily",
        columns=(
            ContractColumn("date_kst", "date", True),
            ContractColumn("merchant_name", "string", False),
            ContractColumn("total_cnt", "bigint", True),
            ContractColumn("failed_cnt", "bigint", True),
            ContractColumn("failure_rate", "decimal(38,6)", True),
            ContractColumn("run_id", "string", True),
            ContractColumn("rule_id", "string", False),
        ),
        description="Daily payment failure rate by merchant.",
    ),
    "gold.ops_payment_refund_daily": TableContract(
        name="gold.ops_payment_refund_daily",
        columns=(
            ContractColumn("date_kst", "date", True),
            ContractColumn("merchant_name", "string", False),
            ContractColumn("total_cnt", "bigint", True),
            ContractColumn("refunded_cnt", "bigint", True),
            ContractColumn("refund_rate", "decimal(38,6)", True),
            ContractColumn("run_id", "string", True),
            ContractColumn("rule_id", "string", False),
        ),
        description="Daily payment refund rate by merchant.",
    ),
    "gold.ops_ledger_pairing_quality_daily": TableContract(
        name="gold.ops_ledger_pairing_quality_daily",
        columns=(
            ContractColumn("date_kst", "date", True),
            ContractColumn("entry_cnt", "bigint", True),
            ContractColumn("related_id_null_rate", "decimal(38,6)", True),
            ContractColumn("pair_candidate_rate", "decimal(38,6)", True),
            ContractColumn("join_payment_orders_rate", "decimal(38,6)", True),
            ContractColumn("run_id", "string", True),
            ContractColumn("rule_id", "string", False),
        ),
        description="Ledger pairing quality metrics.",
    ),
    "gold.exception_ledger": TableContract(
        name="gold.exception_ledger",
        columns=(
            ContractColumn("date_kst", "date", True),
            ContractColumn("domain", "string", True),
            ContractColumn("exception_type", "string", True),
            ContractColumn("severity", "string", True),
            ContractColumn("source_table", "string", False),
            ContractColumn("window_start_ts", "timestamp", False),
            ContractColumn("window_end_ts", "timestamp", False),
            ContractColumn("metric", "string", False),
            ContractColumn("metric_value", "decimal(38,6)", False),
            ContractColumn("message", "string", False),
            ContractColumn("run_id", "string", True),
            ContractColumn("rule_id", "string", False),
            ContractColumn("generated_at", "timestamp", True),
        ),
        description="Unified exception ledger (dq/recon/analytics).",
    ),
    "gold.pipeline_state": TableContract(
        name="gold.pipeline_state",
        columns=(
            ContractColumn("pipeline_name", "string", True),
            ContractColumn("last_success_ts", "timestamp", False),
            ContractColumn("last_processed_end", "timestamp", False),
            ContractColumn("last_run_id", "string", False),
            # JSON map: {source_table: consecutive_zero_window_count}
            ContractColumn("dq_zero_window_counts", "string", False),
            ContractColumn("updated_at", "timestamp", True),
        ),
        description="Pipeline execution state store.",
    ),
    "gold.dim_rule_scd2": TableContract(
        name="gold.dim_rule_scd2",
        columns=(
            ContractColumn("rule_id", "string", True),
            ContractColumn("domain", "string", True),
            ContractColumn("metric", "string", True),
            ContractColumn("threshold", "double", False),
            ContractColumn("severity_map", "map<string,double>", False),
            ContractColumn("allowed_values", "array<string>", False),
            ContractColumn("comment", "string", False),
            ContractColumn("effective_start_ts", "timestamp", True),
            ContractColumn("effective_end_ts", "timestamp", False),
            ContractColumn("is_current", "boolean", True),
        ),
        description="SCD2 rule definitions for runtime thresholds and allowlists.",
    ),
}

CONTRACTS: dict[str, TableContract] = {
    **BRONZE_CONTRACTS,
    **SILVER_CONTRACTS,
    **GOLD_CONTRACTS,
}


def get_contract(table_name: str) -> TableContract:
    try:
        return CONTRACTS[table_name]
    except KeyError as exc:
        raise KeyError(f"Unknown contract table: {table_name}") from exc


def missing_required_columns(
    actual_columns: Iterable[str],
    contract: TableContract,
) -> list[str]:
    actual = set(actual_columns)
    missing = sorted(contract.required_columns - actual)
    return missing


def validate_required_columns(
    actual_columns: Iterable[str],
    contract: TableContract,
) -> None:
    missing = missing_required_columns(actual_columns, contract)
    if missing:
        missing_str = ", ".join(missing)
        raise ContractValidationError(
            f"Missing required columns for {contract.name}: {missing_str}"
        )

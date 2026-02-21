from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Mapping

from src.common.contracts import (
    BRONZE_CONTRACTS,
    ContractValidationError,
    TableContract,
    get_contract,
)
from src.common.time_utils import now_utc, to_utc

DEFAULT_BRONZE_BASE_DIR = Path("mock_data/bronze")
DEFAULT_FILE_GLOB = "*.jsonl"


@dataclass(frozen=True)
class BronzeTableFiles:
    table_name: str
    table_dir: Path
    data_files: tuple[Path, ...]


def normalize_table_name(table_name: str) -> str:
    if table_name in BRONZE_CONTRACTS:
        return table_name
    candidate = f"bronze.{table_name}"
    if candidate in BRONZE_CONTRACTS:
        return candidate
    raise KeyError(f"Unknown bronze table: {table_name}")


def bronze_short_name(table_name: str) -> str:
    normalized = normalize_table_name(table_name)
    return normalized.split(".", maxsplit=1)[1]


def bronze_table_dir(
    table_name: str, base_dir: Path | str = DEFAULT_BRONZE_BASE_DIR
) -> Path:
    base = Path(base_dir)
    return base / bronze_short_name(table_name)


def discover_bronze_files(
    table_name: str,
    base_dir: Path | str = DEFAULT_BRONZE_BASE_DIR,
    file_glob: str = DEFAULT_FILE_GLOB,
) -> BronzeTableFiles:
    table_dir = bronze_table_dir(table_name, base_dir=base_dir)
    if not table_dir.exists():
        raise FileNotFoundError(f"Bronze directory not found: {table_dir}")
    data_files = tuple(sorted(table_dir.glob(file_glob)))
    if not data_files:
        raise FileNotFoundError(
            f"No bronze files found for {table_name} in {table_dir}"
        )
    return BronzeTableFiles(
        table_name=normalize_table_name(table_name),
        table_dir=table_dir,
        data_files=data_files,
    )


def _load_jsonl_file(path: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            stripped = line.strip()
            if not stripped:
                continue
            payload = json.loads(stripped)
            if not isinstance(payload, dict):
                raise ValueError(f"JSONL payload must be an object: {path}")
            records.append(payload)
    return records


def load_jsonl_records(paths: Iterable[Path]) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for path in paths:
        records.extend(_load_jsonl_file(path))
    return records


def load_bronze_records(
    table_name: str,
    base_dir: Path | str = DEFAULT_BRONZE_BASE_DIR,
    file_glob: str = DEFAULT_FILE_GLOB,
) -> list[dict[str, Any]]:
    files = discover_bronze_files(table_name, base_dir=base_dir, file_glob=file_glob)
    return load_jsonl_records(files.data_files)


def _validate_required_columns(
    records: Iterable[Mapping[str, Any]],
    contract: TableContract,
) -> None:
    required = contract.required_columns
    for index, record in enumerate(records):
        missing = required - set(record.keys())
        if missing:
            missing_str = ", ".join(sorted(missing))
            raise ContractValidationError(
                f"Missing required columns for {contract.name} "
                f"(record {index}): {missing_str}"
            )


def _normalize_datetime(value: datetime | str | None) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return to_utc(value)
    if isinstance(value, str):
        normalized = value.replace("Z", "+00:00")
        parsed = datetime.fromisoformat(normalized)
        return to_utc(parsed)
    raise TypeError("Unsupported datetime value")


def build_bronze_metadata(
    *,
    ingested_at: datetime | str | None = None,
    source_extracted_at: datetime | str | None = None,
    batch_id: str | None = None,
    source_system: str | None = None,
) -> dict[str, Any]:
    ingested_at_ts = _normalize_datetime(ingested_at) or now_utc()
    return {
        "ingested_at": ingested_at_ts,
        "source_extracted_at": _normalize_datetime(source_extracted_at),
        "batch_id": batch_id,
        "source_system": source_system,
    }


def enrich_bronze_records(
    records: Iterable[Mapping[str, Any]],
    *,
    ingested_at: datetime | str | None = None,
    source_extracted_at: datetime | str | None = None,
    batch_id: str | None = None,
    source_system: str | None = None,
) -> list[dict[str, Any]]:
    meta = build_bronze_metadata(
        ingested_at=ingested_at,
        source_extracted_at=source_extracted_at,
        batch_id=batch_id,
        source_system=source_system,
    )
    enriched: list[dict[str, Any]] = []
    for record in records:
        payload = dict(record)
        payload.update(meta)
        enriched.append(payload)
    return enriched


def default_batch_id(prefix: str = "mock") -> str:
    timestamp = now_utc().strftime("%Y%m%dT%H%M%SZ")
    return f"{prefix}_{timestamp}"


def prepare_bronze_records(
    table_name: str,
    base_dir: Path | str = DEFAULT_BRONZE_BASE_DIR,
    *,
    ingested_at: datetime | str | None = None,
    source_extracted_at: datetime | str | None = None,
    batch_id: str | None = None,
    source_system: str | None = None,
    file_glob: str = DEFAULT_FILE_GLOB,
) -> list[dict[str, Any]]:
    normalized = normalize_table_name(table_name)
    records = load_bronze_records(normalized, base_dir=base_dir, file_glob=file_glob)
    if records:
        _validate_required_columns(records, get_contract(normalized))
    return enrich_bronze_records(
        records,
        ingested_at=ingested_at,
        source_extracted_at=source_extracted_at,
        batch_id=batch_id,
        source_system=source_system,
    )


def build_bronze_dataframe(  # pragma: no cover
    spark,
    table_name: str,
    base_dir: Path | str = DEFAULT_BRONZE_BASE_DIR,
    *,
    ingested_at: datetime | str | None = None,
    source_extracted_at: datetime | str | None = None,
    batch_id: str | None = None,
    source_system: str | None = None,
    file_glob: str = DEFAULT_FILE_GLOB,
):
    from pyspark.sql import functions as F

    normalized = normalize_table_name(table_name)
    files = discover_bronze_files(normalized, base_dir=base_dir, file_glob=file_glob)
    df = spark.read.json(_to_spark_paths(files.data_files))
    meta = build_bronze_metadata(
        ingested_at=ingested_at,
        source_extracted_at=source_extracted_at,
        batch_id=batch_id,
        source_system=source_system,
    )
    df = (
        df.withColumn("ingested_at", F.lit(meta["ingested_at"]).cast("timestamp"))
        .withColumn(
            "source_extracted_at",
            F.lit(meta["source_extracted_at"]).cast("timestamp"),
        )
        .withColumn("batch_id", F.lit(meta["batch_id"]).cast("string"))
        .withColumn("source_system", F.lit(meta["source_system"]).cast("string"))
    )
    return _cast_dataframe_to_contract(df, get_contract(normalized))


def _to_spark_paths(paths: Iterable[Path]) -> list[str]:
    resolved: list[str] = []
    for path in paths:
        path_str = str(path)
        if path_str.startswith("/dbfs/"):
            resolved.append(f"dbfs:/{path_str[len('/dbfs/') :]}")
        else:
            resolved.append(path_str)
    return resolved


def _cast_dataframe_to_contract(df, contract: TableContract):  # pragma: no cover
    from pyspark.sql import functions as F

    for column, data_type in contract.cast_map.items():
        if column in df.columns:
            df = df.withColumn(column, F.col(column).cast(data_type))
        else:
            df = df.withColumn(column, F.lit(None).cast(data_type))
    return df.select(*contract.column_names)


def write_bronze_delta(  # pragma: no cover
    df,
    table_fqn: str,
    *,
    mode: str = "append",
    merge_schema: bool = True,
) -> None:
    # Unity Catalog + ADLS managed locations can fail during CREATE TABLE when using
    # `mode("overwrite")` because Spark may execute an atomic replace flow that
    # requests table-scoped SAS tokens before the table metadata is committed.
    # For first-time table creation, `append` is equivalent and avoids that path.
    spark = df.sparkSession
    table_exists = spark.catalog.tableExists(table_fqn)

    effective_mode = mode
    if not table_exists:
        effective_mode = "append"

    writer = df.write.format("delta").mode(effective_mode)
    # Avoid `mergeSchema` on first CREATE TABLE in Unity Catalog managed locations:
    # it can trigger a DeltaLog read on an uncommitted/staged table path.
    if merge_schema and table_exists:
        writer = writer.option("mergeSchema", "true")
    writer.saveAsTable(table_fqn)

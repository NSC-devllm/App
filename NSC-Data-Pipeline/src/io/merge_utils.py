from __future__ import annotations

from typing import Iterable


def build_merge_condition(
    keys: Iterable[str],
    *,
    target_alias: str = "target",
    source_alias: str = "source",
) -> str:
    key_list = [key for key in keys if key]
    if not key_list:
        raise ValueError("merge keys are required")
    return " AND ".join(
        f"{target_alias}.{key} = {source_alias}.{key}" for key in key_list
    )


def merge_delta_table(  # pragma: no cover
    df,
    table_fqn: str,
    merge_keys: Iterable[str],
    *,
    target_alias: str = "target",
    source_alias: str = "source",
) -> None:
    import os

    from delta.tables import DeltaTable
    from pyspark.sql import functions as F

    key_list = [key for key in merge_keys if key]
    if not key_list:
        raise ValueError("merge keys are required")

    # Fail fast on obvious merge hazards. Delta MERGE will fail on duplicates,
    # but the error is often hard to interpret; null keys typically mean the
    # write would be silently wrong even if it "succeeds".
    null_expr = None
    for key in key_list:
        expr = F.col(key).isNull()
        null_expr = expr if null_expr is None else (null_expr | expr)
    if null_expr is not None and df.filter(null_expr).limit(1).count() > 0:
        raise ValueError(f"NULL merge keys detected for {table_fqn}: {key_list}")

    validate_uniqueness = os.environ.get(
        "PIPELINE_VALIDATE_MERGE_KEY_UNIQUENESS", "0"
    ).strip().lower() in {"1", "true", "yes", "on"}
    if validate_uniqueness:
        has_dupes = (
            df.groupBy(*key_list).count().filter(F.col("count") > 1).limit(1).count()
            > 0
        )
        if has_dupes:
            raise ValueError(
                f"Duplicate merge keys detected for {table_fqn}: {key_list}. "
                "Set PIPELINE_VALIDATE_MERGE_KEY_UNIQUENESS=0 to disable this check."
            )

    condition = build_merge_condition(
        merge_keys, target_alias=target_alias, source_alias=source_alias
    )
    delta_table = DeltaTable.forName(df.sparkSession, table_fqn)
    (
        delta_table.alias(target_alias)
        .merge(df.alias(source_alias), condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

from __future__ import annotations

from typing import Iterable

from sqlalchemy import and_, or_
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.sql.schema import Table


def latest_wins_upsert(
    table: Table,
    values: dict,
    conflict_cols: Iterable[str],
    updated_at_col: str = "updated_at",
    version_col: str = "source_version",
    ingested_at_col: str = "ingested_at",
):
    stmt = pg_insert(table).values(**values)
    conflict_cols = list(conflict_cols)

    update_columns = {
        column.name: getattr(stmt.excluded, column.name)
        for column in table.columns
        if column.name not in conflict_cols
    }

    predicate = None
    if updated_at_col in table.c and ingested_at_col in table.c:
        incoming_updated = getattr(stmt.excluded, updated_at_col)
        incoming_ingested = getattr(stmt.excluded, ingested_at_col)
        existing_updated = table.c[updated_at_col]
        existing_ingested = table.c[ingested_at_col]

        conditions = [
            and_(
                incoming_updated.isnot(None),
                or_(existing_updated.is_(None), incoming_updated >= existing_updated),
            )
        ]

        if version_col in table.c:
            incoming_version = getattr(stmt.excluded, version_col)
            existing_version = table.c[version_col]
            conditions.append(
                and_(
                    incoming_updated.is_(None),
                    incoming_version.isnot(None),
                    or_(
                        existing_version.is_(None), incoming_version >= existing_version
                    ),
                )
            )

        fallback_checks = [incoming_updated.is_(None)]
        if version_col in table.c:
            fallback_checks.append(incoming_version.is_(None))
        fallback_checks.append(existing_updated.is_(None))
        if version_col in table.c:
            fallback_checks.append(existing_version.is_(None))
        fallback_checks.append(incoming_ingested >= existing_ingested)
        conditions.append(and_(*fallback_checks))

        predicate = or_(*conditions)

    return stmt.on_conflict_do_update(
        index_elements=conflict_cols,
        set_=update_columns,
        where=predicate,
    )

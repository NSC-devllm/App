from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Generator

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

DEFAULT_DB_URL = "postgresql+psycopg://bo:bo@localhost:5432/bo"


@pytest.fixture(scope="session")
def db_engine():
    url = os.getenv("DATABASE_URL", DEFAULT_DB_URL)
    return create_engine(url, pool_pre_ping=True)


@pytest.fixture()
def db_session(db_engine) -> Generator:
    Session = sessionmaker(bind=db_engine, autoflush=False, autocommit=False)
    session = Session()
    try:
        yield session
        session.commit()
    finally:
        session.close()


@pytest.fixture()
def cleanup_test_rows(db_engine):
    prefix = f"test-{int(datetime.now(timezone.utc).timestamp())}"
    yield prefix
    with db_engine.begin() as conn:
        conn.execute(
            text(
                "delete from bo.payment_ledger_pairs where payment_order_id like :prefix"
            ),
            {"prefix": f"{prefix}%"},
        )
        conn.execute(
            text(
                "delete from bo.ledger_entries where related_id like :prefix or tx_id like :prefix"
            ),
            {"prefix": f"{prefix}%"},
        )
        conn.execute(
            text("delete from bo.payment_orders where order_id like :prefix"),
            {"prefix": f"{prefix}%"},
        )
        conn.execute(
            text("delete from bo.admin_audit_logs where resource_id like :prefix"),
            {"prefix": f"{prefix}%"},
        )

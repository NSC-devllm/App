from __future__ import annotations

import time
from contextlib import contextmanager

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from src.common.config import load_config
from src.common.metrics import DB_POOL_CHECKOUT_LATENCY_SECONDS, register_pool_engine
from src.db.observability import install_sqlalchemy_observability

_ENGINE = None
_SESSION_FACTORY = None


def get_engine():
    global _ENGINE
    if _ENGINE is None:
        config = load_config()
        _ENGINE = create_engine(
            config.database_url,
            pool_pre_ping=True,
            pool_size=config.db_pool_size,
            max_overflow=config.db_max_overflow,
            pool_timeout=config.db_pool_timeout,
            pool_recycle=config.db_pool_recycle,
        )
        install_sqlalchemy_observability(_ENGINE)
        register_pool_engine(_ENGINE)
    return _ENGINE


def get_session_factory() -> sessionmaker[Session]:
    global _SESSION_FACTORY
    if _SESSION_FACTORY is None:
        _SESSION_FACTORY = sessionmaker(
            bind=get_engine(), autocommit=False, autoflush=False
        )
    return _SESSION_FACTORY


@contextmanager
def session_scope() -> Session:
    t0 = time.perf_counter()
    session = get_session_factory()()
    DB_POOL_CHECKOUT_LATENCY_SECONDS.record(time.perf_counter() - t0)
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()

"""Unit tests for DB pool Observable Gauge metrics and checkout latency."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import src.common.metrics as metrics
from src.common.metrics import (
    _db_replication_lag_cb,
    _pool_checked_in_cb,
    _pool_checked_out_cb,
    _pool_engine,
    _pool_overflow_cb,
    _pool_size_cb,
    register_pool_engine,
    register_replication_lag_provider,
)


def _make_mock_engine(
    size: int = 5,
    checkedout: int = 2,
    overflow: int = 0,
    checkedin: int = 3,
) -> MagicMock:
    engine = MagicMock()
    engine.pool.size.return_value = size
    engine.pool.checkedout.return_value = checkedout
    engine.pool.overflow.return_value = overflow
    engine.pool.checkedin.return_value = checkedin
    return engine


class TestRegisterPoolEngine:
    def setup_method(self):
        _pool_engine.clear()

    def teardown_method(self):
        _pool_engine.clear()

    def test_register_populates_engine_ref(self) -> None:
        engine = _make_mock_engine()
        register_pool_engine(engine)
        assert len(_pool_engine) == 1
        assert _pool_engine[0] is engine

    def test_register_replaces_previous(self) -> None:
        register_pool_engine(_make_mock_engine())
        new_engine = _make_mock_engine()
        register_pool_engine(new_engine)
        assert len(_pool_engine) == 1
        assert _pool_engine[0] is new_engine


class TestPoolCallbacksWithoutEngine:
    def setup_method(self):
        _pool_engine.clear()

    def teardown_method(self):
        _pool_engine.clear()

    def test_size_cb_returns_empty(self) -> None:
        assert _pool_size_cb(None) == []

    def test_checked_out_cb_returns_empty(self) -> None:
        assert _pool_checked_out_cb(None) == []

    def test_overflow_cb_returns_empty(self) -> None:
        assert _pool_overflow_cb(None) == []

    def test_checked_in_cb_returns_empty(self) -> None:
        assert _pool_checked_in_cb(None) == []


class TestPoolCallbacksWithEngine:
    def setup_method(self):
        _pool_engine.clear()
        self.engine = _make_mock_engine(
            size=5, checkedout=3, overflow=1, checkedin=2,
        )
        register_pool_engine(self.engine)

    def teardown_method(self):
        _pool_engine.clear()

    def test_size_cb(self) -> None:
        result = _pool_size_cb(None)
        assert len(result) == 1
        assert result[0].value == 5

    def test_checked_out_cb(self) -> None:
        result = _pool_checked_out_cb(None)
        assert len(result) == 1
        assert result[0].value == 3

    def test_overflow_cb(self) -> None:
        result = _pool_overflow_cb(None)
        assert len(result) == 1
        assert result[0].value == 1

    def test_checked_in_cb(self) -> None:
        result = _pool_checked_in_cb(None)
        assert len(result) == 1
        assert result[0].value == 2


class TestCheckoutLatencyInSessionScope:
    @patch("src.db.session.get_session_factory")
    @patch("src.db.session.DB_POOL_CHECKOUT_LATENCY_SECONDS")
    def test_checkout_latency_recorded(self, mock_histogram, mock_factory) -> None:
        mock_session = MagicMock()
        mock_factory.return_value = MagicMock(return_value=mock_session)

        from src.db.session import session_scope

        with session_scope() as session:
            assert session is mock_session

        mock_histogram.record.assert_called_once()
        latency = mock_histogram.record.call_args[0][0]
        assert isinstance(latency, float)
        assert latency >= 0


class TestReplicationLagCallback:
    def setup_method(self):
        metrics._replication_lag_provider = None

    def teardown_method(self):
        metrics._replication_lag_provider = None

    def test_replication_lag_cb_returns_empty_without_provider(self) -> None:
        assert _db_replication_lag_cb(None) == []

    def test_replication_lag_cb_returns_value_when_available(self) -> None:
        register_replication_lag_provider(lambda: 3.25)
        result = _db_replication_lag_cb(None)
        assert len(result) == 1
        assert result[0].value == 3.25

    def test_replication_lag_cb_clamps_negative(self) -> None:
        register_replication_lag_provider(lambda: -1.0)
        result = _db_replication_lag_cb(None)
        assert len(result) == 1
        assert result[0].value == 0.0

    def test_replication_lag_cb_returns_empty_on_none(self) -> None:
        register_replication_lag_provider(lambda: None)
        assert _db_replication_lag_cb(None) == []

    def test_replication_lag_cb_returns_empty_on_exception(self) -> None:
        def _raise() -> float:
            raise RuntimeError("boom")

        register_replication_lag_provider(_raise)
        assert _db_replication_lag_cb(None) == []

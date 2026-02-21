from __future__ import annotations

import pytest

from scripts.e2e.setup_e2e_env import (
    FailFastCheck,
    persist_bad_records_and_enforce_fail_fast,
)
from src.common.rules import RuleDefinition


def _bad_rate_rule(threshold: float) -> RuleDefinition:
    return RuleDefinition.from_dict(
        {
            "rule_id": "silver_bad_records_default",
            "domain": "silver",
            "metric": "bad_records_rate",
            "threshold": threshold,
            "severity_map": {"fail": threshold},
        }
    )


def test_persist_then_failfast_under_threshold() -> None:
    calls: list[tuple[str, int]] = []

    def _persist(records) -> None:
        calls.append(("persist", len(records)))

    rates = persist_bad_records_and_enforce_fail_fast(
        bad_records=[{"reason": "invalid_amount"}, {"reason": "missing_event_time"}],
        persist_bad_records=_persist,
        fail_fast_checks=(
            FailFastCheck(
                table_name="silver.wallet_snapshot",
                valid_count=8,
                bad_count=2,
            ),
            FailFastCheck(
                table_name="silver.ledger_entries",
                valid_count=9,
                bad_count=1,
            ),
        ),
        rule=_bad_rate_rule(0.30),
    )

    assert calls == [("persist", 2)]
    assert rates["silver.wallet_snapshot"] == pytest.approx(0.2)
    assert rates["silver.ledger_entries"] == pytest.approx(0.1)


def test_persist_then_failfast_exceeds_threshold() -> None:
    calls: list[tuple[str, int]] = []

    def _persist(records) -> None:
        calls.append(("persist", len(records)))

    with pytest.raises(RuntimeError):
        persist_bad_records_and_enforce_fail_fast(
            bad_records=[{"reason": "invalid_amount"}],
            persist_bad_records=_persist,
            fail_fast_checks=(
                FailFastCheck(
                    table_name="silver.wallet_snapshot",
                    valid_count=1,
                    bad_count=1,
                ),
            ),
            rule=_bad_rate_rule(0.20),
        )

    # Persistence must happen before fail-fast raises.
    assert calls == [("persist", 1)]


def test_persist_then_failfast_equal_threshold_does_not_raise() -> None:
    calls: list[tuple[str, int]] = []

    def _persist(records) -> None:
        calls.append(("persist", len(records)))

    rates = persist_bad_records_and_enforce_fail_fast(
        bad_records=[{"reason": "missing_user_id"}],
        persist_bad_records=_persist,
        fail_fast_checks=(
            FailFastCheck(
                table_name="silver.wallet_snapshot",
                valid_count=1,
                bad_count=1,
            ),
        ),
        rule=_bad_rate_rule(0.50),
    )

    assert calls == [("persist", 1)]
    assert rates["silver.wallet_snapshot"] == pytest.approx(0.5)

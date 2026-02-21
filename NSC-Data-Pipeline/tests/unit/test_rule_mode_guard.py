from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

from src.common.rule_mode_guard import (
    detect_prod_context_reason,
    enforce_prod_strict_rule_mode,
)


def _write_prod_catalog_config(repo_root: Path, *, catalog: str) -> None:
    configs = repo_root / "configs"
    configs.mkdir(parents=True, exist_ok=True)
    (configs / "prod.yaml").write_text(
        textwrap.dedent(
            f"""\
            databricks:
              catalog: {catalog}
            """
        ),
        encoding="utf-8",
    )


def test_detect_prod_context_reason_from_pipeline_env(tmp_path: Path) -> None:
    _write_prod_catalog_config(tmp_path, catalog="prod_from_config")
    reason = detect_prod_context_reason(
        catalog="dev_catalog",
        repo_root=tmp_path,
        env={"PIPELINE_ENV": "prod"},
    )
    assert reason == "PIPELINE_ENV=prod"


def test_detect_prod_context_reason_from_literal_prod_catalog(tmp_path: Path) -> None:
    _write_prod_catalog_config(tmp_path, catalog="another_prod_catalog")
    reason = detect_prod_context_reason(
        catalog="prod_catalog",
        repo_root=tmp_path,
        env={},
    )
    assert reason == "catalog=prod_catalog"


def test_detect_prod_context_reason_from_literal_prod_catalog_is_case_insensitive(
    tmp_path: Path,
) -> None:
    _write_prod_catalog_config(tmp_path, catalog="another_prod_catalog")
    reason = detect_prod_context_reason(
        catalog="PROD_CATALOG",
        repo_root=tmp_path,
        env={},
    )
    assert reason == "catalog=prod_catalog"


def test_detect_prod_context_reason_from_prod_yaml_catalog(tmp_path: Path) -> None:
    _write_prod_catalog_config(tmp_path, catalog="team_prod_catalog")
    reason = detect_prod_context_reason(
        catalog="team_prod_catalog",
        repo_root=tmp_path,
        env={},
    )
    assert reason == "catalog matches configs/prod.yaml (team_prod_catalog)"


def test_detect_prod_context_reason_from_prod_yaml_catalog_is_case_insensitive(
    tmp_path: Path,
) -> None:
    _write_prod_catalog_config(tmp_path, catalog="team_prod_catalog")
    reason = detect_prod_context_reason(
        catalog="TEAM_PROD_CATALOG",
        repo_root=tmp_path,
        env={},
    )
    assert reason == "catalog matches configs/prod.yaml (team_prod_catalog)"


def test_enforce_prod_strict_rule_mode_blocks_fallback_in_prod(tmp_path: Path) -> None:
    _write_prod_catalog_config(tmp_path, catalog="team_prod_catalog")
    with pytest.raises(RuntimeError, match="required='strict'"):
        enforce_prod_strict_rule_mode(
            pipeline_name="pipeline_a",
            rule_load_mode="fallback",
            catalog="team_prod_catalog",
            repo_root=tmp_path,
        )


def test_enforce_prod_strict_rule_mode_allows_strict_in_prod(tmp_path: Path) -> None:
    _write_prod_catalog_config(tmp_path, catalog="team_prod_catalog")
    enforce_prod_strict_rule_mode(
        pipeline_name="pipeline_b",
        rule_load_mode="strict",
        catalog="team_prod_catalog",
        repo_root=tmp_path,
    )


def test_enforce_prod_strict_rule_mode_allows_non_prod_fallback(tmp_path: Path) -> None:
    _write_prod_catalog_config(tmp_path, catalog="team_prod_catalog")
    enforce_prod_strict_rule_mode(
        pipeline_name="pipeline_silver",
        rule_load_mode="fallback",
        catalog="dev_catalog",
        repo_root=tmp_path,
        env={"PIPELINE_ENV": "dev"},
    )


def test_enforce_prod_strict_rule_mode_error_includes_task(tmp_path: Path) -> None:
    _write_prod_catalog_config(tmp_path, catalog="team_prod_catalog")
    with pytest.raises(RuntimeError, match="task=finalize_success"):
        enforce_prod_strict_rule_mode(
            pipeline_name="pipeline_b",
            task="finalize_success",
            rule_load_mode="fallback",
            catalog="team_prod_catalog",
            repo_root=tmp_path,
        )

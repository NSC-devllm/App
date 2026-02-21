from __future__ import annotations

import os
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import yaml

PROD_ENV_NAME = "prod"
PROD_CATALOG_LITERAL = "prod_catalog"
STRICT_RULE_MODE = "strict"


def _normalize_token(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _load_prod_catalog_from_config(repo_root: Path) -> str | None:
    prod_path = repo_root / "configs" / "prod.yaml"
    if not prod_path.exists():
        return None
    try:
        payload = yaml.safe_load(prod_path.read_text(encoding="utf-8")) or {}
    except Exception as exc:
        raise RuntimeError(f"Failed to read prod config: {prod_path}") from exc

    databricks = payload.get("databricks")
    if not isinstance(databricks, dict):
        return None
    catalog = _normalize_token(databricks.get("catalog"))
    return catalog or None


def detect_prod_context_reason(
    *,
    catalog: str,
    repo_root: Path | str,
    env: Mapping[str, str] | None = None,
) -> str | None:
    env_map = env if env is not None else os.environ

    pipeline_env = _normalize_token(env_map.get("PIPELINE_ENV")).lower()
    if pipeline_env == PROD_ENV_NAME:
        return "PIPELINE_ENV=prod"

    normalized_catalog = _normalize_token(catalog).lower()
    if normalized_catalog == PROD_CATALOG_LITERAL:
        return "catalog=prod_catalog"

    repo_root_path = Path(repo_root)
    prod_catalog = _load_prod_catalog_from_config(repo_root_path)
    normalized_prod_catalog = _normalize_token(prod_catalog).lower()
    if normalized_prod_catalog and normalized_catalog == normalized_prod_catalog:
        return f"catalog matches configs/prod.yaml ({prod_catalog})"

    return None


def enforce_prod_strict_rule_mode(
    *,
    pipeline_name: str,
    rule_load_mode: str,
    catalog: str,
    repo_root: Path | str,
    task: str | None = None,
    env: Mapping[str, str] | None = None,
) -> None:
    normalized_mode = _normalize_token(rule_load_mode).lower()
    if normalized_mode == STRICT_RULE_MODE:
        return

    prod_reason = detect_prod_context_reason(
        catalog=catalog,
        repo_root=repo_root,
        env=env,
    )
    if prod_reason is None:
        return

    task_fragment = f", task={task}" if task else ""
    raise RuntimeError(
        "Blocked non-strict rule_load_mode in prod context: "
        f"pipeline={pipeline_name}{task_fragment}, "
        f"detected_by={prod_reason}, "
        f"rule_load_mode={rule_load_mode!r}, required='strict'"
    )

from __future__ import annotations

import os
import textwrap
from pathlib import Path

import pytest

from src.common.config_loader import (
    _deep_merge,
    find_repo_root,
    get_config_value,
    load_config,
    reset_cache,
)


@pytest.fixture(autouse=True)
def _clear_cache():
    """Reset module-level cache before each test."""
    reset_cache()
    yield
    reset_cache()


@pytest.fixture(autouse=True)
def _clear_pipeline_cfg_env(monkeypatch: pytest.MonkeyPatch):
    for key in list(os.environ.keys()):
        if key.startswith("PIPELINE_CFG__"):
            monkeypatch.delenv(key, raising=False)


@pytest.fixture()
def config_tree(tmp_path: Path) -> Path:
    """Create a minimal configs directory with common + dev + prod yamls."""
    configs = tmp_path / "configs"
    configs.mkdir()

    (configs / "common.yaml").write_text(
        textwrap.dedent("""\
        project:
          name: test-pipeline
        databricks:
          catalog: null
        analytics:
          secret_scope: common-scope
          secret_key: common-key
        flags:
          enabled: true
        numeric:
          int_value: 1
          float_value: 1.5
        optional:
          maybe: present
        """)
    )
    (configs / "dev.yaml").write_text(
        textwrap.dedent("""\
        databricks:
          catalog: dev-catalog
        """)
    )
    (configs / "prod.yaml").write_text(
        textwrap.dedent("""\
        databricks:
          catalog: prod-catalog
        analytics:
          secret_scope: prod-scope
        """)
    )

    # Marker directories so find_repo_root can detect this as repo root.
    (tmp_path / "src").mkdir()
    (tmp_path / "mock_data").mkdir()

    return tmp_path


# ---- _deep_merge ----


def test_deep_merge_simple():
    base = {"a": 1, "b": {"x": 10}}
    override = {"b": {"y": 20}, "c": 3}
    result = _deep_merge(base, override)
    assert result == {"a": 1, "b": {"x": 10, "y": 20}, "c": 3}


def test_deep_merge_override_value():
    base = {"a": {"nested": "old"}}
    override = {"a": {"nested": "new"}}
    assert _deep_merge(base, override) == {"a": {"nested": "new"}}


# ---- load_config ----


def test_load_config_dev(config_tree: Path):
    cfg = load_config(env="dev", repo_root=config_tree)
    assert cfg["databricks"]["catalog"] == "dev-catalog"
    assert cfg["project"]["name"] == "test-pipeline"
    # common.yaml analytics keys preserved
    assert cfg["analytics"]["secret_scope"] == "common-scope"


def test_load_config_prod(config_tree: Path):
    cfg = load_config(env="prod", repo_root=config_tree)
    assert cfg["databricks"]["catalog"] == "prod-catalog"
    # prod.yaml overrides analytics.secret_scope
    assert cfg["analytics"]["secret_scope"] == "prod-scope"
    # common.yaml analytics.secret_key preserved
    assert cfg["analytics"]["secret_key"] == "common-key"


def test_load_config_missing_env_yaml_uses_common_only(config_tree: Path):
    cfg = load_config(env="staging", repo_root=config_tree)
    assert cfg["databricks"]["catalog"] is None
    assert cfg["project"]["name"] == "test-pipeline"


def test_load_config_missing_common_yaml_raises(tmp_path: Path):
    (tmp_path / "configs").mkdir()
    with pytest.raises(FileNotFoundError, match="common.yaml"):
        load_config(env="dev", repo_root=tmp_path)


def test_load_config_caches_result(config_tree: Path):
    cfg1 = load_config(env="dev", repo_root=config_tree)
    cfg2 = load_config(env="prod", repo_root=config_tree)
    # Second call returns cached result (ignores different env).
    assert cfg1 is cfg2


# ---- get_config_value ----


def test_get_config_value_dot_notation(config_tree: Path):
    load_config(env="dev", repo_root=config_tree)
    assert get_config_value("databricks.catalog") == "dev-catalog"
    assert get_config_value("analytics.secret_scope") == "common-scope"


def test_get_config_value_top_level(config_tree: Path):
    load_config(env="dev", repo_root=config_tree)
    result = get_config_value("project")
    assert isinstance(result, dict)
    assert result["name"] == "test-pipeline"


def test_get_config_value_missing_key_raises(config_tree: Path):
    load_config(env="dev", repo_root=config_tree)
    with pytest.raises(KeyError, match="nonexistent.key"):
        get_config_value("nonexistent.key")


def test_get_config_value_missing_key_with_default(config_tree: Path):
    load_config(env="dev", repo_root=config_tree)
    assert get_config_value("nonexistent.key", "fallback") == "fallback"
    assert get_config_value("nonexistent.key", None) is None


# ---- find_repo_root ----


def test_find_repo_root_returns_path():
    root = find_repo_root()
    assert isinstance(root, Path)
    # The actual repo root should contain src/ and configs/
    assert (root / "src").is_dir()


# ---- PIPELINE_ENV ----


def test_load_config_reads_pipeline_env(config_tree: Path, monkeypatch):
    monkeypatch.setenv("PIPELINE_ENV", "prod")
    cfg = load_config(repo_root=config_tree)
    assert cfg["databricks"]["catalog"] == "prod-catalog"


# ---- find_repo_root with PIPELINE_ROOT env var ----


def test_find_repo_root_uses_pipeline_root_env(tmp_path: Path, monkeypatch):
    """PIPELINE_ROOT env var is used when cwd/argv have no marker dirs."""
    target = tmp_path / "custom_root"
    target.mkdir()
    (target / "src").mkdir()
    (target / "mock_data").mkdir()
    # Ensure cwd and argv[0] do not contain marker dirs.
    neutral = tmp_path / "neutral"
    neutral.mkdir()
    monkeypatch.chdir(neutral)
    monkeypatch.setattr("sys.argv", [str(neutral / "script.py")])
    monkeypatch.setenv("PIPELINE_ROOT", str(target))
    root = find_repo_root()
    assert root == target


# ---- load_config edge cases ----


def test_load_config_empty_env_yaml(config_tree: Path):
    """Empty env YAML should not break loading (treated as empty dict)."""
    (config_tree / "configs" / "empty.yaml").write_text("")
    cfg = load_config(env="empty", repo_root=config_tree)
    # Falls back to common.yaml values only.
    assert cfg["databricks"]["catalog"] is None
    assert cfg["project"]["name"] == "test-pipeline"


# ---- get_config_value edge cases ----


def test_get_config_value_nested_missing_mid_path(config_tree: Path):
    """Key path fails mid-traversal when an intermediate value is not a dict."""
    load_config(env="dev", repo_root=config_tree)
    # "project.name" is a string; "project.name.sub" tries to index into it.
    with pytest.raises(KeyError, match="project.name.sub"):
        get_config_value("project.name.sub")


def test_get_config_value_nested_missing_mid_path_with_default(config_tree: Path):
    load_config(env="dev", repo_root=config_tree)
    assert get_config_value("project.name.sub", "safe") == "safe"


def test_get_config_value_null_value_returned(config_tree: Path):
    """Config key with explicit null should return None, not fallback."""
    load_config(env="dev", repo_root=config_tree)
    # databricks.catalog is null in common.yaml but overridden to "dev-catalog"
    # in dev.yaml. Use staging env to test null pass-through.
    reset_cache()
    load_config(env="staging", repo_root=config_tree)
    assert get_config_value("databricks.catalog") is None


# ---- env overrides ----


def test_load_config_applies_env_overrides(config_tree: Path, monkeypatch):
    monkeypatch.setenv("PIPELINE_CFG__DATABRICKS__CATALOG", "env-catalog")
    monkeypatch.setenv("PIPELINE_CFG__FLAGS__ENABLED", "false")
    monkeypatch.setenv("PIPELINE_CFG__NUMERIC__INT_VALUE", "42")
    monkeypatch.setenv("PIPELINE_CFG__NUMERIC__FLOAT_VALUE", "2.75")
    monkeypatch.setenv("PIPELINE_CFG__OPTIONAL__MAYBE", "null")

    cfg = load_config(env="dev", repo_root=config_tree)
    assert cfg["databricks"]["catalog"] == "env-catalog"
    assert cfg["flags"]["enabled"] is False
    assert cfg["numeric"]["int_value"] == 42
    assert cfg["numeric"]["float_value"] == pytest.approx(2.75)
    assert cfg["optional"]["maybe"] is None


def test_load_config_env_override_unknown_key_raises(config_tree: Path, monkeypatch):
    monkeypatch.setenv("PIPELINE_CFG__NOT__EXIST", "1")
    with pytest.raises(KeyError, match="not.exist"):
        load_config(env="dev", repo_root=config_tree)


def test_load_config_ignores_non_matching_env_prefix(config_tree: Path, monkeypatch):
    monkeypatch.setenv("PIPELINE_CF__PROJECT__NAME", "ignored")
    cfg = load_config(env="dev", repo_root=config_tree)
    assert cfg["project"]["name"] == "test-pipeline"

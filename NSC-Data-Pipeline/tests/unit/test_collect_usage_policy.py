from __future__ import annotations

import ast
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
RUNTIME_FILES = (
    "scripts/run_pipeline_a.py",
    "scripts/run_pipeline_b.py",
    "scripts/run_pipeline_c.py",
    "scripts/run_pipeline_silver.py",
    "src/io/rule_loader.py",
    "src/io/upstream_readiness.py",
    "src/io/gold_io.py",
)


def _collect_call_lines(module_path: Path) -> list[int]:
    tree = ast.parse(module_path.read_text(encoding="utf-8"))
    lines: list[int] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            if node.func.attr == "collect":
                lines.append(node.lineno)
    return sorted(lines)


def test_runtime_collect_calls_use_safety_wrapper() -> None:
    violations: list[str] = []
    for relative_path in RUNTIME_FILES:
        module_path = REPO_ROOT / relative_path
        call_lines = _collect_call_lines(module_path)
        if call_lines:
            violations.append(
                f"{relative_path}:{', '.join(str(line) for line in call_lines)}"
            )

    assert not violations, (
        "Direct DataFrame.collect() is not allowed in runtime paths. "
        "Use src.io.spark_safety.safe_collect with explicit max_rows.\n"
        + "\n".join(violations)
    )

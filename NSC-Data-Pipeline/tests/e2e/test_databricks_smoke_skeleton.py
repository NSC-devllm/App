from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest

REQUIRED_ENV = (
    "DATABRICKS_HOST",
    "DATABRICKS_TOKEN",
    "DATABRICKS_PIPELINE_A_JOB_ID",
    "DATABRICKS_PIPELINE_B_JOB_ID",
    "DATABRICKS_PIPELINE_C_JOB_ID",
)


def _missing_env() -> list[str]:
    return [name for name in REQUIRED_ENV if not os.environ.get(name)]


@pytest.mark.e2e
def test_databricks_smoke_skeleton() -> None:
    missing = _missing_env()
    if missing:
        pytest.skip(f"Missing E2E env vars: {', '.join(missing)}")

    command = [
        "bash",
        str(Path("scripts/e2e/run_databricks_smoke.sh")),
    ]
    completed = subprocess.run(
        command,
        check=False,
        text=True,
        capture_output=True,
    )
    if completed.returncode != 0:
        pytest.fail(
            "Databricks smoke skeleton failed.\n"
            f"stdout:\n{completed.stdout}\n"
            f"stderr:\n{completed.stderr}"
        )

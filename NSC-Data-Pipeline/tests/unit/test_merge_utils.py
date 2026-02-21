from __future__ import annotations

import pytest

from src.io.merge_utils import build_merge_condition


def test_build_merge_condition() -> None:
    condition = build_merge_condition(["a", "b"])
    assert condition == "target.a = source.a AND target.b = source.b"


def test_build_merge_condition_requires_keys() -> None:
    with pytest.raises(ValueError):
        build_merge_condition([])

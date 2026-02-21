from __future__ import annotations

# ruff: noqa: E402
import pytest

pyspark_types = pytest.importorskip("pyspark.sql.types")
ArrayType = pyspark_types.ArrayType
MapType = pyspark_types.MapType

from scripts.e2e.setup_e2e_env import _contract_schema as setup_contract_schema
from scripts.sync_dim_rule_scd2 import _contract_schema as sync_contract_schema
from src.common.contracts import get_contract


def test_sync_schema_parses_dim_rule_complex_types() -> None:
    contract = get_contract("gold.dim_rule_scd2")
    schema = sync_contract_schema(contract)

    severity_map_type = schema["severity_map"].dataType
    allowed_values_type = schema["allowed_values"].dataType

    assert isinstance(severity_map_type, MapType)
    assert isinstance(allowed_values_type, ArrayType)


def test_setup_schema_parses_dim_rule_complex_types() -> None:
    contract = get_contract("gold.dim_rule_scd2")
    schema = setup_contract_schema(contract)

    severity_map_type = schema["severity_map"].dataType
    allowed_values_type = schema["allowed_values"].dataType

    assert isinstance(severity_map_type, MapType)
    assert isinstance(allowed_values_type, ArrayType)

"""Schema management and validation package."""

from __future__ import annotations

from datafusion_engine.schema import registry
from datafusion_engine.schema.registry import (
    extract_base_schema_for,
    extract_base_schema_names,
    extract_nested_schema_for,
    extract_nested_schema_names,
    extract_schema_contract_for,
    extract_schema_for,
    schema_contract_for_table,
)

__all__ = [
    "extract_base_schema_for",
    "extract_base_schema_names",
    "extract_nested_schema_for",
    "extract_nested_schema_names",
    "extract_schema_contract_for",
    "extract_schema_for",
    "registry",
    "schema_contract_for_table",
]

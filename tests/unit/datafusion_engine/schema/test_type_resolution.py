"""Unit tests for Arrow type resolution helpers."""

from __future__ import annotations

import pyarrow as pa
import pytest

from datafusion_engine.schema.type_resolution import (
    arrow_type_to_sql,
    ensure_arrow_dtype,
    resolve_arrow_type,
)
from schema_spec.arrow_types import ArrowPrimitiveSpec


def test_resolve_arrow_type_handles_aliases_and_map_types() -> None:
    """Type hints resolve to expected Arrow aliases and map types."""
    assert resolve_arrow_type("string") == pa.string()
    assert resolve_arrow_type(" map<string, string> ") == pa.map_(pa.string(), pa.string())
    assert resolve_arrow_type("int") == pa.int64()


def test_resolve_arrow_type_rejects_unknown_hints() -> None:
    """Unknown type hints raise ``ValueError``."""
    with pytest.raises(ValueError, match="Unsupported type hint"):
        resolve_arrow_type("not_a_type")


def test_arrow_type_to_sql_handles_native_and_nested_storage_types() -> None:
    """SQL labels resolve for native and storage-backed Arrow types."""
    assert arrow_type_to_sql(pa.int64()) == "Int64"
    assert arrow_type_to_sql(pa.dictionary(pa.int32(), pa.string())) == "Utf8"


def test_ensure_arrow_dtype_accepts_arrow_spec_variants() -> None:
    """Dtype normalization accepts native Arrow and spec-based values."""
    assert ensure_arrow_dtype(pa.string()) == pa.string()
    assert ensure_arrow_dtype(ArrowPrimitiveSpec(name="string")) == pa.string()

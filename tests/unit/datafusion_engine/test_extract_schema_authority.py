"""Tests for extract schema authority resolution policy."""

from __future__ import annotations

import pyarrow as pa
import pytest

from datafusion_engine.schema import extraction_schemas as _extraction_mod
from datafusion_engine.schema import registry as schema_registry


def test_extract_schema_for_prefers_derived_when_shape_is_compatible(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Derived schemas should be returned when available."""
    derived = pa.schema([pa.field("file_id", pa.int64())])

    monkeypatch.setattr(_extraction_mod, "_derived_extract_schema_for", lambda _name: derived)

    result = schema_registry.extract_schema_for("repo_files_v1")
    assert result == derived


def test_extract_schema_for_raises_when_derived_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Missing derived schemas should raise instead of static fallback."""
    monkeypatch.setattr(_extraction_mod, "_derived_extract_schema_for", lambda _name: None)

    with pytest.raises(KeyError, match="No derived extract schema available"):
        schema_registry.extract_schema_for("repo_files_v1")


def test_extract_schema_for_resolves_nested_dataset_names() -> None:
    """Nested dataset lookup should resolve through authority policy."""
    schema = schema_registry.extract_schema_for("ast_nodes")
    assert isinstance(schema, pa.Schema)
    assert "file_id" in schema.names


def test_nested_row_schema_authority_prefers_derived_on_conflict() -> None:
    """Conflict resolution should deterministically favor derived schema."""
    derived = pa.schema([pa.field("node_id", pa.string()), pa.field("kind", pa.string())])
    struct = pa.schema([pa.field("id", pa.string()), pa.field("type", pa.string())])

    resolve_nested_row_schema_authority = schema_registry._resolve_nested_row_schema_authority  # noqa: SLF001
    resolved = resolve_nested_row_schema_authority(
        dataset_name="ast_nodes",
        nested_path="nodes",
        derived_schema=derived,
        struct_schema=struct,
    )

    assert resolved == derived


def test_nested_row_schema_authority_uses_struct_when_derived_missing() -> None:
    """Struct-derived schema should be used when metadata shape is absent."""
    struct = pa.schema([pa.field("id", pa.string()), pa.field("type", pa.string())])

    resolve_nested_row_schema_authority = schema_registry._resolve_nested_row_schema_authority  # noqa: SLF001
    resolved = resolve_nested_row_schema_authority(
        dataset_name="ast_nodes",
        nested_path="nodes",
        derived_schema=None,
        struct_schema=struct,
    )

    assert resolved == struct

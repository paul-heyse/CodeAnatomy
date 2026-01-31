"""Tests for semantics.adapters module."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pyarrow as pa
import pytest
from datafusion import SessionContext

from semantics.adapters import (
    DEFAULT_CONFIDENCE,
    DEFAULT_SCORE,
    DEFAULT_TASK_PRIORITY,
    REL_DEF_SYMBOL_OPTIONS,
    REL_NAME_SYMBOL_OPTIONS,
    RelationshipProjectionOptions,
    legacy_relationship_projection,
    legacy_relationship_projection_extended,
    project_semantic_to_legacy,
)

if TYPE_CHECKING:
    from datafusion.dataframe import DataFrame


@pytest.fixture
def semantic_df() -> DataFrame:
    """Create a DataFrame with semantic relationship schema.

    Returns
    -------
    DataFrame
        DataFrame with semantic output columns.
    """
    ctx = SessionContext()
    schema = pa.schema(
        [
            pa.field("entity_id", pa.string()),
            pa.field("symbol", pa.string()),
            pa.field("path", pa.string()),
            pa.field("bstart", pa.int64()),
            pa.field("bend", pa.int64()),
            pa.field("origin", pa.string()),
        ]
    )
    data = pa.table(
        {
            "entity_id": ["ref_001", "ref_002"],
            "symbol": ["foo", "bar"],
            "path": ["src/main.py", "src/main.py"],
            "bstart": [100, 200],
            "bend": [105, 210],
            "origin": ["cst_ref", "cst_ref"],
        },
        schema=schema,
    )
    ctx.register_record_batches("semantic_output", [data.to_batches()])
    return ctx.table("semantic_output")


def test_relationship_projection_options_defaults() -> None:
    """RelationshipProjectionOptions has sensible defaults."""
    options = RelationshipProjectionOptions(
        entity_id_alias="ref_id",
        edge_kind="py_references_symbol",
        task_name="rel.name_symbol",
    )
    assert options.task_priority == DEFAULT_TASK_PRIORITY
    assert options.confidence == DEFAULT_CONFIDENCE
    assert options.score == DEFAULT_SCORE
    assert options.include_extended_columns is False


def test_legacy_projection_column_names(semantic_df: DataFrame) -> None:
    """legacy_relationship_projection produces expected column names."""
    projected = legacy_relationship_projection(semantic_df, REL_NAME_SYMBOL_OPTIONS)
    schema = projected.schema()
    names = schema.names if hasattr(schema, "names") else [f.name for f in schema]

    expected = [
        "ref_id",
        "symbol",
        "symbol_roles",
        "path",
        "edge_owner_file_id",
        "bstart",
        "bend",
        "resolution_method",
        "confidence",
        "score",
        "task_name",
        "task_priority",
    ]
    assert list(names) == expected


def test_legacy_projection_entity_id_mapping(semantic_df: DataFrame) -> None:
    """entity_id is mapped to the specified alias."""
    options = RelationshipProjectionOptions(
        entity_id_alias="def_id",
        edge_kind="py_defines_symbol",
        task_name="rel.def_symbol",
    )
    projected = legacy_relationship_projection(semantic_df, options)
    schema = projected.schema()
    names = schema.names if hasattr(schema, "names") else [f.name for f in schema]

    assert "def_id" in names
    assert "entity_id" not in names


def test_legacy_projection_origin_mapped_to_resolution_method(
    semantic_df: DataFrame,
) -> None:
    """Origin column is mapped to resolution_method."""
    projected = legacy_relationship_projection(semantic_df, REL_NAME_SYMBOL_OPTIONS)
    result = projected.collect()

    # Check that resolution_method contains the origin values
    resolution_methods = result[0].column("resolution_method").to_pylist()
    assert "cst_ref" in resolution_methods


def test_legacy_projection_adds_null_symbol_roles(semantic_df: DataFrame) -> None:
    """symbol_roles column is added with null values."""
    projected = legacy_relationship_projection(semantic_df, REL_NAME_SYMBOL_OPTIONS)
    result = projected.collect()

    symbol_roles = result[0].column("symbol_roles").to_pylist()
    assert all(v is None for v in symbol_roles)


def test_legacy_projection_adds_metadata_columns(semantic_df: DataFrame) -> None:
    """confidence, score, task_name, task_priority are added."""
    projected = legacy_relationship_projection(semantic_df, REL_NAME_SYMBOL_OPTIONS)
    result = projected.collect()

    confidences = result[0].column("confidence").to_pylist()
    scores = result[0].column("score").to_pylist()
    task_names = result[0].column("task_name").to_pylist()
    task_priorities = result[0].column("task_priority").to_pylist()

    assert all(c == DEFAULT_CONFIDENCE for c in confidences)
    assert all(s == DEFAULT_SCORE for s in scores)
    assert all(t == "rel.name_symbol" for t in task_names)
    assert all(p == DEFAULT_TASK_PRIORITY for p in task_priorities)


def test_extended_projection_includes_extra_columns(semantic_df: DataFrame) -> None:
    """Extended projection includes binding_kind, def_site_kind, etc."""
    projected = legacy_relationship_projection_extended(
        semantic_df,
        REL_NAME_SYMBOL_OPTIONS,
    )
    schema = projected.schema()
    names = schema.names if hasattr(schema, "names") else [f.name for f in schema]

    extended_cols = [
        "binding_kind",
        "def_site_kind",
        "use_kind",
        "reason",
        "diag_source",
        "severity",
    ]
    for col in extended_cols:
        assert col in names


def test_extended_projection_includes_edge_kind(semantic_df: DataFrame) -> None:
    """Extended projection includes edge_kind column."""
    projected = legacy_relationship_projection_extended(
        semantic_df,
        REL_NAME_SYMBOL_OPTIONS,
    )
    result = projected.collect()

    edge_kinds = result[0].column("edge_kind").to_pylist()
    assert all(k == "py_references_symbol" for k in edge_kinds)


def test_project_semantic_to_legacy_dispatches_basic(semantic_df: DataFrame) -> None:
    """project_semantic_to_legacy uses basic projection by default."""
    projected = project_semantic_to_legacy(semantic_df, REL_NAME_SYMBOL_OPTIONS)
    schema = projected.schema()
    names = schema.names if hasattr(schema, "names") else [f.name for f in schema]

    # Basic projection should not include extended columns
    assert "binding_kind" not in names


def test_project_semantic_to_legacy_dispatches_extended(
    semantic_df: DataFrame,
) -> None:
    """project_semantic_to_legacy uses extended projection when requested."""
    options = RelationshipProjectionOptions(
        entity_id_alias="ref_id",
        edge_kind="py_references_symbol",
        task_name="rel.name_symbol",
        include_extended_columns=True,
    )
    projected = project_semantic_to_legacy(semantic_df, options)
    schema = projected.schema()
    names = schema.names if hasattr(schema, "names") else [f.name for f in schema]

    # Extended projection should include extended columns
    assert "binding_kind" in names


def test_def_symbol_options_has_higher_confidence() -> None:
    """REL_DEF_SYMBOL_OPTIONS has higher confidence/score."""
    assert REL_DEF_SYMBOL_OPTIONS.confidence == 0.6
    assert REL_DEF_SYMBOL_OPTIONS.score == 0.6
    assert REL_DEF_SYMBOL_OPTIONS.entity_id_alias == "def_id"

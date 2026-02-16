"""Tests for semantic diagnostics helpers."""

from __future__ import annotations

from datafusion import SessionContext

from semantics.diagnostics import dataframe_row_count, semantic_quality_issue_batches

SAMPLE_ROW_COUNT = 3


def test_dataframe_row_count() -> None:
    """Row counts should reflect DataFrame cardinality."""
    ctx = SessionContext()
    ctx.from_pydict({"value": [1, 2, 3]}, name="sample")
    df = ctx.table("sample")
    assert dataframe_row_count(df) == SAMPLE_ROW_COUNT


def test_semantic_quality_issue_batches_file_quality() -> None:
    """File quality issues should surface low quality scores."""
    ctx = SessionContext()
    ctx.from_pydict(
        {
            "file_id": ["file_a", "file_b"],
            "file_quality_score": [750.0, 900.0],
        },
        name="file_quality",
    )
    batches = semantic_quality_issue_batches(
        view_name="file_quality",
        df=ctx.table("file_quality"),
        max_rows=10,
    )
    assert len(batches) == 1
    batch = batches[0]
    assert batch.issue_kind == "file_quality_score_below_800"
    assert len(batch.rows) == 1
    row = batch.rows[0]
    assert row["entity_id"] == "file_a"
    assert row["entity_kind"] == "file"
    assert row["issue"] == "file_quality_score_below_800"
    assert row["source_table"] == "file_quality"


def test_semantic_quality_issue_batches_relationship_ambiguity() -> None:
    """Relationship ambiguity issues should surface ambiguous sources."""
    ctx = SessionContext()
    ctx.from_pydict(
        {
            "relationship_name": ["rel_name_symbol"],
            "ambiguous_sources": [2],
        },
        name="relationship_ambiguity_report",
    )
    batches = semantic_quality_issue_batches(
        view_name="relationship_ambiguity_report",
        df=ctx.table("relationship_ambiguity_report"),
        max_rows=5,
    )
    assert len(batches) == 1
    batch = batches[0]
    assert batch.issue_kind == "ambiguous_sources"
    assert batch.rows[0]["entity_id"] == "rel_name_symbol"

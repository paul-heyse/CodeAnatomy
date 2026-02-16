"""Tests for SemanticExprBuilder extraction."""

from __future__ import annotations

from datafusion import SessionContext

from semantics.expr_builder import SemanticExprBuilder
from semantics.schema import SemanticSchema


def _schema() -> SemanticSchema:
    ctx = SessionContext()
    ctx.from_pydict(
        {
            "path": ["a.py"],
            "bstart": [1],
            "bend": [2],
            "entity_id": ["e1"],
            "symbol": ["sym"],
        },
        name="rows",
    )
    return SemanticSchema.from_df(ctx.table("rows"), table_name="rows")


def test_expr_builder_generates_core_expressions() -> None:
    """SemanticExprBuilder generates stable core expressions."""
    builder = SemanticExprBuilder(_schema())

    assert "path" in str(builder.path_col())
    assert "bstart" in str(builder.span_start_col())
    assert "bend" in str(builder.span_end_col())
    assert "span_make" in str(builder.span_expr())
    assert "stable_id" in str(builder.entity_id_expr("node"))

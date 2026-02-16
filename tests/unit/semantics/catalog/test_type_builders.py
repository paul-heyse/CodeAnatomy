"""Tests for type analysis builder module."""

from __future__ import annotations

from datafusion import SessionContext

from semantics.catalog.type_builders import type_exprs_df_builder


def test_type_exprs_builder_generates_ids() -> None:
    """Type-expression builder generates type_expr_id and type_id."""
    ctx = SessionContext()
    ctx.from_pydict(
        {
            "file_id": ["f1"],
            "path": ["a.py"],
            "bstart": [10],
            "bend": [11],
            "expr_text": ["int"],
            "expr_kind": ["Name"],
            "expr_role": ["annotation"],
            "owner_def_id": ["d1"],
        },
        name="cst_type_exprs",
    )

    df = type_exprs_df_builder(ctx)

    cols = set(df.schema().names)
    assert "type_expr_id" in cols
    assert "type_id" in cols

"""Tests for extracted quality compiler module."""

from __future__ import annotations

from datafusion import SessionContext

from semantics.compiler import SemanticCompiler
from semantics.exprs import c, eq
from semantics.ir import SemanticIRJoinGroup
from semantics.quality import Feature, HardPredicate, QualityRelationshipSpec, SignalsSpec
from semantics.quality_compiler import build_join_group, compile_relationship_with_quality
from semantics.table_registry import TableRegistry


def _seed_tables(ctx: SessionContext) -> None:
    ctx.from_pydict(
        {
            "file_id": ["f1"],
            "path": ["a.py"],
            "bstart": [10],
            "bend": [11],
            "entity_id": ["e1"],
        },
        name="left_table",
    )
    ctx.from_pydict(
        {
            "file_id": ["f1"],
            "symbol": ["sym"],
            "bstart": [10],
            "bend": [11],
        },
        name="right_table",
    )


def test_build_join_group_returns_prefixed_join() -> None:
    """Join-group helper builds prefixed joined rows."""
    ctx = SessionContext()
    _seed_tables(ctx)
    compiler = SemanticCompiler(ctx, table_registry=TableRegistry())

    joined = build_join_group(
        compiler,
        SemanticIRJoinGroup(
            name="join_group",
            left_view="left_table",
            right_view="right_table",
            left_on=("file_id",),
            right_on=("file_id",),
            how="inner",
            relationship_names=("rel",),
        ),
    )

    cols = set(joined.schema().names)
    assert "l__file_id" in cols
    assert "r__file_id" in cols


def test_compile_relationship_with_quality_returns_confidence_and_score() -> None:
    """Quality compiler computes score/confidence output columns."""
    ctx = SessionContext()
    _seed_tables(ctx)

    spec = QualityRelationshipSpec(
        name="rel_quality",
        left_view="left_table",
        right_view="right_table",
        left_on=["file_id"],
        right_on=["file_id"],
        how="inner",
        signals=SignalsSpec(
            hard=[HardPredicate(eq("l__bstart", "r__bstart"))],
            features=[Feature("span_match", c("l__bstart"), weight=1.0)],
        ),
    )

    result = compile_relationship_with_quality(
        SemanticCompiler(ctx, table_registry=TableRegistry()),
        spec,
    )

    cols = set(result.schema().names)
    assert "score" in cols
    assert "confidence" in cols

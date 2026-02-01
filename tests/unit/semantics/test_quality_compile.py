"""Integration tests for quality-aware relationship compilation."""

from __future__ import annotations

from datafusion import SessionContext

from semantics.compiler import SemanticCompiler
from semantics.exprs import c, case_eq, eq, eq_value
from semantics.quality import (
    Feature,
    HardPredicate,
    OrderSpec,
    QualityRelationshipSpec,
    RankSpec,
    SelectExpr,
    SignalsSpec,
)


def test_compile_relationship_with_quality_computes_scores() -> None:
    """Quality compilation computes confidence and score columns."""
    ctx = SessionContext()
    ctx.from_pydict(
        {
            "file_id": ["file_a"],
            "path": ["src/main.py"],
            "bstart": [10],
            "bend": [15],
            "entity_id": ["ref_1"],
        },
        name="left_table",
    )
    ctx.from_pydict(
        {
            "file_id": ["file_a"],
            "symbol": ["sym"],
            "bstart": [10],
            "bend": [15],
            "is_read": [True],
        },
        name="right_table",
    )
    ctx.from_pydict(
        {
            "file_id": ["file_a"],
            "file_quality_score": [900.0],
        },
        name="file_quality_v1",
    )

    spec = QualityRelationshipSpec(
        name="rel_test_quality_v1",
        left_view="left_table",
        right_view="right_table",
        left_on=["file_id"],
        right_on=["file_id"],
        how="inner",
        provider="scip",
        origin="test_origin",
        signals=SignalsSpec(
            base_score=1000.0,
            base_confidence=0.5,
            hard=[
                HardPredicate(eq("l__bstart", "r__bstart")),
                HardPredicate(eq_value("r__is_read", value=True)),
            ],
            features=[
                Feature("exact_span", case_eq("l__bstart", "r__bstart"), weight=10.0),
            ],
        ),
        rank=RankSpec(
            ambiguity_key_expr=c("l__entity_id"),
            order_by=[OrderSpec(c("score"), direction="desc")],
            keep="best",
            top_k=1,
        ),
        select_exprs=[
            SelectExpr(c("l__entity_id"), "entity_id"),
            SelectExpr(c("r__symbol"), "symbol"),
            SelectExpr(c("l__path"), "path"),
            SelectExpr(c("l__bstart"), "bstart"),
            SelectExpr(c("l__bend"), "bend"),
        ],
    )

    df = SemanticCompiler(ctx).compile_relationship_with_quality(
        spec,
        file_quality_df=ctx.table("file_quality_v1"),
    )
    result = df.collect()[0]
    scores = result["score"].to_pylist()
    confidences = result["confidence"].to_pylist()
    assert scores == [1010.0]
    assert 0.0 <= confidences[0] <= 1.0

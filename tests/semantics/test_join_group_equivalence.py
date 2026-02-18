"""Join group equivalence tests for semantic compiler."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pyarrow as pa
from datafusion import SessionContext, col

from semantics.compiler import SemanticCompiler
from semantics.exprs import c, eq, v
from semantics.ir import SemanticIRJoinGroup
from semantics.quality import (
    HardPredicate,
    OrderSpec,
    QualityRelationshipSpec,
    RankSpec,
    SelectExpr,
    SignalsSpec,
)
from semantics.table_registry import TableRegistry

if TYPE_CHECKING:
    from datafusion.dataframe import DataFrame


def _collect_rows(df: DataFrame) -> list[dict[str, object]]:
    batches = df.collect()
    table = pa.Table.from_batches(batches)
    return table.to_pylist()


def test_join_group_equivalence() -> None:
    """Join group compilation matches direct relationship compilation."""
    ctx = SessionContext()
    ctx.from_pydict(
        {
            "file_id": ["file_a", "file_a", "file_b"],
            "entity_id": ["left_1", "left_2", "left_3"],
            "owner_def_id": ["def_1", "def_2", "def_3"],
        },
        name="left_view",
    )
    ctx.from_pydict(
        {
            "file_id": ["file_a", "file_a", "file_b"],
            "entity_id": ["def_1", "def_2", "def_3"],
        },
        name="right_view",
    )

    spec = QualityRelationshipSpec(
        name="rel_join_group_test_v1",
        left_view="left_view",
        right_view="right_view",
        left_on=["file_id"],
        right_on=["file_id"],
        how="inner",
        provider="tests",
        origin="tests",
        join_file_quality=False,
        signals=SignalsSpec(
            base_score=100.0,
            base_confidence=0.9,
            hard=[HardPredicate(eq("l__owner_def_id", "r__entity_id"))],
        ),
        rank=RankSpec(
            ambiguity_key_expr=c("l__entity_id"),
            order_by=[OrderSpec(c("score"), direction="desc")],
            keep="best",
            top_k=1,
        ),
        select_exprs=[
            SelectExpr(c("l__entity_id"), "src"),
            SelectExpr(c("r__entity_id"), "dst"),
            SelectExpr(v("rel_test"), "kind"),
        ],
    )

    compiler = SemanticCompiler(ctx, table_registry=TableRegistry())
    direct_df = compiler.compile_relationship_with_quality(spec)
    join_group = SemanticIRJoinGroup(
        name="join_group_test",
        left_view="left_view",
        right_view="right_view",
        left_on=("file_id",),
        right_on=("file_id",),
        how="inner",
        relationship_names=(spec.name,),
    )
    joined_df = compiler.build_join_group(join_group)
    grouped_df = compiler.compile_relationship_from_join(joined_df, spec)

    direct_rows = _collect_rows(direct_df.sort(col("src"), col("dst")))
    grouped_rows = _collect_rows(grouped_df.sort(col("src"), col("dst")))
    assert direct_rows == grouped_rows

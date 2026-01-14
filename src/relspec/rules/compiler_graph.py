"""Central graph plan utilities for rule domains."""

from __future__ import annotations

from collections.abc import Sequence

from arrowdsl.core.context import ExecutionContext
from relspec.compiler import RelationshipRuleCompiler
from relspec.compiler_graph import GraphPlan as RelationshipGraphPlan
from relspec.compiler_graph import compile_graph_plan as compile_relationship_graph
from relspec.model import RelationshipRule
from relspec.rules.evidence import EvidenceCatalog


def compile_cpg_graph_plan(
    rules: Sequence[RelationshipRule],
    *,
    ctx: ExecutionContext,
    compiler: RelationshipRuleCompiler,
    evidence: EvidenceCatalog,
) -> RelationshipGraphPlan:
    """Compile a graph-level plan for relationship rules.

    Returns
    -------
    RelationshipGraphPlan
        Graph plan for relationship rules.
    """
    return compile_relationship_graph(rules, ctx=ctx, compiler=compiler, evidence=evidence)


__all__ = ["RelationshipGraphPlan", "compile_cpg_graph_plan"]

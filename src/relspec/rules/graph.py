"""Compatibility wrapper for relspec graph helpers."""

from relspec.graph import (
    GraphPlan,
    RuleSelectors,
    compile_union_graph_plan,
    order_rules_by_evidence,
    rule_graph_signature,
)

compile_graph_plan = compile_union_graph_plan

__all__ = [
    "GraphPlan",
    "RuleSelectors",
    "compile_graph_plan",
    "order_rules_by_evidence",
    "rule_graph_signature",
]

"""Snapshot helpers for centralized relspec registries."""

from __future__ import annotations

from dataclasses import dataclass

import pyarrow as pa

from arrowdsl.io.ipc import ipc_hash
from datafusion_engine.runtime import DataFusionRuntimeProfile
from relspec.compiler import rel_plan_for_rule
from relspec.graph import rule_graph_signature
from relspec.incremental import RelspecIncrementalSpec, build_incremental_spec
from relspec.plan import rel_plan_signature
from relspec.registry.rules import RuleRegistry
from relspec.rules.coverage import RuleCoverageAssessment, assess_rule_coverage
from relspec.rules.definitions import RuleDefinition
from relspec.rules.handlers.cpg import relationship_rule_from_definition
from relspec.rules.rel_ops import rel_ops_signature
from relspec.rules.spec_tables import rule_definition_table
from relspec.schema_context import RelspecSchemaContext


@dataclass(frozen=True)
class RelspecSnapshot:
    """Snapshot of centralized rule registries and signatures."""

    rule_table: pa.Table
    template_table: pa.Table
    rule_diagnostics: pa.Table
    template_diagnostics: pa.Table
    plan_signatures: dict[str, str]
    graph_signature: str
    coverage: RuleCoverageAssessment
    bundle_inventory: dict[str, str]
    incremental_spec: RelspecIncrementalSpec


def build_relspec_snapshot(registry: RuleRegistry) -> RelspecSnapshot:
    """Build a snapshot from a centralized rule registry.

    Returns
    -------
    RelspecSnapshot
        Snapshot containing rule and template tables plus signatures.
    """
    rules = registry.rule_definitions()
    return RelspecSnapshot(
        rule_table=rule_definition_table(rules),
        template_table=registry.template_table(),
        rule_diagnostics=registry.rule_diagnostics_table(),
        template_diagnostics=registry.template_diagnostics_table(),
        plan_signatures=_rule_plan_signatures(rules),
        graph_signature=_graph_signature(rules),
        coverage=assess_rule_coverage(rules),
        bundle_inventory=_bundle_inventory(registry),
        incremental_spec=_incremental_spec(registry, rules=rules),
    )


def _rule_plan_signatures(rules: tuple[RuleDefinition, ...]) -> dict[str, str]:
    return {rule.name: _rule_signature(rule) for rule in rules}


def _rule_signature(rule: RuleDefinition) -> str:
    if rule.domain == "cpg":
        rel_rule = relationship_rule_from_definition(rule)
        plan = rel_plan_for_rule(rel_rule)
        if plan is not None:
            return rel_plan_signature(plan)
    if rule.rel_ops:
        return rel_ops_signature(rule.rel_ops)
    table = rule_definition_table((rule,))
    return ipc_hash(table)


def _graph_signature(rules: tuple[RuleDefinition, ...]) -> str:
    return rule_graph_signature(
        rules,
        name_for=lambda rule: rule.name,
        signature_for=_rule_signature,
        label="relspec",
    )


def _bundle_inventory(registry: RuleRegistry) -> dict[str, str]:
    return {bundle.name: bundle.domain for bundle in registry.bundles}


def _incremental_spec(
    registry: RuleRegistry,
    *,
    rules: tuple[RuleDefinition, ...],
) -> RelspecIncrementalSpec:
    if registry.schema_context is not None:
        schema_context = registry.schema_context
    elif registry.engine_session is not None:
        schema_context = RelspecSchemaContext.from_engine_session(registry.engine_session)
    else:
        schema_context = RelspecSchemaContext.from_session(
            DataFusionRuntimeProfile().session_context()
        )
    return build_incremental_spec(rules, schema_context=schema_context)


__all__ = ["RelspecSnapshot", "build_relspec_snapshot"]

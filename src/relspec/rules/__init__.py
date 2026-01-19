"""Centralized rule models and registries."""

from __future__ import annotations

import importlib
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from relspec.rules.compiler import RuleCompiler, RuleHandler
    from relspec.rules.definitions import (
        EdgeEmitPayload,
        EvidenceOutput,
        EvidenceSpec,
        ExecutionMode,
        ExtractPayload,
        NormalizePayload,
        PolicyOverrides,
        RelationshipPayload,
        RuleDefinition,
        RuleDomain,
        RulePayload,
        RuleStage,
        RuleStageMode,
        stage_enabled,
    )
    from relspec.rules.diagnostics import RuleDiagnostic, rule_diagnostic_table
    from relspec.rules.handlers import (
        ExtractRuleHandler,
        NormalizeRuleHandler,
        RelationshipRuleHandler,
    )
    from relspec.rules.options import RuleExecutionOptions
    from relspec.rules.registry import RuleAdapter, RuleRegistry, collect_rule_definitions
    from relspec.rules.rel_ops import (
        AggregateExpr,
        AggregateOp,
        DeriveOp,
        FilterOp,
        JoinOp,
        ParamOp,
        ProjectOp,
        PushdownFilterOp,
        RelOp,
        RelOpKind,
        RelOpT,
        ScanOp,
        UnionOp,
        query_spec_from_rel_ops,
        rel_ops_from_rows,
        rel_ops_signature,
        rel_ops_to_rows,
        validate_rel_ops,
    )
    from relspec.rules.relationship_specs import relationship_rule_definitions
    from relspec.rules.spec_tables import (
        EXTRACT_STRUCT,
        NORMALIZE_STRUCT,
        RELATIONSHIP_STRUCT,
        RULE_DEF_SCHEMA,
        rule_definition_table,
        rule_definitions_from_table,
    )
    from relspec.rules.templates import RuleTemplateSpec, rule_template_table

_EXPORT_MAP: dict[str, tuple[str, str]] = {
    "AggregateExpr": ("relspec.rules.rel_ops", "AggregateExpr"),
    "AggregateOp": ("relspec.rules.rel_ops", "AggregateOp"),
    "DeriveOp": ("relspec.rules.rel_ops", "DeriveOp"),
    "EdgeEmitPayload": ("relspec.rules.definitions", "EdgeEmitPayload"),
    "EvidenceOutput": ("relspec.rules.definitions", "EvidenceOutput"),
    "EvidenceSpec": ("relspec.rules.definitions", "EvidenceSpec"),
    "ExecutionMode": ("relspec.rules.definitions", "ExecutionMode"),
    "EXTRACT_STRUCT": ("relspec.rules.spec_tables", "EXTRACT_STRUCT"),
    "ExtractPayload": ("relspec.rules.definitions", "ExtractPayload"),
    "ExtractRuleHandler": ("relspec.rules.handlers", "ExtractRuleHandler"),
    "FilterOp": ("relspec.rules.rel_ops", "FilterOp"),
    "JoinOp": ("relspec.rules.rel_ops", "JoinOp"),
    "NORMALIZE_STRUCT": ("relspec.rules.spec_tables", "NORMALIZE_STRUCT"),
    "NormalizePayload": ("relspec.rules.definitions", "NormalizePayload"),
    "NormalizeRuleHandler": ("relspec.rules.handlers", "NormalizeRuleHandler"),
    "ParamOp": ("relspec.rules.rel_ops", "ParamOp"),
    "PolicyOverrides": ("relspec.rules.definitions", "PolicyOverrides"),
    "ProjectOp": ("relspec.rules.rel_ops", "ProjectOp"),
    "PushdownFilterOp": ("relspec.rules.rel_ops", "PushdownFilterOp"),
    "RELATIONSHIP_STRUCT": ("relspec.rules.spec_tables", "RELATIONSHIP_STRUCT"),
    "RelOp": ("relspec.rules.rel_ops", "RelOp"),
    "RelOpKind": ("relspec.rules.rel_ops", "RelOpKind"),
    "RelOpT": ("relspec.rules.rel_ops", "RelOpT"),
    "RelationshipPayload": ("relspec.rules.definitions", "RelationshipPayload"),
    "RelationshipRuleHandler": ("relspec.rules.handlers", "RelationshipRuleHandler"),
    "RuleAdapter": ("relspec.rules.registry", "RuleAdapter"),
    "RuleCompiler": ("relspec.rules.compiler", "RuleCompiler"),
    "RuleDefinition": ("relspec.rules.definitions", "RuleDefinition"),
    "RuleDiagnostic": ("relspec.rules.diagnostics", "RuleDiagnostic"),
    "RuleDomain": ("relspec.rules.definitions", "RuleDomain"),
    "RULE_DEF_SCHEMA": ("relspec.rules.spec_tables", "RULE_DEF_SCHEMA"),
    "RuleExecutionOptions": ("relspec.rules.options", "RuleExecutionOptions"),
    "RuleHandler": ("relspec.rules.compiler", "RuleHandler"),
    "RulePayload": ("relspec.rules.definitions", "RulePayload"),
    "RuleRegistry": ("relspec.rules.registry", "RuleRegistry"),
    "RuleStage": ("relspec.rules.definitions", "RuleStage"),
    "RuleStageMode": ("relspec.rules.definitions", "RuleStageMode"),
    "RuleTemplateSpec": ("relspec.rules.templates", "RuleTemplateSpec"),
    "ScanOp": ("relspec.rules.rel_ops", "ScanOp"),
    "UnionOp": ("relspec.rules.rel_ops", "UnionOp"),
    "collect_rule_definitions": ("relspec.rules.registry", "collect_rule_definitions"),
    "query_spec_from_rel_ops": ("relspec.rules.rel_ops", "query_spec_from_rel_ops"),
    "rel_ops_from_rows": ("relspec.rules.rel_ops", "rel_ops_from_rows"),
    "rel_ops_signature": ("relspec.rules.rel_ops", "rel_ops_signature"),
    "rel_ops_to_rows": ("relspec.rules.rel_ops", "rel_ops_to_rows"),
    "relationship_rule_definitions": (
        "relspec.rules.relationship_specs",
        "relationship_rule_definitions",
    ),
    "rule_definition_table": ("relspec.rules.spec_tables", "rule_definition_table"),
    "rule_definitions_from_table": ("relspec.rules.spec_tables", "rule_definitions_from_table"),
    "rule_diagnostic_table": ("relspec.rules.diagnostics", "rule_diagnostic_table"),
    "rule_template_table": ("relspec.rules.templates", "rule_template_table"),
    "stage_enabled": ("relspec.rules.definitions", "stage_enabled"),
    "validate_rel_ops": ("relspec.rules.rel_ops", "validate_rel_ops"),
}


def __getattr__(name: str) -> object:
    target = _EXPORT_MAP.get(name)
    if target is None:
        msg = f"module {__name__!r} has no attribute {name!r}"
        raise AttributeError(msg)
    module_path, attr_name = target
    module = importlib.import_module(module_path)
    return getattr(module, attr_name)


def __dir__() -> list[str]:
    return sorted(list(globals()) + list(_EXPORT_MAP))


__all__ = [
    "EXTRACT_STRUCT",
    "NORMALIZE_STRUCT",
    "RELATIONSHIP_STRUCT",
    "RULE_DEF_SCHEMA",
    "AggregateExpr",
    "AggregateOp",
    "DeriveOp",
    "EdgeEmitPayload",
    "EvidenceOutput",
    "EvidenceSpec",
    "ExecutionMode",
    "ExtractPayload",
    "ExtractRuleHandler",
    "FilterOp",
    "JoinOp",
    "NormalizePayload",
    "NormalizeRuleHandler",
    "ParamOp",
    "PolicyOverrides",
    "ProjectOp",
    "PushdownFilterOp",
    "RelOp",
    "RelOpKind",
    "RelOpT",
    "RelationshipPayload",
    "RelationshipRuleHandler",
    "RuleAdapter",
    "RuleCompiler",
    "RuleDefinition",
    "RuleDiagnostic",
    "RuleDomain",
    "RuleExecutionOptions",
    "RuleHandler",
    "RulePayload",
    "RuleRegistry",
    "RuleStage",
    "RuleStageMode",
    "RuleTemplateSpec",
    "ScanOp",
    "UnionOp",
    "collect_rule_definitions",
    "query_spec_from_rel_ops",
    "rel_ops_from_rows",
    "rel_ops_signature",
    "rel_ops_to_rows",
    "relationship_rule_definitions",
    "rule_definition_table",
    "rule_definitions_from_table",
    "rule_diagnostic_table",
    "rule_template_table",
    "stage_enabled",
    "validate_rel_ops",
]

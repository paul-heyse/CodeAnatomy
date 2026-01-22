"""Centralized rule models and registries."""

from __future__ import annotations

import importlib
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from relspec.registry.rules import RuleAdapter, RuleRegistry, collect_rule_definitions
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
    "EdgeEmitPayload": ("relspec.rules.definitions", "EdgeEmitPayload"),
    "EvidenceOutput": ("relspec.rules.definitions", "EvidenceOutput"),
    "EvidenceSpec": ("relspec.rules.definitions", "EvidenceSpec"),
    "ExecutionMode": ("relspec.rules.definitions", "ExecutionMode"),
    "EXTRACT_STRUCT": ("relspec.rules.spec_tables", "EXTRACT_STRUCT"),
    "ExtractPayload": ("relspec.rules.definitions", "ExtractPayload"),
    "ExtractRuleHandler": ("relspec.rules.handlers", "ExtractRuleHandler"),
    "NORMALIZE_STRUCT": ("relspec.rules.spec_tables", "NORMALIZE_STRUCT"),
    "NormalizePayload": ("relspec.rules.definitions", "NormalizePayload"),
    "NormalizeRuleHandler": ("relspec.rules.handlers", "NormalizeRuleHandler"),
    "PolicyOverrides": ("relspec.rules.definitions", "PolicyOverrides"),
    "RELATIONSHIP_STRUCT": ("relspec.rules.spec_tables", "RELATIONSHIP_STRUCT"),
    "RelationshipPayload": ("relspec.rules.definitions", "RelationshipPayload"),
    "RelationshipRuleHandler": ("relspec.rules.handlers", "RelationshipRuleHandler"),
    "RuleAdapter": ("relspec.registry.rules", "RuleAdapter"),
    "RuleCompiler": ("relspec.rules.compiler", "RuleCompiler"),
    "RuleDefinition": ("relspec.rules.definitions", "RuleDefinition"),
    "RuleDiagnostic": ("relspec.rules.diagnostics", "RuleDiagnostic"),
    "RuleDomain": ("relspec.rules.definitions", "RuleDomain"),
    "RULE_DEF_SCHEMA": ("relspec.rules.spec_tables", "RULE_DEF_SCHEMA"),
    "RuleExecutionOptions": ("relspec.rules.options", "RuleExecutionOptions"),
    "RuleHandler": ("relspec.rules.compiler", "RuleHandler"),
    "RulePayload": ("relspec.rules.definitions", "RulePayload"),
    "RuleRegistry": ("relspec.registry.rules", "RuleRegistry"),
    "RuleStage": ("relspec.rules.definitions", "RuleStage"),
    "RuleStageMode": ("relspec.rules.definitions", "RuleStageMode"),
    "RuleTemplateSpec": ("relspec.rules.templates", "RuleTemplateSpec"),
    "collect_rule_definitions": ("relspec.registry.rules", "collect_rule_definitions"),
    "relationship_rule_definitions": (
        "relspec.rules.relationship_specs",
        "relationship_rule_definitions",
    ),
    "rule_definition_table": ("relspec.rules.spec_tables", "rule_definition_table"),
    "rule_definitions_from_table": ("relspec.rules.spec_tables", "rule_definitions_from_table"),
    "rule_diagnostic_table": ("relspec.rules.diagnostics", "rule_diagnostic_table"),
    "rule_template_table": ("relspec.rules.templates", "rule_template_table"),
    "stage_enabled": ("relspec.rules.definitions", "stage_enabled"),
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
    "EdgeEmitPayload",
    "EvidenceOutput",
    "EvidenceSpec",
    "ExecutionMode",
    "ExtractPayload",
    "ExtractRuleHandler",
    "NormalizePayload",
    "NormalizeRuleHandler",
    "PolicyOverrides",
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
    "collect_rule_definitions",
    "relationship_rule_definitions",
    "rule_definition_table",
    "rule_definitions_from_table",
    "rule_diagnostic_table",
    "rule_template_table",
    "stage_enabled",
]

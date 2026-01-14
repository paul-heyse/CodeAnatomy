"""Centralized rule models and registries."""

from relspec.rules.compiler import RuleCompiler, RuleHandler
from relspec.rules.definitions import (
    EdgeEmitPayload,
    EvidenceOutput,
    EvidenceSpec,
    ExecutionMode,
    ExtractPayload,
    NormalizePayload,
    PipelineOp,
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
from relspec.rules.handlers import ExtractRuleHandler, NormalizeRuleHandler, RelationshipRuleHandler
from relspec.rules.options import RuleExecutionOptions
from relspec.rules.registry import (
    RuleAdapter,
    RuleRegistry,
    collect_rule_definitions,
)
from relspec.rules.relationship_specs import relationship_rule_definitions
from relspec.rules.spec_tables import (
    EXTRACT_STRUCT,
    NORMALIZE_STRUCT,
    RELATIONSHIP_STRUCT,
    RULE_DEF_SCHEMA,
    query_from_ops,
    rule_definition_table,
    rule_definitions_from_table,
)
from relspec.rules.templates import RuleTemplateSpec, rule_template_table

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
    "PipelineOp",
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
    "query_from_ops",
    "relationship_rule_definitions",
    "rule_definition_table",
    "rule_definitions_from_table",
    "rule_diagnostic_table",
    "rule_template_table",
    "stage_enabled",
]

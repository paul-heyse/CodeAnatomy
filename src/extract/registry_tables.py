"""Spec tables for extract dataset registry rows."""

from __future__ import annotations

from relspec.adapters import ExtractRuleAdapter
from relspec.rules.registry import RuleRegistry
from relspec.rules.spec_tables import rule_definition_table as canonical_rule_table

_REGISTRY = RuleRegistry(adapters=(ExtractRuleAdapter(),))
EXTRACT_RULE_TABLE = canonical_rule_table(_REGISTRY.rules_for_domain("extract"))

__all__ = ["EXTRACT_RULE_TABLE"]

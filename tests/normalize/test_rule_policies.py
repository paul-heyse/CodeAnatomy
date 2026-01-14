"""Tests for normalize policy defaults."""

from __future__ import annotations

from typing import cast

from arrowdsl.core.context import ExecutionContext, RuntimeProfile
from normalize.rule_factories import build_rule_definitions_from_specs
from normalize.rule_model import NormalizeRule
from normalize.rule_registry_specs import rule_family_specs
from normalize.runner import apply_policy_defaults
from relspec.rules.compiler import RuleCompiler
from relspec.rules.handlers.normalize import NormalizeRuleHandler


def _normalize_rules() -> tuple[NormalizeRule, ...]:
    ctx = ExecutionContext(runtime=RuntimeProfile(name="TEST"))
    definitions = build_rule_definitions_from_specs(rule_family_specs())
    compiler = RuleCompiler(handlers={"normalize": NormalizeRuleHandler()})
    compiled = compiler.compile_rules(definitions, ctx=ctx)
    return cast("tuple[NormalizeRule, ...]", compiled)


def test_rule_policies_have_contract_defaults() -> None:
    """Ensure normalize rules derive confidence/ambiguity policies from contracts."""
    rules = apply_policy_defaults(_normalize_rules())
    missing_confidence = sorted(rule.name for rule in rules if rule.confidence_policy is None)
    missing_ambiguity = sorted(rule.name for rule in rules if rule.ambiguity_policy is None)
    assert not missing_confidence, f"Missing confidence policies for: {missing_confidence}"
    assert not missing_ambiguity, f"Missing ambiguity policies for: {missing_ambiguity}"

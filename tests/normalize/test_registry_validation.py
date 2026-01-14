"""Tests for normalize registry validation."""

from __future__ import annotations

from typing import cast

from arrowdsl.core.context import ExecutionContext, RuntimeProfile
from normalize.registry_validation import validate_rule_specs
from normalize.rule_factories import build_rule_definitions_from_specs
from normalize.rule_model import NormalizeRule
from normalize.rule_registry_specs import rule_family_specs
from relspec.rules.compiler import RuleCompiler
from relspec.rules.handlers.normalize import NormalizeRuleHandler


def _normalize_rules() -> tuple[NormalizeRule, ...]:
    ctx = ExecutionContext(runtime=RuntimeProfile(name="TEST"))
    definitions = build_rule_definitions_from_specs(rule_family_specs())
    compiler = RuleCompiler(handlers={"normalize": NormalizeRuleHandler()})
    compiled = compiler.compile_rules(definitions, ctx=ctx)
    return cast("tuple[NormalizeRule, ...]", compiled)


def test_normalize_registry_validation() -> None:
    """Ensure normalize rules validate against registry contracts."""
    validate_rule_specs(_normalize_rules())

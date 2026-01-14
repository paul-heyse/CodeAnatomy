"""Tests for normalize rule family construction."""

from __future__ import annotations

from typing import cast

from arrowdsl.core.context import ExecutionContext, RuntimeProfile
from normalize.registry_specs import dataset_names
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


def test_rule_family_outputs_registered() -> None:
    """Validate that normalize rule outputs are registered datasets."""
    rules = _normalize_rules()
    outputs = {rule.output for rule in rules}
    assert outputs
    registered = set(dataset_names())
    missing = sorted(outputs - registered)
    assert not missing, f"Missing dataset specs for: {missing}"
    assert len(outputs) == len(rules)

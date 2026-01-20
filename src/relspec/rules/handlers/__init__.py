"""Domain-specific rule handlers."""

from __future__ import annotations

from relspec.policies import PolicyRegistry
from relspec.rules.compiler import RuleHandler
from relspec.rules.definitions import RuleDomain
from relspec.rules.handlers.cpg import RelationshipRuleHandler
from relspec.rules.handlers.cpg_emit import (
    EdgeEmitRuleHandler,
    NodeEmitRuleHandler,
    PropEmitRuleHandler,
)
from relspec.rules.handlers.extract import ExtractRuleCompilation, ExtractRuleHandler
from relspec.rules.handlers.normalize import NormalizeRuleHandler


def default_rule_handlers(
    *,
    policies: PolicyRegistry | None = None,
) -> dict[RuleDomain, RuleHandler]:
    """Return default rule handlers by domain.

    Parameters
    ----------
    policies
        Optional policy registry for relationship rule compilation.

    Returns
    -------
    dict[RuleDomain, RuleHandler]
        Handler mapping keyed by rule domain.
    """
    return {
        "cpg": RelationshipRuleHandler(policies=policies or PolicyRegistry()),
        "normalize": NormalizeRuleHandler(),
        "extract": ExtractRuleHandler(),
    }


__all__ = [
    "EdgeEmitRuleHandler",
    "ExtractRuleCompilation",
    "ExtractRuleHandler",
    "NodeEmitRuleHandler",
    "NormalizeRuleHandler",
    "PropEmitRuleHandler",
    "RelationshipRuleHandler",
    "default_rule_handlers",
]

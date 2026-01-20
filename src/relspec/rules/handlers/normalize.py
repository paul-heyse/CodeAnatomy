"""Normalize rule handler for centralized compilation."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from arrowdsl.core.execution_context import ExecutionContext
from relspec.rules.compiler import RuleHandler

if TYPE_CHECKING:
    from relspec.rules.definitions import RuleDefinition, RuleDomain


@dataclass(frozen=True)
class NormalizeRuleHandler(RuleHandler):
    """Pass through normalize rule definitions."""

    domain: RuleDomain = "normalize"

    def compile_rule(self, rule: RuleDefinition, *, ctx: ExecutionContext) -> RuleDefinition:
        """Return the normalize rule definition unchanged.

        Returns
        -------
        RuleDefinition
            Normalize rule definition.

        Raises
        ------
        ValueError
            Raised when a non-normalize rule is passed.
        """
        _ = ctx
        if rule.domain != self.domain:
            msg = f"NormalizeRuleHandler received rule for domain {rule.domain!r}."
            raise ValueError(msg)
        return rule


__all__ = ["NormalizeRuleHandler"]

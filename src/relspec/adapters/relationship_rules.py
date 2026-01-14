"""Adapter for relspec relationship rule definitions."""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING

from relspec.rules.diagnostics import RuleDiagnostic
from relspec.rules.registry import RuleAdapter
from relspec.rules.relationship_specs import relationship_rule_definitions
from relspec.rules.templates import RuleTemplateSpec

if TYPE_CHECKING:
    from relspec.rules.definitions import RuleDefinition, RuleDomain


class RelspecRuleAdapter(RuleAdapter):
    """Adapter exposing relspec relationship rule definitions."""

    domain: RuleDomain = "cpg"

    @staticmethod
    def rule_definitions() -> Sequence[RuleDefinition]:
        """Return relspec relationship rule definitions for the registry.

        Returns
        -------
        Sequence[RuleDefinition]
            Relationship rule definitions.
        """
        return relationship_rule_definitions()

    @staticmethod
    def templates() -> Sequence[RuleTemplateSpec]:
        """Return template specs for relspec (none today).

        Returns
        -------
        Sequence[RuleTemplateSpec]
            Template specs (empty).
        """
        return ()

    @staticmethod
    def template_diagnostics() -> Sequence[RuleDiagnostic]:
        """Return template diagnostics for relspec (none today).

        Returns
        -------
        Sequence[RuleDiagnostic]
            Template diagnostics (empty).
        """
        return ()


__all__ = ["RelspecRuleAdapter"]

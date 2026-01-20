"""Adapter for relspec relationship rule definitions."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING

from relspec.registry.rules import RuleAdapter
from relspec.rules.diagnostics import RuleDiagnostic
from relspec.rules.relationship_specs import relationship_rule_definitions
from relspec.rules.templates import RuleTemplateSpec

if TYPE_CHECKING:
    from relspec.adapters.factory import RuleFactoryRegistry
    from relspec.rules.definitions import RuleDefinition, RuleDomain


@dataclass(frozen=True)
class RelspecRuleAdapter(RuleAdapter):
    """Adapter exposing relspec relationship rule definitions."""

    registry: RuleFactoryRegistry
    domain: RuleDomain = "cpg"
    factory_name: str = "relspec"

    def rule_definitions(self) -> Sequence[RuleDefinition]:
        """Return relspec relationship rule definitions for the registry.

        Returns
        -------
        Sequence[RuleDefinition]
            Relationship rule definitions.
        """
        return self.registry.rule_definitions(self.factory_name)

    def templates(self) -> Sequence[RuleTemplateSpec]:
        """Return template specs for relspec (none today).

        Returns
        -------
        Sequence[RuleTemplateSpec]
            Template specs (empty).
        """
        return self.registry.templates(self.factory_name)

    def template_diagnostics(self) -> Sequence[RuleDiagnostic]:
        """Return template diagnostics for relspec (none today).

        Returns
        -------
        Sequence[RuleDiagnostic]
            Template diagnostics (empty).
        """
        return self.registry.template_diagnostics(self.factory_name)


def relspec_rule_definitions() -> Sequence[RuleDefinition]:
    """Return relspec relationship rule definitions for factory registration.

    Returns
    -------
    Sequence[RuleDefinition]
        Relationship rule definitions.
    """
    return relationship_rule_definitions()


def relspec_template_specs() -> Sequence[RuleTemplateSpec]:
    """Return relspec template specs for factory registration.

    Returns
    -------
    Sequence[RuleTemplateSpec]
        Relspec template specs.
    """
    return ()


def relspec_template_diagnostics() -> Sequence[RuleDiagnostic]:
    """Return relspec template diagnostics for factory registration.

    Returns
    -------
    Sequence[RuleDiagnostic]
        Relspec template diagnostics.
    """
    return ()


__all__ = ["RelspecRuleAdapter"]

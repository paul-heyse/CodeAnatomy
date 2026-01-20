"""Extract adapter for centralized rule definitions."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING

from extract.registry_definitions import extract_rule_definitions
from relspec.extract.registry_bundles import (
    extract_template_diagnostics as extract_template_diagnostics_from_specs,
)
from relspec.extract.registry_bundles import (
    extract_template_specs as extract_template_specs_from_specs,
)
from relspec.registry.rules import RuleAdapter
from relspec.rules.definitions import RuleDefinition
from relspec.rules.diagnostics import RuleDiagnostic
from relspec.rules.templates import RuleTemplateSpec

if TYPE_CHECKING:
    from relspec.adapters.factory import RuleFactoryRegistry
    from relspec.rules.definitions import RuleDomain


@dataclass(frozen=True)
class ExtractRuleAdapter(RuleAdapter):
    """Adapter that exposes extract dataset specs as rule definitions."""

    registry: RuleFactoryRegistry
    domain: RuleDomain = "extract"
    factory_name: str = "extract"

    def rule_definitions(self) -> Sequence[RuleDefinition]:
        """Return extract rule definitions for the central registry.

        Returns
        -------
        Sequence[RuleDefinition]
            Extract rule definitions.
        """
        return self.registry.rule_definitions(self.factory_name)

    def templates(self) -> Sequence[RuleTemplateSpec]:
        """Return extract dataset templates for expansion.

        Returns
        -------
        Sequence[RuleTemplateSpec]
            Centralized template specs.
        """
        return self.registry.templates(self.factory_name)

    def template_diagnostics(self) -> Sequence[RuleDiagnostic]:
        """Return template diagnostics for extract templates.

        Returns
        -------
        Sequence[RuleDiagnostic]
            Template diagnostics for extract templates.
        """
        return self.registry.template_diagnostics(self.factory_name)


def extract_rule_definitions_for_adapter() -> Sequence[RuleDefinition]:
    """Return extract rule definitions for factory registration.

    Returns
    -------
    Sequence[RuleDefinition]
        Extract rule definitions.
    """
    return extract_rule_definitions()


def extract_template_specs() -> Sequence[RuleTemplateSpec]:
    """Return extract template specs for factory registration.

    Returns
    -------
    Sequence[RuleTemplateSpec]
        Extract template specs.
    """
    return extract_template_specs_from_specs()


def extract_template_diagnostics() -> Sequence[RuleDiagnostic]:
    """Return extract template diagnostics for factory registration.

    Returns
    -------
    Sequence[RuleDiagnostic]
        Extract template diagnostics.
    """
    return extract_template_diagnostics_from_specs()


__all__ = ["ExtractRuleAdapter"]

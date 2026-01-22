"""CPG adapter for centralized rule definitions."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING

from relspec import errors as relspec_errors
from relspec.registry.rules import RuleAdapter
from relspec.rules.cpg_relationship_specs import (
    cpg_rule_definitions as cpg_rule_definitions_from_specs,
)
from relspec.rules.cpg_relationship_specs import (
    cpg_template_diagnostics as cpg_template_diagnostics_from_specs,
)
from relspec.rules.cpg_relationship_specs import (
    cpg_template_specs as cpg_template_specs_from_specs,
)
from relspec.rules.definitions import RuleDefinition
from relspec.rules.diagnostics import RuleDiagnostic
from relspec.rules.templates import RuleTemplateSpec

if TYPE_CHECKING:
    from relspec.adapters.factory import RuleFactoryRegistry
    from relspec.rules.definitions import RuleDomain


@dataclass(frozen=True)
class CpgRuleAdapter(RuleAdapter):
    """Adapter that exposes CPG relationship rules as central RuleDefinitions."""

    registry: RuleFactoryRegistry
    domain: RuleDomain = "cpg"
    factory_name: str = "cpg"

    def rule_definitions(self) -> Sequence[RuleDefinition]:
        """Return CPG rule definitions for the central registry.

        Returns
        -------
        Sequence[RuleDefinition]
            CPG rule definitions.

        Raises
        ------
        RelspecValidationError
            Raised when the adapter domain is misconfigured.
        """
        if self.domain != "cpg":
            msg = f"Unexpected adapter domain: {self.domain!r}."
            raise relspec_errors.RelspecValidationError(msg)
        return self.registry.rule_definitions(self.factory_name)

    def templates(self) -> Sequence[RuleTemplateSpec]:
        """Return template specs for the CPG adapter.

        Returns
        -------
        Sequence[RuleTemplateSpec]
            Centralized template specs.

        Raises
        ------
        RelspecValidationError
            Raised when the adapter domain is misconfigured.
        """
        if self.domain != "cpg":
            msg = f"Unexpected adapter domain: {self.domain!r}."
            raise relspec_errors.RelspecValidationError(msg)
        return self.registry.templates(self.factory_name)

    def template_diagnostics(self) -> Sequence[RuleDiagnostic]:
        """Return template diagnostics for CPG templates.

        Returns
        -------
        Sequence[RuleDiagnostic]
            Template diagnostics for CPG templates.

        Raises
        ------
        RelspecValidationError
            Raised when the adapter domain is misconfigured.
        """
        if self.domain != "cpg":
            msg = f"Unexpected adapter domain: {self.domain!r}."
            raise relspec_errors.RelspecValidationError(msg)
        return self.registry.template_diagnostics(self.factory_name)


def cpg_rule_definitions() -> Sequence[RuleDefinition]:
    """Return CPG rule definitions for factory registration.

    Returns
    -------
    Sequence[RuleDefinition]
        CPG rule definitions.
    """
    return cpg_rule_definitions_from_specs()


def cpg_template_specs() -> Sequence[RuleTemplateSpec]:
    """Return CPG template specs for factory registration.

    Returns
    -------
    Sequence[RuleTemplateSpec]
        CPG template specs.
    """
    return cpg_template_specs_from_specs()


def cpg_template_diagnostics() -> Sequence[RuleDiagnostic]:
    """Return CPG template diagnostics for factory registration.

    Returns
    -------
    Sequence[RuleDiagnostic]
        Diagnostics emitted while building CPG templates.
    """
    return cpg_template_diagnostics_from_specs()


__all__ = ["CpgRuleAdapter"]

"""Normalize adapter for centralized rule definitions."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING

from normalize.rule_factories import build_rule_definitions_from_specs
from relspec.normalize.rule_registry_specs import rule_family_specs
from relspec.registry.rules import RuleAdapter
from relspec.rules.definitions import RuleDefinition
from relspec.rules.diagnostics import RuleDiagnostic
from relspec.rules.templates import RuleTemplateSpec

if TYPE_CHECKING:
    from relspec.adapters.factory import RuleFactoryRegistry
    from relspec.rules.definitions import RuleDomain


@dataclass(frozen=True)
class NormalizeRuleAdapter(RuleAdapter):
    """Adapter that exposes normalize rule definitions."""

    registry: RuleFactoryRegistry
    domain: RuleDomain = "normalize"
    factory_name: str = "normalize"

    def rule_definitions(self) -> Sequence[RuleDefinition]:
        """Return normalize rule definitions for the central registry.

        Returns
        -------
        Sequence[RuleDefinition]
            Normalize rule definitions.

        Raises
        ------
        ValueError
            Raised when the adapter domain is misconfigured.
        """
        if self.domain != "normalize":
            msg = f"Unexpected adapter domain: {self.domain!r}."
            raise ValueError(msg)
        return self.registry.rule_definitions(self.factory_name)

    def templates(self) -> Sequence[RuleTemplateSpec]:
        """Return template specs for normalize (none today).

        Returns
        -------
        Sequence[RuleTemplateSpec]
            Template specs (empty).

        Raises
        ------
        ValueError
            Raised when the adapter domain is misconfigured.
        """
        if self.domain != "normalize":
            msg = f"Unexpected adapter domain: {self.domain!r}."
            raise ValueError(msg)
        return self.registry.templates(self.factory_name)

    def template_diagnostics(self) -> Sequence[RuleDiagnostic]:
        """Return template diagnostics for normalize (none today).

        Returns
        -------
        Sequence[RuleDiagnostic]
            Template diagnostics (empty).

        Raises
        ------
        ValueError
            Raised when the adapter domain is misconfigured.
        """
        if self.domain != "normalize":
            msg = f"Unexpected adapter domain: {self.domain!r}."
            raise ValueError(msg)
        return self.registry.template_diagnostics(self.factory_name)


def normalize_rule_definitions() -> Sequence[RuleDefinition]:
    """Return normalize rule definitions for factory registration.

    Returns
    -------
    Sequence[RuleDefinition]
        Normalize rule definitions.
    """
    return build_rule_definitions_from_specs(rule_family_specs())


def normalize_template_specs() -> Sequence[RuleTemplateSpec]:
    """Return normalize template specs for factory registration.

    Returns
    -------
    Sequence[RuleTemplateSpec]
        Normalize template specs.
    """
    return ()


def normalize_template_diagnostics() -> Sequence[RuleDiagnostic]:
    """Return normalize template diagnostics for factory registration.

    Returns
    -------
    Sequence[RuleDiagnostic]
        Normalize template diagnostics.
    """
    return ()


__all__ = ["NormalizeRuleAdapter"]

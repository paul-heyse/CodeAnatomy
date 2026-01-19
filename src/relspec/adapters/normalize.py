"""Normalize adapter for centralized rule definitions."""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING

from normalize.rule_factories import build_rule_definitions_from_specs
from relspec.normalize.rule_registry_specs import rule_family_specs
from relspec.rules.definitions import RuleDefinition
from relspec.rules.diagnostics import RuleDiagnostic
from relspec.rules.registry import RuleAdapter
from relspec.rules.templates import RuleTemplateSpec

if TYPE_CHECKING:
    from relspec.rules.definitions import RuleDomain


class NormalizeRuleAdapter(RuleAdapter):
    """Adapter that exposes normalize rule definitions."""

    domain: RuleDomain = "normalize"

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
        return build_rule_definitions_from_specs(rule_family_specs())

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
        return ()

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
        return ()


__all__ = ["NormalizeRuleAdapter"]

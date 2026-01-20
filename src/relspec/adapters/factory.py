"""Factory registry for centralized rule adapters."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from functools import cache

from relspec.adapters.cpg import (
    cpg_rule_definitions,
    cpg_template_diagnostics,
    cpg_template_specs,
)
from relspec.adapters.extract import (
    extract_rule_definitions_for_adapter,
    extract_template_diagnostics,
    extract_template_specs,
)
from relspec.adapters.normalize import (
    normalize_rule_definitions,
    normalize_template_diagnostics,
    normalize_template_specs,
)
from relspec.adapters.relationship_rules import (
    relspec_rule_definitions,
    relspec_template_diagnostics,
    relspec_template_specs,
)
from relspec.rules.definitions import RuleDefinition
from relspec.rules.diagnostics import RuleDiagnostic
from relspec.rules.templates import RuleTemplateSpec

RuleDefinitionFactory = Callable[[], Sequence[RuleDefinition]]
RuleTemplateFactory = Callable[[], Sequence[RuleTemplateSpec]]
RuleDiagnosticFactory = Callable[[], Sequence[RuleDiagnostic]]


@dataclass(frozen=True)
class RuleFactoryBundle:
    """Rule and template factories for a single adapter."""

    rules: RuleDefinitionFactory
    templates: RuleTemplateFactory
    template_diagnostics: RuleDiagnosticFactory

    def build_rules(self) -> tuple[RuleDefinition, ...]:
        """Return rule definitions from the bundle.

        Returns
        -------
        tuple[RuleDefinition, ...]
            Rule definitions for the adapter.
        """
        return tuple(self.rules())

    def build_templates(self) -> tuple[RuleTemplateSpec, ...]:
        """Return template specs from the bundle.

        Returns
        -------
        tuple[RuleTemplateSpec, ...]
            Template specs for the adapter.
        """
        return tuple(self.templates())

    def build_template_diagnostics(self) -> tuple[RuleDiagnostic, ...]:
        """Return template diagnostics from the bundle.

        Returns
        -------
        tuple[RuleDiagnostic, ...]
            Template diagnostics for the adapter.
        """
        return tuple(self.template_diagnostics())


@dataclass(frozen=True)
class RuleFactoryRegistry:
    """Registry of factories for centralized rule adapters."""

    factories: Mapping[str, RuleFactoryBundle]

    def rule_definitions(self, name: str) -> tuple[RuleDefinition, ...]:
        """Return rule definitions for a named adapter.

        Returns
        -------
        tuple[RuleDefinition, ...]
            Rule definitions for the adapter.
        """
        return self._bundle(name).build_rules()

    def templates(self, name: str) -> tuple[RuleTemplateSpec, ...]:
        """Return template specs for a named adapter.

        Returns
        -------
        tuple[RuleTemplateSpec, ...]
            Template specs for the adapter.
        """
        return self._bundle(name).build_templates()

    def template_diagnostics(self, name: str) -> tuple[RuleDiagnostic, ...]:
        """Return template diagnostics for a named adapter.

        Returns
        -------
        tuple[RuleDiagnostic, ...]
            Template diagnostics for the adapter.
        """
        return self._bundle(name).build_template_diagnostics()

    def _bundle(self, name: str) -> RuleFactoryBundle:
        bundle = self.factories.get(name)
        if bundle is None:
            msg = f"Unknown rule factory {name!r}."
            raise KeyError(msg)
        return bundle


@cache
def default_rule_factory_registry() -> RuleFactoryRegistry:
    """Return the default registry of rule factories.

    Returns
    -------
    RuleFactoryRegistry
        Registry populated with default adapter factories.
    """
    return RuleFactoryRegistry(
        factories={
            "cpg": RuleFactoryBundle(
                rules=cpg_rule_definitions,
                templates=cpg_template_specs,
                template_diagnostics=cpg_template_diagnostics,
            ),
            "relspec": RuleFactoryBundle(
                rules=relspec_rule_definitions,
                templates=relspec_template_specs,
                template_diagnostics=relspec_template_diagnostics,
            ),
            "normalize": RuleFactoryBundle(
                rules=normalize_rule_definitions,
                templates=normalize_template_specs,
                template_diagnostics=normalize_template_diagnostics,
            ),
            "extract": RuleFactoryBundle(
                rules=extract_rule_definitions_for_adapter,
                templates=extract_template_specs,
                template_diagnostics=extract_template_diagnostics,
            ),
        }
    )


__all__ = ["RuleFactoryBundle", "RuleFactoryRegistry", "default_rule_factory_registry"]

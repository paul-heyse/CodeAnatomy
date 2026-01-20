"""Decorators for registering rule bundles."""

from __future__ import annotations

from collections.abc import Callable

from relspec.rules.bundles import (
    DiagnosticFactory,
    RuleBundle,
    RuleFactory,
    TemplateFactory,
    register_bundle,
)
from relspec.rules.definitions import RuleDomain
from relspec.rules.diagnostics import RuleDiagnostic
from relspec.rules.templates import RuleTemplateSpec


def _empty_templates() -> tuple[RuleTemplateSpec, ...]:
    """Return an empty template spec sequence.

    Returns
    -------
    tuple[RuleTemplateSpec, ...]
        Empty template sequence.
    """
    return ()


def _empty_diagnostics() -> tuple[RuleDiagnostic, ...]:
    """Return an empty diagnostics sequence.

    Returns
    -------
    tuple[RuleDiagnostic, ...]
        Empty diagnostics sequence.
    """
    return ()


def rule_bundle(
    *,
    name: str,
    domain: RuleDomain,
    templates: TemplateFactory | None = None,
    diagnostics: DiagnosticFactory | None = None,
) -> Callable[[RuleFactory], RuleFactory]:
    """Register a rule bundle for discovery.

    Parameters
    ----------
    name
        Bundle name used for deterministic ordering.
    domain
        Rule domain for the bundle.
    templates
        Optional template factory for the bundle.
    diagnostics
        Optional diagnostics factory for the bundle.

    Returns
    -------
    Callable[[RuleFactory], RuleFactory]
        Decorator that registers the rule bundle.
    """

    def _wrap(factory: RuleFactory) -> RuleFactory:
        register_bundle(
            RuleBundle(
                name=name,
                domain=domain,
                rules=factory,
                templates=templates or _empty_templates,
                diagnostics=diagnostics or _empty_diagnostics,
            )
        )
        return factory

    return _wrap


__all__ = ["rule_bundle"]

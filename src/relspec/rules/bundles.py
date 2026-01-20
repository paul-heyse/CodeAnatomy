"""Bundle registry for programmatic rule discovery."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass

from relspec.rules.definitions import RuleDefinition, RuleDomain
from relspec.rules.diagnostics import RuleDiagnostic
from relspec.rules.templates import RuleTemplateSpec

RuleFactory = Callable[[], Sequence[RuleDefinition]]
TemplateFactory = Callable[[], Sequence[RuleTemplateSpec]]
DiagnosticFactory = Callable[[], Sequence[RuleDiagnostic]]


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


@dataclass(frozen=True)
class RuleBundle:
    """Collection of rules and templates for a single source."""

    name: str
    domain: RuleDomain
    rules: RuleFactory
    templates: TemplateFactory = _empty_templates
    diagnostics: DiagnosticFactory = _empty_diagnostics

    def build_rules(self) -> tuple[RuleDefinition, ...]:
        """Return rule definitions from the bundle.

        Returns
        -------
        tuple[RuleDefinition, ...]
            Rule definitions declared by the bundle.
        """
        return tuple(self.rules())

    def build_templates(self) -> tuple[RuleTemplateSpec, ...]:
        """Return template specs from the bundle.

        Returns
        -------
        tuple[RuleTemplateSpec, ...]
            Template specs declared by the bundle.
        """
        return tuple(self.templates())

    def build_diagnostics(self) -> tuple[RuleDiagnostic, ...]:
        """Return template diagnostics from the bundle.

        Returns
        -------
        tuple[RuleDiagnostic, ...]
            Diagnostics declared by the bundle.
        """
        return tuple(self.diagnostics())


_BUNDLES: dict[str, RuleBundle] = {}


def register_bundle(bundle: RuleBundle) -> None:
    """Register a bundle for rule discovery.

    Parameters
    ----------
    bundle
        Bundle to register.

    Raises
    ------
    ValueError
        Raised when a bundle with the same name is already registered.
    """
    if bundle.name in _BUNDLES:
        msg = f"Duplicate rule bundle: {bundle.name!r}."
        raise ValueError(msg)
    _BUNDLES[bundle.name] = bundle


def iter_bundles() -> tuple[RuleBundle, ...]:
    """Return registered bundles in deterministic order.

    Returns
    -------
    tuple[RuleBundle, ...]
        Registered bundles sorted by name.
    """
    return tuple(sorted(_BUNDLES.values(), key=lambda bundle: bundle.name))


def bundle_map() -> Mapping[str, RuleBundle]:
    """Return the registered bundle mapping.

    Returns
    -------
    Mapping[str, RuleBundle]
        Mapping of bundle names to bundles.
    """
    return dict(_BUNDLES)


def clear_bundles() -> None:
    """Clear all registered bundles."""
    _BUNDLES.clear()


__all__ = [
    "DiagnosticFactory",
    "RuleBundle",
    "RuleFactory",
    "TemplateFactory",
    "bundle_map",
    "clear_bundles",
    "iter_bundles",
    "register_bundle",
]

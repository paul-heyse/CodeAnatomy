"""Policy registry defaults for inference-driven pipelines."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Literal

from relspec.policies.model import AmbiguityPolicy, ConfidencePolicy, WinnerSelectConfig

RuleDomain = Literal["cpg", "extract", "normalize"]


def _default_confidence_policies() -> dict[str, dict[str, ConfidencePolicy]]:
    base = {
        "scip": ConfidencePolicy(base=1.0),
        "cst": ConfidencePolicy(base=0.9),
        "bytecode": ConfidencePolicy(base=0.8),
        "diagnostic": ConfidencePolicy(base=0.7),
        "span": ConfidencePolicy(base=0.6),
        "type": ConfidencePolicy(base=0.6),
        "evidence": ConfidencePolicy(base=0.5),
    }
    return {
        "cpg": dict(base),
        "extract": dict(base),
        "normalize": dict(base),
    }


def _default_ambiguity_policies() -> dict[str, dict[str, AmbiguityPolicy]]:
    preserve = AmbiguityPolicy()
    winner = AmbiguityPolicy(winner_select=WinnerSelectConfig())
    return {
        "cpg": {"preserve": preserve, "winner": winner},
        "extract": {"preserve": preserve, "winner": winner},
        "normalize": {"preserve": preserve, "winner": winner},
    }


@dataclass(frozen=True)
class PolicyRegistry:
    """Registry for confidence and ambiguity policies."""

    confidence: Mapping[str, Mapping[str, ConfidencePolicy]] = field(
        default_factory=_default_confidence_policies
    )
    ambiguity: Mapping[str, Mapping[str, AmbiguityPolicy]] = field(
        default_factory=_default_ambiguity_policies
    )

    def resolve_confidence(self, domain: str, name: str | None) -> ConfidencePolicy | None:
        """Return a confidence policy for the given domain and name.

        Parameters
        ----------
        domain : str
            Policy domain (e.g., "cpg", "normalize", "extract").
        name : str | None
            Policy name; ``None`` returns ``None``.

        Returns
        -------
        ConfidencePolicy | None
            Resolved policy or ``None`` if name is ``None``.

        Raises
        ------
        KeyError
            Raised when the domain or policy name is unknown.
        """
        if name is None:
            return None
        domain_policies = self.confidence.get(domain)
        if domain_policies is None:
            msg = f"Unknown confidence policy domain: {domain!r}."
            raise KeyError(msg)
        try:
            return domain_policies[name]
        except KeyError as exc:
            msg = f"Unknown confidence policy {name!r} for domain {domain!r}."
            raise KeyError(msg) from exc

    def resolve_ambiguity(self, domain: str, name: str | None) -> AmbiguityPolicy | None:
        """Return an ambiguity policy for the given domain and name.

        Parameters
        ----------
        domain : str
            Policy domain (e.g., "cpg", "normalize", "extract").
        name : str | None
            Policy name; ``None`` returns ``None``.

        Returns
        -------
        AmbiguityPolicy | None
            Resolved policy or ``None`` if name is ``None``.

        Raises
        ------
        KeyError
            Raised when the domain or policy name is unknown.
        """
        if name is None:
            return None
        domain_policies = self.ambiguity.get(domain)
        if domain_policies is None:
            msg = f"Unknown ambiguity policy domain: {domain!r}."
            raise KeyError(msg)
        try:
            return domain_policies[name]
        except KeyError as exc:
            msg = f"Unknown ambiguity policy {name!r} for domain {domain!r}."
            raise KeyError(msg) from exc


__all__ = ["PolicyRegistry", "RuleDomain"]

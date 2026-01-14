"""Central policy registry for rule resolution."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Literal, overload

from normalize.rule_model import AmbiguityPolicy as NormalizeAmbiguityPolicy
from normalize.rule_model import ConfidencePolicy as NormalizeConfidencePolicy
from relspec.model import AmbiguityPolicy as RelationshipAmbiguityPolicy
from relspec.model import ConfidencePolicy as RelationshipConfidencePolicy
from relspec.policy_registry import (
    AMBIGUITY_POLICIES as RELSPEC_AMBIGUITY_POLICIES,
)
from relspec.policy_registry import (
    CONFIDENCE_POLICIES as RELSPEC_CONFIDENCE_POLICIES,
)
from relspec.rules.definitions import RuleDomain

NORMALIZE_CONFIDENCE_POLICIES: dict[str, NormalizeConfidencePolicy] = {
    "bytecode": NormalizeConfidencePolicy(base=1.0),
    "cst": NormalizeConfidencePolicy(base=1.0),
    "diagnostic": NormalizeConfidencePolicy(base=1.0),
    "evidence": NormalizeConfidencePolicy(base=1.0),
    "scip": NormalizeConfidencePolicy(base=1.0),
    "span": NormalizeConfidencePolicy(base=1.0),
    "type": NormalizeConfidencePolicy(base=1.0),
}

NORMALIZE_AMBIGUITY_POLICIES: dict[str, NormalizeAmbiguityPolicy] = {
    "preserve": NormalizeAmbiguityPolicy(),
}


def _default_confidence_policies() -> Mapping[RuleDomain, Mapping[str, object]]:
    return {
        "cpg": RELSPEC_CONFIDENCE_POLICIES,
        "normalize": NORMALIZE_CONFIDENCE_POLICIES,
        "extract": {},
    }


def _default_ambiguity_policies() -> Mapping[RuleDomain, Mapping[str, object]]:
    return {
        "cpg": RELSPEC_AMBIGUITY_POLICIES,
        "normalize": NORMALIZE_AMBIGUITY_POLICIES,
        "extract": {},
    }


@dataclass(frozen=True)
class PolicyRegistry:
    """Domain-aware policy registry."""

    confidence_policies: Mapping[RuleDomain, Mapping[str, object]] = field(
        default_factory=_default_confidence_policies
    )
    ambiguity_policies: Mapping[RuleDomain, Mapping[str, object]] = field(
        default_factory=_default_ambiguity_policies
    )

    @overload
    def resolve_confidence(
        self, domain: Literal["cpg"], name: str | None
    ) -> RelationshipConfidencePolicy | None: ...

    @overload
    def resolve_confidence(
        self, domain: Literal["normalize"], name: str | None
    ) -> NormalizeConfidencePolicy | None: ...

    @overload
    def resolve_confidence(self, domain: Literal["extract"], name: str | None) -> None: ...

    def resolve_confidence(self, domain: RuleDomain, name: str | None) -> object | None:
        """Return a confidence policy for a domain/name pair.

        Returns
        -------
        object | None
            Policy instance, or None if name is None.

        Raises
        ------
        KeyError
            Raised when the policy name is unknown.
        """
        if name is None:
            return None
        policies = self.confidence_policies.get(domain, {})
        policy = policies.get(name)
        if policy is None:
            msg = f"Unknown confidence policy {name!r} for domain {domain!r}."
            raise KeyError(msg)
        return policy

    @overload
    def resolve_ambiguity(
        self, domain: Literal["cpg"], name: str | None
    ) -> RelationshipAmbiguityPolicy | None: ...

    @overload
    def resolve_ambiguity(
        self, domain: Literal["normalize"], name: str | None
    ) -> NormalizeAmbiguityPolicy | None: ...

    @overload
    def resolve_ambiguity(self, domain: Literal["extract"], name: str | None) -> None: ...

    def resolve_ambiguity(self, domain: RuleDomain, name: str | None) -> object | None:
        """Return an ambiguity policy for a domain/name pair.

        Returns
        -------
        object | None
            Policy instance, or None if name is None.

        Raises
        ------
        KeyError
            Raised when the policy name is unknown.
        """
        if name is None:
            return None
        policies = self.ambiguity_policies.get(domain, {})
        policy = policies.get(name)
        if policy is None:
            msg = f"Unknown ambiguity policy {name!r} for domain {domain!r}."
            raise KeyError(msg)
        return policy


__all__ = ["PolicyRegistry"]

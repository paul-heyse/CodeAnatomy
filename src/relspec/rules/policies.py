"""Central policy registry for rule resolution."""

from __future__ import annotations

import importlib
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Literal, overload

from relspec.model import AmbiguityPolicy as RelationshipAmbiguityPolicy
from relspec.model import ConfidencePolicy as RelationshipConfidencePolicy
from relspec.model import WinnerSelectConfig
from relspec.rules.definitions import RuleDomain

if TYPE_CHECKING:
    from relspec.normalize.rule_model import AmbiguityPolicy as NormalizeAmbiguityPolicy
    from relspec.normalize.rule_model import ConfidencePolicy as NormalizeConfidencePolicy

RELSPEC_CONFIDENCE_POLICIES: Mapping[str, RelationshipConfidencePolicy] = {
    "scip": RelationshipConfidencePolicy(base=1.0),
    "qname_fallback": RelationshipConfidencePolicy(base=0.4, penalty=0.1),
    "runtime": RelationshipConfidencePolicy(base=1.0),
    "type": RelationshipConfidencePolicy(base=1.0),
}

RELSPEC_AMBIGUITY_POLICIES: Mapping[str, RelationshipAmbiguityPolicy] = {
    "qname_fallback": RelationshipAmbiguityPolicy(
        winner_select=WinnerSelectConfig(
            keys=("call_id",), score_col="score", score_order="descending"
        )
    ),
}


def _normalize_confidence_policies() -> Mapping[str, object]:
    module = importlib.import_module("relspec.normalize.rule_model")
    policy_cls = module.ConfidencePolicy
    return {
        "bytecode": policy_cls(base=1.0),
        "cst": policy_cls(base=1.0),
        "diagnostic": policy_cls(base=1.0),
        "evidence": policy_cls(base=1.0),
        "scip": policy_cls(base=1.0),
        "span": policy_cls(base=1.0),
        "type": policy_cls(base=1.0),
    }


def _normalize_ambiguity_policies() -> Mapping[str, object]:
    module = importlib.import_module("relspec.normalize.rule_model")
    policy_cls = module.AmbiguityPolicy
    return {"preserve": policy_cls()}


def _default_confidence_policies() -> Mapping[RuleDomain, Mapping[str, object]]:
    """Return default confidence policies by domain.

    Returns
    -------
    Mapping[RuleDomain, Mapping[str, object]]
        Default policy mapping.
    """
    return {
        "cpg": RELSPEC_CONFIDENCE_POLICIES,
        "normalize": _normalize_confidence_policies(),
        "extract": {},
    }


def _default_ambiguity_policies() -> Mapping[RuleDomain, Mapping[str, object]]:
    """Return default ambiguity policies by domain.

    Returns
    -------
    Mapping[RuleDomain, Mapping[str, object]]
        Default policy mapping.
    """
    return {
        "cpg": RELSPEC_AMBIGUITY_POLICIES,
        "normalize": _normalize_ambiguity_policies(),
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

"""Policy registries for normalize confidence and ambiguity defaults."""

from __future__ import annotations

from normalize.rule_model import AmbiguityPolicy, ConfidencePolicy

CONFIDENCE_POLICIES: dict[str, ConfidencePolicy] = {
    "bytecode": ConfidencePolicy(base=1.0),
    "cst": ConfidencePolicy(base=1.0),
    "diagnostic": ConfidencePolicy(base=1.0),
    "evidence": ConfidencePolicy(base=1.0),
    "scip": ConfidencePolicy(base=1.0),
    "span": ConfidencePolicy(base=1.0),
    "type": ConfidencePolicy(base=1.0),
}

AMBIGUITY_POLICIES: dict[str, AmbiguityPolicy] = {
    "preserve": AmbiguityPolicy(),
}


def resolve_confidence_policy(name: str) -> ConfidencePolicy:
    """Return a named confidence policy.

    Returns
    -------
    ConfidencePolicy
        Resolved confidence policy.

    Raises
    ------
    KeyError
        Raised when the policy name is unknown.
    """
    policy = CONFIDENCE_POLICIES.get(name)
    if policy is None:
        msg = f"Unknown normalize confidence policy: {name!r}."
        raise KeyError(msg)
    return policy


def resolve_ambiguity_policy(name: str) -> AmbiguityPolicy:
    """Return a named ambiguity policy.

    Returns
    -------
    AmbiguityPolicy
        Resolved ambiguity policy.

    Raises
    ------
    KeyError
        Raised when the policy name is unknown.
    """
    policy = AMBIGUITY_POLICIES.get(name)
    if policy is None:
        msg = f"Unknown normalize ambiguity policy: {name!r}."
        raise KeyError(msg)
    return policy


__all__ = [
    "AMBIGUITY_POLICIES",
    "CONFIDENCE_POLICIES",
    "resolve_ambiguity_policy",
    "resolve_confidence_policy",
]

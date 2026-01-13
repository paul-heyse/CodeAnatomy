"""Named confidence and ambiguity policy registry."""

from __future__ import annotations

from relspec.model import AmbiguityPolicy, ConfidencePolicy, WinnerSelectConfig

CONFIDENCE_POLICIES: dict[str, ConfidencePolicy] = {
    "scip": ConfidencePolicy(base=1.0),
    "qname_fallback": ConfidencePolicy(base=0.4, penalty=0.1),
    "runtime": ConfidencePolicy(base=1.0),
    "type": ConfidencePolicy(base=1.0),
}

AMBIGUITY_POLICIES: dict[str, AmbiguityPolicy] = {
    "qname_fallback": AmbiguityPolicy(
        winner_select=WinnerSelectConfig(
            keys=("call_id",), score_col="score", score_order="descending"
        )
    ),
}


def resolve_confidence_policy(name: str | None) -> ConfidencePolicy | None:
    """Return a confidence policy by name.

    Parameters
    ----------
    name:
        Policy name to resolve.

    Returns
    -------
    ConfidencePolicy | None
        Policy for the name, or None if not provided.

    Raises
    ------
    ValueError
        Raised when the policy name is unknown.
    """
    if name is None:
        return None
    policy = CONFIDENCE_POLICIES.get(name)
    if policy is None:
        msg = f"Unknown confidence policy: {name!r}."
        raise ValueError(msg)
    return policy


def resolve_ambiguity_policy(name: str | None) -> AmbiguityPolicy | None:
    """Return an ambiguity policy by name.

    Parameters
    ----------
    name:
        Policy name to resolve.

    Returns
    -------
    AmbiguityPolicy | None
        Policy for the name, or None if not provided.

    Raises
    ------
    ValueError
        Raised when the policy name is unknown.
    """
    if name is None:
        return None
    policy = AMBIGUITY_POLICIES.get(name)
    if policy is None:
        msg = f"Unknown ambiguity policy: {name!r}."
        raise ValueError(msg)
    return policy


__all__ = [
    "AMBIGUITY_POLICIES",
    "CONFIDENCE_POLICIES",
    "resolve_ambiguity_policy",
    "resolve_confidence_policy",
]

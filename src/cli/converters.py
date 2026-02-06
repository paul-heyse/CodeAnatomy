"""Type converters for CLI inputs."""

from __future__ import annotations

from core_types import DeterminismTier

DETERMINISM_ALIASES: dict[str, DeterminismTier] = {
    "tier2": DeterminismTier.CANONICAL,
    "canonical": DeterminismTier.CANONICAL,
    "tier1": DeterminismTier.STABLE_SET,
    "stable": DeterminismTier.STABLE_SET,
    "stable_set": DeterminismTier.STABLE_SET,
    "tier0": DeterminismTier.BEST_EFFORT,
    "fast": DeterminismTier.BEST_EFFORT,
    "best_effort": DeterminismTier.BEST_EFFORT,
}


def resolve_determinism_alias(value: str | None) -> DeterminismTier | None:
    """Resolve determinism tier aliases to the canonical enum.

    Parameters
    ----------
    value
        Determinism tier alias string.

    Returns:
    -------
    DeterminismTier | None
        Resolved determinism tier or None when not provided.
    """
    if value is None:
        return None
    return DETERMINISM_ALIASES.get(value.strip().lower())


__all__ = ["DETERMINISM_ALIASES", "resolve_determinism_alias"]

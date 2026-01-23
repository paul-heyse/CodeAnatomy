"""Normalize rule spec models."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class NormalizeRuleFamilySpec:
    """Specification for a normalize rule family.

    Note: The ``inputs`` field has been removed. Dependencies are now inferred
    from Ibis/DataFusion expression analysis rather than declared manually.
    """

    name: str
    factory: str
    output: str | None = None
    confidence_policy: str | None = None
    ambiguity_policy: str | None = None
    option_flag: str | None = None
    execution_mode: str | None = None


__all__ = ["NormalizeRuleFamilySpec"]

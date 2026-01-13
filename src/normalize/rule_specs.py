"""Normalize rule spec models."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class NormalizeRuleFamilySpec:
    """Specification for a normalize rule family."""

    name: str
    factory: str
    inputs: tuple[str, ...] = ()
    output: str | None = None
    confidence_policy: str | None = None
    ambiguity_policy: str | None = None
    option_flag: str | None = None
    execution_mode: str | None = None


__all__ = ["NormalizeRuleFamilySpec"]

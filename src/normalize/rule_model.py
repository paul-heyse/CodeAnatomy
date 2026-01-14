"""Normalize rule models and policy specifications."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Literal

from arrowdsl.compute.expr_core import ScalarValue
from arrowdsl.plan.catalog import PlanDeriver
from arrowdsl.plan.ops import SortKey
from arrowdsl.plan.query import QuerySpec


@dataclass(frozen=True)
class EvidenceSpec:
    """Evidence requirements for normalize rules."""

    sources: tuple[str, ...] = ()
    required_columns: tuple[str, ...] = ()
    required_types: Mapping[str, str] = field(default_factory=dict)
    required_metadata: Mapping[bytes, bytes] = field(default_factory=dict)


@dataclass(frozen=True)
class EvidenceOutput:
    """Evidence projection hints for canonical evidence output."""

    column_map: Mapping[str, str] = field(default_factory=dict)
    literals: Mapping[str, ScalarValue] = field(default_factory=dict)


@dataclass(frozen=True)
class ConfidencePolicy:
    """Policy for computing confidence scores."""

    base: float = 0.5
    source_weight: Mapping[str, float] = field(default_factory=dict)
    penalty: float = 0.0


@dataclass(frozen=True)
class WinnerSelectConfig:
    """Configuration for selecting winners among ambiguous groups."""

    keys: tuple[str, ...] = ()
    score_col: str = "confidence"
    score_order: Literal["ascending", "descending"] = "descending"
    tie_breakers: tuple[SortKey, ...] = ()


@dataclass(frozen=True)
class AmbiguityPolicy:
    """Policy for ambiguity resolution."""

    winner_select: WinnerSelectConfig | None = None
    tie_breakers: tuple[SortKey, ...] = ()


@dataclass(frozen=True)
class NormalizeRule:
    """Declarative normalize rule configuration."""

    name: str
    output: str
    inputs: tuple[str, ...] = ()
    derive: PlanDeriver | None = None
    query: QuerySpec | None = None
    evidence: EvidenceSpec | None = None
    evidence_output: EvidenceOutput | None = None
    confidence_policy: ConfidencePolicy | None = None
    ambiguity_policy: AmbiguityPolicy | None = None
    metadata_extra: Mapping[bytes, bytes] | None = None
    priority: int = 100
    emit_rule_meta: bool = True
    execution_mode: ExecutionMode = "auto"

    def __post_init__(self) -> None:
        """Validate rule invariants.

        Raises
        ------
        ValueError
            Raised when required fields are empty or duplicate inputs exist.
        """
        if not self.name:
            msg = "NormalizeRule.name must be non-empty."
            raise ValueError(msg)
        if not self.output:
            msg = "NormalizeRule.output must be non-empty."
            raise ValueError(msg)
        if len(set(self.inputs)) != len(self.inputs):
            msg = f"NormalizeRule.inputs contains duplicates for rule {self.name!r}."
            raise ValueError(msg)
        if self.execution_mode not in {"auto", "plan", "table"}:
            msg = f"NormalizeRule.execution_mode is invalid: {self.execution_mode!r}."
            raise ValueError(msg)


__all__ = [
    "AmbiguityPolicy",
    "ConfidencePolicy",
    "EvidenceOutput",
    "EvidenceSpec",
    "ExecutionMode",
    "NormalizeRule",
    "WinnerSelectConfig",
]
ExecutionMode = Literal["auto", "plan", "table", "external", "hybrid"]

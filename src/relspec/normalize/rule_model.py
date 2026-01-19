"""Normalize rule models and policy specifications."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field

from arrowdsl.core.expr_types import ScalarValue
from ibis_engine.query_compiler import IbisQuerySpec
from relspec.model import (
    AmbiguityPolicy,
    ConfidencePolicy,
    EvidenceSpec,
    ExecutionMode,
    WinnerSelectConfig,
)


@dataclass(frozen=True)
class EvidenceOutput:
    """Evidence projection hints for canonical evidence output."""

    column_map: Mapping[str, str] = field(default_factory=dict)
    literals: Mapping[str, ScalarValue] = field(default_factory=dict)
    provenance_columns: tuple[str, ...] = ()


@dataclass(frozen=True)
class NormalizeRule:
    """Declarative normalize rule configuration."""

    name: str
    output: str
    inputs: tuple[str, ...] = ()
    ibis_builder: str | None = None
    query: IbisQuerySpec | None = None
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

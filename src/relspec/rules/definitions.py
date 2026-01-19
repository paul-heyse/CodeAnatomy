"""Central rule definition models for relspec-driven pipelines."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Literal

from arrowdsl.core.context import OrderingKey
from arrowdsl.core.expr_types import ScalarValue
from arrowdsl.spec.expr_ir import ExprIR
from extract.spec_tables import ExtractDerivedIdSpec
from ibis_engine.query_compiler import IbisQuerySpec
from relspec.model import (
    HashJoinConfig,
    IntervalAlignConfig,
    KernelSpecT,
    ProjectConfig,
    WinnerSelectConfig,
)
from relspec.rules.rel_ops import validate_rel_ops

if TYPE_CHECKING:
    from relspec.rules.rel_ops import RelOpT

RuleDomain = Literal["cpg", "normalize", "extract"]
ExecutionMode = Literal["auto", "plan", "table", "external", "hybrid"]
RuleStageMode = Literal["source", "plan", "post_kernel", "finalize"]


@dataclass(frozen=True)
class EvidenceSpec:
    """Evidence requirements for centralized rules."""

    sources: tuple[str, ...] = ()
    required_columns: tuple[str, ...] = ()
    required_types: Mapping[str, str] = field(default_factory=dict)
    required_metadata: Mapping[bytes, bytes] = field(default_factory=dict)


@dataclass(frozen=True)
class EvidenceOutput:
    """Evidence projection hints for centralized rules."""

    column_map: Mapping[str, str] = field(default_factory=dict)
    literals: Mapping[str, ScalarValue] = field(default_factory=dict)
    provenance_columns: tuple[str, ...] = ()


@dataclass(frozen=True)
class PolicyOverrides:
    """Policy override values attached to a rule definition."""

    confidence_policy: str | None = None
    ambiguity_policy: str | None = None


@dataclass(frozen=True)
class EdgeEmitPayload:
    """Edge emission payload embedded in relationship rules."""

    edge_kind: str
    src_cols: tuple[str, ...]
    dst_cols: tuple[str, ...]
    origin: str
    resolution_method: str
    option_flag: str
    path_cols: tuple[str, ...] = ("path",)
    bstart_cols: tuple[str, ...] = ("bstart",)
    bend_cols: tuple[str, ...] = ("bend",)


@dataclass(frozen=True)
class RelationshipPayload:
    """Relationship-specific rule payload."""

    output_dataset: str | None = None
    contract_name: str | None = None
    hash_join: HashJoinConfig | None = None
    interval_align: IntervalAlignConfig | None = None
    winner_select: WinnerSelectConfig | None = None
    predicate: ExprIR | None = None
    project: ProjectConfig | None = None
    rule_name_col: str = "rule_name"
    rule_priority_col: str = "rule_priority"
    edge_emit: EdgeEmitPayload | None = None


@dataclass(frozen=True)
class NormalizePayload:
    """Normalize-specific rule payload."""

    plan_builder: str | None = None
    query: IbisQuerySpec | None = None


@dataclass(frozen=True)
class ExtractPayload:
    """Extract-specific rule payload."""

    version: int = 1
    template: str | None = None
    bundles: tuple[str, ...] = ()
    fields: tuple[str, ...] = ()
    derived_ids: tuple[ExtractDerivedIdSpec, ...] = ()
    row_fields: tuple[str, ...] = ()
    row_extras: tuple[str, ...] = ()
    ordering_keys: tuple[OrderingKey, ...] = ()
    join_keys: tuple[str, ...] = ()
    enabled_when: str | None = None
    feature_flag: str | None = None
    postprocess: str | None = None
    metadata_extra: Mapping[bytes, bytes] = field(default_factory=dict)
    evidence_required_columns: tuple[str, ...] = ()
    pipeline_name: str | None = None


RulePayload = RelationshipPayload | NormalizePayload | ExtractPayload


@dataclass(frozen=True)
class RuleStage:
    """Stage metadata for optional rule execution."""

    name: str
    mode: RuleStageMode
    enabled_when: str | None = None


@dataclass(frozen=True)
class RuleDefinition:
    """Centralized rule definition across CPG, normalize, and extract."""

    name: str
    domain: RuleDomain
    kind: str
    inputs: tuple[str, ...]
    output: str
    execution_mode: ExecutionMode = "auto"
    priority: int = 100
    evidence: EvidenceSpec | None = None
    evidence_output: EvidenceOutput | None = None
    policy_overrides: PolicyOverrides = field(default_factory=PolicyOverrides)
    emit_rule_meta: bool = True
    rel_ops: tuple[RelOpT, ...] = ()
    post_kernels: tuple[KernelSpecT, ...] = ()
    stages: tuple[RuleStage, ...] = ()
    payload: RulePayload | None = None

    def __post_init__(self) -> None:
        """Validate central rule definition invariants.

        Raises
        ------
        ValueError
            Raised when required fields are missing or invalid.
        """
        if not self.name:
            msg = "RuleDefinition.name must be non-empty."
            raise ValueError(msg)
        if not self.output:
            msg = "RuleDefinition.output must be non-empty."
            raise ValueError(msg)
        if len(set(self.inputs)) != len(self.inputs):
            msg = f"RuleDefinition.inputs contains duplicates for {self.name!r}."
            raise ValueError(msg)
        if self.domain not in {"cpg", "normalize", "extract"}:
            msg = f"RuleDefinition.domain is invalid: {self.domain!r}."
            raise ValueError(msg)
        if self.rel_ops:
            validate_rel_ops(self.rel_ops)


def stage_enabled(stage: RuleStage, options: Mapping[str, object]) -> bool:
    """Return whether a stage is enabled for the provided options.

    Returns
    -------
    bool
        ``True`` when the stage is enabled.
    """
    if stage.enabled_when is None:
        return True
    if stage.enabled_when == "allowlist":
        allowlist = options.get("module_allowlist", ())
        return bool(allowlist)
    if stage.enabled_when.startswith("feature_flag:"):
        flag = stage.enabled_when.split(":", 1)[1]
        value = options.get(flag)
        return value if isinstance(value, bool) else bool(value)
    value = options.get(stage.enabled_when)
    if isinstance(value, bool):
        return value
    return bool(value)


__all__ = [
    "EdgeEmitPayload",
    "EvidenceOutput",
    "EvidenceSpec",
    "ExecutionMode",
    "ExtractPayload",
    "NormalizePayload",
    "PolicyOverrides",
    "RelationshipPayload",
    "RuleDefinition",
    "RuleDomain",
    "RulePayload",
    "RuleStage",
    "RuleStageMode",
    "stage_enabled",
]

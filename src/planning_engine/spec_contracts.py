"""Typed Rust execution-spec contracts consumed by Python adapters.

This module intentionally excludes spec derivation logic. Rust remains the sole
authority for building spec payloads via ``SemanticPlanCompiler.build_spec_json``.
"""

from __future__ import annotations

from typing import Literal

import msgspec

type RuleClass = Literal[
    "SemanticIntegrity",
    "DeltaScanAware",
    "CostShape",
    "Safety",
]
type RulepackProfile = Literal["Default", "LowLatency", "Replay", "Strict"]
type RuntimeTunerMode = Literal["Off", "Observe", "Apply"]
type PushdownEnforcementMode = Literal["warn", "strict", "disabled"]
type ExtensionGovernanceMode = Literal["strict_allowlist", "warn_on_unregistered", "permissive"]
type RuleTraceMode = Literal["Disabled", "PhaseOnly", "Full"]
type PreviewRedactionMode = Literal["None", "DenyList", "AllowList"]
type OtlpProtocol = Literal["grpc", "http/protobuf", "http/json"]
type TracingPreset = Literal["Maximal", "MaximalNoData", "ProductionLean"]
type MaterializationMode = Literal["Append", "Overwrite"]


class InputRelation(msgspec.Struct, frozen=True):
    """Delta table input relation for the execution spec."""

    logical_name: str
    delta_location: str
    requires_lineage: bool = False
    version_pin: int | None = None


class ViewDefinition(msgspec.Struct, frozen=True):
    """Named view in the logical execution DAG."""

    name: str
    view_kind: str
    view_dependencies: tuple[str, ...] = ()
    transform: dict[str, object] = msgspec.field(default_factory=dict)
    output_schema: dict[str, object] = msgspec.field(default_factory=dict)


class RuleIntent(msgspec.Struct, frozen=True):
    """Declarative rule intent for analyzer/optimizer/physical rules."""

    name: str
    rule_class: RuleClass = msgspec.field(name="class")
    params: dict[str, object] = msgspec.field(default_factory=dict)


class OutputTarget(msgspec.Struct, frozen=True):
    """Delta table materialization target."""

    table_name: str
    source_view: str
    columns: tuple[str, ...] = ()
    delta_location: str | None = None
    materialization_mode: MaterializationMode = "Overwrite"
    partition_by: tuple[str, ...] = ()
    write_metadata: dict[str, str] = msgspec.field(default_factory=dict)
    max_commit_retries: int | None = None


class JoinEdge(msgspec.Struct, frozen=True):
    """Join graph edge descriptor."""

    left_relation: str
    right_relation: str
    join_type: str
    left_keys: tuple[str, ...] = ()
    right_keys: tuple[str, ...] = ()


class JoinGraph(msgspec.Struct, frozen=True):
    """Join graph container."""

    edges: tuple[JoinEdge, ...] = ()
    constraints: tuple[dict[str, object], ...] = ()


class TraceExportPolicy(msgspec.Struct, frozen=True):
    """OpenTelemetry batch/sampling throughput controls."""

    traces_sampler: str = "parentbased_always_on"
    traces_sampler_arg: str | None = None
    bsp_max_queue_size: int = 2048
    bsp_max_export_batch_size: int = 512
    bsp_schedule_delay_ms: int = 5000
    bsp_export_timeout_ms: int = 30000


class TracingConfig(msgspec.Struct, frozen=True):
    """Tracing control plane for DataFusion + OTel wiring."""

    enabled: bool = False
    record_metrics: bool = True
    rule_mode: RuleTraceMode = "Disabled"
    plan_diff: bool = False
    preview_limit: int = 0
    preview_redaction_mode: PreviewRedactionMode = "None"
    preview_redacted_columns: tuple[str, ...] = ()
    preview_redaction_token: str = "[REDACTED]"
    preview_max_width: int = 96
    preview_max_row_height: int = 4
    preview_min_compacted_col_width: int = 10
    instrument_object_store: bool = False
    otlp_endpoint: str | None = None
    otlp_protocol: OtlpProtocol | None = None
    otel_service_name: str | None = None
    otel_resource_attributes: dict[str, str] = msgspec.field(default_factory=dict)
    custom_span_fields: dict[str, str] = msgspec.field(default_factory=dict)
    export_policy: TraceExportPolicy = msgspec.field(default_factory=TraceExportPolicy)


def _default_tracing_config() -> TracingConfig:
    return TracingConfig()


class RuntimeConfig(msgspec.Struct, frozen=True):
    """Optional runtime controls for compliance and tuning side channels."""

    compliance_capture: bool = False
    tuner_mode: RuntimeTunerMode = "Off"
    capture_substrait: bool = False
    capture_optimizer_lab: bool = False
    capture_delta_codec: bool = False
    pushdown_enforcement_mode: PushdownEnforcementMode = "warn"
    extension_governance_mode: ExtensionGovernanceMode = "permissive"
    enable_tracing: bool = False
    enable_rule_tracing: bool = False
    enable_plan_preview: bool = False
    enable_function_factory: bool = False
    enable_domain_planner: bool = False
    lineage_tags: dict[str, str] = msgspec.field(default_factory=dict)
    tracing_preset: TracingPreset | None = None
    tracing: TracingConfig = msgspec.field(default_factory=_default_tracing_config)


class SemanticExecutionSpec(msgspec.Struct, frozen=True):
    """Complete execution contract for the Rust engine."""

    version: int
    input_relations: tuple[InputRelation, ...]
    view_definitions: tuple[ViewDefinition, ...]
    join_graph: JoinGraph
    output_targets: tuple[OutputTarget, ...]
    rule_intents: tuple[RuleIntent, ...]
    rulepack_profile: RulepackProfile
    typed_parameters: tuple[dict[str, object], ...] = ()
    runtime: RuntimeConfig = msgspec.field(default_factory=RuntimeConfig)
    spec_hash: bytes = b""


__all__ = [
    "InputRelation",
    "JoinEdge",
    "JoinGraph",
    "OutputTarget",
    "RuleClass",
    "RuleIntent",
    "RulepackProfile",
    "RuntimeConfig",
    "SemanticExecutionSpec",
    "TracingConfig",
    "TracingPreset",
]

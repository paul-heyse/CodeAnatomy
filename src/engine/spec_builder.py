"""Spec builder structs mirroring the Rust SemanticExecutionSpec contract."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Literal

import msgspec

if TYPE_CHECKING:
    from semantics.ir import SemanticIR, SemanticIRJoinGroup, SemanticIRView


type RuleClass = Literal[
    "SemanticIntegrity",
    "DeltaScanAware",
    "CostShape",
    "Safety",
]
type RulepackProfile = Literal["Default", "LowLatency", "Replay", "Strict"]
type RuntimeTunerMode = Literal["Off", "Observe", "Apply"]
type JoinType = Literal["Inner", "Left", "Right", "Full", "Semi", "Anti"]
type MaterializationMode = Literal["Append", "Overwrite"]
_MIN_RELATE_INPUTS = 2


class InputRelation(msgspec.Struct, frozen=True):
    """Delta table input relation for the execution spec."""

    logical_name: str
    delta_location: str
    requires_lineage: bool = False
    version_pin: int | None = None


class JoinKeyPair(msgspec.Struct, frozen=True):
    """Pair of join keys for relate transforms."""

    left_key: str
    right_key: str


class AggregationExpr(msgspec.Struct, frozen=True):
    """Aggregate expression descriptor."""

    column: str
    function: str
    alias: str


class NormalizeTransform(msgspec.Struct, frozen=True, tag="Normalize", tag_field="kind"):
    """Normalization transform."""

    source: str
    id_columns: tuple[str, ...] = ()
    span_columns: tuple[str, str] | None = None
    text_columns: tuple[str, ...] = ()


class RelateTransform(msgspec.Struct, frozen=True, tag="Relate", tag_field="kind"):
    """Relational join transform."""

    left: str
    right: str
    join_type: JoinType
    join_keys: tuple[JoinKeyPair, ...]


class UnionTransform(msgspec.Struct, frozen=True, tag="Union", tag_field="kind"):
    """Union transform."""

    sources: tuple[str, ...]
    discriminator_column: str | None = None
    distinct: bool = False


class ProjectTransform(msgspec.Struct, frozen=True, tag="Project", tag_field="kind"):
    """Projection transform."""

    source: str
    columns: tuple[str, ...]


class FilterTransform(msgspec.Struct, frozen=True, tag="Filter", tag_field="kind"):
    """Filter transform."""

    source: str
    predicate: str


class AggregateTransform(msgspec.Struct, frozen=True, tag="Aggregate", tag_field="kind"):
    """Aggregation transform."""

    source: str
    group_by: tuple[str, ...]
    aggregations: tuple[AggregationExpr, ...]


class IncrementalCdfTransform(msgspec.Struct, frozen=True, tag="IncrementalCdf", tag_field="kind"):
    """Incremental CDF transform for Delta change data feed."""

    source: str
    starting_version: int | None = None
    ending_version: int | None = None
    starting_timestamp: str | None = None
    ending_timestamp: str | None = None


class MetadataTransform(msgspec.Struct, frozen=True, tag="Metadata", tag_field="kind"):
    """Delta table metadata transform."""

    source: str


class FileManifestTransform(msgspec.Struct, frozen=True, tag="FileManifest", tag_field="kind"):
    """Delta table file manifest transform."""

    source: str


type ViewTransform = (
    NormalizeTransform
    | RelateTransform
    | UnionTransform
    | ProjectTransform
    | FilterTransform
    | AggregateTransform
    | IncrementalCdfTransform
    | MetadataTransform
    | FileManifestTransform
)


class SchemaContract(msgspec.Struct, frozen=True):
    """Output schema contract for a view definition."""

    columns: dict[str, str] = msgspec.field(default_factory=dict)


class ViewDefinition(msgspec.Struct, frozen=True):
    """Named view in the logical plan DAG."""

    name: str
    view_kind: str
    view_dependencies: tuple[str, ...]
    transform: ViewTransform
    output_schema: SchemaContract = msgspec.field(default_factory=SchemaContract)


class RuleIntent(msgspec.Struct, frozen=True):
    """Declarative rule intent for analyzer/optimizer/physical rules."""

    name: str
    rule_class: RuleClass = msgspec.field(name="class")
    params: dict[str, object] = msgspec.field(default_factory=dict)


class OutputTarget(msgspec.Struct, frozen=True):
    """Delta table materialization target."""

    table_name: str
    source_view: str
    columns: tuple[str, ...]
    delta_location: str | None = None
    materialization_mode: MaterializationMode = "Overwrite"
    partition_by: tuple[str, ...] = ()
    write_metadata: dict[str, str] = msgspec.field(default_factory=dict)
    max_commit_retries: int | None = None


class ParameterTemplate(msgspec.Struct, frozen=True):
    """Runtime parameter template."""

    name: str
    base_table: str
    filter_column: str
    parameter_type: str


class JoinEdge(msgspec.Struct, frozen=True):
    """Join graph edge descriptor."""

    left_relation: str
    right_relation: str
    join_type: JoinType
    left_keys: tuple[str, ...]
    right_keys: tuple[str, ...]


class JoinConstraint(msgspec.Struct, frozen=True):
    """Join graph constraint descriptor."""

    name: str
    constraint_type: str
    relations: tuple[str, ...]
    columns: tuple[str, ...]


class JoinGraph(msgspec.Struct, frozen=True):
    """Join graph container."""

    edges: tuple[JoinEdge, ...] = ()
    constraints: tuple[JoinConstraint, ...] = ()


class RuntimeConfig(msgspec.Struct, frozen=True):
    """Optional runtime controls for compliance and tuning side channels."""

    compliance_capture: bool = False
    tuner_mode: RuntimeTunerMode = "Off"
    capture_substrait: bool = False
    enable_tracing: bool = False
    enable_rule_tracing: bool = False
    enable_plan_preview: bool = False
    enable_function_factory: bool = False
    enable_domain_planner: bool = False


class SemanticExecutionSpec(msgspec.Struct, frozen=True):
    """Complete execution contract for the Rust engine."""

    version: int
    input_relations: tuple[InputRelation, ...]
    view_definitions: tuple[ViewDefinition, ...]
    join_graph: JoinGraph
    output_targets: tuple[OutputTarget, ...]
    rule_intents: tuple[RuleIntent, ...]
    rulepack_profile: RulepackProfile
    parameter_templates: tuple[ParameterTemplate, ...] = ()
    runtime: RuntimeConfig = msgspec.field(default_factory=RuntimeConfig)
    spec_hash: bytes = b""


def _map_join_type(value: str) -> JoinType:
    normalized = value.strip().lower()
    if normalized == "inner":
        return "Inner"
    if normalized == "left":
        return "Left"
    if normalized == "right":
        return "Right"
    if normalized == "full":
        return "Full"
    if normalized == "semi":
        return "Semi"
    if normalized == "anti":
        return "Anti"
    msg = f"Unsupported join type: {value!r}"
    raise ValueError(msg)


def _canonical_rulepack_profile(value: str) -> RulepackProfile:
    normalized = value.strip().lower()
    mapping: dict[str, RulepackProfile] = {
        "default": "Default",
        "low_latency": "LowLatency",
        "lowlatency": "LowLatency",
        "replay": "Replay",
        "strict": "Strict",
    }
    return mapping.get(normalized, "Default")


def _group_index(ir: SemanticIR) -> tuple[dict[str, SemanticIRJoinGroup], tuple[JoinEdge, ...]]:
    by_relationship: dict[str, SemanticIRJoinGroup] = {}
    edges: list[JoinEdge] = []
    for group in ir.join_groups:
        for rel in group.relationship_names:
            by_relationship[rel] = group
        edges.append(
            JoinEdge(
                left_relation=group.left_view,
                right_relation=group.right_view,
                join_type=_map_join_type(group.how),
                left_keys=tuple(group.left_on),
                right_keys=tuple(group.right_on),
            )
        )
    return by_relationship, tuple(edges)


def _build_relate_transform(
    view: SemanticIRView,
    group_by_relationship: Mapping[str, SemanticIRJoinGroup],
) -> RelateTransform:
    group = group_by_relationship.get(view.name)
    if group is not None:
        join_keys = tuple(
            JoinKeyPair(left_key=left_key, right_key=right_key)
            for left_key, right_key in zip(group.left_on, group.right_on, strict=False)
        )
        return RelateTransform(
            left=group.left_view,
            right=group.right_view,
            join_type=_map_join_type(group.how),
            join_keys=join_keys,
        )
    if len(view.inputs) < _MIN_RELATE_INPUTS:
        msg = f"relate view {view.name!r} must have two inputs"
        raise ValueError(msg)
    return RelateTransform(
        left=view.inputs[0],
        right=view.inputs[1],
        join_type="Inner",
        join_keys=(),
    )


def _build_transform(
    view: SemanticIRView,
    group_by_relationship: Mapping[str, SemanticIRJoinGroup],
) -> ViewTransform:
    if view.kind == "relate":
        return _build_relate_transform(view, group_by_relationship)
    if view.kind.startswith("union") or len(view.inputs) > 1:
        return UnionTransform(sources=tuple(view.inputs), discriminator_column=None, distinct=False)
    if len(view.inputs) == 1 and view.kind in {"projection", "project"}:
        return ProjectTransform(source=view.inputs[0], columns=())
    if len(view.inputs) == 1 and view.kind in {"aggregate"}:
        return AggregateTransform(source=view.inputs[0], group_by=(), aggregations=())
    if len(view.inputs) == 1 and view.kind.endswith("normalize"):
        return NormalizeTransform(
            source=view.inputs[0], id_columns=(), span_columns=None, text_columns=()
        )
    if len(view.inputs) == 1:
        return FilterTransform(source=view.inputs[0], predicate="TRUE")
    msg = f"Unable to derive transform for view {view.name!r} with kind {view.kind!r}"
    raise ValueError(msg)


def _default_rule_intents(profile: RulepackProfile) -> tuple[RuleIntent, ...]:
    baseline = [
        RuleIntent(name="semantic_integrity", rule_class="SemanticIntegrity"),
        RuleIntent(name="span_containment_rewrite", rule_class="SemanticIntegrity"),
        RuleIntent(name="delta_scan_aware", rule_class="DeltaScanAware"),
        RuleIntent(name="cpg_physical", rule_class="SemanticIntegrity"),
        RuleIntent(name="cost_shape", rule_class="CostShape"),
    ]
    if profile in {"Strict", "Replay"}:
        baseline.append(RuleIntent(name="strict_safety", rule_class="Safety"))
    return tuple(baseline)


def build_execution_spec(
    ir: SemanticIR,
    input_locations: Mapping[str, str],
    output_targets: Sequence[str],
    rulepack_profile: str = "default",
) -> SemanticExecutionSpec:
    """Build a Rust-compatible execution spec from semantic IR.

    Args:
        ir: Semantic IR containing normalized views and join groups.
        input_locations: Mapping of logical input names to Delta locations.
        output_targets: Output table names to materialize.
        rulepack_profile: Desired rulepack profile, case-insensitive.

    Returns:
        Canonical SemanticExecutionSpec suitable for Rust deserialization.

    Raises:
        ValueError: If output targets are requested but the IR has no views.
    """
    profile = _canonical_rulepack_profile(rulepack_profile)
    group_by_relationship, join_edges = _group_index(ir)
    view_names = {view.name for view in ir.views}

    input_relations = tuple(
        InputRelation(logical_name=name, delta_location=location)
        for name, location in sorted(input_locations.items())
    )

    view_definitions = tuple(
        ViewDefinition(
            name=view.name,
            view_kind=view.kind,
            view_dependencies=tuple(
                input_name for input_name in view.inputs if input_name in view_names
            ),
            transform=_build_transform(view, group_by_relationship),
            output_schema=SchemaContract(),
        )
        for view in ir.views
    )
    if output_targets and not ir.views:
        msg = "Cannot build output targets without at least one IR view"
        raise ValueError(msg)

    default_output_source = ir.views[-1].name if ir.views else ""
    output_specs = tuple(
        OutputTarget(
            table_name=target,
            source_view=target if target in view_names else default_output_source,
            columns=(),
            materialization_mode="Overwrite",
        )
        for target in output_targets
    )

    return SemanticExecutionSpec(
        version=1,
        input_relations=input_relations,
        view_definitions=view_definitions,
        join_graph=JoinGraph(edges=join_edges, constraints=()),
        output_targets=output_specs,
        rule_intents=_default_rule_intents(profile),
        rulepack_profile=profile,
        parameter_templates=(),
        runtime=RuntimeConfig(),
    )


def build_spec_from_ir(
    ir: SemanticIR,
    input_locations: Mapping[str, str],
    output_targets: Sequence[str],
    rulepack_profile: str = "default",
) -> SemanticExecutionSpec:
    """Build a SemanticExecutionSpec using the legacy helper name.

    Args:
        ir: Semantic IR containing normalized views and join groups.
        input_locations: Mapping of logical input names to Delta locations.
        output_targets: Output table names to materialize.
        rulepack_profile: Desired rulepack profile, case-insensitive.

    Returns:
        Canonical SemanticExecutionSpec suitable for Rust deserialization.
    """
    return build_execution_spec(
        ir=ir,
        input_locations=input_locations,
        output_targets=output_targets,
        rulepack_profile=rulepack_profile,
    )

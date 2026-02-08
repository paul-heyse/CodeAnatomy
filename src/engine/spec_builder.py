"""Spec builder structs mirroring the Rust SemanticExecutionSpec."""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal

import msgspec

if TYPE_CHECKING:
    from semantics.ir import SemanticIR


class InputRelation(msgspec.Struct, frozen=True):
    """Delta table input relation for the execution spec."""

    logical_name: str
    delta_location: str
    requires_lineage: bool = False
    version_pin: int | None = None


class ViewTransform(msgspec.Struct, frozen=True):
    """View transformation descriptor (normalize, filter, project, etc.)."""

    kind: str
    params: dict[str, object]


class ViewDefinition(msgspec.Struct, frozen=True):
    """Named view in the logical plan DAG."""

    name: str
    view_kind: str
    view_dependencies: tuple[str, ...]
    transform: ViewTransform
    output_schema: dict[str, str] | None = None


class RuleIntent(msgspec.Struct, frozen=True):
    """Declarative rule intent for analyzer/optimizer/physical rules."""

    name: str
    rule_class: str
    params: dict[str, object]


class OutputTarget(msgspec.Struct, frozen=True):
    """Delta table materialization target."""

    table_name: str
    source_view: str
    columns: tuple[str, ...]
    materialization_mode: Literal["append", "overwrite"] = "overwrite"


class SemanticExecutionSpec(msgspec.Struct, frozen=True):
    """Complete execution contract for the Rust engine."""

    version: int
    input_relations: tuple[InputRelation, ...]
    view_definitions: tuple[ViewDefinition, ...]
    join_graph: dict[str, object]
    output_targets: tuple[OutputTarget, ...]
    rule_intents: tuple[RuleIntent, ...]
    rulepack_profile: str
    parameter_templates: tuple[dict[str, object], ...] = ()
    spec_hash: bytes = b""


def build_spec_from_ir(
    ir: SemanticIR,
    input_locations: dict[str, str],
    output_targets: list[str],
    rulepack_profile: str = "default",
) -> SemanticExecutionSpec:
    """Build SemanticExecutionSpec from the Python semantic IR.

    Args:
        ir: The compiled semantic IR from the upstream compiler.
        input_locations: Map of logical name -> Delta location for extraction outputs.
        output_targets: List of CPG output table names to materialize.
        rulepack_profile: One of "default", "low_latency", "replay", "strict".

    Raises:
        NotImplementedError: Not yet wired to SemanticIR.
    """
    msg = "build_spec_from_ir not yet wired to SemanticIR"
    raise NotImplementedError(msg)

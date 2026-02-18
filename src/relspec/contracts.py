"""Relationship output metadata contracts for view-driven pipelines."""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

import msgspec

from relspec.compiled_policy import JsonMapping
from relspec.metadata import (
    REL_CALLSITE_SYMBOL_NAME,
    REL_DEF_SYMBOL_NAME,
    REL_IMPORT_SYMBOL_NAME,
    REL_NAME_SYMBOL_NAME,
    rel_callsite_symbol_metadata_spec,
    rel_def_symbol_metadata_spec,
    rel_import_symbol_metadata_spec,
    rel_name_symbol_metadata_spec,
    relation_output_metadata_spec,
    relspec_metadata_spec,
)
from relspec.ports import RuntimeProfilePort
from semantics.output_names import RELATION_OUTPUT_NAME, RELATION_OUTPUT_ORDERING_KEYS

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from datafusion_engine.dataset.registry import DatasetLocation
    from datafusion_engine.views.graph import ViewNode
    from relspec.pipeline_policy import DiagnosticsPolicy
    from semantics.ir import SemanticIR


class OutDegreeGraph(Protocol):
    def out_degree(self, node_idx: int) -> int: ...


class TaskGraphLike(Protocol):
    task_idx: Mapping[str, int]
    graph: OutDegreeGraph

    def out_degree(self, task_name: str) -> int: ...


class ScanOverrideLike(Protocol):
    dataset_name: str
    policy: JsonMapping
    reasons: Sequence[str] | str
    inference_confidence: JsonMapping | None


class CompileExecutionPolicyRequestV1(msgspec.Struct, frozen=True):
    """Request envelope for execution-policy compilation."""

    task_graph: TaskGraphLike
    output_locations: dict[str, DatasetLocation]
    runtime_profile: RuntimeProfilePort
    view_nodes: tuple[ViewNode, ...] | None = None
    semantic_ir: SemanticIR | None = None
    scan_overrides: tuple[ScanOverrideLike, ...] = ()
    diagnostics_policy: DiagnosticsPolicy | None = None
    workload_class: str | None = None


__all__ = [
    "RELATION_OUTPUT_NAME",
    "RELATION_OUTPUT_ORDERING_KEYS",
    "REL_CALLSITE_SYMBOL_NAME",
    "REL_DEF_SYMBOL_NAME",
    "REL_IMPORT_SYMBOL_NAME",
    "REL_NAME_SYMBOL_NAME",
    "CompileExecutionPolicyRequestV1",
    "rel_callsite_symbol_metadata_spec",
    "rel_def_symbol_metadata_spec",
    "rel_import_symbol_metadata_spec",
    "rel_name_symbol_metadata_spec",
    "relation_output_metadata_spec",
    "relspec_metadata_spec",
]

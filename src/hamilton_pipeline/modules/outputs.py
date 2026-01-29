"""Hamilton output nodes for inference-driven pipeline."""

from __future__ import annotations

import time
import uuid
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, TypeVar, cast

import pyarrow as pa
from hamilton.function_modifiers import (
    cache,
    check_output_custom,
    datasaver,
    pipe_input,
    step,
    tag,
)

from core_types import JsonDict, JsonValue
from datafusion_engine.arrow_interop import TableLike
from datafusion_engine.diagnostics import record_artifact, recorder_for_profile
from datafusion_engine.ingest import datafusion_from_arrow
from datafusion_engine.write_pipeline import WriteFormat, WriteMode, WritePipeline, WriteRequest
from engine.runtime_profile import RuntimeProfileSpec
from hamilton_pipeline.pipeline_types import OutputConfig
from hamilton_pipeline.validators import NonEmptyTableValidator
from serde_artifacts import (
    ExtractErrorsArtifact,
    NormalizeOutputsArtifact,
    RunManifest,
    RunManifestEnvelope,
)
from serde_msgspec import convert, dumps_msgpack, to_builtins
from storage.deltalake import DeltaWritePolicy, delta_table_version
from storage.ipc_utils import payload_hash


def _rows(table: TableLike) -> int:
    value = getattr(table, "num_rows", 0)
    if isinstance(value, bool):
        return 0
    if isinstance(value, int):
        return value
    return 0


def _stage_identity(table: TableLike) -> TableLike:
    return table


def _stage_ready(table: TableLike) -> TableLike:
    return table


@dataclass(frozen=True)
class SemanticTagSpec:
    """Semantic tag inputs for CPG output tagging."""

    semantic_id: str
    entity: str
    grain: str
    schema_ref: str
    entity_keys: tuple[str, ...]
    join_keys: tuple[str, ...] | None = None


@dataclass(frozen=True)
class OutputRuntimeContext:
    """Runtime and output configuration context for outputs."""

    runtime_profile_spec: RuntimeProfileSpec
    output_config: OutputConfig


@dataclass(frozen=True)
class OutputPlanContext:
    """Plan metadata context for outputs."""

    plan_signature: str
    plan_fingerprints: Mapping[str, str]
    plan_bundles_by_task: Mapping[str, DataFusionPlanBundle]
    run_id: str
    artifact_ids: Mapping[str, str]


@dataclass(frozen=True)
class DeltaWriteInputs:
    """Inputs for Delta output writes."""

    runtime: OutputRuntimeContext
    plan: OutputPlanContext
    dataset_name: str
    plan_dataset_name: str | None = None
    write_policy_override: DeltaWritePolicy | None = None


@dataclass(frozen=True)
class PrimaryOutputs:
    """Primary CPG outputs for run manifests."""

    cpg_nodes: DataSaverDict
    cpg_edges: DataSaverDict
    cpg_props: DataSaverDict


@dataclass(frozen=True)
class AdjacencyOutputs:
    """Adjacency outputs for run manifests."""

    cpg_props_map: DataSaverDict
    cpg_edges_by_src: DataSaverDict
    cpg_edges_by_dst: DataSaverDict


@dataclass(frozen=True)
class _DeltaWritePlanDetails:
    dataset_name: str
    plan_signature: str
    plan_fingerprint: str | None
    plan_identity_hash: str | None
    run_id: str
    delta_inputs: tuple[str, ...] | None
    write_policy_override: DeltaWritePolicy | None


F = TypeVar("F", bound=Callable[..., object])


if TYPE_CHECKING:
    from core_types import JsonValue
    from datafusion_engine.plan_bundle import DataFusionPlanBundle

    type DataSaverDict = dict[str, JsonValue]
else:
    DataSaverDict = dict


def _semantic_tag(*, artifact: str, spec: SemanticTagSpec) -> Callable[[F], F]:
    resolved_join_keys = spec.join_keys if spec.join_keys is not None else spec.entity_keys
    return tag(
        layer="semantic",
        artifact=artifact,
        kind="table",
        semantic_id=spec.semantic_id,
        entity=spec.entity,
        grain=spec.grain,
        version="1",
        stability="design",
        schema_ref=spec.schema_ref,
        entity_keys=",".join(spec.entity_keys),
        join_keys=",".join(resolved_join_keys),
        materialization="delta",
        materialized_name=spec.schema_ref,
    )


@tag(layer="inputs", artifact="run_id", kind="scalar")
def run_id() -> str:
    """Return a run-scoped UUID for pipeline outputs.

    Returns
    -------
    str
        Unique run identifier.
    """
    return str(uuid.uuid4())


def output_runtime_context(
    runtime_profile_spec: RuntimeProfileSpec,
    output_config: OutputConfig,
) -> OutputRuntimeContext:
    """Build output runtime context.

    Parameters
    ----------
    runtime_profile_spec
        Runtime profile specification for this run.
    output_config
        Output configuration settings.

    Returns
    -------
    OutputRuntimeContext
        Runtime and output configuration context.
    """
    return OutputRuntimeContext(
        runtime_profile_spec=runtime_profile_spec,
        output_config=output_config,
    )


def output_plan_context(
    plan_signature: str,
    plan_fingerprints: Mapping[str, str],
    plan_bundles_by_task: Mapping[str, DataFusionPlanBundle],
    plan_artifact_ids: Mapping[str, str],
    run_id: str,
) -> OutputPlanContext:
    """Build output plan context.

    Parameters
    ----------
    plan_signature
        Plan signature string for the run.
    plan_fingerprints
        Mapping of plan fingerprints by dataset.
    plan_bundles_by_task
        Plan bundles grouped by task name.
    plan_artifact_ids
        Mapping of plan artifact identifiers by dataset.
    run_id
        Run identifier for the pipeline execution.

    Returns
    -------
    OutputPlanContext
        Plan metadata context.
    """
    artifact_ids = dict(plan_artifact_ids)
    artifact_ids.update(_plan_identity_hashes_for_outputs(plan_bundles_by_task))
    return OutputPlanContext(
        plan_signature=plan_signature,
        plan_fingerprints=plan_fingerprints,
        plan_bundles_by_task=plan_bundles_by_task,
        run_id=run_id,
        artifact_ids=artifact_ids,
    )


def primary_outputs(
    write_cpg_nodes_delta: DataSaverDict,
    write_cpg_edges_delta: DataSaverDict,
    write_cpg_props_delta: DataSaverDict,
) -> PrimaryOutputs:
    """Bundle primary output artifacts.

    Parameters
    ----------
    write_cpg_nodes_delta
        CPG nodes output artifact metadata.
    write_cpg_edges_delta
        CPG edges output artifact metadata.
    write_cpg_props_delta
        CPG props output artifact metadata.

    Returns
    -------
    PrimaryOutputs
        Bundle of primary output artifacts.
    """
    return PrimaryOutputs(
        cpg_nodes=write_cpg_nodes_delta,
        cpg_edges=write_cpg_edges_delta,
        cpg_props=write_cpg_props_delta,
    )


def adjacency_outputs(
    write_cpg_props_map_delta: DataSaverDict,
    write_cpg_edges_by_src_delta: DataSaverDict,
    write_cpg_edges_by_dst_delta: DataSaverDict,
) -> AdjacencyOutputs:
    """Bundle adjacency output artifacts.

    Parameters
    ----------
    write_cpg_props_map_delta
        CPG props map output artifact metadata.
    write_cpg_edges_by_src_delta
        CPG edges-by-source output artifact metadata.
    write_cpg_edges_by_dst_delta
        CPG edges-by-destination output artifact metadata.

    Returns
    -------
    AdjacencyOutputs
        Bundle of adjacency output artifacts.
    """
    return AdjacencyOutputs(
        cpg_props_map=write_cpg_props_map_delta,
        cpg_edges_by_src=write_cpg_edges_by_src_delta,
        cpg_edges_by_dst=write_cpg_edges_by_dst_delta,
    )


def _delta_write_plan_details(inputs: DeltaWriteInputs) -> _DeltaWritePlanDetails:
    plan = inputs.plan
    dataset_name = inputs.dataset_name
    plan_dataset_name = inputs.plan_dataset_name or dataset_name
    plan_fingerprint = _plan_fingerprint_for_dataset(
        plan.plan_fingerprints,
        dataset_name=plan_dataset_name,
    )
    plan_bundle = plan.plan_bundles_by_task.get(plan_dataset_name)
    plan_identity_hash = plan_bundle.plan_identity_hash if plan_bundle is not None else None
    delta_inputs = _delta_inputs_for_run(plan.plan_bundles_by_task)
    return _DeltaWritePlanDetails(
        dataset_name=dataset_name,
        plan_signature=plan.plan_signature,
        plan_fingerprint=plan_fingerprint,
        plan_identity_hash=plan_identity_hash,
        run_id=plan.run_id,
        delta_inputs=delta_inputs,
        write_policy_override=inputs.write_policy_override,
    )


def _delta_target_dir(output_config: OutputConfig, *, dataset_name: str) -> Path:
    base_dir = output_config.output_dir or output_config.work_dir
    if not base_dir:
        msg = "Output directory must be configured for Delta materialization."
        raise ValueError(msg)
    target_dir = Path(base_dir) / dataset_name
    target_dir.mkdir(parents=True, exist_ok=True)
    return target_dir


def _delta_commit_metadata(
    table: TableLike,
    *,
    dataset_name: str,
    plan_signature: str | None,
    plan_identity_hash: str | None,
) -> dict[str, object]:
    commit_metadata: dict[str, object] = {
        "dataset_name": dataset_name,
        "operation": "output_materialize",
        "mode": "overwrite",
        "schema_fingerprint": payload_hash(
            {"schema": list(getattr(table.schema, "names", []))},
            pa.schema([pa.field("schema", pa.list_(pa.string()))]),
        ),
    }
    if plan_signature is not None:
        commit_metadata["plan_signature"] = plan_signature
    if plan_identity_hash is not None:
        commit_metadata["plan_identity_hash"] = plan_identity_hash
    return commit_metadata


def _delta_write(
    table: TableLike,
    *,
    inputs: DeltaWriteInputs,
) -> JsonDict:
    output_config = inputs.runtime.output_config
    runtime_profile = inputs.runtime.runtime_profile_spec.datafusion
    session_runtime = runtime_profile.session_runtime()
    details = _delta_write_plan_details(inputs)
    target_dir = _delta_target_dir(output_config, dataset_name=details.dataset_name)
    commit_metadata = _delta_commit_metadata(
        table,
        dataset_name=details.dataset_name,
        plan_signature=details.plan_signature,
        plan_identity_hash=details.plan_identity_hash,
    )
    df = datafusion_from_arrow(
        session_runtime.ctx,
        name=f"__output_{details.dataset_name}_{uuid.uuid4().hex}",
        value=table,
    )
    format_options: dict[str, object] = {
        "commit_metadata": commit_metadata,
        "schema_mode": "overwrite",
    }
    resolved_write_policy = (
        details.write_policy_override
        if details.write_policy_override is not None
        else output_config.delta_write_policy
    )
    if resolved_write_policy is not None:
        format_options["delta_write_policy"] = resolved_write_policy
    if output_config.delta_schema_policy is not None:
        format_options["delta_schema_policy"] = output_config.delta_schema_policy
    if output_config.delta_storage_options is not None:
        format_options["storage_options"] = dict(output_config.delta_storage_options)
    pipeline = WritePipeline(
        session_runtime.ctx,
        sql_options=runtime_profile.sql_options(),
        recorder=recorder_for_profile(
            runtime_profile,
            operation_id=f"hamilton_output::{details.dataset_name}",
        ),
        runtime_profile=runtime_profile,
    )
    write_result = pipeline.write(
        WriteRequest(
            source=df,
            destination=str(target_dir),
            format=WriteFormat.DELTA,
            mode=WriteMode.OVERWRITE,
            format_options=format_options,
            plan_fingerprint=details.plan_fingerprint,
            plan_identity_hash=details.plan_identity_hash,
            run_id=details.run_id,
            delta_inputs=details.delta_inputs,
        )
    )
    final_version = (
        write_result.delta_result.version if write_result.delta_result is not None else None
    )
    if final_version is None:
        final_version = delta_table_version(
            str(target_dir),
            storage_options=output_config.delta_storage_options,
        )
    if final_version is None:
        msg = f"Failed to resolve Delta version for output dataset: {details.dataset_name!r}."
        raise RuntimeError(msg)
    return {
        "path": str(target_dir),
        "delta_version": final_version,
        "rows": _rows(table),
    }


def _delta_feature_gate_payload(gate: object | None) -> dict[str, object] | None:
    if gate is None:
        return None
    min_reader_version = getattr(gate, "min_reader_version", None)
    min_writer_version = getattr(gate, "min_writer_version", None)
    required_reader_features = getattr(gate, "required_reader_features", ())
    required_writer_features = getattr(gate, "required_writer_features", ())
    return {
        "min_reader_version": min_reader_version,
        "min_writer_version": min_writer_version,
        "required_reader_features": list(required_reader_features),
        "required_writer_features": list(required_writer_features),
    }


def _delta_protocol_payload(protocol: object | None) -> dict[str, object] | None:
    if not isinstance(protocol, Mapping):
        return None
    payload: dict[str, object] = {}
    for key, value in protocol.items():
        if isinstance(value, (str, int, float)) or value is None:
            payload[str(key)] = value
            continue
        if isinstance(value, (list, tuple)):
            payload[str(key)] = [str(item) for item in value]
            continue
        payload[str(key)] = str(value)
    return payload or None


def _delta_inputs_payload(
    plan_bundles_by_task: Mapping[str, DataFusionPlanBundle],
) -> tuple[dict[str, object], ...]:
    payloads: list[dict[str, object]] = []
    seen: set[tuple[object, ...]] = set()
    for bundle in plan_bundles_by_task.values():
        for pin in bundle.delta_inputs:
            key = (pin.dataset_name, pin.version, pin.timestamp, pin.storage_options_hash)
            if key in seen:
                continue
            seen.add(key)
            payloads.append(
                {
                    "dataset_name": pin.dataset_name,
                    "version": pin.version,
                    "timestamp": pin.timestamp,
                    "feature_gate": _delta_feature_gate_payload(pin.feature_gate),
                    "protocol": _delta_protocol_payload(pin.protocol),
                    "storage_options_hash": pin.storage_options_hash,
                    "delta_scan_config": pin.delta_scan_config,
                    "delta_scan_config_hash": pin.delta_scan_config_hash,
                    "datafusion_provider": pin.datafusion_provider,
                    "protocol_compatible": pin.protocol_compatible,
                    "protocol_compatibility": pin.protocol_compatibility,
                }
            )
    payloads.sort(
        key=lambda row: (
            str(row["dataset_name"]),
            row["version"] if row["version"] is not None else -1,
            row["timestamp"] or "",
        )
    )
    return tuple(payloads)


def _delta_input_tokens(payloads: Sequence[Mapping[str, object]]) -> tuple[str, ...]:
    tokens: list[str] = []
    for payload in payloads:
        dataset_name = str(payload.get("dataset_name", ""))
        version = payload.get("version")
        timestamp = payload.get("timestamp")
        if isinstance(version, int):
            token = f"{dataset_name}@v{version}"
        elif isinstance(timestamp, str) and timestamp:
            token = f"{dataset_name}@ts:{timestamp}"
        else:
            token = f"{dataset_name}@latest"
        tokens.append(token)
    return tuple(tokens)


def _delta_inputs_for_run(
    plan_bundles_by_task: Mapping[str, DataFusionPlanBundle],
) -> tuple[str, ...]:
    payloads = _delta_inputs_payload(plan_bundles_by_task)
    return _delta_input_tokens(payloads)


def _plan_fingerprint_for_dataset(
    plan_fingerprints: Mapping[str, str],
    *,
    dataset_name: str,
) -> str | None:
    view_name = _OUTPUT_PLAN_FINGERPRINTS.get(dataset_name)
    if view_name is None:
        return None
    value = plan_fingerprints.get(view_name)
    return value if value else None


def _plan_identity_hashes_for_outputs(
    plan_bundles_by_task: Mapping[str, DataFusionPlanBundle],
) -> dict[str, str]:
    payload: dict[str, str] = {}
    for dataset_name, view_name in _OUTPUT_PLAN_FINGERPRINTS.items():
        bundle = plan_bundles_by_task.get(view_name)
        if bundle is None or bundle.plan_identity_hash is None:
            continue
        payload[dataset_name] = bundle.plan_identity_hash
    return payload


_OUTPUT_PLAN_FINGERPRINTS: dict[str, str] = {
    "cpg_nodes": "cpg_nodes_v1",
    "cpg_edges": "cpg_edges_v1",
    "cpg_props": "cpg_props_v1",
    "cpg_props_map": "cpg_props_map_v1",
    "cpg_edges_by_src": "cpg_edges_by_src_v1",
    "cpg_edges_by_dst": "cpg_edges_by_dst_v1",
}
_NORMALIZE_OUTPUTS_TABLE_NAME = "normalize_outputs_v1"
_EXTRACT_ERRORS_TABLE_NAME = "extract_errors_v1"

_RUN_MANIFEST_SCHEMA = pa.schema(
    [
        pa.field("run_id", pa.string(), nullable=False),
        pa.field("status", pa.string(), nullable=False),
        pa.field("event_time_unix_ms", pa.int64(), nullable=False),
        pa.field("plan_signature", pa.string(), nullable=False),
        pa.field("plan_fingerprints", pa.map_(pa.string(), pa.string()), nullable=False),
        pa.field("delta_inputs_msgpack", pa.binary(), nullable=False),
        pa.field("outputs_msgpack", pa.binary(), nullable=False),
        pa.field("artifact_ids", pa.map_(pa.string(), pa.string()), nullable=True),
        pa.field("runtime_profile_name", pa.string(), nullable=True),
        pa.field("runtime_profile_hash", pa.string(), nullable=True),
        pa.field("determinism_tier", pa.string(), nullable=True),
        pa.field("output_dir", pa.string(), nullable=True),
    ]
)

_RUN_MANIFEST_WRITE_POLICY = DeltaWritePolicy(
    target_file_size=32 * 1024 * 1024,
    zorder_by=("run_id", "event_time_unix_ms"),
    stats_policy="explicit",
    stats_columns=("run_id", "event_time_unix_ms", "status"),
)
_NORMALIZE_OUTPUTS_SCHEMA = pa.schema(
    [
        pa.field("event_time_unix_ms", pa.int64(), nullable=False),
        pa.field("run_id", pa.string(), nullable=False),
        pa.field("output_dir", pa.string(), nullable=False),
        pa.field("outputs", pa.list_(pa.string()), nullable=False),
        pa.field("row_count", pa.int64(), nullable=False),
    ]
)
_EXTRACT_ERRORS_SCHEMA = pa.schema(
    [
        pa.field("event_time_unix_ms", pa.int64(), nullable=False),
        pa.field("run_id", pa.string(), nullable=False),
        pa.field("output_dir", pa.string(), nullable=False),
        pa.field("errors", pa.list_(pa.string()), nullable=False),
        pa.field("error_count", pa.int64(), nullable=False),
    ]
)
_OUTPUT_METADATA_WRITE_POLICY = DeltaWritePolicy(
    target_file_size=8 * 1024 * 1024,
    zorder_by=("run_id", "event_time_unix_ms"),
    stats_policy="explicit",
    stats_columns=("run_id", "event_time_unix_ms"),
)

_CPG_NODES_WRITE_POLICY = DeltaWritePolicy(
    target_file_size=256 * 1024 * 1024,
    partition_by=("node_kind",),
    zorder_by=("file_id", "bstart", "node_id"),
    stats_policy="explicit",
    stats_columns=("file_id", "path", "node_id", "node_kind", "bstart", "bend"),
)
_CPG_EDGES_WRITE_POLICY = DeltaWritePolicy(
    target_file_size=256 * 1024 * 1024,
    partition_by=("edge_kind",),
    zorder_by=("path", "bstart", "src_node_id", "dst_node_id"),
    stats_policy="explicit",
    stats_columns=(
        "path",
        "edge_id",
        "edge_kind",
        "src_node_id",
        "dst_node_id",
        "bstart",
        "bend",
    ),
)
_CPG_PROPS_WRITE_POLICY = DeltaWritePolicy(
    target_file_size=256 * 1024 * 1024,
    partition_by=("entity_kind",),
    zorder_by=("entity_kind", "prop_key", "entity_id"),
    stats_policy="explicit",
    stats_columns=("entity_kind", "entity_id", "prop_key", "node_kind"),
)
_CPG_PROPS_MAP_WRITE_POLICY = DeltaWritePolicy(
    target_file_size=128 * 1024 * 1024,
    zorder_by=("entity_kind", "entity_id"),
    stats_policy="explicit",
    stats_columns=("entity_kind", "entity_id", "node_kind"),
)
_CPG_EDGES_BY_SRC_WRITE_POLICY = DeltaWritePolicy(
    target_file_size=128 * 1024 * 1024,
    zorder_by=("src_node_id",),
    stats_policy="explicit",
    stats_columns=("src_node_id",),
)
_CPG_EDGES_BY_DST_WRITE_POLICY = DeltaWritePolicy(
    target_file_size=128 * 1024 * 1024,
    zorder_by=("dst_node_id",),
    stats_policy="explicit",
    stats_columns=("dst_node_id",),
)


@pipe_input(
    step(_stage_identity),
    step(_stage_ready),
    on_input="cpg_nodes_final",
    namespace="cpg_nodes",
)
@cache(format="delta", behavior="default")
@check_output_custom(NonEmptyTableValidator())
@_semantic_tag(
    artifact="cpg_nodes",
    spec=SemanticTagSpec(
        semantic_id="cpg.nodes.v1",
        entity="node",
        grain="per_node",
        schema_ref="semantic.cpg_nodes_v1",
        entity_keys=("repo", "commit", "node_id"),
    ),
)
def cpg_nodes(cpg_nodes_final: TableLike) -> TableLike:
    """Return the final CPG nodes table.

    Returns
    -------
    TableLike
        Final nodes table.
    """
    return cpg_nodes_final


@pipe_input(
    step(_stage_identity),
    step(_stage_ready),
    on_input="cpg_edges_final",
    namespace="cpg_edges",
)
@cache(format="delta", behavior="default")
@check_output_custom(NonEmptyTableValidator())
@_semantic_tag(
    artifact="cpg_edges",
    spec=SemanticTagSpec(
        semantic_id="cpg.edges.v1",
        entity="edge",
        grain="per_edge",
        schema_ref="semantic.cpg_edges_v1",
        entity_keys=("repo", "commit", "edge_id"),
    ),
)
def cpg_edges(cpg_edges_final: TableLike) -> TableLike:
    """Return the final CPG edges table.

    Returns
    -------
    TableLike
        Final edges table.
    """
    return cpg_edges_final


@pipe_input(
    step(_stage_identity),
    step(_stage_ready),
    on_input="cpg_props_final",
    namespace="cpg_props",
)
@cache(format="delta", behavior="default")
@check_output_custom(NonEmptyTableValidator())
@_semantic_tag(
    artifact="cpg_props",
    spec=SemanticTagSpec(
        semantic_id="cpg.props.v1",
        entity="prop",
        grain="per_prop",
        schema_ref="semantic.cpg_props_v1",
        entity_keys=("repo", "commit", "node_id", "key"),
        join_keys=("repo", "commit", "node_id"),
    ),
)
def cpg_props(cpg_props_final: TableLike) -> TableLike:
    """Return the final CPG properties table.

    Returns
    -------
    TableLike
        Final properties table.
    """
    return cpg_props_final


@datasaver()
@tag(layer="outputs", artifact="write_cpg_nodes_delta", kind="delta")
def write_cpg_nodes_delta(
    cpg_nodes: TableLike,
    output_runtime_context: OutputRuntimeContext,
    output_plan_context: OutputPlanContext,
) -> DataSaverDict:
    """Return stub metadata for CPG nodes output.

    Returns
    -------
    JsonDict
        Stub metadata payload.
    """
    return _delta_write(
        cpg_nodes,
        inputs=DeltaWriteInputs(
            runtime=output_runtime_context,
            plan=output_plan_context,
            dataset_name="cpg_nodes",
            write_policy_override=(
                _CPG_NODES_WRITE_POLICY
                if output_runtime_context.output_config.delta_write_policy is None
                else None
            ),
        ),
    )


@datasaver()
@tag(layer="outputs", artifact="write_cpg_edges_delta", kind="delta")
def write_cpg_edges_delta(
    cpg_edges: TableLike,
    output_runtime_context: OutputRuntimeContext,
    output_plan_context: OutputPlanContext,
) -> DataSaverDict:
    """Return stub metadata for CPG edges output.

    Returns
    -------
    JsonDict
        Stub metadata payload.
    """
    return _delta_write(
        cpg_edges,
        inputs=DeltaWriteInputs(
            runtime=output_runtime_context,
            plan=output_plan_context,
            dataset_name="cpg_edges",
            write_policy_override=(
                _CPG_EDGES_WRITE_POLICY
                if output_runtime_context.output_config.delta_write_policy is None
                else None
            ),
        ),
    )


@datasaver()
@tag(layer="outputs", artifact="write_cpg_props_delta", kind="delta")
def write_cpg_props_delta(
    cpg_props: TableLike,
    output_runtime_context: OutputRuntimeContext,
    output_plan_context: OutputPlanContext,
) -> DataSaverDict:
    """Return stub metadata for CPG properties output.

    Returns
    -------
    JsonDict
        Stub metadata payload.
    """
    return _delta_write(
        cpg_props,
        inputs=DeltaWriteInputs(
            runtime=output_runtime_context,
            plan=output_plan_context,
            dataset_name="cpg_props",
            write_policy_override=(
                _CPG_PROPS_WRITE_POLICY
                if output_runtime_context.output_config.delta_write_policy is None
                else None
            ),
        ),
    )


@datasaver()
@tag(layer="outputs", artifact="write_cpg_props_map_delta", kind="delta")
def write_cpg_props_map_delta(
    cpg_props_map_v1: TableLike,
    output_runtime_context: OutputRuntimeContext,
    output_plan_context: OutputPlanContext,
) -> DataSaverDict:
    """Return metadata for the CPG property map output.

    Returns
    -------
    JsonDict
        Output metadata payload.
    """
    return _delta_write(
        cpg_props_map_v1,
        inputs=DeltaWriteInputs(
            runtime=output_runtime_context,
            plan=output_plan_context,
            dataset_name="cpg_props_map",
            write_policy_override=(
                _CPG_PROPS_MAP_WRITE_POLICY
                if output_runtime_context.output_config.delta_write_policy is None
                else None
            ),
        ),
    )


@datasaver()
@tag(layer="outputs", artifact="write_cpg_edges_by_src_delta", kind="delta")
def write_cpg_edges_by_src_delta(
    cpg_edges_by_src_v1: TableLike,
    output_runtime_context: OutputRuntimeContext,
    output_plan_context: OutputPlanContext,
) -> DataSaverDict:
    """Return metadata for the CPG edges-by-src output.

    Returns
    -------
    JsonDict
        Output metadata payload.
    """
    return _delta_write(
        cpg_edges_by_src_v1,
        inputs=DeltaWriteInputs(
            runtime=output_runtime_context,
            plan=output_plan_context,
            dataset_name="cpg_edges_by_src",
            write_policy_override=(
                _CPG_EDGES_BY_SRC_WRITE_POLICY
                if output_runtime_context.output_config.delta_write_policy is None
                else None
            ),
        ),
    )


@datasaver()
@tag(layer="outputs", artifact="write_cpg_edges_by_dst_delta", kind="delta")
def write_cpg_edges_by_dst_delta(
    cpg_edges_by_dst_v1: TableLike,
    output_runtime_context: OutputRuntimeContext,
    output_plan_context: OutputPlanContext,
) -> DataSaverDict:
    """Return metadata for the CPG edges-by-dst output.

    Returns
    -------
    JsonDict
        Output metadata payload.
    """
    return _delta_write(
        cpg_edges_by_dst_v1,
        inputs=DeltaWriteInputs(
            runtime=output_runtime_context,
            plan=output_plan_context,
            dataset_name="cpg_edges_by_dst",
            write_policy_override=(
                _CPG_EDGES_BY_DST_WRITE_POLICY
                if output_runtime_context.output_config.delta_write_policy is None
                else None
            ),
        ),
    )


@datasaver()
@tag(layer="outputs", artifact="write_cpg_nodes_quality_delta", kind="delta")
def write_cpg_nodes_quality_delta(
    cpg_nodes: TableLike,
    output_runtime_context: OutputRuntimeContext,
    output_plan_context: OutputPlanContext,
) -> DataSaverDict:
    """Return stub metadata for CPG node quality output.

    Returns
    -------
    JsonDict
        Stub metadata payload.
    """
    return _delta_write(
        cpg_nodes,
        inputs=DeltaWriteInputs(
            runtime=output_runtime_context,
            plan=output_plan_context,
            dataset_name="cpg_nodes_quality",
            plan_dataset_name="cpg_nodes",
            write_policy_override=(
                _CPG_NODES_WRITE_POLICY
                if output_runtime_context.output_config.delta_write_policy is None
                else None
            ),
        ),
    )


@datasaver()
@tag(layer="outputs", artifact="write_cpg_props_quality_delta", kind="delta")
def write_cpg_props_quality_delta(
    cpg_props: TableLike,
    output_runtime_context: OutputRuntimeContext,
    output_plan_context: OutputPlanContext,
) -> DataSaverDict:
    """Return stub metadata for CPG prop quality output.

    Returns
    -------
    JsonDict
        Stub metadata payload.
    """
    return _delta_write(
        cpg_props,
        inputs=DeltaWriteInputs(
            runtime=output_runtime_context,
            plan=output_plan_context,
            dataset_name="cpg_props_quality",
            plan_dataset_name="cpg_props",
            write_policy_override=(
                _CPG_PROPS_WRITE_POLICY
                if output_runtime_context.output_config.delta_write_policy is None
                else None
            ),
        ),
    )


@datasaver()
@tag(layer="outputs", artifact="write_normalize_outputs_delta", kind="delta")
def write_normalize_outputs_delta(
    output_runtime_context: OutputRuntimeContext,
    output_plan_context: OutputPlanContext,
) -> DataSaverDict:
    """Persist normalize output metadata as a Delta-backed artifact.

    Returns
    -------
    JsonDict
        Normalize output artifact metadata payload.
    """
    outputs: list[str] = []
    artifact = NormalizeOutputsArtifact(
        event_time_unix_ms=int(time.time() * 1000),
        run_id=output_plan_context.run_id,
        output_dir=str(_manifest_base_dir(output_runtime_context.output_config)),
        outputs=tuple(outputs),
        row_count=0,
    )
    payload = to_builtins(artifact, str_keys=True)
    record_artifact(
        output_runtime_context.runtime_profile_spec.datafusion,
        "normalize_outputs_v1",
        cast("Mapping[str, object]", payload),
    )
    delta_inputs = _delta_input_tokens(
        _delta_inputs_payload(output_plan_context.plan_bundles_by_task)
    )
    result = _output_table_write(
        output_runtime_context=output_runtime_context,
        output_plan_context=output_plan_context,
        request=OutputMetadataWriteRequest(
            table_payload={
                "event_time_unix_ms": artifact.event_time_unix_ms,
                "run_id": artifact.run_id,
                "output_dir": artifact.output_dir,
                "outputs": list(artifact.outputs),
                "row_count": artifact.row_count,
            },
            schema=_NORMALIZE_OUTPUTS_SCHEMA,
            table_name=_NORMALIZE_OUTPUTS_TABLE_NAME,
            operation_id="hamilton_output::normalize_outputs",
            write_policy=_OUTPUT_METADATA_WRITE_POLICY,
            plan_fingerprint=output_plan_context.plan_signature,
            delta_inputs=delta_inputs,
        ),
    )
    result["artifact"] = cast("JsonValue", payload)
    return result


@datasaver()
@tag(layer="outputs", artifact="write_extract_error_artifacts_delta", kind="delta")
def write_extract_error_artifacts_delta(
    output_runtime_context: OutputRuntimeContext,
    output_plan_context: OutputPlanContext,
) -> DataSaverDict:
    """Persist extract error artifacts as a Delta-backed artifact.

    Returns
    -------
    JsonDict
        Extract error artifact metadata payload.
    """
    errors: list[str] = []
    artifact = ExtractErrorsArtifact(
        event_time_unix_ms=int(time.time() * 1000),
        run_id=output_plan_context.run_id,
        output_dir=str(_manifest_base_dir(output_runtime_context.output_config)),
        errors=tuple(errors),
        error_count=0,
    )
    payload = to_builtins(artifact, str_keys=True)
    record_artifact(
        output_runtime_context.runtime_profile_spec.datafusion,
        "extract_errors_v1",
        cast("Mapping[str, object]", payload),
    )
    delta_inputs = _delta_input_tokens(
        _delta_inputs_payload(output_plan_context.plan_bundles_by_task)
    )
    result = _output_table_write(
        output_runtime_context=output_runtime_context,
        output_plan_context=output_plan_context,
        request=OutputMetadataWriteRequest(
            table_payload={
                "event_time_unix_ms": artifact.event_time_unix_ms,
                "run_id": artifact.run_id,
                "output_dir": artifact.output_dir,
                "errors": list(artifact.errors),
                "error_count": artifact.error_count,
            },
            schema=_EXTRACT_ERRORS_SCHEMA,
            table_name=_EXTRACT_ERRORS_TABLE_NAME,
            operation_id="hamilton_output::extract_errors",
            write_policy=_OUTPUT_METADATA_WRITE_POLICY,
            plan_fingerprint=output_plan_context.plan_signature,
            delta_inputs=delta_inputs,
        ),
    )
    result["artifact"] = cast("JsonValue", payload)
    return result


def _run_manifest_output_payloads(
    *,
    primary: PrimaryOutputs,
    adjacency: AdjacencyOutputs,
) -> list[dict[str, object]]:
    return [
        {
            "name": "cpg_nodes",
            "path": primary.cpg_nodes.get("path"),
            "delta_version": primary.cpg_nodes.get("delta_version"),
            "rows": primary.cpg_nodes.get("rows"),
        },
        {
            "name": "cpg_edges",
            "path": primary.cpg_edges.get("path"),
            "delta_version": primary.cpg_edges.get("delta_version"),
            "rows": primary.cpg_edges.get("rows"),
        },
        {
            "name": "cpg_props",
            "path": primary.cpg_props.get("path"),
            "delta_version": primary.cpg_props.get("delta_version"),
            "rows": primary.cpg_props.get("rows"),
        },
        {
            "name": "cpg_props_map",
            "path": adjacency.cpg_props_map.get("path"),
            "delta_version": adjacency.cpg_props_map.get("delta_version"),
            "rows": adjacency.cpg_props_map.get("rows"),
        },
        {
            "name": "cpg_edges_by_src",
            "path": adjacency.cpg_edges_by_src.get("path"),
            "delta_version": adjacency.cpg_edges_by_src.get("delta_version"),
            "rows": adjacency.cpg_edges_by_src.get("rows"),
        },
        {
            "name": "cpg_edges_by_dst",
            "path": adjacency.cpg_edges_by_dst.get("path"),
            "delta_version": adjacency.cpg_edges_by_dst.get("delta_version"),
            "rows": adjacency.cpg_edges_by_dst.get("rows"),
        },
    ]


def _manifest_base_dir(output_config: OutputConfig) -> Path:
    base = output_config.output_dir or output_config.work_dir
    if not base:
        msg = "Output directory must be configured for output artifacts."
        raise ValueError(msg)
    return Path(base)


@dataclass(frozen=True)
class OutputMetadataWriteRequest:
    """Inputs for writing output metadata tables."""

    table_payload: Mapping[str, object]
    schema: pa.Schema
    table_name: str
    operation_id: str
    write_policy: DeltaWritePolicy
    plan_fingerprint: str | None
    delta_inputs: tuple[str, ...] | None


def _output_table_write(
    *,
    output_runtime_context: OutputRuntimeContext,
    output_plan_context: OutputPlanContext,
    request: OutputMetadataWriteRequest,
) -> DataSaverDict:
    output_config = output_runtime_context.output_config
    runtime_profile_spec = output_runtime_context.runtime_profile_spec
    table = pa.Table.from_pylist([dict(request.table_payload)], schema=request.schema)
    runtime_profile = runtime_profile_spec.datafusion
    session_runtime = runtime_profile.session_runtime()
    df = datafusion_from_arrow(
        session_runtime.ctx,
        name=f"__{request.table_name}_{output_plan_context.run_id}_{uuid.uuid4().hex}",
        value=table,
    )
    commit_metadata = {
        "run_id": output_plan_context.run_id,
        "plan_signature": output_plan_context.plan_signature,
    }
    format_options: dict[str, object] = {
        "commit_metadata": commit_metadata,
        "delta_write_policy": request.write_policy,
    }
    if output_config.delta_schema_policy is not None:
        format_options["delta_schema_policy"] = output_config.delta_schema_policy
    if output_config.delta_storage_options is not None:
        format_options["storage_options"] = dict(output_config.delta_storage_options)
    pipeline = WritePipeline(
        session_runtime.ctx,
        sql_options=runtime_profile.sql_options(),
        recorder=recorder_for_profile(runtime_profile, operation_id=request.operation_id),
        runtime_profile=runtime_profile,
    )
    output_dir = str(_manifest_base_dir(output_config))
    destination = str(Path(output_dir) / request.table_name)
    write_result = pipeline.write(
        WriteRequest(
            source=df,
            destination=destination,
            format=WriteFormat.DELTA,
            mode=WriteMode.OVERWRITE,
            format_options=format_options,
            plan_fingerprint=request.plan_fingerprint,
            run_id=output_plan_context.run_id,
            delta_inputs=request.delta_inputs,
        )
    )
    return {
        "path": write_result.request.destination,
        "delta_version": (
            write_result.delta_result.version if write_result.delta_result is not None else None
        ),
        "rows": 1,
    }


def _run_manifest_payload(
    *,
    output_runtime_context: OutputRuntimeContext,
    output_plan_context: OutputPlanContext,
    primary_outputs: PrimaryOutputs,
    adjacency_outputs: AdjacencyOutputs,
    output_dir: str,
) -> RunManifest:
    output_payloads = _run_manifest_output_payloads(
        primary=primary_outputs,
        adjacency=adjacency_outputs,
    )
    delta_inputs_payload = _delta_inputs_payload(output_plan_context.plan_bundles_by_task)
    runtime_profile_spec = output_runtime_context.runtime_profile_spec
    artifact_ids = dict(output_plan_context.artifact_ids)
    return RunManifest(
        run_id=output_plan_context.run_id,
        status="completed",
        event_time_unix_ms=int(time.time() * 1000),
        plan_signature=output_plan_context.plan_signature,
        plan_fingerprints=dict(output_plan_context.plan_fingerprints),
        delta_inputs=tuple(delta_inputs_payload),
        outputs=tuple(output_payloads),
        runtime_profile_name=runtime_profile_spec.name,
        runtime_profile_hash=runtime_profile_spec.runtime_profile_hash,
        determinism_tier=runtime_profile_spec.determinism_tier.value,
        output_dir=output_dir,
        artifact_ids=artifact_ids or None,
    )


def _run_manifest_table_payload(manifest: RunManifest) -> JsonDict:
    return {
        "run_id": manifest.run_id,
        "status": manifest.status,
        "event_time_unix_ms": manifest.event_time_unix_ms,
        "plan_signature": manifest.plan_signature or "",
        "plan_fingerprints": dict(manifest.plan_fingerprints),
        "delta_inputs_msgpack": dumps_msgpack(to_builtins(manifest.delta_inputs, str_keys=True)),
        "outputs_msgpack": dumps_msgpack(to_builtins(manifest.outputs, str_keys=True)),
        "artifact_ids": dict(manifest.artifact_ids) if manifest.artifact_ids else None,
        "runtime_profile_name": manifest.runtime_profile_name,
        "runtime_profile_hash": manifest.runtime_profile_hash,
        "determinism_tier": manifest.determinism_tier,
        "output_dir": manifest.output_dir,
    }


def _write_run_manifest_table(
    *,
    output_runtime_context: OutputRuntimeContext,
    manifest_payload: RunManifest,
    run_id: str,
    plan_fingerprint: str | None,
    delta_inputs: tuple[str, ...] | None,
) -> DataSaverDict:
    output_config = output_runtime_context.output_config
    runtime_profile_spec = output_runtime_context.runtime_profile_spec
    table_payload = _run_manifest_table_payload(manifest_payload)
    table = pa.Table.from_pylist([table_payload], schema=_RUN_MANIFEST_SCHEMA)
    runtime_profile = runtime_profile_spec.datafusion
    session_runtime = runtime_profile.session_runtime()
    df = datafusion_from_arrow(
        session_runtime.ctx,
        name=f"__run_manifest_{run_id}_{uuid.uuid4().hex}",
        value=table,
    )
    commit_metadata = {
        "run_id": run_id,
        "plan_signature": manifest_payload.plan_signature,
        "status": manifest_payload.status,
    }
    format_options: dict[str, object] = {
        "commit_metadata": commit_metadata,
        "delta_write_policy": _RUN_MANIFEST_WRITE_POLICY,
    }
    if output_config.delta_schema_policy is not None:
        format_options["delta_schema_policy"] = output_config.delta_schema_policy
    if output_config.delta_storage_options is not None:
        format_options["storage_options"] = dict(output_config.delta_storage_options)
    pipeline = WritePipeline(
        session_runtime.ctx,
        sql_options=runtime_profile.sql_options(),
        recorder=recorder_for_profile(
            runtime_profile,
            operation_id="hamilton_output::run_manifest",
        ),
        runtime_profile=runtime_profile,
    )
    output_dir = str(manifest_payload.output_dir)
    write_result = pipeline.write(
        WriteRequest(
            source=df,
            destination=str(Path(output_dir) / "run_manifest"),
            format=WriteFormat.DELTA,
            mode=WriteMode.OVERWRITE,
            format_options=format_options,
            plan_fingerprint=plan_fingerprint,
            run_id=run_id,
            delta_inputs=delta_inputs,
        )
    )
    return {
        "path": write_result.request.destination,
        "delta_version": (
            write_result.delta_result.version if write_result.delta_result is not None else None
        ),
        "rows": 1,
    }


@datasaver()
@tag(layer="outputs", artifact="write_run_manifest_delta", kind="delta")
def write_run_manifest_delta(
    output_runtime_context: OutputRuntimeContext,
    output_plan_context: OutputPlanContext,
    primary_outputs: PrimaryOutputs,
    adjacency_outputs: AdjacencyOutputs,
) -> DataSaverDict:
    """Write the run manifest Delta table.

    Parameters
    ----------
    output_runtime_context
        Runtime/output configuration context.
    output_plan_context
        Plan metadata context for this run.
    primary_outputs
        Primary output artifacts bundle.
    adjacency_outputs
        Adjacency output artifacts bundle.

    Returns
    -------
    JsonDict
        Run manifest metadata payload.

    """
    base_dir = _manifest_base_dir(output_runtime_context.output_config)
    manifest_dir = base_dir / "run_manifest"
    manifest_dir.mkdir(parents=True, exist_ok=True)
    manifest_payload = _run_manifest_payload(
        output_runtime_context=output_runtime_context,
        output_plan_context=output_plan_context,
        primary_outputs=primary_outputs,
        adjacency_outputs=adjacency_outputs,
        output_dir=str(base_dir),
    )
    envelope = RunManifestEnvelope(payload=manifest_payload)
    validated = convert(
        to_builtins(envelope, str_keys=True),
        target_type=RunManifestEnvelope,
        strict=True,
    )
    manifest_payload_map = to_builtins(validated, str_keys=True)
    record_artifact(
        output_runtime_context.runtime_profile_spec.datafusion,
        "run_manifest_v2",
        cast("Mapping[str, object]", manifest_payload_map),
    )
    result = _write_run_manifest_table(
        output_runtime_context=output_runtime_context,
        manifest_payload=manifest_payload,
        run_id=output_plan_context.run_id,
        plan_fingerprint=output_plan_context.plan_signature,
        delta_inputs=_delta_input_tokens(
            _delta_inputs_payload(output_plan_context.plan_bundles_by_task)
        ),
    )
    result["manifest"] = cast("JsonValue", manifest_payload_map)
    return result


@datasaver()
@tag(layer="outputs", artifact="write_run_bundle_dir", kind="bundle")
def write_run_bundle_dir(output_config: OutputConfig, run_id: str) -> DataSaverDict:
    """Return stub metadata for run bundle directory.

    Returns
    -------
    JsonDict
        Stub metadata payload.

    Raises
    ------
    ValueError
        Raised when no output directory is configured.
    """
    base = output_config.output_dir or output_config.work_dir
    if not base:
        msg = "Output directory must be configured for run bundles."
        raise ValueError(msg)
    bundle_dir = Path(base) / "run_bundle" / run_id
    bundle_dir.mkdir(parents=True, exist_ok=True)
    return {"bundle_dir": str(bundle_dir), "run_id": run_id}


__all__ = [
    "cpg_edges",
    "cpg_nodes",
    "cpg_props",
    "write_cpg_edges_by_dst_delta",
    "write_cpg_edges_by_src_delta",
    "write_cpg_edges_delta",
    "write_cpg_nodes_delta",
    "write_cpg_nodes_quality_delta",
    "write_cpg_props_delta",
    "write_cpg_props_map_delta",
    "write_cpg_props_quality_delta",
    "write_extract_error_artifacts_delta",
    "write_normalize_outputs_delta",
    "write_run_bundle_dir",
    "write_run_manifest_delta",
]

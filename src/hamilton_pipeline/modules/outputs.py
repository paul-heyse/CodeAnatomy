"""Hamilton output nodes for inference-driven pipeline."""

import json
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, TypeVar

import pyarrow as pa
from hamilton.function_modifiers import (
    cache,
    check_output_custom,
    datasaver,
    pipe_input,
    step,
    tag,
)

from core_types import JsonDict
from datafusion_engine.arrow_interop import TableLike
from datafusion_engine.diagnostics import recorder_for_profile
from datafusion_engine.ingest import datafusion_from_arrow
from datafusion_engine.write_pipeline import WriteFormat, WriteMode, WritePipeline, WriteRequest
from engine.runtime_profile import RuntimeProfileSpec
from hamilton_pipeline.pipeline_types import OutputConfig
from hamilton_pipeline.validators import NonEmptyTableValidator
from storage.deltalake import (
    delta_schema_configuration,
    delta_table_version,
    delta_write_configuration,
)
from storage.deltalake.delta import DEFAULT_DELTA_FEATURE_PROPERTIES
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


F = TypeVar("F", bound=Callable[..., object])


if TYPE_CHECKING:
    from core_types import JsonValue

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


def _delta_write(
    table: TableLike,
    *,
    runtime_profile_spec: RuntimeProfileSpec,
    output_config: OutputConfig,
    dataset_name: str,
) -> JsonDict:
    base_dir = output_config.output_dir or output_config.work_dir
    if not base_dir:
        msg = "Output directory must be configured for Delta materialization."
        raise ValueError(msg)
    target_dir = Path(base_dir) / dataset_name
    target_dir.mkdir(parents=True, exist_ok=True)
    runtime_profile = runtime_profile_spec.datafusion
    session_runtime = runtime_profile.session_runtime()
    configuration: dict[str, str | None] = {}
    if output_config.delta_write_policy is not None:
        configuration.update(delta_write_configuration(output_config.delta_write_policy) or {})
    if output_config.delta_schema_policy is not None:
        configuration.update(delta_schema_configuration(output_config.delta_schema_policy) or {})
    configuration.update(DEFAULT_DELTA_FEATURE_PROPERTIES)
    commit_metadata = {
        "dataset_name": dataset_name,
        "operation": "output_materialize",
        "mode": "overwrite",
        "schema_fingerprint": payload_hash(
            {"schema": list(getattr(table.schema, "names", []))},
            pa.schema([pa.field("schema", pa.list_(pa.string()))]),
        ),
    }
    df = datafusion_from_arrow(
        session_runtime.ctx,
        name=f"__output_{dataset_name}_{uuid.uuid4().hex}",
        value=table,
    )
    format_options: dict[str, object] = {
        "commit_metadata": commit_metadata,
        "table_properties": configuration or None,
        "schema_mode": "overwrite",
    }
    if output_config.delta_storage_options is not None:
        format_options["storage_options"] = dict(output_config.delta_storage_options)
    if output_config.delta_write_policy is not None:
        format_options["target_file_size"] = output_config.delta_write_policy.target_file_size
    pipeline = WritePipeline(
        session_runtime.ctx,
        sql_options=runtime_profile.sql_options(),
        recorder=recorder_for_profile(
            runtime_profile,
            operation_id=f"hamilton_output::{dataset_name}",
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
        msg = f"Failed to resolve Delta version for output dataset: {dataset_name!r}."
        raise RuntimeError(msg)
    return {
        "path": str(target_dir),
        "delta_version": final_version,
        "rows": _rows(table),
    }


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
    runtime_profile_spec: RuntimeProfileSpec,
    output_config: OutputConfig,
) -> DataSaverDict:
    """Return stub metadata for CPG nodes output.

    Returns
    -------
    JsonDict
        Stub metadata payload.
    """
    return _delta_write(
        cpg_nodes,
        runtime_profile_spec=runtime_profile_spec,
        output_config=output_config,
        dataset_name="cpg_nodes",
    )


@datasaver()
@tag(layer="outputs", artifact="write_cpg_edges_delta", kind="delta")
def write_cpg_edges_delta(
    cpg_edges: TableLike,
    runtime_profile_spec: RuntimeProfileSpec,
    output_config: OutputConfig,
) -> DataSaverDict:
    """Return stub metadata for CPG edges output.

    Returns
    -------
    JsonDict
        Stub metadata payload.
    """
    return _delta_write(
        cpg_edges,
        runtime_profile_spec=runtime_profile_spec,
        output_config=output_config,
        dataset_name="cpg_edges",
    )


@datasaver()
@tag(layer="outputs", artifact="write_cpg_props_delta", kind="delta")
def write_cpg_props_delta(
    cpg_props: TableLike,
    runtime_profile_spec: RuntimeProfileSpec,
    output_config: OutputConfig,
) -> DataSaverDict:
    """Return stub metadata for CPG properties output.

    Returns
    -------
    JsonDict
        Stub metadata payload.
    """
    return _delta_write(
        cpg_props,
        runtime_profile_spec=runtime_profile_spec,
        output_config=output_config,
        dataset_name="cpg_props",
    )


@datasaver()
@tag(layer="outputs", artifact="write_cpg_nodes_quality_delta", kind="delta")
def write_cpg_nodes_quality_delta(
    cpg_nodes: TableLike,
    runtime_profile_spec: RuntimeProfileSpec,
    output_config: OutputConfig,
) -> DataSaverDict:
    """Return stub metadata for CPG node quality output.

    Returns
    -------
    JsonDict
        Stub metadata payload.
    """
    return _delta_write(
        cpg_nodes,
        runtime_profile_spec=runtime_profile_spec,
        output_config=output_config,
        dataset_name="cpg_nodes_quality",
    )


@datasaver()
@tag(layer="outputs", artifact="write_cpg_props_quality_delta", kind="delta")
def write_cpg_props_quality_delta(
    cpg_props: TableLike,
    runtime_profile_spec: RuntimeProfileSpec,
    output_config: OutputConfig,
) -> DataSaverDict:
    """Return stub metadata for CPG prop quality output.

    Returns
    -------
    JsonDict
        Stub metadata payload.
    """
    return _delta_write(
        cpg_props,
        runtime_profile_spec=runtime_profile_spec,
        output_config=output_config,
        dataset_name="cpg_props_quality",
    )


@datasaver()
@tag(layer="outputs", artifact="write_cpg_props_json_delta", kind="delta")
def write_cpg_props_json_delta(
    cpg_props: TableLike,
    runtime_profile_spec: RuntimeProfileSpec,
    output_config: OutputConfig,
) -> DataSaverDict:
    """Return stub metadata for CPG props JSON output.

    Returns
    -------
    JsonDict
        Stub metadata payload.
    """
    return _delta_write(
        cpg_props,
        runtime_profile_spec=runtime_profile_spec,
        output_config=output_config,
        dataset_name="cpg_props_json",
    )


@datasaver()
@tag(layer="outputs", artifact="write_normalize_outputs_delta", kind="delta")
def write_normalize_outputs_delta(output_config: OutputConfig) -> DataSaverDict:
    """Return stub metadata for normalize outputs.

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
        msg = "Output directory must be configured for normalize outputs."
        raise ValueError(msg)
    path = Path(base) / "normalize_outputs.json"
    outputs: list[str] = []
    payload: dict[str, object] = {"outputs": outputs, "rows": 0}
    path.write_text(json.dumps(payload))
    return {"path": str(path), "rows": 0}


@datasaver()
@tag(layer="outputs", artifact="write_extract_error_artifacts_delta", kind="delta")
def write_extract_error_artifacts_delta(output_config: OutputConfig) -> DataSaverDict:
    """Return stub metadata for extract error artifacts.

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
        msg = "Output directory must be configured for extract errors."
        raise ValueError(msg)
    path = Path(base) / "extract_errors.json"
    errors: list[str] = []
    error_payload: dict[str, object] = {"errors": errors}
    path.write_text(json.dumps(error_payload))
    return {"path": str(path), "errors": errors}


@datasaver()
@tag(layer="outputs", artifact="write_run_manifest_delta", kind="delta")
def write_run_manifest_delta(output_config: OutputConfig) -> DataSaverDict:
    """Return stub metadata for run manifest.

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
        msg = "Output directory must be configured for run manifests."
        raise ValueError(msg)
    path = Path(base) / "manifest.json"
    path.write_text(json.dumps({"manifest": "pending"}))
    return {"path": str(path)}


@datasaver()
@tag(layer="outputs", artifact="write_run_bundle_dir", kind="bundle")
def write_run_bundle_dir(output_config: OutputConfig) -> DataSaverDict:
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
    bundle_dir = Path(base) / "run_bundle"
    bundle_dir.mkdir(parents=True, exist_ok=True)
    return {"bundle_dir": str(bundle_dir)}


__all__ = [
    "cpg_edges",
    "cpg_nodes",
    "cpg_props",
    "write_cpg_edges_delta",
    "write_cpg_nodes_delta",
    "write_cpg_nodes_quality_delta",
    "write_cpg_props_delta",
    "write_cpg_props_json_delta",
    "write_cpg_props_quality_delta",
    "write_extract_error_artifacts_delta",
    "write_normalize_outputs_delta",
    "write_run_bundle_dir",
    "write_run_manifest_delta",
]

"""Hamilton output nodes for inference-driven pipeline."""

from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING

import pyarrow as pa
from hamilton.function_modifiers import (
    cache,
    check_output_custom,
    datasaver,
    pipe_input,
    step,
    tag,
)

from arrowdsl.core.interop import TableLike
from core_types import JsonDict
from hamilton_pipeline.pipeline_types import OutputConfig
from hamilton_pipeline.validators import NonEmptyTableValidator
from ibis_engine.io_bridge import IbisDatasetWriteOptions, IbisDeltaWriteOptions
from storage.io import write_ibis_dataset_delta
from storage.ipc import payload_hash

if TYPE_CHECKING:
    from ibis_engine.execution import IbisExecutionContext


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


def _delta_write(
    table: TableLike,
    *,
    output_config: OutputConfig,
    dataset_name: str,
    ibis_execution: IbisExecutionContext,
) -> JsonDict:
    base_dir = output_config.output_dir or output_config.work_dir
    if not base_dir:
        msg = "Output directory must be configured for Delta materialization."
        raise ValueError(msg)
    target_dir = Path(base_dir) / dataset_name
    target_dir.mkdir(parents=True, exist_ok=True)
    delta_options = IbisDeltaWriteOptions(
        mode="overwrite",
        schema_mode="overwrite",
        commit_metadata={
            "dataset_name": dataset_name,
            "schema_fingerprint": payload_hash(
                {"schema": list(getattr(table.schema, "names", []))},
                pa.schema([pa.field("schema", pa.list_(pa.string()))]),
            ),
        },
    )
    result = write_ibis_dataset_delta(
        table,
        str(target_dir),
        options=IbisDatasetWriteOptions(
            execution=ibis_execution,
            writer_strategy="datafusion",
            delta_options=delta_options,
            delta_write_policy=output_config.delta_write_policy,
            delta_schema_policy=output_config.delta_schema_policy,
            storage_options=output_config.delta_storage_options,
        ),
        table_name=dataset_name,
    )
    return {
        "path": str(target_dir),
        "delta_version": result.version,
        "rows": _rows(table),
    }


@pipe_input(
    step(_stage_identity),
    step(_stage_ready),
    on_input="cpg_nodes_final",
    namespace="cpg_nodes",
)
@cache(format="parquet", behavior="default")
@check_output_custom(NonEmptyTableValidator())
@tag(layer="outputs", artifact="cpg_nodes", kind="table")
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
@cache(format="parquet", behavior="default")
@check_output_custom(NonEmptyTableValidator())
@tag(layer="outputs", artifact="cpg_edges", kind="table")
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
@cache(format="parquet", behavior="default")
@check_output_custom(NonEmptyTableValidator())
@tag(layer="outputs", artifact="cpg_props", kind="table")
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
    output_config: OutputConfig,
    ibis_execution: IbisExecutionContext,
) -> JsonDict:
    """Return stub metadata for CPG nodes output.

    Returns
    -------
    JsonDict
        Stub metadata payload.
    """
    return _delta_write(
        cpg_nodes,
        output_config=output_config,
        dataset_name="cpg_nodes",
        ibis_execution=ibis_execution,
    )


@datasaver()
@tag(layer="outputs", artifact="write_cpg_edges_delta", kind="delta")
def write_cpg_edges_delta(
    cpg_edges: TableLike,
    output_config: OutputConfig,
    ibis_execution: IbisExecutionContext,
) -> JsonDict:
    """Return stub metadata for CPG edges output.

    Returns
    -------
    JsonDict
        Stub metadata payload.
    """
    return _delta_write(
        cpg_edges,
        output_config=output_config,
        dataset_name="cpg_edges",
        ibis_execution=ibis_execution,
    )


@datasaver()
@tag(layer="outputs", artifact="write_cpg_props_delta", kind="delta")
def write_cpg_props_delta(
    cpg_props: TableLike,
    output_config: OutputConfig,
    ibis_execution: IbisExecutionContext,
) -> JsonDict:
    """Return stub metadata for CPG properties output.

    Returns
    -------
    JsonDict
        Stub metadata payload.
    """
    return _delta_write(
        cpg_props,
        output_config=output_config,
        dataset_name="cpg_props",
        ibis_execution=ibis_execution,
    )


@datasaver()
@tag(layer="outputs", artifact="write_cpg_nodes_quality_delta", kind="delta")
def write_cpg_nodes_quality_delta(
    cpg_nodes: TableLike,
    output_config: OutputConfig,
    ibis_execution: IbisExecutionContext,
) -> JsonDict:
    """Return stub metadata for CPG node quality output.

    Returns
    -------
    JsonDict
        Stub metadata payload.
    """
    return _delta_write(
        cpg_nodes,
        output_config=output_config,
        dataset_name="cpg_nodes_quality",
        ibis_execution=ibis_execution,
    )


@datasaver()
@tag(layer="outputs", artifact="write_cpg_props_quality_delta", kind="delta")
def write_cpg_props_quality_delta(
    cpg_props: TableLike,
    output_config: OutputConfig,
    ibis_execution: IbisExecutionContext,
) -> JsonDict:
    """Return stub metadata for CPG prop quality output.

    Returns
    -------
    JsonDict
        Stub metadata payload.
    """
    return _delta_write(
        cpg_props,
        output_config=output_config,
        dataset_name="cpg_props_quality",
        ibis_execution=ibis_execution,
    )


@datasaver()
@tag(layer="outputs", artifact="write_cpg_props_json_delta", kind="delta")
def write_cpg_props_json_delta(
    cpg_props: TableLike,
    output_config: OutputConfig,
    ibis_execution: IbisExecutionContext,
) -> JsonDict:
    """Return stub metadata for CPG props JSON output.

    Returns
    -------
    JsonDict
        Stub metadata payload.
    """
    return _delta_write(
        cpg_props,
        output_config=output_config,
        dataset_name="cpg_props_json",
        ibis_execution=ibis_execution,
    )


@datasaver()
@tag(layer="outputs", artifact="write_normalize_outputs_delta", kind="delta")
def write_normalize_outputs_delta(output_config: OutputConfig) -> JsonDict:
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
def write_extract_error_artifacts_delta(output_config: OutputConfig) -> JsonDict:
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
def write_run_manifest_delta(output_config: OutputConfig) -> JsonDict:
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
def write_run_bundle_dir(output_config: OutputConfig) -> JsonDict:
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

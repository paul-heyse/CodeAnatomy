"""Hamilton IO contracts for shared input/output metadata."""

from __future__ import annotations

import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, TypeVar, cast

import pyarrow as pa
from hamilton.function_modifiers import (
    dataloader,
    datasaver,
    parameterize,
    source,
    tag_outputs,
    value,
)

from datafusion_engine.arrow_interop import RecordBatchReaderLike
from datafusion_engine.delta_protocol import DeltaFeatureGate
from datafusion_engine.diagnostics import record_artifact, recorder_for_profile
from datafusion_engine.ingest import datafusion_from_arrow
from datafusion_engine.write_pipeline import WriteFormat, WriteMode, WritePipeline, WriteRequest
from engine.runtime_profile import RuntimeProfileSpec
from extract.extractors.scip.extract import ScipExtractContext, extract_scip_tables
from extract.helpers import ExtractExecutionContext
from extract.python.scope import PythonScopePolicy
from extract.scanning.repo_scan import RepoScanOptions, scan_repo_tables
from extract.scanning.repo_scope import RepoScopeOptions
from hamilton_pipeline.tag_policy import TagPolicy, apply_tag, tag_outputs_by_name
from incremental.types import IncrementalConfig
from schema_spec.system import DeltaMaintenancePolicy
from serde_artifacts import (
    ExtractErrorsArtifact,
    NormalizeOutputsArtifact,
    RunManifest,
    RunManifestEnvelope,
)
from serde_msgspec import convert, dumps_msgpack, to_builtins
from storage.deltalake import DeltaWritePolicy, delta_table_version
from storage.deltalake.config import DeltaSchemaPolicy, ParquetWriterPolicy
from storage.ipc_utils import payload_hash
from utils.uuid_factory import uuid7_hex

if TYPE_CHECKING:
    from collections.abc import Sequence
    from typing import Literal

    from datafusion.dataframe import DataFrame
    from hamilton.function_modifiers.dependencies import ParametrizedDependency

    from core_types import JsonDict, JsonValue
    from datafusion_engine.arrow_interop import TableLike
    from datafusion_engine.plan_bundle import DataFusionPlanBundle
    from datafusion_engine.runtime import SessionRuntime
    from datafusion_engine.write_pipeline import WriteResult
    from extract.extractors.scip.extract import ScipExtractOptions
    from hamilton_pipeline.types import CacheRuntimeContext, OutputConfig, RepoScanConfig

    type DataSaverDict = dict[str, JsonValue]
else:
    DataSaverDict = dict


@dataclass(frozen=True)
class InputContract:
    """Describe a tagged Hamilton input node."""

    name: str
    kind: str

    def tag_policy(self) -> TagPolicy:
        """Return the TagPolicy for this input contract.

        Returns
        -------
        TagPolicy
            Tag policy describing this input contract.
        """
        return TagPolicy(layer="inputs", kind=self.kind, artifact=self.name)


SCIP_INDEX_PATH = InputContract(name="scip_index_path", kind="path")
REPO_FILES = InputContract(name="repo_files", kind="dataloader")
SCIP_TABLES = InputContract(name="scip_tables", kind="dataloader")
SCIP_TABLES_EMPTY = InputContract(name="scip_tables_empty", kind="catalog")
SOURCE_CATALOG_INPUTS = InputContract(name="source_catalog_inputs", kind="catalog")


@dataclass(frozen=True)
class OutputMaterializationSpec:
    """Materialization metadata for Hamilton UI artifact capture."""

    table_node: str
    dataset_name: str
    materialized_name: str
    materialization: str = "delta"


_DELTA_OUTPUT_SPECS: tuple[OutputMaterializationSpec, ...] = (
    OutputMaterializationSpec(
        table_node="cpg_nodes",
        dataset_name="cpg_nodes",
        materialized_name="semantic.cpg_nodes_v1",
    ),
    OutputMaterializationSpec(
        table_node="cpg_edges",
        dataset_name="cpg_edges",
        materialized_name="semantic.cpg_edges_v1",
    ),
    OutputMaterializationSpec(
        table_node="cpg_props",
        dataset_name="cpg_props",
        materialized_name="semantic.cpg_props_v1",
    ),
    OutputMaterializationSpec(
        table_node="cpg_nodes_quality",
        dataset_name="cpg_nodes_quality",
        materialized_name="semantic.cpg_nodes_quality_v1",
    ),
    OutputMaterializationSpec(
        table_node="cpg_props_quality",
        dataset_name="cpg_props_quality",
        materialized_name="semantic.cpg_props_quality_v1",
    ),
    OutputMaterializationSpec(
        table_node="cpg_props_map_v1",
        dataset_name="cpg_props_map",
        materialized_name="semantic.cpg_props_map_v1",
    ),
    OutputMaterializationSpec(
        table_node="cpg_edges_by_src_v1",
        dataset_name="cpg_edges_by_src",
        materialized_name="semantic.cpg_edges_by_src_v1",
    ),
    OutputMaterializationSpec(
        table_node="cpg_edges_by_dst_v1",
        dataset_name="cpg_edges_by_dst",
        materialized_name="semantic.cpg_edges_by_dst_v1",
    ),
)
_DELTA_OUTPUT_SPECS_BY_DATASET: dict[str, OutputMaterializationSpec] = {
    spec.dataset_name: spec for spec in _DELTA_OUTPUT_SPECS
}


def delta_output_specs() -> tuple[OutputMaterializationSpec, ...]:
    """Return the canonical delta output materialization specs.

    Returns
    -------
    tuple[OutputMaterializationSpec, ...]
        Canonical materialization specs for delta outputs.
    """
    return _DELTA_OUTPUT_SPECS


def delta_output_spec_for(dataset_name: str) -> OutputMaterializationSpec | None:
    """Return the delta output spec for a dataset name.

    Parameters
    ----------
    dataset_name
        Dataset name to resolve.

    Returns
    -------
    OutputMaterializationSpec | None
        Materialization spec for the dataset, if configured.
    """
    return _DELTA_OUTPUT_SPECS_BY_DATASET.get(dataset_name)


def validate_delta_output_payload(
    payload: Mapping[str, object],
    *,
    dataset_name: str,
) -> None:
    """Validate an output payload against the delta output contract.

    Parameters
    ----------
    payload
        Output payload to validate.
    dataset_name
        Dataset name for spec lookup.

    Raises
    ------
    ValueError
        Raised when the payload is missing required keys or values.
    """
    spec = delta_output_spec_for(dataset_name)
    if spec is None:
        msg = f"No delta output spec registered for dataset: {dataset_name!r}."
        raise ValueError(msg)
    required = {
        "dataset_name",
        "materialization",
        "materialized_name",
        "path",
        "rows",
        "delta_version",
    }
    missing = required - payload.keys()
    if missing:
        msg = f"Output payload missing required keys: {sorted(missing)}."
        raise ValueError(msg)
    if payload.get("dataset_name") != spec.dataset_name:
        msg = "Output payload dataset_name does not match contract."
        raise ValueError(msg)
    if payload.get("materialization") != spec.materialization:
        msg = "Output payload materialization does not match contract."
        raise ValueError(msg)
    if payload.get("materialized_name") != spec.materialized_name:
        msg = "Output payload materialized_name does not match contract."
        raise ValueError(msg)
    path = payload.get("path")
    if not isinstance(path, str) or not path:
        msg = "Output payload path must be a non-empty string."
        raise ValueError(msg)
    rows = payload.get("rows")
    if not isinstance(rows, int) or rows < 0:
        msg = "Output payload rows must be a non-negative integer."
        raise ValueError(msg)
    delta_version = payload.get("delta_version")
    if not isinstance(delta_version, int) or delta_version < 0:
        msg = "Output payload delta_version must be a non-negative integer."
        raise ValueError(msg)


def _rows(table: TableLike | RecordBatchReaderLike) -> int:
    if isinstance(table, RecordBatchReaderLike):
        table = table.read_all()
    value = getattr(table, "num_rows", 0)
    if isinstance(value, bool):
        return 0
    if isinstance(value, int):
        return value
    return 0


def _repo_scan_options(
    config: RepoScanConfig,
    *,
    incremental: IncrementalConfig | None,
    cache_salt: str,
) -> RepoScanOptions:
    repo_id = incremental.repo_id if incremental is not None else None
    if cache_salt:
        repo_id = f"{repo_id}:{cache_salt}" if repo_id else cache_salt
    scope_config = config.scope_config
    scope_policy = RepoScopeOptions(
        python_scope=PythonScopePolicy(extra_extensions=scope_config.python_extensions),
        include_globs=scope_config.include_globs,
        exclude_globs=scope_config.exclude_globs,
        include_untracked=scope_config.include_untracked,
        include_submodules=scope_config.include_submodules,
        include_worktrees=scope_config.include_worktrees,
        follow_symlinks=scope_config.follow_symlinks,
    )
    return RepoScanOptions(
        repo_id=repo_id,
        scope_policy=scope_policy,
        max_files=config.max_files,
        diff_base_ref=config.diff_base_ref,
        diff_head_ref=config.diff_head_ref,
        changed_only=config.changed_only,
        record_pathspec_trace=config.record_pathspec_trace,
        pathspec_trace_limit=config.pathspec_trace_limit,
        pathspec_trace_pattern_limit=config.pathspec_trace_pattern_limit,
    )


@dataloader()
@apply_tag(REPO_FILES.tag_policy())
def repo_files(
    repo_scan_config: RepoScanConfig,
    runtime_profile_spec: RuntimeProfileSpec,
    cache_salt: str,
    incremental_config: IncrementalConfig | None = None,
) -> tuple[TableLike, dict[str, object]]:
    """Load repo scan outputs as a TableLike plus metadata.

    Returns
    -------
    tuple[TableLike, dict[str, object]]
        Repo scan table and metadata payload.
    """
    options = _repo_scan_options(
        repo_scan_config,
        incremental=incremental_config,
        cache_salt=cache_salt,
    )
    exec_ctx = ExtractExecutionContext(runtime_spec=runtime_profile_spec)
    tables = scan_repo_tables(repo_scan_config.repo_root, options=options, context=exec_ctx)
    table = tables["repo_files_v1"]
    if isinstance(table, RecordBatchReaderLike):
        table = table.read_all()
    metadata: dict[str, object] = {
        "repo_root": repo_scan_config.repo_root,
        "rows": _rows(table),
        "include_globs": list(repo_scan_config.scope_config.include_globs),
        "exclude_globs": list(repo_scan_config.scope_config.exclude_globs),
        "python_extensions": list(repo_scan_config.scope_config.python_extensions),
        "include_untracked": repo_scan_config.scope_config.include_untracked,
        "include_submodules": repo_scan_config.scope_config.include_submodules,
        "include_worktrees": repo_scan_config.scope_config.include_worktrees,
        "changed_only": options.changed_only,
        "diff_base_ref": options.diff_base_ref,
        "diff_head_ref": options.diff_head_ref,
        "repo_id": options.repo_id,
    }
    return table, metadata


@dataloader()
@apply_tag(SCIP_TABLES.tag_policy())
def scip_tables(
    repo_root: str,
    scip_index_path: str | None,
    scip_extract_options: ScipExtractOptions,
    runtime_profile_spec: RuntimeProfileSpec,
) -> tuple[Mapping[str, TableLike], dict[str, object]]:
    """Load SCIP tables from an index.scip file.

    Returns
    -------
    tuple[Mapping[str, TableLike], dict[str, object]]
        Mapping of table names to tables plus metadata.
    """
    context = ScipExtractContext(
        scip_index_path=scip_index_path,
        repo_root=repo_root,
        runtime_spec=runtime_profile_spec,
    )
    tables = extract_scip_tables(context=context, options=scip_extract_options, prefer_reader=False)
    metadata: dict[str, object] = {
        "scip_index_path": scip_index_path,
        "tables": sorted(tables.keys()),
        "row_counts": {name: _rows(table) for name, table in tables.items()},
    }
    return tables, metadata


@dataclass(frozen=True)
class OutputRuntimeContext:
    """Runtime and output configuration context for outputs."""

    runtime_profile_spec: RuntimeProfileSpec
    output_config: OutputConfig
    cache_context: CacheRuntimeContext


@dataclass(frozen=True)
class OutputPlanContext:
    """Plan metadata context for outputs."""

    plan_signature: str
    plan_fingerprints: Mapping[str, str]
    plan_bundles_by_task: Mapping[str, DataFusionPlanBundle]
    run_id: str
    artifact_ids: Mapping[str, str]
    materialized_outputs: tuple[str, ...] | None = None


@dataclass(frozen=True)
class DeltaWriteInputs:
    """Inputs for Delta output writes."""

    runtime: OutputRuntimeContext
    plan: OutputPlanContext
    dataset_name: str
    plan_dataset_name: str | None = None
    write_policy_override: DeltaWritePolicy | None = None


@dataclass(frozen=True)
class OutputPlanArtifactsContext:
    """Artifacts metadata for building output plan contexts."""

    plan_fingerprints: Mapping[str, str]
    plan_bundles_by_task: Mapping[str, DataFusionPlanBundle]
    plan_artifact_ids: Mapping[str, str]


@dataclass(frozen=True)
class PrimaryOutputs:
    """Primary CPG outputs for run manifests."""

    cpg_nodes: DataSaverDict
    cpg_edges: DataSaverDict
    cpg_props: DataSaverDict
    cpg_nodes_quality: DataSaverDict
    cpg_props_quality: DataSaverDict


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


@dataclass(frozen=True)
class _DeltaWriteSpec:
    """Parameterized Delta write specification for CPG outputs."""

    dataset_name: str
    plan_dataset_name: str | None = None
    write_policy_override: DeltaWritePolicy | None = None


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


F = TypeVar("F", bound=Callable[..., object])


def _normalize_datasaver_return[F: Callable[..., object]](fn: F) -> F:
    annotations = dict(getattr(fn, "__annotations__", {}))
    annotations["return"] = dict
    fn.__annotations__ = annotations
    return fn


def datasaver_dict() -> Callable[[F], F]:
    """Return a datasaver decorator that normalizes return type annotations.

    Returns
    -------
    Callable[[F], F]
        Decorator that annotates datasaver returns as dict payloads.
    """
    decorator = datasaver()

    def _wrapped(fn: F) -> F:
        return decorator(_normalize_datasaver_return(fn))

    return _wrapped


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
        "schema_identity_hash": payload_hash(
            {"schema": list(getattr(table.schema, "names", []))},
            pa.schema([pa.field("schema", pa.list_(pa.string()))]),
        ),
    }
    if plan_signature is not None:
        commit_metadata["plan_signature"] = plan_signature
    if plan_identity_hash is not None:
        commit_metadata["plan_identity_hash"] = plan_identity_hash
    return commit_metadata


def _delta_write_dataframe(
    session_runtime: SessionRuntime,
    *,
    dataset_name: str,
    table: TableLike,
) -> DataFrame:
    return datafusion_from_arrow(
        session_runtime.ctx,
        name=f"__output_{dataset_name}_{uuid7_hex()}",
        value=table,
    )


def _delta_write_format_options(
    table: TableLike,
    *,
    details: _DeltaWritePlanDetails,
    output_config: OutputConfig,
) -> dict[str, object]:
    commit_metadata = _delta_commit_metadata(
        table,
        dataset_name=details.dataset_name,
        plan_signature=details.plan_signature,
        plan_identity_hash=details.plan_identity_hash,
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
    schema_policy = output_config.delta_schema_policy or _CPG_SCHEMA_POLICY
    if schema_policy is not None:
        format_options["delta_schema_policy"] = schema_policy
    if output_config.delta_storage_options is not None:
        format_options["storage_options"] = dict(output_config.delta_storage_options)
    maintenance_policy = _CPG_MAINTENANCE_POLICIES.get(details.dataset_name)
    if maintenance_policy is not None:
        format_options["delta_maintenance_policy"] = maintenance_policy
        format_options["delta_feature_gate"] = _OUTPUT_FEATURE_GATE
    return format_options


def _delta_output_version(
    *,
    write_result: WriteResult,
    target_dir: Path,
    output_config: OutputConfig,
    dataset_name: str,
) -> int:
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
    return final_version


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
    df = _delta_write_dataframe(
        session_runtime,
        dataset_name=details.dataset_name,
        table=table,
    )
    format_options = _delta_write_format_options(
        table,
        details=details,
        output_config=output_config,
    )
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
    final_version = _delta_output_version(
        write_result=write_result,
        target_dir=target_dir,
        output_config=output_config,
        dataset_name=details.dataset_name,
    )
    spec = delta_output_spec_for(details.dataset_name)
    if spec is None:
        msg = f"Missing output contract for dataset: {details.dataset_name!r}."
        raise ValueError(msg)
    payload: JsonDict = {
        "dataset_name": spec.dataset_name,
        "materialization": spec.materialization,
        "materialized_name": spec.materialized_name,
        "path": str(target_dir),
        "delta_version": final_version,
        "rows": _rows(table),
    }
    validate_delta_output_payload(payload, dataset_name=details.dataset_name)
    return payload


def _delta_protocol_payload(protocol: object | None) -> dict[str, object] | None:
    if not isinstance(protocol, Mapping):
        return None
    payload: dict[str, object] = {}
    for key, protocol_value in protocol.items():
        if isinstance(protocol_value, (str, int, float)) or protocol_value is None:
            payload[str(key)] = protocol_value
            continue
        if isinstance(protocol_value, (list, tuple)):
            payload[str(key)] = [str(item) for item in protocol_value]
            continue
        payload[str(key)] = str(protocol_value)
    return payload or None


def _delta_inputs_payload(
    plan_bundles_by_task: Mapping[str, DataFusionPlanBundle],
) -> tuple[dict[str, object], ...]:
    payloads: list[dict[str, object]] = []
    seen: set[tuple[object, ...]] = set()
    for bundle in plan_bundles_by_task.values():
        for pin in bundle.delta_inputs:
            key = (pin.dataset_name, pin.version, pin.timestamp)
            if key in seen:
                continue
            seen.add(key)
            payloads.append(
                {
                    "dataset_name": pin.dataset_name,
                    "version": pin.version,
                    "timestamp": pin.timestamp,
                    "protocol": _delta_protocol_payload(pin.protocol),
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
        if bundle is None:
            continue
        plan_identity_hash = bundle.plan_identity_hash
        if plan_identity_hash:
            payload[dataset_name] = plan_identity_hash
    return payload


_OUTPUT_PLAN_FINGERPRINTS: dict[str, str] = {
    "cpg_nodes": "cpg_nodes",
    "cpg_edges": "cpg_edges",
    "cpg_props": "cpg_props",
    "cpg_nodes_quality": "cpg_nodes_quality",
    "cpg_props_quality": "cpg_props_quality",
    "cpg_props_map": "cpg_props_map",
    "cpg_edges_by_src": "cpg_edges_by_src",
    "cpg_edges_by_dst": "cpg_edges_by_dst",
}

_OUTPUT_DELTA_FEATURES: tuple[
    Literal[
        "change_data_feed",
        "column_mapping",
        "deletion_vectors",
        "in_commit_timestamps",
        "row_tracking",
        "v2_checkpoints",
    ],
    ...,
] = (
    "change_data_feed",
    "column_mapping",
    "v2_checkpoints",
)
_OUTPUT_FEATURE_GATE = DeltaFeatureGate(
    required_writer_features=_OUTPUT_DELTA_FEATURES,
)
_CPG_SCHEMA_POLICY = DeltaSchemaPolicy(column_mapping_mode="name")
_BLOOM_FILTER_FPP = 0.01
_BLOOM_FILTER_NDV = 10_000_000


def _parquet_policy(
    *,
    stats_columns: tuple[str, ...],
    bloom_columns: tuple[str, ...],
) -> ParquetWriterPolicy:
    return ParquetWriterPolicy(
        statistics_enabled=stats_columns,
        bloom_filter_enabled=bloom_columns,
        bloom_filter_fpp=_BLOOM_FILTER_FPP,
        bloom_filter_ndv=_BLOOM_FILTER_NDV,
    )


def _maintenance_policy(z_order_cols: tuple[str, ...]) -> DeltaMaintenancePolicy:
    return DeltaMaintenancePolicy(
        optimize_on_write=True,
        optimize_target_size=256 * 1024 * 1024,
        z_order_cols=z_order_cols,
        z_order_when="after_partition_complete",
        vacuum_on_write=False,
        enable_deletion_vectors=True,
        enable_v2_checkpoints=True,
        enable_log_compaction=True,
    )


_CPG_NODES_WRITE_POLICY = DeltaWritePolicy(
    target_file_size=256 * 1024 * 1024,
    partition_by=("node_kind",),
    zorder_by=("file_id", "bstart", "node_id"),
    stats_policy="explicit",
    stats_columns=("file_id", "path", "node_id", "node_kind", "bstart", "bend"),
    parquet_writer_policy=_parquet_policy(
        stats_columns=("file_id", "path", "node_id", "node_kind", "bstart", "bend"),
        bloom_columns=("node_id", "file_id"),
    ),
    enable_features=_OUTPUT_DELTA_FEATURES,
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
    parquet_writer_policy=_parquet_policy(
        stats_columns=(
            "path",
            "edge_id",
            "edge_kind",
            "src_node_id",
            "dst_node_id",
            "bstart",
            "bend",
        ),
        bloom_columns=("edge_id", "src_node_id", "dst_node_id"),
    ),
    enable_features=_OUTPUT_DELTA_FEATURES,
)
_CPG_PROPS_WRITE_POLICY = DeltaWritePolicy(
    target_file_size=256 * 1024 * 1024,
    partition_by=("entity_kind",),
    zorder_by=("entity_kind", "prop_key", "entity_id"),
    stats_policy="explicit",
    stats_columns=("entity_kind", "entity_id", "prop_key", "node_kind"),
    parquet_writer_policy=_parquet_policy(
        stats_columns=("entity_kind", "entity_id", "prop_key", "node_kind"),
        bloom_columns=("entity_id",),
    ),
    enable_features=_OUTPUT_DELTA_FEATURES,
)
_CPG_PROPS_MAP_WRITE_POLICY = DeltaWritePolicy(
    target_file_size=128 * 1024 * 1024,
    zorder_by=("entity_kind", "entity_id"),
    stats_policy="explicit",
    stats_columns=("entity_kind", "entity_id", "node_kind"),
    parquet_writer_policy=_parquet_policy(
        stats_columns=("entity_kind", "entity_id", "node_kind"),
        bloom_columns=("entity_id",),
    ),
    enable_features=_OUTPUT_DELTA_FEATURES,
)
_CPG_EDGES_BY_SRC_WRITE_POLICY = DeltaWritePolicy(
    target_file_size=128 * 1024 * 1024,
    zorder_by=("src_node_id",),
    stats_policy="explicit",
    stats_columns=("src_node_id",),
    parquet_writer_policy=_parquet_policy(
        stats_columns=("src_node_id",),
        bloom_columns=("src_node_id",),
    ),
    enable_features=_OUTPUT_DELTA_FEATURES,
)
_CPG_EDGES_BY_DST_WRITE_POLICY = DeltaWritePolicy(
    target_file_size=128 * 1024 * 1024,
    zorder_by=("dst_node_id",),
    stats_policy="explicit",
    stats_columns=("dst_node_id",),
    parquet_writer_policy=_parquet_policy(
        stats_columns=("dst_node_id",),
        bloom_columns=("dst_node_id",),
    ),
    enable_features=_OUTPUT_DELTA_FEATURES,
)
_CPG_MAINTENANCE_POLICIES: dict[str, DeltaMaintenancePolicy] = {
    "cpg_nodes": _maintenance_policy(("file_id", "node_id", "bstart")),
    "cpg_nodes_quality": _maintenance_policy(("file_id", "node_id", "bstart")),
    "cpg_edges": _maintenance_policy(("src_node_id", "dst_node_id", "edge_id")),
    "cpg_props": _maintenance_policy(("entity_kind", "entity_id", "prop_key")),
    "cpg_props_quality": _maintenance_policy(("entity_kind", "entity_id", "prop_key")),
    "cpg_props_map": _maintenance_policy(("entity_kind", "entity_id")),
    "cpg_edges_by_src": _maintenance_policy(("src_node_id",)),
    "cpg_edges_by_dst": _maintenance_policy(("dst_node_id",)),
}


def _delta_write_spec(
    *,
    table: str,
    dataset_name: str,
    write_policy: DeltaWritePolicy | None,
    plan_dataset_name: str | None = None,
) -> dict[str, ParametrizedDependency]:
    return {
        "table": source(table),
        "write_spec": value(
            _DeltaWriteSpec(
                dataset_name=dataset_name,
                plan_dataset_name=plan_dataset_name,
                write_policy_override=write_policy,
            )
        ),
    }


_CPG_DELTA_WRITE_PARAMS: dict[str, dict[str, ParametrizedDependency]] = {
    "write_cpg_nodes_delta": _delta_write_spec(
        table="cpg_nodes",
        dataset_name="cpg_nodes",
        write_policy=_CPG_NODES_WRITE_POLICY,
    ),
    "write_cpg_edges_delta": _delta_write_spec(
        table="cpg_edges",
        dataset_name="cpg_edges",
        write_policy=_CPG_EDGES_WRITE_POLICY,
    ),
    "write_cpg_props_delta": _delta_write_spec(
        table="cpg_props",
        dataset_name="cpg_props",
        write_policy=_CPG_PROPS_WRITE_POLICY,
    ),
    "write_cpg_props_map_delta": _delta_write_spec(
        table="cpg_props_map_v1",
        dataset_name="cpg_props_map",
        write_policy=_CPG_PROPS_MAP_WRITE_POLICY,
    ),
    "write_cpg_edges_by_src_delta": _delta_write_spec(
        table="cpg_edges_by_src_v1",
        dataset_name="cpg_edges_by_src",
        write_policy=_CPG_EDGES_BY_SRC_WRITE_POLICY,
    ),
    "write_cpg_edges_by_dst_delta": _delta_write_spec(
        table="cpg_edges_by_dst_v1",
        dataset_name="cpg_edges_by_dst",
        write_policy=_CPG_EDGES_BY_DST_WRITE_POLICY,
    ),
    "write_cpg_nodes_quality_delta": _delta_write_spec(
        table="cpg_nodes_quality",
        dataset_name="cpg_nodes_quality",
        plan_dataset_name="cpg_nodes",
        write_policy=_CPG_NODES_WRITE_POLICY,
    ),
    "write_cpg_props_quality_delta": _delta_write_spec(
        table="cpg_props_quality",
        dataset_name="cpg_props_quality",
        plan_dataset_name="cpg_props",
        write_policy=_CPG_PROPS_WRITE_POLICY,
    ),
}


@parameterize(**_CPG_DELTA_WRITE_PARAMS)
@tag_outputs(
    **tag_outputs_by_name(
        tuple(_CPG_DELTA_WRITE_PARAMS),
        layer="outputs",
        kind="delta",
    )
)
@datasaver_dict()
def write_cpg_delta_output(
    table: TableLike,
    output_runtime_context: OutputRuntimeContext,
    output_plan_context: OutputPlanContext,
    write_spec: _DeltaWriteSpec,
) -> DataSaverDict:
    """Return metadata for Delta-backed CPG outputs.

    Returns
    -------
    JsonDict
        Output metadata payload.
    """
    write_override = (
        write_spec.write_policy_override
        if output_runtime_context.output_config.delta_write_policy is None
        else None
    )
    return _delta_write(
        table,
        inputs=DeltaWriteInputs(
            runtime=output_runtime_context,
            plan=output_plan_context,
            dataset_name=write_spec.dataset_name,
            plan_dataset_name=write_spec.plan_dataset_name,
            write_policy_override=write_override,
        ),
    )


@datasaver_dict()
@apply_tag(
    TagPolicy(
        layer="outputs",
        kind="delta",
        artifact="write_normalize_outputs_delta",
    )
)
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


@datasaver_dict()
@apply_tag(
    TagPolicy(
        layer="outputs",
        kind="delta",
        artifact="write_extract_error_artifacts_delta",
    )
)
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
            "name": "cpg_nodes_quality",
            "path": primary.cpg_nodes_quality.get("path"),
            "delta_version": primary.cpg_nodes_quality.get("delta_version"),
            "rows": primary.cpg_nodes_quality.get("rows"),
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
            "name": "cpg_props_quality",
            "path": primary.cpg_props_quality.get("path"),
            "delta_version": primary.cpg_props_quality.get("delta_version"),
            "rows": primary.cpg_props_quality.get("rows"),
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
        name=f"__{request.table_name}_{output_plan_context.run_id}_{uuid7_hex()}",
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
    cache_context = output_runtime_context.cache_context
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
        cache_path=cache_context.cache_path,
        cache_log_glob=cache_context.cache_log_glob,
        cache_policy_profile=cache_context.cache_policy_profile,
        cache_log_enabled=cache_context.cache_log_enabled,
        materialized_outputs=output_plan_context.materialized_outputs,
    )


def _run_manifest_table_payload(manifest: RunManifest) -> JsonDict:
    materialized_outputs_msgpack = (
        dumps_msgpack(to_builtins(manifest.materialized_outputs, str_keys=True))
        if manifest.materialized_outputs is not None
        else None
    )
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
        "cache_path": manifest.cache_path,
        "cache_log_glob": manifest.cache_log_glob,
        "cache_policy_profile": manifest.cache_policy_profile,
        "cache_log_enabled": manifest.cache_log_enabled,
        "materialized_outputs_msgpack": materialized_outputs_msgpack,
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
        name=f"__run_manifest_{run_id}_{uuid7_hex()}",
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


@datasaver_dict()
@apply_tag(
    TagPolicy(
        layer="outputs",
        kind="delta",
        artifact="write_run_manifest_delta",
    )
)
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


@datasaver_dict()
@apply_tag(
    TagPolicy(
        layer="outputs",
        kind="bundle",
        artifact="write_run_bundle_dir",
    )
)
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


_NORMALIZE_OUTPUTS_TABLE_NAME = "normalize_outputs_v1"
_EXTRACT_ERRORS_TABLE_NAME = "extract_errors_v1"
_OUTPUT_METADATA_WRITE_POLICY = DeltaWritePolicy(
    target_file_size=128 * 1024 * 1024,
)
_RUN_MANIFEST_WRITE_POLICY = DeltaWritePolicy(
    target_file_size=128 * 1024 * 1024,
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
        pa.field("runtime_profile_name", pa.string(), nullable=False),
        pa.field("runtime_profile_hash", pa.string(), nullable=False),
        pa.field("determinism_tier", pa.string(), nullable=False),
        pa.field("output_dir", pa.string(), nullable=False),
        pa.field("cache_path", pa.string(), nullable=True),
        pa.field("cache_log_glob", pa.string(), nullable=True),
        pa.field("cache_policy_profile", pa.string(), nullable=True),
        pa.field("cache_log_enabled", pa.bool_(), nullable=False),
        pa.field("materialized_outputs_msgpack", pa.binary(), nullable=True),
    ]
)


__all__ = [
    "REPO_FILES",
    "SCIP_INDEX_PATH",
    "SCIP_TABLES",
    "SCIP_TABLES_EMPTY",
    "SOURCE_CATALOG_INPUTS",
    "AdjacencyOutputs",
    "DeltaWriteInputs",
    "InputContract",
    "OutputMaterializationSpec",
    "OutputPlanArtifactsContext",
    "OutputPlanContext",
    "OutputRuntimeContext",
    "PrimaryOutputs",
    "delta_output_spec_for",
    "delta_output_specs",
    "repo_files",
    "scip_tables",
    "validate_delta_output_payload",
    "write_cpg_delta_output",
    "write_extract_error_artifacts_delta",
    "write_normalize_outputs_delta",
    "write_run_bundle_dir",
    "write_run_manifest_delta",
]

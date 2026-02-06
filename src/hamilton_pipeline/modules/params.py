"""Hamilton nodes for parameter bundles and param-table registration."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

import pyarrow as pa
from datafusion.dataframe import DataFrame

from core_types import JsonDict
from datafusion_engine.arrow.schema import task_name_field
from datafusion_engine.identity import schema_identity_hash
from datafusion_engine.io.ingest import datafusion_from_arrow
from datafusion_engine.io.write import WriteFormat, WriteMode, WritePipeline, WriteRequest
from datafusion_engine.lineage.diagnostics import recorder_for_profile
from datafusion_engine.session.runtime import read_delta_as_reader
from datafusion_engine.tables.param import (
    ListParamSpec,
    ParamTableArtifact,
    ParamTablePolicy,
    ParamTableRegistry,
    ParamTableScope,
    ParamTableSpec,
    param_signature_from_array,
    param_table_name,
    unique_values,
)
from datafusion_engine.tables.param import (
    scalar_param_signature as build_scalar_param_signature,
)
from engine.runtime_profile import RuntimeProfileSpec
from engine.session import EngineSession
from hamilton_pipeline.tag_policy import TagPolicy, apply_tag
from hamilton_pipeline.types import ActiveParamSet, OutputConfig, ParamBundle, TaskDependencyReport
from relspec.inferred_deps import infer_deps_from_view_nodes
from relspec.pipeline_policy import PipelinePolicy
from storage.deltalake import delta_schema_configuration, delta_write_configuration
from storage.deltalake.config import DeltaSchemaPolicy, DeltaWritePolicy
from utils.uuid_factory import uuid7_hex

if TYPE_CHECKING:
    from datafusion_engine.views.graph import ViewNode
else:
    ViewNode = object


@apply_tag(TagPolicy(layer="params", kind="object", artifact="param_table_policy"))
def param_table_policy(pipeline_policy: PipelinePolicy) -> ParamTablePolicy:
    """Return the default parameter table policy.

    Returns:
    -------
    ParamTablePolicy
        Default parameter table policy.
    """
    return pipeline_policy.param_table_policy


@apply_tag(TagPolicy(layer="params", kind="scalar", artifact="param_table_scope_key"))
def param_table_scope_key(
    param_table_policy: ParamTablePolicy,
    engine_session: EngineSession | None = None,
) -> str | None:
    """Return an optional scope key for parameter table registration.

    Returns:
    -------
    str | None
        Scope key for schema/table scoping when configured.
    """
    if param_table_policy.scope == ParamTableScope.PER_SESSION:
        if engine_session is None or engine_session.datafusion_profile is None:
            return None
        return engine_session.datafusion_profile.context_cache_key()
    return None


@apply_tag(TagPolicy(layer="params", kind="spec", artifact="param_table_specs"))
def param_table_specs() -> tuple[ParamTableSpec, ...]:
    """Return default parameter table specs.

    Returns:
    -------
    tuple[ParamTableSpec, ...]
        Default parameter table specs.
    """
    return (
        ListParamSpec(
            logical_name="file_allowlist",
            key_col="file_id",
            schema=pa.schema([pa.field("file_id", pa.string())]),
        ),
        ListParamSpec(
            logical_name="symbol_allowlist",
            key_col="symbol",
            schema=pa.schema([pa.field("symbol", pa.string())]),
        ),
        ListParamSpec(
            logical_name="task_allowlist",
            key_col="task_name",
            schema=pa.schema([task_name_field(nullable=True)]),
        ),
    )


@dataclass(frozen=True)
class ParamTableInputs:
    """Bundled inputs for param table registration."""

    scope_key: str | None = None
    delta_paths: Mapping[str, str] = field(default_factory=dict)
    active_set: frozenset[str] | None = None


@apply_tag(TagPolicy(layer="params", kind="object", artifact="param_table_inputs"))
def param_table_inputs(
    param_table_scope_key: str | None,
    param_table_delta_paths: Mapping[str, str] | None,
    active_param_set: ActiveParamSet | None,
) -> ParamTableInputs:
    """Bundle inputs for param table registration.

    Returns:
    -------
    ParamTableInputs
        Normalized param table inputs for registration.
    """
    normalized_paths = (
        {str(key): str(val) for key, val in param_table_delta_paths.items()}
        if param_table_delta_paths
        else {}
    )
    active_set = active_param_set.active if active_param_set is not None else None
    return ParamTableInputs(
        scope_key=param_table_scope_key,
        delta_paths=normalized_paths,
        active_set=active_set,
    )


@apply_tag(TagPolicy(layer="params", kind="bundle", artifact="relspec_param_dependency_reports"))
def relspec_param_dependency_reports(
    view_nodes: tuple[ViewNode, ...],
    param_table_policy: ParamTablePolicy,
) -> tuple[TaskDependencyReport, ...]:
    """Infer param-table dependencies from view inputs.

    Returns:
    -------
    tuple[TaskDependencyReport, ...]
        Per-task parameter dependency reports.
    """
    prefix = param_table_policy.prefix
    reports: list[TaskDependencyReport] = []
    inferred = infer_deps_from_view_nodes(view_nodes)
    for dep in inferred:
        inputs = dep.inputs
        param_tables = {
            name[len(prefix) :] for name in inputs if prefix and name.startswith(prefix)
        }
        dataset_tables = {name for name in inputs if not name.startswith(prefix)}
        reports.append(
            TaskDependencyReport(
                task_name=dep.task_name,
                param_tables=tuple(sorted(param_tables)),
                dataset_tables=tuple(sorted(dataset_tables)),
            )
        )
    return tuple(reports)


@apply_tag(TagPolicy(layer="params", kind="object", artifact="param_bundle"))
def param_bundle(
    relspec_param_values: JsonDict,
    param_table_specs: tuple[ParamTableSpec, ...],
) -> ParamBundle:
    """Return a parameter bundle split into scalar and list values.

    Returns:
    -------
    ParamBundle
        Parameter bundle with scalar/list values separated.
    """
    list_names = {spec.logical_name for spec in param_table_specs}
    scalar_values: dict[str, object] = {}
    list_values: dict[str, tuple[object, ...]] = {}
    for key, value in relspec_param_values.items():
        if key in list_names:
            list_values[key] = _parse_list_values(key, value)
        else:
            scalar_values[key] = value
    for name in list_names:
        list_values.setdefault(name, ())
    return ParamBundle(scalar=scalar_values, lists=list_values)


@apply_tag(TagPolicy(layer="params", kind="scalar", artifact="param_scalar_signature"))
def param_scalar_signature(param_bundle: ParamBundle) -> str:
    """Return a stable signature for scalar parameters.

    Returns:
    -------
    str
        Signature hash for scalar parameter values.
    """
    return build_scalar_param_signature(param_bundle.scalar)


@apply_tag(TagPolicy(layer="params", kind="object", artifact="param_table_registry"))
def param_table_registry(
    param_bundle: ParamBundle,
    param_table_specs: tuple[ParamTableSpec, ...],
    param_table_policy: ParamTablePolicy,
    param_table_inputs: ParamTableInputs | None = None,
) -> ParamTableRegistry:
    """Return a param table registry populated from runtime values.

    Returns:
    -------
    ParamTableRegistry
        Registry populated with param table artifacts.
    """
    specs = {spec.logical_name: spec for spec in param_table_specs}
    inputs = param_table_inputs or ParamTableInputs()
    registry = ParamTableRegistry.from_specs(
        specs,
        policy=param_table_policy,
        scope_key=inputs.scope_key,
    )
    paths = inputs.delta_paths
    active = inputs.active_set if inputs.active_set is not None else frozenset(specs.keys())
    for spec in param_table_specs:
        if spec.logical_name not in active:
            continue
        if spec.logical_name in paths:
            artifact = _artifact_from_delta(spec, paths[spec.logical_name])
            registry.artifacts.register(spec.logical_name, artifact, overwrite=True)
            continue
        values = param_bundle.list_values(spec.logical_name)
        registry.register_values(spec.logical_name, values)
    return registry


@apply_tag(TagPolicy(layer="params", kind="object", artifact="param_table_artifacts"))
def param_table_artifacts(
    param_table_registry: ParamTableRegistry,
) -> Mapping[str, ParamTableArtifact]:
    """Return param table artifacts from the registry.

    Returns:
    -------
    Mapping[str, ParamTableArtifact]
        Param table artifacts keyed by logical name.
    """
    return param_table_registry.artifacts.snapshot()


@apply_tag(TagPolicy(layer="params", kind="object", artifact="param_table_name_map"))
def param_table_name_map(
    param_table_registry: ParamTableRegistry,
    engine_session: EngineSession,
) -> dict[str, str]:
    """Return param table names without backend registration.

    Returns:
    -------
    dict[str, str]
        Mapping of logical param names to qualified table names.
    """
    _ = engine_session
    policy = param_table_registry.policy
    return {
        logical_name: param_table_name(policy, logical_name)
        for logical_name in param_table_registry.artifacts
    }


@apply_tag(TagPolicy(layer="params", kind="object", artifact="param_tables_df"))
def param_tables_datafusion(
    param_table_registry: ParamTableRegistry,
    engine_session: EngineSession,
) -> dict[str, DataFrame]:
    """Return DataFusion DataFrames for registered param tables.

    Returns:
    -------
    dict[str, datafusion.dataframe.DataFrame]
        DataFusion DataFrames keyed by logical name.
    """
    return param_table_registry.datafusion_tables(engine_session.df_ctx())


@apply_tag(TagPolicy(layer="params", kind="side_effect", artifact="param_table_delta"))
def write_param_tables_delta(
    param_table_artifacts: Mapping[str, ParamTableArtifact],
    runtime_profile_spec: RuntimeProfileSpec,
    output_config: OutputConfig,
) -> Mapping[str, JsonDict] | None:
    """Write param tables as Delta tables when enabled.

    Returns:
    -------
    Mapping[str, JsonDict] | None
        Mapping of logical names to Delta table reports, or ``None`` when disabled.
    """
    if not output_config.materialize_param_tables:
        return None
    base = output_config.work_dir or output_config.output_dir
    if not base:
        return None
    base_dir = Path(base) / "params"
    base_dir.mkdir(parents=True, exist_ok=True)
    write_policy = output_config.delta_write_policy
    schema_policy = output_config.delta_schema_policy
    storage_options = output_config.delta_storage_options
    runtime_profile = runtime_profile_spec.datafusion
    session_runtime = runtime_profile.session_runtime()
    output: dict[str, JsonDict] = {}
    for logical_name, artifact in param_table_artifacts.items():
        target_dir = base_dir / logical_name
        target_dir.mkdir(parents=True, exist_ok=True)
        configuration: dict[str, str] = {}
        configuration.update(delta_write_configuration(write_policy) or {})
        configuration.update(delta_schema_configuration(schema_policy) or {})
        df = datafusion_from_arrow(
            session_runtime.ctx,
            name=f"__param_table_{logical_name}_{uuid7_hex()}",
            value=artifact.table,
        )
        format_options: dict[str, object] = {
            "commit_metadata": {
                "dataset_name": logical_name,
                "schema_identity_hash": artifact.schema_identity_hash,
            },
            "table_properties": configuration or None,
            "schema_mode": "overwrite",
        }
        if storage_options is not None:
            format_options["storage_options"] = dict(storage_options)
        if write_policy is not None:
            format_options["target_file_size"] = write_policy.target_file_size
        pipeline = WritePipeline(
            session_runtime.ctx,
            sql_options=runtime_profile.sql_options(),
            recorder=recorder_for_profile(
                runtime_profile,
                operation_id=f"hamilton_params::{logical_name}",
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
        output[logical_name] = {
            "path": str(target_dir),
            "delta_version": write_result.delta_result.version
            if write_result.delta_result is not None
            else None,
            "rows": int(artifact.rows),
            "delta_write_policy": _delta_write_policy_payload(write_policy),
            "delta_schema_policy": _delta_schema_policy_payload(schema_policy),
        }
    return output


def _delta_write_policy_payload(policy: DeltaWritePolicy | None) -> JsonDict | None:
    if policy is None:
        return None
    parquet_policy = None
    if policy.parquet_writer_policy is not None:
        parquet_policy = {
            "statistics_enabled": list(policy.parquet_writer_policy.statistics_enabled),
            "statistics_level": policy.parquet_writer_policy.statistics_level,
            "bloom_filter_enabled": list(policy.parquet_writer_policy.bloom_filter_enabled),
            "bloom_filter_fpp": policy.parquet_writer_policy.bloom_filter_fpp,
            "bloom_filter_ndv": policy.parquet_writer_policy.bloom_filter_ndv,
            "dictionary_enabled": list(policy.parquet_writer_policy.dictionary_enabled),
        }
    return {
        "target_file_size": policy.target_file_size,
        "partition_by": list(policy.partition_by),
        "zorder_by": list(policy.zorder_by),
        "stats_policy": policy.stats_policy,
        "stats_columns": list(policy.stats_columns) if policy.stats_columns is not None else None,
        "stats_max_columns": policy.stats_max_columns,
        "enable_features": list(policy.enable_features),
        "parquet_writer_policy": parquet_policy,
    }


def _delta_schema_policy_payload(policy: DeltaSchemaPolicy | None) -> JsonDict | None:
    if policy is None:
        return None
    return {
        "schema_mode": policy.schema_mode,
        "column_mapping_mode": policy.column_mapping_mode,
    }


@apply_tag(TagPolicy(layer="params", kind="object", artifact="active_param_set"))
def active_param_set(
    relspec_param_dependency_reports: tuple[TaskDependencyReport, ...],
) -> ActiveParamSet:
    """Return the set of active param tables for the run.

    Returns:
    -------
    ActiveParamSet
        Active parameter table logical names.
    """
    active: set[str] = set()
    for report in relspec_param_dependency_reports:
        active.update(report.param_tables)
    return ActiveParamSet(frozenset(active))


def _parse_list_values(name: str, value: object) -> tuple[object, ...]:
    if value is None:
        return ()
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return tuple(value)
    msg = f"Param list {name!r} must be a sequence."
    raise TypeError(msg)


def _artifact_from_delta(spec: ParamTableSpec, path: str) -> ParamTableArtifact:
    table = read_delta_as_reader(path).read_all()
    if table.schema != spec.schema:
        table = table.cast(spec.schema, safe=False)
    if spec.distinct:
        unique = unique_values(table[spec.key_col])
        table = pa.table({spec.key_col: unique}, schema=spec.schema)
    signature = param_signature_from_array(
        logical_name=spec.logical_name,
        values=table[spec.key_col],
    )
    return ParamTableArtifact(
        logical_name=spec.logical_name,
        table=table,
        signature=signature,
        rows=table.num_rows,
        schema_identity_hash=schema_identity_hash(table.schema),
    )

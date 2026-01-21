"""Hamilton output and materialization nodes."""

from __future__ import annotations

import uuid
from collections.abc import Callable, Mapping, Sequence
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from typing import cast

import pyarrow as pa
from hamilton.function_modifiers import tag

from arrowdsl.core.execution_context import ExecutionContext
from arrowdsl.core.interop import RecordBatchReaderLike, TableLike
from arrowdsl.core.metrics import (
    column_stats_table,
    dataset_stats_table,
    empty_scan_telemetry_table,
    scan_telemetry_table,
)
from arrowdsl.core.ordering_policy import require_explicit_ordering
from arrowdsl.core.scan_telemetry import ScanTelemetry, ScanTelemetryOptions, fragment_telemetry
from arrowdsl.finalize.finalize import FinalizeResult
from arrowdsl.schema.build import table_from_rows
from arrowdsl.schema.metadata import ordering_from_schema
from arrowdsl.schema.schema import EncodingPolicy
from arrowdsl.schema.serialization import schema_fingerprint
from arrowdsl.spec.expr_ir import ExprIR
from core_types import JsonDict, JsonValue
from cpg.constants import CpgBuildArtifacts
from datafusion_engine.bridge import validate_table_constraints
from datafusion_engine.registry_bridge import cached_dataset_names, register_dataset_df
from datafusion_engine.runtime import DataFusionRuntimeProfile
from datafusion_engine.schema_registry import is_nested_dataset
from engine.function_registry import default_function_registry
from engine.plan_cache import PlanCacheEntry
from engine.plan_policy import WriterStrategy
from engine.pyarrow_registry import pyarrow_registry_snapshot as kernel_registry_snapshot
from engine.runtime_profile import RuntimeProfileSnapshot, runtime_profile_snapshot
from extract.evidence_plan import EvidencePlan
from hamilton_pipeline.modules.extraction import ExtractErrorArtifacts
from hamilton_pipeline.pipeline_types import (
    CpgOutputTables,
    OutputConfig,
    RelationshipOutputTables,
    RelspecConfig,
    RelspecInputsBundle,
    RelspecSnapshots,
    RepoScanConfig,
)
from ibis_engine.execution import IbisExecutionContext
from ibis_engine.io_bridge import (
    IbisNamedDatasetWriteOptions,
    write_ibis_named_datasets_delta,
)
from ibis_engine.param_tables import ParamTableArtifact, ParamTableSpec
from ibis_engine.params_bridge import IbisParamRegistry, ScalarParamSpec
from ibis_engine.plan import IbisPlan
from ibis_engine.registry import registry_snapshot
from incremental.fingerprint_changes import (
    output_fingerprint_change_table,
    read_dataset_fingerprints,
    write_dataset_fingerprints,
)
from incremental.registry_specs import dataset_schema as incremental_dataset_schema
from incremental.state_store import StateStore
from incremental.types import IncrementalConfig
from normalize.runner import NormalizeRuleCompilation
from normalize.utils import encoding_policy_from_schema
from obs.diagnostics import DiagnosticsCollector
from obs.diagnostics_tables import (
    datafusion_explains_table,
    datafusion_fallbacks_table,
    datafusion_schema_registry_validation_table,
    feature_state_table,
)
from obs.manifest import (
    ManifestContext,
    ManifestData,
    build_manifest,
    relationship_output_fingerprints,
    write_manifest_delta,
)
from obs.repro import RunBundleContext, write_run_bundle
from registry_common.arrow_payloads import ipc_hash
from relspec.compiler import CompiledOutput
from relspec.model import RelationshipRule
from relspec.param_deps import RuleDependencyReport, build_param_reverse_index
from relspec.registry import (
    ContractCatalog,
    DatasetCatalog,
    DatasetLocation,
    build_relspec_snapshot,
)
from relspec.rules.diagnostics import rule_diagnostics_from_table
from relspec.rules.handlers.cpg import relationship_rule_from_definition
from relspec.rules.spec_tables import rule_definitions_from_table
from relspec.runtime import RelspecRuntime
from schema_spec.catalog_registry import dataset_spec as catalog_spec
from schema_spec.system import DatasetSpec, dataset_spec_from_schema, make_dataset_spec
from sqlglot_tools.optimizer import sqlglot_policy_snapshot
from storage.dataset_sources import (
    DatasetDiscoveryOptions,
    DatasetSourceOptions,
    normalize_dataset_source,
)
from storage.deltalake import (
    DeltaWriteOptions,
    DeltaWriteResult,
    apply_delta_write_policies,
    coerce_delta_table,
    delta_table_version,
    read_table_delta,
    write_dataset_delta,
    write_finalize_result_delta,
    write_named_datasets_delta,
    write_table_delta,
)
from storage.deltalake.config import DeltaSchemaPolicy, DeltaWritePolicy

# -----------------------
# Public CPG outputs
# -----------------------


@tag(layer="outputs", artifact="cpg_nodes", kind="table")
def cpg_nodes(cpg_nodes_final: TableLike) -> TableLike:
    """Return the final CPG nodes table.

    Returns
    -------
    TableLike
        Final CPG nodes table.
    """
    return cpg_nodes_final


@tag(layer="outputs", artifact="cpg_edges", kind="table")
def cpg_edges(cpg_edges_final: TableLike) -> TableLike:
    """Return the final CPG edges table.

    Returns
    -------
    TableLike
        Final CPG edges table.
    """
    return cpg_edges_final


@tag(layer="outputs", artifact="cpg_props", kind="table")
def cpg_props(cpg_props_final: TableLike) -> TableLike:
    """Return the final CPG properties table.

    Returns
    -------
    TableLike
        Final CPG properties table.
    """
    return cpg_props_final


@tag(layer="outputs", artifact="cpg_bundle", kind="bundle")
def cpg_bundle(
    cpg_nodes: TableLike,
    cpg_edges: TableLike,
    cpg_props: TableLike,
    cpg_props_json: TableLike | None,
) -> dict[str, TableLike]:
    """Bundle CPG tables into a dictionary.

    Returns
    -------
    dict[str, TableLike]
        Bundle of CPG nodes, edges, and properties.
    """
    bundle = {"cpg_nodes": cpg_nodes, "cpg_edges": cpg_edges, "cpg_props": cpg_props}
    if cpg_props_json is not None:
        bundle["cpg_props_json"] = cpg_props_json
    return bundle


# -----------------------
# Materialization helpers
# -----------------------


def _ensure_dir(path: Path) -> None:
    path.mkdir(exist_ok=True, parents=True)


def _delta_commit_metadata(dataset_name: str, schema: pa.Schema) -> dict[str, str]:
    ordering = ordering_from_schema(schema)
    metadata = {
        "dataset_name": dataset_name,
        "schema_fingerprint": schema_fingerprint(schema),
        "ordering_level": ordering.level.value,
    }
    if ordering.keys:
        metadata["ordering_keys"] = _stable_repr([list(key) for key in ordering.keys])
    return metadata


@dataclass(frozen=True)
class DeltaWriteContext:
    """Resolved Delta policies and options for a dataset write."""

    options: DeltaWriteOptions
    write_policy: DeltaWritePolicy | None
    schema_policy: DeltaSchemaPolicy | None
    constraints: tuple[str, ...]
    storage_options: Mapping[str, str] | None


def _merge_delta_write_policy(
    primary: DeltaWritePolicy | None,
    fallback: DeltaWritePolicy | None,
) -> DeltaWritePolicy | None:
    if primary is None:
        return fallback
    if fallback is None:
        return primary
    target_file_size = primary.target_file_size
    if target_file_size is None:
        target_file_size = fallback.target_file_size
    stats_columns = primary.stats_columns
    if stats_columns is None:
        stats_columns = fallback.stats_columns
    return DeltaWritePolicy(
        target_file_size=target_file_size,
        stats_columns=stats_columns,
    )


def _merge_delta_schema_policy(
    primary: DeltaSchemaPolicy | None,
    fallback: DeltaSchemaPolicy | None,
) -> DeltaSchemaPolicy | None:
    if primary is None:
        return fallback
    if fallback is None:
        return primary
    schema_mode = primary.schema_mode
    if schema_mode is None:
        schema_mode = fallback.schema_mode
    column_mapping_mode = primary.column_mapping_mode
    if column_mapping_mode is None:
        column_mapping_mode = fallback.column_mapping_mode
    return DeltaSchemaPolicy(
        schema_mode=schema_mode,
        column_mapping_mode=column_mapping_mode,
    )


def _default_delta_write_policy(spec: DatasetSpec | None) -> DeltaWritePolicy | None:
    if spec is None:
        return None
    if spec.table_spec.key_fields:
        return DeltaWritePolicy(stats_columns=spec.table_spec.key_fields)
    return None


def _dataset_spec_for_name(name: str) -> DatasetSpec | None:
    if is_nested_dataset(name):
        return None
    try:
        return catalog_spec(name)
    except KeyError:
        return _dataset_spec_fallback(name)


def _dataset_spec_fallback(name: str) -> DatasetSpec | None:
    try:
        return catalog_spec(f"{name}_v1")
    except KeyError:
        return None


def _resolve_delta_write_context(
    dataset_name: str,
    base_options: DeltaWriteOptions,
    *,
    output_config: OutputConfig | None,
    dataset_spec: DatasetSpec | None = None,
) -> DeltaWriteContext:
    spec = dataset_spec or _dataset_spec_for_name(dataset_name)
    base_write_policy = spec.delta_write_policy if spec is not None else None
    base_schema_policy = spec.delta_schema_policy if spec is not None else None
    fallback_write_policy = output_config.delta_write_policy if output_config is not None else None
    fallback_schema_policy = (
        output_config.delta_schema_policy if output_config is not None else None
    )
    write_policy = _merge_delta_write_policy(base_write_policy, fallback_write_policy)
    if write_policy is None:
        write_policy = _default_delta_write_policy(spec)
    schema_policy = _merge_delta_schema_policy(base_schema_policy, fallback_schema_policy)
    options = apply_delta_write_policies(
        base_options,
        write_policy=write_policy,
        schema_policy=schema_policy,
    )
    constraints = spec.delta_constraints if spec is not None else ()
    storage_options = output_config.delta_storage_options if output_config is not None else None
    return DeltaWriteContext(
        options=options,
        write_policy=write_policy,
        schema_policy=schema_policy,
        constraints=constraints,
        storage_options=storage_options,
    )


def _delta_write_policy_payload(policy: DeltaWritePolicy | None) -> JsonDict | None:
    if policy is None:
        return None
    return {
        "target_file_size": policy.target_file_size,
        "stats_columns": list(policy.stats_columns) if policy.stats_columns is not None else None,
    }


def _delta_schema_policy_payload(policy: DeltaSchemaPolicy | None) -> JsonDict | None:
    if policy is None:
        return None
    return {
        "schema_mode": policy.schema_mode,
        "column_mapping_mode": policy.column_mapping_mode,
    }


def _validate_delta_constraints(
    dataset_name: str,
    table: TableLike,
    *,
    constraints: Sequence[str],
    ctx: ExecutionContext,
) -> None:
    if not constraints:
        return
    if not isinstance(table, pa.Table):
        msg = f"Delta constraint validation expected pyarrow.Table, got {type(table)}"
        raise TypeError(msg)
    runtime = ctx.runtime.datafusion
    if runtime is None:
        msg = "Delta constraint validation requires a DataFusion runtime profile."
        raise ValueError(msg)
    temp_name = f"__delta_constraints_{dataset_name}_{uuid.uuid4().hex}"
    violations = validate_table_constraints(
        runtime.session_context(),
        name=temp_name,
        table=table,
        constraints=constraints,
    )
    if not violations:
        return
    sink = runtime.diagnostics_sink
    if sink is not None:
        sink.record_artifact(
            "delta_constraint_violations_v1",
            {
                "dataset": dataset_name,
                "constraints": list(constraints),
                "violations": list(violations),
                "rows": int(table.num_rows),
                "schema_fingerprint": schema_fingerprint(table.schema),
            },
        )
    msg = f"Delta constraint validation failed for {dataset_name!r}: {violations}"
    raise ValueError(msg)


def _datafusion_settings_snapshot(
    ctx: ExecutionContext,
) -> list[dict[str, str]] | None:
    profile = ctx.runtime.datafusion
    if profile is None or not profile.enable_information_schema:
        return None
    session = profile.session_context()
    table = profile.settings_snapshot(session)
    snapshot: list[dict[str, str]] = []
    columns = table.to_pydict()
    names = columns.get("name", [])
    values = columns.get("value", [])
    for name, value in zip(names, values, strict=False):
        if name is None or value is None:
            continue
        snapshot.append({"name": str(name), "value": str(value)})
    return snapshot or None


def _datafusion_settings_hash(ctx: ExecutionContext) -> str | None:
    profile = ctx.runtime.datafusion
    if profile is None:
        return None
    return profile.settings_hash()


def _datafusion_feature_gates(ctx: ExecutionContext) -> dict[str, str] | None:
    profile = ctx.runtime.datafusion
    if profile is None:
        return None
    return dict(profile.feature_gates.settings())


def _datafusion_catalog_snapshot(
    ctx: ExecutionContext,
) -> list[dict[str, str]] | None:
    profile = ctx.runtime.datafusion
    if profile is None or not profile.enable_information_schema:
        return None
    session = profile.session_context()
    table = profile.catalog_snapshot(session)
    snapshot: list[dict[str, str]] = []
    columns = table.to_pydict()
    catalogs = columns.get("table_catalog", [])
    schemas = columns.get("table_schema", [])
    names = columns.get("table_name", [])
    table_types = columns.get("table_type", [])
    for catalog, schema, name, table_type in zip(
        catalogs, schemas, names, table_types, strict=False
    ):
        if catalog is None or schema is None or name is None:
            continue
        snapshot.append(
            {
                "table_catalog": str(catalog),
                "table_schema": str(schema),
                "table_name": str(name),
                "table_type": str(table_type) if table_type is not None else "",
            }
        )
    return snapshot or None


def _datafusion_function_catalog_snapshot(
    ctx: ExecutionContext,
) -> tuple[list[dict[str, object]] | None, str | None]:
    profile = ctx.runtime.datafusion
    if profile is None:
        return None, None
    session = profile.session_context()
    snapshot = profile.function_catalog_snapshot(
        session,
        include_routines=profile.enable_information_schema,
    )
    if not snapshot:
        return None, None
    table = pa.Table.from_pylist(snapshot)
    return snapshot, ipc_hash(table)


def _datafusion_write_policy_snapshot(ctx: ExecutionContext) -> Mapping[str, object] | None:
    profile = ctx.runtime.datafusion
    if profile is None or profile.write_policy is None:
        return None
    return profile.write_policy.payload()


def _json_mapping(payload: Mapping[str, object] | None) -> JsonDict | None:
    if payload is None:
        return None
    return cast("JsonDict", _normalized_payload(payload))


def _json_payload(payload: Mapping[str, object]) -> JsonDict:
    return cast("JsonDict", _normalized_payload(payload))


def _normalized_payload(payload: Mapping[str, object]) -> dict[str, object]:
    return {str(key): _normalize_value(value) for key, value in payload.items()}


def _normalize_value(value: object) -> object:
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, bytes):
        return value.hex()
    if isinstance(value, Mapping):
        return _normalized_payload(value)
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [_normalize_value(item) for item in value]
    return _stable_repr(value)


def _stable_repr(value: object) -> str:
    if isinstance(value, Mapping):
        items = ", ".join(
            f"{_stable_repr(key)}:{_stable_repr(val)}"
            for key, val in sorted(value.items(), key=lambda item: str(item[0]))
        )
        return f"{{{items}}}"
    if isinstance(value, (list, tuple, set)):
        rendered = [_stable_repr(item) for item in value]
        if isinstance(value, set):
            rendered = sorted(rendered)
        items = ", ".join(rendered)
        bracket = "()" if isinstance(value, tuple) else "[]"
        return f"{bracket[0]}{items}{bracket[1]}"
    return repr(value)


def _scan_profile_payload(ctx: ExecutionContext) -> JsonDict:
    profile = ctx.runtime.scan
    return {
        "name": profile.name,
        "scanner_kwargs": _json_payload(profile.scanner_kwargs()),
        "scan_node_kwargs": _json_payload(profile.scan_node_kwargs()),
        "scan_provenance_columns": list(profile.scan_provenance_columns),
        "implicit_ordering": profile.implicit_ordering,
        "require_sequenced_output": profile.require_sequenced_output,
        "parquet_read_options": _json_mapping(profile.parquet_read_payload()),
        "parquet_fragment_scan_options": _json_mapping(profile.parquet_fragment_scan_payload()),
    }


def _datafusion_runtime_artifacts(
    ctx: ExecutionContext,
) -> tuple[JsonDict | None, JsonDict | None]:
    profile = ctx.runtime.datafusion
    if profile is None:
        return None, None
    metrics = _json_mapping(profile.collect_metrics())
    traces = _json_mapping(profile.collect_traces())
    return metrics, traces


def _ibis_sql_ingest_artifacts(
    ctx: ExecutionContext | None,
) -> Sequence[Mapping[str, object]] | None:
    if ctx is None or ctx.runtime.datafusion is None:
        return None
    diagnostics = ctx.runtime.datafusion.diagnostics_sink
    if diagnostics is None:
        return None
    artifacts = diagnostics.artifacts_snapshot().get("ibis_sql_ingest_v1", [])
    return artifacts or None


def _ibis_namespace_actions(
    ctx: ExecutionContext | None,
) -> Sequence[Mapping[str, object]] | None:
    if ctx is None or ctx.runtime.datafusion is None:
        return None
    diagnostics = ctx.runtime.datafusion.diagnostics_sink
    if diagnostics is None:
        return None
    actions = diagnostics.artifacts_snapshot().get("ibis_namespace_actions_v1", [])
    return actions or None


def _ibis_cache_events(
    ctx: ExecutionContext | None,
) -> Sequence[Mapping[str, object]] | None:
    if ctx is None or ctx.runtime.datafusion is None:
        return None
    diagnostics = ctx.runtime.datafusion.diagnostics_sink
    if diagnostics is None:
        return None
    events = diagnostics.events_snapshot().get("ibis_cache_events_v1", [])
    return events or None


def _datafusion_cache_events(
    ctx: ExecutionContext | None,
) -> Sequence[Mapping[str, object]] | None:
    if ctx is None or ctx.runtime.datafusion is None:
        return None
    diagnostics = ctx.runtime.datafusion.diagnostics_sink
    if diagnostics is None:
        return None
    events = diagnostics.events_snapshot().get("datafusion_cache_events_v1", [])
    return events or None


def _ibis_support_matrix(
    ctx: ExecutionContext | None,
) -> Sequence[Mapping[str, object]] | None:
    if ctx is None or ctx.runtime.datafusion is None:
        return None
    diagnostics = ctx.runtime.datafusion.diagnostics_sink
    if diagnostics is None:
        return None
    entries = diagnostics.artifacts_snapshot().get("ibis_support_matrix_v1", [])
    return entries or None


def _datafusion_runtime_diag_tables(
    ctx: ExecutionContext,
    *,
    output_dir: str | None,
    work_dir: str | None,
) -> tuple[pa.Table | None, pa.Table | None]:
    profile = ctx.runtime.datafusion
    if profile is None:
        return None, None
    fallback_events: list[dict[str, object]] = []
    explain_events: list[dict[str, object]] = []
    if profile.fallback_collector is not None:
        fallback_events = profile.fallback_collector.snapshot()
    artifact_root = _diagnostics_artifact_root(output_dir, work_dir)
    if profile.explain_collector is not None:
        for idx, entry in enumerate(profile.explain_collector.snapshot()):
            payload = dict(entry)
            name = f"explain_{idx}_{uuid.uuid4().hex}"
            payload["rows"] = _diagnostics_rows_payload(
                payload.get("rows"),
                spec=DiagnosticsPayloadSpec(
                    root=artifact_root,
                    group="datafusion_explains",
                    name=name,
                    suffix="rows",
                    runtime_profile=profile,
                ),
            )
            payload["explain_analyze"] = profile.explain_analyze
            explain_events.append(payload)
    fallback_table = (
        datafusion_fallbacks_table(fallback_events)
        if profile.fallback_collector is not None
        else None
    )
    explain_table = (
        datafusion_explains_table(explain_events) if profile.explain_collector is not None else None
    )
    return fallback_table, explain_table


def _datafusion_input_plugins(
    ctx: ExecutionContext,
) -> Sequence[Mapping[str, object]] | None:
    profile = ctx.runtime.datafusion
    if profile is None or profile.diagnostics_sink is None:
        return None
    plugins = profile.diagnostics_sink.artifacts_snapshot().get(
        "datafusion_input_plugins_v1",
        [],
    )
    return plugins or None


def _datafusion_arrow_ingest(
    ctx: ExecutionContext,
) -> Sequence[Mapping[str, object]] | None:
    profile = ctx.runtime.datafusion
    if profile is None or profile.diagnostics_sink is None:
        return None
    payloads = profile.diagnostics_sink.artifacts_snapshot().get(
        "datafusion_arrow_ingest_v1",
        [],
    )
    return payloads or None


def _datafusion_prepared_statements(
    ctx: ExecutionContext,
) -> Sequence[Mapping[str, object]] | None:
    profile = ctx.runtime.datafusion
    if profile is None or profile.diagnostics_sink is None:
        return None
    statements = profile.diagnostics_sink.artifacts_snapshot().get(
        "datafusion_prepared_statements_v1",
        [],
    )
    return statements or None


def _datafusion_dml_statements(
    ctx: ExecutionContext,
) -> Sequence[Mapping[str, object]] | None:
    profile = ctx.runtime.datafusion
    if profile is None or profile.diagnostics_sink is None:
        return None
    statements = profile.diagnostics_sink.artifacts_snapshot().get(
        "datafusion_dml_statements_v1",
        [],
    )
    return statements or None


def _datafusion_function_factory(
    ctx: ExecutionContext,
) -> Sequence[Mapping[str, object]] | None:
    profile = ctx.runtime.datafusion
    if profile is None or profile.diagnostics_sink is None:
        return None
    entries = profile.diagnostics_sink.artifacts_snapshot().get(
        "datafusion_function_factory_v1",
        [],
    )
    return entries or None


def _datafusion_expr_planners(
    ctx: ExecutionContext,
) -> Sequence[Mapping[str, object]] | None:
    profile = ctx.runtime.datafusion
    if profile is None or profile.diagnostics_sink is None:
        return None
    entries = profile.diagnostics_sink.artifacts_snapshot().get(
        "datafusion_expr_planners_v1",
        [],
    )
    return entries or None


def _datafusion_listing_tables(
    ctx: ExecutionContext,
) -> Sequence[Mapping[str, object]] | None:
    profile = ctx.runtime.datafusion
    if profile is None or profile.diagnostics_sink is None:
        return None
    tables = profile.diagnostics_sink.artifacts_snapshot().get(
        "datafusion_listing_tables_v1",
        [],
    )
    return tables or None


def _datafusion_listing_refresh_events(
    ctx: ExecutionContext,
) -> Sequence[Mapping[str, object]] | None:
    profile = ctx.runtime.datafusion
    if profile is None or profile.diagnostics_sink is None:
        return None
    events = profile.diagnostics_sink.artifacts_snapshot().get(
        "datafusion_listing_refresh_v1",
        [],
    )
    return events or None


def _datafusion_delta_tables(
    ctx: ExecutionContext,
) -> Sequence[Mapping[str, object]] | None:
    profile = ctx.runtime.datafusion
    if profile is None or profile.diagnostics_sink is None:
        return None
    tables = profile.diagnostics_sink.artifacts_snapshot().get(
        "datafusion_delta_tables_v1",
        [],
    )
    return tables or None


def _delta_maintenance_reports(
    ctx: ExecutionContext,
) -> Sequence[Mapping[str, object]] | None:
    profile = ctx.runtime.datafusion
    if profile is None or profile.diagnostics_sink is None:
        return None
    reports = profile.diagnostics_sink.artifacts_snapshot().get(
        "delta_maintenance_v1",
        [],
    )
    return reports or None


def _datafusion_udf_registry(
    ctx: ExecutionContext,
) -> Sequence[Mapping[str, object]] | None:
    profile = ctx.runtime.datafusion
    if profile is None or profile.diagnostics_sink is None:
        return None
    registry = profile.diagnostics_sink.artifacts_snapshot().get(
        "datafusion_udf_registry_v1",
        [],
    )
    return registry or None


def _datafusion_schema_registry_validation(
    ctx: ExecutionContext,
) -> pa.Table | None:
    profile = ctx.runtime.datafusion
    if profile is None or profile.diagnostics_sink is None:
        return None
    entries = profile.diagnostics_sink.artifacts_snapshot().get(
        "datafusion_schema_registry_validation_v1",
        [],
    )
    if not entries:
        return None
    return datafusion_schema_registry_validation_table(entries)


def _datafusion_table_providers(
    ctx: ExecutionContext,
) -> Sequence[Mapping[str, object]] | None:
    profile = ctx.runtime.datafusion
    if profile is None or profile.diagnostics_sink is None:
        return None
    providers = profile.diagnostics_sink.artifacts_snapshot().get(
        "datafusion_table_providers_v1",
        [],
    )
    return providers or None


def _datafusion_view_registry(
    ctx: ExecutionContext,
) -> Sequence[Mapping[str, object]] | None:
    profile = ctx.runtime.datafusion
    if profile is None:
        return None
    snapshot = profile.view_registry_snapshot()
    return snapshot or None


def _datafusion_plan_artifacts_table(
    ctx: ExecutionContext,
    *,
    output_dir: str | None,
    work_dir: str | None,
) -> pa.Table | None:
    profile = ctx.runtime.datafusion
    if profile is None or profile.plan_collector is None:
        return None
    entries = profile.plan_collector.snapshot()
    if not entries:
        return None
    artifact_root = _diagnostics_artifact_root(output_dir, work_dir)
    rows = [
        _datafusion_plan_row(
            entry,
            artifact_root=artifact_root,
            runtime_profile=profile,
        )
        for entry in entries
    ]
    schema = incremental_dataset_schema("datafusion_plan_artifacts_v1")
    return table_from_rows(schema, rows)


def _datafusion_plan_cache_entries(
    ctx: ExecutionContext,
) -> Sequence[PlanCacheEntry] | None:
    profile = ctx.runtime.datafusion
    if profile is None or profile.plan_cache is None:
        return None
    return profile.plan_cache.snapshot()


@tag(layer="obs", artifact="feature_state_table", kind="table")
def feature_state_diagnostics_table(
    diagnostics_collector: DiagnosticsCollector,
) -> pa.Table | None:
    """Return feature state diagnostics table when available.

    Returns
    -------
    pa.Table | None
        Feature state table or ``None`` when no events exist.
    """
    events = diagnostics_collector.events_snapshot().get("feature_state_v1", [])
    if not events:
        return None
    return feature_state_table(events)


def _dataset_registry_snapshot(
    locations: Mapping[str, DatasetLocation],
) -> list[dict[str, object]] | None:
    if not locations:
        return None
    catalog = DatasetCatalog()
    for name, location in locations.items():
        catalog.register(name, location)
    return registry_snapshot(catalog) or None


def _default_debug_dir(output_dir: str | None, work_dir: str | None) -> Path | None:
    base = output_dir or work_dir
    if not base:
        return None
    debug_dir = Path(base) / "debug"
    _ensure_dir(debug_dir)
    return debug_dir


def _diagnostics_artifact_root(output_dir: str | None, work_dir: str | None) -> Path | None:
    debug_dir = _default_debug_dir(output_dir, work_dir)
    if debug_dir is None:
        return None
    root = debug_dir / "diagnostics"
    _ensure_dir(root)
    return root


def _diagnostics_artifact_path(
    root: Path,
    *,
    group: str,
    name: str,
    suffix: str,
) -> Path:
    base = root / group / name
    _ensure_dir(base)
    return base / f"{suffix}.delta"


def _artifact_table_name(group: str, name: str, suffix: str) -> str:
    parts = [group, name, suffix]
    return "__".join(part.replace("/", "_") for part in parts if part)


def _register_delta_artifact(
    runtime_profile: DataFusionRuntimeProfile | None,
    *,
    name: str,
    path: Path,
    storage_options: Mapping[str, str] | None,
) -> None:
    if runtime_profile is None:
        return
    location = DatasetLocation(
        path=str(path),
        format="delta",
        storage_options=dict(storage_options) if storage_options else {},
    )
    with suppress(ValueError, KeyError):
        register_dataset_df(
            runtime_profile.session_context(),
            name=name,
            location=location,
            runtime_profile=runtime_profile,
        )


def _artifact_fields(payload: object | None) -> tuple[str | None, str | None, str | None]:
    if isinstance(payload, Mapping):
        return (
            _optional_str(payload.get("artifact_path")),
            _optional_str(payload.get("artifact_format")),
            _optional_str(payload.get("schema_fingerprint")),
        )
    return None, None, None


def _optional_str(value: object | None) -> str | None:
    if value is None:
        return None
    return str(value)


@dataclass(frozen=True)
class DiagnosticsPayloadSpec:
    """Diagnostics payload configuration for row artifacts."""

    root: Path | None
    group: str
    name: str
    suffix: str
    runtime_profile: DataFusionRuntimeProfile | None = None
    storage_options: Mapping[str, str] | None = None


def _diagnostics_rows_payload(rows: object, *, spec: DiagnosticsPayloadSpec) -> object | None:
    if rows is None:
        return None
    if isinstance(rows, (RecordBatchReaderLike, TableLike)):
        schema = rows.schema
        payload: dict[str, object] = {
            "artifact_path": None,
            "artifact_format": "delta",
            "schema_fingerprint": schema_fingerprint(schema),
        }
        if spec.root is None:
            return payload
        path = _diagnostics_artifact_path(
            spec.root,
            group=spec.group,
            name=spec.name,
            suffix=spec.suffix,
        )
        delta_options = DeltaWriteOptions(mode="overwrite", schema_mode="overwrite")
        result = write_dataset_delta(
            rows,
            str(path),
            options=delta_options,
            storage_options=spec.storage_options,
        )
        payload["artifact_path"] = result.path
        _register_delta_artifact(
            spec.runtime_profile,
            name=_artifact_table_name(spec.group, spec.name, spec.suffix),
            path=Path(result.path),
            storage_options=spec.storage_options,
        )
        return payload
    return rows


def _substrait_validation_fields(payload: object | None) -> dict[str, object]:
    fields: dict[str, object] = {
        "status": None,
        "stage": None,
        "error": None,
        "match": None,
        "datafusion_rows": None,
        "datafusion_hash": None,
        "substrait_rows": None,
        "substrait_hash": None,
    }
    if not isinstance(payload, Mapping):
        return fields
    status = payload.get("status")
    stage = payload.get("stage")
    error = payload.get("error")
    match = payload.get("match")
    datafusion_rows = payload.get("datafusion_rows")
    datafusion_hash = payload.get("datafusion_hash")
    substrait_rows = payload.get("substrait_rows")
    substrait_hash = payload.get("substrait_hash")
    fields["status"] = str(status) if status is not None else None
    fields["stage"] = str(stage) if stage is not None else None
    fields["error"] = str(error) if error is not None else None
    fields["match"] = match if isinstance(match, bool) else None
    fields["datafusion_rows"] = int(datafusion_rows) if isinstance(datafusion_rows, int) else None
    fields["datafusion_hash"] = str(datafusion_hash) if datafusion_hash is not None else None
    fields["substrait_rows"] = int(substrait_rows) if isinstance(substrait_rows, int) else None
    fields["substrait_hash"] = str(substrait_hash) if substrait_hash is not None else None
    return fields


def _datafusion_plan_row(
    entry: Mapping[str, object],
    *,
    artifact_root: Path | None,
    runtime_profile: DataFusionRuntimeProfile | None = None,
) -> dict[str, object]:
    plan_hash = str(entry.get("plan_hash") or "")
    plan_key = plan_hash or uuid.uuid4().hex
    explain_payload = _diagnostics_rows_payload(
        entry.get("explain"),
        spec=DiagnosticsPayloadSpec(
            root=artifact_root,
            group="datafusion_plan_artifacts_v1",
            name=plan_key,
            suffix="explain",
            runtime_profile=runtime_profile,
        ),
    )
    explain_analyze_payload = _explain_analyze_payload(
        entry,
        artifact_root=artifact_root,
        plan_key=plan_key,
        runtime_profile=runtime_profile,
    )
    validation_fields = _substrait_validation_fields(entry.get("substrait_validation"))
    explain_path, explain_format, explain_schema_fp = _artifact_fields(explain_payload)
    (
        explain_analyze_path,
        explain_analyze_format,
        explain_analyze_schema_fp,
    ) = _artifact_fields(explain_analyze_payload)
    return {
        "plan_hash": plan_hash,
        "sql": str(entry.get("sql") or ""),
        "explain_artifact_path": explain_path,
        "explain_artifact_format": explain_format,
        "explain_schema_fingerprint": explain_schema_fp,
        "explain_analyze_artifact_path": explain_analyze_path,
        "explain_analyze_artifact_format": explain_analyze_format,
        "explain_analyze_schema_fingerprint": explain_analyze_schema_fp,
        "substrait_b64": str(entry.get("substrait_b64") or ""),
        "substrait_validation_status": validation_fields["status"],
        "substrait_validation_stage": validation_fields["stage"],
        "substrait_validation_error": validation_fields["error"],
        "substrait_validation_match": validation_fields["match"],
        "substrait_validation_datafusion_rows": validation_fields["datafusion_rows"],
        "substrait_validation_datafusion_hash": validation_fields["datafusion_hash"],
        "substrait_validation_substrait_rows": validation_fields["substrait_rows"],
        "substrait_validation_substrait_hash": validation_fields["substrait_hash"],
        "unparsed_sql": entry.get("unparsed_sql"),
        "unparse_error": entry.get("unparse_error"),
        "logical_plan": entry.get("logical_plan"),
        "optimized_plan": entry.get("optimized_plan"),
        "physical_plan": entry.get("physical_plan"),
        "graphviz": entry.get("graphviz"),
        "partition_count": entry.get("partition_count"),
    }


def _explain_analyze_payload(
    entry: Mapping[str, object],
    *,
    artifact_root: Path | None,
    plan_key: str,
    runtime_profile: DataFusionRuntimeProfile | None = None,
) -> object | None:
    explain_analyze = entry.get("explain_analyze")
    if explain_analyze is None:
        return None
    return _diagnostics_rows_payload(
        explain_analyze,
        spec=DiagnosticsPayloadSpec(
            root=artifact_root,
            group="datafusion_plan_artifacts_v1",
            name=plan_key,
            suffix="explain_analyze",
            runtime_profile=runtime_profile,
        ),
    )


def _incremental_output_dir(output_dir: str | None, work_dir: str | None) -> Path | None:
    base = output_dir or work_dir
    if not base:
        return None
    incremental_dir = Path(base) / "incremental"
    _ensure_dir(incremental_dir)
    return incremental_dir


@dataclass(frozen=True)
class IncrementalWriteContext:
    """Context for incremental Delta writes."""

    output_dir: str | None
    work_dir: str | None
    incremental_config: IncrementalConfig
    output_config: OutputConfig | None = None


@dataclass(frozen=True)
class CpgDeltaWriteContext:
    """Context for CPG Delta materialization."""

    output_dir: str | None
    output_config: OutputConfig
    incremental_config: IncrementalConfig
    ctx: ExecutionContext


@tag(layer="materialize", artifact="cpg_delta_write_context", kind="object")
def cpg_delta_write_context(
    output_dir: str | None,
    output_config: OutputConfig,
    incremental_config: IncrementalConfig,
    ctx: ExecutionContext,
) -> CpgDeltaWriteContext:
    """Bundle CPG Delta materialization inputs.

    Returns
    -------
    CpgDeltaWriteContext
        Context for CPG Delta materialization.
    """
    return CpgDeltaWriteContext(
        output_dir=output_dir,
        output_config=output_config,
        incremental_config=incremental_config,
        ctx=ctx,
    )


def _write_incremental_dataset(
    *,
    name: str,
    table: pa.Table | None,
    context: IncrementalWriteContext,
) -> str | None:
    if not context.incremental_config.enabled or table is None:
        return None
    base = _incremental_output_dir(context.output_dir, context.work_dir)
    if base is None:
        return None
    schema = incremental_dataset_schema(name)
    data = coerce_delta_table(
        table,
        schema=schema,
        encoding_policy=encoding_policy_from_schema(schema),
    )
    delta_context = _resolve_delta_write_context(
        name,
        DeltaWriteOptions(
            mode="overwrite",
            schema_mode="overwrite",
            commit_metadata=_delta_commit_metadata(name, schema),
        ),
        output_config=context.output_config,
    )
    result = write_dataset_delta(
        data,
        str(base / name),
        options=delta_context.options,
        storage_options=delta_context.storage_options,
    )
    return result.path


def _output_fingerprint_map(run_manifest: Mapping[str, JsonValue]) -> dict[str, str]:
    outputs = run_manifest.get("outputs")
    if not isinstance(outputs, Sequence) or isinstance(outputs, (str, bytes)):
        return {}
    fingerprints: dict[str, str] = {}
    for record in outputs:
        if not isinstance(record, Mapping):
            continue
        name = record.get("name")
        fingerprint = record.get("dataset_fingerprint")
        if isinstance(name, str) and isinstance(fingerprint, str) and fingerprint:
            fingerprints[name] = fingerprint
    return fingerprints


# -----------------------
# Incremental fingerprint diagnostics
# -----------------------


@tag(layer="incremental", kind="table")
def incremental_output_fingerprint_changes(
    run_manifest: JsonDict,
    incremental_relationship_updates: Mapping[str, str] | None,
    incremental_state_store: StateStore | None,
    incremental_config: IncrementalConfig,
) -> pa.Table | None:
    """Return output fingerprint change diagnostics for incremental runs.

    Returns
    -------
    pa.Table | None
        Change table aligned to ``inc_output_fingerprint_changes_v1``.
    """
    _ = incremental_relationship_updates
    if not incremental_config.enabled or incremental_state_store is None:
        return None
    current = _output_fingerprint_map(run_manifest)
    previous = read_dataset_fingerprints(incremental_state_store)
    table = output_fingerprint_change_table(previous, current)
    write_dataset_fingerprints(incremental_state_store, current)
    return table


# -----------------------
# Bundle helper types
# -----------------------


@dataclass(frozen=True)
class RunBundleTables:
    """Table bundles needed to write run bundles."""

    relspec_inputs: RelspecInputsBundle
    relationship_outputs: RelationshipOutputTables
    cpg_outputs: CpgOutputTables


@dataclass(frozen=True)
class RunBundleMetadata:
    """Manifest and config bundle for run bundles."""

    run_manifest: JsonDict
    run_config: JsonDict


@dataclass(frozen=True)
class RunBundleIncrementalTables:
    """Incremental diagnostic tables for run bundles."""

    incremental_diff: pa.Table | None
    incremental_plan_diff: pa.Table | None
    incremental_changed_exports: pa.Table | None
    incremental_impacted_callers: pa.Table | None
    incremental_impacted_importers: pa.Table | None
    incremental_impacted_files: pa.Table | None
    incremental_output_fingerprint_changes: pa.Table | None


@dataclass(frozen=True)
class RunBundleIncrementalInputs:
    """Inputs required to bundle incremental diagnostics."""

    incremental_diff: pa.Table | None
    incremental_plan_diff: pa.Table | None
    incremental_changed_exports: pa.Table | None
    incremental_impacted_callers: pa.Table | None
    incremental_impacted_importers: pa.Table | None
    incremental_impacted_files: pa.Table | None
    incremental_output_fingerprint_changes: pa.Table | None


@dataclass(frozen=True)
class RunBundleIncrementalTablesBundle:
    """Bundle of incremental tables for easier wiring."""

    incremental_diff: pa.Table | None
    incremental_plan_diff: pa.Table | None
    incremental_changed_exports: pa.Table | None
    incremental_impacted_callers: pa.Table | None
    incremental_impacted_importers: pa.Table | None
    incremental_impacted_files: pa.Table | None
    incremental_output_fingerprint_changes: pa.Table | None


@dataclass(frozen=True)
class RunBundleInputs:
    """Inputs required to build a run bundle context."""

    run_manifest: JsonDict
    run_config: JsonDict
    relspec_snapshots: RelspecSnapshots
    tables: RunBundleTables
    param_inputs: RunBundleParamInputs | None = None
    incremental_diff: pa.Table | None = None
    incremental_plan_diff: pa.Table | None = None
    incremental_changed_exports: pa.Table | None = None
    incremental_impacted_callers: pa.Table | None = None
    incremental_impacted_importers: pa.Table | None = None
    incremental_impacted_files: pa.Table | None = None
    incremental_output_fingerprint_changes: pa.Table | None = None


@dataclass(frozen=True)
class RunBundleParamInputs:
    """Parameter-table inputs for run bundle materialization."""

    param_table_specs: tuple[ParamTableSpec, ...]
    param_scalar_specs: tuple[ScalarParamSpec, ...]
    param_table_artifacts: Mapping[str, ParamTableArtifact] | None
    param_scalar_signature: str | None = None
    param_dependency_reports: tuple[RuleDependencyReport, ...] = ()
    param_reverse_index: Mapping[str, tuple[str, ...]] | None = None
    include_param_table_data: bool = False


@dataclass(frozen=True)
class RunBundleParamInputsContext:
    """Context inputs used to build run bundle param inputs."""

    param_table_specs: tuple[ParamTableSpec, ...]
    relspec_param_registry: IbisParamRegistry
    param_table_artifacts: Mapping[str, ParamTableArtifact] | None
    param_scalar_signature: str | None
    relspec_param_dependency_reports: tuple[RuleDependencyReport, ...]


@dataclass(frozen=True)
class RunBundleContextInputs:
    """Inputs required to build a run bundle context."""

    output_config: OutputConfig
    relspec_rule_exec_events: pa.Table | None
    relspec_scan_telemetry: TableLike
    feature_state_diagnostics_table: pa.Table | None
    ctx: ExecutionContext | None = None


@dataclass(frozen=True)
class ManifestInputs:
    """Inputs required to build manifest data."""

    relspec_inputs_bundle: RelspecInputsBundle
    relationship_output_tables: RelationshipOutputTables
    cpg_output_tables: CpgOutputTables
    relspec_snapshots: RelspecSnapshots
    param_table_artifacts: Mapping[str, ParamTableArtifact] | None = None
    param_scalar_signature: str | None = None
    extract_inputs: ExtractManifestInputs | None = None
    materialization_reports: JsonDict | None = None


@dataclass(frozen=True)
class ManifestInputTables:
    """Table bundles required to build manifest inputs."""

    relspec_inputs_bundle: RelspecInputsBundle
    relationship_output_tables: RelationshipOutputTables
    cpg_output_tables: CpgOutputTables


@dataclass(frozen=True)
class ManifestInputArtifacts:
    """Artifacts required to build manifest inputs."""

    relspec_snapshots: RelspecSnapshots
    param_table_artifacts: Mapping[str, ParamTableArtifact] | None = None
    param_scalar_signature: str | None = None
    extract_inputs: ExtractManifestInputs | None = None
    materialization_reports: JsonDict | None = None


@dataclass(frozen=True)
class ExtractManifestInputs:
    """Extract-specific inputs for manifest data."""

    evidence_plan: EvidencePlan | None = None
    extract_error_counts: Mapping[str, int] | None = None


@dataclass(frozen=True)
class CpgPropsReportInputs:
    """Artifacts for CPG props materialization."""

    cpg_props_delta: JsonDict | None = None
    cpg_props_json_delta: JsonDict | None = None


@dataclass(frozen=True)
class MaterializationReportInputs:
    """Inputs required to assemble materialization reports."""

    normalized_inputs_delta: JsonDict | None = None
    param_table_delta: Mapping[str, JsonDict] | None = None
    cpg_nodes_delta: JsonDict | None = None
    cpg_edges_delta: JsonDict | None = None
    cpg_props: CpgPropsReportInputs | None = None


@dataclass(frozen=True)
class NormalizedInputsWriteContext:
    """Bundle configuration for normalized input writes."""

    schemas: Mapping[str, pa.Schema]
    encoding_policies: Mapping[str, EncodingPolicy]
    file_lists: dict[str, list[str]]


@dataclass(frozen=True)
class NormalizedInputsReportContext:
    """Context required to build normalized input reports."""

    schemas: Mapping[str, pa.Schema]
    file_lists: Mapping[str, list[str]]
    write_results: Mapping[str, DeltaWriteResult]
    ibis_execution: IbisExecutionContext
    out_dir: Path
    requested_strategy: WriterStrategy
    effective_strategy: WriterStrategy


@dataclass(frozen=True)
class NormalizedInputsWriteOptions:
    """Execution options for normalized input writes."""

    context: NormalizedInputsWriteContext
    ibis_execution: IbisExecutionContext
    writer_strategy: WriterStrategy
    output_config: OutputConfig
    reporter: Callable[[DeltaWriteResult], None] | None


@dataclass(frozen=True)
class ManifestOptionalInputs:
    """Optional manifest inputs bundled for simpler node signatures."""

    param_table_artifacts: Mapping[str, ParamTableArtifact] | None = None
    param_scalar_signature: str | None = None
    extract_manifest_inputs: ExtractManifestInputs | None = None
    materialization_reports: JsonDict | None = None


# -----------------------
# Materializers: normalized datasets (relationship inputs)
# -----------------------


def _build_normalized_inputs_write_context(
    input_datasets: Mapping[str, TableLike | RecordBatchReaderLike | IbisPlan],
) -> NormalizedInputsWriteContext:
    schemas: dict[str, pa.Schema] = {}
    encoding_policies: dict[str, EncodingPolicy] = {}
    file_lists: dict[str, list[str]] = {}
    for name, table in input_datasets.items():
        file_lists[name] = []
        if isinstance(table, RecordBatchReaderLike):
            continue
        schema = table.expr.schema().to_pyarrow() if isinstance(table, IbisPlan) else table.schema
        schemas[name] = schema
        encoding_policies[name] = encoding_policy_from_schema(schema)
    return NormalizedInputsWriteContext(
        schemas=schemas,
        encoding_policies=encoding_policies,
        file_lists=file_lists,
    )


def _write_normalized_inputs(
    input_datasets: Mapping[str, TableLike | RecordBatchReaderLike | IbisPlan],
    out_dir: Path,
    *,
    options: NormalizedInputsWriteOptions,
) -> dict[str, DeltaWriteResult]:
    delta_options = apply_delta_write_policies(
        DeltaWriteOptions(mode="overwrite", schema_mode="overwrite"),
        write_policy=options.output_config.delta_write_policy,
        schema_policy=options.output_config.delta_schema_policy,
    )
    storage_options = options.output_config.delta_storage_options
    if any(isinstance(table, IbisPlan) for table in input_datasets.values()):
        return write_ibis_named_datasets_delta(
            input_datasets,
            str(out_dir),
            options=IbisNamedDatasetWriteOptions(
                execution=options.ibis_execution,
                writer_strategy=options.writer_strategy,
                delta_reporter=options.reporter,
                delta_options=delta_options,
                delta_write_policy=options.output_config.delta_write_policy,
                delta_schema_policy=options.output_config.delta_schema_policy,
                storage_options=storage_options,
            ),
        )
    converted: dict[str, TableLike | RecordBatchReaderLike] = {}
    for name, table in input_datasets.items():
        if isinstance(table, IbisPlan):
            msg = "Normalized input delta writes do not accept Ibis plans in Arrow mode."
            raise TypeError(msg)
        schema = options.context.schemas.get(name)
        policy = options.context.encoding_policies.get(name)
        converted[name] = coerce_delta_table(table, schema=schema, encoding_policy=policy)
    results = write_named_datasets_delta(
        converted,
        str(out_dir),
        options=delta_options,
        storage_options=storage_options,
    )
    if options.reporter is not None:
        for result in results.values():
            options.reporter(result)
    return results


def _normalized_input_row_count(path: str | None) -> int | None:
    if not path:
        return None
    table = read_table_delta(path)
    return int(table.num_rows)


def _normalized_input_metrics(
    table: TableLike | RecordBatchReaderLike | IbisPlan,
    *,
    path: str | None,
) -> tuple[int | None, int]:
    if isinstance(table, RecordBatchReaderLike):
        return _normalized_input_row_count(path), len(table.schema.names)
    if isinstance(table, IbisPlan):
        schema = table.expr.schema().to_pyarrow()
        return _normalized_input_row_count(path), len(schema.names)
    return int(table.num_rows), len(table.column_names)


def _normalized_input_entry(
    name: str,
    table: TableLike | RecordBatchReaderLike | IbisPlan,
    *,
    path: str | None,
    context: NormalizedInputsReportContext,
) -> JsonDict:
    rows, columns = _normalized_input_metrics(table, path=path)
    entry: JsonDict = {
        "path": path,
        "rows": rows,
        "columns": columns,
        "files": list(context.file_lists.get(name, ())),
    }
    schema = context.schemas.get(name)
    if schema is not None:
        ordering = ordering_from_schema(schema)
        entry["ordering_level"] = ordering.level.value
        entry["ordering_keys"] = [list(key) for key in ordering.keys] if ordering.keys else None
    result = context.write_results.get(name)
    if result is not None:
        entry["delta_version"] = result.version
    return entry


def _normalized_inputs_report(
    input_datasets: Mapping[str, TableLike | RecordBatchReaderLike | IbisPlan],
    results: Mapping[str, DeltaWriteResult],
    *,
    context: NormalizedInputsReportContext,
) -> JsonDict:
    datasets: dict[str, JsonDict] = {}
    for name, table in input_datasets.items():
        result = results.get(name)
        datasets[name] = _normalized_input_entry(
            name,
            table,
            path=result.path if result is not None else None,
            context=context,
        )
    return {
        "base_dir": str(context.out_dir),
        "datasets": datasets,
        "writer_strategy_requested": context.requested_strategy,
        "writer_strategy_effective": context.effective_strategy,
    }


@tag(layer="materialize", artifact="normalized_inputs_delta", kind="side_effect")
def write_normalized_inputs_delta(
    relspec_input_datasets: dict[str, TableLike],
    output_dir: str | None,
    work_dir: str | None,
    ibis_execution: IbisExecutionContext,
    output_config: OutputConfig,
) -> JsonDict | None:
    """Write relationship-input normalized datasets for debugging.

    Output structure:
      <base>/debug/normalized_inputs/<dataset_name>

    Request this node explicitly when you want to inspect intermediate tables.

    Returns
    -------
    JsonDict | None
        Report of written datasets, or None when output is disabled.
    """
    input_datasets = cast(
        "dict[str, TableLike | RecordBatchReaderLike | IbisPlan]",
        relspec_input_datasets,
    )
    base = _default_debug_dir(output_dir, work_dir)
    if not base:
        return None

    out_dir = base / "normalized_inputs"
    _ensure_dir(out_dir)

    write_context = _build_normalized_inputs_write_context(input_datasets)
    requested_strategy = output_config.writer_strategy
    effective_strategy = requested_strategy if requested_strategy == "arrow" else "arrow"
    results = _write_normalized_inputs(
        input_datasets,
        out_dir,
        options=NormalizedInputsWriteOptions(
            context=write_context,
            ibis_execution=ibis_execution,
            writer_strategy=effective_strategy,
            output_config=output_config,
            reporter=None,
        ),
    )
    report_context = NormalizedInputsReportContext(
        schemas=write_context.schemas,
        file_lists=write_context.file_lists,
        write_results=results,
        ibis_execution=ibis_execution,
        out_dir=out_dir,
        requested_strategy=requested_strategy,
        effective_strategy=effective_strategy,
    )
    return _normalized_inputs_report(
        input_datasets,
        results,
        context=report_context,
    )


# -----------------------
# Materializers: final CPG tables (Delta tables)
# -----------------------


def _override_finalize_good(
    finalize: FinalizeResult,
    *,
    good: TableLike | None,
) -> FinalizeResult:
    if good is None:
        return finalize
    return FinalizeResult(
        good=good,
        errors=finalize.errors,
        stats=finalize.stats,
        alignment=finalize.alignment,
    )


@tag(layer="materialize", artifact="cpg_nodes_delta", kind="side_effect")
def write_cpg_nodes_delta(
    context: CpgDeltaWriteContext,
    cpg_nodes_finalize: CpgBuildArtifacts,
    cpg_nodes_final: TableLike,
) -> JsonDict | None:
    """Write CPG nodes and error artifacts to Delta.

    Returns
    -------
    JsonDict | None
        Report of the written files, or None when output is disabled.
    """
    if not context.output_dir:
        return None
    output_path = Path(context.output_dir)
    _ensure_dir(output_path)
    good_override = cpg_nodes_final if context.incremental_config.enabled else None
    finalize = _override_finalize_good(cpg_nodes_finalize.finalize, good=good_override)
    require_explicit_ordering(finalize.good.schema, label="cpg_nodes_final")
    delta_context = _resolve_delta_write_context(
        "cpg_nodes",
        DeltaWriteOptions(
            mode="overwrite",
            schema_mode="overwrite",
            commit_metadata=_delta_commit_metadata("cpg_nodes", finalize.good.schema),
        ),
        output_config=context.output_config,
    )
    _validate_delta_constraints(
        "cpg_nodes",
        finalize.good,
        constraints=delta_context.constraints,
        ctx=context.ctx,
    )
    paths = write_finalize_result_delta(
        finalize,
        str(output_path / "cpg_nodes"),
        options=delta_context.options,
        storage_options=delta_context.storage_options,
    )
    versions = {key: delta_table_version(path) for key, path in paths.items()}
    return {
        "paths": paths,
        "files": list(paths.values()),
        "rows": int(finalize.good.num_rows),
        "error_rows": int(finalize.errors.num_rows),
        "delta_versions": versions,
        "delta_write_policy": _delta_write_policy_payload(delta_context.write_policy),
        "delta_schema_policy": _delta_schema_policy_payload(delta_context.schema_policy),
        "delta_constraints": list(delta_context.constraints) if delta_context.constraints else None,
    }


@tag(layer="materialize", artifact="cpg_nodes_quality_delta", kind="side_effect")
def write_cpg_nodes_quality_delta(
    output_dir: str | None,
    output_config: OutputConfig,
    cpg_nodes_quality: TableLike,
) -> JsonDict | None:
    """Write CPG node quality diagnostics to Delta.

    Returns
    -------
    JsonDict | None
        Report of the written file, or None when output is disabled.
    """
    if not output_dir:
        return None
    output_path = Path(output_dir)
    _ensure_dir(output_path)
    delta_context = _resolve_delta_write_context(
        "cpg_nodes_quality",
        DeltaWriteOptions(
            mode="overwrite",
            schema_mode="overwrite",
            commit_metadata=_delta_commit_metadata("cpg_nodes_quality", cpg_nodes_quality.schema),
        ),
        output_config=output_config,
    )
    result = write_table_delta(
        cpg_nodes_quality,
        str(output_path / "cpg_nodes_quality"),
        options=delta_context.options,
        storage_options=delta_context.storage_options,
    )
    return {
        "path": result.path,
        "files": [result.path],
        "rows": int(cpg_nodes_quality.num_rows),
        "delta_version": result.version,
        "delta_write_policy": _delta_write_policy_payload(delta_context.write_policy),
        "delta_schema_policy": _delta_schema_policy_payload(delta_context.schema_policy),
    }


@tag(layer="materialize", artifact="cpg_edges_delta", kind="side_effect")
def write_cpg_edges_delta(
    context: CpgDeltaWriteContext,
    cpg_edges_finalize: CpgBuildArtifacts,
    cpg_edges_final: TableLike,
) -> JsonDict | None:
    """Write CPG edges and error artifacts to Delta.

    Returns
    -------
    JsonDict | None
        Report of the written files, or None when output is disabled.
    """
    if not context.output_dir:
        return None
    output_path = Path(context.output_dir)
    _ensure_dir(output_path)
    good_override = cpg_edges_final if context.incremental_config.enabled else None
    finalize = _override_finalize_good(cpg_edges_finalize.finalize, good=good_override)
    require_explicit_ordering(finalize.good.schema, label="cpg_edges_final")
    delta_context = _resolve_delta_write_context(
        "cpg_edges",
        DeltaWriteOptions(
            mode="overwrite",
            schema_mode="overwrite",
            commit_metadata=_delta_commit_metadata("cpg_edges", finalize.good.schema),
        ),
        output_config=context.output_config,
    )
    _validate_delta_constraints(
        "cpg_edges",
        finalize.good,
        constraints=delta_context.constraints,
        ctx=context.ctx,
    )
    paths = write_finalize_result_delta(
        finalize,
        str(output_path / "cpg_edges"),
        options=delta_context.options,
        storage_options=delta_context.storage_options,
    )
    versions = {key: delta_table_version(path) for key, path in paths.items()}
    return {
        "paths": paths,
        "files": list(paths.values()),
        "rows": int(finalize.good.num_rows),
        "error_rows": int(finalize.errors.num_rows),
        "delta_versions": versions,
        "delta_write_policy": _delta_write_policy_payload(delta_context.write_policy),
        "delta_schema_policy": _delta_schema_policy_payload(delta_context.schema_policy),
        "delta_constraints": list(delta_context.constraints) if delta_context.constraints else None,
    }


@tag(layer="materialize", artifact="cpg_props_delta", kind="side_effect")
def write_cpg_props_delta(
    context: CpgDeltaWriteContext,
    cpg_props_finalize: CpgBuildArtifacts,
    cpg_props_final: TableLike,
) -> JsonDict | None:
    """Write CPG properties and error artifacts to Delta.

    Returns
    -------
    JsonDict | None
        Report of the written files, or None when output is disabled.
    """
    if not context.output_dir:
        return None
    output_path = Path(context.output_dir)
    _ensure_dir(output_path)
    good_override = cpg_props_final if context.incremental_config.enabled else None
    finalize = _override_finalize_good(cpg_props_finalize.finalize, good=good_override)
    require_explicit_ordering(finalize.good.schema, label="cpg_props_final")
    delta_context = _resolve_delta_write_context(
        "cpg_props",
        DeltaWriteOptions(
            mode="overwrite",
            schema_mode="overwrite",
            commit_metadata=_delta_commit_metadata("cpg_props", finalize.good.schema),
        ),
        output_config=context.output_config,
    )
    _validate_delta_constraints(
        "cpg_props",
        finalize.good,
        constraints=delta_context.constraints,
        ctx=context.ctx,
    )
    paths = write_finalize_result_delta(
        finalize,
        str(output_path / "cpg_props"),
        options=delta_context.options,
        storage_options=delta_context.storage_options,
    )
    versions = {key: delta_table_version(path) for key, path in paths.items()}
    return {
        "paths": paths,
        "files": list(paths.values()),
        "rows": int(finalize.good.num_rows),
        "error_rows": int(finalize.errors.num_rows),
        "delta_versions": versions,
        "delta_write_policy": _delta_write_policy_payload(delta_context.write_policy),
        "delta_schema_policy": _delta_schema_policy_payload(delta_context.schema_policy),
        "delta_constraints": list(delta_context.constraints) if delta_context.constraints else None,
    }


@tag(layer="materialize", artifact="cpg_props_json_delta", kind="side_effect")
def write_cpg_props_json_delta(
    output_dir: str | None,
    output_config: OutputConfig,
    cpg_props_json: TableLike | None,
) -> JsonDict | None:
    """Write optional JSON-heavy CPG properties to Delta.

    Returns
    -------
    JsonDict | None
        Report of the written file, or None when output is disabled or absent.
    """
    if not output_dir or cpg_props_json is None:
        return None
    output_path = Path(output_dir)
    _ensure_dir(output_path)
    delta_context = _resolve_delta_write_context(
        "cpg_props_json",
        DeltaWriteOptions(
            mode="overwrite",
            schema_mode="overwrite",
            commit_metadata=_delta_commit_metadata("cpg_props_json", cpg_props_json.schema),
        ),
        output_config=output_config,
    )
    result = write_table_delta(
        cpg_props_json,
        str(output_path / "cpg_props_json"),
        options=delta_context.options,
        storage_options=delta_context.storage_options,
    )
    return {
        "path": result.path,
        "files": [result.path],
        "rows": int(cpg_props_json.num_rows),
        "delta_version": result.version,
        "delta_write_policy": _delta_write_policy_payload(delta_context.write_policy),
        "delta_schema_policy": _delta_schema_policy_payload(delta_context.schema_policy),
    }


@tag(layer="materialize", artifact="cpg_props_quality_delta", kind="side_effect")
def write_cpg_props_quality_delta(
    output_dir: str | None,
    output_config: OutputConfig,
    cpg_props_quality: TableLike,
) -> JsonDict | None:
    """Write CPG property quality diagnostics to Delta.

    Returns
    -------
    JsonDict | None
        Report of the written file, or None when output is disabled.
    """
    if not output_dir:
        return None
    output_path = Path(output_dir)
    _ensure_dir(output_path)
    delta_context = _resolve_delta_write_context(
        "cpg_props_quality",
        DeltaWriteOptions(
            mode="overwrite",
            schema_mode="overwrite",
            commit_metadata=_delta_commit_metadata("cpg_props_quality", cpg_props_quality.schema),
        ),
        output_config=output_config,
    )
    result = write_table_delta(
        cpg_props_quality,
        str(output_path / "cpg_props_quality"),
        options=delta_context.options,
        storage_options=delta_context.storage_options,
    )
    return {
        "path": result.path,
        "files": [result.path],
        "rows": int(cpg_props_quality.num_rows),
        "delta_version": result.version,
        "delta_write_policy": _delta_write_policy_payload(delta_context.write_policy),
        "delta_schema_policy": _delta_schema_policy_payload(delta_context.schema_policy),
    }


@tag(layer="obs", artifact="materialization_reports", kind="object")
def materialization_reports(
    output_config: OutputConfig,
    report_inputs: MaterializationReportInputs,
) -> JsonDict | None:
    """Bundle materialization metadata for manifest notes.

    Returns
    -------
    JsonDict | None
        Materialization metadata when any reports are present.
    """
    artifacts: JsonDict = {}
    if report_inputs.normalized_inputs_delta is not None:
        artifacts["normalized_inputs_delta"] = report_inputs.normalized_inputs_delta
    if report_inputs.param_table_delta is not None:
        artifacts["param_table_delta"] = report_inputs.param_table_delta
    if report_inputs.cpg_nodes_delta is not None:
        artifacts["cpg_nodes_delta"] = report_inputs.cpg_nodes_delta
    if report_inputs.cpg_edges_delta is not None:
        artifacts["cpg_edges_delta"] = report_inputs.cpg_edges_delta
    if report_inputs.cpg_props is not None:
        if report_inputs.cpg_props.cpg_props_delta is not None:
            artifacts["cpg_props_delta"] = report_inputs.cpg_props.cpg_props_delta
        if report_inputs.cpg_props.cpg_props_json_delta is not None:
            artifacts["cpg_props_json_delta"] = report_inputs.cpg_props.cpg_props_json_delta
    if not artifacts:
        return None
    return {
        "writer_strategy": output_config.writer_strategy,
        "artifacts": artifacts,
    }


@tag(layer="obs", artifact="cpg_props_report_inputs", kind="object")
def cpg_props_report_inputs(
    cpg_props_delta: JsonDict | None,
    cpg_props_json_delta: JsonDict | None,
) -> CpgPropsReportInputs | None:
    """Bundle CPG props materialization artifacts.

    Returns
    -------
    CpgPropsReportInputs | None
        CPG props report artifacts when any are present.
    """
    if cpg_props_delta is None and cpg_props_json_delta is None:
        return None
    return CpgPropsReportInputs(
        cpg_props_delta=cpg_props_delta,
        cpg_props_json_delta=cpg_props_json_delta,
    )


@tag(layer="obs", artifact="materialization_report_inputs", kind="object")
def materialization_report_inputs(
    normalized_inputs_delta: JsonDict | None,
    param_table_delta: Mapping[str, JsonDict] | None,
    cpg_nodes_delta: JsonDict | None,
    cpg_edges_delta: JsonDict | None,
    cpg_props: CpgPropsReportInputs | None,
) -> MaterializationReportInputs:
    """Bundle materialization report inputs.

    Returns
    -------
    MaterializationReportInputs
        Bundled materialization report inputs.
    """
    return MaterializationReportInputs(
        normalized_inputs_delta=normalized_inputs_delta,
        param_table_delta=param_table_delta,
        cpg_nodes_delta=cpg_nodes_delta,
        cpg_edges_delta=cpg_edges_delta,
        cpg_props=cpg_props,
    )


# -----------------------
# Materializers: extract error artifacts
# -----------------------


@tag(layer="materialize", artifact="extract_errors_delta", kind="side_effect")
def write_extract_error_artifacts_delta(
    output_dir: str | None,
    output_config: OutputConfig,
    extract_error_artifacts: ExtractErrorArtifacts,
) -> JsonDict | None:
    """Write extract error artifacts to Delta tables.

    Output structure:
      <output_dir>/extract_errors/<output>/{errors,error_stats,alignment}

    Returns
    -------
    JsonDict | None
        Report of written error artifacts, or None when output is disabled.
    """
    if not output_dir:
        return None
    base = Path(output_dir) / "extract_errors"
    _ensure_dir(base)
    datasets: dict[str, JsonDict] = {}
    for output in sorted(extract_error_artifacts.errors):
        errors = extract_error_artifacts.errors[output]
        stats = extract_error_artifacts.stats[output]
        alignment = extract_error_artifacts.alignment[output]
        dataset_dir = base / output
        _ensure_dir(dataset_dir)
        error_context = _resolve_delta_write_context(
            f"{output}.errors",
            DeltaWriteOptions(
                mode="overwrite",
                schema_mode="overwrite",
                commit_metadata=_delta_commit_metadata(f"{output}.errors", errors.schema),
            ),
            output_config=output_config,
        )
        stats_context = _resolve_delta_write_context(
            f"{output}.error_stats",
            DeltaWriteOptions(
                mode="overwrite",
                schema_mode="overwrite",
                commit_metadata=_delta_commit_metadata(f"{output}.error_stats", stats.schema),
            ),
            output_config=output_config,
        )
        alignment_context = _resolve_delta_write_context(
            f"{output}.alignment",
            DeltaWriteOptions(
                mode="overwrite",
                schema_mode="overwrite",
                commit_metadata=_delta_commit_metadata(f"{output}.alignment", alignment.schema),
            ),
            output_config=output_config,
        )
        error_result = write_table_delta(
            errors,
            str(dataset_dir / "errors"),
            options=error_context.options,
            storage_options=error_context.storage_options,
        )
        stats_result = write_table_delta(
            stats,
            str(dataset_dir / "error_stats"),
            options=stats_context.options,
            storage_options=stats_context.storage_options,
        )
        alignment_result = write_table_delta(
            alignment,
            str(dataset_dir / "alignment"),
            options=alignment_context.options,
            storage_options=alignment_context.storage_options,
        )
        datasets[output] = {
            "paths": {
                "errors": error_result.path,
                "stats": stats_result.path,
                "alignment": alignment_result.path,
            },
            "files": [
                error_result.path,
                stats_result.path,
                alignment_result.path,
            ],
            "delta_versions": {
                "errors": error_result.version,
                "stats": stats_result.version,
                "alignment": alignment_result.version,
            },
            "error_rows": int(errors.num_rows),
            "stat_rows": int(stats.num_rows),
            "alignment_rows": int(alignment.num_rows),
            "delta_write_policy": _delta_write_policy_payload(error_context.write_policy),
            "delta_schema_policy": _delta_schema_policy_payload(error_context.schema_policy),
        }
    return {"base_dir": str(base), "datasets": datasets}


# -----------------------
# Incremental impact diagnostics
# -----------------------


@tag(layer="obs", artifact="write_inc_changed_exports_delta", kind="table")
def write_inc_changed_exports_delta(
    incremental_changed_exports: pa.Table | None,
    output_dir: str | None,
    work_dir: str | None,
    incremental_config: IncrementalConfig,
) -> str | None:
    """Write incremental export deltas for diagnostics.

    Returns
    -------
    str | None
        Dataset path for the written export deltas.
    """
    context = IncrementalWriteContext(
        output_dir=output_dir,
        work_dir=work_dir,
        incremental_config=incremental_config,
    )
    return _write_incremental_dataset(
        name="inc_changed_exports_v1",
        table=incremental_changed_exports,
        context=context,
    )


@tag(layer="obs", artifact="write_inc_impacted_callers_delta", kind="table")
def write_inc_impacted_callers_delta(
    incremental_impacted_callers: pa.Table | None,
    output_dir: str | None,
    work_dir: str | None,
    incremental_config: IncrementalConfig,
) -> str | None:
    """Write impacted caller diagnostics.

    Returns
    -------
    str | None
        Dataset path for the written caller impacts.
    """
    context = IncrementalWriteContext(
        output_dir=output_dir,
        work_dir=work_dir,
        incremental_config=incremental_config,
    )
    return _write_incremental_dataset(
        name="inc_impacted_callers_v1",
        table=incremental_impacted_callers,
        context=context,
    )


@tag(layer="obs", artifact="write_inc_impacted_importers_delta", kind="table")
def write_inc_impacted_importers_delta(
    incremental_impacted_importers: pa.Table | None,
    output_dir: str | None,
    work_dir: str | None,
    incremental_config: IncrementalConfig,
) -> str | None:
    """Write impacted importer diagnostics.

    Returns
    -------
    str | None
        Dataset path for the written importer impacts.
    """
    context = IncrementalWriteContext(
        output_dir=output_dir,
        work_dir=work_dir,
        incremental_config=incremental_config,
    )
    return _write_incremental_dataset(
        name="inc_impacted_importers_v1",
        table=incremental_impacted_importers,
        context=context,
    )


@tag(layer="obs", artifact="write_inc_impacted_files_delta", kind="table")
def write_inc_impacted_files_delta(
    incremental_impacted_files: pa.Table | None,
    output_dir: str | None,
    work_dir: str | None,
    incremental_config: IncrementalConfig,
) -> str | None:
    """Write impacted file diagnostics.

    Returns
    -------
    str | None
        Dataset path for the written impacted files.
    """
    context = IncrementalWriteContext(
        output_dir=output_dir,
        work_dir=work_dir,
        incremental_config=incremental_config,
    )
    return _write_incremental_dataset(
        name="inc_impacted_files_v2",
        table=incremental_impacted_files,
        context=context,
    )


@tag(layer="obs", artifact="write_inc_output_fingerprint_changes_delta", kind="table")
def write_inc_output_fingerprint_changes_delta(
    incremental_output_fingerprint_changes: pa.Table | None,
    output_dir: str | None,
    work_dir: str | None,
    incremental_config: IncrementalConfig,
) -> str | None:
    """Write output fingerprint change diagnostics.

    Returns
    -------
    str | None
        Dataset path for the fingerprint change report.
    """
    context = IncrementalWriteContext(
        output_dir=output_dir,
        work_dir=work_dir,
        incremental_config=incremental_config,
    )
    return _write_incremental_dataset(
        name="inc_output_fingerprint_changes_v1",
        table=incremental_output_fingerprint_changes,
        context=context,
    )


# -----------------------
# Debug stats for normalized datasets
# -----------------------


@tag(layer="obs", artifact="relspec_input_dataset_stats", kind="table")
def relspec_input_dataset_stats(relspec_input_datasets: dict[str, TableLike]) -> TableLike:
    """Build dataset-level stats for relationship inputs.

    Returns
    -------
    TableLike
        Dataset-level statistics table.
    """
    return dataset_stats_table(relspec_input_datasets)


@tag(layer="obs", artifact="relspec_input_column_stats", kind="table")
def relspec_input_column_stats(relspec_input_datasets: dict[str, TableLike]) -> TableLike:
    """Build column-level stats for relationship inputs.

    Returns
    -------
    TableLike
        Column-level statistics table.
    """
    return column_stats_table(relspec_input_datasets)


@tag(layer="obs", artifact="relspec_scan_telemetry", kind="table")
def relspec_scan_telemetry(
    persist_relspec_input_datasets: dict[str, DatasetLocation],
    ctx: ExecutionContext,
) -> TableLike:
    """Collect scan telemetry for filesystem-backed relationship inputs.

    Returns
    -------
    TableLike
        Telemetry table with fragment counts and estimated rows.
    """
    if not persist_relspec_input_datasets:
        return empty_scan_telemetry_table()

    rows: list[dict[str, object]] = []
    for name, loc in persist_relspec_input_datasets.items():
        dataset_options = DatasetSourceOptions(
            dataset_format=loc.format,
            filesystem=loc.filesystem,
            partitioning=loc.partitioning,
            parquet_read_options=ctx.runtime.scan.parquet_read_options,
            storage_options=loc.storage_options,
            delta_version=loc.delta_version,
            delta_timestamp=loc.delta_timestamp,
            discovery=DatasetDiscoveryOptions(),
        )
        dataset = normalize_dataset_source(loc.path, options=dataset_options)
        if loc.table_spec is not None:
            dataset_spec = make_dataset_spec(table_spec=loc.table_spec)
        else:
            dataset_spec = dataset_spec_from_schema(name=name, schema=dataset.schema)
        query = dataset_spec.query()
        scan_provenance = tuple(ctx.runtime.scan.scan_provenance_columns)
        scan_column_names = list(query.projection.base)
        for col_name in scan_provenance:
            if col_name not in scan_column_names:
                scan_column_names.append(col_name)
        scan_columns = scan_column_names or None
        required_columns = list(scan_column_names)
        predicate = None
        if isinstance(query.pushdown_predicate, ExprIR):
            predicate = query.pushdown_predicate.to_expression()
        scanner = dataset.scanner(
            columns=scan_columns,
            filter=predicate,
            **ctx.runtime.scan.scanner_kwargs(),
        )
        telemetry = fragment_telemetry(
            dataset,
            predicate=predicate,
            scanner=scanner,
            options=ScanTelemetryOptions(
                discovery_policy=dataset_options.discovery_payload(),
                scan_profile=_scan_profile_payload(ctx),
                required_columns=required_columns,
                scan_columns=scan_column_names,
            ),
        )
        scan_profile_text = _stable_repr(_normalize_value(telemetry.scan_profile))
        dataset_schema = (
            _stable_repr(_normalize_value(telemetry.dataset_schema))
            if telemetry.dataset_schema is not None
            else None
        )
        projected_schema = (
            _stable_repr(_normalize_value(telemetry.projected_schema))
            if telemetry.projected_schema is not None
            else None
        )
        rows.append(
            {
                "dataset": name,
                "fragment_count": int(telemetry.fragment_count),
                "row_group_count": int(telemetry.row_group_count),
                "count_rows": telemetry.count_rows,
                "estimated_rows": telemetry.estimated_rows,
                "file_hints": list(telemetry.file_hints),
                "fragment_paths": list(telemetry.fragment_paths),
                "partition_expressions": list(telemetry.partition_expressions),
                "required_columns": list(telemetry.required_columns),
                "scan_columns": list(telemetry.scan_columns),
                "dataset_schema_json": dataset_schema,
                "projected_schema_json": projected_schema,
                "discovery_policy_json": _stable_repr(_normalize_value(telemetry.discovery_policy))
                if telemetry.discovery_policy is not None
                else None,
                "scan_profile_json": scan_profile_text,
            }
        )
    return scan_telemetry_table(rows)


# -----------------------
# Manifest nodes
# -----------------------


@tag(layer="obs", artifact="manifest_context", kind="object")
def manifest_context(
    repo_scan_config: RepoScanConfig,
    relspec_config: RelspecConfig,
    output_config: OutputConfig,
) -> ManifestContext:
    """Build the manifest context from config bundles.

    Returns
    -------
    ManifestContext
        Manifest context fields.
    """
    return ManifestContext(
        repo_root=repo_scan_config.repo_root,
        relspec_mode=relspec_config.relspec_mode,
        work_dir=output_config.work_dir,
        output_dir=output_config.output_dir,
    )


@tag(layer="obs", artifact="relspec_inputs_bundle", kind="bundle")
def relspec_inputs_bundle(
    relspec_input_datasets: dict[str, TableLike],
    persist_relspec_input_datasets: dict[str, DatasetLocation],
) -> RelspecInputsBundle:
    """Bundle relationship input tables and locations.

    Returns
    -------
    RelspecInputsBundle
        Relationship input bundle.
    """
    return RelspecInputsBundle(
        tables=relspec_input_datasets,
        locations=persist_relspec_input_datasets,
    )


@tag(layer="obs", artifact="manifest_input_tables", kind="object")
def manifest_input_tables(run_bundle_tables: RunBundleTables) -> ManifestInputTables:
    """Bundle tables required for manifest inputs.

    Returns
    -------
    ManifestInputTables
        Table inputs for manifest assembly.
    """
    return ManifestInputTables(
        relspec_inputs_bundle=run_bundle_tables.relspec_inputs,
        relationship_output_tables=run_bundle_tables.relationship_outputs,
        cpg_output_tables=run_bundle_tables.cpg_outputs,
    )


@tag(layer="obs", artifact="manifest_input_artifacts", kind="object")
def manifest_input_artifacts(
    relspec_snapshots: RelspecSnapshots,
    param_table_artifacts: Mapping[str, ParamTableArtifact] | None = None,
    param_scalar_signature: str | None = None,
    extract_manifest_inputs: ExtractManifestInputs | None = None,
    materialization_reports: JsonDict | None = None,
) -> ManifestInputArtifacts:
    """Bundle artifacts required for manifest inputs.

    Returns
    -------
    ManifestInputArtifacts
        Artifact inputs for manifest assembly.
    """
    return ManifestInputArtifacts(
        relspec_snapshots=relspec_snapshots,
        param_table_artifacts=param_table_artifacts,
        param_scalar_signature=param_scalar_signature,
        extract_inputs=extract_manifest_inputs,
        materialization_reports=materialization_reports,
    )


@tag(layer="obs", artifact="cpg_output_tables", kind="bundle")
def cpg_output_tables(
    cpg_nodes: TableLike,
    cpg_edges: TableLike,
    cpg_props: TableLike,
    cpg_props_json: TableLike | None,
) -> CpgOutputTables:
    """Bundle CPG output tables.

    Returns
    -------
    CpgOutputTables
        CPG output bundle.
    """
    return CpgOutputTables(
        cpg_nodes=cpg_nodes,
        cpg_edges=cpg_edges,
        cpg_props=cpg_props,
        cpg_props_json=cpg_props_json,
    )


@tag(layer="obs", artifact="relspec_snapshots", kind="object")
def relspec_snapshots(
    relspec_runtime: RelspecRuntime,
    relationship_contracts: ContractCatalog,
    compiled_relationship_outputs: dict[str, CompiledOutput],
) -> RelspecSnapshots:
    """Bundle relationship snapshots for observability.

    Returns
    -------
    RelspecSnapshots
        Relationship snapshot bundle.
    """
    snapshot = build_relspec_snapshot(relspec_runtime.registry)
    return RelspecSnapshots(
        registry_snapshot=snapshot,
        contracts=relationship_contracts,
        compiled_outputs=compiled_relationship_outputs,
    )


@tag(layer="obs", artifact="manifest_inputs", kind="object")
def manifest_inputs(
    manifest_input_tables: ManifestInputTables,
    manifest_input_artifacts: ManifestInputArtifacts,
) -> ManifestInputs:
    """Bundle manifest inputs from pipeline outputs.

    Returns
    -------
    ManifestInputs
        Manifest input bundle.
    """
    return ManifestInputs(
        relspec_inputs_bundle=manifest_input_tables.relspec_inputs_bundle,
        relationship_output_tables=manifest_input_tables.relationship_output_tables,
        cpg_output_tables=manifest_input_tables.cpg_output_tables,
        relspec_snapshots=manifest_input_artifacts.relspec_snapshots,
        param_table_artifacts=manifest_input_artifacts.param_table_artifacts,
        param_scalar_signature=manifest_input_artifacts.param_scalar_signature,
        extract_inputs=manifest_input_artifacts.extract_inputs,
        materialization_reports=manifest_input_artifacts.materialization_reports,
    )


@tag(layer="obs", artifact="extract_manifest_inputs", kind="object")
def extract_manifest_inputs(
    evidence_plan: EvidencePlan | None = None,
    extract_error_counts: Mapping[str, int] | None = None,
) -> ExtractManifestInputs:
    """Bundle extract-specific inputs for manifest data.

    Returns
    -------
    ExtractManifestInputs
        Extract-specific manifest inputs.
    """
    return ExtractManifestInputs(
        evidence_plan=evidence_plan,
        extract_error_counts=extract_error_counts,
    )


@tag(layer="obs", artifact="manifest_data", kind="object")
def manifest_data(
    manifest_inputs: ManifestInputs,
    normalize_rule_compilation: NormalizeRuleCompilation | None = None,
    output_config: OutputConfig | None = None,
    ctx: ExecutionContext | None = None,
) -> ManifestData:
    """Assemble manifest data inputs from pipeline outputs.

    Returns
    -------
    ManifestData
        Manifest input bundle.

    Raises
    ------
    ValueError
        Raised when ExecutionContext is not provided.
    """
    normalize_rules = (
        normalize_rule_compilation.resolved_rules if normalize_rule_compilation else None
    )
    if ctx is None:
        msg = "manifest_data requires ExecutionContext."
        raise ValueError(msg)
    relationship_metadata = _manifest_relationship_metadata(
        manifest_inputs,
        normalize_rule_compilation,
        ctx,
    )
    runtime_artifacts = _manifest_runtime_artifacts(ctx)
    sqlglot_ast_payloads = _sqlglot_ast_payloads(manifest_inputs)
    function_registry = default_function_registry()
    dataset_snapshot = _dataset_registry_snapshot(manifest_inputs.relspec_inputs_bundle.locations)
    extract_inputs = manifest_inputs.extract_inputs
    relationship_rules = _relationship_rules_from_snapshots(manifest_inputs)
    return ManifestData(
        relspec_input_tables=manifest_inputs.relspec_inputs_bundle.tables,
        relspec_input_locations=manifest_inputs.relspec_inputs_bundle.locations,
        relationship_outputs=manifest_inputs.relationship_output_tables.as_dict(),
        compiled_relationship_outputs=manifest_inputs.relspec_snapshots.compiled_outputs,
        cpg_nodes=manifest_inputs.cpg_output_tables.cpg_nodes,
        cpg_edges=manifest_inputs.cpg_output_tables.cpg_edges,
        cpg_props=manifest_inputs.cpg_output_tables.cpg_props,
        cpg_props_json=manifest_inputs.cpg_output_tables.cpg_props_json,
        param_table_artifacts=manifest_inputs.param_table_artifacts,
        param_scalar_signature=manifest_inputs.param_scalar_signature,
        extract_evidence_plan=extract_inputs.evidence_plan if extract_inputs else None,
        extract_error_counts=extract_inputs.extract_error_counts if extract_inputs else None,
        relationship_rules=relationship_rules,
        normalize_rules=normalize_rules,
        produced_relationship_output_names=relationship_metadata.produced_outputs,
        relationship_output_lineage=relationship_metadata.lineage,
        normalize_output_lineage=relationship_metadata.normalize_lineage,
        relspec_scan_telemetry=_relspec_scan_telemetry(manifest_inputs) or None,
        datafusion_settings=runtime_artifacts.datafusion_settings,
        datafusion_settings_hash=runtime_artifacts.datafusion_settings_hash,
        datafusion_feature_gates=runtime_artifacts.datafusion_feature_gates,
        datafusion_metrics=runtime_artifacts.datafusion_metrics,
        datafusion_traces=runtime_artifacts.datafusion_traces,
        datafusion_function_catalog=runtime_artifacts.datafusion_function_catalog,
        datafusion_function_catalog_hash=runtime_artifacts.datafusion_function_catalog_hash,
        runtime_profile_snapshot=runtime_artifacts.runtime_snapshot.payload(),
        runtime_profile_hash=runtime_artifacts.runtime_snapshot.profile_hash,
        sqlglot_policy_snapshot=sqlglot_policy_snapshot().payload(),
        function_registry_snapshot=function_registry.payload(),
        function_registry_hash=function_registry.fingerprint(),
        sqlglot_ast_payloads=sqlglot_ast_payloads,
        dataset_registry_snapshot=dataset_snapshot,
        runtime_profile_name=ctx.runtime.name,
        determinism_tier=ctx.determinism.value,
        writer_strategy=output_config.writer_strategy if output_config is not None else None,
        notes=relationship_metadata.notes,
    )


@tag(layer="obs", artifact="relationship_output_fingerprints", kind="object")
def relationship_output_fingerprints_map(
    manifest_data: ManifestData,
) -> Mapping[str, str]:
    """Return dataset fingerprints for relationship outputs.

    Returns
    -------
    Mapping[str, str]
        Mapping of output dataset names to fingerprint hashes.
    """
    return relationship_output_fingerprints(manifest_data)


def _produced_relationship_outputs(manifest_inputs: ManifestInputs) -> list[str]:
    return sorted(manifest_inputs.relspec_snapshots.compiled_outputs.keys())


def _relationship_output_lineage(
    manifest_inputs: ManifestInputs,
) -> dict[str, list[str]]:
    return {
        name: [cr.rule.name for cr in compiled.contributors]
        for name, compiled in manifest_inputs.relspec_snapshots.compiled_outputs.items()
    }


def _normalize_output_lineage(
    normalize_rule_compilation: NormalizeRuleCompilation | None,
) -> dict[str, list[str]] | None:
    if normalize_rule_compilation is None:
        return None
    lineage: dict[str, list[str]] = {}
    for rule in normalize_rule_compilation.rules:
        lineage.setdefault(rule.output, []).append(rule.name)
    return lineage


@dataclass(frozen=True)
class _ManifestRuntimeArtifacts:
    datafusion_settings: list[dict[str, str]] | None
    datafusion_settings_hash: str | None
    datafusion_feature_gates: dict[str, str] | None
    datafusion_metrics: Mapping[str, object] | None
    datafusion_traces: Mapping[str, object] | None
    datafusion_function_catalog: list[dict[str, object]] | None
    datafusion_function_catalog_hash: str | None
    runtime_snapshot: RuntimeProfileSnapshot


@dataclass(frozen=True)
class _ManifestRelationshipMetadata:
    produced_outputs: list[str]
    lineage: dict[str, list[str]]
    normalize_lineage: dict[str, list[str]] | None
    notes: JsonDict


def _manifest_runtime_artifacts(ctx: ExecutionContext) -> _ManifestRuntimeArtifacts:
    datafusion_settings = _datafusion_settings_snapshot(ctx)
    datafusion_settings_hash = _datafusion_settings_hash(ctx)
    datafusion_feature_gates = _datafusion_feature_gates(ctx)
    datafusion_metrics, datafusion_traces = _datafusion_runtime_artifacts(ctx)
    function_catalog, function_catalog_hash = _datafusion_function_catalog_snapshot(ctx)
    runtime_snapshot = runtime_profile_snapshot(ctx.runtime)
    return _ManifestRuntimeArtifacts(
        datafusion_settings=datafusion_settings,
        datafusion_settings_hash=datafusion_settings_hash,
        datafusion_feature_gates=datafusion_feature_gates,
        datafusion_metrics=datafusion_metrics,
        datafusion_traces=datafusion_traces,
        datafusion_function_catalog=function_catalog,
        datafusion_function_catalog_hash=function_catalog_hash,
        runtime_snapshot=runtime_snapshot,
    )


def _manifest_relationship_metadata(
    manifest_inputs: ManifestInputs,
    normalize_rule_compilation: NormalizeRuleCompilation | None,
    ctx: ExecutionContext,
) -> _ManifestRelationshipMetadata:
    produced_outputs = _produced_relationship_outputs(manifest_inputs)
    lineage = _relationship_output_lineage(manifest_inputs)
    normalize_lineage = _normalize_output_lineage(normalize_rule_compilation)
    notes = _manifest_notes(
        produced_outputs,
        normalize_lineage=normalize_lineage,
        normalize_rule_compilation=normalize_rule_compilation,
        manifest_inputs=manifest_inputs,
        ctx=ctx,
    )
    return _ManifestRelationshipMetadata(
        produced_outputs=produced_outputs,
        lineage=lineage,
        normalize_lineage=normalize_lineage,
        notes=notes,
    )


def _sqlglot_ast_payloads(manifest_inputs: ManifestInputs) -> list[JsonDict] | None:
    diagnostics = rule_diagnostics_from_table(
        manifest_inputs.relspec_snapshots.registry_snapshot.rule_diagnostics
    )
    payloads: list[JsonDict] = []
    for diagnostic in diagnostics:
        metadata = diagnostic.metadata
        raw_sql = metadata.get("raw_sql")
        optimized_sql = metadata.get("optimized_sql")
        if not raw_sql or not optimized_sql:
            continue
        payloads.append(
            {
                "domain": str(diagnostic.domain),
                "rule_name": diagnostic.rule_name,
                "plan_signature": diagnostic.plan_signature,
                "plan_fingerprint": metadata.get("plan_fingerprint"),
                "sqlglot_policy_hash": metadata.get("sqlglot_policy_hash"),
                "sql_dialect": metadata.get("sql_dialect"),
                "normalization_distance": metadata.get("normalization_distance"),
                "normalization_max_distance": metadata.get("normalization_max_distance"),
                "normalization_applied": metadata.get("normalization_applied"),
                "raw_sql": raw_sql,
                "optimized_sql": optimized_sql,
                "ast_repr": metadata.get("ast_repr"),
            }
        )
    return payloads or None


def _manifest_notes(
    produced_outputs: Sequence[str],
    *,
    normalize_lineage: Mapping[str, Sequence[str]] | None,
    normalize_rule_compilation: NormalizeRuleCompilation | None,
    manifest_inputs: ManifestInputs,
    ctx: ExecutionContext,
) -> JsonDict:
    notes: JsonDict = {"relationship_output_keys": list(produced_outputs)}
    notes.update(_normalize_notes(normalize_lineage, normalize_rule_compilation))
    notes.update(_runtime_telemetry_notes(manifest_inputs))
    notes.update(_datafusion_notes(ctx))
    notes.update(_rule_plan_hash_notes(manifest_inputs))
    if manifest_inputs.materialization_reports:
        notes["materialization_reports"] = manifest_inputs.materialization_reports
    return notes


def _rule_plan_hash_notes(manifest_inputs: ManifestInputs) -> JsonDict:
    rule_diagnostics = manifest_inputs.relspec_snapshots.registry_snapshot.rule_diagnostics
    hashes: dict[str, str] = {}
    for diagnostic in rule_diagnostics_from_table(rule_diagnostics):
        if diagnostic.rule_name is None:
            continue
        plan_hash = diagnostic.metadata.get("plan_hash")
        if plan_hash:
            hashes[diagnostic.rule_name] = plan_hash
    return {"rule_plan_hashes": hashes} if hashes else {}


def _normalize_notes(
    normalize_lineage: Mapping[str, Sequence[str]] | None,
    normalize_rule_compilation: NormalizeRuleCompilation | None,
) -> JsonDict:
    notes: JsonDict = {}
    if normalize_lineage:
        notes["normalize_output_keys"] = sorted(normalize_lineage)
    if normalize_rule_compilation is None or not normalize_rule_compilation.output_storage:
        return notes
    notes["normalize_output_storage"] = cast(
        "JsonValue", dict(normalize_rule_compilation.output_storage)
    )
    return notes


def _runtime_telemetry_notes(manifest_inputs: ManifestInputs) -> JsonDict:
    runtime_telemetry = next(
        (
            compiled.runtime_telemetry
            for compiled in manifest_inputs.relspec_snapshots.compiled_outputs.values()
            if compiled.runtime_telemetry
        ),
        {},
    )
    if not runtime_telemetry:
        return {}
    return {"relspec_runtime_telemetry": cast("JsonValue", runtime_telemetry)}


def _explain_note_payload(entry: Mapping[str, object]) -> JsonDict:
    payload: JsonDict = {}
    for key in ("rule", "output", "sql", "explain_analyze"):
        if key in entry:
            payload[key] = cast("JsonValue", entry.get(key))
    rows = entry.get("rows")
    if isinstance(rows, (RecordBatchReaderLike, TableLike)):
        payload["rows_schema_fingerprint"] = schema_fingerprint(rows.schema)
    elif rows is not None:
        payload["rows"] = cast("JsonValue", rows)
    return payload


def _datafusion_diagnostics_notes(runtime: DataFusionRuntimeProfile) -> JsonDict:
    notes: JsonDict = {}
    diagnostics = runtime.diagnostics_sink
    if diagnostics is None:
        return notes
    artifacts = diagnostics.artifacts_snapshot()
    udf_registry = artifacts.get("datafusion_udf_registry_v1", [])
    if udf_registry:
        notes["datafusion_udf_registry"] = cast("JsonValue", udf_registry)
    table_providers = artifacts.get("datafusion_table_providers_v1", [])
    if table_providers:
        notes["datafusion_table_providers"] = cast("JsonValue", table_providers)
    return notes


def _datafusion_catalog_notes(
    ctx: ExecutionContext,
    runtime: DataFusionRuntimeProfile,
) -> JsonDict:
    notes: JsonDict = {}
    catalog_snapshot = _datafusion_catalog_snapshot(ctx)
    if catalog_snapshot:
        notes["datafusion_catalog"] = cast("JsonValue", catalog_snapshot)
    cached = cached_dataset_names(runtime.session_context())
    if cached:
        notes["datafusion_cached_datasets"] = cast("JsonValue", list(cached))
    return notes


def _datafusion_explain_notes(runtime: DataFusionRuntimeProfile) -> JsonDict:
    explain_collector = runtime.explain_collector
    if explain_collector is None:
        return {}
    explain_entries = explain_collector.snapshot()
    if not explain_entries:
        return {}
    return {"datafusion_explain": [_explain_note_payload(entry) for entry in explain_entries]}


def _datafusion_fallback_notes(runtime: DataFusionRuntimeProfile) -> JsonDict:
    notes: JsonDict = {}
    fallback_collector = runtime.fallback_collector
    if fallback_collector is not None:
        fallback_entries = fallback_collector.snapshot()
        if fallback_entries:
            notes["datafusion_fallbacks"] = cast("JsonValue", fallback_entries)
    if runtime.labeled_explains:
        notes["datafusion_rule_explain"] = [
            _explain_note_payload(entry) for entry in runtime.labeled_explains
        ]
    if runtime.labeled_fallbacks:
        notes["datafusion_rule_fallbacks"] = cast(
            "JsonValue",
            list(runtime.labeled_fallbacks),
        )
    return notes


def _datafusion_notes(ctx: ExecutionContext) -> JsonDict:
    runtime = ctx.runtime.datafusion
    if runtime is None:
        return {}
    notes: JsonDict = {"datafusion_policy": cast("JsonValue", runtime.telemetry_payload_v1())}
    notes.update(_datafusion_diagnostics_notes(runtime))
    notes.update(_datafusion_catalog_notes(ctx, runtime))
    notes.update(_datafusion_explain_notes(runtime))
    notes.update(_datafusion_fallback_notes(runtime))
    return notes


def _relspec_scan_telemetry(
    manifest_inputs: ManifestInputs,
) -> dict[str, Mapping[str, ScanTelemetry]]:
    return {
        name: compiled.telemetry
        for name, compiled in manifest_inputs.relspec_snapshots.compiled_outputs.items()
        if compiled.telemetry
    }


def _relationship_rules_from_snapshots(
    manifest_inputs: ManifestInputs,
) -> tuple[RelationshipRule, ...]:
    rule_definitions = rule_definitions_from_table(
        manifest_inputs.relspec_snapshots.registry_snapshot.rule_table
    )
    return tuple(
        relationship_rule_from_definition(defn) for defn in rule_definitions if defn.domain == "cpg"
    )


@tag(layer="obs", artifact="run_manifest", kind="object")
def run_manifest(
    manifest_context: ManifestContext,
    manifest_data: ManifestData,
) -> JsonDict:
    """Produce a structured manifest dictionary.

    Records:
      - dataset paths (when filesystem-backed)
      - schema fingerprints + row counts
      - rule outputs produced

    Returns
    -------
    JsonDict
        Manifest dictionary.
    """
    manifest = build_manifest(manifest_context, manifest_data)
    return manifest.to_dict()


@tag(layer="obs", artifact="run_manifest_delta", kind="side_effect")
def write_run_manifest_delta(
    run_manifest: JsonDict,
    output_dir: str | None,
    work_dir: str | None,
) -> JsonDict | None:
    """Write run manifest Delta table.

    Output location:
      - if output_dir set: <output_dir>/manifest.delta
      - else if work_dir set: <work_dir>/manifest.delta
      - else: None (no-op)

    Returns
    -------
    JsonDict | None
        Path info for the manifest, or None when disabled.
    """
    base = output_dir or work_dir
    if not base:
        return None
    base_path = Path(base)
    _ensure_dir(base_path)
    path = base_path / "manifest.delta"
    write_manifest_delta(run_manifest, str(path), overwrite=True)
    return {"path": str(path)}


@tag(layer="obs", artifact="run_config", kind="object")
def run_config(
    repo_scan_config: RepoScanConfig,
    relspec_config: RelspecConfig,
    output_config: OutputConfig,
    relspec_param_values: JsonDict,
    incremental_config: IncrementalConfig,
) -> JsonDict:
    """Return the execution-relevant config snapshot for this run.

    This is what makes a run bundle re-playable.

    Returns
    -------
    JsonDict
        Run configuration snapshot.
    """
    return {
        "repo_root": repo_scan_config.repo_root,
        "scip_index_path": relspec_config.scip_index_path,
        "include_globs": list(repo_scan_config.include_globs),
        "exclude_globs": list(repo_scan_config.exclude_globs),
        "max_files": int(repo_scan_config.max_files),
        "relspec_mode": relspec_config.relspec_mode,
        "work_dir": output_config.work_dir,
        "output_dir": output_config.output_dir,
        "overwrite_intermediate_datasets": bool(output_config.overwrite_intermediate_datasets),
        "materialize_param_tables": bool(output_config.materialize_param_tables),
        "writer_strategy": output_config.writer_strategy,
        "relspec_param_values": dict(relspec_param_values),
        "incremental_enabled": bool(incremental_config.enabled),
        "incremental_state_dir": (
            str(incremental_config.state_dir) if incremental_config.state_dir else None
        ),
        "incremental_repo_id": incremental_config.repo_id,
        "incremental_impact_strategy": incremental_config.impact_strategy,
    }


@tag(layer="obs", artifact="run_bundle_metadata", kind="object")
def run_bundle_metadata(run_manifest: JsonDict, run_config: JsonDict) -> RunBundleMetadata:
    """Bundle manifest and config inputs for run bundles.

    Returns
    -------
    RunBundleMetadata
        Manifest/config bundle for run bundle assembly.
    """
    return RunBundleMetadata(run_manifest=run_manifest, run_config=run_config)


@tag(layer="obs", artifact="run_bundle_tables", kind="bundle")
def run_bundle_tables(
    relspec_inputs_bundle: RelspecInputsBundle,
    relationship_output_tables: RelationshipOutputTables,
    cpg_output_tables: CpgOutputTables,
    incremental_relationship_updates: Mapping[str, str] | None,
) -> RunBundleTables:
    """Bundle tables for run bundle writing.

    Returns
    -------
    RunBundleTables
        Table bundle for run bundles.
    """
    _ = incremental_relationship_updates
    return RunBundleTables(
        relspec_inputs=relspec_inputs_bundle,
        relationship_outputs=relationship_output_tables,
        cpg_outputs=cpg_output_tables,
    )


@tag(layer="obs", artifact="run_bundle_incremental_tables", kind="object")
def run_bundle_incremental_tables(
    incremental_inputs: RunBundleIncrementalInputs,
) -> RunBundleIncrementalTables:
    """Bundle incremental diagnostics for run bundle writing.

    Returns
    -------
    RunBundleIncrementalTables
        Incremental diagnostics for run bundles.
    """
    return RunBundleIncrementalTables(
        incremental_diff=incremental_inputs.incremental_diff,
        incremental_plan_diff=incremental_inputs.incremental_plan_diff,
        incremental_changed_exports=incremental_inputs.incremental_changed_exports,
        incremental_impacted_callers=incremental_inputs.incremental_impacted_callers,
        incremental_impacted_importers=incremental_inputs.incremental_impacted_importers,
        incremental_impacted_files=incremental_inputs.incremental_impacted_files,
        incremental_output_fingerprint_changes=(
            incremental_inputs.incremental_output_fingerprint_changes
        ),
    )


@tag(layer="obs", artifact="run_bundle_incremental_inputs", kind="object")
def run_bundle_incremental_inputs(
    incremental_tables: RunBundleIncrementalTablesBundle,
) -> RunBundleIncrementalInputs:
    """Bundle inputs required for incremental diagnostics.

    Returns
    -------
    RunBundleIncrementalInputs
        Bundled incremental diagnostic inputs.
    """
    return RunBundleIncrementalInputs(
        incremental_diff=incremental_tables.incremental_diff,
        incremental_plan_diff=incremental_tables.incremental_plan_diff,
        incremental_changed_exports=incremental_tables.incremental_changed_exports,
        incremental_impacted_callers=incremental_tables.incremental_impacted_callers,
        incremental_impacted_importers=incremental_tables.incremental_impacted_importers,
        incremental_impacted_files=incremental_tables.incremental_impacted_files,
        incremental_output_fingerprint_changes=(
            incremental_tables.incremental_output_fingerprint_changes
        ),
    )


@tag(layer="obs", artifact="run_bundle_incremental_tables_bundle", kind="object")
def run_bundle_incremental_tables_bundle(
    incremental_tables: RunBundleIncrementalTables,
) -> RunBundleIncrementalTablesBundle:
    """Bundle incremental tables for input wiring.

    Returns
    -------
    RunBundleIncrementalTablesBundle
        Bundle of incremental tables for run bundle wiring.
    """
    return RunBundleIncrementalTablesBundle(
        incremental_diff=incremental_tables.incremental_diff,
        incremental_plan_diff=incremental_tables.incremental_plan_diff,
        incremental_changed_exports=incremental_tables.incremental_changed_exports,
        incremental_impacted_callers=incremental_tables.incremental_impacted_callers,
        incremental_impacted_importers=incremental_tables.incremental_impacted_importers,
        incremental_impacted_files=incremental_tables.incremental_impacted_files,
        incremental_output_fingerprint_changes=(
            incremental_tables.incremental_output_fingerprint_changes
        ),
    )


@tag(layer="obs", artifact="run_bundle_inputs", kind="object")
def run_bundle_inputs(
    run_bundle_metadata: RunBundleMetadata,
    relspec_snapshots: RelspecSnapshots,
    run_bundle_tables: RunBundleTables,
    run_bundle_param_inputs: RunBundleParamInputs | None,
    run_bundle_incremental_tables: RunBundleIncrementalTables,
) -> RunBundleInputs:
    """Bundle inputs for run bundle context creation.

    Returns
    -------
    RunBundleInputs
        Inputs used to build a run bundle context.
    """
    return RunBundleInputs(
        run_manifest=run_bundle_metadata.run_manifest,
        run_config=run_bundle_metadata.run_config,
        relspec_snapshots=relspec_snapshots,
        tables=run_bundle_tables,
        param_inputs=run_bundle_param_inputs,
        incremental_diff=run_bundle_incremental_tables.incremental_diff,
        incremental_plan_diff=run_bundle_incremental_tables.incremental_plan_diff,
        incremental_changed_exports=run_bundle_incremental_tables.incremental_changed_exports,
        incremental_impacted_callers=run_bundle_incremental_tables.incremental_impacted_callers,
        incremental_impacted_importers=run_bundle_incremental_tables.incremental_impacted_importers,
        incremental_impacted_files=run_bundle_incremental_tables.incremental_impacted_files,
        incremental_output_fingerprint_changes=(
            run_bundle_incremental_tables.incremental_output_fingerprint_changes
        ),
    )


@tag(layer="obs", artifact="run_bundle_param_inputs", kind="object")
def run_bundle_param_inputs_context(
    param_table_specs: tuple[ParamTableSpec, ...],
    relspec_param_registry: IbisParamRegistry,
    param_table_artifacts: Mapping[str, ParamTableArtifact] | None,
    param_scalar_signature: str | None,
    relspec_param_dependency_reports: tuple[RuleDependencyReport, ...],
) -> RunBundleParamInputsContext:
    """Bundle param-table inputs for run bundle context creation.

    Returns
    -------
    RunBundleParamInputsContext
        Param-table inputs for run bundle context creation.
    """
    return RunBundleParamInputsContext(
        param_table_specs=param_table_specs,
        relspec_param_registry=relspec_param_registry,
        param_table_artifacts=param_table_artifacts,
        param_scalar_signature=param_scalar_signature,
        relspec_param_dependency_reports=relspec_param_dependency_reports,
    )


@tag(layer="obs", artifact="run_bundle_param_inputs", kind="object")
def run_bundle_param_inputs(
    context: RunBundleParamInputsContext,
    output_config: OutputConfig,
) -> RunBundleParamInputs:
    """Bundle param-table inputs for run bundle context creation.

    Returns
    -------
    RunBundleParamInputs
        Parameter-table inputs for run bundle context.
    """
    reverse_index = build_param_reverse_index(context.relspec_param_dependency_reports)
    param_scalar_specs = tuple(
        sorted(context.relspec_param_registry.specs.values(), key=lambda spec: spec.name)
    )
    return RunBundleParamInputs(
        param_table_specs=context.param_table_specs,
        param_scalar_specs=param_scalar_specs,
        param_table_artifacts=context.param_table_artifacts,
        param_scalar_signature=context.param_scalar_signature,
        param_dependency_reports=context.relspec_param_dependency_reports,
        param_reverse_index=reverse_index or None,
        include_param_table_data=output_config.materialize_param_tables,
    )


@tag(layer="obs", artifact="run_bundle_context_inputs", kind="object")
def run_bundle_context_inputs(
    output_config: OutputConfig,
    relspec_rule_exec_events: pa.Table | None,
    relspec_scan_telemetry: TableLike,
    feature_state_diagnostics_table: pa.Table | None,
    ctx: ExecutionContext | None = None,
) -> RunBundleContextInputs:
    """Bundle inputs required for run bundle context creation.

    Returns
    -------
    RunBundleContextInputs
        Run bundle context inputs.
    """
    return RunBundleContextInputs(
        output_config=output_config,
        relspec_rule_exec_events=relspec_rule_exec_events,
        relspec_scan_telemetry=relspec_scan_telemetry,
        feature_state_diagnostics_table=feature_state_diagnostics_table,
        ctx=ctx,
    )


@dataclass(frozen=True)
class _RunBundleDatafusionArtifacts:
    metrics: JsonDict | None
    traces: JsonDict | None
    fallbacks: pa.Table | None
    explains: pa.Table | None
    plan_artifacts: pa.Table | None
    plan_cache: Sequence[PlanCacheEntry] | None
    cache_events: Sequence[Mapping[str, object]] | None
    prepared_statements: Sequence[Mapping[str, object]] | None
    input_plugins: Sequence[Mapping[str, object]] | None
    arrow_ingest: Sequence[Mapping[str, object]] | None
    view_registry: Sequence[Mapping[str, object]] | None
    dml_statements: Sequence[Mapping[str, object]] | None
    function_factory: Sequence[Mapping[str, object]] | None
    expr_planners: Sequence[Mapping[str, object]] | None
    listing_tables: Sequence[Mapping[str, object]] | None
    listing_refresh_events: Sequence[Mapping[str, object]] | None
    delta_tables: Sequence[Mapping[str, object]] | None
    udf_registry: Sequence[Mapping[str, object]] | None
    schema_registry_validation: pa.Table | None
    table_providers: Sequence[Mapping[str, object]] | None
    function_catalog: Sequence[Mapping[str, object]] | None
    function_catalog_hash: str | None


@dataclass(frozen=True)
class _RunBundleParamValues:
    param_specs: tuple[ParamTableSpec, ...]
    param_scalar_specs: tuple[ScalarParamSpec, ...]
    param_artifacts: Mapping[str, ParamTableArtifact] | None
    param_scalar_signature: str | None
    param_reports: tuple[RuleDependencyReport, ...]
    param_reverse_index: Mapping[str, Sequence[str]] | None
    include_param_table_data: bool


def _run_bundle_datafusion_artifacts(
    ctx: ExecutionContext | None,
    *,
    output_dir: str | None,
    work_dir: str | None,
) -> _RunBundleDatafusionArtifacts:
    if ctx is None:
        return _RunBundleDatafusionArtifacts(
            metrics=None,
            traces=None,
            fallbacks=None,
            explains=None,
            plan_artifacts=None,
            plan_cache=None,
            cache_events=None,
            prepared_statements=None,
            input_plugins=None,
            arrow_ingest=None,
            view_registry=None,
            dml_statements=None,
            function_factory=None,
            expr_planners=None,
            listing_tables=None,
            listing_refresh_events=None,
            delta_tables=None,
            udf_registry=None,
            schema_registry_validation=None,
            table_providers=None,
            function_catalog=None,
            function_catalog_hash=None,
        )
    metrics, traces = _datafusion_runtime_artifacts(ctx)
    fallbacks, explains = _datafusion_runtime_diag_tables(
        ctx,
        output_dir=output_dir,
        work_dir=work_dir,
    )
    function_catalog, function_catalog_hash = _datafusion_function_catalog_snapshot(ctx)
    return _RunBundleDatafusionArtifacts(
        metrics=metrics,
        traces=traces,
        fallbacks=fallbacks,
        explains=explains,
        plan_artifacts=_datafusion_plan_artifacts_table(
            ctx,
            output_dir=output_dir,
            work_dir=work_dir,
        ),
        plan_cache=_datafusion_plan_cache_entries(ctx),
        cache_events=_datafusion_cache_events(ctx),
        prepared_statements=_datafusion_prepared_statements(ctx),
        input_plugins=_datafusion_input_plugins(ctx),
        arrow_ingest=_datafusion_arrow_ingest(ctx),
        view_registry=_datafusion_view_registry(ctx),
        dml_statements=_datafusion_dml_statements(ctx),
        function_factory=_datafusion_function_factory(ctx),
        expr_planners=_datafusion_expr_planners(ctx),
        listing_tables=_datafusion_listing_tables(ctx),
        listing_refresh_events=_datafusion_listing_refresh_events(ctx),
        delta_tables=_datafusion_delta_tables(ctx),
        udf_registry=_datafusion_udf_registry(ctx),
        schema_registry_validation=_datafusion_schema_registry_validation(ctx),
        table_providers=_datafusion_table_providers(ctx),
        function_catalog=function_catalog,
        function_catalog_hash=function_catalog_hash,
    )


def _run_bundle_param_values(
    param_inputs: RunBundleParamInputs | None,
) -> _RunBundleParamValues:
    if param_inputs is None:
        return _RunBundleParamValues(
            param_specs=(),
            param_scalar_specs=(),
            param_artifacts=None,
            param_scalar_signature=None,
            param_reports=(),
            param_reverse_index=None,
            include_param_table_data=False,
        )
    return _RunBundleParamValues(
        param_specs=param_inputs.param_table_specs,
        param_scalar_specs=param_inputs.param_scalar_specs,
        param_artifacts=param_inputs.param_table_artifacts,
        param_scalar_signature=param_inputs.param_scalar_signature,
        param_reports=param_inputs.param_dependency_reports,
        param_reverse_index=param_inputs.param_reverse_index,
        include_param_table_data=param_inputs.include_param_table_data,
    )


@tag(layer="obs", artifact="run_bundle_context", kind="object")
def run_bundle_context(
    run_bundle_inputs: RunBundleInputs,
    context_inputs: RunBundleContextInputs,
) -> RunBundleContext | None:
    """Build the run bundle context when outputs are enabled.

    Returns
    -------
    RunBundleContext | None
        Run bundle context, or None if outputs are disabled.
    """
    output_config = context_inputs.output_config
    base = output_config.output_dir or output_config.work_dir
    if not base:
        return None

    base_dir = Path(base) / "run_bundles"
    datafusion_artifacts = _run_bundle_datafusion_artifacts(
        context_inputs.ctx,
        output_dir=output_config.output_dir,
        work_dir=output_config.work_dir,
    )
    delta_maintenance_reports = (
        _delta_maintenance_reports(context_inputs.ctx) if context_inputs.ctx is not None else None
    )
    sql_ingest_artifacts = _ibis_sql_ingest_artifacts(context_inputs.ctx)
    namespace_actions = _ibis_namespace_actions(context_inputs.ctx)
    cache_events = _ibis_cache_events(context_inputs.ctx)
    support_matrix = _ibis_support_matrix(context_inputs.ctx)
    allocator_debug = bool(context_inputs.ctx.debug) if context_inputs.ctx is not None else False
    function_registry = default_function_registry()
    kernel_registry = kernel_registry_snapshot()
    datafusion_write_policy = (
        _datafusion_write_policy_snapshot(context_inputs.ctx)
        if context_inputs.ctx is not None
        else None
    )
    param_values = _run_bundle_param_values(run_bundle_inputs.param_inputs)
    snapshot = run_bundle_inputs.relspec_snapshots.registry_snapshot
    return RunBundleContext(
        base_dir=str(base_dir),
        run_manifest=run_bundle_inputs.run_manifest,
        run_config=run_bundle_inputs.run_config,
        rule_table=snapshot.rule_table,
        template_table=snapshot.template_table,
        template_diagnostics=snapshot.template_diagnostics,
        rule_diagnostics=snapshot.rule_diagnostics,
        relationship_contracts=run_bundle_inputs.relspec_snapshots.contracts,
        compiled_relationship_outputs=run_bundle_inputs.relspec_snapshots.compiled_outputs,
        datafusion_metrics=datafusion_artifacts.metrics,
        datafusion_traces=datafusion_artifacts.traces,
        datafusion_fallbacks=datafusion_artifacts.fallbacks,
        datafusion_explains=datafusion_artifacts.explains,
        datafusion_plan_artifacts=datafusion_artifacts.plan_artifacts,
        datafusion_plan_cache=datafusion_artifacts.plan_cache,
        datafusion_cache_events=datafusion_artifacts.cache_events,
        datafusion_prepared_statements=datafusion_artifacts.prepared_statements,
        datafusion_input_plugins=datafusion_artifacts.input_plugins,
        datafusion_arrow_ingest=datafusion_artifacts.arrow_ingest,
        datafusion_view_registry=datafusion_artifacts.view_registry,
        datafusion_dml_statements=datafusion_artifacts.dml_statements,
        datafusion_function_factory=datafusion_artifacts.function_factory,
        datafusion_expr_planners=datafusion_artifacts.expr_planners,
        datafusion_listing_tables=datafusion_artifacts.listing_tables,
        datafusion_listing_refreshes=datafusion_artifacts.listing_refresh_events,
        datafusion_delta_tables=datafusion_artifacts.delta_tables,
        delta_maintenance_reports=delta_maintenance_reports,
        datafusion_udf_registry=datafusion_artifacts.udf_registry,
        datafusion_schema_registry_validation=datafusion_artifacts.schema_registry_validation,
        datafusion_table_providers=datafusion_artifacts.table_providers,
        datafusion_function_catalog=datafusion_artifacts.function_catalog,
        datafusion_function_catalog_hash=datafusion_artifacts.function_catalog_hash,
        datafusion_write_policy=datafusion_write_policy,
        function_registry_snapshot=function_registry.payload(),
        function_registry_hash=function_registry.fingerprint(),
        arrow_kernel_registry=kernel_registry,
        ibis_sql_ingest_artifacts=sql_ingest_artifacts,
        ibis_namespace_actions=namespace_actions,
        ibis_cache_events=cache_events,
        ibis_support_matrix=support_matrix,
        feature_state=context_inputs.feature_state_diagnostics_table,
        ipc_dump_enabled=output_config.ipc_dump_enabled,
        ipc_write_config=output_config.ipc_write_config,
        allocator_debug=allocator_debug,
        relspec_scan_telemetry=cast("pa.Table", context_inputs.relspec_scan_telemetry),
        relspec_rule_exec_events=context_inputs.relspec_rule_exec_events,
        relspec_input_locations=run_bundle_inputs.tables.relspec_inputs.locations,
        relspec_input_tables=run_bundle_inputs.tables.relspec_inputs.tables,
        relationship_output_tables=run_bundle_inputs.tables.relationship_outputs.as_dict(),
        cpg_output_tables=run_bundle_inputs.tables.cpg_outputs.as_dict(),
        incremental_diff=run_bundle_inputs.incremental_diff,
        incremental_plan_diff=run_bundle_inputs.incremental_plan_diff,
        incremental_changed_exports=run_bundle_inputs.incremental_changed_exports,
        incremental_impacted_callers=run_bundle_inputs.incremental_impacted_callers,
        incremental_impacted_importers=run_bundle_inputs.incremental_impacted_importers,
        incremental_impacted_files=run_bundle_inputs.incremental_impacted_files,
        incremental_output_fingerprint_changes=(
            run_bundle_inputs.incremental_output_fingerprint_changes
        ),
        param_table_specs=param_values.param_specs,
        param_scalar_specs=param_values.param_scalar_specs,
        param_table_artifacts=param_values.param_artifacts,
        param_scalar_signature=param_values.param_scalar_signature,
        param_dependency_reports=param_values.param_reports,
        param_reverse_index=param_values.param_reverse_index,
        include_param_table_data=param_values.include_param_table_data,
        include_schemas=True,
        overwrite=bool(output_config.overwrite_intermediate_datasets),
    )


@tag(layer="obs", artifact="run_bundle", kind="side_effect")
def write_run_bundle_dir(run_bundle_context: RunBundleContext | None) -> JsonDict | None:
    """Write a reproducible run bundle directory.

    Directory:
      <base>/run_bundles/run_<ts>_<hash>/

    Includes:
      manifest + config + repro info + relspec snapshots + schema snapshots

    Returns
    -------
    JsonDict | None
        Bundle metadata or None when output is disabled.
    """
    if run_bundle_context is None:
        return None
    return write_run_bundle(context=run_bundle_context)

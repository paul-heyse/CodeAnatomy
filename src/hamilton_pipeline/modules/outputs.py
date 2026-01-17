"""Hamilton output and materialization nodes."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import cast

import pyarrow as pa
import pyarrow.dataset as ds
from hamilton.function_modifiers import tag

from arrowdsl.core.context import ExecutionContext
from arrowdsl.core.interop import RecordBatchReaderLike, TableLike
from arrowdsl.finalize.finalize import FinalizeResult
from arrowdsl.io.parquet import (
    NamedDatasetWriteConfig,
    ParquetWriteOptions,
    write_finalize_result_parquet,
    write_named_datasets_parquet,
    write_table_parquet,
)
from arrowdsl.plan.metrics import (
    column_stats_table,
    dataset_stats_table,
    empty_scan_telemetry_table,
    scan_telemetry_table,
)
from arrowdsl.plan.query import open_dataset
from arrowdsl.plan.scan_builder import ScanBuildSpec
from arrowdsl.plan.scan_telemetry import ScanTelemetry, fragment_telemetry
from arrowdsl.plan.schema_utils import plan_schema
from arrowdsl.schema.schema import EncodingPolicy
from core_types import JsonDict, JsonValue
from cpg.constants import CpgBuildArtifacts
from datafusion_engine.registry_bridge import cached_dataset_names
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
from ibis_engine.execution import IbisAdapterExecution
from ibis_engine.io_bridge import (
    IbisNamedDatasetWriteOptions,
    write_ibis_named_datasets_parquet,
)
from ibis_engine.param_tables import ParamTableArtifact, ParamTableSpec
from ibis_engine.plan import IbisPlan
from ibis_engine.registry import registry_snapshot
from incremental.types import IncrementalConfig
from normalize.runner import NormalizeRuleCompilation
from normalize.utils import encoding_policy_from_schema
from obs.diagnostics_tables import datafusion_explains_table, datafusion_fallbacks_table
from obs.manifest import ManifestContext, ManifestData, build_manifest, write_manifest_json
from obs.repro import RunBundleContext, write_run_bundle
from relspec.compiler import CompiledOutput
from relspec.model import RelationshipRule
from relspec.param_deps import RuleDependencyReport, build_param_reverse_index
from relspec.registry import ContractCatalog, DatasetCatalog, DatasetLocation
from relspec.rules.handlers.cpg import relationship_rule_from_definition
from relspec.rules.registry import RuleRegistry
from relspec.rules.spec_tables import rule_definitions_from_table
from schema_spec.system import dataset_spec_from_schema, make_dataset_spec

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
    cpg_nodes: TableLike, cpg_edges: TableLike, cpg_props: TableLike
) -> dict[str, TableLike]:
    """Bundle CPG tables into a dictionary.

    Returns
    -------
    dict[str, TableLike]
        Bundle of CPG nodes, edges, and properties.
    """
    return {"cpg_nodes": cpg_nodes, "cpg_edges": cpg_edges, "cpg_props": cpg_props}


# -----------------------
# Materialization helpers
# -----------------------


def _ensure_dir(path: Path) -> None:
    path.mkdir(exist_ok=True, parents=True)


def _datafusion_settings_snapshot(
    ctx: ExecutionContext,
) -> list[dict[str, str]] | None:
    profile = ctx.runtime.datafusion
    if profile is None or not profile.enable_information_schema:
        return None
    session = profile.session_context()
    table = profile.settings_snapshot(session)
    snapshot: list[dict[str, str]] = []
    for row in table.to_pylist():
        name = row.get("name")
        value = row.get("value")
        if name is None or value is None:
            continue
        snapshot.append({"name": str(name), "value": str(value)})
    return snapshot or None


def _datafusion_catalog_snapshot(
    ctx: ExecutionContext,
) -> list[dict[str, str]] | None:
    profile = ctx.runtime.datafusion
    if profile is None or not profile.enable_information_schema:
        return None
    session = profile.session_context()
    table = profile.catalog_snapshot(session)
    snapshot: list[dict[str, str]] = []
    for row in table.to_pylist():
        catalog = row.get("table_catalog")
        schema = row.get("table_schema")
        name = row.get("table_name")
        table_type = row.get("table_type")
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


def _json_mapping(payload: Mapping[str, object] | None) -> JsonDict | None:
    if payload is None:
        return None
    return cast("JsonDict", dict(payload))


def _datafusion_runtime_artifacts(
    ctx: ExecutionContext,
) -> tuple[JsonDict | None, JsonDict | None]:
    profile = ctx.runtime.datafusion
    if profile is None:
        return None, None
    metrics = _json_mapping(profile.collect_metrics())
    traces = _json_mapping(profile.collect_traces())
    return metrics, traces


def _datafusion_runtime_diag_tables(
    ctx: ExecutionContext,
) -> tuple[pa.Table | None, pa.Table | None]:
    profile = ctx.runtime.datafusion
    if profile is None:
        return None, None
    fallback_events: list[dict[str, object]] = []
    explain_events: list[dict[str, object]] = []
    if profile.fallback_collector is not None:
        fallback_events = profile.fallback_collector.snapshot()
    if profile.explain_collector is not None:
        for entry in profile.explain_collector.snapshot():
            payload = dict(entry)
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
class RunBundleInputs:
    """Inputs required to build a run bundle context."""

    run_manifest: JsonDict
    run_config: JsonDict
    relspec_snapshots: RelspecSnapshots
    tables: RunBundleTables
    param_inputs: RunBundleParamInputs | None = None
    incremental_diff: pa.Table | None = None


@dataclass(frozen=True)
class RunBundleParamInputs:
    """Parameter-table inputs for run bundle materialization."""

    param_table_specs: tuple[ParamTableSpec, ...]
    param_table_artifacts: Mapping[str, ParamTableArtifact] | None
    param_scalar_signature: str | None = None
    param_dependency_reports: tuple[RuleDependencyReport, ...] = ()
    param_reverse_index: Mapping[str, tuple[str, ...]] | None = None
    include_param_table_data: bool = False


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


@dataclass(frozen=True)
class ExtractManifestInputs:
    """Extract-specific inputs for manifest data."""

    evidence_plan: EvidencePlan | None = None
    extract_error_counts: Mapping[str, int] | None = None


# -----------------------
# Materializers: normalized datasets (relationship inputs)
# -----------------------


@tag(layer="materialize", artifact="normalized_inputs_parquet", kind="side_effect")
def write_normalized_inputs_parquet(
    relspec_input_datasets: dict[str, TableLike],
    output_dir: str | None,
    work_dir: str | None,
    ibis_execution: IbisAdapterExecution,
) -> JsonDict | None:
    """Write relationship-input normalized datasets for debugging.

    Output structure:
      <base>/debug/normalized_inputs/<dataset_name>/part-*.parquet

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

    schemas: dict[str, pa.Schema] = {}
    encoding_policies: dict[str, EncodingPolicy] = {}
    for name, table in input_datasets.items():
        if isinstance(table, RecordBatchReaderLike):
            continue
        schema = (
            plan_schema(table, ctx=ibis_execution.ctx)
            if isinstance(table, IbisPlan)
            else table.schema
        )
        schemas[name] = schema
        encoding_policies[name] = encoding_policy_from_schema(schema)
    config = NamedDatasetWriteConfig(
        opts=ParquetWriteOptions(),
        overwrite=True,
        schemas=schemas,
        encoding_policies=encoding_policies,
    )
    if any(isinstance(table, IbisPlan) for table in input_datasets.values()):
        paths = write_ibis_named_datasets_parquet(
            input_datasets,
            str(out_dir),
            options=IbisNamedDatasetWriteOptions(
                config=config,
                execution=ibis_execution,
            ),
        )
    else:
        paths = write_named_datasets_parquet(
            cast("dict[str, TableLike | RecordBatchReaderLike]", input_datasets),
            str(out_dir),
            config=config,
        )

    # Provide lightweight report
    datasets: dict[str, JsonDict] = {}
    report: JsonDict = {"base_dir": str(out_dir), "datasets": datasets}
    for name, t in input_datasets.items():
        path = paths.get(name)
        if isinstance(t, RecordBatchReaderLike):
            rows = int(ds.dataset(path, format="parquet").count_rows()) if path else None
            columns = len(t.schema.names)
        elif isinstance(t, IbisPlan):
            rows = int(ds.dataset(path, format="parquet").count_rows()) if path else None
            schema = plan_schema(t, ctx=ibis_execution.ctx)
            columns = len(schema.names)
        else:
            rows = int(t.num_rows)
            columns = len(t.column_names)
        datasets[name] = {
            "path": path,
            "rows": rows,
            "columns": columns,
        }
    return report


# -----------------------
# Materializers: final CPG tables (parquet files)
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


@tag(layer="materialize", artifact="cpg_nodes_parquet", kind="side_effect")
def write_cpg_nodes_parquet(
    output_dir: str | None,
    cpg_nodes_finalize: CpgBuildArtifacts,
    cpg_nodes_final: TableLike,
    incremental_config: IncrementalConfig,
) -> JsonDict | None:
    """Write CPG nodes and error artifacts to Parquet.

    Returns
    -------
    JsonDict | None
        Report of the written files, or None when output is disabled.
    """
    if not output_dir:
        return None
    output_path = Path(output_dir)
    _ensure_dir(output_path)
    good_override = cpg_nodes_final if incremental_config.enabled else None
    finalize = _override_finalize_good(cpg_nodes_finalize.finalize, good=good_override)
    paths = write_finalize_result_parquet(
        finalize,
        output_path / "cpg_nodes.parquet",
        opts=ParquetWriteOptions(),
        overwrite=True,
    )
    return {
        "paths": paths,
        "rows": int(finalize.good.num_rows),
        "error_rows": int(finalize.errors.num_rows),
    }


@tag(layer="materialize", artifact="cpg_nodes_quality_parquet", kind="side_effect")
def write_cpg_nodes_quality_parquet(
    output_dir: str | None,
    cpg_nodes_quality: TableLike,
) -> JsonDict | None:
    """Write CPG node quality diagnostics to Parquet.

    Returns
    -------
    JsonDict | None
        Report of the written file, or None when output is disabled.
    """
    if not output_dir:
        return None
    output_path = Path(output_dir)
    _ensure_dir(output_path)
    path = write_table_parquet(
        cpg_nodes_quality,
        output_path / "cpg_nodes_quality.parquet",
        opts=ParquetWriteOptions(),
        overwrite=True,
    )
    return {"path": path, "rows": int(cpg_nodes_quality.num_rows)}


@tag(layer="materialize", artifact="cpg_edges_parquet", kind="side_effect")
def write_cpg_edges_parquet(
    output_dir: str | None,
    cpg_edges_finalize: CpgBuildArtifacts,
    cpg_edges_final: TableLike,
    incremental_config: IncrementalConfig,
) -> JsonDict | None:
    """Write CPG edges and error artifacts to Parquet.

    Returns
    -------
    JsonDict | None
        Report of the written files, or None when output is disabled.
    """
    if not output_dir:
        return None
    output_path = Path(output_dir)
    _ensure_dir(output_path)
    good_override = cpg_edges_final if incremental_config.enabled else None
    finalize = _override_finalize_good(cpg_edges_finalize.finalize, good=good_override)
    paths = write_finalize_result_parquet(
        finalize,
        output_path / "cpg_edges.parquet",
        opts=ParquetWriteOptions(),
        overwrite=True,
    )
    return {
        "paths": paths,
        "rows": int(finalize.good.num_rows),
        "error_rows": int(finalize.errors.num_rows),
    }


@tag(layer="materialize", artifact="cpg_props_parquet", kind="side_effect")
def write_cpg_props_parquet(
    output_dir: str | None,
    cpg_props_finalize: CpgBuildArtifacts,
    cpg_props_final: TableLike,
    incremental_config: IncrementalConfig,
) -> JsonDict | None:
    """Write CPG properties and error artifacts to Parquet.

    Returns
    -------
    JsonDict | None
        Report of the written files, or None when output is disabled.
    """
    if not output_dir:
        return None
    output_path = Path(output_dir)
    _ensure_dir(output_path)
    good_override = cpg_props_final if incremental_config.enabled else None
    finalize = _override_finalize_good(cpg_props_finalize.finalize, good=good_override)
    paths = write_finalize_result_parquet(
        finalize,
        output_path / "cpg_props.parquet",
        opts=ParquetWriteOptions(),
        overwrite=True,
    )
    return {
        "paths": paths,
        "rows": int(finalize.good.num_rows),
        "error_rows": int(finalize.errors.num_rows),
    }


@tag(layer="materialize", artifact="cpg_props_quality_parquet", kind="side_effect")
def write_cpg_props_quality_parquet(
    output_dir: str | None,
    cpg_props_quality: TableLike,
) -> JsonDict | None:
    """Write CPG property quality diagnostics to Parquet.

    Returns
    -------
    JsonDict | None
        Report of the written file, or None when output is disabled.
    """
    if not output_dir:
        return None
    output_path = Path(output_dir)
    _ensure_dir(output_path)
    path = write_table_parquet(
        cpg_props_quality,
        output_path / "cpg_props_quality.parquet",
        opts=ParquetWriteOptions(),
        overwrite=True,
    )
    return {"path": path, "rows": int(cpg_props_quality.num_rows)}


# -----------------------
# Materializers: extract error artifacts
# -----------------------


@tag(layer="materialize", artifact="extract_errors_parquet", kind="side_effect")
def write_extract_error_artifacts_parquet(
    output_dir: str | None,
    extract_error_artifacts: ExtractErrorArtifacts,
) -> JsonDict | None:
    """Write extract error artifacts to Parquet.

    Output structure:
      <output_dir>/extract_errors/<output>/{errors,error_stats,alignment}.parquet

    Returns
    -------
    JsonDict | None
        Report of written error artifacts, or None when output is disabled.
    """
    if not output_dir:
        return None
    base = Path(output_dir) / "extract_errors"
    _ensure_dir(base)
    options = ParquetWriteOptions()
    datasets: dict[str, JsonDict] = {}
    for output in sorted(extract_error_artifacts.errors):
        errors = extract_error_artifacts.errors[output]
        stats = extract_error_artifacts.stats[output]
        alignment = extract_error_artifacts.alignment[output]
        dataset_dir = base / output
        _ensure_dir(dataset_dir)
        datasets[output] = {
            "paths": {
                "errors": write_table_parquet(
                    errors,
                    dataset_dir / "errors.parquet",
                    opts=options,
                    overwrite=True,
                ),
                "stats": write_table_parquet(
                    stats,
                    dataset_dir / "error_stats.parquet",
                    opts=options,
                    overwrite=True,
                ),
                "alignment": write_table_parquet(
                    alignment,
                    dataset_dir / "alignment.parquet",
                    opts=options,
                    overwrite=True,
                ),
            },
            "error_rows": int(errors.num_rows),
            "stat_rows": int(stats.num_rows),
            "alignment_rows": int(alignment.num_rows),
        }
    return {"base_dir": str(base), "datasets": datasets}


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
        dataset = open_dataset(
            loc.path,
            dataset_format=loc.format,
            filesystem=loc.filesystem,
            partitioning=loc.partitioning,
        )
        if loc.table_spec is not None:
            dataset_spec = make_dataset_spec(table_spec=loc.table_spec)
        else:
            dataset_spec = dataset_spec_from_schema(name=name, schema=dataset.schema)
        query = dataset_spec.query()
        scan_provenance = tuple(ctx.runtime.scan.scan_provenance_columns)
        scanner = ScanBuildSpec(
            dataset=dataset,
            query=query,
            ctx=ctx,
            scan_provenance=scan_provenance,
        ).scanner()
        telemetry = fragment_telemetry(
            dataset,
            predicate=query.pushdown_expression(),
            scanner=scanner,
        )
        rows.append(
            {
                "dataset": name,
                "fragment_count": int(telemetry.fragment_count),
                "row_group_count": int(telemetry.row_group_count),
                "count_rows": telemetry.count_rows,
                "estimated_rows": telemetry.estimated_rows,
                "file_hints": list(telemetry.file_hints),
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


@tag(layer="obs", artifact="cpg_output_tables", kind="bundle")
def cpg_output_tables(
    cpg_nodes: TableLike,
    cpg_edges: TableLike,
    cpg_props: TableLike,
) -> CpgOutputTables:
    """Bundle CPG output tables.

    Returns
    -------
    CpgOutputTables
        CPG output bundle.
    """
    return CpgOutputTables(cpg_nodes=cpg_nodes, cpg_edges=cpg_edges, cpg_props=cpg_props)


@tag(layer="obs", artifact="relspec_snapshots", kind="object")
def relspec_snapshots(
    rule_registry: RuleRegistry,
    relationship_contracts: ContractCatalog,
    compiled_relationship_outputs: dict[str, CompiledOutput],
) -> RelspecSnapshots:
    """Bundle relationship snapshots for observability.

    Returns
    -------
    RelspecSnapshots
        Relationship snapshot bundle.
    """
    return RelspecSnapshots(
        rule_table=rule_registry.rule_table(),
        template_table=rule_registry.template_table(),
        template_diagnostics=rule_registry.template_diagnostics_table(),
        rule_diagnostics=rule_registry.rule_diagnostics_table(),
        contracts=relationship_contracts,
        compiled_outputs=compiled_relationship_outputs,
    )


@tag(layer="obs", artifact="manifest_inputs", kind="object")
def manifest_inputs(
    run_bundle_tables: RunBundleTables,
    relspec_snapshots: RelspecSnapshots,
    param_table_artifacts: Mapping[str, ParamTableArtifact] | None = None,
    param_scalar_signature: str | None = None,
    extract_manifest_inputs: ExtractManifestInputs | None = None,
) -> ManifestInputs:
    """Bundle manifest inputs from pipeline outputs.

    Returns
    -------
    ManifestInputs
        Manifest input bundle.
    """
    return ManifestInputs(
        relspec_inputs_bundle=run_bundle_tables.relspec_inputs,
        relationship_output_tables=run_bundle_tables.relationship_outputs,
        cpg_output_tables=run_bundle_tables.cpg_outputs,
        relspec_snapshots=relspec_snapshots,
        param_table_artifacts=param_table_artifacts,
        param_scalar_signature=param_scalar_signature,
        extract_inputs=extract_manifest_inputs,
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
    normalize_rules = normalize_rule_compilation.rules if normalize_rule_compilation else None
    if ctx is None:
        msg = "manifest_data requires ExecutionContext."
        raise ValueError(msg)
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
    scan_telemetry = _relspec_scan_telemetry(manifest_inputs)
    datafusion_settings = _datafusion_settings_snapshot(ctx)
    datafusion_metrics, datafusion_traces = _datafusion_runtime_artifacts(ctx)
    dataset_snapshot = _dataset_registry_snapshot(
        manifest_inputs.relspec_inputs_bundle.locations,
    )
    extract_inputs = manifest_inputs.extract_inputs
    relationship_rules = _relationship_rules_from_snapshots(manifest_inputs)
    return ManifestData(
        relspec_input_tables=manifest_inputs.relspec_inputs_bundle.tables,
        relspec_input_locations=manifest_inputs.relspec_inputs_bundle.locations,
        relationship_outputs=manifest_inputs.relationship_output_tables.as_dict(),
        cpg_nodes=manifest_inputs.cpg_output_tables.cpg_nodes,
        cpg_edges=manifest_inputs.cpg_output_tables.cpg_edges,
        cpg_props=manifest_inputs.cpg_output_tables.cpg_props,
        param_table_artifacts=manifest_inputs.param_table_artifacts,
        param_scalar_signature=manifest_inputs.param_scalar_signature,
        extract_evidence_plan=extract_inputs.evidence_plan if extract_inputs else None,
        extract_error_counts=extract_inputs.extract_error_counts if extract_inputs else None,
        relationship_rules=relationship_rules,
        normalize_rules=normalize_rules,
        produced_relationship_output_names=produced_outputs,
        relationship_output_lineage=lineage,
        normalize_output_lineage=normalize_lineage,
        relspec_scan_telemetry=scan_telemetry or None,
        datafusion_settings=datafusion_settings,
        datafusion_metrics=datafusion_metrics,
        datafusion_traces=datafusion_traces,
        dataset_registry_snapshot=dataset_snapshot,
        notes=notes,
    )


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
    return notes


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


def _datafusion_notes(ctx: ExecutionContext) -> JsonDict:
    runtime = ctx.runtime.datafusion
    if runtime is None:
        return {}
    notes: JsonDict = {"datafusion_policy": cast("JsonValue", runtime.telemetry_payload())}
    catalog_snapshot = _datafusion_catalog_snapshot(ctx)
    if catalog_snapshot:
        notes["datafusion_catalog"] = cast("JsonValue", catalog_snapshot)
    cached = cached_dataset_names(runtime.session_context())
    if cached:
        notes["datafusion_cached_datasets"] = cast("JsonValue", list(cached))
    explain_collector = runtime.explain_collector
    if explain_collector is not None:
        explain_entries = explain_collector.snapshot()
        if explain_entries:
            notes["datafusion_explain"] = cast("JsonValue", explain_entries)
    fallback_collector = runtime.fallback_collector
    if fallback_collector is not None:
        fallback_entries = fallback_collector.snapshot()
        if fallback_entries:
            notes["datafusion_fallbacks"] = cast("JsonValue", fallback_entries)
    if runtime.labeled_explains:
        notes["datafusion_rule_explain"] = cast("JsonValue", list(runtime.labeled_explains))
    if runtime.labeled_fallbacks:
        notes["datafusion_rule_fallbacks"] = cast(
            "JsonValue",
            list(runtime.labeled_fallbacks),
        )
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
    rule_definitions = rule_definitions_from_table(manifest_inputs.relspec_snapshots.rule_table)
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


@tag(layer="obs", artifact="run_manifest_json", kind="side_effect")
def write_run_manifest_json(
    run_manifest: JsonDict,
    output_dir: str | None,
    work_dir: str | None,
) -> JsonDict | None:
    """Write run manifest JSON.

    Output location:
      - if output_dir set: <output_dir>/manifest.json
      - else if work_dir set: <work_dir>/manifest.json
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
    path = base_path / "manifest.json"
    write_manifest_json(run_manifest, str(path), overwrite=True)
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
        "relspec_param_values": dict(relspec_param_values),
        "incremental_enabled": bool(incremental_config.enabled),
        "incremental_state_dir": (
            str(incremental_config.state_dir) if incremental_config.state_dir else None
        ),
        "incremental_repo_id": incremental_config.repo_id,
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


@tag(layer="obs", artifact="run_bundle_inputs", kind="object")
def run_bundle_inputs(
    run_bundle_metadata: RunBundleMetadata,
    relspec_snapshots: RelspecSnapshots,
    run_bundle_tables: RunBundleTables,
    run_bundle_param_inputs: RunBundleParamInputs | None,
    incremental_diff: pa.Table | None,
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
        incremental_diff=incremental_diff,
    )


@tag(layer="obs", artifact="run_bundle_param_inputs", kind="object")
def run_bundle_param_inputs(
    param_table_specs: tuple[ParamTableSpec, ...],
    param_table_artifacts: Mapping[str, ParamTableArtifact] | None,
    param_scalar_signature: str | None,
    relspec_param_dependency_reports: tuple[RuleDependencyReport, ...],
    output_config: OutputConfig,
) -> RunBundleParamInputs:
    """Bundle param-table inputs for run bundle context creation.

    Returns
    -------
    RunBundleParamInputs
        Parameter-table inputs for run bundle context.
    """
    reverse_index = build_param_reverse_index(relspec_param_dependency_reports)
    return RunBundleParamInputs(
        param_table_specs=param_table_specs,
        param_table_artifacts=param_table_artifacts,
        param_scalar_signature=param_scalar_signature,
        param_dependency_reports=relspec_param_dependency_reports,
        param_reverse_index=reverse_index or None,
        include_param_table_data=output_config.materialize_param_tables,
    )


@tag(layer="obs", artifact="run_bundle_context", kind="object")
def run_bundle_context(
    run_bundle_inputs: RunBundleInputs,
    output_config: OutputConfig,
    relspec_rule_exec_events: pa.Table | None,
    relspec_scan_telemetry: TableLike,
    ctx: ExecutionContext | None = None,
) -> RunBundleContext | None:
    """Build the run bundle context when outputs are enabled.

    Returns
    -------
    RunBundleContext | None
        Run bundle context, or None if outputs are disabled.
    """
    base = output_config.output_dir or output_config.work_dir
    if not base:
        return None

    base_dir = Path(base) / "run_bundles"
    datafusion_metrics: JsonDict | None = None
    datafusion_traces: JsonDict | None = None
    datafusion_fallbacks: pa.Table | None = None
    datafusion_explains: pa.Table | None = None
    if ctx is not None:
        datafusion_metrics, datafusion_traces = _datafusion_runtime_artifacts(ctx)
        datafusion_fallbacks, datafusion_explains = _datafusion_runtime_diag_tables(ctx)
    param_inputs = run_bundle_inputs.param_inputs
    param_specs = param_inputs.param_table_specs if param_inputs is not None else ()
    param_artifacts = param_inputs.param_table_artifacts if param_inputs is not None else None
    param_scalar_signature = (
        param_inputs.param_scalar_signature if param_inputs is not None else None
    )
    param_reports = param_inputs.param_dependency_reports if param_inputs is not None else ()
    param_reverse_index = param_inputs.param_reverse_index if param_inputs is not None else None
    include_param_table_data = (
        param_inputs.include_param_table_data if param_inputs is not None else False
    )
    return RunBundleContext(
        base_dir=str(base_dir),
        run_manifest=run_bundle_inputs.run_manifest,
        run_config=run_bundle_inputs.run_config,
        rule_table=run_bundle_inputs.relspec_snapshots.rule_table,
        template_table=run_bundle_inputs.relspec_snapshots.template_table,
        template_diagnostics=run_bundle_inputs.relspec_snapshots.template_diagnostics,
        rule_diagnostics=run_bundle_inputs.relspec_snapshots.rule_diagnostics,
        relationship_contracts=run_bundle_inputs.relspec_snapshots.contracts,
        compiled_relationship_outputs=run_bundle_inputs.relspec_snapshots.compiled_outputs,
        datafusion_metrics=datafusion_metrics,
        datafusion_traces=datafusion_traces,
        datafusion_fallbacks=datafusion_fallbacks,
        datafusion_explains=datafusion_explains,
        relspec_scan_telemetry=cast("pa.Table", relspec_scan_telemetry),
        relspec_rule_exec_events=relspec_rule_exec_events,
        relspec_input_locations=run_bundle_inputs.tables.relspec_inputs.locations,
        relspec_input_tables=run_bundle_inputs.tables.relspec_inputs.tables,
        relationship_output_tables=run_bundle_inputs.tables.relationship_outputs.as_dict(),
        cpg_output_tables=run_bundle_inputs.tables.cpg_outputs.as_dict(),
        incremental_diff=run_bundle_inputs.incremental_diff,
        param_table_specs=param_specs,
        param_table_artifacts=param_artifacts,
        param_scalar_signature=param_scalar_signature,
        param_dependency_reports=param_reports,
        param_reverse_index=param_reverse_index,
        include_param_table_data=include_param_table_data,
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

"""Hamilton output and materialization nodes."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pyarrow as pa
import pyarrow.dataset as ds
from hamilton.function_modifiers import tag

from arrowdsl.core.context import ExecutionContext
from arrowdsl.core.interop import RecordBatchReaderLike, TableLike
from arrowdsl.plan.query import open_dataset
from core_types import JsonDict
from cpg.artifacts import CpgBuildArtifacts
from hamilton_pipeline.pipeline_types import (
    CpgOutputTables,
    OutputConfig,
    RelationshipOutputTables,
    RelspecConfig,
    RelspecInputsBundle,
    RelspecSnapshots,
    RepoScanConfig,
)
from normalize.encoding import encoding_policy_from_schema
from obs.manifest import ManifestContext, ManifestData, build_manifest, write_manifest_json
from obs.repro import RunBundleContext, write_run_bundle
from obs.stats import column_stats_table, dataset_stats_table
from relspec.compiler import CompiledOutput
from relspec.registry import ContractCatalog, DatasetLocation, RelationshipRegistry
from schema_spec.system import dataset_spec_from_schema, make_dataset_spec
from storage.parquet import (
    NamedDatasetWriteConfig,
    ParquetWriteOptions,
    write_finalize_result_parquet,
    write_named_datasets_parquet,
)

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
class RunBundleInputs:
    """Inputs required to build a run bundle context."""

    run_manifest: JsonDict
    run_config: JsonDict
    relspec_snapshots: RelspecSnapshots
    tables: RunBundleTables


# -----------------------
# Materializers: normalized datasets (relationship inputs)
# -----------------------


@tag(layer="materialize", artifact="normalized_inputs_parquet", kind="side_effect")
def write_normalized_inputs_parquet(
    relspec_input_datasets: dict[str, TableLike | RecordBatchReaderLike],
    output_dir: str | None,
    work_dir: str | None,
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
    base = _default_debug_dir(output_dir, work_dir)
    if not base:
        return None

    out_dir = base / "normalized_inputs"
    _ensure_dir(out_dir)

    schemas = {
        name: table.schema
        for name, table in relspec_input_datasets.items()
        if not isinstance(table, RecordBatchReaderLike)
    }
    encoding_policies = {
        name: encoding_policy_from_schema(table.schema)
        for name, table in relspec_input_datasets.items()
        if not isinstance(table, RecordBatchReaderLike)
    }
    paths = write_named_datasets_parquet(
        relspec_input_datasets,
        str(out_dir),
        config=NamedDatasetWriteConfig(
            opts=ParquetWriteOptions(),
            overwrite=True,
            schemas=schemas,
            encoding_policies=encoding_policies,
        ),
    )

    # Provide lightweight report
    datasets: dict[str, JsonDict] = {}
    report: JsonDict = {"base_dir": str(out_dir), "datasets": datasets}
    for name, t in relspec_input_datasets.items():
        path = paths.get(name)
        if isinstance(t, RecordBatchReaderLike):
            rows = int(ds.dataset(path, format="parquet").count_rows()) if path else None
            columns = len(t.schema.names)
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


@tag(layer="materialize", artifact="cpg_nodes_parquet", kind="side_effect")
def write_cpg_nodes_parquet(
    output_dir: str | None,
    cpg_nodes_finalize: CpgBuildArtifacts,
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
    paths = write_finalize_result_parquet(
        cpg_nodes_finalize.finalize,
        output_path / "cpg_nodes.parquet",
        opts=ParquetWriteOptions(),
        overwrite=True,
    )
    return {
        "paths": paths,
        "rows": int(cpg_nodes_finalize.finalize.good.num_rows),
        "error_rows": int(cpg_nodes_finalize.finalize.errors.num_rows),
    }


@tag(layer="materialize", artifact="cpg_edges_parquet", kind="side_effect")
def write_cpg_edges_parquet(
    output_dir: str | None,
    cpg_edges_finalize: CpgBuildArtifacts,
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
    paths = write_finalize_result_parquet(
        cpg_edges_finalize.finalize,
        output_path / "cpg_edges.parquet",
        opts=ParquetWriteOptions(),
        overwrite=True,
    )
    return {
        "paths": paths,
        "rows": int(cpg_edges_finalize.finalize.good.num_rows),
        "error_rows": int(cpg_edges_finalize.finalize.errors.num_rows),
    }


@tag(layer="materialize", artifact="cpg_props_parquet", kind="side_effect")
def write_cpg_props_parquet(
    output_dir: str | None,
    cpg_props_finalize: CpgBuildArtifacts,
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
    paths = write_finalize_result_parquet(
        cpg_props_finalize.finalize,
        output_path / "cpg_props.parquet",
        opts=ParquetWriteOptions(),
        overwrite=True,
    )
    return {
        "paths": paths,
        "rows": int(cpg_props_finalize.finalize.good.num_rows),
        "error_rows": int(cpg_props_finalize.finalize.errors.num_rows),
    }


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
        return pa.Table.from_arrays(
            [
                pa.array([], type=pa.string()),
                pa.array([], type=pa.int64()),
                pa.array([], type=pa.int64()),
            ],
            names=["dataset", "fragment_count", "estimated_rows"],
        )

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
        scan_ctx = dataset_spec.scan_context(dataset, ctx)
        telemetry = scan_ctx.telemetry()
        rows.append(
            {
                "dataset": name,
                "fragment_count": int(telemetry.fragment_count),
                "estimated_rows": telemetry.estimated_rows,
            }
        )
    return pa.Table.from_pylist(rows)


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
    relationship_registry: RelationshipRegistry,
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
        registry=relationship_registry,
        contracts=relationship_contracts,
        compiled_outputs=compiled_relationship_outputs,
    )


@tag(layer="obs", artifact="manifest_data", kind="object")
def manifest_data(
    relspec_inputs_bundle: RelspecInputsBundle,
    relationship_output_tables: RelationshipOutputTables,
    cpg_output_tables: CpgOutputTables,
    relspec_snapshots: RelspecSnapshots,
) -> ManifestData:
    """Assemble manifest data inputs from pipeline outputs.

    Returns
    -------
    ManifestData
        Manifest input bundle.
    """
    produced_outputs = sorted(relspec_snapshots.compiled_outputs.keys())
    return ManifestData(
        relspec_input_tables=relspec_inputs_bundle.tables,
        relspec_input_locations=relspec_inputs_bundle.locations,
        relationship_outputs=relationship_output_tables.as_dict(),
        cpg_nodes=cpg_output_tables.cpg_nodes,
        cpg_edges=cpg_output_tables.cpg_edges,
        cpg_props=cpg_output_tables.cpg_props,
        relationship_rules=relspec_snapshots.registry.rules(),
        produced_relationship_output_names=produced_outputs,
        notes={"relationship_output_keys": produced_outputs},
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
    }


@tag(layer="obs", artifact="run_bundle_tables", kind="bundle")
def run_bundle_tables(
    relspec_inputs_bundle: RelspecInputsBundle,
    relationship_output_tables: RelationshipOutputTables,
    cpg_output_tables: CpgOutputTables,
) -> RunBundleTables:
    """Bundle tables for run bundle writing.

    Returns
    -------
    RunBundleTables
        Table bundle for run bundles.
    """
    return RunBundleTables(
        relspec_inputs=relspec_inputs_bundle,
        relationship_outputs=relationship_output_tables,
        cpg_outputs=cpg_output_tables,
    )


@tag(layer="obs", artifact="run_bundle_inputs", kind="object")
def run_bundle_inputs(
    run_manifest: JsonDict,
    run_config: JsonDict,
    relspec_snapshots: RelspecSnapshots,
    run_bundle_tables: RunBundleTables,
) -> RunBundleInputs:
    """Bundle inputs for run bundle context creation.

    Returns
    -------
    RunBundleInputs
        Inputs used to build a run bundle context.
    """
    return RunBundleInputs(
        run_manifest=run_manifest,
        run_config=run_config,
        relspec_snapshots=relspec_snapshots,
        tables=run_bundle_tables,
    )


@tag(layer="obs", artifact="run_bundle_context", kind="object")
def run_bundle_context(
    run_bundle_inputs: RunBundleInputs,
    output_config: OutputConfig,
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
    return RunBundleContext(
        base_dir=str(base_dir),
        run_manifest=run_bundle_inputs.run_manifest,
        run_config=run_bundle_inputs.run_config,
        relationship_registry=run_bundle_inputs.relspec_snapshots.registry,
        relationship_contracts=run_bundle_inputs.relspec_snapshots.contracts,
        compiled_relationship_outputs=run_bundle_inputs.relspec_snapshots.compiled_outputs,
        relspec_input_locations=run_bundle_inputs.tables.relspec_inputs.locations,
        relspec_input_tables=run_bundle_inputs.tables.relspec_inputs.tables,
        relationship_output_tables=run_bundle_inputs.tables.relationship_outputs.as_dict(),
        cpg_output_tables=run_bundle_inputs.tables.cpg_outputs.as_dict(),
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

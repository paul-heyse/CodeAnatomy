from __future__ import annotations

import os
from typing import Any, Dict, Optional

import pyarrow as pa
import pyarrow.parquet as pq
from hamilton.function_modifiers import tag

from ...obs.stats import dataset_stats_table, column_stats_table
from ...obs.manifest import build_manifest, write_manifest_json
from ...storage.parquet import ParquetWriteOptions, write_named_datasets_parquet
from ...obs.repro import write_run_bundle



# -----------------------
# Public CPG outputs
# -----------------------

@tag(layer="outputs", artifact="cpg_nodes", kind="table")
def cpg_nodes(cpg_nodes_final: pa.Table) -> pa.Table:
    return cpg_nodes_final


@tag(layer="outputs", artifact="cpg_edges", kind="table")
def cpg_edges(cpg_edges_final: pa.Table) -> pa.Table:
    return cpg_edges_final


@tag(layer="outputs", artifact="cpg_props", kind="table")
def cpg_props(cpg_props_final: pa.Table) -> pa.Table:
    return cpg_props_final


@tag(layer="outputs", artifact="cpg_bundle", kind="bundle")
def cpg_bundle(cpg_nodes: pa.Table, cpg_edges: pa.Table, cpg_props: pa.Table) -> Dict[str, pa.Table]:
    return {"cpg_nodes": cpg_nodes, "cpg_edges": cpg_edges, "cpg_props": cpg_props}


# -----------------------
# Materialization helpers
# -----------------------

def _ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def _default_debug_dir(output_dir: Optional[str], work_dir: Optional[str]) -> Optional[str]:
    base = output_dir or work_dir
    if not base:
        return None
    p = os.path.join(base, "debug")
    _ensure_dir(p)
    return p


# -----------------------
# Materializers: normalized datasets (relationship inputs)
# -----------------------

@tag(layer="materialize", artifact="normalized_inputs_parquet", kind="side_effect")
def write_normalized_inputs_parquet(
    relspec_input_datasets: Dict[str, pa.Table],
    output_dir: Optional[str],
    work_dir: Optional[str],
) -> Optional[Dict[str, Any]]:
    """
    Writes relationship-input normalized datasets for debugging.

    Output structure:
      <base>/debug/normalized_inputs/<dataset_name>/part-*.parquet

    Request this node explicitly when you want to inspect intermediate tables.
    """
    base = _default_debug_dir(output_dir, work_dir)
    if not base:
        return None

    out_dir = os.path.join(base, "normalized_inputs")
    _ensure_dir(out_dir)

    paths = write_named_datasets_parquet(
        relspec_input_datasets,
        out_dir,
        opts=ParquetWriteOptions(),
        overwrite=True,
    )

    # Provide lightweight report
    report: Dict[str, Any] = {"base_dir": out_dir, "datasets": {}}
    for name, t in relspec_input_datasets.items():
        report["datasets"][name] = {
            "path": paths.get(name),
            "rows": int(t.num_rows),
            "columns": int(len(t.column_names)),
        }
    return report


# -----------------------
# Materializers: final CPG tables (parquet files)
# -----------------------

@tag(layer="materialize", artifact="cpg_nodes_parquet", kind="side_effect")
def write_cpg_nodes_parquet(output_dir: Optional[str], cpg_nodes: pa.Table) -> Optional[Dict[str, Any]]:
    if not output_dir:
        return None
    _ensure_dir(output_dir)
    path = os.path.join(output_dir, "cpg_nodes.parquet")
    pq.write_table(cpg_nodes, path)
    return {"path": path, "rows": int(cpg_nodes.num_rows)}


@tag(layer="materialize", artifact="cpg_edges_parquet", kind="side_effect")
def write_cpg_edges_parquet(output_dir: Optional[str], cpg_edges: pa.Table) -> Optional[Dict[str, Any]]:
    if not output_dir:
        return None
    _ensure_dir(output_dir)
    path = os.path.join(output_dir, "cpg_edges.parquet")
    pq.write_table(cpg_edges, path)
    return {"path": path, "rows": int(cpg_edges.num_rows)}


@tag(layer="materialize", artifact="cpg_props_parquet", kind="side_effect")
def write_cpg_props_parquet(output_dir: Optional[str], cpg_props: pa.Table) -> Optional[Dict[str, Any]]:
    if not output_dir:
        return None
    _ensure_dir(output_dir)
    path = os.path.join(output_dir, "cpg_props.parquet")
    pq.write_table(cpg_props, path)
    return {"path": path, "rows": int(cpg_props.num_rows)}


# -----------------------
# Debug stats for normalized datasets
# -----------------------

@tag(layer="obs", artifact="relspec_input_dataset_stats", kind="table")
def relspec_input_dataset_stats(relspec_input_datasets: Dict[str, pa.Table]) -> pa.Table:
    return dataset_stats_table(relspec_input_datasets)


@tag(layer="obs", artifact="relspec_input_column_stats", kind="table")
def relspec_input_column_stats(relspec_input_datasets: Dict[str, pa.Table]) -> pa.Table:
    return column_stats_table(relspec_input_datasets)


# -----------------------
# Manifest nodes
# -----------------------

@tag(layer="obs", artifact="run_manifest", kind="object")
def run_manifest(
    repo_root: str,
    relspec_mode: str,
    work_dir: Optional[str],
    output_dir: Optional[str],

    # inputs + persisted locations (filesystem mode)
    relspec_input_datasets: Dict[str, pa.Table],
    persist_relspec_input_datasets: Dict[str, Any],

    # relationship rule metadata
    relationship_registry: Any,
    compiled_relationship_outputs: Dict[str, Any],

    # relationship output tables (already finalized)
    rel_name_symbol: pa.Table,
    rel_import_symbol: pa.Table,
    rel_callsite_symbol: pa.Table,
    rel_callsite_qname: pa.Table,

    # final outputs
    cpg_nodes: pa.Table,
    cpg_edges: pa.Table,
    cpg_props: pa.Table,
) -> Dict[str, Any]:
    """
    Produces a structured manifest dict.

    Records:
      - dataset paths (from persist_relspec_input_datasets when mode=filesystem)
      - schema fingerprints + row counts (from tables)
      - rule outputs produced (keys of compiled_relationship_outputs)
    """
    relationship_outputs = {
        "rel_name_symbol": rel_name_symbol,
        "rel_import_symbol": rel_import_symbol,
        "rel_callsite_symbol": rel_callsite_symbol,
        "rel_callsite_qname": rel_callsite_qname,
    }

    rules = relationship_registry.rules() if hasattr(relationship_registry, "rules") else []

    manifest = build_manifest(
        repo_root=repo_root,
        relspec_mode=relspec_mode,
        work_dir=work_dir,
        output_dir=output_dir,
        relspec_input_tables=relspec_input_datasets,
        relspec_input_locations=persist_relspec_input_datasets,
        relationship_outputs=relationship_outputs,
        cpg_nodes=cpg_nodes,
        cpg_edges=cpg_edges,
        cpg_props=cpg_props,
        relationship_rules=rules,
        produced_relationship_output_names=sorted(list(compiled_relationship_outputs.keys())),
        notes={
            "relationship_output_keys": sorted(list(compiled_relationship_outputs.keys())),
        },
    )
    return manifest.to_dict()


@tag(layer="obs", artifact="run_manifest_json", kind="side_effect")
def write_run_manifest_json(
    run_manifest: Dict[str, Any],
    output_dir: Optional[str],
    work_dir: Optional[str],
) -> Optional[Dict[str, Any]]:
    """
    Writes run manifest JSON.

    Output location:
      - if output_dir set: <output_dir>/manifest.json
      - else if work_dir set: <work_dir>/manifest.json
      - else: None (no-op)
    """
    base = output_dir or work_dir
    if not base:
        return None
    _ensure_dir(base)
    path = os.path.join(base, "manifest.json")
    write_manifest_json(run_manifest, path, overwrite=True)
    return {"path": path}

@tag(layer="obs", artifact="run_config", kind="object")
def run_config(
    repo_root: str,
    scip_index_path: Optional[str],
    include_globs: list[str],
    exclude_globs: list[str],
    max_files: int,
    relspec_mode: str,
    work_dir: Optional[str],
    output_dir: Optional[str],
    overwrite_intermediate_datasets: bool,
) -> Dict[str, Any]:
    """
    The *execution-relevant* config snapshot for this run.
    This is what makes a run bundle re-playable.
    """
    return {
        "repo_root": repo_root,
        "scip_index_path": scip_index_path,
        "include_globs": list(include_globs),
        "exclude_globs": list(exclude_globs),
        "max_files": int(max_files),
        "relspec_mode": relspec_mode,
        "work_dir": work_dir,
        "output_dir": output_dir,
        "overwrite_intermediate_datasets": bool(overwrite_intermediate_datasets),
    }


@tag(layer="obs", artifact="run_bundle", kind="side_effect")
def write_run_bundle_dir(
    run_manifest: Dict[str, Any],
    run_config: Dict[str, Any],

    # relationship inputs + persisted locations
    relspec_input_datasets: Dict[str, pa.Table],
    persist_relspec_input_datasets: Dict[str, Any],

    # registry + contracts + compiled outputs snapshots
    relationship_registry: Any,
    relationship_contracts: Any,
    compiled_relationship_outputs: Dict[str, Any],

    # relationship output tables
    rel_name_symbol: pa.Table,
    rel_import_symbol: pa.Table,
    rel_callsite_symbol: pa.Table,
    rel_callsite_qname: pa.Table,

    # final outputs
    cpg_nodes: pa.Table,
    cpg_edges: pa.Table,
    cpg_props: pa.Table,

    # base dirs
    output_dir: Optional[str],
    work_dir: Optional[str],
) -> Optional[Dict[str, Any]]:
    """
    Writes a reproducible run bundle dir:
      <base>/run_bundles/run_<ts>_<hash>/

    Includes:
      manifest + config + repro info + relspec snapshots + schema snapshots
    """
    base = output_dir or work_dir
    if not base:
        return None

    base_dir = os.path.join(base, "run_bundles")
    os.makedirs(base_dir, exist_ok=True)

    relationship_outputs = {
        "rel_name_symbol": rel_name_symbol,
        "rel_import_symbol": rel_import_symbol,
        "rel_callsite_symbol": rel_callsite_symbol,
        "rel_callsite_qname": rel_callsite_qname,
    }
    cpg_outputs = {
        "cpg_nodes": cpg_nodes,
        "cpg_edges": cpg_edges,
        "cpg_props": cpg_props,
    }

    return write_run_bundle(
        base_dir=base_dir,
        run_manifest=run_manifest,
        run_config=run_config,
        relationship_registry=relationship_registry,
        relationship_contracts=relationship_contracts,
        compiled_relationship_outputs=compiled_relationship_outputs,
        relspec_input_locations=persist_relspec_input_datasets,
        relspec_input_tables=relspec_input_datasets,
        relationship_output_tables=relationship_outputs,
        cpg_output_tables=cpg_outputs,
        include_schemas=True,
        overwrite=True,
    )

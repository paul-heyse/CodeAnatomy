"""Simplified Hamilton output nodes for inference-driven pipeline."""

from __future__ import annotations

from pathlib import Path

from hamilton.function_modifiers import tag

from arrowdsl.core.interop import TableLike
from core_types import JsonDict


def _rows(table: TableLike) -> int:
    value = getattr(table, "num_rows", 0)
    if isinstance(value, bool):
        return 0
    if isinstance(value, int):
        return value
    return 0


def _finalize_stub(output_dir: str, name: str, *, rows: int, error_rows: int = 0) -> JsonDict:
    base = Path(output_dir) / name
    paths = {
        "data": str(base / "data"),
        "errors": str(base / "errors"),
        "stats": str(base / "stats"),
        "alignment": str(base / "alignment"),
    }
    return {
        "paths": paths,
        "rows": rows,
        "error_rows": error_rows,
    }


def _table_stub(output_dir: str, name: str, *, rows: int) -> JsonDict:
    base = Path(output_dir) / name
    return {"path": str(base), "rows": rows}


@tag(layer="outputs", artifact="cpg_nodes", kind="table")
def cpg_nodes(cpg_nodes_final: TableLike) -> TableLike:
    """Return the final CPG nodes table.

    Returns
    -------
    TableLike
        Final nodes table.
    """
    return cpg_nodes_final


@tag(layer="outputs", artifact="cpg_edges", kind="table")
def cpg_edges(cpg_edges_final: TableLike) -> TableLike:
    """Return the final CPG edges table.

    Returns
    -------
    TableLike
        Final edges table.
    """
    return cpg_edges_final


@tag(layer="outputs", artifact="cpg_props", kind="table")
def cpg_props(cpg_props_final: TableLike) -> TableLike:
    """Return the final CPG properties table.

    Returns
    -------
    TableLike
        Final properties table.
    """
    return cpg_props_final


@tag(layer="outputs", artifact="write_cpg_nodes_delta", kind="delta")
def write_cpg_nodes_delta(cpg_nodes: TableLike, output_dir: str) -> JsonDict:
    """Return stub metadata for CPG nodes output.

    Returns
    -------
    JsonDict
        Stub metadata payload.
    """
    return _finalize_stub(output_dir, "cpg_nodes", rows=_rows(cpg_nodes))


@tag(layer="outputs", artifact="write_cpg_edges_delta", kind="delta")
def write_cpg_edges_delta(cpg_edges: TableLike, output_dir: str) -> JsonDict:
    """Return stub metadata for CPG edges output.

    Returns
    -------
    JsonDict
        Stub metadata payload.
    """
    return _finalize_stub(output_dir, "cpg_edges", rows=_rows(cpg_edges))


@tag(layer="outputs", artifact="write_cpg_props_delta", kind="delta")
def write_cpg_props_delta(cpg_props: TableLike, output_dir: str) -> JsonDict:
    """Return stub metadata for CPG properties output.

    Returns
    -------
    JsonDict
        Stub metadata payload.
    """
    return _finalize_stub(output_dir, "cpg_props", rows=_rows(cpg_props))


@tag(layer="outputs", artifact="write_cpg_nodes_quality_delta", kind="delta")
def write_cpg_nodes_quality_delta(cpg_nodes: TableLike, output_dir: str) -> JsonDict:
    """Return stub metadata for CPG node quality output.

    Returns
    -------
    JsonDict
        Stub metadata payload.
    """
    return _table_stub(output_dir, "cpg_nodes_quality", rows=_rows(cpg_nodes))


@tag(layer="outputs", artifact="write_cpg_props_quality_delta", kind="delta")
def write_cpg_props_quality_delta(cpg_props: TableLike, output_dir: str) -> JsonDict:
    """Return stub metadata for CPG prop quality output.

    Returns
    -------
    JsonDict
        Stub metadata payload.
    """
    return _table_stub(output_dir, "cpg_props_quality", rows=_rows(cpg_props))


@tag(layer="outputs", artifact="write_cpg_props_json_delta", kind="delta")
def write_cpg_props_json_delta(cpg_props: TableLike, output_dir: str) -> JsonDict:
    """Return stub metadata for CPG props JSON output.

    Returns
    -------
    JsonDict
        Stub metadata payload.
    """
    return _table_stub(output_dir, "cpg_props_json", rows=_rows(cpg_props))


@tag(layer="outputs", artifact="write_normalize_outputs_delta", kind="delta")
def write_normalize_outputs_delta(output_dir: str) -> JsonDict:
    """Return stub metadata for normalize outputs.

    Returns
    -------
    JsonDict
        Stub metadata payload.
    """
    return _table_stub(output_dir, "normalize_outputs", rows=0)


@tag(layer="outputs", artifact="write_extract_error_artifacts_delta", kind="delta")
def write_extract_error_artifacts_delta() -> JsonDict:
    """Return stub metadata for extract error artifacts.

    Returns
    -------
    JsonDict
        Stub metadata payload.
    """
    return {"errors": []}


@tag(layer="outputs", artifact="write_run_manifest_delta", kind="delta")
def write_run_manifest_delta(output_dir: str) -> JsonDict:
    """Return stub metadata for run manifest.

    Returns
    -------
    JsonDict
        Stub metadata payload.
    """
    path = Path(output_dir) / "manifest.json"
    return {"path": str(path)}


@tag(layer="outputs", artifact="write_run_bundle_dir", kind="bundle")
def write_run_bundle_dir(output_dir: str) -> JsonDict:
    """Return stub metadata for run bundle directory.

    Returns
    -------
    JsonDict
        Stub metadata payload.
    """
    bundle_dir = Path(output_dir) / "run_bundle"
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

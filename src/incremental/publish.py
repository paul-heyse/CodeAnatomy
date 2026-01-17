"""Helpers for publishing incremental state into full datasets."""

from __future__ import annotations

from pathlib import Path
from typing import cast

import pyarrow as pa

from arrowdsl.core.interop import TableLike
from arrowdsl.plan.query import open_dataset
from arrowdsl.schema.schema import align_table, empty_table
from cpg.schemas import (
    CPG_EDGES_SCHEMA,
    CPG_NODES_SCHEMA,
    CPG_PROPS_BY_FILE_ID_SCHEMA,
    CPG_PROPS_GLOBAL_SCHEMA,
    CPG_PROPS_SCHEMA,
)
from incremental.invalidations import validate_schema_identity
from incremental.state_store import StateStore

_CPG_NODES_DATASET = "cpg_nodes_v1"
_CPG_EDGES_DATASET = "cpg_edges_v1"
_CPG_PROPS_BY_FILE_DATASET = "cpg_props_by_file_id_v1"
_CPG_PROPS_GLOBAL_DATASET = "cpg_props_global_v1"


def publish_cpg_nodes(
    *,
    state_store: StateStore,
    dataset_path: str | None = None,
    fallback: TableLike | None = None,
) -> TableLike:
    """Return the published CPG nodes table.

    Returns
    -------
    TableLike
        Published CPG nodes table.
    """
    path = _dataset_path(state_store, _CPG_NODES_DATASET, dataset_path)
    return _read_state_dataset(path, schema=CPG_NODES_SCHEMA, fallback=fallback)


def publish_cpg_edges(
    *,
    state_store: StateStore,
    dataset_path: str | None = None,
    fallback: TableLike | None = None,
) -> TableLike:
    """Return the published CPG edges table.

    Returns
    -------
    TableLike
        Published CPG edges table.
    """
    path = _dataset_path(state_store, _CPG_EDGES_DATASET, dataset_path)
    return _read_state_dataset(path, schema=CPG_EDGES_SCHEMA, fallback=fallback)


def publish_cpg_props(
    *,
    state_store: StateStore,
    by_file_path: str | None = None,
    global_path: str | None = None,
    fallback: TableLike | None = None,
) -> TableLike:
    """Return the published CPG properties table.

    Returns
    -------
    TableLike
        Published CPG properties table.
    """
    by_file_dataset = _dataset_path(
        state_store,
        _CPG_PROPS_BY_FILE_DATASET,
        by_file_path,
    )
    global_dataset = _dataset_path(
        state_store,
        _CPG_PROPS_GLOBAL_DATASET,
        global_path,
    )
    by_file_table = _read_state_dataset_optional(
        by_file_dataset,
        schema=CPG_PROPS_BY_FILE_ID_SCHEMA,
    )
    global_table = _read_state_dataset_optional(
        global_dataset,
        schema=CPG_PROPS_GLOBAL_SCHEMA,
    )

    parts: list[pa.Table] = []
    if by_file_table is not None:
        if "file_id" in by_file_table.column_names:
            by_file_table = by_file_table.drop(["file_id"])
        parts.append(
            align_table(by_file_table, schema=CPG_PROPS_SCHEMA, safe_cast=True)
        )
    if global_table is not None:
        parts.append(
            align_table(global_table, schema=CPG_PROPS_SCHEMA, safe_cast=True)
        )

    if not parts:
        if fallback is not None:
            return fallback
        return empty_table(CPG_PROPS_SCHEMA)

    return cast(
        "pa.Table",
        pa.concat_tables(parts, promote_options="default"),
    )


def _dataset_path(
    state_store: StateStore,
    dataset_name: str,
    dataset_path: str | None,
) -> Path:
    if dataset_path:
        return Path(dataset_path)
    return state_store.dataset_dir(dataset_name)


def _read_state_dataset_optional(path: Path, *, schema: pa.Schema) -> pa.Table | None:
    if not path.exists():
        return None
    dataset = open_dataset(path, schema=schema, partitioning="hive")
    validate_schema_identity(
        expected=schema,
        actual=dataset.schema,
        dataset_name=path.name,
    )
    return cast("pa.Table", dataset.to_table())


def _read_state_dataset(
    path: Path,
    *,
    schema: pa.Schema,
    fallback: TableLike | None,
) -> TableLike:
    table = _read_state_dataset_optional(path, schema=schema)
    if table is not None:
        return table
    if fallback is not None:
        return fallback
    return empty_table(schema)


__all__ = ["publish_cpg_edges", "publish_cpg_nodes", "publish_cpg_props"]

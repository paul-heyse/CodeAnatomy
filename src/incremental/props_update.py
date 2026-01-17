"""Incremental CPG property updates."""

from __future__ import annotations

from collections.abc import Sequence
from typing import cast

import pyarrow as pa

from arrowdsl.core.interop import RecordBatchReaderLike, TableLike, pc
from arrowdsl.io.parquet import (
    DatasetWriteConfig,
    upsert_dataset_partitions_parquet,
    write_dataset_parquet,
)
from arrowdsl.schema.build import table_from_schema
from arrowdsl.schema.metadata import encoding_policy_from_schema
from arrowdsl.schema.schema import align_table
from cpg.schemas import (
    CPG_PROPS_BY_FILE_ID_SCHEMA,
    CPG_PROPS_GLOBAL_SCHEMA,
)
from incremental.state_store import StateStore
from incremental.types import IncrementalFileChanges

_PROPS_BY_FILE_DATASET = "cpg_props_by_file_id_v1"
_PROPS_GLOBAL_DATASET = "cpg_props_global_v1"
_NODE_KIND = "node"
_EDGE_KIND = "edge"


def upsert_cpg_props(
    props: TableLike | RecordBatchReaderLike,
    *,
    cpg_nodes: TableLike | RecordBatchReaderLike,
    cpg_edges: TableLike | RecordBatchReaderLike,
    state_store: StateStore,
    changes: IncrementalFileChanges,
) -> dict[str, str]:
    """Upsert CPG properties into the incremental state store.

    Returns
    -------
    dict[str, str]
        Mapping of dataset name to dataset path.
    """
    props_by_file, props_global = split_props_by_file_id(
        props,
        cpg_nodes=cpg_nodes,
        cpg_edges=cpg_edges,
    )
    delete_partitions = _partition_specs("file_id", changes.deleted_file_ids)
    updated: dict[str, str] = {}
    path = upsert_dataset_partitions_parquet(
        props_by_file,
        base_dir=state_store.dataset_dir(_PROPS_BY_FILE_DATASET),
        partition_cols=("file_id",),
        delete_partitions=delete_partitions,
        config=DatasetWriteConfig(
            schema=CPG_PROPS_BY_FILE_ID_SCHEMA,
            encoding_policy=encoding_policy_from_schema(CPG_PROPS_BY_FILE_ID_SCHEMA),
        ),
    )
    updated[_PROPS_BY_FILE_DATASET] = path

    if changes.full_refresh:
        global_path = write_dataset_parquet(
            props_global,
            state_store.dataset_dir(_PROPS_GLOBAL_DATASET),
            config=DatasetWriteConfig(
                schema=CPG_PROPS_GLOBAL_SCHEMA,
                encoding_policy=encoding_policy_from_schema(CPG_PROPS_GLOBAL_SCHEMA),
            ),
        )
        updated[_PROPS_GLOBAL_DATASET] = global_path

    return updated


def split_props_by_file_id(
    props: TableLike | RecordBatchReaderLike,
    *,
    cpg_nodes: TableLike | RecordBatchReaderLike,
    cpg_edges: TableLike | RecordBatchReaderLike,
) -> tuple[pa.Table, pa.Table]:
    """Split properties into file-scoped and global tables.

    Returns
    -------
    tuple[pyarrow.Table, pyarrow.Table]
        File-scoped props (with file_id) and global props tables.
    """
    props_table = _ensure_table(props)
    if props_table.num_rows == 0:
        return _empty_by_file(), _empty_global()

    if "entity_kind" not in props_table.column_names:
        aligned = align_table(props_table, schema=CPG_PROPS_GLOBAL_SCHEMA, safe_cast=True)
        return _empty_by_file(), aligned

    node_props = _filter_props_kind(props_table, _NODE_KIND)
    edge_props = _filter_props_kind(props_table, _EDGE_KIND)
    other_props = _filter_props_other(props_table)

    node_file, node_global = _attach_file_id(
        node_props,
        mapping=_ensure_table(cpg_nodes),
        mapping_id="node_id",
        mapping_file_id="file_id",
    )
    edge_file, edge_global = _attach_file_id(
        edge_props,
        mapping=_ensure_table(cpg_edges),
        mapping_id="edge_id",
        mapping_file_id="edge_owner_file_id",
    )
    file_props = _concat_tables(
        [node_file, edge_file],
        schema=CPG_PROPS_BY_FILE_ID_SCHEMA,
    )
    global_props = _concat_tables(
        [node_global, edge_global, other_props],
        schema=CPG_PROPS_GLOBAL_SCHEMA,
    )
    return file_props, global_props


def _attach_file_id(
    props: pa.Table,
    *,
    mapping: pa.Table,
    mapping_id: str,
    mapping_file_id: str,
) -> tuple[pa.Table, pa.Table]:
    if props.num_rows == 0:
        return _empty_by_file(), _empty_global()
    if (
        mapping_id not in mapping.column_names
        or mapping_file_id not in mapping.column_names
    ):
        aligned = align_table(props, schema=CPG_PROPS_GLOBAL_SCHEMA, safe_cast=True)
        return _empty_by_file(), aligned
    mapping_table = mapping.select([mapping_id, mapping_file_id]).rename_columns(
        ["entity_id", "file_id"]
    )
    joined = props.join(mapping_table, keys=["entity_id"], join_type="left outer")
    file_mask = pc.is_valid(joined["file_id"])
    file_props = joined.filter(file_mask)
    global_props = joined.filter(pc.invert(file_mask)).drop(["file_id"])
    return file_props, global_props


def _filter_props_kind(props: pa.Table, kind: str) -> pa.Table:
    mask = pc.equal(props["entity_kind"], kind)
    return props.filter(mask)


def _filter_props_other(props: pa.Table) -> pa.Table:
    value_set = pa.array([_NODE_KIND, _EDGE_KIND], type=pa.string())
    mask = pc.is_in(props["entity_kind"], value_set=value_set)
    return props.filter(pc.invert(mask))


def _concat_tables(
    tables: Sequence[pa.Table],
    *,
    schema: pa.Schema,
) -> pa.Table:
    aligned = [
        align_table(table, schema=schema, safe_cast=True)
        for table in tables
    ]
    non_empty = [table for table in aligned if table.num_rows > 0]
    if not non_empty:
        return table_from_schema(schema, columns={}, num_rows=0)
    return cast(
        "pa.Table",
        pa.concat_tables(non_empty, promote_options="default"),
    )


def _ensure_table(value: TableLike | RecordBatchReaderLike) -> pa.Table:
    if isinstance(value, RecordBatchReaderLike):
        return value.read_all()
    return cast("pa.Table", value)


def _empty_by_file() -> pa.Table:
    return table_from_schema(CPG_PROPS_BY_FILE_ID_SCHEMA, columns={}, num_rows=0)


def _empty_global() -> pa.Table:
    return table_from_schema(CPG_PROPS_GLOBAL_SCHEMA, columns={}, num_rows=0)


def _partition_specs(
    column: str,
    values: Sequence[str],
) -> tuple[dict[str, str], ...]:
    return tuple({column: value} for value in values)


__all__ = ["split_props_by_file_id", "upsert_cpg_props"]

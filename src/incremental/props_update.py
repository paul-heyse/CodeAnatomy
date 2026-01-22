"""Incremental CPG property updates."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import cast
from uuid import uuid4

import pyarrow as pa
from datafusion import SessionContext

from arrowdsl.core.interop import RecordBatchReaderLike, TableLike
from arrowdsl.schema.build import table_from_schema
from arrowdsl.schema.metadata import encoding_policy_from_schema
from arrowdsl.schema.schema import align_table
from cpg.schemas import (
    CPG_PROPS_BY_FILE_ID_SCHEMA,
    CPG_PROPS_GLOBAL_SCHEMA,
)
from datafusion_engine.runtime import DataFusionRuntimeProfile
from incremental.state_store import StateStore
from incremental.types import IncrementalFileChanges
from storage.deltalake import (
    DeltaUpsertOptions,
    DeltaWriteOptions,
    coerce_delta_table,
    upsert_dataset_partitions_delta,
    write_dataset_delta,
)

_PROPS_BY_FILE_DATASET = "cpg_props_by_file_id_v1"
_PROPS_GLOBAL_DATASET = "cpg_props_global_v1"
_NODE_KIND = "node"
_EDGE_KIND = "edge"


@dataclass(frozen=True)
class _AttachFileIdInputs:
    props_name: str
    mapping_name: str
    mapping_id: str
    mapping_file_id: str
    kind: str


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
    file_table = coerce_delta_table(
        props_by_file,
        schema=CPG_PROPS_BY_FILE_ID_SCHEMA,
        encoding_policy=encoding_policy_from_schema(CPG_PROPS_BY_FILE_ID_SCHEMA),
    )
    file_result = upsert_dataset_partitions_delta(
        file_table,
        options=DeltaUpsertOptions(
            base_dir=str(state_store.dataset_dir(_PROPS_BY_FILE_DATASET)),
            partition_cols=("file_id",),
            delete_partitions=delete_partitions,
            options=DeltaWriteOptions(schema_mode="merge"),
        ),
    )
    updated[_PROPS_BY_FILE_DATASET] = file_result.path

    if changes.full_refresh:
        global_table = coerce_delta_table(
            props_global,
            schema=CPG_PROPS_GLOBAL_SCHEMA,
            encoding_policy=encoding_policy_from_schema(CPG_PROPS_GLOBAL_SCHEMA),
        )
        global_result = write_dataset_delta(
            global_table,
            str(state_store.dataset_dir(_PROPS_GLOBAL_DATASET)),
            options=DeltaWriteOptions(mode="overwrite", schema_mode="overwrite"),
        )
        updated[_PROPS_GLOBAL_DATASET] = global_result.path

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

    nodes_table = _ensure_table(cpg_nodes)
    edges_table = _ensure_table(cpg_edges)
    df_ctx = _datafusion_context()
    props_name = _register_table(df_ctx, props_table, prefix="props")
    nodes_name = _register_table(df_ctx, nodes_table, prefix="nodes")
    edges_name = _register_table(df_ctx, edges_table, prefix="edges")
    try:
        node_file, node_global = _attach_file_id(
            df_ctx,
            props=props_table,
            mapping=nodes_table,
            inputs=_AttachFileIdInputs(
                props_name=props_name,
                mapping_name=nodes_name,
                mapping_id="node_id",
                mapping_file_id="file_id",
                kind=_NODE_KIND,
            ),
        )
        edge_file, edge_global = _attach_file_id(
            df_ctx,
            props=props_table,
            mapping=edges_table,
            inputs=_AttachFileIdInputs(
                props_name=props_name,
                mapping_name=edges_name,
                mapping_id="edge_id",
                mapping_file_id="edge_owner_file_id",
                kind=_EDGE_KIND,
            ),
        )
        other_props = _filter_props_other(df_ctx, props_name=props_name)
        file_props = _concat_tables(
            [node_file, edge_file],
            schema=CPG_PROPS_BY_FILE_ID_SCHEMA,
        )
        global_props = _concat_tables(
            [node_global, edge_global, other_props],
            schema=CPG_PROPS_GLOBAL_SCHEMA,
        )
        return file_props, global_props
    finally:
        _deregister_table(df_ctx, props_name)
        _deregister_table(df_ctx, nodes_name)
        _deregister_table(df_ctx, edges_name)


def _attach_file_id(
    ctx: SessionContext,
    *,
    props: pa.Table,
    mapping: pa.Table,
    inputs: _AttachFileIdInputs,
) -> tuple[pa.Table, pa.Table]:
    if props.num_rows == 0:
        return _empty_by_file(), _empty_global()
    if "entity_id" not in props.column_names:
        aligned = align_table(props, schema=CPG_PROPS_GLOBAL_SCHEMA, safe_cast=True)
        return _empty_by_file(), aligned
    if (
        inputs.mapping_id not in mapping.column_names
        or inputs.mapping_file_id not in mapping.column_names
    ):
        aligned = align_table(
            _filter_props_kind(ctx, props_name=inputs.props_name, kind=inputs.kind),
            schema=CPG_PROPS_GLOBAL_SCHEMA,
            safe_cast=True,
        )
        return _empty_by_file(), aligned
    mapping_sql = (
        f"SELECT {_sql_identifier(inputs.mapping_id)} AS entity_id, "
        f"{_sql_identifier(inputs.mapping_file_id)} AS file_id "
        f"FROM {_sql_identifier(inputs.mapping_name)}"
    )
    kind_literal = _sql_literal(inputs.kind)
    file_sql = (
        "SELECT props.*, mapping.file_id "
        f"FROM {_sql_identifier(inputs.props_name)} AS props "
        f"LEFT JOIN ({mapping_sql}) AS mapping "
        "ON props.entity_id = mapping.entity_id "
        f"WHERE props.entity_kind = {kind_literal} AND mapping.file_id IS NOT NULL"
    )
    global_sql = (
        "SELECT props.* "
        f"FROM {_sql_identifier(inputs.props_name)} AS props "
        f"LEFT JOIN ({mapping_sql}) AS mapping "
        "ON props.entity_id = mapping.entity_id "
        f"WHERE props.entity_kind = {kind_literal} AND mapping.file_id IS NULL"
    )
    file_props = ctx.sql(file_sql).to_arrow_table()
    global_props = ctx.sql(global_sql).to_arrow_table()
    return file_props, global_props


def _filter_props_kind(ctx: SessionContext, *, props_name: str, kind: str) -> pa.Table:
    kind_literal = _sql_literal(kind)
    sql = f"SELECT * FROM {_sql_identifier(props_name)} WHERE entity_kind = {kind_literal}"
    return ctx.sql(sql).to_arrow_table()


def _filter_props_other(ctx: SessionContext, *, props_name: str) -> pa.Table:
    sql = (
        f"SELECT * FROM {_sql_identifier(props_name)} "
        f"WHERE entity_kind IS NULL OR entity_kind NOT IN ('{_NODE_KIND}', '{_EDGE_KIND}')"
    )
    return ctx.sql(sql).to_arrow_table()


def _concat_tables(
    tables: Sequence[pa.Table],
    *,
    schema: pa.Schema,
) -> pa.Table:
    aligned = [align_table(table, schema=schema, safe_cast=True) for table in tables]
    non_empty = [table for table in aligned if table.num_rows > 0]
    if not non_empty:
        return table_from_schema(schema, columns={}, num_rows=0)
    return cast(
        "pa.Table",
        pa.concat_tables(non_empty, promote_options="default"),
    )


def _ensure_table(value: TableLike | RecordBatchReaderLike) -> pa.Table:
    if isinstance(value, RecordBatchReaderLike):
        return pa.Table.from_batches(list(value))
    return cast("pa.Table", value)


def _datafusion_context() -> SessionContext:
    profile = DataFusionRuntimeProfile()
    return profile.session_context()


def _register_table(ctx: SessionContext, table: pa.Table, *, prefix: str) -> str:
    name = f"_{prefix}_{uuid4().hex}"
    ctx.register_record_batches(name, [table.to_batches()])
    return name


def _deregister_table(ctx: SessionContext, name: str) -> None:
    deregister = getattr(ctx, "deregister_table", None)
    if callable(deregister):
        deregister(name)


def _sql_identifier(name: str) -> str:
    escaped = name.replace('"', '""')
    return f'"{escaped}"'


def _sql_literal(value: str) -> str:
    escaped = value.replace("'", "''")
    return f"'{escaped}'"


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

"""Incremental CPG property updates via shared runtime."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Protocol, cast

import ibis
import pyarrow as pa

from arrowdsl.core.interop import RecordBatchReaderLike, TableLike
from arrowdsl.schema.build import table_from_schema
from arrowdsl.schema.schema import align_table
from cpg.schemas import (
    CPG_PROPS_BY_FILE_ID_SCHEMA,
    CPG_PROPS_GLOBAL_SCHEMA,
)
from incremental.delta_context import DeltaAccessContext
from incremental.delta_updates import (
    OverwriteDatasetSpec,
    PartitionedDatasetSpec,
    upsert_partitioned_dataset,
    write_overwrite_dataset,
)
from incremental.ibis_exec import ibis_expr_to_table
from incremental.ibis_utils import ibis_table_from_arrow
from incremental.runtime import IncrementalRuntime
from incremental.state_store import StateStore
from incremental.types import IncrementalFileChanges

_PROPS_BY_FILE_DATASET = "cpg_props_by_file_id_v1"
_PROPS_GLOBAL_DATASET = "cpg_props_global_v1"
_NODE_KIND = "node"
_EDGE_KIND = "edge"


@dataclass(frozen=True)
class _AttachFileIdInputs:
    mapping_id: str
    mapping_file_id: str
    kind: str


class CpgPropsInputs(Protocol):
    """Protocol for CPG property input bundles."""

    @property
    def props(self) -> TableLike | RecordBatchReaderLike:
        """Return the CPG props table."""
        ...

    @property
    def nodes(self) -> TableLike | RecordBatchReaderLike:
        """Return the CPG nodes table."""
        ...

    @property
    def edges(self) -> TableLike | RecordBatchReaderLike:
        """Return the CPG edges table."""
        ...


def upsert_cpg_props(
    inputs: CpgPropsInputs,
    *,
    state_store: StateStore,
    changes: IncrementalFileChanges,
    runtime: IncrementalRuntime,
) -> dict[str, str]:
    """Upsert CPG properties into the incremental state store.

    Returns
    -------
    dict[str, str]
        Mapping of dataset names to dataset paths.
    """
    props_by_file, props_global = split_props_by_file_id(
        inputs.props,
        cpg_nodes=inputs.nodes,
        cpg_edges=inputs.edges,
        runtime=runtime,
    )
    context = DeltaAccessContext(runtime=runtime)
    updated: dict[str, str] = {}
    spec = PartitionedDatasetSpec(
        name=_PROPS_BY_FILE_DATASET,
        partition_column="file_id",
        schema=CPG_PROPS_BY_FILE_ID_SCHEMA,
    )
    file_path = upsert_partitioned_dataset(
        props_by_file,
        spec=spec,
        base_dir=str(state_store.dataset_dir(spec.name)),
        changes=changes,
        context=context,
    )
    if file_path is not None:
        updated[spec.name] = file_path

    if changes.full_refresh:
        global_spec = OverwriteDatasetSpec(
            name=_PROPS_GLOBAL_DATASET,
            schema=CPG_PROPS_GLOBAL_SCHEMA,
            commit_metadata={"snapshot_kind": "cpg_props_global"},
        )
        updated.update(
            write_overwrite_dataset(
                props_global,
                spec=global_spec,
                state_store=state_store,
                context=context,
            )
        )
    return updated


def split_props_by_file_id(
    props: TableLike | RecordBatchReaderLike,
    *,
    cpg_nodes: TableLike | RecordBatchReaderLike,
    cpg_edges: TableLike | RecordBatchReaderLike,
    runtime: IncrementalRuntime,
) -> tuple[pa.Table, pa.Table]:
    """Split properties into file-scoped and global tables.

    Returns
    -------
    tuple[pyarrow.Table, pyarrow.Table]
        File-scoped and global property tables.
    """
    props_table = _ensure_table(props)
    if props_table.num_rows == 0:
        return _empty_by_file(), _empty_global()
    if "entity_kind" not in props_table.column_names:
        aligned = align_table(props_table, schema=CPG_PROPS_GLOBAL_SCHEMA, safe_cast=True)
        return _empty_by_file(), aligned
    if "entity_id" not in props_table.column_names:
        aligned = align_table(props_table, schema=CPG_PROPS_GLOBAL_SCHEMA, safe_cast=True)
        return _empty_by_file(), aligned

    backend = runtime.ibis_backend()
    props_expr = ibis_table_from_arrow(backend, props_table, name="cpg_props")
    nodes_expr = ibis_table_from_arrow(backend, _ensure_table(cpg_nodes), name="cpg_nodes")
    edges_expr = ibis_table_from_arrow(backend, _ensure_table(cpg_edges), name="cpg_edges")

    node_file, node_global = _attach_file_id_expr(
        props_expr,
        mapping_expr=nodes_expr,
        inputs=_AttachFileIdInputs(
            mapping_id="node_id",
            mapping_file_id="file_id",
            kind=_NODE_KIND,
        ),
    )
    edge_file, edge_global = _attach_file_id_expr(
        props_expr,
        mapping_expr=edges_expr,
        inputs=_AttachFileIdInputs(
            mapping_id="edge_id",
            mapping_file_id="edge_owner_file_id",
            kind=_EDGE_KIND,
        ),
    )
    other_props = _other_props_expr(props_expr)
    file_expr = _union_exprs([expr for expr in (node_file, edge_file) if expr is not None])
    global_expr = _union_exprs([node_global, edge_global, other_props])

    if file_expr is None:
        file_table = _empty_by_file()
    else:
        file_table = ibis_expr_to_table(
            file_expr,
            runtime=runtime,
            name="cpg_props_by_file_id",
        )
        file_table = align_table(file_table, schema=CPG_PROPS_BY_FILE_ID_SCHEMA, safe_cast=True)
    if global_expr is None:
        global_table = _empty_global()
    else:
        global_table = ibis_expr_to_table(
            global_expr,
            runtime=runtime,
            name="cpg_props_global",
        )
        global_table = align_table(global_table, schema=CPG_PROPS_GLOBAL_SCHEMA, safe_cast=True)
    return file_table, global_table


def _attach_file_id_expr(
    props_expr: ibis.Table,
    *,
    mapping_expr: ibis.Table,
    inputs: _AttachFileIdInputs,
) -> tuple[ibis.Table | None, ibis.Table]:
    kind_props = props_expr.filter(props_expr.entity_kind == ibis.literal(inputs.kind))
    if (
        inputs.mapping_id not in mapping_expr.columns
        or inputs.mapping_file_id not in mapping_expr.columns
    ):
        return None, kind_props
    mapping = mapping_expr.select(
        entity_id=mapping_expr[inputs.mapping_id],
        mapped_file_id=mapping_expr[inputs.mapping_file_id],
    )
    joined = kind_props.left_join(mapping, predicates=[kind_props.entity_id == mapping.entity_id])
    props_cols = [kind_props[col] for col in kind_props.columns]
    file_expr = joined.filter(joined.mapped_file_id.notnull()).select(
        *props_cols,
        joined.mapped_file_id.name("file_id"),
    )
    global_expr = joined.filter(joined.mapped_file_id.isnull()).select(*props_cols)
    return file_expr, global_expr


def _other_props_expr(props_expr: ibis.Table) -> ibis.Table:
    kind_col = props_expr.entity_kind
    values = [ibis.literal(_NODE_KIND), ibis.literal(_EDGE_KIND)]
    return props_expr.filter(
        ibis.or_(
            kind_col.isnull(),
            kind_col.notin(values),
        )
    )


def _union_exprs(exprs: Sequence[ibis.Table]) -> ibis.Table | None:
    if not exprs:
        return None
    if len(exprs) == 1:
        return exprs[0]
    return ibis.union(*exprs, distinct=False)


def _ensure_table(value: TableLike | RecordBatchReaderLike) -> pa.Table:
    if isinstance(value, RecordBatchReaderLike):
        return pa.Table.from_batches(list(value))
    return cast("pa.Table", value)


def _empty_by_file() -> pa.Table:
    return table_from_schema(CPG_PROPS_BY_FILE_ID_SCHEMA, columns={}, num_rows=0)


def _empty_global() -> pa.Table:
    return table_from_schema(CPG_PROPS_GLOBAL_SCHEMA, columns={}, num_rows=0)


__all__ = ["CpgPropsInputs", "split_props_by_file_id", "upsert_cpg_props"]

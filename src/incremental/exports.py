"""Exported definition index builder for incremental impact analysis."""

from __future__ import annotations

from typing import cast

import pyarrow as pa
from ibis.backends import BaseBackend
from ibis.expr.types import Table

from arrowdsl.core.interop import TableLike
from arrowdsl.schema.build import table_from_arrays
from arrowdsl.schema.schema import align_table
from ibis_engine.sources import SourceToIbisOptions, source_to_ibis
from incremental.registry_specs import dataset_schema


def build_exported_defs_index(
    cst_defs_norm: TableLike,
    *,
    backend: BaseBackend,
    rel_def_symbol: TableLike | None = None,
) -> pa.Table:
    """Build the exported definitions index for top-level defs.

    Returns
    -------
    pa.Table
        Exported definitions table with qname and optional symbol bindings.
    """
    _require_datafusion_backend(backend)
    schema = dataset_schema("dim_exported_defs_v1")
    plan = source_to_ibis(
        cst_defs_norm,
        options=SourceToIbisOptions(backend=backend, name="cst_defs_norm"),
    )
    if "qnames" not in plan.expr.schema().names:
        return _empty_exported_defs(schema)
    if rel_def_symbol is not None:
        source_to_ibis(
            rel_def_symbol,
            options=SourceToIbisOptions(backend=backend, name="rel_def_symbol_v1"),
        )
    result = _build_exported_defs_base(
        backend,
        has_rel_def_symbol=rel_def_symbol is not None,
        has_container_def_id="container_def_id" in plan.expr.schema().names,
    )
    return align_table(result, schema=schema, safe_cast=True)


def _build_exported_defs_base(
    backend: BaseBackend,
    *,
    has_rel_def_symbol: bool,
    has_container_def_id: bool,
) -> pa.Table:
    sql = _build_exported_defs_sql(
        has_rel_def_symbol=has_rel_def_symbol,
        has_container_def_id=has_container_def_id,
    )
    table = _sql_expr(backend, sql).to_pyarrow()
    if table.num_rows == 0:
        return _empty_exported_defs(dataset_schema("dim_exported_defs_v1"))
    return table


def _build_exported_defs_sql(
    *,
    has_rel_def_symbol: bool,
    has_container_def_id: bool,
) -> str:
    container_clause = "WHERE base.container_def_id IS NULL" if has_container_def_id else ""
    join_clause = ""
    symbol_expr = "CAST(NULL AS STRING) AS symbol"
    symbol_roles_expr = "CAST(NULL AS INT) AS symbol_roles"
    if has_rel_def_symbol:
        join_clause = (
            "LEFT JOIN rel_def_symbol_v1 rel ON rel.def_id = base.def_id AND rel.path = base.path"
        )
        symbol_expr = "rel.symbol AS symbol"
        symbol_roles_expr = "rel.symbol_roles AS symbol_roles"
    return f"""
    WITH base AS (
      SELECT * FROM cst_defs_norm
      {container_clause}
    )
    SELECT
      base.file_id AS file_id,
      base.path AS path,
      base.def_id AS def_id,
      COALESCE(base.def_kind_norm, base.kind) AS def_kind_norm,
      base.name AS name,
      prefixed_hash64('qname', get_field(qname, 'name')) AS qname_id,
      get_field(qname, 'name') AS qname,
      get_field(qname, 'source') AS qname_source,
      {symbol_expr},
      {symbol_roles_expr}
    FROM base
    CROSS JOIN unnest(base.qnames) AS qname
    {join_clause}
    """


def _sql_expr(backend: BaseBackend, sql: str) -> Table:
    sql_method = getattr(backend, "sql", None)
    if not callable(sql_method):
        msg = "Ibis backend does not support SQL queries."
        raise TypeError(msg)
    return cast("Table", sql_method(sql))


def _require_datafusion_backend(backend: BaseBackend) -> None:
    name = getattr(backend, "name", "")
    if str(name).lower() == "datafusion":
        return
    msg = "Exported definition indexing requires a DataFusion Ibis backend."
    raise ValueError(msg)


def _empty_exported_defs(schema: pa.Schema) -> pa.Table:
    return table_from_arrays(schema, columns={}, num_rows=0)


__all__ = ["build_exported_defs_index"]

"""DataFusion registration helpers for parameter tables."""

from __future__ import annotations

from collections.abc import Mapping

import pyarrow as pa
from datafusion import SessionContext
from datafusion.catalog import Catalog, Schema

from ibis_engine.param_tables import ParamTableArtifact, ParamTablePolicy, param_table_name


def ensure_param_schema(
    ctx: SessionContext,
    *,
    catalog: str,
    schema: str,
) -> Schema:
    """Ensure the parameter schema exists in the session catalog.

    Returns
    -------
    datafusion.catalog.Schema
        The schema instance for param table registration.
    """
    try:
        cat = ctx.catalog(catalog)
    except KeyError:
        cat = Catalog.memory_catalog()
        ctx.register_catalog_provider(catalog, cat)
    if schema not in cat.schema_names():
        cat.register_schema(schema, Schema.memory_schema())
    return cat.schema(schema)


def register_param_arrow_table(
    ctx: SessionContext,
    *,
    catalog: str,
    schema: str,
    table_name: str,
    table: pa.Table,
) -> None:
    """Register an Arrow table into a catalog schema."""
    schema_obj = ensure_param_schema(ctx, catalog=catalog, schema=schema)
    if table_name in schema_obj.table_names():
        schema_obj.deregister_table(table_name)
    schema_obj.register_table(table_name, table)


def register_param_tables_df(
    ctx: SessionContext,
    *,
    artifacts: Mapping[str, ParamTableArtifact],
    policy: ParamTablePolicy,
) -> dict[str, str]:
    """Register param tables and return logical -> qualified name mapping.

    Returns
    -------
    dict[str, str]
        Mapping of logical param names to fully qualified table names.
    """
    mapping: dict[str, str] = {}
    for logical_name, artifact in artifacts.items():
        table_name = param_table_name(policy, logical_name)
        register_param_arrow_table(
            ctx,
            catalog=policy.catalog,
            schema=policy.schema,
            table_name=table_name,
            table=artifact.table,
        )
        mapping[logical_name] = f"{policy.catalog}.{policy.schema}.{table_name}"
    return mapping


__all__ = [
    "ensure_param_schema",
    "register_param_arrow_table",
    "register_param_tables_df",
]

"""DataFusion registration helpers for parameter tables."""

from __future__ import annotations

from collections.abc import Mapping, MutableMapping
from dataclasses import dataclass

import pyarrow as pa
from datafusion import SessionContext
from datafusion.catalog import Catalog, Schema

from ibis_engine.param_tables import (
    ParamTableArtifact,
    ParamTablePolicy,
    param_table_name,
    param_table_schema,
)


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


@dataclass(frozen=True)
class ParamTableRegistrationOptions:
    """Capture optional parameter table registration settings."""

    scope_key: str | None = None
    signature_cache: MutableMapping[str, str] | None = None


def register_param_tables_df(
    ctx: SessionContext,
    *,
    artifacts: Mapping[str, ParamTableArtifact],
    policy: ParamTablePolicy,
    options: ParamTableRegistrationOptions | None = None,
) -> dict[str, str]:
    """Register param tables and return logical -> qualified name mapping.

    Parameters
    ----------
    ctx
        DataFusion session context used for registration.
    artifacts
        Param table artifacts to register.
    policy
        Policy controlling catalog/schema/prefix naming.
    options
        Optional registration settings controlling cache and validation.

    Returns
    -------
    dict[str, str]
        Mapping of logical param names to fully qualified table names.
    """
    mapping: dict[str, str] = {}
    resolved_options = options or ParamTableRegistrationOptions()
    schema_name = param_table_schema(policy, scope_key=resolved_options.scope_key)
    for logical_name, artifact in artifacts.items():
        table_name = param_table_name(policy, logical_name)
        qualified = f"{policy.catalog}.{schema_name}.{table_name}"
        if resolved_options.signature_cache is not None:
            cached = resolved_options.signature_cache.get(logical_name)
            if cached == artifact.signature:
                mapping[logical_name] = qualified
                continue
        register_param_arrow_table(
            ctx,
            catalog=policy.catalog,
            schema=schema_name,
            table_name=table_name,
            table=artifact.table,
        )
        mapping[logical_name] = qualified
        if resolved_options.signature_cache is not None:
            resolved_options.signature_cache[logical_name] = artifact.signature
    return mapping


__all__ = [
    "ParamTableRegistrationOptions",
    "ensure_param_schema",
    "register_param_arrow_table",
    "register_param_tables_df",
]

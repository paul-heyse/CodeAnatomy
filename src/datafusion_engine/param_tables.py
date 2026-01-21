"""DataFusion registration helpers for parameter tables."""

from __future__ import annotations

from collections.abc import Mapping, MutableMapping, Sequence
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
    parameters_snapshot: Sequence[Mapping[str, object]] | None = None


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
        _validate_param_table_schema(
            ctx,
            catalog=policy.catalog,
            schema=schema_name,
            table_name=table_name,
            expected_schema=artifact.table.schema,
        )
        if resolved_options.parameters_snapshot is not None:
            _validate_param_table_parameters(
                resolved_options.parameters_snapshot,
                table_name=table_name,
                expected_schema=artifact.table.schema,
            )
        mapping[logical_name] = qualified
        if resolved_options.signature_cache is not None:
            resolved_options.signature_cache[logical_name] = artifact.signature
    return mapping


def _validate_param_table_schema(
    ctx: SessionContext,
    *,
    catalog: str,
    schema: str,
    table_name: str,
    expected_schema: pa.Schema,
) -> None:
    """Validate parameter table schema against DataFusion metadata.

    Parameters
    ----------
    ctx
        DataFusion session context.
    catalog
        Catalog name for the parameter table.
    schema
        Schema name for the parameter table.
    table_name
        Table name for the parameter table.
    expected_schema
        Expected Arrow schema for the parameter table.

    Raises
    ------
    ValueError
        Raised when DataFusion metadata does not match the expected schema.
    """
    query = (
        "SELECT column_name, data_type, ordinal_position "
        "FROM information_schema.columns "
        f"WHERE table_catalog = '{catalog}' "
        f"AND table_schema = '{schema}' "
        f"AND table_name = '{table_name}' "
        "ORDER BY ordinal_position"
    )
    try:
        rows = ctx.sql(query).to_arrow_table().to_pylist()
    except (RuntimeError, TypeError, ValueError):
        return
    if not rows:
        return
    actual_names = [str(row.get("column_name")) for row in rows if row.get("column_name")]
    expected_names = list(expected_schema.names)
    if actual_names != expected_names:
        msg = (
            "Param table schema mismatch for "
            f"{catalog}.{schema}.{table_name}: expected {expected_names}, got {actual_names}."
        )
        raise ValueError(msg)
    expected_types = {field.name: str(field.type).lower() for field in expected_schema}
    for row in rows:
        name = row.get("column_name")
        data_type = row.get("data_type")
        if name is None or data_type is None:
            continue
        expected = expected_types.get(str(name))
        actual = str(data_type).lower()
        if expected is None:
            continue
        if expected != actual:
            msg = (
                "Param table type mismatch for "
                f"{catalog}.{schema}.{table_name}.{name}: expected {expected}, got {actual}."
            )
            raise ValueError(msg)


def _validate_param_table_parameters(
    parameters_snapshot: Sequence[Mapping[str, object]],
    *,
    table_name: str,
    expected_schema: pa.Schema,
) -> None:
    """Validate param table types against DataFusion parameter metadata.

    Parameters
    ----------
    parameters_snapshot
        Parameter metadata rows from ``information_schema.parameters``.
    table_name
        Parameter table name used as a lookup hint.
    expected_schema
        Expected Arrow schema for the parameter table.

    Raises
    ------
    ValueError
        Raised when parameter metadata disagrees with expected types.
    """
    expected_types = {field.name: str(field.type).lower() for field in expected_schema}
    for row in parameters_snapshot:
        param_name = row.get("parameter_name")
        data_type = row.get("data_type")
        if param_name is None or data_type is None:
            continue
        name = str(param_name)
        if name not in expected_types:
            continue
        expected = expected_types[name]
        actual = str(data_type).lower()
        if expected != actual:
            msg = (
                "Prepared parameter type mismatch for "
                f"{table_name}.{name}: expected {expected}, got {actual}."
            )
            raise ValueError(msg)


__all__ = [
    "ParamTableRegistrationOptions",
    "ensure_param_schema",
    "register_param_arrow_table",
    "register_param_tables_df",
]

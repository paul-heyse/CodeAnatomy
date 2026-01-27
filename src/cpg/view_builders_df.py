"""DataFusion-native view builders for CPG node/edge/property outputs.

IMPORTANT: This module provides DataFusion DataFrame-based CPG builders.
The Ibis-based builders in view_builders.py are DEPRECATED and will be removed
in a future release.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import TYPE_CHECKING

import pyarrow as pa
from datafusion import SessionContext, col, lit
from datafusion import functions as f
from datafusion.dataframe import DataFrame
from datafusion.expr import Expr

from cpg.emit_nodes_ibis import _NODE_OUTPUT_COLUMNS
from cpg.emit_props_ibis import _PROP_OUTPUT_COLUMNS, CpgPropOptions
from cpg.spec_registry import (
    edge_prop_spec,
    node_plan_specs,
    prop_table_specs,
    scip_role_flag_prop_spec,
)
from cpg.specs import NodeEmitSpec, PropFieldSpec, PropTableSpec, TaskIdentity
from datafusion_ext import stable_id
from relspec.view_defs import RELATION_OUTPUT_NAME

if TYPE_CHECKING:
    from cpg.specs import PropOptions


def _arrow_cast(expr: Expr, data_type: str) -> Expr:
    """Cast an expression to an Arrow data type.

    Returns
    -------
    Expr
        Arrow-casted expression.
    """
    return f.arrow_cast(expr, lit(data_type))


def _null_expr(data_type: str) -> Expr:
    """Create a null literal with the specified Arrow data type.

    Returns
    -------
    Expr
        Null expression with specified type.
    """
    return _arrow_cast(lit(None), data_type)


def _coalesce_cols(df: DataFrame, columns: Sequence[str], dtype: pa.DataType) -> Expr:
    """Coalesce multiple columns, falling back to null if all are missing.

    Returns
    -------
    Expr
        Coalesced expression.
    """
    available = [name for name in columns if name in df.schema().names]
    if not available:
        arrow_type = str(dtype).replace("_", "").title()
        return _null_expr(arrow_type)
    exprs = [col(name) for name in available]
    return f.coalesce(*exprs)


def _literal_or_null(value: object | None, dtype: pa.DataType) -> Expr:
    """Create a literal or null expression based on the value.

    Returns
    -------
    Expr
        Literal or null expression.
    """
    if value is None:
        arrow_type = str(dtype).replace("_", "").title()
        return _null_expr(arrow_type)
    return lit(value)


def build_cpg_nodes_df(
    ctx: SessionContext, *, task_identity: TaskIdentity | None = None
) -> DataFrame:
    """Build CPG nodes DataFrame from view specs using DataFusion.

    Parameters
    ----------
    ctx
        DataFusion SessionContext with registered views.
    task_identity
        Optional task identity metadata for CPG outputs.

    Returns
    -------
    DataFrame
        CPG nodes DataFrame with standardized schema.

    Raises
    ------
    ValueError
        Raised when required source tables are missing or no plans are produced.
    """
    specs = node_plan_specs()
    task_name = task_identity.name if task_identity is not None else None
    task_priority = task_identity.priority if task_identity is not None else None

    frames: list[DataFrame] = []
    for spec in specs:
        try:
            source_df = ctx.table(spec.table_ref)
        except KeyError as exc:
            msg = f"Missing required source table {spec.table_ref!r} for CPG nodes."
            raise ValueError(msg) from exc

        node_df = _emit_nodes_df(
            source_df,
            spec=spec.emit,
            task_name=task_name,
            task_priority=task_priority,
        )
        frames.append(node_df)

    if not frames:
        msg = "CPG node builder did not produce any plans."
        raise ValueError(msg)

    combined = frames[0]
    for frame in frames[1:]:
        combined = combined.union(frame)

    return combined.select(*_NODE_OUTPUT_COLUMNS)


def _emit_nodes_df(
    df: DataFrame,
    *,
    spec: NodeEmitSpec,
    task_name: str | None = None,
    task_priority: int | None = None,
) -> DataFrame:
    """Emit CPG nodes from a DataFrame using DataFusion expressions.

    Parameters
    ----------
    df
        Source DataFrame.
    spec
        Node emission specification.
    task_name
        Optional task name for metadata.
    task_priority
        Optional task priority for metadata.

    Returns
    -------
    DataFrame
        DataFrame with CPG node rows.
    """
    id_cols, _ = _prepare_id_columns_df(df, spec.id_cols)
    node_id_parts = [col(c) if c in df.schema().names else _null_expr("Utf8") for c in id_cols]
    node_id_parts.append(lit(str(spec.node_kind)))

    node_id = stable_id("node", *node_id_parts)
    node_kind = lit(str(spec.node_kind))
    path = _coalesce_cols(df, spec.path_cols, pa.string())
    bstart = _coalesce_cols(df, spec.bstart_cols, pa.int64())
    bend = _coalesce_cols(df, spec.bend_cols, pa.int64())
    file_id = _coalesce_cols(df, spec.file_id_cols, pa.string())

    return df.select(
        node_id.alias("node_id"),
        node_kind.alias("node_kind"),
        path.alias("path"),
        bstart.alias("bstart"),
        bend.alias("bend"),
        file_id.alias("file_id"),
        _literal_or_null(task_name, pa.string()).alias("task_name"),
        _literal_or_null(task_priority, pa.int32()).alias("task_priority"),
    )


def _prepare_id_columns_df(
    df: DataFrame,
    columns: Sequence[str],
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    """Prepare ID columns, adding nulls for missing ones.

    Returns
    -------
    tuple[tuple[str, ...], tuple[str, ...]]
        Tuple of (all_columns, required_columns).
    """
    schema_names = df.schema().names
    _ = tuple(column for column in columns if column in schema_names)

    if not columns:
        return ("__id_null",), ()

    return tuple(columns), _


def build_cpg_edges_df(ctx: SessionContext) -> DataFrame:
    """Build CPG edges DataFrame from relation outputs using DataFusion.

    Parameters
    ----------
    ctx
        DataFusion SessionContext with registered views.

    Returns
    -------
    DataFrame
        CPG edges DataFrame with standardized schema.

    Raises
    ------
    ValueError
        Raised when the relation output table is missing.
    """
    try:
        relation_df = ctx.table(RELATION_OUTPUT_NAME)
    except KeyError as exc:
        msg = f"Missing required source table {RELATION_OUTPUT_NAME!r} for CPG edges."
        raise ValueError(msg) from exc

    return _emit_edges_from_relation_df(relation_df)


def _emit_edges_from_relation_df(df: DataFrame) -> DataFrame:  # noqa: PLR0914
    """Emit CPG edges from relation_output rows using DataFusion.

    Parameters
    ----------
    df
        Source DataFrame with relation outputs.

    Returns
    -------
    DataFrame
        DataFrame with CPG edge rows.
    """
    names = df.schema().names

    edge_kind = col("kind") if "kind" in names else _null_expr("Utf8")

    base_id_parts = [edge_kind, col("src"), col("dst")]
    base_id = stable_id("edge", *base_id_parts)

    span_id_parts = [edge_kind, col("src"), col("dst"), col("path"), col("bstart"), col("bend")]
    span_id = stable_id("edge", *span_id_parts)

    has_span = col("path").is_not_null() & col("bstart").is_not_null() & col("bend").is_not_null()
    valid_nodes = col("src").is_not_null() & col("dst").is_not_null()

    edge_id = (
        f.case(valid_nodes)
        .when(lit(value=True), f.case(has_span).when(lit(value=True), span_id).otherwise(base_id))
        .otherwise(_null_expr("Utf8"))
    )

    origin = col("origin") if "origin" in names else _null_expr("Utf8")
    resolution_method = (
        col("resolution_method") if "resolution_method" in names else _null_expr("Utf8")
    )
    confidence = col("confidence") if "confidence" in names else lit(0.5)
    score = col("score") if "score" in names else lit(0.5)
    symbol_roles = col("symbol_roles") if "symbol_roles" in names else _null_expr("Int32")
    qname_source = col("qname_source") if "qname_source" in names else _null_expr("Utf8")
    ambiguity_group_id = (
        col("ambiguity_group_id") if "ambiguity_group_id" in names else _null_expr("Utf8")
    )
    task_name = col("task_name") if "task_name" in names else _null_expr("Utf8")
    task_priority = col("task_priority") if "task_priority" in names else _null_expr("Int32")

    return df.select(
        edge_id.alias("edge_id"),
        edge_kind.alias("edge_kind"),
        col("src").alias("src_node_id"),
        col("dst").alias("dst_node_id"),
        col("path").alias("path"),
        col("bstart").alias("bstart"),
        col("bend").alias("bend"),
        origin.alias("origin"),
        resolution_method.alias("resolution_method"),
        confidence.alias("confidence"),
        score.alias("score"),
        symbol_roles.alias("symbol_roles"),
        qname_source.alias("qname_source"),
        ambiguity_group_id.alias("ambiguity_group_id"),
        task_name.alias("task_name"),
        task_priority.alias("task_priority"),
    )


def build_cpg_props_df(
    ctx: SessionContext,
    *,
    options: PropOptions | None = None,
    task_identity: TaskIdentity | None = None,
) -> DataFrame:
    """Build CPG properties DataFrame from prop specs using DataFusion.

    Parameters
    ----------
    ctx
        DataFusion SessionContext with registered views.
    options
        Optional property options for filtering.
    task_identity
        Optional task identity metadata for CPG outputs.

    Returns
    -------
    DataFrame
        CPG properties DataFrame with standardized schema.

    Raises
    ------
    ValueError
        Raised when required source tables are missing.
    """
    resolved_options = options or CpgPropOptions()
    source_columns_lookup = _source_columns_lookup_df(ctx)
    prop_specs = list(prop_table_specs(source_columns_lookup=source_columns_lookup))
    prop_specs.append(scip_role_flag_prop_spec())
    prop_specs.append(edge_prop_spec())

    frames: list[DataFrame] = []
    for spec in prop_specs:
        try:
            source_df = ctx.table(spec.table_ref)
        except KeyError as exc:
            msg = f"Missing required source table {spec.table_ref!r} for CPG props."
            raise ValueError(msg) from exc

        prop_df = _emit_props_df(
            source_df,
            spec=spec,
            options=resolved_options,
            task_identity=task_identity,
        )
        if prop_df is not None:
            frames.append(prop_df)

    if not frames:
        return _empty_props_df(ctx)

    combined = frames[0]
    for frame in frames[1:]:
        combined = combined.union(frame)

    return combined.select(*_PROP_OUTPUT_COLUMNS)


def _emit_props_df(
    df: DataFrame,
    *,
    spec: PropTableSpec,
    options: PropOptions,
    task_identity: TaskIdentity | None = None,
) -> DataFrame | None:
    """Emit CPG properties from a DataFrame using DataFusion expressions.

    Parameters
    ----------
    df
        Source DataFrame.
    spec
        Property table specification.
    options
        Property options for filtering.
    task_identity
        Optional task identity metadata.

    Returns
    -------
    DataFrame | None
        DataFrame with CPG property rows, or None if no fields.
    """
    from cpg.specs import filter_fields

    fields = filter_fields(spec.fields, options=options)
    if not fields:
        return None

    task_name = task_identity.name if task_identity is not None else None
    task_priority = task_identity.priority if task_identity is not None else None

    rows: list[DataFrame] = []
    for field in fields:
        row_df = _prop_row_df(
            df,
            spec=spec,
            field=field,
            task_name=task_name,
            task_priority=task_priority,
        )
        if row_df is not None:
            rows.append(row_df)

    if not rows:
        return None

    combined = rows[0]
    for row in rows[1:]:
        combined = combined.union(row)

    return combined


def _prop_row_df(
    df: DataFrame,
    *,
    spec: PropTableSpec,
    field: PropFieldSpec,
    task_name: str | None,
    task_priority: int | None,
) -> DataFrame | None:
    """Create a property row DataFrame from a field spec.

    Parameters
    ----------
    df
        Source DataFrame.
    spec
        Property table specification.
    field
        Property field specification.
    task_name
        Optional task name for metadata.
    task_priority
        Optional task priority for metadata.

    Returns
    -------
    DataFrame | None
        DataFrame with a single property row, or None if invalid.
    """
    value_expr = _field_value_expr_df(df, field)
    if value_expr is None:
        return None

    if field.skip_if_none:
        df = df.filter(value_expr.is_not_null())

    entity_id = _entity_id_expr_df(df, spec)
    entity_kind = lit(spec.entity_kind.value)
    node_kind = _literal_or_null(
        str(spec.node_kind) if spec.node_kind is not None else None, pa.string()
    )
    prop_key = lit(field.prop_key)
    value_type = lit(field.value_type or "string")

    value_columns = _value_columns_df(value_expr, value_type_str=field.value_type or "string")

    return df.select(
        entity_kind.alias("entity_kind"),
        entity_id.alias("entity_id"),
        node_kind.alias("node_kind"),
        prop_key.alias("prop_key"),
        value_type.alias("value_type"),
        value_columns["value_string"].alias("value_string"),
        value_columns["value_int"].alias("value_int"),
        value_columns["value_float"].alias("value_float"),
        value_columns["value_bool"].alias("value_bool"),
        value_columns["value_json"].alias("value_json"),
        _literal_or_null(task_name, pa.string()).alias("task_name"),
        _literal_or_null(task_priority, pa.int32()).alias("task_priority"),
    )


def _field_value_expr_df(df: DataFrame, field: PropFieldSpec) -> Expr | None:
    """Extract the value expression for a property field.

    Parameters
    ----------
    df
        Source DataFrame.
    field
        Property field specification.

    Returns
    -------
    Expr | None
        Value expression, or None if invalid.
    """
    names = df.schema().names

    if field.literal is not None:
        return lit(field.literal)
    if field.source_col is not None:
        if field.source_col in names:
            return col(field.source_col)
        return _null_expr("Utf8")
    return None


def _entity_id_expr_df(df: DataFrame, spec: PropTableSpec) -> Expr:
    """Generate an entity ID expression for properties.

    Parameters
    ----------
    df
        Source DataFrame.
    spec
        Property table specification.

    Returns
    -------
    Expr
        Entity ID expression.
    """
    from cpg.kind_catalog import EntityKind

    id_cols = spec.id_cols
    schema_names = df.schema().names

    if spec.entity_kind == EntityKind.EDGE and len(id_cols) == 1 and id_cols[0] in schema_names:
        return col(id_cols[0])

    id_parts = [col(c) if c in schema_names else _null_expr("Utf8") for c in id_cols]

    prefix = "node" if spec.entity_kind == EntityKind.NODE else "edge"
    if spec.entity_kind == EntityKind.NODE and spec.node_kind is not None:
        id_parts.append(lit(str(spec.node_kind)))

    return stable_id(prefix, *id_parts)


def _value_columns_df(value_expr: Expr, *, value_type_str: str) -> dict[str, Expr]:
    """Create value column expressions for different types.

    Parameters
    ----------
    value_expr
        Value expression to cast.
    value_type_str
        Type of value (string, int, float, bool, json).

    Returns
    -------
    dict[str, Expr]
        Dictionary mapping column names to expressions.
    """
    columns: dict[str, Expr] = {
        "value_string": _null_expr("Utf8"),
        "value_int": _null_expr("Int64"),
        "value_float": _null_expr("Float64"),
        "value_bool": _null_expr("Boolean"),
        "value_json": _null_expr("Utf8"),
    }

    target_map = {
        "string": ("value_string", "Utf8"),
        "int": ("value_int", "Int64"),
        "float": ("value_float", "Float64"),
        "bool": ("value_bool", "Boolean"),
        "json": ("value_json", "Utf8"),
    }

    target_col, target_type = target_map.get(value_type_str, ("value_string", "Utf8"))
    columns[target_col] = _arrow_cast(value_expr, target_type)

    return columns


def _empty_props_df(ctx: SessionContext) -> DataFrame:
    """Create an empty properties DataFrame with correct schema.

    Parameters
    ----------
    ctx
        DataFusion SessionContext.

    Returns
    -------
    DataFrame
        Empty DataFrame with properties schema.
    """
    return ctx.sql("""
        SELECT
            CAST(NULL AS VARCHAR) AS entity_kind,
            CAST(NULL AS VARCHAR) AS entity_id,
            CAST(NULL AS VARCHAR) AS node_kind,
            CAST(NULL AS VARCHAR) AS prop_key,
            CAST(NULL AS VARCHAR) AS value_type,
            CAST(NULL AS VARCHAR) AS value_string,
            CAST(NULL AS BIGINT) AS value_int,
            CAST(NULL AS DOUBLE) AS value_float,
            CAST(NULL AS BOOLEAN) AS value_bool,
            CAST(NULL AS VARCHAR) AS value_json,
            CAST(NULL AS VARCHAR) AS task_name,
            CAST(NULL AS INTEGER) AS task_priority
        WHERE FALSE
    """)


def _source_columns_lookup_df(ctx: SessionContext) -> Callable[[str], Sequence[str] | None]:
    """Build a columns lookup function from registered tables.

    Parameters
    ----------
    ctx
        DataFusion SessionContext.

    Returns
    -------
    Callable[[str], Sequence[str] | None]
        Lookup function for table columns.
    """
    columns_by_table: dict[str, tuple[str, ...]] = {}

    catalog = ctx.catalog()
    for schema_name in catalog.names():
        schema = catalog.schema(schema_name)
        for table_name in schema.names():
            try:
                table_provider = schema.table(table_name)
                arrow_schema = table_provider.schema()
                columns_by_table[table_name] = tuple(sorted(arrow_schema.names))
            except Exception:  # noqa: BLE001
                # Skip tables that can't be introspected
                continue

    def _lookup(table_name: str) -> Sequence[str] | None:
        return columns_by_table.get(table_name)

    return _lookup


__all__ = [
    "build_cpg_edges_df",
    "build_cpg_nodes_df",
    "build_cpg_props_df",
]

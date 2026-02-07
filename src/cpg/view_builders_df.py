"""DataFusion-native view builders for CPG node/edge/property outputs.

IMPORTANT: This module provides DataFusion DataFrame-based CPG builders.
The Ibis-based builders in view_builders.py are DEPRECATED and will be removed
in a future release.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from typing import TYPE_CHECKING, Literal, cast

import pyarrow as pa
from datafusion import col, lit
from datafusion import functions as f
from datafusion.dataframe import DataFrame
from datafusion.expr import Expr

from cpg.emit_specs import _NODE_OUTPUT_COLUMNS, _PROP_OUTPUT_COLUMNS, CpgPropOptions
from cpg.spec_registry import (
    edge_prop_spec,
    node_plan_specs,
    prop_table_specs,
    scip_role_flag_prop_spec,
)
from cpg.specs import NodeEmitSpec, PropFieldSpec, PropTableSpec, TaskIdentity
from datafusion_engine.arrow.semantic import SEMANTIC_TYPE_META
from datafusion_engine.expr.cast import safe_cast
from datafusion_engine.lineage.diagnostics import record_artifact
from datafusion_engine.schema.introspection import SchemaIntrospector
from datafusion_engine.session.runtime import DataFusionRuntimeProfile, SessionRuntime
from datafusion_engine.sql.options import sql_options_for_profile
from datafusion_engine.udf.expr import udf_expr
from obs.otel.scopes import SCOPE_CPG
from obs.otel.tracing import stage_span
from relspec.view_defs import RELATION_OUTPUT_NAME
from serde_artifacts import (
    SemanticValidationArtifact,
    SemanticValidationArtifactEnvelope,
    SemanticValidationEntry,
)
from serde_msgspec import convert, to_builtins

if TYPE_CHECKING:
    from datafusion import SessionContext

    from cpg.specs import PropOptions


def _arrow_cast(expr: Expr, data_type: str) -> Expr:
    """Cast an expression to an Arrow data type.

    Returns:
    -------
    Expr
        Arrow-casted expression.
    """
    return safe_cast(expr, data_type)


def _null_expr(data_type: str) -> Expr:
    """Create a null literal with the specified Arrow data type.

    Returns:
    -------
    Expr
        Null expression with specified type.
    """
    return _arrow_cast(lit(None), data_type)


def _semantic_validation_enabled() -> bool:
    import datafusion_ext

    return not bool(getattr(datafusion_ext, "IS_STUB", False))


def _schema_from_df(df: DataFrame) -> pa.Schema:
    schema = df.schema()
    if isinstance(schema, pa.Schema):
        return schema
    to_arrow = getattr(schema, "to_arrow", None)
    if callable(to_arrow):
        resolved = to_arrow()
        if isinstance(resolved, pa.Schema):
            return resolved
    msg = "Failed to resolve Arrow schema for CPG output."
    raise TypeError(msg)


def _metadata_value(metadata: Mapping[object, object], key: bytes) -> str | None:
    for meta_key, meta_value in metadata.items():
        if meta_key == key or (isinstance(meta_key, str) and meta_key.encode() == key):
            if isinstance(meta_value, bytes):
                return meta_value.decode("utf-8", errors="replace")
            return str(meta_value)
    return None


def _semantic_validation_artifact(
    df: DataFrame,
    *,
    view_name: str,
    expected: Mapping[str, str],
) -> SemanticValidationArtifact:
    schema = _schema_from_df(df)
    entries: list[SemanticValidationEntry] = []
    errors: list[str] = []
    for column, semantic_type in expected.items():
        if column not in schema.names:
            entries.append(
                SemanticValidationEntry(
                    column_name=column,
                    expected=semantic_type,
                    actual=None,
                    status="missing",
                )
            )
            errors.append(f"Missing semantic column {column!r} in CPG output.")
            continue
        field = schema.field(column)
        metadata = dict(field.metadata or {})
        actual = _metadata_value(metadata, SEMANTIC_TYPE_META)
        if actual is None:
            entries.append(
                SemanticValidationEntry(
                    column_name=column,
                    expected=semantic_type,
                    actual=None,
                    status="missing",
                )
            )
            errors.append(f"Missing semantic_type metadata for CPG column {column!r}.")
            continue
        if actual != semantic_type:
            entries.append(
                SemanticValidationEntry(
                    column_name=column,
                    expected=semantic_type,
                    actual=actual,
                    status="mismatch",
                )
            )
            errors.append(
                f"Semantic type mismatch for CPG column {column!r}: "
                f"expected {semantic_type!r}, got {actual!r}."
            )
            continue
        entries.append(
            SemanticValidationEntry(
                column_name=column,
                expected=semantic_type,
                actual=actual,
                status="ok",
            )
        )
    status: Literal["ok", "error"] = "ok" if not errors else "error"
    return SemanticValidationArtifact(
        view_name=view_name,
        status=status,
        entries=tuple(entries),
        errors=tuple(errors),
    )


def _require_semantic_types(
    df: DataFrame,
    *,
    view_name: str,
    expected: Mapping[str, str],
    runtime_profile: DataFusionRuntimeProfile | None = None,
    udf_snapshot: Mapping[str, object] | None = None,
) -> None:
    if not _semantic_validation_enabled():
        return
    if udf_snapshot is not None and not _semantic_validation_supported(udf_snapshot):
        return
    artifact = _semantic_validation_artifact(df, view_name=view_name, expected=expected)
    envelope = SemanticValidationArtifactEnvelope(payload=artifact)
    validated = convert(
        to_builtins(envelope, str_keys=True),
        target_type=SemanticValidationArtifactEnvelope,
        strict=True,
    )
    payload = to_builtins(validated, str_keys=True)
    record_artifact(
        runtime_profile,
        "semantic_validation_v1",
        cast("Mapping[str, object]", payload),
    )
    if artifact.status == "ok":
        return
    msg = "Semantic validation failed: " + "; ".join(artifact.errors)
    raise ValueError(msg)


def _semantic_validation_supported(snapshot: Mapping[str, object]) -> bool:
    scalar = snapshot.get("scalar", ())
    if not isinstance(scalar, Sequence) or isinstance(scalar, (str, bytes)):
        return False
    for entry in scalar:
        if isinstance(entry, Mapping) and entry.get("name") == "semantic_tag":
            return True
    return False


def _coalesce_cols(df: DataFrame, columns: Sequence[str], dtype: pa.DataType) -> Expr:
    """Coalesce multiple columns, falling back to null if all are missing.

    Returns:
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


def _span_exprs_from_df(
    df: DataFrame,
    *,
    bstart_cols: Sequence[str],
    bend_cols: Sequence[str],
) -> tuple[Expr, Expr]:
    bstart_expr = (
        _coalesce_cols(df, bstart_cols, pa.int64())
        if any(name in df.schema().names for name in bstart_cols)
        else col("span")["bstart"]
        if "span" in df.schema().names
        else _null_expr("Int64")
    )
    bend_expr = (
        _coalesce_cols(df, bend_cols, pa.int64())
        if any(name in df.schema().names for name in bend_cols)
        else col("span")["bend"]
        if "span" in df.schema().names
        else _null_expr("Int64")
    )
    return bstart_expr, bend_expr


def _literal_or_null(value: object | None, dtype: pa.DataType) -> Expr:
    """Create a literal or null expression based on the value.

    Returns:
    -------
    Expr
        Literal or null expression.
    """
    if value is None:
        arrow_type = str(dtype).replace("_", "").title()
        return _null_expr(arrow_type)
    return lit(value)


def _stable_id_from_parts(prefix: str, parts: Sequence[Expr]) -> Expr:
    """Build a stable identifier from variadic expression parts.

    Args:
        prefix: Stable ID namespace prefix.
        parts: Expression parts used to build the identifier.

    Returns:
        Expr: Result.

    Raises:
        ValueError: If no parts are provided.
    """
    if not parts:
        msg = "stable identifiers require at least one part."
        raise ValueError(msg)
    return udf_expr("stable_id_parts", prefix, parts[0], *parts[1:])


def build_cpg_nodes_df(
    session_runtime: SessionRuntime, *, task_identity: TaskIdentity | None = None
) -> DataFrame:
    """Build CPG nodes DataFrame from view specs using DataFusion.

    Args:
        session_runtime: Active DataFusion session runtime.
        task_identity: Optional task identity for tagging output rows.

    Returns:
        DataFrame: Result.

    Raises:
        ValueError: If a required source table is missing.
    """
    with stage_span(
        "cpg.nodes",
        stage="cpg",
        scope_name=SCOPE_CPG,
        attributes={"codeanatomy.view_name": "cpg_nodes"},
    ):
        ctx = session_runtime.ctx
        specs = node_plan_specs()
        task_name = task_identity.name if task_identity is not None else None
        task_priority = task_identity.priority if task_identity is not None else None
        optional_prefixes = ("scip_", "type_")

        frames: list[DataFrame] = []
        for spec in specs:
            try:
                source_df = ctx.table(spec.table_ref)
            except KeyError as exc:
                if spec.table_ref.startswith(optional_prefixes):
                    continue
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

        result = combined.select(*_NODE_OUTPUT_COLUMNS)
        _require_semantic_types(
            result,
            view_name="cpg_nodes",
            expected={"node_id": "NodeId"},
            runtime_profile=session_runtime.profile,
            udf_snapshot=session_runtime.udf_snapshot,
        )
        return result


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

    Returns:
    -------
    DataFrame
        DataFrame with CPG node rows.
    """
    id_cols, _ = _prepare_id_columns_df(df, spec.id_cols)
    node_id_parts = [col(c) if c in df.schema().names else _null_expr("Utf8") for c in id_cols]
    node_id_parts.append(lit(str(spec.node_kind)))

    node_id = udf_expr("semantic_tag", "NodeId", _stable_id_from_parts("node", node_id_parts))
    node_kind = lit(str(spec.node_kind))
    path = _coalesce_cols(df, spec.path_cols, pa.string())
    bstart, bend = _span_exprs_from_df(
        df,
        bstart_cols=spec.bstart_cols,
        bend_cols=spec.bend_cols,
    )
    file_id = _coalesce_cols(df, spec.file_id_cols, pa.string())
    span_expr = udf_expr("span_make", bstart, bend)

    return df.select(
        node_id.alias("node_id"),
        node_kind.alias("node_kind"),
        path.alias("path"),
        span_expr.alias("span"),
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

    Returns:
    -------
    tuple[tuple[str, ...], tuple[str, ...]]
        Tuple of (all_columns, required_columns).
    """
    schema_names = df.schema().names
    _ = tuple(column for column in columns if column in schema_names)

    if not columns:
        return ("__id_null",), ()

    return tuple(columns), _


def build_cpg_edges_df(session_runtime: SessionRuntime) -> DataFrame:
    """Build CPG edges DataFrame from relation outputs using DataFusion.

    Args:
        session_runtime: Description.

    Returns:
        DataFrame: Result.

    Raises:
        ValueError: If the operation cannot be completed.
    """
    with stage_span(
        "cpg.edges",
        stage="cpg",
        scope_name=SCOPE_CPG,
        attributes={"codeanatomy.view_name": "cpg_edges"},
    ):
        ctx = session_runtime.ctx
        try:
            relation_df = ctx.table(RELATION_OUTPUT_NAME)
        except KeyError as exc:
            msg = f"Missing required source table {RELATION_OUTPUT_NAME!r} for CPG edges."
            raise ValueError(msg) from exc

        result = _emit_edges_from_relation_df(relation_df)
        _require_semantic_types(
            result,
            view_name="cpg_edges",
            expected={
                "edge_id": "EdgeId",
                "src_node_id": "NodeId",
                "dst_node_id": "NodeId",
            },
            runtime_profile=session_runtime.profile,
            udf_snapshot=session_runtime.udf_snapshot,
        )
        return result


def _optional_edge_expr(names: set[str], name: str, default: Expr) -> Expr:
    return col(name) if name in names else default


def _edge_span_bounds(names: set[str]) -> tuple[Expr, Expr]:
    if "bstart" in names and "bend" in names:
        return col("bstart"), col("bend")
    if "span" in names:
        return col("span")["bstart"], col("span")["bend"]
    return _null_expr("Int64"), _null_expr("Int64")


def _edge_id_expr(edge_kind: Expr, span_bstart: Expr, span_bend: Expr) -> Expr:
    base_id = _stable_id_from_parts("edge", [edge_kind, col("src"), col("dst")])
    valid_nodes = col("src").is_not_null() & col("dst").is_not_null()
    span_id_expr = udf_expr(
        "span_id",
        "edge",
        col("path"),
        span_bstart,
        span_bend,
        kind=edge_kind,
    )
    edge_id = (
        f.case(valid_nodes)
        .when(lit(value=True), f.coalesce(span_id_expr, base_id))
        .otherwise(_null_expr("Utf8"))
    )
    return udf_expr("semantic_tag", "EdgeId", edge_id)


def _edge_attr_exprs(names: set[str]) -> dict[str, Expr]:
    return {
        "origin": _optional_edge_expr(names, "origin", _null_expr("Utf8")),
        "resolution_method": _optional_edge_expr(names, "resolution_method", _null_expr("Utf8")),
        "confidence": _optional_edge_expr(names, "confidence", lit(0.5)),
        "score": _optional_edge_expr(names, "score", lit(0.5)),
        "symbol_roles": _optional_edge_expr(names, "symbol_roles", _null_expr("Int32")),
        "qname_source": _optional_edge_expr(names, "qname_source", _null_expr("Utf8")),
        "ambiguity_group_id": _optional_edge_expr(
            names,
            "ambiguity_group_id",
            _null_expr("Utf8"),
        ),
        "task_name": _optional_edge_expr(names, "task_name", _null_expr("Utf8")),
        "task_priority": _optional_edge_expr(names, "task_priority", _null_expr("Int32")),
    }


def _emit_edges_from_relation_df(df: DataFrame) -> DataFrame:
    """Emit CPG edges from relation_output rows using DataFusion.

    Parameters
    ----------
    df
        Source DataFrame with relation outputs.

    Returns:
    -------
    DataFrame
        DataFrame with CPG edge rows.
    """
    names = set(df.schema().names)
    edge_kind = _optional_edge_expr(names, "kind", _null_expr("Utf8"))
    span_bstart, span_bend = _edge_span_bounds(names)
    edge_id = _edge_id_expr(edge_kind, span_bstart, span_bend)
    attrs = _edge_attr_exprs(names)
    span_expr = udf_expr("span_make", span_bstart, span_bend)

    return df.select(
        edge_id.alias("edge_id"),
        edge_kind.alias("edge_kind"),
        udf_expr("semantic_tag", "NodeId", col("src")).alias("src_node_id"),
        udf_expr("semantic_tag", "NodeId", col("dst")).alias("dst_node_id"),
        col("path").alias("path"),
        span_expr.alias("span"),
        span_bstart.alias("bstart"),
        span_bend.alias("bend"),
        attrs["origin"].alias("origin"),
        attrs["resolution_method"].alias("resolution_method"),
        attrs["confidence"].alias("confidence"),
        attrs["score"].alias("score"),
        attrs["symbol_roles"].alias("symbol_roles"),
        attrs["qname_source"].alias("qname_source"),
        attrs["ambiguity_group_id"].alias("ambiguity_group_id"),
        attrs["task_name"].alias("task_name"),
        attrs["task_priority"].alias("task_priority"),
    )


def build_cpg_props_df(
    session_runtime: SessionRuntime,
    *,
    options: PropOptions | None = None,
    task_identity: TaskIdentity | None = None,
) -> DataFrame:
    """Build CPG properties DataFrame from prop specs using DataFusion.

    Args:
        session_runtime: Active DataFusion session runtime.
        options: Optional property-building options.
        task_identity: Optional task identity for tagging output rows.

    Returns:
        DataFrame: Result.

    Raises:
        ValueError: If a required source table is missing.
    """
    with stage_span(
        "cpg.props",
        stage="cpg",
        scope_name=SCOPE_CPG,
        attributes={"codeanatomy.view_name": "cpg_props"},
    ):
        ctx = session_runtime.ctx
        resolved_options = options or CpgPropOptions()
        source_columns_lookup = _source_columns_lookup_df(session_runtime)
        prop_specs = list(prop_table_specs(source_columns_lookup=source_columns_lookup))
        prop_specs.append(scip_role_flag_prop_spec())
        prop_specs.append(edge_prop_spec())

        optional_prefixes = ("scip_", "type_")
        optional_tables = {"cpg_edges"}

        frames: list[DataFrame] = []
        for spec in prop_specs:
            try:
                source_df = ctx.table(spec.table_ref)
            except KeyError as exc:
                if spec.table_ref in optional_tables or spec.table_ref.startswith(
                    optional_prefixes
                ):
                    continue
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


def build_cpg_props_map_df(session_runtime: SessionRuntime) -> DataFrame:
    """Build CPG property maps grouped by entity.

    Returns:
    -------
    DataFrame
        Aggregated property map rows keyed by entity.
    """
    with stage_span(
        "cpg.props_map",
        stage="cpg",
        scope_name=SCOPE_CPG,
        attributes={"codeanatomy.view_name": "cpg_props_map"},
    ):
        ctx = session_runtime.ctx
        df = ctx.table("cpg_props")
        value_struct = f.named_struct(
            [
                ("value_type", col("value_type")),
                ("value_string", col("value_string")),
                ("value_int", col("value_int")),
                ("value_float", col("value_float")),
                ("value_bool", col("value_bool")),
                ("value_json", col("value_json")),
            ]
        )
        entry = f.named_struct(
            [
                ("prop_key", col("prop_key")),
                ("value", value_struct),
            ]
        )
        return df.aggregate(
            [col("entity_kind"), col("entity_id"), col("node_kind")],
            [f.array_agg(entry).alias("props")],
        )


def build_cpg_edges_by_src_df(session_runtime: SessionRuntime) -> DataFrame:
    """Build adjacency lists grouped by source node.

    Returns:
    -------
    DataFrame
        Aggregated adjacency rows keyed by source node.
    """
    with stage_span(
        "cpg.edges_by_src",
        stage="cpg",
        scope_name=SCOPE_CPG,
        attributes={"codeanatomy.view_name": "cpg_edges_by_src"},
    ):
        ctx = session_runtime.ctx
        df = ctx.table("cpg_edges")
        names = df.schema().names
        bstart = (
            col("bstart")
            if "bstart" in names
            else col("span")["bstart"]
            if "span" in names
            else _null_expr("Int64")
        )
        bend = (
            col("bend")
            if "bend" in names
            else col("span")["bend"]
            if "span" in names
            else _null_expr("Int64")
        )
        entry = f.named_struct(
            [
                ("edge_id", col("edge_id")),
                ("edge_kind", col("edge_kind")),
                ("dst_node_id", col("dst_node_id")),
                ("path", col("path")),
                ("bstart", bstart),
                ("bend", bend),
                ("origin", col("origin")),
                ("resolution_method", col("resolution_method")),
                ("confidence", col("confidence")),
                ("score", col("score")),
                ("symbol_roles", col("symbol_roles")),
                ("qname_source", col("qname_source")),
                ("ambiguity_group_id", col("ambiguity_group_id")),
            ]
        )
        return df.aggregate(
            [col("src_node_id")],
            [f.array_agg(entry).alias("edges")],
        )


def build_cpg_edges_by_dst_df(session_runtime: SessionRuntime) -> DataFrame:
    """Build adjacency lists grouped by destination node.

    Returns:
    -------
    DataFrame
        Aggregated adjacency rows keyed by destination node.
    """
    with stage_span(
        "cpg.edges_by_dst",
        stage="cpg",
        scope_name=SCOPE_CPG,
        attributes={"codeanatomy.view_name": "cpg_edges_by_dst"},
    ):
        ctx = session_runtime.ctx
        df = ctx.table("cpg_edges")
        names = df.schema().names
        bstart = (
            col("bstart")
            if "bstart" in names
            else col("span")["bstart"]
            if "span" in names
            else _null_expr("Int64")
        )
        bend = (
            col("bend")
            if "bend" in names
            else col("span")["bend"]
            if "span" in names
            else _null_expr("Int64")
        )
        entry = f.named_struct(
            [
                ("edge_id", col("edge_id")),
                ("edge_kind", col("edge_kind")),
                ("src_node_id", col("src_node_id")),
                ("path", col("path")),
                ("bstart", bstart),
                ("bend", bend),
                ("origin", col("origin")),
                ("resolution_method", col("resolution_method")),
                ("confidence", col("confidence")),
                ("score", col("score")),
                ("symbol_roles", col("symbol_roles")),
                ("qname_source", col("qname_source")),
                ("ambiguity_group_id", col("ambiguity_group_id")),
            ]
        )
        return df.aggregate(
            [col("dst_node_id")],
            [f.array_agg(entry).alias("edges")],
        )


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

    Returns:
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

    Returns:
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

    Returns:
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

    Returns:
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

    return _stable_id_from_parts(prefix, id_parts)


def _value_columns_df(value_expr: Expr, *, value_type_str: str) -> dict[str, Expr]:
    """Create value column expressions for different types.

    Parameters
    ----------
    value_expr
        Value expression to cast.
    value_type_str
        Type of value (string, int, float, bool, json).

    Returns:
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

    Returns:
    -------
    DataFrame
        Empty DataFrame with properties schema.
    """
    return ctx.sql_with_options(
        """
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
        """,
        sql_options_for_profile(None),
    )


def _source_columns_lookup_df(
    session_runtime: SessionRuntime,
) -> Callable[[str], Sequence[str] | None]:
    """Build a columns lookup function from information_schema when available.

    Parameters
    ----------
    session_runtime
        DataFusion SessionRuntime.

    Returns:
    -------
    Callable[[str], Sequence[str] | None]
        Lookup function for table columns.
    """
    columns_by_table: dict[str, set[str]] = {}
    sql_options = sql_options_for_profile(session_runtime.profile)
    introspector = SchemaIntrospector(session_runtime.ctx, sql_options=sql_options)
    snapshot = introspector.snapshot
    if snapshot is not None:
        for row in snapshot.columns.to_pylist():
            table_name = row.get("table_name")
            column_name = row.get("column_name")
            if not isinstance(table_name, str) or not isinstance(column_name, str):
                continue
            columns_by_table.setdefault(table_name, set()).add(column_name)
    catalog = session_runtime.ctx.catalog()
    for schema_name in catalog.names():
        schema = catalog.schema(schema_name)
        for table_name in schema.names():
            try:
                table_provider = schema.table(table_name)
                arrow_schema = table_provider.schema()
                columns_by_table.setdefault(table_name, set()).update(arrow_schema.names)
            except (AttributeError, RuntimeError, TypeError, ValueError):
                # Skip tables that can't be introspected
                continue

    def _lookup(table_name: str) -> Sequence[str] | None:
        columns = columns_by_table.get(table_name)
        if not columns:
            return None
        return tuple(sorted(columns))

    return _lookup


__all__ = [
    "build_cpg_edges_by_dst_df",
    "build_cpg_edges_by_src_df",
    "build_cpg_edges_df",
    "build_cpg_nodes_df",
    "build_cpg_props_df",
    "build_cpg_props_map_df",
]

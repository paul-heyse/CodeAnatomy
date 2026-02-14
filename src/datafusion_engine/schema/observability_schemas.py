"""Schema definitions for pipeline observability events and artifacts.

Arrow schema contracts for pipeline events, plan artifacts, execution
metrics, and session runtime state. Used by the observability layer
(src/obs/) and the plan artifact store for telemetry collection.

DataFusion best practice: Use explicit schemas with semantic metadata
for observability data to enable stable querying and aggregation.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING

import pyarrow as pa
from datafusion import SessionContext

from arrow_utils.core.schema_constants import (
    SCHEMA_META_NAME,
    SCHEMA_META_VERSION,
)
from datafusion_engine.arrow.metadata import (
    function_requirements_metadata_spec,
)
from datafusion_engine.arrow.schema import (
    plan_fingerprint_field,
    task_name_field,
)
from datafusion_engine.arrow.semantic import (
    SEMANTIC_TYPE_META,
    semantic_type_for_field_name,
)
from datafusion_engine.schema.extraction_schemas import (
    AST_SPAN_META,
    AST_VIEW_REQUIRED_NON_NULL_FIELDS,
    BYTECODE_FILES_SCHEMA,
    LIBCST_FILES_SCHEMA,
    SYMTABLE_FILES_SCHEMA,
    TREE_SITTER_SPAN_META,
    _function_requirements,
    _schema_with_metadata,
    extract_base_schema_names,
)
from datafusion_engine.schema.introspection import SchemaIntrospector, table_names_snapshot
from datafusion_engine.sql.options import sql_options_for_profile
from datafusion_engine.udf.expr import udf_expr
from utils.validation import find_missing

_LOGGER = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Observability and pipeline schemas
# ---------------------------------------------------------------------------

DATAFUSION_EXPLAINS_SCHEMA = _schema_with_metadata(
    "datafusion_explains_v1",
    pa.schema(
        [
            pa.field("event_time_unix_ms", pa.int64(), nullable=False),
            pa.field("sql", pa.string(), nullable=False),
            pa.field("explain_rows_artifact_path", pa.string(), nullable=True),
            pa.field("explain_rows_artifact_format", pa.string(), nullable=True),
            pa.field("explain_rows_schema_identity_hash", pa.string(), nullable=True),
            pa.field("explain_analyze", pa.bool_(), nullable=False),
        ]
    ),
)

DATAFUSION_SCHEMA_REGISTRY_VALIDATION_SCHEMA = _schema_with_metadata(
    "datafusion_schema_registry_validation_v1",
    pa.schema(
        [
            pa.field("event_time_unix_ms", pa.int64(), nullable=False),
            pa.field("missing", pa.list_(pa.string()), nullable=True),
            pa.field("type_errors", pa.map_(pa.string(), pa.string()), nullable=True),
            pa.field("view_errors", pa.map_(pa.string(), pa.string()), nullable=True),
            pa.field("constraint_drift", pa.list_(pa.string()), nullable=True),
            pa.field(
                "relationship_constraint_errors",
                pa.map_(pa.string(), pa.string()),
                nullable=True,
            ),
            pa.field("tree_sitter_checks", pa.string(), nullable=True),
            pa.field("sql_parse_errors", pa.string(), nullable=True),
            pa.field("sql_parser_dialect", pa.string(), nullable=True),
        ]
    ),
)

DATAFUSION_SCHEMA_MAP_FINGERPRINTS_SCHEMA = _schema_with_metadata(
    "datafusion_schema_map_fingerprints_v1",
    pa.schema(
        [
            pa.field("event_time_unix_ms", pa.int64(), nullable=False),
            pa.field("schema_map_hash", pa.string(), nullable=False),
            pa.field("schema_map_version", pa.int32(), nullable=False),
            pa.field("table_count", pa.int64(), nullable=False),
            pa.field("column_count", pa.int64(), nullable=False),
        ]
    ),
)

FEATURE_STATE_SCHEMA = _schema_with_metadata(
    "feature_state_v1",
    pa.schema(
        [
            pa.field("profile_name", pa.string(), nullable=False),
            pa.field("determinism_tier", pa.string(), nullable=False),
            pa.field("dynamic_filters_enabled", pa.bool_(), nullable=False),
            pa.field("spill_enabled", pa.bool_(), nullable=False),
            pa.field("named_args_supported", pa.bool_(), nullable=False),
        ]
    ),
)

DATAFUSION_CACHE_STATE_SCHEMA = _schema_with_metadata(
    "datafusion_cache_state_v1",
    pa.schema(
        [
            pa.field("cache_name", pa.string(), nullable=False),
            pa.field("event_time_unix_ms", pa.int64(), nullable=False),
            pa.field("entry_count", pa.int64(), nullable=True),
            pa.field("hit_count", pa.int64(), nullable=True),
            pa.field("miss_count", pa.int64(), nullable=True),
            pa.field("eviction_count", pa.int64(), nullable=True),
            pa.field("config_ttl", pa.string(), nullable=True),
            pa.field("config_limit", pa.string(), nullable=True),
        ]
    ),
)

DATAFUSION_SEMANTIC_DIFF_SCHEMA = _schema_with_metadata(
    "datafusion_semantic_diff_v1",
    pa.schema(
        [
            pa.field("event_time_unix_ms", pa.int64(), nullable=False),
            pa.field("run_id", pa.string(), nullable=True),
            pa.field("plan_fingerprint", pa.string(), nullable=True),
            pa.field("base_plan_fingerprint", pa.string(), nullable=True),
            pa.field("category", pa.string(), nullable=False),
            pa.field("changed", pa.bool_(), nullable=False),
            pa.field("breaking", pa.bool_(), nullable=False),
            pa.field("row_multiplying", pa.bool_(), nullable=False),
            pa.field("change_count", pa.int64(), nullable=True),
        ]
    ),
)

DATAFUSION_RUNS_SCHEMA = _schema_with_metadata(
    "datafusion_runs_v1",
    pa.schema(
        [
            pa.field("run_id", pa.string(), nullable=False),
            pa.field("label", pa.string(), nullable=False),
            pa.field("start_time_unix_ms", pa.int64(), nullable=False),
            pa.field("end_time_unix_ms", pa.int64(), nullable=True),
            pa.field("status", pa.string(), nullable=False),
            pa.field("duration_ms", pa.int64(), nullable=True),
            pa.field("metadata", pa.string(), nullable=True),
        ]
    ),
)

DATAFUSION_PLAN_ARTIFACTS_SCHEMA = _schema_with_metadata(
    "datafusion_plan_artifacts_v10",
    pa.schema(
        [
            pa.field("event_time_unix_ms", pa.int64(), nullable=False),
            pa.field("profile_name", pa.string(), nullable=True),
            pa.field("event_kind", pa.string(), nullable=False),
            pa.field("view_name", pa.string(), nullable=False),
            plan_fingerprint_field(),
            pa.field("plan_identity_hash", pa.string(), nullable=False),
            pa.field("udf_snapshot_hash", pa.string(), nullable=False),
            pa.field("function_registry_hash", pa.string(), nullable=False),
            pa.field("required_udfs", pa.list_(pa.string()), nullable=False),
            pa.field("required_rewrite_tags", pa.list_(pa.string()), nullable=False),
            pa.field("domain_planner_names", pa.list_(pa.string()), nullable=False),
            pa.field("delta_inputs_msgpack", pa.binary(), nullable=False),
            pa.field("df_settings", pa.map_(pa.string(), pa.string()), nullable=False),
            pa.field("planning_env_msgpack", pa.binary(), nullable=False),
            pa.field("planning_env_hash", pa.string(), nullable=False),
            pa.field("rulepack_msgpack", pa.binary(), nullable=True),
            pa.field("rulepack_hash", pa.string(), nullable=True),
            pa.field("information_schema_msgpack", pa.binary(), nullable=False),
            pa.field("information_schema_hash", pa.string(), nullable=False),
            pa.field("substrait_msgpack", pa.binary(), nullable=False),
            pa.field("logical_plan_proto_msgpack", pa.binary(), nullable=True),
            pa.field("optimized_plan_proto_msgpack", pa.binary(), nullable=True),
            pa.field("execution_plan_proto_msgpack", pa.binary(), nullable=True),
            pa.field("explain_tree_rows_msgpack", pa.binary(), nullable=True),
            pa.field("explain_verbose_rows_msgpack", pa.binary(), nullable=True),
            pa.field("explain_analyze_duration_ms", pa.float64(), nullable=True),
            pa.field("explain_analyze_output_rows", pa.int64(), nullable=True),
            pa.field("substrait_validation_msgpack", pa.binary(), nullable=True),
            pa.field("lineage_msgpack", pa.binary(), nullable=False),
            pa.field("scan_units_msgpack", pa.binary(), nullable=False),
            pa.field("scan_keys", pa.list_(pa.string()), nullable=False),
            pa.field("plan_details_msgpack", pa.binary(), nullable=False),
            pa.field("plan_signals_msgpack", pa.binary(), nullable=True),
            pa.field("udf_snapshot_msgpack", pa.binary(), nullable=False),
            pa.field("udf_planner_snapshot_msgpack", pa.binary(), nullable=True),
            pa.field("udf_compatibility_ok", pa.bool_(), nullable=False),
            pa.field("udf_compatibility_detail_msgpack", pa.binary(), nullable=False),
            pa.field("execution_duration_ms", pa.float64(), nullable=True),
            pa.field("execution_status", pa.string(), nullable=True),
            pa.field("execution_error", pa.string(), nullable=True),
        ]
    ),
)

DATAFUSION_VIEW_ARTIFACTS_SCHEMA = _schema_with_metadata(
    "datafusion_view_artifacts_v4",
    pa.schema(
        [
            pa.field("event_time_unix_ms", pa.int64(), nullable=False),
            pa.field("name", pa.string(), nullable=False),
            pa.field("plan_fingerprint", pa.string(), nullable=False),
            pa.field("plan_task_signature", pa.string(), nullable=False),
            pa.field("schema_identity_hash", pa.string(), nullable=False),
            pa.field("schema_msgpack", pa.binary(), nullable=False),
            pa.field("schema_describe_msgpack", pa.binary(), nullable=True),
            pa.field("schema_provenance_msgpack", pa.binary(), nullable=True),
            pa.field("required_udfs", pa.list_(pa.string()), nullable=True),
            pa.field("referenced_tables", pa.list_(pa.string()), nullable=True),
        ]
    ),
)

PIPELINE_TASK_FACT_STRUCT = pa.struct(
    [
        task_name_field(),
        pa.field("bottom_level_cost", pa.float64(), nullable=False),
        pa.field("on_critical_path", pa.bool_(), nullable=False),
        pa.field("generation_index", pa.int64(), nullable=True),
        pa.field("schedule_index", pa.int64(), nullable=True),
    ]
)

PIPELINE_TASK_SUBMISSION_SCHEMA = _schema_with_metadata(
    "pipeline_task_submission_v1",
    pa.schema(
        [
            pa.field("run_id", pa.string(), nullable=False),
            pa.field("task_id", pa.string(), nullable=False),
            pa.field("purpose", pa.string(), nullable=False),
            pa.field("plan_signature", pa.string(), nullable=False),
            pa.field("reduced_plan_signature", pa.string(), nullable=False),
            pa.field("plan_task_count", pa.int64(), nullable=False),
            pa.field(
                "execution_task_names",
                pa.list_(pa.string()),
                nullable=False,
            ),
            pa.field("admitted_tasks", pa.list_(pa.string()), nullable=False),
            pa.field("rejected_tasks", pa.list_(pa.string()), nullable=False),
            pa.field("unknown_tasks", pa.list_(pa.string()), nullable=False),
            pa.field(
                "critical_path_tasks",
                pa.list_(pa.string()),
                nullable=False,
            ),
            pa.field(
                "task_facts",
                pa.list_(PIPELINE_TASK_FACT_STRUCT),
                nullable=False,
            ),
            pa.field("reduction_edge_count", pa.int64(), nullable=False),
            pa.field("reduction_removed_edge_count", pa.int64(), nullable=False),
        ]
    ),
)

PIPELINE_TASK_GROUPING_SCHEMA = _schema_with_metadata(
    "pipeline_task_grouping_v1",
    pa.schema(
        [
            pa.field("run_id", pa.string(), nullable=False),
            pa.field("plan_signature", pa.string(), nullable=False),
            pa.field("reduced_plan_signature", pa.string(), nullable=False),
            pa.field("task_ids", pa.list_(pa.string()), nullable=False),
            pa.field("task_count", pa.int64(), nullable=False),
        ]
    ),
)

PIPELINE_TASK_EXPANSION_SCHEMA = _schema_with_metadata(
    "pipeline_task_expansion_v1",
    pa.schema(
        [
            pa.field("run_id", pa.string(), nullable=False),
            pa.field("plan_signature", pa.string(), nullable=False),
            pa.field("task_id", pa.string(), nullable=False),
            pa.field("parameter_keys", pa.list_(pa.string()), nullable=False),
            pa.field("parameter_count", pa.int64(), nullable=False),
        ]
    ),
)

PIPELINE_PLAN_DRIFT_SCHEMA = _schema_with_metadata(
    "pipeline_plan_drift_v1",
    pa.schema(
        [
            pa.field("run_id", pa.string(), nullable=False),
            pa.field("plan_signature", pa.string(), nullable=False),
            pa.field("reduced_plan_signature", pa.string(), nullable=False),
            pa.field("plan_task_count", pa.int64(), nullable=False),
            pa.field("admitted_task_count", pa.int64(), nullable=False),
            pa.field("admitted_task_coverage_ratio", pa.float64(), nullable=False),
            pa.field(
                "missing_admitted_tasks",
                pa.list_(pa.string()),
                nullable=False,
            ),
            pa.field(
                "unexpected_admitted_tasks",
                pa.list_(pa.string()),
                nullable=False,
            ),
            pa.field("plan_generation_count", pa.int64(), nullable=False),
            pa.field("plan_generations", pa.list_(pa.int64()), nullable=False),
            pa.field("admitted_generation_count", pa.int64(), nullable=False),
            pa.field("admitted_generations", pa.list_(pa.int64()), nullable=False),
            pa.field("missing_generations", pa.list_(pa.int64()), nullable=False),
            pa.field("submission_event_count", pa.int64(), nullable=False),
            pa.field("grouping_event_count", pa.int64(), nullable=False),
            pa.field("expansion_event_count", pa.int64(), nullable=False),
        ]
    ),
)

DATAFUSION_PIPELINE_EVENTS_V2_SCHEMA = _schema_with_metadata(
    "datafusion_pipeline_events_v2",
    pa.schema(
        [
            pa.field("event_time_unix_ms", pa.int64(), nullable=False),
            pa.field("profile_name", pa.string(), nullable=True),
            pa.field("run_id", pa.string(), nullable=False),
            pa.field("event_name", pa.string(), nullable=False),
            pa.field("plan_signature", pa.string(), nullable=False),
            pa.field("reduced_plan_signature", pa.string(), nullable=False),
            pa.field("event_payload_msgpack", pa.binary(), nullable=False),
            pa.field("event_payload_hash", pa.string(), nullable=False),
        ]
    ),
)

DATAFUSION_SQL_INGEST_SCHEMA = _schema_with_metadata(
    "datafusion_sql_ingest_v1",
    pa.schema(
        [
            pa.field("event_time_unix_ms", pa.int64(), nullable=False),
            pa.field("ingest_kind", pa.string(), nullable=False),
            pa.field("source_name", pa.string(), nullable=True),
            pa.field("sql", pa.string(), nullable=False),
            pa.field("schema", pa.map_(pa.string(), pa.string()), nullable=True),
            pa.field("dialect", pa.string(), nullable=True),
        ]
    ),
)

ENGINE_RUNTIME_SCHEMA = _schema_with_metadata(
    "engine_runtime_v2",
    pa.schema(
        [
            pa.field("event_time_unix_ms", pa.int64(), nullable=False),
            pa.field("runtime_profile_name", pa.string(), nullable=False),
            pa.field("determinism_tier", pa.string(), nullable=False),
            pa.field("runtime_profile_hash", pa.string(), nullable=False),
            pa.field("runtime_profile_snapshot", pa.binary(), nullable=False),
            pa.field("function_registry_hash", pa.string(), nullable=True),
            pa.field("datafusion_settings_hash", pa.string(), nullable=True),
            pa.field("datafusion_settings", pa.binary(), nullable=True),
        ]
    ),
)

DATAFUSION_UDF_VALIDATION_SCHEMA = _schema_with_metadata(
    "datafusion_udf_validation_v1",
    pa.schema(
        [
            pa.field("event_time_unix_ms", pa.int64(), nullable=False),
            pa.field("udf_catalog_policy", pa.string(), nullable=False),
            pa.field("missing_udfs", pa.list_(pa.string()), nullable=True),
            pa.field("missing_count", pa.int32(), nullable=True),
        ]
    ),
)

DATAFUSION_OBJECT_STORES_SCHEMA = _schema_with_metadata(
    "datafusion_object_stores_v1",
    pa.schema(
        [
            pa.field("event_time_unix_ms", pa.int64(), nullable=False),
            pa.field("scheme", pa.string(), nullable=False),
            pa.field("host", pa.string(), nullable=True),
            pa.field("store_type", pa.string(), nullable=True),
        ]
    ),
)

# Apply function requirements metadata to ENGINE_RUNTIME_SCHEMA
_ENGINE_FUNCTION_REQUIREMENTS = function_requirements_metadata_spec(
    required=(
        "array_agg",
        "array_distinct",
        "array_sort",
        "array_to_string",
        "bool_or",
        "coalesce",
        "col_to_byte",
        "concat_ws",
        "prefixed_hash64",
        "row_number",
        "sha256",
        "stable_hash64",
        "stable_hash128",
        "stable_id",
    ),
).schema_metadata

ENGINE_RUNTIME_SCHEMA = _schema_with_metadata(
    "engine_runtime_v2",
    ENGINE_RUNTIME_SCHEMA,
    extra_metadata=_ENGINE_FUNCTION_REQUIREMENTS,
)


# ---------------------------------------------------------------------------
# Validation functions
# ---------------------------------------------------------------------------


def validate_schema_metadata(schema: pa.Schema) -> None:
    """Validate required schema metadata tags.

    Args:
        schema: Description.

    Raises:
        ValueError: If the operation cannot be completed.
    """
    meta = schema.metadata or {}
    if SCHEMA_META_NAME not in meta:
        msg = "Schema metadata missing schema_name."
        raise ValueError(msg)
    if SCHEMA_META_VERSION not in meta:
        msg = "Schema metadata missing schema_version."
        raise ValueError(msg)


def schema_contract_for_table(ctx: SessionContext, *, table_name: str) -> SchemaContract:
    """Return the SchemaContract for a registered DataFusion table.

    Returns:
    -------
    SchemaContract
        Schema contract derived from the catalog schema.
    """
    from datafusion_engine.schema.introspection import schema_contract_from_table

    return schema_contract_from_table(ctx, table_name=table_name)


def _require_semantic_type(
    ctx: SessionContext,
    *,
    table_name: str,
    column_name: str,
    expected: str,
    allow_row_probe_fallback: bool = True,
) -> None:
    try:
        table_schema = ctx.table(table_name).schema()
    except (RuntimeError, TypeError, ValueError, KeyError) as exc:
        msg = f"Unable to resolve schema for {table_name!r}: {exc}"
        raise ValueError(msg) from exc
    arrow_schema = _arrow_schema_from_dfschema(table_schema)
    semantic_type: str | None = None
    if arrow_schema is not None:
        try:
            field = arrow_schema.field(column_name)
        except KeyError as exc:
            msg = f"Missing required column {table_name}.{column_name}."
            raise ValueError(msg) from exc
        semantic_type = _semantic_type_from_field_metadata(field)
    if semantic_type is None and allow_row_probe_fallback:
        semantic_type = _semantic_type_via_row_probe(
            ctx,
            table_name=table_name,
            column_name=column_name,
        )
    if semantic_type is None:
        msg = f"Missing semantic type metadata on {table_name}.{column_name}."
        raise ValueError(msg)
    if semantic_type != expected:
        msg = (
            f"Semantic type mismatch for {table_name}.{column_name}: "
            f"expected {expected!r}, got {semantic_type!r}."
        )
        raise ValueError(msg)


def _semantic_type_from_field_metadata(field: pa.Field) -> str | None:
    metadata = field.metadata or {}
    raw = metadata.get(SEMANTIC_TYPE_META)
    if raw is None:
        return None
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError:
        return str(raw)


def _semantic_type_via_row_probe(
    ctx: SessionContext,
    *,
    table_name: str,
    column_name: str,
) -> str | None:
    from datafusion import col as df_col

    meta_key = SEMANTIC_TYPE_META.decode("utf-8")
    df = ctx.table(table_name).select(
        udf_expr("arrow_metadata", df_col(column_name), meta_key).alias("semantic_type")
    )
    rows = df.limit(1).to_arrow_table().to_pylist()
    semantic_type = rows[0].get("semantic_type") if rows else None
    if semantic_type is None:
        return None
    return str(semantic_type)


def _semantic_validation_tables() -> tuple[str, ...]:
    tables = set(extract_base_schema_names())
    tables.update({"cpg_nodes", "cpg_edges"})
    return tuple(sorted(tables))


def validate_semantic_types(
    ctx: SessionContext,
    *,
    table_names: Sequence[str] | None = None,
    allow_row_probe_fallback: bool = True,
) -> None:
    """Validate semantic type metadata for known tables."""
    names = table_names or _semantic_validation_tables()
    sql_options = sql_options_for_profile(None)
    introspector = SchemaIntrospector(ctx, sql_options=sql_options)
    for name in names:
        if not ctx.table_exist(name):
            continue
        column_names = introspector.table_column_names(name)
        for column_name in column_names:
            semantic = semantic_type_for_field_name(column_name)
            if semantic is None:
                continue
            _require_semantic_type(
                ctx,
                table_name=name,
                column_name=column_name,
                expected=semantic.name,
                allow_row_probe_fallback=allow_row_probe_fallback,
            )


def validate_ast_views(
    ctx: SessionContext,
    *,
    view_names: Sequence[str] | None = None,
) -> None:
    """No-op: AST view schemas are derived dynamically at registration time."""
    _ = (ctx, view_names)


def _validate_ast_file_types(ctx: SessionContext, errors: dict[str, str]) -> None:
    from datafusion import col as df_col
    from datafusion import functions as df_f

    try:
        df = ctx.table("ast_files_v1")
        exprs = [
            df_f.arrow_typeof(df_col("nodes")).alias("nodes_type"),
            df_f.arrow_typeof(df_col("edges")).alias("edges_type"),
            df_f.arrow_typeof(df_col("errors")).alias("errors_type"),
            df_f.arrow_typeof(df_col("docstrings")).alias("docstrings_type"),
            df_f.arrow_typeof(df_col("imports")).alias("imports_type"),
            df_f.arrow_typeof(df_col("defs")).alias("defs_type"),
            df_f.arrow_typeof(df_col("calls")).alias("calls_type"),
            df_f.arrow_typeof(df_col("type_ignores")).alias("type_ignores_type"),
        ]
        df.select(*exprs).limit(1).collect()
    except (RuntimeError, TypeError, ValueError) as exc:
        errors["ast_files_v1"] = str(exc)


def validate_ts_views(ctx: SessionContext) -> None:
    """No-op: tree-sitter view schemas are derived dynamically at registration time."""
    _ = ctx


def validate_symtable_views(ctx: SessionContext) -> None:
    """No-op: symtable view schemas are derived dynamically at registration time."""
    _ = ctx


def validate_scip_views(ctx: SessionContext) -> None:
    """No-op: SCIP view schemas are derived dynamically at registration time."""
    _ = ctx


def _function_names(ctx: SessionContext) -> set[str]:
    try:
        return SchemaIntrospector(ctx, sql_options=sql_options_for_profile(None)).function_names()
    except (RuntimeError, TypeError, ValueError):
        return set()


@dataclass(frozen=True)
class FunctionCatalog:
    """Introspected function catalog for DataFusion sessions."""

    function_names: frozenset[str]
    signature_counts: Mapping[str, int]
    parameter_signatures: Mapping[str, list[tuple[tuple[str, ...], tuple[str, ...]]]]
    parameters_available: bool

    @classmethod
    def from_information_schema(
        cls,
        *,
        routines: Sequence[Mapping[str, object]],
        parameters: Sequence[Mapping[str, object]],
        parameters_available: bool = True,
    ) -> FunctionCatalog:
        """Build a function catalog from information_schema rows.

        Returns:
        -------
        FunctionCatalog
            Populated function catalog instance.
        """
        names: set[str] = set()
        for row in routines:
            value = row.get("routine_name") or row.get("function_name") or row.get("name")
            if isinstance(value, str):
                names.add(value.lower())
        counts = _parameter_counts(parameters)
        signatures = _parameter_signatures(parameters)
        return cls(
            function_names=frozenset(names),
            signature_counts=counts,
            parameter_signatures=signatures,
            parameters_available=parameters_available,
        )


def _function_catalog(ctx: SessionContext) -> FunctionCatalog | None:
    introspector = SchemaIntrospector(ctx, sql_options=sql_options_for_profile(None))
    routines: list[dict[str, object]]
    parameters: list[dict[str, object]]
    try:
        routines = introspector.routines_snapshot()
    except (RuntimeError, TypeError, ValueError):
        routines = []
    routines = list(routines)
    try:
        parameters = introspector.parameters_snapshot()
        parameters_available = True
    except (RuntimeError, TypeError, ValueError):
        parameters = []
        parameters_available = False
    parameters = list(parameters)
    if not routines:
        return None
    return FunctionCatalog.from_information_schema(
        routines=routines,
        parameters=parameters,
        parameters_available=parameters_available,
    )


def _validate_required_functions(
    ctx: SessionContext,
    *,
    required: Sequence[str],
    errors: dict[str, str],
    catalog: FunctionCatalog | None = None,
) -> None:
    resolved = catalog or _function_catalog(ctx)
    available = set(resolved.function_names) if resolved is not None else set()
    try:
        from datafusion_engine.udf.extension_runtime import (
            rust_udf_snapshot,
            udf_names_from_snapshot,
        )

        snapshot = rust_udf_snapshot(ctx)
        available.update(name.lower() for name in udf_names_from_snapshot(snapshot))
    except (RuntimeError, TypeError, ValueError):
        pass
    if not available:
        errors["datafusion_functions"] = "information_schema.routines returned no entries."
        return
    missing_lower = find_missing([name.lower() for name in required], available)
    missing = sorted(name for name in required if name.lower() in missing_lower)
    if missing:
        errors["datafusion_functions"] = f"Missing required functions: {missing}."


def _signature_name(row: Mapping[str, object]) -> str | None:
    for key in ("specific_name", "routine_name", "function_name", "name"):
        value = row.get(key)
        if isinstance(value, str):
            return value
    return None


def _parameter_signatures(
    rows: Sequence[Mapping[str, object]],
) -> dict[str, list[tuple[tuple[str, ...], tuple[str, ...]]]]:
    signatures: dict[tuple[str, str], list[tuple[int, str, str]]] = {}
    for row in rows:
        name = _signature_name(row)
        if name is None:
            continue
        specific = row.get("specific_name")
        signature_id = specific if isinstance(specific, str) else name.lower()
        ordinal = row.get("ordinal_position")
        data_type = row.get("data_type")
        mode = row.get("parameter_mode")
        if not isinstance(ordinal, int) or data_type is None or mode is None:
            continue
        signatures.setdefault((name.lower(), signature_id), []).append(
            (ordinal, str(data_type), str(mode))
        )
    grouped: dict[str, list[tuple[tuple[str, ...], tuple[str, ...]]]] = {}
    for (name, _), entries in signatures.items():
        entries.sort(key=lambda entry: entry[0])
        types = tuple(dtype for _, dtype, _ in entries)
        modes = tuple(mode for _, _, mode in entries)
        grouped.setdefault(name, []).append((types, modes))
    return grouped


def _matches_type(value: str, tokens: frozenset[str]) -> bool:
    lowered = value.lower()
    return any(token in lowered for token in tokens)


def _signature_matches_hints(
    types: Sequence[str],
    hints: Sequence[frozenset[str] | None],
) -> bool:
    if len(types) < len(hints):
        return False
    for dtype, tokens in zip(types, hints, strict=False):
        if tokens is None:
            continue
        if not _matches_type(dtype, tokens):
            return False
    return True


def _signature_has_input_modes(
    entries: Sequence[tuple[tuple[str, ...], tuple[str, ...]]],
) -> bool:
    return any(all(mode.lower() == "in" for mode in modes) for _, modes in entries)


def _signature_matches_required_types(
    entries: Sequence[tuple[tuple[str, ...], tuple[str, ...]]],
    hints: Sequence[frozenset[str] | None],
) -> bool:
    if not hints:
        return True
    return any(_signature_matches_hints(types, hints) for types, _ in entries)


def _signature_type_validation(
    required: Mapping[str, Sequence[frozenset[str] | None]],
    signatures: Mapping[str, list[tuple[tuple[str, ...], tuple[str, ...]]]],
) -> dict[str, list[str]]:
    missing_types: list[str] = []
    mode_mismatches: list[str] = []
    for name, hints in required.items():
        entries = signatures.get(name.lower())
        if not entries:
            continue
        if not _signature_has_input_modes(entries):
            mode_mismatches.append(name)
        if not _signature_matches_required_types(entries, hints):
            missing_types.append(name)
    details: dict[str, list[str]] = {}
    if missing_types:
        details["type_mismatches"] = missing_types
    if mode_mismatches:
        details["mode_mismatches"] = mode_mismatches
    return details


def _validate_function_signatures(
    ctx: SessionContext,
    *,
    required: Mapping[str, int],
    errors: dict[str, str],
    catalog: FunctionCatalog | None = None,
) -> None:
    resolved = catalog or _function_catalog(ctx)
    if resolved is None:
        errors["datafusion_function_signatures"] = "information_schema.parameters returned no rows."
        return
    if not resolved.parameters_available:
        return
    counts = resolved.signature_counts
    if not counts:
        errors["datafusion_function_signatures"] = "information_schema.parameters returned no rows."
        return
    signature_errors = _signature_errors(required, counts)
    if signature_errors:
        errors["datafusion_function_signatures"] = str(signature_errors)


def _validate_function_signature_types(
    ctx: SessionContext,
    *,
    required: Mapping[str, Sequence[frozenset[str] | None]],
    errors: dict[str, str],
    catalog: FunctionCatalog | None = None,
) -> None:
    resolved = catalog or _function_catalog(ctx)
    if resolved is None:
        errors["datafusion_function_signature_types"] = (
            "information_schema.parameters returned no rows."
        )
        return
    if not resolved.parameters_available:
        return
    signatures = resolved.parameter_signatures
    if not signatures:
        errors["datafusion_function_signature_types"] = (
            "information_schema.parameters returned no rows."
        )
        return
    details = _signature_type_validation(required, signatures)
    if details:
        errors["datafusion_function_signature_types"] = str(details)


def _parameter_counts(rows: Sequence[Mapping[str, object]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        name = _signature_name(row)
        if name is None:
            continue
        normalized = name.lower()
        counts[normalized] = counts.get(normalized, 0) + 1
    return counts


def _signature_errors(
    required: Mapping[str, int],
    counts: Mapping[str, int],
) -> dict[str, list[str]]:
    missing_lower = find_missing([name.lower() for name in required], counts)
    missing = [name for name in required if name.lower() in missing_lower]
    mismatched = _mismatched_signatures(required, counts)
    details: dict[str, list[str]] = {}
    if missing:
        details["missing"] = missing
    if mismatched:
        details["mismatched"] = mismatched
    return details


def _mismatched_signatures(
    required: Mapping[str, int],
    counts: Mapping[str, int],
) -> list[str]:
    mismatched: list[str] = []
    for name, min_args in required.items():
        count = counts.get(name.lower())
        if count is not None and count < min_args:
            mismatched.append(f"{name} ({count} < {min_args})")
    return mismatched


def _describe_column_names(rows: Sequence[Mapping[str, object]]) -> tuple[str, ...]:
    names: list[str] = []
    for row in rows:
        for key in ("column_name", "name", "column"):
            value = row.get(key)
            if value is not None:
                names.append(str(value))
                break
    return tuple(names)


def _invalid_output_names(names: Sequence[str]) -> tuple[str, ...]:
    invalid = []
    for name in names:
        if not name:
            invalid.append(name)
            continue
        if any(token in name for token in (".", "(", ")", "{", "}", " ")):
            invalid.append(name)
    return tuple(invalid)


def _normalize_meta_value(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value)


def _ast_span_expected_meta() -> dict[str, str]:
    return {
        "line_base": _normalize_meta_value(AST_SPAN_META[b"line_base"]) or "0",
        "col_unit": _normalize_meta_value(AST_SPAN_META[b"col_unit"]) or "byte",
        "end_exclusive": _normalize_meta_value(AST_SPAN_META[b"end_exclusive"]) or "true",
    }


def _ts_span_expected_meta() -> dict[str, str]:
    return {
        "line_base": _normalize_meta_value(TREE_SITTER_SPAN_META[b"line_base"]) or "0",
        "col_unit": _normalize_meta_value(TREE_SITTER_SPAN_META[b"col_unit"]) or "byte",
        "end_exclusive": _normalize_meta_value(TREE_SITTER_SPAN_META[b"end_exclusive"]) or "true",
    }


def _validate_ast_span_metadata(ctx: SessionContext, errors: dict[str, str]) -> None:
    expected = _ast_span_expected_meta()
    try:
        table = ctx.table("ast_span_metadata").to_arrow_table()
    except (RuntimeError, TypeError, ValueError) as exc:
        errors["ast_span_metadata"] = str(exc)
        return
    rows = table.to_pylist()
    if not rows:
        return
    row = rows[0]
    prefixes = (
        "nodes",
        "errors",
        "docstrings",
        "imports",
        "defs",
        "calls",
        "type_ignores",
    )
    mismatches: dict[str, dict[str, str]] = {}
    for prefix in prefixes:
        for key, expected_value in expected.items():
            column = f"{prefix}_{key}"
            actual = _normalize_meta_value(row.get(column))
            if actual is None:
                mismatches.setdefault(prefix, {})[key] = "missing"
                continue
            if actual.lower() != expected_value.lower():
                mismatches.setdefault(prefix, {})[key] = actual
    if mismatches:
        errors["ast_span_metadata_values"] = str(mismatches)


def _validate_ts_span_metadata(ctx: SessionContext, errors: dict[str, str]) -> None:
    expected = _ts_span_expected_meta()
    try:
        table = ctx.table("ts_span_metadata").to_arrow_table()
    except (RuntimeError, TypeError, ValueError) as exc:
        errors["ts_span_metadata"] = str(exc)
        return
    rows = table.to_pylist()
    if not rows:
        return
    row = rows[0]
    prefixes = (
        "nodes",
        "errors",
        "missing",
        "captures",
        "defs",
        "calls",
        "imports",
        "docstrings",
    )
    mismatches: dict[str, dict[str, str]] = {}
    for prefix in prefixes:
        for key, expected_value in expected.items():
            column = f"{prefix}_{key}"
            actual = _normalize_meta_value(row.get(column))
            if actual is None:
                mismatches.setdefault(prefix, {})[key] = "missing"
                continue
            if actual.lower() != expected_value.lower():
                mismatches.setdefault(prefix, {})[key] = actual
    if mismatches:
        errors["ts_span_metadata_values"] = str(mismatches)


def _validate_ast_view_dfschema(
    ctx: SessionContext,
    *,
    name: str,
    errors: dict[str, str],
) -> None:
    try:
        df_schema = ctx.table(name).schema()
    except (RuntimeError, TypeError, ValueError, KeyError) as exc:
        errors[f"{name}_dfschema"] = str(exc)
        return
    df_names = _dfschema_names(df_schema)
    df_invalid = _invalid_output_names(df_names)
    if df_invalid:
        errors[f"{name}_dfschema_names"] = f"Invalid DFSchema names: {df_invalid}"
    nullability = _dfschema_nullability(df_schema)
    if not nullability:
        return
    nullable_required = [
        field for field in AST_VIEW_REQUIRED_NON_NULL_FIELDS if nullability.get(field) is True
    ]
    if nullable_required:
        errors[f"{name}_dfschema_nullability"] = (
            f"Expected non-nullable fields are nullable: {sorted(nullable_required)}"
        )


def _validate_ast_view_outputs(
    ctx: SessionContext,
    *,
    introspector: SchemaIntrospector,
    name: str,
    errors: dict[str, str],
) -> None:
    try:
        schema = introspector.table_schema(name)
    except (RuntimeError, TypeError, ValueError) as exc:
        errors[name] = str(exc)
        return
    columns = [field.name for field in schema]
    invalid = _invalid_output_names(columns)
    if invalid:
        errors[f"{name}_output_names"] = f"Invalid output column names: {invalid}"
    _validate_ast_view_dfschema(ctx, name=name, errors=errors)


def _validate_ts_view_dfschema(
    ctx: SessionContext,
    *,
    name: str,
    errors: dict[str, str],
) -> None:
    try:
        df_schema = ctx.table(name).schema()
    except (RuntimeError, TypeError, ValueError, KeyError) as exc:
        errors[f"{name}_dfschema"] = str(exc)
        return
    df_names = _dfschema_names(df_schema)
    df_invalid = _invalid_output_names(df_names)
    if df_invalid:
        errors[f"{name}_dfschema_names"] = f"Invalid DFSchema names: {df_invalid}"


def _validate_ts_view_outputs(
    ctx: SessionContext,
    *,
    introspector: SchemaIntrospector,
    name: str,
    errors: dict[str, str],
) -> None:
    try:
        schema = introspector.table_schema(name)
    except (RuntimeError, TypeError, ValueError) as exc:
        errors[name] = str(exc)
        return
    columns = [field.name for field in schema]
    invalid = _invalid_output_names(columns)
    if invalid:
        errors[f"{name}_output_names"] = f"Invalid output column names: {invalid}"
    _validate_ts_view_dfschema(ctx, name=name, errors=errors)
    try:
        actual = introspector.table_column_names(name)
        missing = sorted(set(columns) - actual)
        if missing:
            errors[f"{name}_information_schema"] = f"Missing columns: {missing}."
    except (RuntimeError, TypeError, ValueError, KeyError) as exc:
        errors[f"{name}_information_schema"] = str(exc)


def _arrow_schema_from_dfschema(schema: object) -> pa.Schema | None:
    if isinstance(schema, pa.Schema):
        return schema
    to_arrow = getattr(schema, "to_arrow", None)
    if callable(to_arrow):
        resolved = to_arrow()
        if isinstance(resolved, pa.Schema):
            return resolved
    return None


def _dfschema_names(schema: object) -> tuple[str, ...]:
    from collections.abc import Sequence as SeqABC
    from typing import cast

    if isinstance(schema, pa.Schema):
        arrow_schema = cast("pa.Schema", schema)
        return tuple(arrow_schema.names)
    names_attr = getattr(schema, "names", None)
    if isinstance(names_attr, SeqABC) and not isinstance(
        names_attr,
        (str, bytes, bytearray),
    ):
        return tuple(str(name) for name in names_attr)
    fields_attr = getattr(schema, "fields", None)
    if callable(fields_attr):
        fields = fields_attr()
        if isinstance(fields, SeqABC):
            return tuple(str(getattr(field, "name", field)) for field in fields)
    return ()


def _dfschema_nullability(schema: object) -> dict[str, bool] | None:
    arrow_schema = _arrow_schema_from_dfschema(schema)
    if arrow_schema is None:
        return None
    return {field.name: field.nullable for field in arrow_schema}


def validate_bytecode_views(ctx: SessionContext) -> None:
    """Validate bytecode view schemas using DataFusion introspection."""
    _ = ctx


def validate_cst_views(ctx: SessionContext) -> None:
    """No-op: CST view schemas are derived dynamically at registration time."""
    _ = ctx


def validate_required_cst_functions(ctx: SessionContext) -> None:
    """Validate required CST functions and signatures.

    Args:
        ctx: Description.

    Raises:
        ValueError: If the operation cannot be completed.
    """
    errors: dict[str, str] = {}
    function_catalog = _function_catalog(ctx)
    requirements = _function_requirements(LIBCST_FILES_SCHEMA)
    _validate_required_functions(
        ctx,
        required=requirements.required,
        errors=errors,
        catalog=function_catalog,
    )
    if requirements.signature_counts:
        _validate_function_signatures(
            ctx,
            required=requirements.signature_counts,
            errors=errors,
            catalog=function_catalog,
        )
    if errors:
        msg = f"Required CST functions validation failed: {errors}."
        raise ValueError(msg)


def validate_required_symtable_functions(ctx: SessionContext) -> None:
    """Validate required symtable functions and signatures.

    Args:
        ctx: Description.

    Raises:
        ValueError: If the operation cannot be completed.
    """
    errors: dict[str, str] = {}
    function_catalog = _function_catalog(ctx)
    requirements = _function_requirements(SYMTABLE_FILES_SCHEMA)
    _validate_required_functions(
        ctx,
        required=requirements.required,
        errors=errors,
        catalog=function_catalog,
    )
    if requirements.signature_counts:
        _validate_function_signatures(
            ctx,
            required=requirements.signature_counts,
            errors=errors,
            catalog=function_catalog,
        )
    if requirements.signature_types:
        _validate_function_signature_types(
            ctx,
            required=requirements.signature_types,
            errors=errors,
            catalog=function_catalog,
        )
    if errors:
        msg = f"Required symtable functions validation failed: {errors}."
        raise ValueError(msg)


def validate_required_bytecode_functions(ctx: SessionContext) -> None:
    """Validate required bytecode functions and signatures.

    Args:
        ctx: Description.

    Raises:
        ValueError: If the operation cannot be completed.
    """
    errors: dict[str, str] = {}
    function_catalog = _function_catalog(ctx)
    requirements = _function_requirements(BYTECODE_FILES_SCHEMA)
    _validate_required_functions(
        ctx,
        required=requirements.required,
        errors=errors,
        catalog=function_catalog,
    )
    if requirements.signature_counts:
        _validate_function_signatures(
            ctx,
            required=requirements.signature_counts,
            errors=errors,
            catalog=function_catalog,
        )
    if requirements.signature_types:
        _validate_function_signature_types(
            ctx,
            required=requirements.signature_types,
            errors=errors,
            catalog=function_catalog,
        )
    if errors:
        msg = f"Required bytecode functions validation failed: {errors}."
        raise ValueError(msg)


def validate_required_engine_functions(ctx: SessionContext) -> None:
    """Validate required engine-level functions.

    Args:
        ctx: Description.

    Raises:
        ValueError: If the operation cannot be completed.
    """
    from datafusion_engine.udf.extension_runtime import udf_backend_available

    if not udf_backend_available():
        return
    errors: dict[str, str] = {}
    function_catalog = _function_catalog(ctx)
    requirements = _function_requirements(ENGINE_RUNTIME_SCHEMA)
    _validate_required_functions(
        ctx,
        required=requirements.required,
        errors=errors,
        catalog=function_catalog,
    )
    if errors:
        msg = f"Required engine functions validation failed: {errors}."
        raise ValueError(msg)


def validate_udf_info_schema_parity(ctx: SessionContext) -> None:
    """Validate UDF parity against information_schema.

    Args:
        ctx: Description.

    Raises:
        ValueError: If the operation cannot be completed.
    """
    from datafusion_engine.udf.parity import udf_info_schema_parity_report

    report = udf_info_schema_parity_report(ctx)
    if report.error is not None:
        msg = f"UDF information_schema parity failed: {report.error}"
        raise ValueError(msg)
    if report.missing_in_information_schema:
        msg = (
            "UDF information_schema parity failed; missing routines: "
            f"{list(report.missing_in_information_schema)}"
        )
        raise ValueError(msg)


def registered_table_names(ctx: SessionContext) -> set[str]:
    """Return registered table names from information_schema.

    Returns:
    -------
    set[str]
        Set of table names present in the session catalog.
    """
    return table_names_snapshot(ctx)


def missing_schema_names(
    ctx: SessionContext,
    *,
    expected: Sequence[str] | None = None,
) -> tuple[str, ...]:
    """Return missing schema names from information_schema.

    Returns:
    -------
    tuple[str, ...]
        Sorted tuple of missing schema names.
    """
    if expected is None:
        return ()
    expected_names = set(expected)
    registered = registered_table_names(ctx)
    missing = sorted(expected_names - registered)
    return tuple(missing)


if TYPE_CHECKING:
    from datafusion_engine.schema.contracts import SchemaContract

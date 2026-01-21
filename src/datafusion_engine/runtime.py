"""Runtime profile helpers for DataFusion execution."""

from __future__ import annotations

import contextlib
import importlib
import logging
import os
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING, Literal, Protocol, cast

import datafusion
import pyarrow as pa
from datafusion import RuntimeEnvBuilder, SessionConfig, SessionContext
from datafusion.dataframe import DataFrame
from datafusion.object_store import LocalFileSystem
from sqlglot.errors import ParseError

from arrowdsl.core.determinism import DeterminismTier
from arrowdsl.core.schema_constants import DEFAULT_VALUE_META
from arrowdsl.schema.serialization import schema_fingerprint
from datafusion_engine.catalog_provider import register_registry_catalogs
from datafusion_engine.compile_options import (
    DataFusionCacheEvent,
    DataFusionCompileOptions,
    DataFusionFallbackEvent,
    DataFusionSqlPolicy,
    resolve_sql_policy,
)
from datafusion_engine.expr_planner import expr_planner_payloads, install_expr_planners
from datafusion_engine.function_factory import function_factory_payloads, install_function_factory
from datafusion_engine.query_fragments import fragment_view_specs
from datafusion_engine.registry_bridge import register_dataset_df
from datafusion_engine.schema_introspection import SchemaIntrospector
from datafusion_engine.schema_registry import (
    AST_CORE_VIEW_NAMES,
    AST_OPTIONAL_VIEW_NAMES,
    CST_VIEW_NAMES,
    SCIP_VIEW_SCHEMA_MAP,
    TREE_SITTER_CHECK_VIEWS,
    TREE_SITTER_VIEW_NAMES,
    missing_schema_names,
    nested_schema_names,
    nested_view_specs,
    register_all_schemas,
    schema_for,
    schema_names,
    validate_ast_views,
    validate_bytecode_views,
    validate_cst_views,
    validate_nested_types,
    validate_scip_views,
    validate_symtable_views,
    validate_ts_views,
)
from datafusion_engine.udf_registry import DataFusionUdfSnapshot, register_datafusion_udfs
from engine.plan_cache import PlanCache
from ibis_engine.registry import DatasetCatalog, DatasetLocation
from registry_common.arrow_payloads import payload_hash
from schema_spec.policies import DataFusionWritePolicy
from schema_spec.system import (
    DataFusionScanOptions,
    DeltaScanOptions,
    TableSchemaContract,
)
from schema_spec.view_specs import ViewSpec
from sqlglot_tools.optimizer import parse_sql_strict, register_datafusion_dialect

if TYPE_CHECKING:
    from ibis.expr.types import Value as IbisValue

    from arrowdsl.core.interop import RecordBatchReaderLike, TableLike

    ExplainRows = TableLike | RecordBatchReaderLike
else:
    ExplainRows = object


class DiagnosticsSink(Protocol):
    """Protocol for diagnostics sinks used by DataFusion runtime."""

    def record_events(self, name: str, rows: Sequence[Mapping[str, object]]) -> None:
        """Record event rows for a named diagnostics table."""
        ...

    def record_artifact(self, name: str, payload: Mapping[str, object]) -> None:
        """Record an artifact payload for diagnostics sinks."""
        ...

    def events_snapshot(self) -> dict[str, list[Mapping[str, object]]]:
        """Return collected event rows."""
        ...

    def artifacts_snapshot(self) -> dict[str, list[Mapping[str, object]]]:
        """Return collected artifact payloads."""
        ...


MemoryPool = Literal["greedy", "fair", "unbounded"]

logger = logging.getLogger(__name__)

KIB: int = 1024
MIB: int = 1024 * KIB
GIB: int = 1024 * MIB

SETTINGS_HASH_VERSION: int = 1
TELEMETRY_PAYLOAD_VERSION: int = 2

_MAP_ENTRY_SCHEMA = pa.struct(
    [
        pa.field("key", pa.string()),
        pa.field("value_kind", pa.string()),
        pa.field("value", pa.string()),
    ]
)
_SETTINGS_HASH_SCHEMA = pa.schema(
    [
        pa.field("version", pa.int32()),
        pa.field("entries", pa.list_(_MAP_ENTRY_SCHEMA)),
    ]
)
_SQL_POLICY_SCHEMA = pa.struct(
    [
        pa.field("allow_ddl", pa.bool_()),
        pa.field("allow_dml", pa.bool_()),
        pa.field("allow_statements", pa.bool_()),
    ]
)
_WRITE_POLICY_SCHEMA = pa.struct(
    [
        pa.field("partition_by", pa.list_(pa.string())),
        pa.field("single_file_output", pa.bool_()),
        pa.field("sort_by", pa.list_(pa.string())),
        pa.field("parquet_compression", pa.string()),
        pa.field("parquet_statistics_enabled", pa.string()),
        pa.field("parquet_row_group_size", pa.int64()),
    ]
)
_SPILL_SCHEMA = pa.struct(
    [
        pa.field("spill_dir", pa.string()),
        pa.field("memory_pool", pa.string()),
        pa.field("memory_limit_bytes", pa.int64()),
    ]
)
_EXECUTION_SCHEMA = pa.struct(
    [
        pa.field("target_partitions", pa.int64()),
        pa.field("batch_size", pa.int64()),
    ]
)
_SQL_SURFACES_SCHEMA = pa.struct(
    [
        pa.field("enable_information_schema", pa.bool_()),
        pa.field("enable_url_table", pa.bool_()),
        pa.field("sql_parser_dialect", pa.string()),
        pa.field("ansi_mode", pa.bool_()),
    ]
)
_EXTENSIONS_SCHEMA = pa.struct(
    [
        pa.field("delta_plan_codecs_enabled", pa.bool_()),
        pa.field("delta_plan_codec_physical", pa.string()),
        pa.field("delta_plan_codec_logical", pa.string()),
        pa.field("expr_planners_enabled", pa.bool_()),
        pa.field("expr_planner_names", pa.list_(pa.string())),
        pa.field("named_args_supported", pa.bool_()),
        pa.field("distributed", pa.bool_()),
        pa.field("distributed_context_factory", pa.bool_()),
    ]
)
_OUTPUT_WRITES_SCHEMA = pa.struct(
    [
        pa.field("cache_enabled", pa.bool_()),
        pa.field("cache_max_columns", pa.int64()),
        pa.field("datafusion_write_policy", _WRITE_POLICY_SCHEMA),
    ]
)
_TELEMETRY_SCHEMA = pa.schema(
    [
        pa.field("version", pa.int32()),
        pa.field("profile_name", pa.string()),
        pa.field("datafusion_version", pa.string()),
        pa.field("sql_policy_name", pa.string()),
        pa.field("session_config", pa.list_(_MAP_ENTRY_SCHEMA)),
        pa.field("settings_hash", pa.string()),
        pa.field("external_table_options", pa.list_(_MAP_ENTRY_SCHEMA)),
        pa.field("sql_policy", _SQL_POLICY_SCHEMA),
        pa.field("param_identifier_allowlist", pa.list_(pa.string())),
        pa.field("write_policy", _WRITE_POLICY_SCHEMA),
        pa.field("feature_gates", pa.list_(_MAP_ENTRY_SCHEMA)),
        pa.field("join_policy", pa.list_(_MAP_ENTRY_SCHEMA)),
        pa.field("parquet_read", pa.list_(_MAP_ENTRY_SCHEMA)),
        pa.field("listing_table", pa.list_(_MAP_ENTRY_SCHEMA)),
        pa.field("spill", _SPILL_SCHEMA),
        pa.field("execution", _EXECUTION_SCHEMA),
        pa.field("sql_surfaces", _SQL_SURFACES_SCHEMA),
        pa.field("extensions", _EXTENSIONS_SCHEMA),
        pa.field("substrait_validation", pa.bool_()),
        pa.field("output_writes", _OUTPUT_WRITES_SCHEMA),
    ]
)


def _parse_major_version(version: str) -> int | None:
    major = version.split(".", maxsplit=1)[0]
    if major.isdigit():
        return int(major)
    return None


def _catalog_autoload_settings() -> dict[str, str]:
    location = os.environ.get("CODEANATOMY_DATAFUSION_CATALOG_LOCATION", "").strip()
    file_format = os.environ.get("CODEANATOMY_DATAFUSION_CATALOG_FORMAT", "").strip()
    settings: dict[str, str] = {}
    if location:
        settings["datafusion.catalog.location"] = location
    if file_format:
        settings["datafusion.catalog.format"] = file_format
    return settings


def _ansi_mode(settings: Mapping[str, str]) -> bool | None:
    dialect = settings.get("datafusion.sql_parser.dialect")
    if dialect is None:
        return None
    return str(dialect).lower() == "ansi"


def _supports_explain_analyze_level() -> bool:
    if DATAFUSION_MAJOR_VERSION is None:
        return False
    return DATAFUSION_MAJOR_VERSION >= DATAFUSION_RUNTIME_SETTINGS_SKIP_VERSION


DATAFUSION_MAJOR_VERSION: int | None = _parse_major_version(datafusion.__version__)
DATAFUSION_RUNTIME_SETTINGS_SKIP_VERSION: int = 51
DATAFUSION_OPTIMIZER_DYNAMIC_FILTER_SKIP_VERSION: int = 51
_AST_OPTIONAL_VIEW_FUNCTIONS: dict[str, tuple[str, ...]] = {
    "ast_node_attrs": ("map_entries",),
    "ast_def_attrs": ("map_entries",),
    "ast_call_attrs": ("map_entries",),
    "ast_edge_attrs": ("map_entries",),
    "ast_span_metadata": ("arrow_metadata",),
}


@dataclass(frozen=True)
class DataFusionConfigPolicy:
    """Configuration policy for DataFusion SessionConfig."""

    settings: Mapping[str, str]

    def apply(self, config: SessionConfig) -> SessionConfig:
        """Return a SessionConfig with policy settings applied.

        Returns
        -------
        datafusion.SessionConfig
            Session config with policy settings applied.
        """
        skip_runtime_settings = (
            DATAFUSION_MAJOR_VERSION is not None
            and DATAFUSION_MAJOR_VERSION >= DATAFUSION_RUNTIME_SETTINGS_SKIP_VERSION
        )
        for key, value in self.settings.items():
            if skip_runtime_settings and key.startswith("datafusion.runtime."):
                continue
            config = config.set(key, value)
        return config


@dataclass(frozen=True)
class DataFusionFeatureGates:
    """Feature gate toggles for DataFusion optimizer behavior."""

    enable_dynamic_filter_pushdown: bool = True
    enable_join_dynamic_filter_pushdown: bool = True
    enable_aggregate_dynamic_filter_pushdown: bool = True
    enable_topk_dynamic_filter_pushdown: bool = True

    def settings(self) -> dict[str, str]:
        """Return DataFusion config settings for the feature gates.

        Returns
        -------
        dict[str, str]
            Mapping of DataFusion config keys to string values.
        """
        settings = {
            "datafusion.optimizer.enable_dynamic_filter_pushdown": str(
                self.enable_dynamic_filter_pushdown
            ).lower(),
            "datafusion.optimizer.enable_join_dynamic_filter_pushdown": str(
                self.enable_join_dynamic_filter_pushdown
            ).lower(),
            "datafusion.optimizer.enable_aggregate_dynamic_filter_pushdown": str(
                self.enable_aggregate_dynamic_filter_pushdown
            ).lower(),
            "datafusion.optimizer.enable_topk_dynamic_filter_pushdown": str(
                self.enable_topk_dynamic_filter_pushdown
            ).lower(),
        }
        if (
            DATAFUSION_MAJOR_VERSION is not None
            and DATAFUSION_MAJOR_VERSION >= DATAFUSION_OPTIMIZER_DYNAMIC_FILTER_SKIP_VERSION
        ):
            settings.pop("datafusion.optimizer.enable_aggregate_dynamic_filter_pushdown", None)
        return settings


@dataclass(frozen=True)
class DataFusionJoinPolicy:
    """Join algorithm preferences for DataFusion."""

    enable_hash_join: bool = True
    enable_sort_merge_join: bool = True
    enable_nested_loop_join: bool = True
    repartition_joins: bool = True

    def settings(self) -> dict[str, str]:
        """Return DataFusion config settings for join preferences.

        Returns
        -------
        dict[str, str]
            Mapping of DataFusion config keys to string values.
        """
        return {
            "datafusion.optimizer.enable_hash_join": str(self.enable_hash_join).lower(),
            "datafusion.optimizer.enable_sort_merge_join": str(self.enable_sort_merge_join).lower(),
            "datafusion.optimizer.enable_nested_loop_join": str(
                self.enable_nested_loop_join
            ).lower(),
            "datafusion.optimizer.repartition_joins": str(self.repartition_joins).lower(),
        }


@dataclass(frozen=True)
class DataFusionSettingsContract:
    """Settings contract for DataFusion session configuration."""

    settings: Mapping[str, str]
    feature_gates: DataFusionFeatureGates

    def apply(self, config: SessionConfig) -> SessionConfig:
        """Return a SessionConfig with settings and feature gates applied.

        Returns
        -------
        datafusion.SessionConfig
            Session config with settings applied.
        """
        merged = {**self.settings, **self.feature_gates.settings()}
        for key, value in merged.items():
            config = config.set(key, value)
        return config


@dataclass(frozen=True)
class SchemaHardeningProfile:
    """Schema-stability settings for DataFusion SessionConfig."""

    enable_view_types: bool = False
    expand_views_at_output: bool = False
    timezone: str = "UTC"
    parser_dialect: str | None = None
    show_schema_in_explain: bool = True
    show_types_in_format: bool = True
    strict_aggregate_schema_check: bool = True

    def settings(self) -> dict[str, str]:
        """Return DataFusion settings for schema hardening.

        Returns
        -------
        dict[str, str]
            Mapping of DataFusion config keys to string values.
        """
        settings = {
            "datafusion.explain.show_schema": str(self.show_schema_in_explain).lower(),
            "datafusion.format.types_info": str(self.show_types_in_format).lower(),
            "datafusion.execution.time_zone": str(self.timezone),
            "datafusion.execution.skip_physical_aggregate_schema_check": str(
                not self.strict_aggregate_schema_check
            ).lower(),
            "datafusion.sql_parser.map_string_types_to_utf8view": str(
                self.enable_view_types
            ).lower(),
            "datafusion.execution.parquet.schema_force_view_types": str(
                self.enable_view_types
            ).lower(),
            "datafusion.optimizer.expand_views_at_output": str(self.expand_views_at_output).lower(),
        }
        if self.parser_dialect is not None:
            settings["datafusion.sql_parser.dialect"] = self.parser_dialect
        return settings

    def apply(self, config: SessionConfig) -> SessionConfig:
        """Return SessionConfig with schema hardening settings applied.

        Returns
        -------
        datafusion.SessionConfig
            Updated session config with schema hardening settings.
        """
        for key, value in self.settings().items():
            config = config.set(key, value)
        return config


@dataclass(frozen=True)
class FeatureStateSnapshot:
    """Snapshot of runtime feature gates and determinism tier."""

    profile_name: str
    determinism_tier: DeterminismTier
    dynamic_filters_enabled: bool
    spill_enabled: bool
    named_args_supported: bool

    def to_row(self) -> dict[str, object]:
        """Return a row mapping for diagnostics sinks.

        Returns
        -------
        dict[str, object]
            Row mapping for diagnostics table ingestion.
        """
        return {
            "profile_name": self.profile_name,
            "determinism_tier": self.determinism_tier.value,
            "dynamic_filters_enabled": self.dynamic_filters_enabled,
            "spill_enabled": self.spill_enabled,
            "named_args_supported": self.named_args_supported,
        }


def feature_state_snapshot(
    *,
    profile_name: str,
    determinism_tier: DeterminismTier,
    runtime_profile: DataFusionRuntimeProfile | None,
) -> FeatureStateSnapshot:
    """Build a feature state snapshot for diagnostics.

    Returns
    -------
    FeatureStateSnapshot
        Snapshot describing runtime feature state.
    """
    if runtime_profile is None:
        return FeatureStateSnapshot(
            profile_name=profile_name,
            determinism_tier=determinism_tier,
            dynamic_filters_enabled=False,
            spill_enabled=False,
            named_args_supported=False,
        )
    gates = runtime_profile.feature_gates
    dynamic_filters_enabled = (
        gates.enable_dynamic_filter_pushdown
        and gates.enable_join_dynamic_filter_pushdown
        and gates.enable_aggregate_dynamic_filter_pushdown
        and gates.enable_topk_dynamic_filter_pushdown
    )
    spill_enabled = runtime_profile.spill_dir is not None
    return FeatureStateSnapshot(
        profile_name=profile_name,
        determinism_tier=determinism_tier,
        dynamic_filters_enabled=dynamic_filters_enabled,
        spill_enabled=spill_enabled,
        named_args_supported=runtime_profile.named_args_supported(),
    )


@dataclass
class DataFusionExplainCollector:
    """Collect EXPLAIN artifacts for diagnostics."""

    entries: list[dict[str, object]] = field(default_factory=list)

    def hook(self, sql: str, rows: ExplainRows) -> None:
        """Collect an explain payload for a single statement."""
        payload = {"sql": sql, "rows": rows}
        self.entries.append(cast("dict[str, object]", payload))

    def snapshot(self) -> list[dict[str, object]]:
        """Return a snapshot of explain artifacts.

        Returns
        -------
        list[dict[str, object]]
            Collected explain artifacts.
        """
        return list(self.entries)


@dataclass
class DataFusionPlanCollector:
    """Collect DataFusion plan artifacts."""

    entries: list[dict[str, object]] = field(default_factory=list)

    def hook(self, payload: Mapping[str, object]) -> None:
        """Collect a plan artifact payload."""
        self.entries.append(dict(payload))

    def snapshot(self) -> list[dict[str, object]]:
        """Return a snapshot of plan artifacts.

        Returns
        -------
        list[dict[str, object]]
            Plan artifact payloads.
        """
        return list(self.entries)


@dataclass
class DataFusionViewRegistry:
    """Record DataFusion view definitions for reproducibility."""

    entries: dict[str, str | None] = field(default_factory=dict)

    def record(self, *, name: str, sql: str | None) -> None:
        """Record a view definition by name."""
        self.entries[name] = sql

    def snapshot(self) -> list[dict[str, object]]:
        """Return a stable snapshot of registered views.

        Returns
        -------
        list[dict[str, object]]
            Ordered view definitions with name and SQL entries.
        """
        return [
            {"name": name, "sql": sql}
            for name, sql in sorted(self.entries.items(), key=lambda item: item[0])
        ]


@dataclass
class DataFusionFallbackCollector:
    """Collect SQL fallback events for diagnostics."""

    entries: list[dict[str, object]] = field(default_factory=list)

    def hook(self, event: DataFusionFallbackEvent) -> None:
        """Collect a fallback event payload."""
        payload = {
            "reason": event.reason,
            "error": event.error,
            "expression_type": event.expression_type,
            "sql": event.sql,
            "dialect": event.dialect,
            "policy_violations": list(event.policy_violations),
            "sql_policy_name": event.sql_policy_name,
            "param_mode": event.param_mode,
        }
        self.entries.append(cast("dict[str, object]", payload))

    def snapshot(self) -> list[dict[str, object]]:
        """Return a snapshot of fallback artifacts.

        Returns
        -------
        list[dict[str, object]]
            Collected fallback artifacts.
        """
        return list(self.entries)


@dataclass(frozen=True)
class PreparedStatementSpec:
    """Prepared statement specification for DataFusion."""

    name: str
    sql: str
    param_types: tuple[str, ...] = ()


@dataclass(frozen=True)
class AdapterExecutionPolicy:
    """Execution policy for adapterized fallback handling."""

    allow_fallback: bool = True
    fail_on_fallback: bool = False
    force_sql: bool = False


@dataclass(frozen=True)
class ExecutionLabel:
    """Execution label for rule-scoped diagnostics."""

    rule_name: str
    output_dataset: str


DEFAULT_DF_POLICY = DataFusionConfigPolicy(
    settings={
        "datafusion.execution.collect_statistics": "true",
        "datafusion.execution.meta_fetch_concurrency": "8",
        "datafusion.execution.planning_concurrency": "8",
        "datafusion.execution.parquet.pushdown_filters": "true",
        "datafusion.execution.parquet.max_predicate_cache_size": str(64 * MIB),
        "datafusion.execution.parquet.enable_page_index": "true",
        "datafusion.execution.parquet.metadata_size_hint": "1048576",
        "datafusion.runtime.list_files_cache_limit": str(128 * MIB),
        "datafusion.runtime.list_files_cache_ttl": "2m",
        "datafusion.runtime.metadata_cache_limit": str(256 * MIB),
        "datafusion.runtime.memory_limit": str(8 * GIB),
        "datafusion.runtime.temp_directory": "/tmp/datafusion",
        "datafusion.runtime.max_temp_directory_size": str(100 * GIB),
    }
)

CST_AUTOLOAD_DF_POLICY = DataFusionConfigPolicy(
    settings={**DEFAULT_DF_POLICY.settings, **_catalog_autoload_settings()}
)

SYMTABLE_DF_POLICY = DataFusionConfigPolicy(
    settings={
        **DEFAULT_DF_POLICY.settings,
        "datafusion.execution.collect_statistics": "false",
        "datafusion.execution.meta_fetch_concurrency": "8",
        "datafusion.runtime.list_files_cache_limit": str(64 * MIB),
        "datafusion.runtime.list_files_cache_ttl": "1m",
        "datafusion.execution.listing_table_factory_infer_partitions": "false",
        "datafusion.explain.show_schema": "true",
        "datafusion.format.types_info": "true",
        "datafusion.execution.time_zone": "UTC",
        "datafusion.sql_parser.map_string_types_to_utf8view": "false",
        "datafusion.execution.parquet.schema_force_view_types": "false",
        "datafusion.optimizer.expand_views_at_output": "false",
    }
)

DEV_DF_POLICY = DataFusionConfigPolicy(
    settings={
        "datafusion.execution.collect_statistics": "true",
        "datafusion.execution.meta_fetch_concurrency": "4",
        "datafusion.execution.planning_concurrency": "2",
        "datafusion.execution.parquet.pushdown_filters": "true",
        "datafusion.execution.parquet.max_predicate_cache_size": str(32 * MIB),
        "datafusion.execution.parquet.enable_page_index": "true",
        "datafusion.execution.parquet.metadata_size_hint": "524288",
        "datafusion.runtime.list_files_cache_limit": str(64 * MIB),
        "datafusion.runtime.list_files_cache_ttl": "2m",
        "datafusion.runtime.metadata_cache_limit": str(128 * MIB),
        "datafusion.runtime.memory_limit": str(4 * GIB),
        "datafusion.runtime.temp_directory": "/tmp/datafusion",
        "datafusion.runtime.max_temp_directory_size": str(50 * GIB),
    }
)

PROD_DF_POLICY = DataFusionConfigPolicy(
    settings={
        "datafusion.execution.collect_statistics": "true",
        "datafusion.execution.meta_fetch_concurrency": "16",
        "datafusion.execution.planning_concurrency": "16",
        "datafusion.execution.parquet.pushdown_filters": "true",
        "datafusion.execution.parquet.max_predicate_cache_size": str(128 * MIB),
        "datafusion.execution.parquet.enable_page_index": "true",
        "datafusion.execution.parquet.metadata_size_hint": "2097152",
        "datafusion.runtime.list_files_cache_limit": str(256 * MIB),
        "datafusion.runtime.list_files_cache_ttl": "5m",
        "datafusion.runtime.metadata_cache_limit": str(512 * MIB),
        "datafusion.runtime.memory_limit": str(16 * GIB),
        "datafusion.runtime.temp_directory": "/tmp/datafusion",
        "datafusion.runtime.max_temp_directory_size": str(200 * GIB),
    }
)

DATAFUSION_POLICY_PRESETS: Mapping[str, DataFusionConfigPolicy] = {
    "cst_autoload": CST_AUTOLOAD_DF_POLICY,
    "dev": DEV_DF_POLICY,
    "default": DEFAULT_DF_POLICY,
    "prod": PROD_DF_POLICY,
    "symtable": SYMTABLE_DF_POLICY,
}

SCHEMA_HARDENING_PRESETS: Mapping[str, SchemaHardeningProfile] = {
    "schema_hardening": SchemaHardeningProfile(),
    "arrow_performance": SchemaHardeningProfile(enable_view_types=True),
}

CST_DIAGNOSTIC_STATEMENTS: tuple[PreparedStatementSpec, ...] = (
    PreparedStatementSpec(
        name="cst_refs_by_file",
        sql="SELECT * FROM cst_refs WHERE file_id = $1",
        param_types=("Utf8",),
    ),
    PreparedStatementSpec(
        name="cst_defs_by_file",
        sql="SELECT * FROM cst_defs WHERE file_id = $1",
        param_types=("Utf8",),
    ),
    PreparedStatementSpec(
        name="cst_callsites_by_file",
        sql="SELECT * FROM cst_callsites WHERE file_id = $1",
        param_types=("Utf8",),
    ),
)

_SESSION_CONTEXT_CACHE: dict[str, SessionContext] = {}


def snapshot_plans(df: DataFrame) -> dict[str, object]:
    """Return logical/optimized/physical plan snapshots for diagnostics.

    Returns
    -------
    dict[str, object]
        Plan snapshots keyed by logical/optimized/physical.
    """
    return {
        "logical": df.logical_plan(),
        "optimized": df.optimized_logical_plan(),
        "physical": df.execution_plan(),
    }


def _prepare_statement_sql(statement: PreparedStatementSpec) -> str:
    sql = statement.sql.strip()
    if sql.lower().startswith("prepare "):
        return sql
    if statement.param_types:
        params = ", ".join(statement.param_types)
        return f"PREPARE {statement.name}({params}) AS {statement.sql}"
    return f"PREPARE {statement.name} AS {statement.sql}"


def _table_logical_plan(ctx: SessionContext, *, name: str) -> str:
    try:
        module = importlib.import_module("datafusion_ext")
    except ImportError:
        module = None
    if module is not None:
        fn = getattr(module, "table_logical_plan", None)
        if callable(fn):
            return str(fn(ctx, name))
    df = ctx.table(name)
    return str(df.logical_plan())


def _table_dfschema_tree(ctx: SessionContext, *, name: str) -> str:
    try:
        module = importlib.import_module("datafusion_ext")
    except ImportError:
        module = None
    if module is not None:
        fn = getattr(module, "table_dfschema_tree", None)
        if callable(fn):
            return str(fn(ctx, name))
    df = ctx.table(name)
    schema = df.schema()
    tree_string = getattr(schema, "tree_string", None)
    if callable(tree_string):
        return str(tree_string())
    return str(schema)


def _sql_parser_dialect(profile: DataFusionRuntimeProfile) -> str:
    resolved = profile._resolved_schema_hardening()
    if resolved is not None and resolved.parser_dialect:
        return resolved.parser_dialect
    return "datafusion"


def _sql_parse_errors(sql: str, *, dialect: str) -> list[dict[str, object]] | None:
    register_datafusion_dialect()
    try:
        parse_sql_strict(sql, dialect=dialect)
    except ParseError as exc:
        errors: list[dict[str, object]] = []
        for item in exc.errors:
            if not isinstance(item, Mapping):
                continue
            entry: dict[str, object] = {}
            for key in (
                "description",
                "line",
                "col",
                "start_context",
                "highlight",
                "end_context",
                "into_expression",
            ):
                value = item.get(key)
                if value is not None:
                    entry[key] = value
            if entry:
                errors.append(entry)
        if not errors:
            errors.append({"message": str(exc)})
        return errors
    except (RuntimeError, TypeError, ValueError) as exc:
        return [{"message": str(exc)}]
    return None


def _collect_view_sql_parse_errors(
    registry: DataFusionViewRegistry,
    *,
    dialect: str,
) -> dict[str, list[dict[str, object]]] | None:
    errors: dict[str, list[dict[str, object]]] = {}
    for name, sql in registry.entries.items():
        if not isinstance(sql, str) or not sql:
            continue
        parse_errors = _sql_parse_errors(sql, dialect=dialect)
        if parse_errors:
            errors[name] = parse_errors
    return errors or None


def register_view_specs(
    ctx: SessionContext,
    *,
    views: Sequence[ViewSpec],
    runtime_profile: DataFusionRuntimeProfile | None = None,
    validate: bool = True,
) -> None:
    """Register view specs and optionally record their definitions.

    Parameters
    ----------
    ctx:
        DataFusion session context used for registration.
    views:
        View specifications to register.
    runtime_profile:
        Optional runtime profile for recording view definitions.
    validate:
        Whether to validate view schemas after registration.
    """
    record_view = None
    if runtime_profile is not None:
        profile = runtime_profile

        def _record_view(name: str, sql: str | None) -> None:
            profile.record_view_definition(name=name, sql=sql)

        record_view = _record_view
    for view in views:
        view.register(
            ctx,
            record_view=record_view,
            validate=validate,
        )


def _register_schema_table(ctx: SessionContext, name: str, schema: pa.Schema) -> None:
    """Register a schema-only table via an empty table provider."""
    arrays = [pa.array([], type=field.type) for field in schema]
    table = pa.Table.from_arrays(arrays, schema=schema)
    ctx.register_table(name, table)


def _apply_config_int(
    config: SessionConfig,
    *,
    method: str,
    key: str,
    value: int,
) -> SessionConfig:
    updater = getattr(config, method, None)
    if callable(updater):
        return cast("SessionConfig", updater(value))
    setter = getattr(config, "set", None)
    if callable(setter):
        return cast("SessionConfig", setter(key, str(value)))
    return config


def _apply_optional_int_config(
    config: SessionConfig,
    *,
    method: str,
    key: str,
    value: int | None,
) -> SessionConfig:
    if value is None:
        return config
    return _apply_config_int(config, method=method, key=key, value=int(value))


def _apply_config_policy(
    config: SessionConfig,
    policy: DataFusionConfigPolicy | None,
) -> SessionConfig:
    if policy is None:
        return config
    return policy.apply(config)


def _apply_settings_overrides(
    config: SessionConfig,
    overrides: Mapping[str, str],
) -> SessionConfig:
    for key, value in overrides.items():
        config = config.set(key, str(value))
    return config


def _apply_catalog_autoload(
    config: SessionConfig,
    *,
    location: str | None,
    file_format: str | None,
) -> SessionConfig:
    if location is not None:
        config = config.set("datafusion.catalog.location", location)
    if file_format is not None:
        config = config.set("datafusion.catalog.format", file_format)
    return config


def _apply_schema_hardening(
    config: SessionConfig,
    schema_hardening: SchemaHardeningProfile | None,
) -> SessionConfig:
    if schema_hardening is None:
        return config
    return schema_hardening.apply(config)


def _apply_feature_settings(
    config: SessionConfig,
    feature_gates: DataFusionFeatureGates | None,
) -> SessionConfig:
    if feature_gates is None:
        return config
    for key, value in feature_gates.settings().items():
        try:
            config = config.set(key, value)
        except Exception as exc:  # pragma: no cover - defensive against FFI config panics.
            message = str(exc)
            if "Config value" in message and "not found" in message:
                continue
            raise
    return config


def _load_schema_evolution_adapter_factory() -> object:
    """Return a schema evolution adapter factory from the native extension.

    Returns
    -------
    object
        Adapter factory instance exposed by the native extension.

    Raises
    ------
    RuntimeError
        Raised when the native extension is missing.
    TypeError
        Raised when the adapter factory is not callable.
    """
    try:
        module = importlib.import_module("datafusion_ext")
    except ImportError as exc:  # pragma: no cover - optional dependency
        msg = "Schema evolution adapter requires datafusion_ext."
        raise RuntimeError(msg) from exc
    factory = getattr(module, "schema_evolution_adapter_factory", None)
    if not callable(factory):
        msg = "Schema evolution adapter factory is not available in datafusion_ext."
        raise TypeError(msg)
    return factory()


def _install_schema_evolution_adapter_factory(ctx: SessionContext) -> None:
    """Install the schema evolution adapter factory via the native extension.

    Parameters
    ----------
    ctx:
        DataFusion session context to update.

    Raises
    ------
    RuntimeError
        Raised when the native extension is missing.
    TypeError
        Raised when the native installer is not callable.
    """
    try:
        module = importlib.import_module("datafusion_ext")
    except ImportError as exc:  # pragma: no cover - optional dependency
        msg = "Schema evolution adapter requires datafusion_ext."
        raise RuntimeError(msg) from exc
    installer = getattr(module, "install_schema_evolution_adapter_factory", None)
    if not callable(installer):
        msg = "Schema evolution adapter installer is not available in datafusion_ext."
        raise TypeError(msg)
    installer(ctx)


def _apply_join_settings(
    config: SessionConfig,
    join_policy: DataFusionJoinPolicy | None,
) -> SessionConfig:
    if join_policy is None:
        return config
    for key, value in join_policy.settings().items():
        config = config.set(key, value)
    return config


def _apply_explain_analyze_level(
    config: SessionConfig,
    level: str | None,
) -> SessionConfig:
    if level is None or not _supports_explain_analyze_level():
        return config
    return config.set("datafusion.explain.analyze_level", level)


def _settings_by_prefix(payload: Mapping[str, str], prefix: str) -> dict[str, str]:
    return {key: value for key, value in payload.items() if key.startswith(prefix)}


def _map_entries(payload: Mapping[str, object]) -> list[dict[str, object]]:
    return [
        _map_entry(key, value)
        for key, value in sorted(payload.items(), key=lambda item: str(item[0]))
    ]


def _map_entry(key: object, value: object) -> dict[str, object]:
    return {
        "key": str(key),
        "value_kind": _value_kind(value),
        "value": _value_text(value),
    }


def _value_kind(value: object) -> str:
    kind = "string"
    if value is None:
        kind = "null"
    elif isinstance(value, bool):
        kind = "bool"
    elif isinstance(value, int):
        kind = "int64"
    elif isinstance(value, float):
        kind = "float64"
    elif isinstance(value, bytes):
        kind = "binary"
    return kind


def _value_text(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, bytes):
        return value.hex()
    if isinstance(value, str):
        return value
    if isinstance(value, (bool, int, float)):
        return str(value)
    return _stable_repr(value)


def _stable_repr(value: object) -> str:
    if isinstance(value, Mapping):
        items = ", ".join(
            f"{_stable_repr(key)}:{_stable_repr(val)}"
            for key, val in sorted(value.items(), key=lambda item: str(item[0]))
        )
        return f"{{{items}}}"
    if isinstance(value, (list, tuple, set)):
        rendered = [_stable_repr(item) for item in value]
        if isinstance(value, set):
            rendered = sorted(rendered)
        items = ", ".join(rendered)
        bracket = "()" if isinstance(value, tuple) else "[]"
        return f"{bracket[0]}{items}{bracket[1]}"
    return repr(value)


def _function_catalog_sort_key(row: Mapping[str, object]) -> tuple[str, str]:
    name = row.get("function_name")
    func_name = str(name) if name is not None else ""
    func_type = row.get("function_type")
    return func_name, str(func_type) if func_type is not None else ""


def _information_schema_routines(ctx: SessionContext) -> list[dict[str, object]]:
    try:
        table = ctx.sql("SELECT * FROM information_schema.routines").to_arrow_table()
    except (RuntimeError, TypeError, ValueError):
        return []
    rows: list[dict[str, object]] = []
    for row in table.to_pylist():
        payload = dict(row)
        if "routine_name" in payload and "function_name" not in payload:
            payload["function_name"] = payload["routine_name"]
        if "routine_type" in payload and "function_type" not in payload:
            payload["function_type"] = payload["routine_type"]
        payload.setdefault("source", "information_schema")
        rows.append(payload)
    return rows


def _information_schema_parameters(ctx: SessionContext) -> list[dict[str, object]]:
    try:
        table = ctx.sql("SELECT * FROM information_schema.parameters").to_arrow_table()
    except (RuntimeError, TypeError, ValueError):
        return []
    rows: list[dict[str, object]] = []
    for row in table.to_pylist():
        payload = dict(row)
        if "routine_name" in payload and "function_name" not in payload:
            payload["function_name"] = payload["routine_name"]
        payload.setdefault("source", "information_schema")
        rows.append(payload)
    return rows


def _datafusion_version(ctx: SessionContext) -> str | None:
    try:
        table = ctx.sql("SELECT version() AS version").to_arrow_table()
    except (RuntimeError, TypeError, ValueError):
        return None
    if "version" not in table.column_names or table.num_rows < 1:
        return None
    values = table["version"].to_pylist()
    value = values[0] if values else None
    return str(value) if value is not None else None


def _datafusion_function_names(ctx: SessionContext) -> set[str]:
    try:
        table = ctx.sql("SHOW FUNCTIONS").to_arrow_table()
    except (RuntimeError, TypeError, ValueError):
        return set()
    names: set[str] = set()
    for row in table.to_pylist():
        for key in ("function_name", "name"):
            value = row.get(key)
            if isinstance(value, str):
                names.add(value.lower())
                break
    return names


def _default_value_entries(schema: pa.Schema) -> list[dict[str, str]]:
    entries: list[dict[str, str]] = []

    def _walk_field(field: pa.Field, *, prefix: str) -> None:
        path = f"{prefix}.{field.name}" if prefix else field.name
        meta = field.metadata or {}
        default_value = meta.get(DEFAULT_VALUE_META)
        if default_value is not None:
            entries.append(
                {
                    "path": path,
                    "default_value": default_value.decode("utf-8", errors="replace"),
                }
            )
        _walk_dtype(field.type, prefix=path)

    def _walk_dtype(dtype: pa.DataType, *, prefix: str) -> None:
        if pa.types.is_struct(dtype):
            for child in dtype:
                _walk_field(child, prefix=prefix)
            return
        if (
            pa.types.is_list(dtype)
            or pa.types.is_large_list(dtype)
            or pa.types.is_list_view(dtype)
            or pa.types.is_large_list_view(dtype)
        ):
            _walk_dtype(dtype.value_type, prefix=prefix)
            return
        if pa.types.is_map(dtype):
            _walk_dtype(dtype.item_type, prefix=prefix)

    for schema_field in schema:
        _walk_field(schema_field, prefix="")
    return entries


def _datafusion_write_policy_payload(
    policy: DataFusionWritePolicy | None,
) -> dict[str, object] | None:
    if policy is None:
        return None
    return policy.payload()


def _apply_builder(
    builder: RuntimeEnvBuilder,
    *,
    method: str,
    args: tuple[object, ...],
) -> RuntimeEnvBuilder:
    updater = getattr(builder, method, None)
    if callable(updater):
        return cast("RuntimeEnvBuilder", updater(*args))
    return builder


def _apply_fallback_policy(
    *,
    policy: AdapterExecutionPolicy | None,
    fallback_hook: Callable[[DataFusionFallbackEvent], None] | None,
    label: ExecutionLabel | None = None,
) -> Callable[[DataFusionFallbackEvent], None] | None:
    if policy is None:
        return fallback_hook
    if policy.allow_fallback and not policy.fail_on_fallback:
        return fallback_hook

    def _hook(event: DataFusionFallbackEvent) -> None:
        if fallback_hook is not None:
            fallback_hook(event)
        label_info = ""
        if label is not None:
            label_info = f" for rule {label.rule_name!r} output {label.output_dataset!r}"
        msg = f"DataFusion fallback blocked{label_info} ({event.reason}): {event.expression_type}"
        raise ValueError(msg)

    return _hook


def _chain_fallback_hooks(
    *hooks: Callable[[DataFusionFallbackEvent], None] | None,
) -> Callable[[DataFusionFallbackEvent], None] | None:
    active = [hook for hook in hooks if hook is not None]
    if not active:
        return None

    def _hook(event: DataFusionFallbackEvent) -> None:
        for hook in active:
            hook(event)

    return _hook


def _chain_explain_hooks(
    *hooks: Callable[[str, ExplainRows], None] | None,
) -> Callable[[str, ExplainRows], None] | None:
    active = [hook for hook in hooks if hook is not None]
    if not active:
        return None

    def _hook(sql: str, rows: ExplainRows) -> None:
        for hook in active:
            hook(sql, rows)

    return _hook


def _chain_plan_artifacts_hooks(
    *hooks: Callable[[Mapping[str, object]], None] | None,
) -> Callable[[Mapping[str, object]], None] | None:
    active = [hook for hook in hooks if hook is not None]
    if not active:
        return None

    def _hook(payload: Mapping[str, object]) -> None:
        for hook in active:
            hook(payload)

    return _hook


def _chain_sql_ingest_hooks(
    *hooks: Callable[[Mapping[str, object]], None] | None,
) -> Callable[[Mapping[str, object]], None] | None:
    active = [hook for hook in hooks if hook is not None]
    if not active:
        return None

    def _hook(payload: Mapping[str, object]) -> None:
        for hook in active:
            hook(payload)

    return _hook


def _chain_cache_hooks(
    *hooks: Callable[[DataFusionCacheEvent], None] | None,
) -> Callable[[DataFusionCacheEvent], None] | None:
    active = [hook for hook in hooks if hook is not None]
    if not active:
        return None

    def _hook(event: DataFusionCacheEvent) -> None:
        for hook in active:
            hook(event)

    return _hook


def labeled_fallback_hook(
    label: ExecutionLabel,
    sink: list[dict[str, object]],
) -> Callable[[DataFusionFallbackEvent], None]:
    """Return a fallback hook that records rule-scoped diagnostics.

    Returns
    -------
    Callable[[DataFusionFallbackEvent], None]
        Hook that appends labeled fallback diagnostics to the sink.
    """

    def _hook(event: DataFusionFallbackEvent) -> None:
        sink.append(
            {
                "rule": label.rule_name,
                "output": label.output_dataset,
                "reason": event.reason,
                "error": event.error,
                "expression_type": event.expression_type,
                "sql": event.sql,
                "dialect": event.dialect,
                "policy_violations": list(event.policy_violations),
            }
        )

    return _hook


def labeled_explain_hook(
    label: ExecutionLabel,
    sink: list[dict[str, object]],
) -> Callable[[str, ExplainRows], None]:
    """Return an explain hook that records rule-scoped diagnostics.

    Returns
    -------
    Callable[[str, ExplainRows], None]
        Hook that appends labeled explain diagnostics to the sink.
    """

    def _hook(sql: str, rows: ExplainRows) -> None:
        sink.append(
            {
                "rule": label.rule_name,
                "output": label.output_dataset,
                "sql": sql,
                "rows": rows,
            }
        )

    return _hook


def diagnostics_fallback_hook(
    sink: DiagnosticsSink,
) -> Callable[[DataFusionFallbackEvent], None]:
    """Return a fallback hook that records diagnostics rows.

    Returns
    -------
    Callable[[DataFusionFallbackEvent], None]
        Hook that records fallback rows in the diagnostics sink.
    """

    def _hook(event: DataFusionFallbackEvent) -> None:
        sink.record_events(
            "datafusion_fallbacks_v1",
            [
                {
                    "event_time_unix_ms": int(time.time() * 1000),
                    "reason": event.reason,
                    "error": event.error,
                    "expression_type": event.expression_type,
                    "sql": event.sql,
                    "dialect": event.dialect,
                    "policy_violations": list(event.policy_violations),
                }
            ],
        )

    return _hook


def diagnostics_cache_hook(
    sink: DiagnosticsSink,
) -> Callable[[DataFusionCacheEvent], None]:
    """Return a cache hook that records diagnostics rows.

    Returns
    -------
    Callable[[DataFusionCacheEvent], None]
        Hook that records cache events in the diagnostics sink.
    """

    def _hook(event: DataFusionCacheEvent) -> None:
        sink.record_events(
            "datafusion_cache_events_v1",
            [
                {
                    "event_time_unix_ms": int(time.time() * 1000),
                    "cache_enabled": event.cache_enabled,
                    "cache_max_columns": event.cache_max_columns,
                    "column_count": event.column_count,
                    "reason": event.reason,
                    "plan_hash": event.plan_hash,
                    "profile_hash": event.profile_hash,
                }
            ],
        )

    return _hook


def diagnostics_explain_hook(
    sink: DiagnosticsSink,
    *,
    explain_analyze: bool,
) -> Callable[[str, ExplainRows], None]:
    """Return an explain hook that records diagnostics rows.

    Returns
    -------
    Callable[[str, ExplainRows], None]
        Hook that records explain rows in the diagnostics sink.
    """

    def _hook(sql: str, rows: ExplainRows) -> None:
        sink.record_events(
            "datafusion_explains_v1",
            [
                {
                    "event_time_unix_ms": int(time.time() * 1000),
                    "sql": sql,
                    "rows": rows,
                    "explain_analyze": explain_analyze,
                }
            ],
        )

    return _hook


def diagnostics_plan_artifacts_hook(
    sink: DiagnosticsSink,
) -> Callable[[Mapping[str, object]], None]:
    """Return a plan artifacts hook that records diagnostics payloads.

    Returns
    -------
    Callable[[Mapping[str, object]], None]
        Hook that records plan artifacts in the diagnostics sink.
    """

    def _hook(payload: Mapping[str, object]) -> None:
        sink.record_artifact("datafusion_plan_artifacts_v1", payload)

    return _hook


def diagnostics_sql_ingest_hook(
    sink: DiagnosticsSink,
) -> Callable[[Mapping[str, object]], None]:
    """Return a SQL ingest hook that records diagnostics payloads.

    Returns
    -------
    Callable[[Mapping[str, object]], None]
        Hook that records SQL ingest artifacts in the diagnostics sink.
    """

    def _hook(payload: Mapping[str, object]) -> None:
        sink.record_artifact("ibis_sql_ingest_v1", payload)

    return _hook


def diagnostics_arrow_ingest_hook(
    sink: DiagnosticsSink,
) -> Callable[[Mapping[str, object]], None]:
    """Return an Arrow ingest hook that records diagnostics payloads.

    Returns
    -------
    Callable[[Mapping[str, object]], None]
        Hook that records Arrow ingestion artifacts in the diagnostics sink.
    """

    def _hook(payload: Mapping[str, object]) -> None:
        sink.record_artifact("datafusion_arrow_ingest_v1", payload)

    return _hook


def _attach_cache_manager(
    builder: RuntimeEnvBuilder,
    *,
    enabled: bool,
    factory: Callable[[], object] | None,
) -> RuntimeEnvBuilder:
    if not enabled:
        return builder
    if factory is None:
        msg = "Cache manager enabled but cache_manager_factory is not set."
        raise ValueError(msg)
    cache_manager = factory()
    if cache_manager is None:
        msg = "cache_manager_factory returned None."
        raise ValueError(msg)
    if not callable(getattr(builder, "with_cache_manager", None)):
        msg = "RuntimeEnvBuilder missing with_cache_manager; upgrade DataFusion to enable."
        raise TypeError(msg)
    return _apply_builder(builder, method="with_cache_manager", args=(cache_manager,))


@dataclass(frozen=True)
class _ResolvedCompileHooks:
    explain_hook: Callable[[str, ExplainRows], None] | None
    plan_artifacts_hook: Callable[[Mapping[str, object]], None] | None
    sql_ingest_hook: Callable[[Mapping[str, object]], None] | None
    fallback_hook: Callable[[DataFusionFallbackEvent], None] | None
    cache_event_hook: Callable[[DataFusionCacheEvent], None] | None


@dataclass(frozen=True)
class _ScipRegistrationSnapshot:
    name: str
    requested_name: str
    location: DatasetLocation
    expected_fingerprint: str
    actual_fingerprint: str
    schema_match: bool


@dataclass(frozen=True)
class DataFusionRuntimeProfile:
    """DataFusion runtime configuration."""

    target_partitions: int | None = None
    batch_size: int | None = None
    spill_dir: str | None = None
    memory_pool: MemoryPool = "greedy"
    memory_limit_bytes: int | None = None
    default_catalog: str = "codeintel"
    default_schema: str = "public"
    registry_catalogs: Mapping[str, DatasetCatalog] = field(default_factory=dict)
    registry_catalog_name: str | None = None
    catalog_auto_load_location: str | None = None
    catalog_auto_load_format: str | None = None
    ast_catalog_location: str | None = None
    ast_catalog_format: str | None = None
    ast_external_location: str | None = None
    ast_external_format: str = "parquet"
    ast_external_provider: Literal["dataset", "listing", "parquet"] | None = None
    ast_external_ordering: tuple[str, ...] = ("repo", "path")
    ast_external_partition_cols: tuple[tuple[str, pa.DataType], ...] = (
        ("repo", pa.string()),
        ("path", pa.string()),
    )
    ast_external_unbounded: bool = False
    ast_external_schema_force_view_types: bool | None = True
    ast_external_skip_arrow_metadata: bool | None = False
    ast_external_listing_table_factory_infer_partitions: bool | None = True
    ast_external_listing_table_ignore_subdirectory: bool | None = False
    ast_external_collect_statistics: bool | None = False
    ast_external_meta_fetch_concurrency: int | None = 64
    ast_external_list_files_cache_ttl: str | None = "2m"
    ast_external_list_files_cache_limit: str | None = str(64 * 1024 * 1024)
    ast_delta_location: str | None = None
    ast_delta_version: int | None = None
    ast_delta_timestamp: str | None = None
    ast_delta_constraints: tuple[str, ...] = ()
    ast_delta_scan: DeltaScanOptions | None = None
    bytecode_catalog_location: str | None = None
    bytecode_catalog_format: str | None = None
    bytecode_external_location: str | None = None
    bytecode_external_format: str = "parquet"
    bytecode_external_provider: Literal["dataset", "listing", "parquet"] | None = None
    bytecode_external_ordering: tuple[str, ...] = ("path", "file_id")
    bytecode_external_partition_cols: tuple[tuple[str, pa.DataType], ...] = ()
    bytecode_external_unbounded: bool = False
    bytecode_external_schema_force_view_types: bool | None = True
    bytecode_external_skip_arrow_metadata: bool | None = False
    bytecode_external_listing_table_factory_infer_partitions: bool | None = True
    bytecode_external_listing_table_ignore_subdirectory: bool | None = False
    bytecode_external_collect_statistics: bool | None = False
    bytecode_external_meta_fetch_concurrency: int | None = 64
    bytecode_external_list_files_cache_ttl: str | None = "5m"
    bytecode_external_list_files_cache_limit: str | None = "10000"
    bytecode_delta_location: str | None = None
    bytecode_delta_version: int | None = None
    bytecode_delta_timestamp: str | None = None
    bytecode_delta_constraints: tuple[str, ...] = ()
    bytecode_delta_scan: DeltaScanOptions | None = None
    scip_dataset_locations: Mapping[str, DatasetLocation] = field(default_factory=dict)
    enable_information_schema: bool = True
    enable_url_table: bool = False  # Dev-only convenience for file-path queries.
    cache_enabled: bool = False
    cache_max_columns: int | None = 64
    enable_cache_manager: bool = False
    cache_manager_factory: Callable[[], object] | None = None
    enable_function_factory: bool = True
    function_factory_hook: Callable[[SessionContext], None] | None = None
    enable_schema_registry: bool = True
    enable_expr_planners: bool = False
    expr_planner_names: tuple[str, ...] = ()
    expr_planner_hook: Callable[[SessionContext], None] | None = None
    physical_expr_adapter_factory: object | None = None
    schema_adapter_factories: Mapping[str, object] = field(default_factory=dict)
    enable_schema_evolution_adapter: bool = True
    enable_udfs: bool = True
    enable_delta_plan_codecs: bool = False
    delta_plan_codec_physical: str = "delta_physical"
    delta_plan_codec_logical: str = "delta_logical"
    enable_metrics: bool = False
    metrics_collector: Callable[[], Mapping[str, object] | None] | None = None
    enable_tracing: bool = False
    tracing_hook: Callable[[], None] | None = None
    tracing_collector: Callable[[], Mapping[str, object] | None] | None = None
    capture_explain: bool = False
    explain_analyze: bool = True
    explain_analyze_level: str | None = None
    explain_collector: DataFusionExplainCollector | None = field(
        default_factory=DataFusionExplainCollector
    )
    capture_plan_artifacts: bool = True
    plan_collector: DataFusionPlanCollector | None = field(default_factory=DataFusionPlanCollector)
    view_registry: DataFusionViewRegistry | None = field(default_factory=DataFusionViewRegistry)
    substrait_validation: bool = False
    capture_fallbacks: bool = True
    fallback_collector: DataFusionFallbackCollector | None = field(
        default_factory=DataFusionFallbackCollector
    )
    diagnostics_sink: DiagnosticsSink | None = None
    labeled_fallbacks: list[dict[str, object]] = field(default_factory=list)
    labeled_explains: list[dict[str, object]] = field(default_factory=list)
    plan_cache: PlanCache | None = field(default_factory=PlanCache)
    local_filesystem_root: str | None = None
    input_plugins: tuple[Callable[[SessionContext], None], ...] = ()
    prepared_statements: tuple[PreparedStatementSpec, ...] = ()
    config_policy_name: str | None = "symtable"
    config_policy: DataFusionConfigPolicy | None = None
    schema_hardening_name: str | None = "schema_hardening"
    schema_hardening: SchemaHardeningProfile | None = None
    sql_policy_name: str | None = "read_only"
    sql_policy: DataFusionSqlPolicy | None = None
    param_identifier_allowlist: tuple[str, ...] = ()
    external_table_options: Mapping[str, object] = field(default_factory=dict)
    write_policy: DataFusionWritePolicy | None = None
    settings_overrides: Mapping[str, str] = field(default_factory=dict)
    feature_gates: DataFusionFeatureGates = field(default_factory=DataFusionFeatureGates)
    join_policy: DataFusionJoinPolicy | None = None
    share_context: bool = True
    session_context_key: str | None = None
    distributed: bool = False
    distributed_context_factory: Callable[[], SessionContext] | None = None
    runtime_env_hook: Callable[[RuntimeEnvBuilder], RuntimeEnvBuilder] | None = None
    session_context_hook: Callable[[SessionContext], SessionContext] | None = None

    def session_config(self) -> SessionConfig:
        """Return a SessionConfig configured from the profile.

        Returns
        -------
        datafusion.SessionConfig
            Session configuration for the profile.
        """
        config = SessionConfig()
        config = config.with_default_catalog_and_schema(
            self.default_catalog,
            self.default_schema,
        )
        config = config.with_create_default_catalog_and_schema(enabled=True)
        config = config.with_information_schema(self.enable_information_schema)
        config = _apply_optional_int_config(
            config,
            method="with_target_partitions",
            key="datafusion.execution.target_partitions",
            value=self.target_partitions,
        )
        config = _apply_optional_int_config(
            config,
            method="with_batch_size",
            key="datafusion.execution.batch_size",
            value=self.batch_size,
        )
        catalog_location, catalog_format = self._effective_catalog_autoload()
        config = _apply_catalog_autoload(
            config,
            location=catalog_location,
            file_format=catalog_format,
        )
        config = _apply_config_policy(config, self._resolved_config_policy())
        config = _apply_schema_hardening(config, self._resolved_schema_hardening())
        config = _apply_settings_overrides(config, self.settings_overrides)
        config = _apply_feature_settings(config, self.feature_gates)
        config = _apply_join_settings(config, self.join_policy)
        return _apply_explain_analyze_level(config, self.explain_analyze_level)

    def _effective_catalog_autoload(self) -> tuple[str | None, str | None]:
        if self.ast_catalog_location is not None or self.ast_catalog_format is not None:
            return self.ast_catalog_location, self.ast_catalog_format
        if self.bytecode_catalog_location is not None or self.bytecode_catalog_format is not None:
            return self.bytecode_catalog_location, self.bytecode_catalog_format
        return self.catalog_auto_load_location, self.catalog_auto_load_format

    def runtime_env_builder(self) -> RuntimeEnvBuilder:
        """Return a RuntimeEnvBuilder configured from the profile.

        Returns
        -------
        datafusion.RuntimeEnvBuilder
            Runtime environment builder for the profile.
        """
        builder = RuntimeEnvBuilder()
        if self.spill_dir is not None:
            builder = _apply_builder(
                builder,
                method="with_disk_manager_specified",
                args=(self.spill_dir,),
            )
            builder = _apply_builder(
                builder,
                method="with_temp_file_path",
                args=(self.spill_dir,),
            )
        if self.memory_limit_bytes is not None:
            limit = int(self.memory_limit_bytes)
            if self.memory_pool == "fair":
                builder = _apply_builder(
                    builder,
                    method="with_fair_spill_pool",
                    args=(limit,),
                )
            elif self.memory_pool == "greedy":
                builder = _apply_builder(
                    builder,
                    method="with_greedy_memory_pool",
                    args=(limit,),
                )
        builder = _attach_cache_manager(
            builder,
            enabled=self.enable_cache_manager,
            factory=self.cache_manager_factory,
        )
        if self.runtime_env_hook is not None:
            builder = self.runtime_env_hook(builder)
        return builder

    def session_context(self) -> SessionContext:
        """Return a SessionContext configured from the profile.

        Returns
        -------
        datafusion.SessionContext
            Session context configured for the profile. When
            ``local_filesystem_root`` is set, the ``file://`` object store
            scheme is registered against that root.
        """
        cached = self._cached_context()
        if cached is not None:
            return cached
        ctx = self._build_session_context()
        ctx = self._apply_url_table(ctx)
        self._register_local_filesystem(ctx)
        self._install_input_plugins(ctx)
        self._install_registry_catalogs(ctx)
        self._install_schema_registry(ctx)
        self._install_udfs(ctx)
        self._prepare_statements(ctx)
        self.ensure_delta_plan_codecs(ctx)
        self._install_function_factory(ctx)
        self._install_expr_planners(ctx)
        self._install_physical_expr_adapter_factory(ctx)
        self._install_tracing()
        if self.session_context_hook is not None:
            ctx = self.session_context_hook(ctx)
        self._cache_context(ctx)
        return ctx

    def named_args_supported(self) -> bool:
        """Return whether named arguments are enabled for SQL execution.

        Returns
        -------
        bool
            ``True`` when named arguments should be supported.
        """
        if not self.enable_expr_planners:
            return False
        if self.expr_planner_hook is not None:
            return True
        return bool(self.expr_planner_names)

    def _install_input_plugins(self, ctx: SessionContext) -> None:
        """Install input plugins on the session context."""
        for plugin in self.input_plugins:
            plugin(ctx)

    def _install_registry_catalogs(self, ctx: SessionContext) -> None:
        """Install registry-backed catalog providers on the session context."""
        if not self.registry_catalogs:
            return
        catalog_name = self.registry_catalog_name or self.default_catalog
        register_registry_catalogs(
            ctx,
            catalogs=self.registry_catalogs,
            catalog_name=catalog_name,
            default_schema=self.default_schema,
        )
        set_default = getattr(ctx, "set_default_catalog_and_schema", None)
        if callable(set_default) and catalog_name != self.default_catalog:
            set_default(catalog_name, self.default_schema)

    def _install_udfs(self, ctx: SessionContext) -> None:
        """Install registered UDFs on the session context."""
        if not self.enable_udfs:
            return
        snapshot = register_datafusion_udfs(ctx)
        self._record_udf_snapshot(snapshot)

    def _record_schema_registry_validation(
        self,
        ctx: SessionContext,
        *,
        view_errors: Mapping[str, str] | None = None,
        tree_sitter_checks: Mapping[str, object] | None = None,
    ) -> None:
        if self.diagnostics_sink is None:
            return
        if not self.enable_information_schema:
            return
        expected_names = set(schema_names())
        missing = missing_schema_names(ctx, expected=tuple(sorted(expected_names)))
        type_errors: dict[str, str] = {}
        for name in nested_schema_names():
            try:
                validate_nested_types(ctx, name)
            except (RuntimeError, TypeError, ValueError) as exc:
                type_errors[name] = str(exc)
        if not missing and not type_errors and not view_errors:
            return
        payload: dict[str, object] = {
            "event_time_unix_ms": int(time.time() * 1000),
            "missing": list(missing),
            "type_errors": type_errors,
            "view_errors": dict(view_errors) if view_errors else None,
        }
        if tree_sitter_checks is not None:
            payload["tree_sitter_checks"] = dict(tree_sitter_checks)
        if view_errors and self.view_registry is not None:
            parser_dialect = _sql_parser_dialect(self)
            parse_errors = _collect_view_sql_parse_errors(
                self.view_registry,
                dialect=parser_dialect,
            )
            if parse_errors:
                payload["sql_parse_errors"] = parse_errors
                payload["sql_parser_dialect"] = parser_dialect
        self.diagnostics_sink.record_artifact("datafusion_schema_registry_validation_v1", payload)

    def _record_catalog_autoload_snapshot(self, ctx: SessionContext) -> None:
        if self.diagnostics_sink is None:
            return
        if not self.enable_information_schema:
            return
        catalog_location, catalog_format = self._effective_catalog_autoload()
        if catalog_location is None and catalog_format is None:
            return
        introspector = self._schema_introspector(ctx)
        payload: dict[str, object] = {
            "event_time_unix_ms": int(time.time() * 1000),
            "catalog_auto_load_location": catalog_location,
            "catalog_auto_load_format": catalog_format,
            "ast_catalog_location": self.ast_catalog_location,
            "ast_catalog_format": self.ast_catalog_format,
            "bytecode_catalog_location": self.bytecode_catalog_location,
            "bytecode_catalog_format": self.bytecode_catalog_format,
        }
        try:
            tables = [
                row
                for row in introspector.tables_snapshot()
                if row.get("table_schema") != "information_schema"
            ]
            payload["tables"] = tables
        except (RuntimeError, TypeError, ValueError) as exc:
            payload["error"] = str(exc)
        self.diagnostics_sink.record_artifact("datafusion_catalog_autoload_v1", payload)

    @staticmethod
    def _ast_feature_gates(
        ctx: SessionContext,
    ) -> tuple[tuple[str, ...], tuple[str, ...], dict[str, object]]:
        version = _datafusion_version(ctx)
        version_source = "sql"
        if version is None:
            version = datafusion.__version__
            version_source = "package"
        major = _parse_major_version(version) if version else None
        functions = _datafusion_function_names(ctx)
        function_support = {name: name in functions for name in ("map_entries", "arrow_metadata")}
        enabled_optional: list[str] = []
        blocked_by_version: list[str] = []
        missing_functions: dict[str, list[str]] = {}
        for view in AST_OPTIONAL_VIEW_NAMES:
            required = _AST_OPTIONAL_VIEW_FUNCTIONS.get(view, ())
            if major is None:
                blocked_by_version.append(view)
                continue
            missing = [name for name in required if name not in functions]
            if missing:
                missing_functions[view] = missing
                continue
            enabled_optional.append(view)
        view_names = AST_CORE_VIEW_NAMES + tuple(enabled_optional)
        disabled_views = tuple(
            view for view in AST_OPTIONAL_VIEW_NAMES if view not in enabled_optional
        )
        payload: dict[str, object] = {
            "event_time_unix_ms": int(time.time() * 1000),
            "datafusion_version": version,
            "datafusion_version_source": version_source,
            "datafusion_version_major": major,
            "required_functions": function_support,
            "enabled_views": list(view_names),
            "disabled_views": list(disabled_views) if disabled_views else None,
            "blocked_by_version": blocked_by_version or None,
            "missing_functions": missing_functions or None,
        }
        return view_names, disabled_views, payload

    def _record_ast_feature_gates(self, payload: Mapping[str, object]) -> None:
        if self.diagnostics_sink is None:
            return
        self.diagnostics_sink.record_artifact("datafusion_ast_feature_gates_v1", payload)

    def _record_ast_span_metadata(self, ctx: SessionContext) -> None:
        if self.diagnostics_sink is None:
            return
        payload: dict[str, object] = {
            "event_time_unix_ms": int(time.time() * 1000),
            "dataset": "ast_files_v1",
        }
        try:
            table = ctx.sql("SELECT * FROM ast_span_metadata").to_arrow_table()
            rows = table.to_pylist()
            schema = schema_for("ast_files_v1")
            payload["schema_fingerprint"] = schema_fingerprint(schema)
            payload["metadata"] = rows[0] if rows else None
        except (KeyError, RuntimeError, TypeError, ValueError) as exc:
            payload["error"] = str(exc)
        version = _datafusion_version(ctx)
        if version is not None:
            payload["datafusion_version"] = version
        self.diagnostics_sink.record_artifact("datafusion_ast_span_metadata_v1", payload)

    def _ast_dataset_location(self) -> DatasetLocation | None:
        if self.ast_external_location and self.ast_delta_location:
            msg = "AST dataset config cannot set both external and delta locations."
            raise ValueError(msg)
        if self.ast_delta_location:
            expected_schema = schema_for("ast_files_v1")
            delta_scan = self.ast_delta_scan
            if delta_scan is None:
                delta_scan = DeltaScanOptions(schema=expected_schema)
            elif delta_scan.schema is None:
                delta_scan = replace(delta_scan, schema=expected_schema)
            return DatasetLocation(
                path=self.ast_delta_location,
                format="delta",
                delta_version=self.ast_delta_version,
                delta_timestamp=self.ast_delta_timestamp,
                delta_constraints=self.ast_delta_constraints,
                delta_scan=delta_scan,
            )
        if self.ast_external_location:
            if self.ast_external_format == "delta":
                msg = "AST external format must not be 'delta'."
                raise ValueError(msg)
            expected_schema = schema_for("ast_files_v1")
            scan = DataFusionScanOptions(
                partition_cols=self.ast_external_partition_cols,
                file_sort_order=self.ast_external_ordering,
                unbounded=self.ast_external_unbounded,
                schema_force_view_types=self.ast_external_schema_force_view_types,
                skip_arrow_metadata=self.ast_external_skip_arrow_metadata,
                table_schema_contract=TableSchemaContract(
                    file_schema=expected_schema,
                    partition_cols=self.ast_external_partition_cols,
                ),
                listing_table_factory_infer_partitions=(
                    self.ast_external_listing_table_factory_infer_partitions
                ),
                listing_table_ignore_subdirectory=(
                    self.ast_external_listing_table_ignore_subdirectory
                ),
                collect_statistics=self.ast_external_collect_statistics,
                meta_fetch_concurrency=self.ast_external_meta_fetch_concurrency,
                list_files_cache_ttl=self.ast_external_list_files_cache_ttl,
                list_files_cache_limit=self.ast_external_list_files_cache_limit,
            )
            return DatasetLocation(
                path=self.ast_external_location,
                format=self.ast_external_format,
                datafusion_provider=self.ast_external_provider,
                datafusion_scan=scan,
            )
        return None

    def ast_dataset_location(self) -> DatasetLocation | None:
        """Return the configured AST dataset location, when available.

        Returns
        -------
        DatasetLocation | None
            DatasetLocation for AST outputs or ``None`` when not configured.
        """
        return self._ast_dataset_location()

    def _register_ast_dataset(self, ctx: SessionContext) -> None:
        location = self._ast_dataset_location()
        if location is None:
            return
        deregister = getattr(ctx, "deregister_table", None)
        if callable(deregister):
            with contextlib.suppress(KeyError, RuntimeError, TypeError, ValueError):
                deregister("ast_files_v1")
        df = register_dataset_df(
            ctx,
            name="ast_files_v1",
            location=location,
            runtime_profile=self,
        )
        expected = schema_for("ast_files_v1").remove_metadata()
        actual = df.schema().remove_metadata()
        expected_fingerprint = schema_fingerprint(expected)
        actual_fingerprint = schema_fingerprint(actual)
        if actual_fingerprint != expected_fingerprint:
            msg = (
                "AST dataset schema mismatch: expected "
                f"{expected_fingerprint}, observed {actual_fingerprint}."
            )
            raise ValueError(msg)
        self._record_ast_registration(location=location)

    def _bytecode_dataset_location(self) -> DatasetLocation | None:
        if self.bytecode_external_location and self.bytecode_delta_location:
            msg = "Bytecode dataset config cannot set both external and delta locations."
            raise ValueError(msg)
        if self.bytecode_delta_location:
            expected_schema = schema_for("bytecode_files_v1")
            delta_scan = self.bytecode_delta_scan
            if delta_scan is None:
                delta_scan = DeltaScanOptions(schema=expected_schema)
            elif delta_scan.schema is None:
                delta_scan = replace(delta_scan, schema=expected_schema)
            return DatasetLocation(
                path=self.bytecode_delta_location,
                format="delta",
                delta_version=self.bytecode_delta_version,
                delta_timestamp=self.bytecode_delta_timestamp,
                delta_constraints=self.bytecode_delta_constraints,
                delta_scan=delta_scan,
            )
        if self.bytecode_external_location:
            if self.bytecode_external_format == "delta":
                msg = "Bytecode external format must not be 'delta'."
                raise ValueError(msg)
            expected_schema = schema_for("bytecode_files_v1")
            scan = DataFusionScanOptions(
                partition_cols=self.bytecode_external_partition_cols,
                file_sort_order=self.bytecode_external_ordering,
                unbounded=self.bytecode_external_unbounded,
                schema_force_view_types=self.bytecode_external_schema_force_view_types,
                skip_arrow_metadata=self.bytecode_external_skip_arrow_metadata,
                table_schema_contract=TableSchemaContract(
                    file_schema=expected_schema,
                    partition_cols=self.bytecode_external_partition_cols,
                ),
                listing_table_factory_infer_partitions=(
                    self.bytecode_external_listing_table_factory_infer_partitions
                ),
                listing_table_ignore_subdirectory=(
                    self.bytecode_external_listing_table_ignore_subdirectory
                ),
                collect_statistics=self.bytecode_external_collect_statistics,
                meta_fetch_concurrency=self.bytecode_external_meta_fetch_concurrency,
                list_files_cache_ttl=self.bytecode_external_list_files_cache_ttl,
                list_files_cache_limit=self.bytecode_external_list_files_cache_limit,
            )
            return DatasetLocation(
                path=self.bytecode_external_location,
                format=self.bytecode_external_format,
                datafusion_provider=self.bytecode_external_provider,
                datafusion_scan=scan,
            )
        return None

    def _register_bytecode_dataset(self, ctx: SessionContext) -> None:
        location = self._bytecode_dataset_location()
        if location is None:
            return
        deregister = getattr(ctx, "deregister_table", None)
        if callable(deregister):
            with contextlib.suppress(KeyError, RuntimeError, TypeError, ValueError):
                deregister("bytecode_files_v1")
        df = register_dataset_df(
            ctx,
            name="bytecode_files_v1",
            location=location,
            runtime_profile=self,
        )
        expected = schema_for("bytecode_files_v1").remove_metadata()
        actual = df.schema().remove_metadata()
        expected_fingerprint = schema_fingerprint(expected)
        actual_fingerprint = schema_fingerprint(actual)
        if actual_fingerprint != expected_fingerprint:
            msg = (
                "Bytecode dataset schema mismatch: expected "
                f"{expected_fingerprint}, observed {actual_fingerprint}."
            )
            raise ValueError(msg)
        self._record_bytecode_registration(location=location)

    def _register_scip_datasets(self, ctx: SessionContext) -> None:
        if not self.scip_dataset_locations:
            return
        for name, location in sorted(self.scip_dataset_locations.items()):
            schema_name = SCIP_VIEW_SCHEMA_MAP.get(name, name)
            try:
                expected_schema = schema_for(schema_name)
            except KeyError as exc:
                msg = f"Unknown SCIP dataset name: {name!r}."
                raise ValueError(msg) from exc
            resolved = location
            scan = resolved.datafusion_scan
            if scan is None:
                scan = DataFusionScanOptions(
                    table_schema_contract=TableSchemaContract(file_schema=expected_schema),
                )
            elif scan.table_schema_contract is None:
                scan = replace(
                    scan,
                    table_schema_contract=TableSchemaContract(
                        file_schema=expected_schema,
                        partition_cols=scan.partition_cols,
                    ),
                )
            resolved = replace(resolved, datafusion_scan=scan)
            deregister = getattr(ctx, "deregister_table", None)
            if callable(deregister):
                with contextlib.suppress(KeyError, RuntimeError, TypeError, ValueError):
                    deregister(schema_name)
            df = register_dataset_df(
                ctx,
                name=schema_name,
                location=resolved,
                runtime_profile=self,
            )
            expected = expected_schema.remove_metadata()
            actual = df.schema().remove_metadata()
            expected_fingerprint = schema_fingerprint(expected)
            actual_fingerprint = schema_fingerprint(actual)
            match = expected_fingerprint == actual_fingerprint
            if not match and not self.enable_schema_evolution_adapter:
                msg = (
                    "SCIP dataset schema mismatch for "
                    f"{schema_name!r}: expected {expected_fingerprint}, "
                    f"observed {actual_fingerprint}."
                )
                raise ValueError(msg)
            if not match:
                logger.warning(
                    "SCIP dataset schema mismatch: %s expected %s observed %s",
                    schema_name,
                    expected_fingerprint,
                    actual_fingerprint,
                )
            snapshot = _ScipRegistrationSnapshot(
                name=schema_name,
                requested_name=name,
                location=resolved,
                expected_fingerprint=expected_fingerprint,
                actual_fingerprint=actual_fingerprint,
                schema_match=match,
            )
            self._record_scip_registration(snapshot=snapshot)

    def _record_ast_registration(self, *, location: DatasetLocation) -> None:
        if self.diagnostics_sink is None:
            return
        scan = location.datafusion_scan
        payload = {
            "event_time_unix_ms": int(time.time() * 1000),
            "name": "ast_files_v1",
            "location": str(location.path),
            "format": location.format,
            "datafusion_provider": location.datafusion_provider,
            "file_sort_order": list(scan.file_sort_order) if scan is not None else None,
            "partition_cols": [
                {"name": name, "dtype": str(dtype)}
                for name, dtype in (scan.partition_cols if scan is not None else ())
            ],
            "schema_force_view_types": scan.schema_force_view_types if scan is not None else None,
            "skip_arrow_metadata": scan.skip_arrow_metadata if scan is not None else None,
            "listing_table_factory_infer_partitions": (
                scan.listing_table_factory_infer_partitions if scan is not None else None
            ),
            "listing_table_ignore_subdirectory": (
                scan.listing_table_ignore_subdirectory if scan is not None else None
            ),
            "collect_statistics": scan.collect_statistics if scan is not None else None,
            "meta_fetch_concurrency": scan.meta_fetch_concurrency if scan is not None else None,
            "list_files_cache_limit": scan.list_files_cache_limit if scan is not None else None,
            "list_files_cache_ttl": scan.list_files_cache_ttl if scan is not None else None,
            "unbounded": scan.unbounded if scan is not None else None,
            "delta_version": location.delta_version,
            "delta_timestamp": location.delta_timestamp,
            "delta_constraints": list(location.delta_constraints),
        }
        self.diagnostics_sink.record_artifact("datafusion_ast_dataset_v1", payload)

    def _record_bytecode_registration(self, *, location: DatasetLocation) -> None:
        if self.diagnostics_sink is None:
            return
        scan = location.datafusion_scan
        payload = {
            "event_time_unix_ms": int(time.time() * 1000),
            "name": "bytecode_files_v1",
            "location": str(location.path),
            "format": location.format,
            "datafusion_provider": location.datafusion_provider,
            "file_sort_order": list(scan.file_sort_order) if scan is not None else None,
            "partition_cols": [
                {"name": name, "dtype": str(dtype)}
                for name, dtype in (scan.partition_cols if scan is not None else ())
            ],
            "schema_force_view_types": scan.schema_force_view_types if scan is not None else None,
            "skip_arrow_metadata": scan.skip_arrow_metadata if scan is not None else None,
            "listing_table_factory_infer_partitions": (
                scan.listing_table_factory_infer_partitions if scan is not None else None
            ),
            "listing_table_ignore_subdirectory": (
                scan.listing_table_ignore_subdirectory if scan is not None else None
            ),
            "collect_statistics": scan.collect_statistics if scan is not None else None,
            "meta_fetch_concurrency": scan.meta_fetch_concurrency if scan is not None else None,
            "list_files_cache_limit": scan.list_files_cache_limit if scan is not None else None,
            "list_files_cache_ttl": scan.list_files_cache_ttl if scan is not None else None,
            "unbounded": scan.unbounded if scan is not None else None,
            "delta_version": location.delta_version,
            "delta_timestamp": location.delta_timestamp,
            "delta_constraints": list(location.delta_constraints),
        }
        self.diagnostics_sink.record_artifact("datafusion_bytecode_dataset_v1", payload)

    def _record_scip_registration(
        self,
        *,
        snapshot: _ScipRegistrationSnapshot,
    ) -> None:
        if self.diagnostics_sink is None:
            return
        location = snapshot.location
        scan = location.datafusion_scan
        payload = {
            "event_time_unix_ms": int(time.time() * 1000),
            "name": snapshot.name,
            "requested_name": snapshot.requested_name,
            "location": str(location.path),
            "format": location.format,
            "datafusion_provider": location.datafusion_provider,
            "file_sort_order": list(scan.file_sort_order) if scan is not None else None,
            "partition_cols": [
                {"name": col_name, "dtype": str(dtype)}
                for col_name, dtype in (scan.partition_cols if scan is not None else ())
            ],
            "schema_force_view_types": scan.schema_force_view_types if scan is not None else None,
            "skip_arrow_metadata": scan.skip_arrow_metadata if scan is not None else None,
            "listing_table_factory_infer_partitions": (
                scan.listing_table_factory_infer_partitions if scan is not None else None
            ),
            "listing_table_ignore_subdirectory": (
                scan.listing_table_ignore_subdirectory if scan is not None else None
            ),
            "collect_statistics": scan.collect_statistics if scan is not None else None,
            "meta_fetch_concurrency": scan.meta_fetch_concurrency if scan is not None else None,
            "list_files_cache_limit": scan.list_files_cache_limit if scan is not None else None,
            "list_files_cache_ttl": scan.list_files_cache_ttl if scan is not None else None,
            "unbounded": scan.unbounded if scan is not None else None,
            "delta_version": location.delta_version,
            "delta_timestamp": location.delta_timestamp,
            "delta_constraints": list(location.delta_constraints),
            "expected_schema_fingerprint": snapshot.expected_fingerprint,
            "observed_schema_fingerprint": snapshot.actual_fingerprint,
            "schema_match": snapshot.schema_match,
        }
        self.diagnostics_sink.record_artifact("datafusion_scip_datasets_v1", payload)

    def _validate_ast_catalog_autoload(self, ctx: SessionContext) -> None:
        if self.ast_catalog_location is None and self.ast_catalog_format is None:
            return
        try:
            ctx.table("ast_files_v1")
        except (KeyError, RuntimeError, TypeError, ValueError) as exc:
            msg = f"AST catalog autoload failed: {exc}."
            raise ValueError(msg) from exc
        if not self.enable_information_schema:
            return
        try:
            ctx.sql(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name = 'ast_files_v1' LIMIT 1"
            ).collect()
        except (RuntimeError, TypeError, ValueError) as exc:
            msg = f"AST catalog column introspection failed: {exc}."
            raise ValueError(msg) from exc

    def _validate_bytecode_catalog_autoload(self, ctx: SessionContext) -> None:
        if self.bytecode_catalog_location is None and self.bytecode_catalog_format is None:
            return
        try:
            ctx.table("bytecode_files_v1")
        except (KeyError, RuntimeError, TypeError, ValueError) as exc:
            msg = f"Bytecode catalog autoload failed: {exc}."
            raise ValueError(msg) from exc
        if not self.enable_information_schema:
            return
        try:
            ctx.sql(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name = 'bytecode_files_v1' LIMIT 1"
            ).collect()
        except (RuntimeError, TypeError, ValueError) as exc:
            msg = f"Bytecode catalog column introspection failed: {exc}."
            raise ValueError(msg) from exc

    def _record_cst_schema_diagnostics(self, ctx: SessionContext) -> None:
        if self.diagnostics_sink is None:
            return
        payload: dict[str, object] = {
            "event_time_unix_ms": int(time.time() * 1000),
            "dataset": "libcst_files_v1",
        }
        try:
            table = ctx.sql("SELECT * FROM cst_schema_diagnostics").to_arrow_table()
            rows = table.to_pylist()
            schema = schema_for("libcst_files_v1")
            payload["schema_fingerprint"] = schema_fingerprint(schema)
            default_entries = _default_value_entries(schema)
            payload["default_values"] = default_entries or None
            payload["diagnostics"] = rows[0] if rows else None
            introspector = SchemaIntrospector(ctx)
            payload["table_definition"] = introspector.table_definition("libcst_files_v1")
            payload["table_constraints"] = (
                list(introspector.table_constraints("libcst_files_v1")) or None
            )
        except (KeyError, RuntimeError, TypeError, ValueError) as exc:
            payload["error"] = str(exc)
        self.diagnostics_sink.record_artifact("datafusion_cst_schema_diagnostics_v1", payload)

    def _record_tree_sitter_stats(self, ctx: SessionContext) -> None:
        if self.diagnostics_sink is None:
            return
        payload: dict[str, object] = {
            "event_time_unix_ms": int(time.time() * 1000),
            "dataset": "tree_sitter_files_v1",
        }
        try:
            table = ctx.sql("SELECT * FROM ts_stats").to_arrow_table()
            rows = table.to_pylist()
            schema = schema_for("tree_sitter_files_v1")
            payload["schema_fingerprint"] = schema_fingerprint(schema)
            payload["stats"] = rows[0] if rows else None
            introspector = SchemaIntrospector(ctx)
            payload["table_definition"] = introspector.table_definition("tree_sitter_files_v1")
            payload["table_constraints"] = (
                list(introspector.table_constraints("tree_sitter_files_v1")) or None
            )
        except (KeyError, RuntimeError, TypeError, ValueError) as exc:
            payload["error"] = str(exc)
        self.diagnostics_sink.record_artifact("datafusion_tree_sitter_stats_v1", payload)

    def _record_tree_sitter_view_schemas(self, ctx: SessionContext) -> None:
        if self.diagnostics_sink is None:
            return
        views: list[dict[str, object]] = []
        payload: dict[str, object] = {
            "event_time_unix_ms": int(time.time() * 1000),
            "dataset": "tree_sitter_files_v1",
            "views": views,
        }
        errors: dict[str, str] = {}
        introspector = SchemaIntrospector(ctx)
        for name in TREE_SITTER_VIEW_NAMES:
            try:
                plan = _table_logical_plan(ctx, name=name)
                dfschema_tree = _table_dfschema_tree(ctx, name=name)
            except (KeyError, RuntimeError, TypeError, ValueError) as exc:
                errors[name] = str(exc)
                continue
            views.append(
                {
                    "name": name,
                    "logical_plan": plan,
                    "dfschema_tree": dfschema_tree,
                }
            )
        try:
            payload["df_settings"] = introspector.settings_snapshot()
        except (KeyError, RuntimeError, TypeError, ValueError) as exc:
            errors["df_settings"] = str(exc)
        if errors:
            payload["errors"] = errors
        version = _datafusion_version(ctx)
        if version is not None:
            payload["datafusion_version"] = version
        self.diagnostics_sink.record_artifact(
            "datafusion_tree_sitter_plan_schema_v1",
            payload,
        )

    def _record_tree_sitter_cross_checks(self, ctx: SessionContext) -> dict[str, object] | None:
        if self.diagnostics_sink is None:
            return None
        views: list[dict[str, object]] = []
        payload: dict[str, object] = {
            "event_time_unix_ms": int(time.time() * 1000),
            "dataset": "tree_sitter_files_v1",
            "views": views,
        }
        errors: dict[str, str] = {}
        for name in TREE_SITTER_CHECK_VIEWS:
            try:
                summary_rows = (
                    ctx.sql(
                        "SELECT COUNT(*) AS row_count, "
                        "SUM(CASE WHEN mismatch THEN 1 ELSE 0 END) AS mismatch_count "
                        f"FROM {name}"
                    )
                    .to_arrow_table()
                    .to_pylist()
                )
                summary: dict[str, object] = summary_rows[0] if summary_rows else {}
                raw_row_count = summary.get("row_count")
                row_count = (
                    int(raw_row_count) if isinstance(raw_row_count, (float, int, str)) else 0
                )
                raw_mismatch_count = summary.get("mismatch_count")
                mismatch_count = (
                    int(raw_mismatch_count)
                    if isinstance(raw_mismatch_count, (float, int, str))
                    else 0
                )
                entry: dict[str, object] = {
                    "name": name,
                    "row_count": row_count,
                    "mismatch_count": mismatch_count,
                }
                if mismatch_count:
                    sample_rows = (
                        ctx.sql(f"SELECT * FROM {name} WHERE mismatch LIMIT 25")
                        .to_arrow_table()
                        .to_pylist()
                    )
                    entry["sample"] = sample_rows or None
                views.append(entry)
            except (KeyError, RuntimeError, TypeError, ValueError) as exc:
                errors[name] = str(exc)
        if errors:
            payload["errors"] = errors
        self.diagnostics_sink.record_artifact("datafusion_tree_sitter_cross_checks_v1", payload)
        return payload

    def _record_cst_view_plans(self, ctx: SessionContext) -> None:
        if self.diagnostics_sink is None:
            return
        views: list[dict[str, object]] = []
        payload: dict[str, object] = {
            "event_time_unix_ms": int(time.time() * 1000),
            "dataset": "libcst_files_v1",
            "views": views,
        }
        errors: dict[str, str] = {}
        for name in CST_VIEW_NAMES:
            try:
                plan = _table_logical_plan(ctx, name=name)
            except (KeyError, RuntimeError, TypeError, ValueError) as exc:
                errors[name] = str(exc)
                continue
            views.append({"name": name, "logical_plan": plan})
        if errors:
            payload["errors"] = errors
        version = _datafusion_version(ctx)
        if version is not None:
            payload["datafusion_version"] = version
        self.diagnostics_sink.record_artifact("datafusion_cst_view_plans_v1", payload)

    def _record_cst_dfschema_snapshots(self, ctx: SessionContext) -> None:
        if self.diagnostics_sink is None:
            return
        views: list[dict[str, object]] = []
        payload: dict[str, object] = {
            "event_time_unix_ms": int(time.time() * 1000),
            "dataset": "libcst_files_v1",
            "views": views,
        }
        errors: dict[str, str] = {}
        for name in CST_VIEW_NAMES:
            try:
                tree = _table_dfschema_tree(ctx, name=name)
            except (KeyError, RuntimeError, TypeError, ValueError) as exc:
                errors[name] = str(exc)
                continue
            views.append({"name": name, "dfschema_tree": tree})
        if errors:
            payload["errors"] = errors
        version = _datafusion_version(ctx)
        if version is not None:
            payload["datafusion_version"] = version
        self.diagnostics_sink.record_artifact("datafusion_cst_dfschema_v1", payload)

    def _record_bytecode_metadata(self, ctx: SessionContext) -> None:
        if self.diagnostics_sink is None:
            return
        if not self.enable_information_schema:
            return
        payload: dict[str, object] = {
            "event_time_unix_ms": int(time.time() * 1000),
            "dataset": "bytecode_files_v1",
        }
        try:
            table = ctx.sql("SELECT * FROM py_bc_metadata").to_arrow_table()
            rows = table.to_pylist()
            schema = schema_for("bytecode_files_v1")
            payload["schema_fingerprint"] = schema_fingerprint(schema)
            payload["metadata"] = rows[0] if rows else None
            introspector = SchemaIntrospector(ctx)
            payload["table_definition"] = introspector.table_definition("bytecode_files_v1")
            payload["table_constraints"] = (
                list(introspector.table_constraints("bytecode_files_v1")) or None
            )
        except (KeyError, RuntimeError, TypeError, ValueError) as exc:
            payload["error"] = str(exc)
        self.diagnostics_sink.record_artifact("datafusion_bytecode_metadata_v1", payload)

    def _record_schema_snapshots(self, ctx: SessionContext) -> None:
        if self.diagnostics_sink is None:
            return
        if not self.enable_information_schema:
            return
        introspector = self._schema_introspector(ctx)
        payload: dict[str, object] = {
            "event_time_unix_ms": int(time.time() * 1000),
        }
        try:
            payload.update(
                {
                    "catalogs": introspector.catalogs_snapshot(),
                    "schemata": introspector.schemata_snapshot(),
                    "tables": introspector.tables_snapshot(),
                    "columns": introspector.columns_snapshot(),
                    "routines": introspector.routines_snapshot(),
                    "parameters": introspector.parameters_snapshot(),
                    "settings": introspector.settings_snapshot(),
                    "functions": self.function_catalog_snapshot(
                        ctx, include_routines=self.enable_information_schema
                    ),
                }
            )
            version = _datafusion_version(ctx)
            if version is not None:
                payload["datafusion_version"] = version
        except (RuntimeError, TypeError, ValueError) as exc:
            payload["error"] = str(exc)
        self.diagnostics_sink.record_artifact(
            "datafusion_schema_introspection_v1",
            payload,
        )

    def _register_schema_views(
        self,
        ctx: SessionContext,
        *,
        fragment_views: Sequence[ViewSpec],
    ) -> set[str]:
        fragment_names = {view.name for view in fragment_views}
        if fragment_views:
            register_view_specs(
                ctx,
                views=fragment_views,
                runtime_profile=self,
                validate=True,
            )
        nested_views = tuple(
            view for view in nested_view_specs() if view.name not in fragment_names
        )
        if nested_views:
            register_view_specs(
                ctx,
                views=nested_views,
                runtime_profile=self,
                validate=True,
            )
        return fragment_names

    def _validate_catalog_autoloads(
        self,
        ctx: SessionContext,
        *,
        ast_registration: bool,
        bytecode_registration: bool,
    ) -> None:
        if not ast_registration and (
            self.ast_catalog_location is not None or self.ast_catalog_format is not None
        ):
            deregister = getattr(ctx, "deregister_table", None)
            if callable(deregister):
                with contextlib.suppress(KeyError, RuntimeError, TypeError, ValueError):
                    deregister("ast_files_v1")
            self._validate_ast_catalog_autoload(ctx)
        if not bytecode_registration and (
            self.bytecode_catalog_location is not None or self.bytecode_catalog_format is not None
        ):
            deregister = getattr(ctx, "deregister_table", None)
            if callable(deregister):
                with contextlib.suppress(KeyError, RuntimeError, TypeError, ValueError):
                    deregister("bytecode_files_v1")
            self._validate_bytecode_catalog_autoload(ctx)

    def _record_schema_diagnostics(
        self,
        ctx: SessionContext,
        *,
        ast_view_names: Sequence[str],
    ) -> Mapping[str, object] | None:
        self._record_cst_schema_diagnostics(ctx)
        self._record_cst_view_plans(ctx)
        self._record_cst_dfschema_snapshots(ctx)
        self._record_tree_sitter_stats(ctx)
        self._record_tree_sitter_view_schemas(ctx)
        tree_sitter_checks = self._record_tree_sitter_cross_checks(ctx)
        if "ast_span_metadata" in ast_view_names:
            self._record_ast_span_metadata(ctx)
        self._record_bytecode_metadata(ctx)
        return tree_sitter_checks

    @staticmethod
    def _validate_schema_views(
        ctx: SessionContext,
        *,
        ast_view_names: Sequence[str],
    ) -> dict[str, str]:
        view_errors: dict[str, str] = {}
        try:
            validate_ast_views(ctx, view_names=ast_view_names)
        except (RuntimeError, TypeError, ValueError) as exc:
            view_errors["ast_views"] = str(exc)
        for label, validator in (
            ("cst_views", validate_cst_views),
            ("ts_views", validate_ts_views),
            ("scip_views", validate_scip_views),
            ("symtable_views", validate_symtable_views),
            ("bytecode_views", validate_bytecode_views),
        ):
            try:
                validator(ctx)
            except (RuntimeError, TypeError, ValueError) as exc:
                view_errors[label] = str(exc)
        return view_errors

    def _install_schema_registry(self, ctx: SessionContext) -> None:
        """Register canonical nested schemas on the session context.

        Raises
        ------
        ValueError
            Raised when schema registration or validation fails.
        """
        if not self.enable_schema_registry:
            return
        self._record_catalog_autoload_snapshot(ctx)
        register_all_schemas(ctx)
        ast_view_names, ast_optional_disabled, ast_gate_payload = self._ast_feature_gates(ctx)
        self._record_ast_feature_gates(ast_gate_payload)
        ast_registration = (
            self.ast_external_location is not None or self.ast_delta_location is not None
        )
        if ast_registration:
            self._register_ast_dataset(ctx)
        bytecode_registration = (
            self.bytecode_external_location is not None or self.bytecode_delta_location is not None
        )
        if bytecode_registration:
            self._register_bytecode_dataset(ctx)
        self._register_scip_datasets(ctx)
        fragment_views = fragment_view_specs(ctx, exclude=ast_optional_disabled)
        self._register_schema_views(ctx, fragment_views=fragment_views)
        self._validate_catalog_autoloads(
            ctx,
            ast_registration=ast_registration,
            bytecode_registration=bytecode_registration,
        )
        tree_sitter_checks = self._record_schema_diagnostics(
            ctx,
            ast_view_names=ast_view_names,
        )
        view_errors = self._validate_schema_views(ctx, ast_view_names=ast_view_names)
        self._record_schema_registry_validation(
            ctx,
            view_errors=view_errors or None,
            tree_sitter_checks=tree_sitter_checks,
        )
        if view_errors:
            msg = f"Schema view validation failed: {view_errors}."
            raise ValueError(msg)
        self._record_schema_snapshots(ctx)

    def _prepare_statements(self, ctx: SessionContext) -> None:
        """Prepare SQL statements when configured."""
        statements = list(self.prepared_statements)
        if self.enable_schema_registry:
            statements.extend(CST_DIAGNOSTIC_STATEMENTS)
        seen: set[str] = set()
        for statement in statements:
            if statement.name in seen:
                continue
            seen.add(statement.name)
            ctx.sql(_prepare_statement_sql(statement))
            self._record_prepared_statement(statement)

    def _record_prepared_statement(self, statement: PreparedStatementSpec) -> None:
        if self.diagnostics_sink is None:
            return
        self.diagnostics_sink.record_artifact(
            "datafusion_prepared_statements_v1",
            {
                "name": statement.name,
                "sql": statement.sql,
                "param_types": list(statement.param_types),
            },
        )

    def ensure_delta_plan_codecs(self, ctx: SessionContext) -> bool:
        """Install Delta plan codecs when enabled.

        Returns
        -------
        bool
            True when codecs were installed, otherwise False.
        """
        if not self.enable_delta_plan_codecs:
            return False
        register = getattr(ctx, "register_extension_codecs", None)
        available = callable(register)
        installed = False
        if available:
            try:
                register(self.delta_plan_codec_physical, self.delta_plan_codec_logical)
            except TypeError:
                try:
                    register(self.delta_plan_codec_logical, self.delta_plan_codec_physical)
                except TypeError:
                    installed = False
                else:
                    installed = True
            else:
                installed = True
        self._record_delta_plan_codecs(
            available=available,
            installed=installed,
        )
        return installed

    def _record_udf_snapshot(self, snapshot: DataFusionUdfSnapshot) -> None:
        if self.diagnostics_sink is None:
            return
        self.diagnostics_sink.record_artifact(
            "datafusion_udf_registry_v1",
            snapshot.payload(),
        )

    def _record_delta_plan_codecs(self, *, available: bool, installed: bool) -> None:
        if self.diagnostics_sink is None:
            return
        self.diagnostics_sink.record_artifact(
            "datafusion_delta_plan_codecs_v1",
            {
                "enabled": self.enable_delta_plan_codecs,
                "available": available,
                "installed": installed,
                "physical_codec": self.delta_plan_codec_physical,
                "logical_codec": self.delta_plan_codec_logical,
            },
        )

    def _build_session_context(self) -> SessionContext:
        """Create the SessionContext base for this runtime profile.

        Returns
        -------
        datafusion.SessionContext
            Base session context for this profile.

        Raises
        ------
        ValueError
            Raised when distributed execution is enabled without a factory.
        """
        if not self.distributed:
            return SessionContext(self.session_config(), self.runtime_env_builder())
        if self.distributed_context_factory is None:
            msg = "Distributed execution requires distributed_context_factory."
            raise ValueError(msg)
        return self.distributed_context_factory()

    def _apply_url_table(self, ctx: SessionContext) -> SessionContext:
        return ctx.enable_url_table() if self.enable_url_table else ctx

    def _register_local_filesystem(self, ctx: SessionContext) -> None:
        if self.local_filesystem_root is None:
            return
        store = LocalFileSystem(prefix=self.local_filesystem_root)
        ctx.register_object_store("file://", store, None)

    def _install_function_factory(self, ctx: SessionContext) -> None:
        if not self.enable_function_factory:
            return
        available = True
        installed = False
        error: str | None = None
        cause: Exception | None = None
        try:
            if self.function_factory_hook is None:
                install_function_factory(ctx)
            else:
                self.function_factory_hook(ctx)
            installed = True
        except ImportError as exc:
            available = False
            error = str(exc)
            cause = exc
        except (RuntimeError, TypeError) as exc:
            error = str(exc)
            cause = exc
        self._record_function_factory(
            available=available,
            installed=installed,
            error=error,
        )
        if error is not None:
            msg = "FunctionFactory installation failed; native extension is required."
            raise RuntimeError(msg) from cause

    def _install_expr_planners(self, ctx: SessionContext) -> None:
        if not self.enable_expr_planners:
            return
        available = True
        installed = False
        error: str | None = None
        cause: Exception | None = None
        try:
            if self.expr_planner_hook is None:
                install_expr_planners(ctx, planner_names=self.expr_planner_names)
            else:
                self.expr_planner_hook(ctx)
            installed = True
        except ImportError as exc:
            available = False
            error = str(exc)
            cause = exc
        except (RuntimeError, TypeError, ValueError) as exc:
            error = str(exc)
            cause = exc
        self._record_expr_planners(
            available=available,
            installed=installed,
            error=error,
        )
        if error is not None:
            msg = "ExprPlanner installation failed; native extension is required."
            raise RuntimeError(msg) from cause

    def _install_physical_expr_adapter_factory(self, ctx: SessionContext) -> None:
        """Install a physical expression adapter factory when available.

        Raises
        ------
        TypeError
            Raised when the SessionContext cannot accept the factory.
        """
        factory = self.physical_expr_adapter_factory
        uses_default_adapter = False
        if factory is None and self.enable_schema_evolution_adapter:
            factory = _load_schema_evolution_adapter_factory()
            uses_default_adapter = True
        if factory is None:
            return
        register = getattr(ctx, "register_physical_expr_adapter_factory", None)
        if not callable(register):
            if uses_default_adapter:
                _install_schema_evolution_adapter_factory(ctx)
                return
            msg = "SessionContext does not expose physical expr adapter registration."
            raise TypeError(msg)
        register(factory)

    def _record_expr_planners(
        self,
        *,
        available: bool,
        installed: bool,
        error: str | None,
    ) -> None:
        if self.diagnostics_sink is None:
            return
        self.diagnostics_sink.record_artifact(
            "datafusion_expr_planners_v1",
            {
                "enabled": self.enable_expr_planners,
                "available": available,
                "installed": installed,
                "hook_enabled": bool(self.expr_planner_hook),
                "planner_names": list(self.expr_planner_names),
                "policy": expr_planner_payloads(self.expr_planner_names),
                "error": error,
            },
        )

    def _record_function_factory(
        self,
        *,
        available: bool,
        installed: bool,
        error: str | None,
    ) -> None:
        if self.diagnostics_sink is None:
            return
        self.diagnostics_sink.record_artifact(
            "datafusion_function_factory_v1",
            {
                "enabled": self.enable_function_factory,
                "available": available,
                "installed": installed,
                "hook_enabled": bool(self.function_factory_hook),
                "policy": function_factory_payloads(),
                "error": error,
            },
        )

    def _install_tracing(self) -> None:
        """Enable tracing when configured.

        Raises
        ------
        ValueError
            Raised when tracing is enabled without a hook.
        """
        if not self.enable_tracing:
            return
        if self.tracing_hook is None:
            msg = "Tracing enabled but tracing_hook is not set."
            raise ValueError(msg)
        self.tracing_hook()

    def _resolve_compile_hooks(
        self,
        resolved: DataFusionCompileOptions,
        *,
        capture_explain: bool,
        explain_analyze: bool,
        capture_plan_artifacts: bool,
    ) -> _ResolvedCompileHooks:
        explain_hook = resolved.explain_hook
        if explain_hook is None and capture_explain and self.explain_collector is not None:
            explain_hook = self.explain_collector.hook
        plan_artifacts_hook = resolved.plan_artifacts_hook
        if (
            plan_artifacts_hook is None
            and capture_plan_artifacts
            and self.plan_collector is not None
        ):
            plan_artifacts_hook = self.plan_collector.hook
        sql_ingest_hook = resolved.sql_ingest_hook
        cache_event_hook = resolved.cache_event_hook
        fallback_hook = resolved.fallback_hook
        if fallback_hook is None and self.capture_fallbacks and self.fallback_collector is not None:
            fallback_hook = self.fallback_collector.hook
        if self.diagnostics_sink is not None:
            fallback_hook = _chain_fallback_hooks(
                fallback_hook,
                diagnostics_fallback_hook(self.diagnostics_sink),
            )
            if capture_explain or explain_hook is not None:
                explain_hook = _chain_explain_hooks(
                    explain_hook,
                    diagnostics_explain_hook(
                        self.diagnostics_sink,
                        explain_analyze=explain_analyze,
                    ),
                )
            if capture_plan_artifacts or plan_artifacts_hook is not None:
                plan_artifacts_hook = _chain_plan_artifacts_hooks(
                    plan_artifacts_hook,
                    diagnostics_plan_artifacts_hook(self.diagnostics_sink),
                )
            sql_ingest_hook = _chain_sql_ingest_hooks(
                sql_ingest_hook,
                diagnostics_sql_ingest_hook(self.diagnostics_sink),
            )
            cache_event_hook = _chain_cache_hooks(
                cache_event_hook,
                diagnostics_cache_hook(self.diagnostics_sink),
            )
        return _ResolvedCompileHooks(
            explain_hook=explain_hook,
            plan_artifacts_hook=plan_artifacts_hook,
            sql_ingest_hook=sql_ingest_hook,
            fallback_hook=fallback_hook,
            cache_event_hook=cache_event_hook,
        )

    def _resolve_sql_policy(
        self,
        resolved: DataFusionCompileOptions,
    ) -> DataFusionSqlPolicy | None:
        if resolved.sql_policy is not None:
            return resolved.sql_policy
        if self.sql_policy is None and self.sql_policy_name is None:
            return None
        return self.sql_policy or resolve_sql_policy(self.sql_policy_name)

    def compile_options(
        self,
        *,
        options: DataFusionCompileOptions | None = None,
        params: Mapping[str, object] | Mapping[IbisValue, object] | None = None,
        execution_policy: AdapterExecutionPolicy | None = None,
        execution_label: ExecutionLabel | None = None,
    ) -> DataFusionCompileOptions:
        """Return DataFusion compile options derived from the profile.

        Returns
        -------
        DataFusionCompileOptions
            Compile options aligned with this runtime profile.
        """
        resolved = options or DataFusionCompileOptions(cache=None, cache_max_columns=None)
        cache = resolved.cache if resolved.cache is not None else self.cache_enabled
        cache_max_columns = (
            resolved.cache_max_columns
            if resolved.cache_max_columns is not None
            else self.cache_max_columns
        )
        resolved_params = resolved.params if resolved.params is not None else params
        param_allowlist = (
            resolved.param_identifier_allowlist
            if resolved.param_identifier_allowlist is not None
            else tuple(self.param_identifier_allowlist) or None
        )
        capture_explain = resolved.capture_explain or self.capture_explain
        explain_analyze = resolved.explain_analyze or self.explain_analyze
        substrait_validation = resolved.substrait_validation or self.substrait_validation
        capture_plan_artifacts = (
            resolved.capture_plan_artifacts
            or self.capture_plan_artifacts
            or capture_explain
            or substrait_validation
        )
        hooks = self._resolve_compile_hooks(
            resolved,
            capture_explain=capture_explain,
            explain_analyze=explain_analyze,
            capture_plan_artifacts=capture_plan_artifacts,
        )
        sql_policy = self._resolve_sql_policy(resolved)
        sql_policy_name = (
            resolved.sql_policy_name
            if resolved.sql_policy_name is not None
            else self.sql_policy_name
        )
        unchanged = (
            cache == resolved.cache,
            cache_max_columns == resolved.cache_max_columns,
            resolved_params == resolved.params,
            capture_explain == resolved.capture_explain,
            explain_analyze == resolved.explain_analyze,
            hooks.explain_hook == resolved.explain_hook,
            substrait_validation == resolved.substrait_validation,
            capture_plan_artifacts == resolved.capture_plan_artifacts,
            hooks.plan_artifacts_hook == resolved.plan_artifacts_hook,
            hooks.sql_ingest_hook == resolved.sql_ingest_hook,
            hooks.fallback_hook == resolved.fallback_hook,
            hooks.cache_event_hook == resolved.cache_event_hook,
            sql_policy == resolved.sql_policy,
            sql_policy_name == resolved.sql_policy_name,
            param_allowlist == resolved.param_identifier_allowlist,
        )
        if all(unchanged) and execution_policy is None and execution_label is None:
            return resolved
        updated = replace(
            resolved,
            cache=cache,
            cache_max_columns=cache_max_columns,
            params=resolved_params,
            param_identifier_allowlist=param_allowlist,
            capture_explain=capture_explain,
            explain_analyze=explain_analyze,
            explain_hook=hooks.explain_hook,
            substrait_validation=substrait_validation,
            capture_plan_artifacts=capture_plan_artifacts,
            plan_artifacts_hook=hooks.plan_artifacts_hook,
            sql_ingest_hook=hooks.sql_ingest_hook,
            fallback_hook=hooks.fallback_hook,
            cache_event_hook=hooks.cache_event_hook,
            sql_policy=sql_policy,
            sql_policy_name=sql_policy_name,
        )
        if execution_label is not None:
            updated = apply_execution_label(
                updated,
                execution_label=execution_label,
                fallback_sink=self.labeled_fallbacks,
                explain_sink=self.labeled_explains,
            )
        if execution_policy is None:
            return updated
        return apply_execution_policy(
            updated,
            execution_policy=execution_policy,
            execution_label=execution_label,
        )

    def record_view_definition(self, *, name: str, sql: str | None) -> None:
        """Record a view definition for diagnostics snapshots.

        Parameters
        ----------
        name:
            Name of the view.
        sql:
            SQL definition for the view, when available.
        """
        if self.view_registry is None:
            return
        self.view_registry.record(name=name, sql=sql)

    def view_registry_snapshot(self) -> list[dict[str, object]] | None:
        """Return a stable snapshot of recorded view definitions.

        Returns
        -------
        list[dict[str, object]] | None
            Snapshot payload or ``None`` when registry tracking is disabled.
        """
        if self.view_registry is None:
            return None
        return self.view_registry.snapshot()

    @staticmethod
    def _schema_introspector(ctx: SessionContext) -> SchemaIntrospector:
        """Return a schema introspector for the session.

        Returns
        -------
        SchemaIntrospector
            Introspector bound to the provided SessionContext.
        """
        return SchemaIntrospector(ctx)

    @staticmethod
    def settings_snapshot(ctx: SessionContext) -> pa.Table:
        """Return a snapshot of DataFusion settings when information_schema is enabled.

        Returns
        -------
        pyarrow.Table
            Table of settings from information_schema.df_settings.
        """
        query = "SELECT name, value FROM information_schema.df_settings"
        return ctx.sql(query).to_arrow_table()

    @staticmethod
    def catalog_snapshot(ctx: SessionContext) -> pa.Table:
        """Return a snapshot of DataFusion catalog tables when available.

        Returns
        -------
        pyarrow.Table
            Table inventory from information_schema.tables.
        """
        query = (
            "SELECT table_catalog, table_schema, table_name, table_type "
            "FROM information_schema.tables"
        )
        return ctx.sql(query).to_arrow_table()

    @staticmethod
    def function_catalog_snapshot(
        ctx: SessionContext,
        *,
        include_routines: bool = False,
    ) -> list[dict[str, object]]:
        """Return a stable snapshot of available DataFusion functions.

        Parameters
        ----------
        ctx:
            Session context to query.
        include_routines:
            Whether to include information_schema routines metadata.

        Returns
        -------
        list[dict[str, object]]
            Sorted function catalog entries from ``SHOW FUNCTIONS``.
        """
        table = ctx.sql("SHOW FUNCTIONS").to_arrow_table()
        rows = table.to_pylist()
        if include_routines:
            rows.extend(_information_schema_routines(ctx))
            rows.extend(_information_schema_parameters(ctx))
        return sorted(rows, key=_function_catalog_sort_key)

    def settings_payload(self) -> dict[str, str]:
        """Return resolved settings applied to DataFusion SessionConfig.

        Returns
        -------
        dict[str, str]
            Resolved DataFusion settings payload.
        """
        resolved_policy = self._resolved_config_policy()
        payload: dict[str, str] = (
            dict(resolved_policy.settings) if resolved_policy is not None else {}
        )
        resolved_schema_hardening = self._resolved_schema_hardening()
        if resolved_schema_hardening is not None:
            payload.update(resolved_schema_hardening.settings())
        if self.settings_overrides:
            payload.update({str(key): str(value) for key, value in self.settings_overrides.items()})
        catalog_location, catalog_format = self._effective_catalog_autoload()
        if catalog_location is not None:
            payload["datafusion.catalog.location"] = catalog_location
        if catalog_format is not None:
            payload["datafusion.catalog.format"] = catalog_format
        payload.update(self.feature_gates.settings())
        if self.join_policy is not None:
            payload.update(self.join_policy.settings())
        if self.explain_analyze_level is not None and _supports_explain_analyze_level():
            payload["datafusion.explain.analyze_level"] = self.explain_analyze_level
        return payload

    def settings_hash(self) -> str:
        """Return a stable hash for the SessionConfig settings payload.

        Returns
        -------
        str
            SHA-256 hash for the settings payload.
        """
        payload = {
            "version": SETTINGS_HASH_VERSION,
            "entries": _map_entries(self.settings_payload()),
        }
        return payload_hash(payload, _SETTINGS_HASH_SCHEMA)

    def telemetry_payload(self) -> dict[str, object]:
        """Return a diagnostics-friendly payload for the runtime profile.

        Returns
        -------
        dict[str, object]
            Runtime settings serialized for telemetry/diagnostics.
        """
        resolved_policy = self._resolved_config_policy()
        ast_partitions = [
            {"name": name, "dtype": str(dtype)} for name, dtype in self.ast_external_partition_cols
        ]
        bytecode_partitions = [
            {"name": name, "dtype": str(dtype)}
            for name, dtype in self.bytecode_external_partition_cols
        ]
        return {
            "datafusion_version": datafusion.__version__,
            "target_partitions": self.target_partitions,
            "batch_size": self.batch_size,
            "spill_dir": self.spill_dir,
            "memory_pool": self.memory_pool,
            "memory_limit_bytes": self.memory_limit_bytes,
            "default_catalog": self.default_catalog,
            "default_schema": self.default_schema,
            "catalog_auto_load_location": self.catalog_auto_load_location,
            "catalog_auto_load_format": self.catalog_auto_load_format,
            "ast_catalog_location": self.ast_catalog_location,
            "ast_catalog_format": self.ast_catalog_format,
            "ast_external_location": self.ast_external_location,
            "ast_external_format": self.ast_external_format,
            "ast_external_provider": self.ast_external_provider,
            "ast_external_ordering": list(self.ast_external_ordering),
            "ast_external_partitions": ast_partitions or None,
            "ast_external_unbounded": self.ast_external_unbounded,
            "ast_external_schema_force_view_types": self.ast_external_schema_force_view_types,
            "ast_external_skip_arrow_metadata": self.ast_external_skip_arrow_metadata,
            "ast_external_listing_table_factory_infer_partitions": (
                self.ast_external_listing_table_factory_infer_partitions
            ),
            "ast_external_listing_table_ignore_subdirectory": (
                self.ast_external_listing_table_ignore_subdirectory
            ),
            "ast_external_collect_statistics": self.ast_external_collect_statistics,
            "ast_external_meta_fetch_concurrency": self.ast_external_meta_fetch_concurrency,
            "ast_external_list_files_cache_ttl": self.ast_external_list_files_cache_ttl,
            "ast_external_list_files_cache_limit": self.ast_external_list_files_cache_limit,
            "ast_delta_location": self.ast_delta_location,
            "ast_delta_version": self.ast_delta_version,
            "ast_delta_timestamp": self.ast_delta_timestamp,
            "ast_delta_constraints": list(self.ast_delta_constraints),
            "ast_delta_scan": bool(self.ast_delta_scan),
            "bytecode_catalog_location": self.bytecode_catalog_location,
            "bytecode_catalog_format": self.bytecode_catalog_format,
            "bytecode_external_location": self.bytecode_external_location,
            "bytecode_external_format": self.bytecode_external_format,
            "bytecode_external_provider": self.bytecode_external_provider,
            "bytecode_external_ordering": list(self.bytecode_external_ordering),
            "bytecode_external_partitions": bytecode_partitions or None,
            "bytecode_external_unbounded": self.bytecode_external_unbounded,
            "bytecode_external_schema_force_view_types": (
                self.bytecode_external_schema_force_view_types
            ),
            "bytecode_external_skip_arrow_metadata": self.bytecode_external_skip_arrow_metadata,
            "bytecode_external_listing_table_factory_infer_partitions": (
                self.bytecode_external_listing_table_factory_infer_partitions
            ),
            "bytecode_external_listing_table_ignore_subdirectory": (
                self.bytecode_external_listing_table_ignore_subdirectory
            ),
            "bytecode_external_collect_statistics": self.bytecode_external_collect_statistics,
            "bytecode_external_meta_fetch_concurrency": (
                self.bytecode_external_meta_fetch_concurrency
            ),
            "bytecode_external_list_files_cache_ttl": self.bytecode_external_list_files_cache_ttl,
            "bytecode_external_list_files_cache_limit": (
                self.bytecode_external_list_files_cache_limit
            ),
            "bytecode_delta_location": self.bytecode_delta_location,
            "bytecode_delta_version": self.bytecode_delta_version,
            "bytecode_delta_timestamp": self.bytecode_delta_timestamp,
            "bytecode_delta_constraints": list(self.bytecode_delta_constraints),
            "bytecode_delta_scan": bool(self.bytecode_delta_scan),
            "enable_information_schema": self.enable_information_schema,
            "enable_url_table": self.enable_url_table,
            "cache_enabled": self.cache_enabled,
            "cache_max_columns": self.cache_max_columns,
            "cache_manager_enabled": self.enable_cache_manager,
            "cache_manager_factory": bool(self.cache_manager_factory),
            "function_factory_enabled": self.enable_function_factory,
            "function_factory_hook": bool(self.function_factory_hook),
            "expr_planners_enabled": self.enable_expr_planners,
            "expr_planner_hook": bool(self.expr_planner_hook),
            "expr_planner_names": list(self.expr_planner_names),
            "physical_expr_adapter_factory": bool(self.physical_expr_adapter_factory),
            "delta_plan_codecs_enabled": self.enable_delta_plan_codecs,
            "delta_plan_codec_physical": self.delta_plan_codec_physical,
            "delta_plan_codec_logical": self.delta_plan_codec_logical,
            "metrics_enabled": self.enable_metrics,
            "metrics_collector": bool(self.metrics_collector),
            "tracing_enabled": self.enable_tracing,
            "tracing_hook": bool(self.tracing_hook),
            "tracing_collector": bool(self.tracing_collector),
            "capture_explain": self.capture_explain,
            "explain_analyze": self.explain_analyze,
            "explain_analyze_level": self.explain_analyze_level,
            "explain_collector": bool(self.explain_collector),
            "capture_plan_artifacts": self.capture_plan_artifacts,
            "plan_collector": bool(self.plan_collector),
            "substrait_validation": self.substrait_validation,
            "capture_fallbacks": self.capture_fallbacks,
            "fallback_collector": bool(self.fallback_collector),
            "diagnostics_sink": bool(self.diagnostics_sink),
            "local_filesystem_root": self.local_filesystem_root,
            "input_plugins": len(self.input_plugins),
            "prepared_statements": [stmt.name for stmt in self.prepared_statements],
            "distributed": self.distributed,
            "distributed_context_factory": bool(self.distributed_context_factory),
            "runtime_env_hook": bool(self.runtime_env_hook),
            "session_context_hook": bool(self.session_context_hook),
            "config_policy_name": self.config_policy_name,
            "schema_hardening_name": self.schema_hardening_name,
            "config_policy": dict(resolved_policy.settings)
            if resolved_policy is not None
            else None,
            "sql_policy_name": self.sql_policy_name,
            "sql_policy": (
                {
                    "allow_ddl": self.sql_policy.allow_ddl,
                    "allow_dml": self.sql_policy.allow_dml,
                    "allow_statements": self.sql_policy.allow_statements,
                }
                if self.sql_policy is not None
                else None
            ),
            "param_identifier_allowlist": (
                list(self.param_identifier_allowlist) if self.param_identifier_allowlist else None
            ),
            "external_table_options": dict(self.external_table_options)
            if self.external_table_options
            else None,
            "write_policy": _datafusion_write_policy_payload(self.write_policy),
            "settings_overrides": dict(self.settings_overrides),
            "feature_gates": self.feature_gates.settings(),
            "join_policy": self.join_policy.settings() if self.join_policy is not None else None,
            "settings_hash": self.settings_hash(),
            "share_context": self.share_context,
            "session_context_key": self.session_context_key,
        }

    def telemetry_payload_v1(self) -> dict[str, object]:
        """Return a versioned runtime payload for diagnostics.

        Returns
        -------
        dict[str, object]
            Versioned runtime payload with grouped settings.
        """
        settings = self.settings_payload()
        ansi_mode = _ansi_mode(settings)
        parser_dialect = settings.get("datafusion.sql_parser.dialect")
        return {
            "version": 2,
            "profile_name": self.config_policy_name,
            "datafusion_version": datafusion.__version__,
            "schema_hardening_name": self.schema_hardening_name,
            "sql_policy_name": self.sql_policy_name,
            "session_config": dict(settings),
            "settings_hash": self.settings_hash(),
            "external_table_options": dict(self.external_table_options)
            if self.external_table_options
            else None,
            "sql_policy": (
                {
                    "allow_ddl": self.sql_policy.allow_ddl,
                    "allow_dml": self.sql_policy.allow_dml,
                    "allow_statements": self.sql_policy.allow_statements,
                }
                if self.sql_policy is not None
                else None
            ),
            "param_identifier_allowlist": (
                list(self.param_identifier_allowlist) if self.param_identifier_allowlist else None
            ),
            "write_policy": _datafusion_write_policy_payload(self.write_policy),
            "feature_gates": dict(self.feature_gates.settings()),
            "join_policy": self.join_policy.settings() if self.join_policy is not None else None,
            "parquet_read": _settings_by_prefix(settings, "datafusion.execution.parquet."),
            "listing_table": _settings_by_prefix(settings, "datafusion.runtime.list_files_"),
            "spill": {
                "spill_dir": self.spill_dir,
                "memory_pool": self.memory_pool,
                "memory_limit_bytes": self.memory_limit_bytes,
            },
            "execution": {
                "target_partitions": self.target_partitions,
                "batch_size": self.batch_size,
            },
            "sql_surfaces": {
                "enable_information_schema": self.enable_information_schema,
                "enable_url_table": self.enable_url_table,
                "sql_parser_dialect": parser_dialect,
                "ansi_mode": ansi_mode,
            },
            "extensions": {
                "delta_plan_codecs_enabled": self.enable_delta_plan_codecs,
                "delta_plan_codec_physical": self.delta_plan_codec_physical,
                "delta_plan_codec_logical": self.delta_plan_codec_logical,
                "expr_planners_enabled": self.enable_expr_planners,
                "expr_planner_names": list(self.expr_planner_names),
                "physical_expr_adapter_factory": bool(self.physical_expr_adapter_factory),
                "schema_evolution_adapter_enabled": self.enable_schema_evolution_adapter,
                "named_args_supported": self.named_args_supported(),
                "distributed": self.distributed,
                "distributed_context_factory": bool(self.distributed_context_factory),
            },
            "substrait_validation": self.substrait_validation,
            "output_writes": {
                "cache_enabled": self.cache_enabled,
                "cache_max_columns": self.cache_max_columns,
                "datafusion_write_policy": _datafusion_write_policy_payload(self.write_policy),
            },
        }

    def telemetry_payload_hash(self) -> str:
        """Return a stable hash for the versioned telemetry payload.

        Returns
        -------
        str
            SHA-256 hash of the telemetry payload.
        """
        return payload_hash(self._telemetry_payload_row(), _TELEMETRY_SCHEMA)

    def collect_metrics(self) -> Mapping[str, object] | None:
        """Return optional DataFusion metrics payload.

        Returns
        -------
        Mapping[str, object] | None
            Metrics payload when enabled and available.
        """
        if not self.enable_metrics or self.metrics_collector is None:
            return None
        return self.metrics_collector()

    def collect_traces(self) -> Mapping[str, object] | None:
        """Return optional DataFusion tracing payload.

        Returns
        -------
        Mapping[str, object] | None
            Tracing payload when enabled and available.
        """
        if not self.enable_tracing or self.tracing_collector is None:
            return None
        return self.tracing_collector()

    def _resolved_config_policy(self) -> DataFusionConfigPolicy | None:
        if self.config_policy is not None:
            return self.config_policy
        if self.config_policy_name is None:
            return DEFAULT_DF_POLICY
        return DATAFUSION_POLICY_PRESETS.get(self.config_policy_name, DEFAULT_DF_POLICY)

    def _resolved_schema_hardening(self) -> SchemaHardeningProfile | None:
        if self.schema_hardening is not None:
            return self.schema_hardening
        if self.schema_hardening_name is None:
            return None
        return SCHEMA_HARDENING_PRESETS.get(
            self.schema_hardening_name,
            SCHEMA_HARDENING_PRESETS["schema_hardening"],
        )

    def _telemetry_payload_row(self) -> dict[str, object]:
        settings = self.settings_payload()
        sql_policy_payload = None
        if self.sql_policy is not None:
            sql_policy_payload = {
                "allow_ddl": self.sql_policy.allow_ddl,
                "allow_dml": self.sql_policy.allow_dml,
                "allow_statements": self.sql_policy.allow_statements,
            }
        write_policy_payload = _datafusion_write_policy_payload(self.write_policy)
        parquet_read = _settings_by_prefix(settings, "datafusion.execution.parquet.")
        listing_table = _settings_by_prefix(settings, "datafusion.runtime.list_files_")
        parser_dialect = settings.get("datafusion.sql_parser.dialect")
        ansi_mode = _ansi_mode(settings)
        return {
            "version": TELEMETRY_PAYLOAD_VERSION,
            "profile_name": self.config_policy_name,
            "datafusion_version": datafusion.__version__,
            "sql_policy_name": self.sql_policy_name,
            "session_config": _map_entries(settings),
            "settings_hash": self.settings_hash(),
            "external_table_options": (
                _map_entries(self.external_table_options) if self.external_table_options else None
            ),
            "sql_policy": sql_policy_payload,
            "param_identifier_allowlist": (
                list(self.param_identifier_allowlist) if self.param_identifier_allowlist else None
            ),
            "write_policy": write_policy_payload,
            "feature_gates": _map_entries(self.feature_gates.settings()),
            "join_policy": (
                _map_entries(self.join_policy.settings()) if self.join_policy is not None else None
            ),
            "parquet_read": _map_entries(parquet_read),
            "listing_table": _map_entries(listing_table),
            "spill": {
                "spill_dir": self.spill_dir,
                "memory_pool": self.memory_pool,
                "memory_limit_bytes": self.memory_limit_bytes,
            },
            "execution": {
                "target_partitions": self.target_partitions,
                "batch_size": self.batch_size,
            },
            "sql_surfaces": {
                "enable_information_schema": self.enable_information_schema,
                "enable_url_table": self.enable_url_table,
                "sql_parser_dialect": parser_dialect,
                "ansi_mode": ansi_mode,
            },
            "extensions": {
                "delta_plan_codecs_enabled": self.enable_delta_plan_codecs,
                "delta_plan_codec_physical": self.delta_plan_codec_physical,
                "delta_plan_codec_logical": self.delta_plan_codec_logical,
                "expr_planners_enabled": self.enable_expr_planners,
                "expr_planner_names": list(self.expr_planner_names),
                "named_args_supported": self.named_args_supported(),
                "distributed": self.distributed,
                "distributed_context_factory": bool(self.distributed_context_factory),
            },
            "substrait_validation": self.substrait_validation,
            "output_writes": {
                "cache_enabled": self.cache_enabled,
                "cache_max_columns": self.cache_max_columns,
                "datafusion_write_policy": write_policy_payload,
            },
        }

    def _cache_key(self) -> str:
        if self.session_context_key:
            return self.session_context_key
        return self.telemetry_payload_hash()

    def context_cache_key(self) -> str:
        """Return a stable cache key for the session context.

        Returns
        -------
        str
            Stable cache key derived from the runtime profile.
        """
        return self._cache_key()

    def _cached_context(self) -> SessionContext | None:
        if not self.share_context:
            return None
        return _SESSION_CONTEXT_CACHE.get(self._cache_key())

    def _cache_context(self, ctx: SessionContext) -> None:
        if not self.share_context:
            return
        _SESSION_CONTEXT_CACHE[self._cache_key()] = ctx


def apply_execution_label(
    options: DataFusionCompileOptions,
    *,
    execution_label: ExecutionLabel | None,
    fallback_sink: list[dict[str, object]] | None,
    explain_sink: list[dict[str, object]] | None,
) -> DataFusionCompileOptions:
    """Return compile options with rule-scoped diagnostics hooks applied.

    Parameters
    ----------
    options:
        Base compile options to update.
    execution_label:
        Optional label used to annotate diagnostics.
    fallback_sink:
        Destination list for labeled fallback entries.
    explain_sink:
        Destination list for labeled explain entries.

    Returns
    -------
    DataFusionCompileOptions
        Options updated with labeled diagnostics hooks when configured.
    """
    if execution_label is None:
        return options
    fallback_hook = options.fallback_hook
    if fallback_sink is not None:
        fallback_hook = _chain_fallback_hooks(
            fallback_hook,
            labeled_fallback_hook(execution_label, fallback_sink),
        )
    explain_hook = options.explain_hook
    if explain_sink is not None and (options.capture_explain or explain_hook is not None):
        explain_hook = _chain_explain_hooks(
            explain_hook,
            labeled_explain_hook(execution_label, explain_sink),
        )
    if fallback_hook is options.fallback_hook and explain_hook is options.explain_hook:
        return options
    return replace(options, fallback_hook=fallback_hook, explain_hook=explain_hook)


def apply_execution_policy(
    options: DataFusionCompileOptions,
    *,
    execution_policy: AdapterExecutionPolicy | None,
    execution_label: ExecutionLabel | None = None,
) -> DataFusionCompileOptions:
    """Return compile options with an execution policy enforced.

    Parameters
    ----------
    options:
        Base compile options to update.
    execution_policy:
        Optional execution policy that can block fallback usage.
    execution_label:
        Optional label used for fallback error context.
    execution_label:
        Optional label used to provide context in fallback errors.

    Returns
    -------
    DataFusionCompileOptions
        Options updated with fallback policy enforcement when configured.
    """
    force_sql = options.force_sql
    if execution_policy is not None and execution_policy.force_sql:
        force_sql = True
    fallback_hook = _apply_fallback_policy(
        policy=execution_policy,
        fallback_hook=options.fallback_hook,
        label=execution_label,
    )
    if fallback_hook is options.fallback_hook and force_sql == options.force_sql:
        return options
    return replace(options, fallback_hook=fallback_hook, force_sql=force_sql)


__all__ = [
    "DATAFUSION_POLICY_PRESETS",
    "DEFAULT_DF_POLICY",
    "DEV_DF_POLICY",
    "PROD_DF_POLICY",
    "SCHEMA_HARDENING_PRESETS",
    "AdapterExecutionPolicy",
    "DataFusionConfigPolicy",
    "DataFusionExplainCollector",
    "DataFusionFallbackCollector",
    "DataFusionFeatureGates",
    "DataFusionJoinPolicy",
    "DataFusionPlanCollector",
    "DataFusionRuntimeProfile",
    "DataFusionSettingsContract",
    "ExecutionLabel",
    "FeatureStateSnapshot",
    "MemoryPool",
    "PreparedStatementSpec",
    "SchemaHardeningProfile",
    "apply_execution_label",
    "apply_execution_policy",
    "diagnostics_arrow_ingest_hook",
    "feature_state_snapshot",
    "register_view_specs",
    "snapshot_plans",
]

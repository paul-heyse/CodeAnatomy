"""Runtime profile helpers for DataFusion execution."""

from __future__ import annotations

import contextlib
import importlib
import logging
import os
import time
import uuid
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from functools import lru_cache
from typing import TYPE_CHECKING, Literal, cast

import datafusion
import msgspec
import pyarrow as pa
from datafusion import RuntimeEnvBuilder, SessionConfig, SessionContext, SQLOptions
from datafusion.dataframe import DataFrame
from datafusion.object_store import LocalFileSystem
from sqlglot.errors import ParseError

from arrowdsl.core.determinism import DeterminismTier
from arrowdsl.core.interop import RecordBatchReaderLike, SchemaLike, TableLike, coerce_table_like
from arrowdsl.core.schema_constants import DEFAULT_VALUE_META
from arrowdsl.schema.metadata import schema_constraints_from_metadata
from arrowdsl.schema.serialization import schema_fingerprint
from cache.diskcache_factory import (
    DiskCacheKind,
    DiskCacheProfile,
    cache_for_kind,
    default_diskcache_profile,
    diskcache_stats_snapshot,
    evict_cache_tag,
    run_profile_maintenance,
)
from datafusion_engine.compile_options import (
    DataFusionCacheEvent,
    DataFusionCompileOptions,
    DataFusionSqlPolicy,
    DataFusionSubstraitFallbackEvent,
    resolve_sql_policy,
)
from datafusion_engine.diagnostics import (
    DiagnosticsSink,
    ensure_recorder_sink,
    record_artifact,
    record_events,
)
from datafusion_engine.expr_planner import expr_planner_payloads, install_expr_planners
from datafusion_engine.function_factory import function_factory_payloads, install_function_factory
from datafusion_engine.introspection import (
    capture_cache_diagnostics,
    introspection_cache_for_ctx,
    register_cache_introspection_functions,
)
from datafusion_engine.schema_introspection import (
    SchemaIntrospector,
    catalogs_snapshot,
    constraint_rows,
    table_constraint_rows,
)
from datafusion_engine.schema_registry import (
    AST_CORE_VIEW_NAMES,
    AST_OPTIONAL_VIEW_NAMES,
    CST_VIEW_NAMES,
    SCIP_VIEW_SCHEMA_MAP,
    TREE_SITTER_CHECK_VIEWS,
    TREE_SITTER_VIEW_NAMES,
    is_nested_dataset,
    missing_schema_names,
    nested_schema_for,
    nested_schema_names,
    nested_view_specs,
    register_all_schemas,
    schema_for,
    schema_names,
    validate_ast_views,
    validate_bytecode_views,
    validate_cst_views,
    validate_nested_types,
    validate_required_engine_functions,
    validate_scip_views,
    validate_symtable_views,
    validate_ts_views,
)
from datafusion_engine.sql_safety import (
    ExecutionProfileOptions,
    execute_with_profile,
    execution_policy_for_profile,
)
from datafusion_engine.table_provider_metadata import table_provider_metadata
from datafusion_engine.udf_catalog import get_default_udf_catalog, get_strict_udf_catalog
from datafusion_engine.udf_runtime import register_rust_udfs
from engine.plan_cache import PlanCache
from serde_msgspec import StructBase
from storage.ipc import payload_hash

if TYPE_CHECKING:
    from diskcache import Cache, FanoutCache

    from datafusion_engine.udf_catalog import UdfCatalog
    from ibis_engine.registry import DatasetCatalog, DatasetLocation
    from obs.datafusion_runs import DataFusionRun
    from storage.deltalake.delta import IdempotentWriteOptions
from schema_spec.policies import DataFusionWritePolicy
from schema_spec.system import (
    DataFusionScanOptions,
    DatasetSpec,
    DeltaScanOptions,
    TableSchemaContract,
    dataset_spec_from_schema,
)
from schema_spec.view_specs import ViewSpec
from sqlglot_tools.optimizer import parse_sql_strict, register_datafusion_dialect

if TYPE_CHECKING:
    from ibis.expr.types import Value as IbisValue

    ExplainRows = TableLike | RecordBatchReaderLike
else:
    ExplainRows = object

ExplainHook = Callable[[str, ExplainRows], None]
PlanArtifactsHook = Callable[[Mapping[str, object]], None]
SemanticDiffHook = Callable[[Mapping[str, object]], None]
SqlIngestHook = Callable[[Mapping[str, object]], None]
CacheEventHook = Callable[[DataFusionCacheEvent], None]
SubstraitFallbackHook = Callable[[DataFusionSubstraitFallbackEvent], None]

_TELEMETRY_MSGPACK_ENCODER = msgspec.msgpack.Encoder(order="deterministic")


def _encode_telemetry_msgpack(payload: object) -> bytes:
    buf = bytearray()
    _TELEMETRY_MSGPACK_ENCODER.encode_into(payload, buf)
    return bytes(buf)


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
        pa.field("parquet_bloom_filter_on_write", pa.bool_()),
        pa.field("parquet_dictionary_enabled", pa.bool_()),
        pa.field("parquet_encoding", pa.string()),
        pa.field("parquet_skip_arrow_metadata", pa.bool_()),
        pa.field("parquet_column_options", pa.map_(pa.string(), pa.string())),
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
        pa.field("repartition_aggregations", pa.bool_()),
        pa.field("repartition_windows", pa.bool_()),
        pa.field("repartition_file_scans", pa.bool_()),
        pa.field("repartition_file_min_size", pa.int64()),
    ]
)
_SQL_SURFACES_SCHEMA = pa.struct(
    [
        pa.field("enable_information_schema", pa.bool_()),
        pa.field("enable_ident_normalization", pa.bool_()),
        pa.field("enable_url_table", pa.bool_()),
        pa.field("sql_parser_dialect", pa.string()),
        pa.field("ansi_mode", pa.bool_()),
    ]
)
_EXTENSIONS_SCHEMA = pa.struct(
    [
        pa.field("delta_session_defaults_enabled", pa.bool_()),
        pa.field("delta_querybuilder_enabled", pa.bool_()),
        pa.field("delta_data_checker_enabled", pa.bool_()),
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
        pa.field("minimum_parallel_output_files", pa.int64()),
        pa.field("soft_max_rows_per_output_file", pa.int64()),
        pa.field("maximum_parallel_row_group_writers", pa.int64()),
        pa.field("objectstore_writer_buffer_size", pa.int64()),
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
    enable_round_robin_repartition: bool = True
    perfect_hash_join_small_build_threshold: int | None = None
    perfect_hash_join_min_key_density: float | None = None

    def settings(self) -> dict[str, str]:
        """Return DataFusion config settings for join preferences.

        Returns
        -------
        dict[str, str]
            Mapping of DataFusion config keys to string values.
        """
        settings = {
            "datafusion.optimizer.enable_hash_join": str(self.enable_hash_join).lower(),
            "datafusion.optimizer.enable_sort_merge_join": str(self.enable_sort_merge_join).lower(),
            "datafusion.optimizer.enable_nested_loop_join": str(
                self.enable_nested_loop_join
            ).lower(),
            "datafusion.optimizer.repartition_joins": str(self.repartition_joins).lower(),
            "datafusion.optimizer.enable_round_robin_repartition": str(
                self.enable_round_robin_repartition
            ).lower(),
        }
        if self.perfect_hash_join_small_build_threshold is not None:
            settings["datafusion.execution.perfect_hash_join_small_build_threshold"] = str(
                self.perfect_hash_join_small_build_threshold
            )
        if self.perfect_hash_join_min_key_density is not None:
            settings["datafusion.execution.perfect_hash_join_min_key_density"] = str(
                self.perfect_hash_join_min_key_density
            )
        return settings


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


class FeatureStateSnapshot(
    StructBase,
    array_like=True,
    gc=False,
    cache_hash=True,
    frozen=True,
):
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


@dataclass(frozen=True)
class PreparedStatementSpec:
    """Prepared statement specification for DataFusion."""

    name: str
    sql: str
    param_types: tuple[str, ...] = ()


@dataclass(frozen=True)
class AdapterExecutionPolicy:
    """Execution policy for adapterized execution handling."""


@dataclass(frozen=True)
class ExecutionLabel:
    """Execution label for task-scoped diagnostics."""

    task_name: str
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

INFO_SCHEMA_STATEMENTS: tuple[PreparedStatementSpec, ...] = (
    PreparedStatementSpec(
        name="table_names_snapshot",
        sql="SELECT table_name FROM information_schema.tables",
    ),
    PreparedStatementSpec(
        name="tables_snapshot",
        sql="SELECT table_catalog, table_schema, table_name, table_type FROM information_schema.tables",
    ),
    PreparedStatementSpec(
        name="df_settings_snapshot",
        sql="SELECT name, value FROM information_schema.df_settings",
    ),
    PreparedStatementSpec(
        name="routines_snapshot",
        sql="SELECT * FROM information_schema.routines",
    ),
    PreparedStatementSpec(
        name="parameters_snapshot",
        sql=(
            "SELECT specific_name, routine_name, parameter_name, parameter_mode, "
            "data_type, ordinal_position FROM information_schema.parameters"
        ),
    ),
)
INFO_SCHEMA_STATEMENT_NAMES: frozenset[str] = frozenset(
    spec.name for spec in INFO_SCHEMA_STATEMENTS
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
    if profile.schema_hardening is not None:
        resolved = profile.schema_hardening
    elif profile.schema_hardening_name is None:
        resolved = None
    else:
        resolved = SCHEMA_HARDENING_PRESETS.get(
            profile.schema_hardening_name,
            SCHEMA_HARDENING_PRESETS["schema_hardening"],
        )
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


def _constraint_key_fields(rows: Sequence[Mapping[str, object]]) -> list[str]:
    constraints: dict[tuple[str, str], list[tuple[int, str]]] = {}
    for row in rows:
        constraint_type = row.get("constraint_type")
        if not isinstance(constraint_type, str):
            continue
        constraint_kind = constraint_type.upper()
        if constraint_kind not in {"PRIMARY KEY", "UNIQUE"}:
            continue
        constraint_name = row.get("constraint_name")
        column_name = row.get("column_name")
        if not isinstance(constraint_name, str) or not constraint_name:
            continue
        if not isinstance(column_name, str) or not column_name:
            continue
        ordinal = row.get("ordinal_position")
        position = int(ordinal) if isinstance(ordinal, (int, float)) else 0
        constraints.setdefault((constraint_kind, constraint_name), []).append(
            (position, column_name)
        )
    if not constraints:
        return []
    for kind in ("PRIMARY KEY", "UNIQUE"):
        candidates = {key: values for key, values in constraints.items() if key[0] == kind}
        if not candidates:
            continue
        _, values = sorted(candidates.items(), key=lambda item: item[0][1])[0]
        return [name for _, name in sorted(values, key=lambda item: item[0])]
    return []


def _is_nullable(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"no", "false", "0"}:
            return False
        if normalized in {"yes", "true", "1"}:
            return True
    return True


def _constraint_drift_entries(
    introspector: SchemaIntrospector,
    *,
    names: Sequence[str],
) -> list[dict[str, object]]:
    entries: list[dict[str, object]] = []
    for name in names:
        schema = schema_for(name)
        expected_required, expected_keys = schema_constraints_from_metadata(schema.metadata)
        expected_required_set = set(expected_required)
        expected_keys_set = set(expected_keys)
        try:
            columns = introspector.table_columns(name)
            constraint_rows = table_constraint_rows(
                introspector.ctx,
                table_name=name,
                sql_options=introspector.sql_options,
            )
        except (RuntimeError, TypeError, ValueError):
            continue
        observed_required = {
            str(row["column_name"])
            for row in columns
            if row.get("column_name") is not None and not _is_nullable(row.get("is_nullable"))
        }
        observed_keys = set(_constraint_key_fields(constraint_rows))
        missing_required = sorted(expected_required_set - observed_required)
        extra_required = sorted(observed_required - expected_required_set)
        missing_keys = sorted(expected_keys_set - observed_keys)
        extra_keys = sorted(observed_keys - expected_keys_set)
        if not missing_required and not extra_required and not missing_keys and not extra_keys:
            continue
        entries.append(
            {
                "schema_name": name,
                "expected_required_non_null": sorted(expected_required_set) or None,
                "observed_required_non_null": sorted(observed_required) or None,
                "missing_required_non_null": missing_required or None,
                "extra_required_non_null": extra_required or None,
                "expected_key_fields": list(expected_keys) or None,
                "observed_key_fields": sorted(observed_keys) or None,
                "missing_key_fields": missing_keys or None,
                "extra_key_fields": extra_keys or None,
            }
        )
    return entries


def _relationship_constraint_errors(
    ctx: SessionContext,
    *,
    sql_options: SQLOptions,
) -> Mapping[str, object] | None:
    try:
        module = importlib.import_module("schema_spec.relationship_specs")
    except ImportError:
        return None
    errors = getattr(module, "relationship_constraint_errors", None)
    if not callable(errors):
        return None
    try:
        result = errors(ctx, sql_options=sql_options)
    except (RuntimeError, TypeError, ValueError):
        return None
    if isinstance(result, Mapping) and result:
        return result
    return None


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
    sql_options = None
    if runtime_profile is not None:
        profile = runtime_profile
        sql_options = statement_sql_options_for_profile(profile)

        def _record_view(name: str, sql: str | None) -> None:
            record_view_definition(profile, name=name, sql=sql)

        record_view = _record_view
    for view in views:
        view.register(
            ctx,
            record_view=record_view,
            validate=validate,
            sql_options=sql_options,
        )


def _register_schema_table(ctx: SessionContext, name: str, schema: pa.Schema) -> None:
    """Register a schema-only table via an empty table provider."""
    arrays = [pa.array([], type=field.type) for field in schema]
    table = pa.Table.from_arrays(arrays, schema=schema)
    from datafusion_engine.io_adapter import DataFusionIOAdapter

    adapter = DataFusionIOAdapter(ctx=ctx, profile=None)
    adapter.register_arrow_table(name, table)


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


def _apply_optional_int_setting(
    config: SessionConfig,
    *,
    key: str,
    value: int | None,
) -> SessionConfig:
    if value is None:
        return config
    setter = getattr(config, "set", None)
    if callable(setter):
        return cast("SessionConfig", setter(key, str(int(value))))
    return config


def _apply_optional_bool_config(
    config: SessionConfig,
    *,
    method: str,
    key: str,
    value: bool | None,
) -> SessionConfig:
    if value is None:
        return config
    updater = getattr(config, method, None)
    if callable(updater):
        return cast("SessionConfig", updater(value))
    setter = getattr(config, "set", None)
    if callable(setter):
        return cast("SessionConfig", setter(key, str(value).lower()))
    return config


def _apply_optional_bool_setting(
    config: SessionConfig,
    *,
    key: str,
    value: bool | None,
) -> SessionConfig:
    if value is None:
        return config
    setter = getattr(config, "set", None)
    if callable(setter):
        return cast("SessionConfig", setter(key, str(value).lower()))
    return config


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


def _apply_identifier_settings(
    config: SessionConfig,
    *,
    enable_ident_normalization: bool,
) -> SessionConfig:
    return config.set(
        "datafusion.sql_parser.enable_ident_normalization",
        str(enable_ident_normalization).lower(),
    )


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


def _read_only_sql_options() -> SQLOptions:
    return DataFusionSqlPolicy().to_sql_options()


def _sql_with_options(
    ctx: SessionContext,
    sql: str,
    *,
    sql_options: SQLOptions | None = None,
    runtime_profile: DataFusionRuntimeProfile | None = None,
    allow_statements: bool | None = None,
) -> DataFrame:
    return execute_with_profile(
        ctx,
        sql,
        profile=runtime_profile,
        options=ExecutionProfileOptions(
            sql_options=sql_options or _read_only_sql_options(),
            allow_statements=allow_statements,
        ),
    )


def sql_options_for_profile(profile: DataFusionRuntimeProfile | None) -> SQLOptions:
    """Return SQL options derived from a runtime profile.

    Returns
    -------
    datafusion.SQLOptions
        SQL options based on the runtime policy or read-only defaults.
    """
    if profile is None:
        return _read_only_sql_options()
    return profile.sql_options()


def statement_sql_options_for_profile(profile: DataFusionRuntimeProfile | None) -> SQLOptions:
    """Return SQL options that allow statement execution.

    Returns
    -------
    datafusion.SQLOptions
        SQL options that allow statements, with fallback defaults.
    """
    return execution_policy_for_profile(profile, allow_statements=True).to_sql_options()


def settings_snapshot_for_profile(
    profile: DataFusionRuntimeProfile, ctx: SessionContext
) -> pa.Table:
    """Return a DataFusion settings snapshot for a runtime profile.

    Returns
    -------
    pyarrow.Table
        Table of settings from information_schema.df_settings.
    """
    cache = introspection_cache_for_ctx(ctx, sql_options=profile.sql_options())
    return cache.snapshot.settings


def catalog_snapshot_for_profile(
    profile: DataFusionRuntimeProfile, ctx: SessionContext
) -> pa.Table:
    """Return a DataFusion catalog snapshot for a runtime profile.

    Returns
    -------
    pyarrow.Table
        Table inventory from information_schema.tables.
    """
    cache = introspection_cache_for_ctx(ctx, sql_options=profile.sql_options())
    return cache.snapshot.tables


def function_catalog_snapshot_for_profile(
    profile: DataFusionRuntimeProfile,
    ctx: SessionContext,
    *,
    include_routines: bool = False,
) -> list[dict[str, object]]:
    """Return a function catalog snapshot for a runtime profile.

    Returns
    -------
    list[dict[str, object]]
        Sorted function catalog entries from information_schema.
    """
    return schema_introspector_for_profile(profile, ctx).function_catalog_snapshot(
        include_parameters=include_routines,
    )


def record_view_definition(
    profile: DataFusionRuntimeProfile,
    *,
    name: str,
    sql: str | None,
) -> None:
    """Record a view definition for diagnostics snapshots."""
    if profile.view_registry is None:
        return
    profile.view_registry.record(name=name, sql=sql)


def _datafusion_version(ctx: SessionContext) -> str | None:
    try:
        table = _sql_with_options(ctx, "SELECT version() AS version").to_arrow_table()
    except (RuntimeError, TypeError, ValueError):
        return None
    if "version" not in table.column_names or table.num_rows < 1:
        return None
    values = table["version"].to_pylist()
    value = values[0] if values else None
    return str(value) if value is not None else None


def _datafusion_function_names(ctx: SessionContext) -> set[str]:
    try:
        names = SchemaIntrospector(ctx, sql_options=sql_options_for_profile(None)).function_names()
    except (RuntimeError, TypeError, ValueError):
        return set()
    return {name.lower() for name in names}


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


def _chain_substrait_fallback_hooks(
    *hooks: Callable[[DataFusionSubstraitFallbackEvent], None] | None,
) -> Callable[[DataFusionSubstraitFallbackEvent], None] | None:
    active = [hook for hook in hooks if hook is not None]
    if not active:
        return None

    def _hook(event: DataFusionSubstraitFallbackEvent) -> None:
        for hook in active:
            hook(event)

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
                "task_name": label.task_name,
                "output": label.output_dataset,
                "sql": sql,
                "rows": rows,
            }
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


def diagnostics_substrait_fallback_hook(
    sink: DiagnosticsSink,
) -> Callable[[DataFusionSubstraitFallbackEvent], None]:
    """Return a Substrait fallback hook that records diagnostics rows.

    Returns
    -------
    Callable[[DataFusionSubstraitFallbackEvent], None]
        Hook that records Substrait fallback events in the diagnostics sink.
    """

    def _hook(event: DataFusionSubstraitFallbackEvent) -> None:
        recorder_sink = ensure_recorder_sink(sink, session_id="runtime")
        recorder_sink.record_events(
            "substrait_fallbacks_v1",
            [
                {
                    "event_time_unix_ms": int(time.time() * 1000),
                    "reason": event.reason,
                    "expr_type": event.expr_type,
                    "plan_hash": event.plan_hash,
                    "profile_hash": event.profile_hash,
                    "run_id": event.run_id,
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
        recorder_sink = ensure_recorder_sink(sink, session_id="runtime")
        recorder_sink.record_events(
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
        recorder_sink = ensure_recorder_sink(sink, session_id="runtime")
        recorder_sink.record_artifact("datafusion_plan_artifacts_v1", payload)

    return _hook


def diagnostics_semantic_diff_hook(
    sink: DiagnosticsSink,
) -> Callable[[Mapping[str, object]], None]:
    """Return a semantic diff hook that records diagnostics payloads.

    Returns
    -------
    Callable[[Mapping[str, object]], None]
        Hook that records semantic diff diagnostics in the sink.
    """

    def _hook(payload: Mapping[str, object]) -> None:
        recorder_sink = ensure_recorder_sink(sink, session_id="runtime")
        recorder_sink.record_artifact("datafusion_semantic_diff_v1", payload)

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
        recorder_sink = ensure_recorder_sink(sink, session_id="runtime")
        recorder_sink.record_artifact("ibis_sql_ingest_v1", payload)

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
        recorder_sink = ensure_recorder_sink(sink, session_id="runtime")
        recorder_sink.record_artifact("datafusion_arrow_ingest_v1", payload)

    return _hook


def diagnostics_dml_hook(
    sink: DiagnosticsSink,
) -> Callable[[Mapping[str, object]], None]:
    """Return a DML hook that records diagnostics payloads.

    Returns
    -------
    Callable[[Mapping[str, object]], None]
        Hook that records DML statement payloads in the diagnostics sink.
    """

    def _hook(payload: Mapping[str, object]) -> None:
        sink.record_events(
            "datafusion_dml_statements_v1",
            [
                {
                    "event_time_unix_ms": int(time.time() * 1000),
                    **dict(payload),
                }
            ],
        )

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
    semantic_diff_hook: Callable[[Mapping[str, object]], None] | None
    sql_ingest_hook: Callable[[Mapping[str, object]], None] | None
    cache_event_hook: Callable[[DataFusionCacheEvent], None] | None
    substrait_fallback_hook: Callable[[DataFusionSubstraitFallbackEvent], None] | None


@dataclass(frozen=True)
class _CompileOptionResolution:
    cache: bool | None
    cache_max_columns: int | None
    params: Mapping[str, object] | Mapping[IbisValue, object] | None
    param_allowlist: tuple[str, ...] | None
    prepared_param_types: Mapping[str, str] | None
    prepared_statements: bool
    dynamic_projection: bool | None
    capture_explain: bool
    explain_analyze: bool
    substrait_validation: bool
    capture_plan_artifacts: bool
    capture_semantic_diff: bool
    sql_policy: DataFusionSqlPolicy | None
    sql_policy_name: str | None


@dataclass(frozen=True)
class _ScipRegistrationSnapshot:
    name: str
    requested_name: str
    location: DatasetLocation
    expected_fingerprint: str
    actual_fingerprint: str
    schema_match: bool


def _resolve_prepared_statement_options(
    resolved: DataFusionCompileOptions,
    *,
    resolved_params: Mapping[str, object] | Mapping[IbisValue, object] | None,
) -> tuple[Mapping[str, str] | None, bool, bool | None]:
    prepared_param_types = resolved.prepared_param_types
    prepared_statements = resolved.prepared_statements
    dynamic_projection = resolved.dynamic_projection
    if prepared_param_types is None and prepared_statements and resolved_params:
        from ibis_engine.params_bridge import param_types_from_bindings

        try:
            prepared_param_types = param_types_from_bindings(resolved_params)
        except ValueError:
            prepared_param_types = None
    return prepared_param_types, prepared_statements, dynamic_projection


def _resolved_config_policy_for_profile(
    profile: DataFusionRuntimeProfile,
) -> DataFusionConfigPolicy | None:
    if profile.config_policy is not None:
        return profile.config_policy
    if profile.config_policy_name is None:
        return DEFAULT_DF_POLICY
    return DATAFUSION_POLICY_PRESETS.get(profile.config_policy_name, DEFAULT_DF_POLICY)


def _resolved_schema_hardening_for_profile(
    profile: DataFusionRuntimeProfile,
) -> SchemaHardeningProfile | None:
    if profile.schema_hardening is not None:
        return profile.schema_hardening
    if profile.schema_hardening_name is None:
        return None
    return SCHEMA_HARDENING_PRESETS.get(
        profile.schema_hardening_name,
        SCHEMA_HARDENING_PRESETS["schema_hardening"],
    )


def _effective_catalog_autoload_for_profile(
    profile: DataFusionRuntimeProfile,
) -> tuple[str | None, str | None]:
    if profile.ast_catalog_location is not None or profile.ast_catalog_format is not None:
        return profile.ast_catalog_location, profile.ast_catalog_format
    if profile.bytecode_catalog_location is not None or profile.bytecode_catalog_format is not None:
        return profile.bytecode_catalog_location, profile.bytecode_catalog_format
    if (
        profile.catalog_auto_load_location is not None
        or profile.catalog_auto_load_format is not None
    ):
        return profile.catalog_auto_load_location, profile.catalog_auto_load_format
    env_settings = _catalog_autoload_settings()
    return (
        env_settings.get("datafusion.catalog.location"),
        env_settings.get("datafusion.catalog.format"),
    )


def _build_telemetry_payload_row(profile: DataFusionRuntimeProfile) -> dict[str, object]:
    settings = profile.settings_payload()
    sql_policy_payload = None
    if profile.sql_policy is not None:
        sql_policy_payload = {
            "allow_ddl": profile.sql_policy.allow_ddl,
            "allow_dml": profile.sql_policy.allow_dml,
            "allow_statements": profile.sql_policy.allow_statements,
        }
    write_policy_payload = _datafusion_write_policy_payload(profile.write_policy)
    parquet_read = _settings_by_prefix(settings, "datafusion.execution.parquet.")
    listing_table = _settings_by_prefix(settings, "datafusion.runtime.list_files_")
    parser_dialect = settings.get("datafusion.sql_parser.dialect")
    ansi_mode = _ansi_mode(settings)
    return {
        "version": TELEMETRY_PAYLOAD_VERSION,
        "profile_name": profile.config_policy_name,
        "datafusion_version": datafusion.__version__,
        "sql_policy_name": profile.sql_policy_name,
        "session_config": _map_entries(settings),
        "settings_hash": profile.settings_hash(),
        "external_table_options": (
            _map_entries(profile.external_table_options) if profile.external_table_options else None
        ),
        "sql_policy": sql_policy_payload,
        "param_identifier_allowlist": (
            list(profile.param_identifier_allowlist) if profile.param_identifier_allowlist else None
        ),
        "write_policy": write_policy_payload,
        "feature_gates": _map_entries(profile.feature_gates.settings()),
        "join_policy": (
            _map_entries(profile.join_policy.settings())
            if profile.join_policy is not None
            else None
        ),
        "parquet_read": _map_entries(parquet_read),
        "listing_table": _map_entries(listing_table),
        "spill": {
            "spill_dir": profile.spill_dir,
            "memory_pool": profile.memory_pool,
            "memory_limit_bytes": profile.memory_limit_bytes,
        },
        "execution": {
            "target_partitions": profile.target_partitions,
            "batch_size": profile.batch_size,
            "repartition_aggregations": profile.repartition_aggregations,
            "repartition_windows": profile.repartition_windows,
            "repartition_file_scans": profile.repartition_file_scans,
            "repartition_file_min_size": profile.repartition_file_min_size,
        },
        "sql_surfaces": {
            "enable_information_schema": profile.enable_information_schema,
            "enable_ident_normalization": _effective_ident_normalization(profile),
            "enable_url_table": profile.enable_url_table,
            "sql_parser_dialect": parser_dialect,
            "ansi_mode": ansi_mode,
        },
        "extensions": {
            "delta_session_defaults_enabled": profile.enable_delta_session_defaults,
            "delta_querybuilder_enabled": profile.enable_delta_querybuilder,
            "delta_data_checker_enabled": profile.enable_delta_data_checker,
            "delta_plan_codecs_enabled": profile.enable_delta_plan_codecs,
            "delta_plan_codec_physical": profile.delta_plan_codec_physical,
            "delta_plan_codec_logical": profile.delta_plan_codec_logical,
            "expr_planners_enabled": profile.enable_expr_planners,
            "expr_planner_names": list(profile.expr_planner_names),
            "named_args_supported": profile.named_args_supported(),
            "distributed": profile.distributed,
            "distributed_context_factory": bool(profile.distributed_context_factory),
        },
        "substrait_validation": profile.substrait_validation,
        "output_writes": {
            "cache_enabled": profile.cache_enabled,
            "cache_max_columns": profile.cache_max_columns,
            "minimum_parallel_output_files": profile.minimum_parallel_output_files,
            "soft_max_rows_per_output_file": profile.soft_max_rows_per_output_file,
            "maximum_parallel_row_group_writers": profile.maximum_parallel_row_group_writers,
            "objectstore_writer_buffer_size": profile.objectstore_writer_buffer_size,
            "datafusion_write_policy": write_policy_payload,
        },
    }


def _apply_optional_settings(
    payload: dict[str, str],
    entries: Mapping[str, object | None],
) -> None:
    for key, value in entries.items():
        if value is None:
            continue
        if isinstance(value, bool):
            payload[key] = str(value).lower()
        else:
            payload[key] = str(value)


def _runtime_settings_payload(profile: DataFusionRuntimeProfile) -> dict[str, str]:
    enable_ident_normalization = _effective_ident_normalization(profile)
    payload: dict[str, str] = {
        "datafusion.sql_parser.enable_ident_normalization": str(enable_ident_normalization).lower()
    }
    optional_values = {
        "datafusion.optimizer.repartition_aggregations": profile.repartition_aggregations,
        "datafusion.optimizer.repartition_windows": profile.repartition_windows,
        "datafusion.execution.repartition_file_scans": profile.repartition_file_scans,
        "datafusion.execution.repartition_file_min_size": profile.repartition_file_min_size,
        "datafusion.execution.minimum_parallel_output_files": profile.minimum_parallel_output_files,
        "datafusion.execution.soft_max_rows_per_output_file": profile.soft_max_rows_per_output_file,
        "datafusion.execution.maximum_parallel_row_group_writers": (
            profile.maximum_parallel_row_group_writers
        ),
        "datafusion.execution.objectstore_writer_buffer_size": profile.objectstore_writer_buffer_size,
    }
    _apply_optional_settings(payload, optional_values)
    return payload


def _effective_ident_normalization(profile: DataFusionRuntimeProfile) -> bool:
    if profile.enable_delta_session_defaults:
        return False
    return profile.enable_ident_normalization


def _extra_settings_payload(profile: DataFusionRuntimeProfile) -> dict[str, str]:
    payload: dict[str, str] = {}
    catalog_location, catalog_format = _effective_catalog_autoload_for_profile(profile)
    if catalog_location is not None:
        payload["datafusion.catalog.location"] = catalog_location
    if catalog_format is not None:
        payload["datafusion.catalog.format"] = catalog_format
    payload.update(profile.feature_gates.settings())
    if profile.join_policy is not None:
        payload.update(profile.join_policy.settings())
    if profile.explain_analyze_level is not None and _supports_explain_analyze_level():
        payload["datafusion.explain.analyze_level"] = profile.explain_analyze_level
    return payload


class _RuntimeDiagnosticsMixin:
    def record_artifact(self, name: str, payload: Mapping[str, object]) -> None:
        """Record an artifact through DiagnosticsRecorder when configured."""
        profile = cast("DataFusionRuntimeProfile", self)
        record_artifact(profile, name, payload)

    def record_events(self, name: str, rows: Sequence[Mapping[str, object]]) -> None:
        """Record events through DiagnosticsRecorder when configured."""
        profile = cast("DataFusionRuntimeProfile", self)
        record_events(profile, name, rows)

    def view_registry_snapshot(self) -> list[dict[str, object]] | None:
        """Return a stable snapshot of recorded view definitions.

        Returns
        -------
        list[dict[str, object]] | None
            Snapshot payload or ``None`` when registry tracking is disabled.
        """
        profile = cast("DataFusionRuntimeProfile", self)
        if profile.view_registry is None:
            return None
        return profile.view_registry.snapshot()

    def settings_payload(self) -> dict[str, str]:
        """Return resolved settings applied to DataFusion SessionConfig.

        Returns
        -------
        dict[str, str]
            Resolved DataFusion settings payload.
        """
        profile = cast("DataFusionRuntimeProfile", self)
        resolved_policy = _resolved_config_policy_for_profile(profile)
        payload: dict[str, str] = (
            dict(resolved_policy.settings) if resolved_policy is not None else {}
        )
        resolved_schema_hardening = _resolved_schema_hardening_for_profile(profile)
        if resolved_schema_hardening is not None:
            payload.update(resolved_schema_hardening.settings())
        payload.update(_runtime_settings_payload(profile))
        if profile.settings_overrides:
            payload.update(
                {str(key): str(value) for key, value in profile.settings_overrides.items()}
            )
        payload.update(_extra_settings_payload(profile))
        return payload

    def settings_hash(self) -> str:
        """Return a stable hash for the SessionConfig settings payload.

        Returns
        -------
        str
            SHA-256 hash for the settings payload.
        """
        profile = cast("DataFusionRuntimeProfile", self)
        payload = {
            "version": SETTINGS_HASH_VERSION,
            "entries": _map_entries(profile.settings_payload()),
        }
        return payload_hash(payload, _SETTINGS_HASH_SCHEMA)

    def telemetry_payload(self) -> dict[str, object]:
        """Return a diagnostics-friendly payload for the runtime profile.

        Returns
        -------
        dict[str, object]
            Runtime settings serialized for telemetry/diagnostics.
        """
        profile = cast("DataFusionRuntimeProfile", self)
        resolved_policy = _resolved_config_policy_for_profile(profile)
        ast_partitions = [
            {"name": name, "dtype": str(dtype)}
            for name, dtype in profile.ast_external_partition_cols
        ]
        bytecode_partitions = [
            {"name": name, "dtype": str(dtype)}
            for name, dtype in profile.bytecode_external_partition_cols
        ]
        return {
            "datafusion_version": datafusion.__version__,
            "target_partitions": profile.target_partitions,
            "batch_size": profile.batch_size,
            "spill_dir": profile.spill_dir,
            "memory_pool": profile.memory_pool,
            "memory_limit_bytes": profile.memory_limit_bytes,
            "default_catalog": profile.default_catalog,
            "default_schema": profile.default_schema,
            "enable_ident_normalization": _effective_ident_normalization(profile),
            "catalog_auto_load_location": profile.catalog_auto_load_location,
            "catalog_auto_load_format": profile.catalog_auto_load_format,
            "ast_catalog_location": profile.ast_catalog_location,
            "ast_catalog_format": profile.ast_catalog_format,
            "ast_external_location": profile.ast_external_location,
            "ast_external_format": profile.ast_external_format,
            "ast_external_provider": profile.ast_external_provider,
            "ast_external_ordering": [list(key) for key in profile.ast_external_ordering],
            "ast_external_partitions": ast_partitions or None,
            "ast_external_schema_force_view_types": profile.ast_external_schema_force_view_types,
            "ast_external_skip_arrow_metadata": profile.ast_external_skip_arrow_metadata,
            "ast_external_listing_table_factory_infer_partitions": (
                profile.ast_external_listing_table_factory_infer_partitions
            ),
            "ast_external_listing_table_ignore_subdirectory": (
                profile.ast_external_listing_table_ignore_subdirectory
            ),
            "ast_external_collect_statistics": profile.ast_external_collect_statistics,
            "ast_external_meta_fetch_concurrency": profile.ast_external_meta_fetch_concurrency,
            "ast_external_list_files_cache_ttl": profile.ast_external_list_files_cache_ttl,
            "ast_external_list_files_cache_limit": profile.ast_external_list_files_cache_limit,
            "ast_delta_location": profile.ast_delta_location,
            "ast_delta_version": profile.ast_delta_version,
            "ast_delta_timestamp": profile.ast_delta_timestamp,
            "ast_delta_constraints": list(profile.ast_delta_constraints),
            "ast_delta_scan": bool(profile.ast_delta_scan),
            "bytecode_catalog_location": profile.bytecode_catalog_location,
            "bytecode_catalog_format": profile.bytecode_catalog_format,
            "bytecode_external_location": profile.bytecode_external_location,
            "bytecode_external_format": profile.bytecode_external_format,
            "bytecode_external_provider": profile.bytecode_external_provider,
            "bytecode_external_ordering": [list(key) for key in profile.bytecode_external_ordering],
            "bytecode_external_partitions": bytecode_partitions or None,
            "bytecode_external_schema_force_view_types": (
                profile.bytecode_external_schema_force_view_types
            ),
            "bytecode_external_skip_arrow_metadata": profile.bytecode_external_skip_arrow_metadata,
            "bytecode_external_listing_table_factory_infer_partitions": (
                profile.bytecode_external_listing_table_factory_infer_partitions
            ),
            "bytecode_external_listing_table_ignore_subdirectory": (
                profile.bytecode_external_listing_table_ignore_subdirectory
            ),
            "bytecode_external_collect_statistics": profile.bytecode_external_collect_statistics,
            "bytecode_external_meta_fetch_concurrency": (
                profile.bytecode_external_meta_fetch_concurrency
            ),
            "bytecode_external_list_files_cache_ttl": profile.bytecode_external_list_files_cache_ttl,
            "bytecode_external_list_files_cache_limit": (
                profile.bytecode_external_list_files_cache_limit
            ),
            "bytecode_delta_location": profile.bytecode_delta_location,
            "bytecode_delta_version": profile.bytecode_delta_version,
            "bytecode_delta_timestamp": profile.bytecode_delta_timestamp,
            "bytecode_delta_constraints": list(profile.bytecode_delta_constraints),
            "bytecode_delta_scan": bool(profile.bytecode_delta_scan),
            "enable_information_schema": profile.enable_information_schema,
            "enable_url_table": profile.enable_url_table,
            "cache_enabled": profile.cache_enabled,
            "cache_max_columns": profile.cache_max_columns,
            "minimum_parallel_output_files": profile.minimum_parallel_output_files,
            "soft_max_rows_per_output_file": profile.soft_max_rows_per_output_file,
            "maximum_parallel_row_group_writers": profile.maximum_parallel_row_group_writers,
            "cache_manager_enabled": profile.enable_cache_manager,
            "cache_manager_factory": bool(profile.cache_manager_factory),
            "function_factory_enabled": profile.enable_function_factory,
            "function_factory_hook": bool(profile.function_factory_hook),
            "expr_planners_enabled": profile.enable_expr_planners,
            "expr_planner_hook": bool(profile.expr_planner_hook),
            "expr_planner_names": list(profile.expr_planner_names),
            "physical_expr_adapter_factory": bool(profile.physical_expr_adapter_factory),
            "delta_session_defaults_enabled": profile.enable_delta_session_defaults,
            "delta_querybuilder_enabled": profile.enable_delta_querybuilder,
            "delta_data_checker_enabled": profile.enable_delta_data_checker,
            "delta_plan_codecs_enabled": profile.enable_delta_plan_codecs,
            "delta_plan_codec_physical": profile.delta_plan_codec_physical,
            "delta_plan_codec_logical": profile.delta_plan_codec_logical,
            "metrics_enabled": profile.enable_metrics,
            "metrics_collector": bool(profile.metrics_collector),
            "tracing_enabled": profile.enable_tracing,
            "tracing_hook": bool(profile.tracing_hook),
            "tracing_collector": bool(profile.tracing_collector),
            "capture_explain": profile.capture_explain,
            "explain_analyze": profile.explain_analyze,
            "explain_analyze_level": profile.explain_analyze_level,
            "explain_collector": bool(profile.explain_collector),
            "capture_plan_artifacts": profile.capture_plan_artifacts,
            "capture_semantic_diff": profile.capture_semantic_diff,
            "plan_collector": bool(profile.plan_collector),
            "substrait_validation": profile.substrait_validation,
            "diagnostics_sink": bool(profile.diagnostics_sink),
            "local_filesystem_root": profile.local_filesystem_root,
            "input_plugins": len(profile.input_plugins),
            "prepared_statements": [stmt.name for stmt in profile.prepared_statements],
            "distributed": profile.distributed,
            "distributed_context_factory": bool(profile.distributed_context_factory),
            "runtime_env_hook": bool(profile.runtime_env_hook),
            "session_context_hook": bool(profile.session_context_hook),
            "config_policy_name": profile.config_policy_name,
            "schema_hardening_name": profile.schema_hardening_name,
            "config_policy": dict(resolved_policy.settings)
            if resolved_policy is not None
            else None,
            "sql_policy_name": profile.sql_policy_name,
            "sql_policy": (
                {
                    "allow_ddl": profile.sql_policy.allow_ddl,
                    "allow_dml": profile.sql_policy.allow_dml,
                    "allow_statements": profile.sql_policy.allow_statements,
                }
                if profile.sql_policy is not None
                else None
            ),
            "param_identifier_allowlist": (
                list(profile.param_identifier_allowlist)
                if profile.param_identifier_allowlist
                else None
            ),
            "external_table_options": dict(profile.external_table_options)
            if profile.external_table_options
            else None,
            "write_policy": _datafusion_write_policy_payload(profile.write_policy),
            "settings_overrides": dict(profile.settings_overrides),
            "feature_gates": profile.feature_gates.settings(),
            "join_policy": (
                profile.join_policy.settings() if profile.join_policy is not None else None
            ),
            "settings_hash": profile.settings_hash(),
            "share_context": profile.share_context,
            "session_context_key": profile.session_context_key,
        }

    def telemetry_payload_v1(self) -> dict[str, object]:
        """Return a versioned runtime payload for diagnostics.

        Returns
        -------
        dict[str, object]
            Versioned runtime payload with grouped settings.
        """
        profile = cast("DataFusionRuntimeProfile", self)
        settings = profile.settings_payload()
        ansi_mode = _ansi_mode(settings)
        parser_dialect = settings.get("datafusion.sql_parser.dialect")
        return {
            "version": 2,
            "profile_name": profile.config_policy_name,
            "datafusion_version": datafusion.__version__,
            "schema_hardening_name": profile.schema_hardening_name,
            "sql_policy_name": profile.sql_policy_name,
            "session_config": dict(settings),
            "settings_hash": profile.settings_hash(),
            "external_table_options": dict(profile.external_table_options)
            if profile.external_table_options
            else None,
            "sql_policy": (
                {
                    "allow_ddl": profile.sql_policy.allow_ddl,
                    "allow_dml": profile.sql_policy.allow_dml,
                    "allow_statements": profile.sql_policy.allow_statements,
                }
                if profile.sql_policy is not None
                else None
            ),
            "param_identifier_allowlist": (
                list(profile.param_identifier_allowlist)
                if profile.param_identifier_allowlist
                else None
            ),
            "write_policy": _datafusion_write_policy_payload(profile.write_policy),
            "feature_gates": dict(profile.feature_gates.settings()),
            "join_policy": profile.join_policy.settings()
            if profile.join_policy is not None
            else None,
            "parquet_read": _settings_by_prefix(settings, "datafusion.execution.parquet."),
            "listing_table": _settings_by_prefix(settings, "datafusion.runtime.list_files_"),
            "spill": {
                "spill_dir": profile.spill_dir,
                "memory_pool": profile.memory_pool,
                "memory_limit_bytes": profile.memory_limit_bytes,
            },
            "execution": {
                "target_partitions": profile.target_partitions,
                "batch_size": profile.batch_size,
                "repartition_aggregations": profile.repartition_aggregations,
                "repartition_windows": profile.repartition_windows,
                "repartition_file_scans": profile.repartition_file_scans,
                "repartition_file_min_size": profile.repartition_file_min_size,
            },
            "sql_surfaces": {
                "enable_information_schema": profile.enable_information_schema,
                "enable_ident_normalization": _effective_ident_normalization(profile),
                "enable_url_table": profile.enable_url_table,
                "sql_parser_dialect": parser_dialect,
                "ansi_mode": ansi_mode,
            },
            "extensions": {
                "delta_session_defaults_enabled": profile.enable_delta_session_defaults,
                "delta_querybuilder_enabled": profile.enable_delta_querybuilder,
                "delta_data_checker_enabled": profile.enable_delta_data_checker,
                "delta_plan_codecs_enabled": profile.enable_delta_plan_codecs,
                "delta_plan_codec_physical": profile.delta_plan_codec_physical,
                "delta_plan_codec_logical": profile.delta_plan_codec_logical,
                "expr_planners_enabled": profile.enable_expr_planners,
                "expr_planner_names": list(profile.expr_planner_names),
                "physical_expr_adapter_factory": bool(profile.physical_expr_adapter_factory),
                "schema_evolution_adapter_enabled": profile.enable_schema_evolution_adapter,
                "named_args_supported": profile.named_args_supported(),
                "distributed": profile.distributed,
                "distributed_context_factory": bool(profile.distributed_context_factory),
            },
            "substrait_validation": profile.substrait_validation,
            "output_writes": {
                "cache_enabled": profile.cache_enabled,
                "cache_max_columns": profile.cache_max_columns,
                "minimum_parallel_output_files": profile.minimum_parallel_output_files,
                "soft_max_rows_per_output_file": profile.soft_max_rows_per_output_file,
                "maximum_parallel_row_group_writers": profile.maximum_parallel_row_group_writers,
                "objectstore_writer_buffer_size": profile.objectstore_writer_buffer_size,
                "datafusion_write_policy": _datafusion_write_policy_payload(profile.write_policy),
            },
        }

    def telemetry_payload_msgpack(self) -> bytes:
        """Return a MessagePack-encoded telemetry payload.

        Returns
        -------
        bytes
            MessagePack payload for runtime telemetry.
        """
        return _encode_telemetry_msgpack(self.telemetry_payload_v1())

    def telemetry_payload_hash(self) -> str:
        """Return a stable hash for the versioned telemetry payload.

        Returns
        -------
        str
            SHA-256 hash of the telemetry payload.
        """
        profile = cast("DataFusionRuntimeProfile", self)
        return payload_hash(_build_telemetry_payload_row(profile), _TELEMETRY_SCHEMA)


@dataclass(frozen=True)
class DataFusionRuntimeProfile(_RuntimeDiagnosticsMixin):
    """DataFusion runtime configuration."""

    target_partitions: int | None = None
    batch_size: int | None = None
    repartition_aggregations: bool | None = None
    repartition_windows: bool | None = None
    repartition_file_scans: bool | None = None
    repartition_file_min_size: int | None = None
    minimum_parallel_output_files: int | None = None
    soft_max_rows_per_output_file: int | None = None
    maximum_parallel_row_group_writers: int | None = None
    objectstore_writer_buffer_size: int | None = None
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
    ast_external_provider: Literal["listing", "parquet"] | None = None
    ast_external_ordering: tuple[tuple[str, str], ...] = (
        ("repo", "ascending"),
        ("path", "ascending"),
    )
    ast_external_partition_cols: tuple[tuple[str, pa.DataType], ...] = (
        ("repo", pa.string()),
        ("path", pa.string()),
    )
    ast_external_schema_force_view_types: bool | None = False
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
    bytecode_external_provider: Literal["listing", "parquet"] | None = None
    bytecode_external_ordering: tuple[tuple[str, str], ...] = (
        ("path", "ascending"),
        ("file_id", "ascending"),
    )
    bytecode_external_partition_cols: tuple[tuple[str, pa.DataType], ...] = ()
    bytecode_external_schema_force_view_types: bool | None = False
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
    extract_dataset_locations: Mapping[str, DatasetLocation] = field(default_factory=dict)
    scip_dataset_locations: Mapping[str, DatasetLocation] = field(default_factory=dict)
    enable_information_schema: bool = True
    enable_ident_normalization: bool = False
    enable_url_table: bool = False  # Dev-only convenience for file-path queries.
    cache_enabled: bool = False
    cache_max_columns: int | None = 64
    enable_cache_manager: bool = False
    cache_manager_factory: Callable[[], object] | None = None
    enable_function_factory: bool = True
    function_factory_hook: Callable[[SessionContext], None] | None = None
    enable_schema_registry: bool = True
    enable_expr_planners: bool = True
    expr_planner_names: tuple[str, ...] = ("codeanatomy_domain",)
    expr_planner_hook: Callable[[SessionContext], None] | None = None
    physical_expr_adapter_factory: object | None = None
    schema_adapter_factories: Mapping[str, object] = field(default_factory=dict)
    enable_schema_evolution_adapter: bool = True
    enable_udfs: bool = True
    udf_catalog_policy: Literal["default", "strict"] = "default"
    require_delta: bool = True
    enable_delta_session_defaults: bool = False
    enable_delta_querybuilder: bool = False
    enable_delta_data_checker: bool = False
    enable_delta_plan_codecs: bool = False
    delta_plan_codec_physical: str = "delta_physical"
    delta_plan_codec_logical: str = "delta_logical"
    enable_metrics: bool = False
    metrics_collector: Callable[[], Mapping[str, object] | None] | None = None
    enable_tracing: bool = False
    tracing_hook: Callable[[SessionContext], None] | None = None
    tracing_collector: Callable[[], Mapping[str, object] | None] | None = None
    enforce_preflight: bool = True
    capture_explain: bool = False
    explain_analyze: bool = True
    explain_analyze_level: str | None = None
    explain_collector: DataFusionExplainCollector | None = field(
        default_factory=DataFusionExplainCollector
    )
    capture_plan_artifacts: bool = True
    capture_semantic_diff: bool = False
    plan_collector: DataFusionPlanCollector | None = field(default_factory=DataFusionPlanCollector)
    view_registry: DataFusionViewRegistry | None = field(default_factory=DataFusionViewRegistry)
    substrait_validation: bool = False
    diagnostics_sink: DiagnosticsSink | None = None
    labeled_explains: list[dict[str, object]] = field(default_factory=list)
    diskcache_profile: DiskCacheProfile | None = field(default_factory=default_diskcache_profile)
    plan_cache: PlanCache | None = None
    udf_catalog_cache: dict[int, UdfCatalog] = field(default_factory=dict, repr=False)
    delta_commit_runs: dict[str, DataFusionRun] = field(default_factory=dict, repr=False)
    local_filesystem_root: str | None = None
    input_plugins: tuple[Callable[[SessionContext], None], ...] = ()
    prepared_statements: tuple[PreparedStatementSpec, ...] = INFO_SCHEMA_STATEMENTS
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

    def __post_init__(self) -> None:
        """Initialize defaults after dataclass construction."""
        if self.plan_cache is None:
            object.__setattr__(
                self,
                "plan_cache",
                PlanCache(cache_profile=self.diskcache_profile),
            )
        if self.diagnostics_sink is not None:
            object.__setattr__(
                self,
                "diagnostics_sink",
                ensure_recorder_sink(
                    self.diagnostics_sink,
                    session_id=self.context_cache_key(),
                ),
            )

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
        config = _apply_identifier_settings(
            config,
            enable_ident_normalization=_effective_ident_normalization(self),
        )
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
        config = _apply_optional_bool_config(
            config,
            method="with_repartition_aggregations",
            key="datafusion.optimizer.repartition_aggregations",
            value=self.repartition_aggregations,
        )
        config = _apply_optional_bool_config(
            config,
            method="with_repartition_windows",
            key="datafusion.optimizer.repartition_windows",
            value=self.repartition_windows,
        )
        config = _apply_optional_bool_config(
            config,
            method="with_repartition_file_scans",
            key="datafusion.execution.repartition_file_scans",
            value=self.repartition_file_scans,
        )
        config = _apply_optional_int_setting(
            config,
            key="datafusion.execution.repartition_file_min_size",
            value=self.repartition_file_min_size,
        )
        config = _apply_optional_int_setting(
            config,
            key="datafusion.execution.minimum_parallel_output_files",
            value=self.minimum_parallel_output_files,
        )
        config = _apply_optional_int_setting(
            config,
            key="datafusion.execution.soft_max_rows_per_output_file",
            value=self.soft_max_rows_per_output_file,
        )
        config = _apply_optional_int_setting(
            config,
            key="datafusion.execution.maximum_parallel_row_group_writers",
            value=self.maximum_parallel_row_group_writers,
        )
        config = _apply_optional_int_setting(
            config,
            key="datafusion.execution.objectstore_writer_buffer_size",
            value=self.objectstore_writer_buffer_size,
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
        return _effective_catalog_autoload_for_profile(self)

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
        self._apply_delta_session_defaults(ctx)
        self._register_local_filesystem(ctx)
        self._install_input_plugins(ctx)
        self._install_registry_catalogs(ctx)
        self._install_delta_table_factory(ctx)
        self._install_udfs(ctx)
        self._install_schema_registry(ctx)
        self._validate_rule_function_allowlist(ctx)
        self._prepare_statements(ctx)
        self.ensure_delta_plan_codecs(ctx)
        self._install_function_factory(ctx)
        self._install_expr_planners(ctx)
        self._record_extension_parity_validation()
        self._install_physical_expr_adapter_factory(ctx)
        self._install_tracing(ctx)
        self._install_cache_tables(ctx)
        self._record_cache_diagnostics(ctx)
        if self.session_context_hook is not None:
            ctx = self.session_context_hook(ctx)
        self._cache_context(ctx)
        return ctx

    def ephemeral_context(self) -> SessionContext:
        """Return a non-cached SessionContext configured from the profile.

        Returns
        -------
        datafusion.SessionContext
            Session context configured for the profile without caching.
        """
        ctx = self._build_session_context()
        ctx = self._apply_url_table(ctx)
        self._apply_delta_session_defaults(ctx)
        self._register_local_filesystem(ctx)
        self._install_input_plugins(ctx)
        self._install_registry_catalogs(ctx)
        self._install_delta_table_factory(ctx)
        self._install_udfs(ctx)
        self._install_schema_registry(ctx)
        self._validate_rule_function_allowlist(ctx)
        self._prepare_statements(ctx)
        self.ensure_delta_plan_codecs(ctx)
        self._install_function_factory(ctx)
        self._install_expr_planners(ctx)
        self._record_extension_parity_validation()
        self._install_physical_expr_adapter_factory(ctx)
        self._install_tracing(ctx)
        self._install_cache_tables(ctx)
        self._record_cache_diagnostics(ctx)
        if self.session_context_hook is not None:
            ctx = self.session_context_hook(ctx)
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

    def validate_named_args_extension_parity(self) -> dict[str, object]:
        """Validate that named-arg support aligns with extension capabilities.

        This method checks whether the Python-side configuration for named arguments
        is consistent with the available Rust extension capabilities.

        Returns
        -------
        dict[str, object]
            Validation report with status and details.
        """
        warnings: list[str] = []

        # Check if function factory is enabled but expr planners are not
        if self.enable_function_factory and not self.enable_expr_planners:
            warnings.append(
                "FunctionFactory enabled without ExprPlanners; "
                "named arguments may not be supported in SQL."
            )

        # Check if expr planners are configured
        valid = True
        if self.enable_expr_planners and not self.expr_planner_names and not self.expr_planner_hook:
            valid = False
            warnings.append("ExprPlanners enabled but no planner names or hook configured.")

        return {
            "valid": valid,
            "enable_function_factory": self.enable_function_factory,
            "enable_expr_planners": self.enable_expr_planners,
            "expr_planner_names": list(self.expr_planner_names),
            "named_args_supported": self.named_args_supported(),
            "warnings": warnings,
        }

    def record_schema_snapshots(self) -> None:
        """Record information_schema snapshots to diagnostics when enabled."""
        self._record_schema_snapshots(self.session_context())

    def _install_input_plugins(self, ctx: SessionContext) -> None:
        """Install input plugins on the session context."""
        for plugin in self.input_plugins:
            plugin(ctx)

    def _install_registry_catalogs(self, ctx: SessionContext) -> None:
        """Install registry-backed catalog providers on the session context."""
        if not self.registry_catalogs:
            return
        from datafusion_engine.catalog_provider import (
            register_registry_catalogs,
        )

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

    def _install_delta_table_factory(self, ctx: SessionContext) -> None:
        """Install Delta TableProviderFactory for DDL registration.

        Raises
        ------
        RuntimeError
            Raised when the DataFusion extension module is unavailable.
        TypeError
            Raised when the factory installer is missing in the extension.
        """
        if not self.require_delta:
            return
        try:
            module = importlib.import_module("datafusion_ext")
        except ImportError as exc:
            msg = "Delta table factory requires datafusion_ext."
            raise RuntimeError(msg) from exc
        installer = getattr(module, "install_delta_table_factory", None)
        if not callable(installer):
            msg = "datafusion_ext.install_delta_table_factory is unavailable."
            raise TypeError(msg)
        installer(ctx, "DELTATABLE")

    def _apply_delta_session_defaults(self, ctx: SessionContext) -> None:
        """Apply Delta session defaults when enabled.

        Raises
        ------
        RuntimeError
            Raised when the DataFusion extension module is unavailable.
        """
        if not self.enable_delta_session_defaults:
            return
        available = True
        installed = False
        error: str | None = None
        cause: Exception | None = None
        try:
            module = importlib.import_module("datafusion_ext")
        except ImportError as exc:
            available = False
            error = str(exc)
            cause = exc
        else:
            applier = getattr(module, "apply_delta_session_defaults", None)
            if not callable(applier):
                error = "datafusion_ext.apply_delta_session_defaults is unavailable."
            else:
                try:
                    applier(ctx)
                except (RuntimeError, TypeError, ValueError) as exc:
                    error = str(exc)
                    cause = exc
                else:
                    installed = True
        self._record_delta_session_defaults(
            available=available,
            installed=installed,
            error=error,
        )
        if error is not None:
            msg = "Delta session defaults require datafusion_ext."
            raise RuntimeError(msg) from cause

    def _install_udfs(self, ctx: SessionContext) -> None:
        """Install registered UDFs on the session context."""
        if self.enable_udfs:
            snapshot = register_rust_udfs(ctx)
            self._record_udf_snapshot(snapshot)
        self._refresh_udf_catalog(ctx)

    def _refresh_udf_catalog(self, ctx: SessionContext) -> None:
        if not self.enable_information_schema:
            msg = "UdfCatalog requires information_schema to be enabled."
            raise ValueError(msg)
        introspector = self._schema_introspector(ctx)
        if self.udf_catalog_policy == "strict":
            catalog = get_strict_udf_catalog(introspector=introspector)
        else:
            catalog = get_default_udf_catalog(introspector=introspector)
        self._validate_ibis_udf_specs(catalog, introspector=introspector)
        self.udf_catalog_cache[id(ctx)] = catalog

    def _validate_ibis_udf_specs(
        self,
        catalog: UdfCatalog,
        *,
        introspector: SchemaIntrospector,
    ) -> None:
        """Validate Ibis builtin UDFs against the runtime catalog.

        Raises
        ------
        ValueError
            Raised when builtin Ibis UDFs are missing from DataFusion.
        """
        missing: list[str] = []
        from engine.unified_registry import build_unified_function_registry

        unified_registry = build_unified_function_registry(
            datafusion_function_catalog=introspector.function_catalog_snapshot(
                include_parameters=True
            ),
            snapshot=introspector.snapshot,
        )
        for name in sorted(unified_registry.required_builtins):
            try:
                if catalog.is_builtin_from_runtime(name):
                    continue
            except (RuntimeError, TypeError, ValueError):
                pass
            missing.append(name)
        if missing:
            if self.diagnostics_sink is not None:
                self.record_artifact(
                    "datafusion_udf_validation_v1",
                    {
                        "event_time_unix_ms": int(time.time() * 1000),
                        "udf_catalog_policy": self.udf_catalog_policy,
                        "missing_udfs": sorted(missing),
                        "missing_count": len(missing),
                    },
                )
            msg = f"Ibis builtin UDFs missing in DataFusion: {sorted(missing)}."
            raise ValueError(msg)

    def udf_catalog(self, ctx: SessionContext) -> UdfCatalog:
        """Return the cached UDF catalog for a session context.

        Returns
        -------
        UdfCatalog
            Cached UDF catalog for the session.
        """
        cache_key = id(ctx)
        catalog = self.udf_catalog_cache.get(cache_key)
        if catalog is None:
            self._refresh_udf_catalog(ctx)
            catalog = self.udf_catalog_cache[cache_key]
        return catalog

    def reserve_delta_commit(
        self,
        *,
        key: str,
        metadata: Mapping[str, object] | None = None,
        commit_metadata: Mapping[str, str] | None = None,
    ) -> tuple[IdempotentWriteOptions, DataFusionRun]:
        """Reserve the next idempotent commit version for a Delta write.

        Returns
        -------
        tuple[IdempotentWriteOptions, DataFusionRun]
            Idempotent write options and updated run context.
        """
        run = self.delta_commit_runs.get(key)
        if run is None:
            from obs.datafusion_runs import create_run_context

            base_metadata: dict[str, str] = {"key": key}
            if metadata:
                base_metadata.update(
                    {str(item_key): str(item_value) for item_key, item_value in metadata.items()}
                )
            run = create_run_context(
                label="delta_commit",
                sink=self.diagnostics_sink,
                metadata=base_metadata,
            )
            self.delta_commit_runs[key] = run
        elif metadata:
            run.metadata.update(dict(metadata))
        if commit_metadata:
            run.metadata["commit_metadata"] = dict(commit_metadata)
        options, updated = run.next_commit_version()
        if self.diagnostics_sink is not None:
            commit_meta_payload = dict(commit_metadata) if commit_metadata is not None else None
            payload = {
                "event_time_unix_ms": int(time.time() * 1000),
                "key": key,
                "run_id": run.run_id,
                "app_id": options.app_id,
                "version": options.version,
                "commit_sequence": run.commit_sequence,
                "status": "reserved",
                "metadata": dict(run.metadata),
                "commit_metadata": commit_meta_payload,
            }
            self.record_artifact("datafusion_delta_commit_v1", payload)
        return options, updated

    def finalize_delta_commit(
        self,
        *,
        key: str,
        run: DataFusionRun,
        metadata: Mapping[str, object] | None = None,
    ) -> None:
        """Persist commit sequencing state after a successful write."""
        if metadata:
            run.metadata.update(dict(metadata))
        self.delta_commit_runs[key] = run
        if self.diagnostics_sink is None:
            return
        committed_version = run.commit_sequence - 1 if run.commit_sequence > 0 else None
        payload = {
            "event_time_unix_ms": int(time.time() * 1000),
            "key": key,
            "run_id": run.run_id,
            "app_id": run.run_id,
            "version": committed_version,
            "commit_sequence": run.commit_sequence,
            "status": "finalized",
            "metadata": dict(run.metadata),
        }
        self.record_artifact("datafusion_delta_commit_v1", payload)

    def _validate_rule_function_allowlist(self, ctx: SessionContext) -> None:
        """Validate rulepack function demands against information_schema.

        Raises
        ------
        ValueError
            Raised when required rulepack functions are missing or mismatched.
        """
        if not self.enable_information_schema:
            return
        try:
            function_catalog = function_catalog_snapshot_for_profile(
                self,
                ctx,
                include_routines=True,
            )
        except (RuntimeError, TypeError, ValueError):
            function_catalog = None
        required, required_counts, required_signatures = _rulepack_required_functions(
            datafusion_function_catalog=function_catalog
        )
        if not required:
            return
        errors = _rulepack_function_errors(
            ctx,
            required=required,
            required_counts=required_counts,
            required_signatures=required_signatures,
            sql_options=self._sql_options(),
        )
        if errors:
            msg = f"Rulepack function validation failed: {errors}."
            raise ValueError(msg)

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
        introspector = self._schema_introspector(ctx)
        constraint_drift = _constraint_drift_entries(
            introspector,
            names=tuple(sorted(expected_names)),
        )
        relationship_errors = _relationship_constraint_errors(
            ctx,
            sql_options=self._sql_options(),
        )
        if (
            not missing
            and not type_errors
            and not view_errors
            and not constraint_drift
            and relationship_errors is None
        ):
            return
        payload: dict[str, object] = {
            "event_time_unix_ms": int(time.time() * 1000),
            "missing": list(missing),
            "type_errors": type_errors,
            "view_errors": dict(view_errors) if view_errors else None,
            "constraint_drift": constraint_drift or None,
            "relationship_constraint_errors": dict(relationship_errors)
            if relationship_errors
            else None,
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
        self.record_artifact("datafusion_schema_registry_validation_v1", payload)

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
        self.record_artifact("datafusion_catalog_autoload_v1", payload)

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
        self.record_artifact("datafusion_ast_feature_gates_v1", payload)

    def _record_ast_span_metadata(self, ctx: SessionContext) -> None:
        if self.diagnostics_sink is None:
            return
        payload: dict[str, object] = {
            "event_time_unix_ms": int(time.time() * 1000),
            "dataset": "ast_files_v1",
        }
        try:
            table = _sql_with_options(
                ctx,
                "SELECT * FROM ast_span_metadata",
                sql_options=self._sql_options(),
            ).to_arrow_table()
            rows = table.to_pylist()
            schema = schema_for("ast_files_v1")
            payload["schema_fingerprint"] = schema_fingerprint(schema)
            payload["metadata"] = rows[0] if rows else None
        except (KeyError, RuntimeError, TypeError, ValueError) as exc:
            payload["error"] = str(exc)
        version = _datafusion_version(ctx)
        if version is not None:
            payload["datafusion_version"] = version
        self.record_artifact("datafusion_ast_span_metadata_v1", payload)

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
            from ibis_engine.registry import DatasetLocation

            return DatasetLocation(
                path=self.ast_delta_location,
                format="delta",
                delta_version=self.ast_delta_version,
                delta_timestamp=self.ast_delta_timestamp,
                delta_constraints=self.ast_delta_constraints,
                delta_scan=delta_scan,
            )
        if self.ast_external_location:
            from ibis_engine.registry import DatasetLocation

            if self.ast_external_format == "delta":
                msg = "AST external format must not be 'delta'."
                raise ValueError(msg)
            expected_schema = schema_for("ast_files_v1")
            scan = DataFusionScanOptions(
                partition_cols=self.ast_external_partition_cols,
                file_sort_order=self.ast_external_ordering,
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
        from datafusion_engine.io_adapter import DataFusionIOAdapter

        adapter = DataFusionIOAdapter(ctx=ctx, profile=self)
        with contextlib.suppress(KeyError, RuntimeError, TypeError, ValueError):
            adapter.deregister_table("ast_files_v1")
        from datafusion_engine.registry_bridge import register_dataset_df

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
            from ibis_engine.registry import DatasetLocation

            return DatasetLocation(
                path=self.bytecode_delta_location,
                format="delta",
                delta_version=self.bytecode_delta_version,
                delta_timestamp=self.bytecode_delta_timestamp,
                delta_constraints=self.bytecode_delta_constraints,
                delta_scan=delta_scan,
            )
        if self.bytecode_external_location:
            from ibis_engine.registry import DatasetLocation

            if self.bytecode_external_format == "delta":
                msg = "Bytecode external format must not be 'delta'."
                raise ValueError(msg)
            expected_schema = schema_for("bytecode_files_v1")
            scan = DataFusionScanOptions(
                partition_cols=self.bytecode_external_partition_cols,
                file_sort_order=self.bytecode_external_ordering,
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
        from datafusion_engine.io_adapter import DataFusionIOAdapter

        adapter = DataFusionIOAdapter(ctx=ctx, profile=self)
        with contextlib.suppress(KeyError, RuntimeError, TypeError, ValueError):
            adapter.deregister_table("bytecode_files_v1")
        from datafusion_engine.registry_bridge import register_dataset_df

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

    def bytecode_dataset_location(self) -> DatasetLocation | None:
        """Return the configured bytecode dataset location, when available.

        Returns
        -------
        DatasetLocation | None
            Bytecode dataset location when configured.
        """
        return self._bytecode_dataset_location()

    def extract_dataset_location(self, name: str) -> DatasetLocation | None:
        """Return a configured extract dataset location for the dataset name.

        Returns
        -------
        DatasetLocation | None
            Extract dataset location when configured.
        """
        if name in self.extract_dataset_locations:
            return self.extract_dataset_locations[name]
        if name == "ast_files_v1":
            return self._ast_dataset_location()
        if name == "bytecode_files_v1":
            return self._bytecode_dataset_location()
        return self.scip_dataset_locations.get(name)

    def dataset_location(self, name: str) -> DatasetLocation | None:
        """Return a configured dataset location for the dataset name.

        Returns
        -------
        DatasetLocation | None
            Dataset location when configured.
        """
        location = self.extract_dataset_location(name)
        if location is not None:
            return location
        for view_name, schema_name in SCIP_VIEW_SCHEMA_MAP.items():
            if schema_name != name:
                continue
            mapped = self.scip_dataset_locations.get(view_name)
            if mapped is not None:
                return mapped
        for catalog in self.registry_catalogs.values():
            if catalog.has(name):
                return catalog.get(name)
        return None

    def dataset_location_or_raise(self, name: str) -> DatasetLocation:
        """Return a configured dataset location for the dataset name.

        Returns
        -------
        DatasetLocation
            Dataset location for the dataset.

        Raises
        ------
        KeyError
            Raised when the dataset location is not configured.
        """
        location = self.dataset_location(name)
        if location is None:
            msg = f"No dataset location configured for {name!r}."
            raise KeyError(msg)
        return location

    def _register_scip_datasets(self, ctx: SessionContext) -> None:
        if not self.scip_dataset_locations:
            return
        from datafusion_engine.io_adapter import DataFusionIOAdapter

        adapter = DataFusionIOAdapter(ctx=ctx, profile=self)
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
            with contextlib.suppress(KeyError, RuntimeError, TypeError, ValueError):
                adapter.deregister_table(schema_name)
            from datafusion_engine.registry_bridge import (
                register_dataset_df,
            )

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
            "file_sort_order": (
                [list(key) for key in scan.file_sort_order] if scan is not None else None
            ),
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
        self.record_artifact("datafusion_ast_dataset_v1", payload)

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
            "file_sort_order": (
                [list(key) for key in scan.file_sort_order] if scan is not None else None
            ),
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
        self.record_artifact("datafusion_bytecode_dataset_v1", payload)

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
            "file_sort_order": (
                [list(key) for key in scan.file_sort_order] if scan is not None else None
            ),
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
        self.record_artifact("datafusion_scip_datasets_v1", payload)

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
            self._schema_introspector(ctx).table_column_names("ast_files_v1")
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
            self._schema_introspector(ctx).table_column_names("bytecode_files_v1")
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
            table = _sql_with_options(
                ctx,
                "SELECT * FROM cst_schema_diagnostics",
                sql_options=self._sql_options(),
            ).to_arrow_table()
            rows = table.to_pylist()
            schema = schema_for("libcst_files_v1")
            payload["schema_fingerprint"] = schema_fingerprint(schema)
            default_entries = _default_value_entries(schema)
            payload["default_values"] = default_entries or None
            payload["diagnostics"] = rows[0] if rows else None
            introspector = self._schema_introspector(ctx)
            payload["table_definition"] = introspector.table_definition("libcst_files_v1")
            payload["table_constraints"] = (
                list(introspector.table_constraints("libcst_files_v1")) or None
            )
        except (KeyError, RuntimeError, TypeError, ValueError) as exc:
            payload["error"] = str(exc)
        self.record_artifact("datafusion_cst_schema_diagnostics_v1", payload)

    def _record_tree_sitter_stats(self, ctx: SessionContext) -> None:
        if self.diagnostics_sink is None:
            return
        payload: dict[str, object] = {
            "event_time_unix_ms": int(time.time() * 1000),
            "dataset": "tree_sitter_files_v1",
        }
        try:
            table = _sql_with_options(
                ctx,
                "SELECT * FROM ts_stats",
                sql_options=self._sql_options(),
            ).to_arrow_table()
            rows = table.to_pylist()
            schema = schema_for("tree_sitter_files_v1")
            payload["schema_fingerprint"] = schema_fingerprint(schema)
            payload["stats"] = rows[0] if rows else None
            introspector = self._schema_introspector(ctx)
            payload["table_definition"] = introspector.table_definition("tree_sitter_files_v1")
            payload["table_constraints"] = (
                list(introspector.table_constraints("tree_sitter_files_v1")) or None
            )
        except (KeyError, RuntimeError, TypeError, ValueError) as exc:
            payload["error"] = str(exc)
        self.record_artifact("datafusion_tree_sitter_stats_v1", payload)

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
        introspector = self._schema_introspector(ctx)
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
        self.record_artifact(
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
                    _sql_with_options(
                        ctx,
                        "SELECT COUNT(*) AS row_count, "
                        "SUM(CASE WHEN mismatch THEN 1 ELSE 0 END) AS mismatch_count "
                        f"FROM {name}",
                        sql_options=self._sql_options(),
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
                        _sql_with_options(
                            ctx,
                            f"SELECT * FROM {name} WHERE mismatch LIMIT 25",
                            sql_options=self._sql_options(),
                        )
                        .to_arrow_table()
                        .to_pylist()
                    )
                    entry["sample"] = sample_rows or None
                views.append(entry)
            except (KeyError, RuntimeError, TypeError, ValueError) as exc:
                errors[name] = str(exc)
        if errors:
            payload["errors"] = errors
        self.record_artifact("datafusion_tree_sitter_cross_checks_v1", payload)
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
        self.record_artifact("datafusion_cst_view_plans_v1", payload)

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
        self.record_artifact("datafusion_cst_dfschema_v1", payload)

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
            table = _sql_with_options(
                ctx,
                "SELECT * FROM py_bc_metadata",
                sql_options=self._sql_options(),
            ).to_arrow_table()
            rows = table.to_pylist()
            schema = schema_for("bytecode_files_v1")
            payload["schema_fingerprint"] = schema_fingerprint(schema)
            payload["metadata"] = rows[0] if rows else None
            introspector = self._schema_introspector(ctx)
            payload["table_definition"] = introspector.table_definition("bytecode_files_v1")
            payload["table_constraints"] = (
                list(introspector.table_constraints("bytecode_files_v1")) or None
            )
        except (KeyError, RuntimeError, TypeError, ValueError) as exc:
            payload["error"] = str(exc)
        self.record_artifact("datafusion_bytecode_metadata_v1", payload)

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
                    "catalogs": catalogs_snapshot(introspector),
                    "schemata": introspector.schemata_snapshot(),
                    "tables": introspector.tables_snapshot(),
                    "columns": introspector.columns_snapshot(),
                    "constraints": constraint_rows(
                        ctx,
                        sql_options=self._sql_options(),
                    ),
                    "routines": introspector.routines_snapshot(),
                    "parameters": introspector.parameters_snapshot(),
                    "settings": introspector.settings_snapshot(),
                    "functions": function_catalog_snapshot_for_profile(
                        self,
                        ctx,
                        include_routines=self.enable_information_schema,
                    ),
                }
            )
            version = _datafusion_version(ctx)
            if version is not None:
                payload["datafusion_version"] = version
        except (RuntimeError, TypeError, ValueError) as exc:
            payload["error"] = str(exc)
        self.record_artifact(
            "datafusion_schema_introspection_v1",
            payload,
        )

    def _register_schema_views(
        self,
        ctx: SessionContext,
        *,
        fragment_views: Sequence[ViewSpec],
    ) -> set[str]:
        from datafusion_engine.schema_registry import (
            symtable_binding_resolution_view_specs,
            symtable_derived_view_specs,
        )

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
        symtable_views = symtable_derived_view_specs(ctx)
        if symtable_views:
            register_view_specs(
                ctx,
                views=symtable_views,
                runtime_profile=self,
                validate=True,
            )
        symtable_resolution_views = symtable_binding_resolution_view_specs(ctx)
        if symtable_resolution_views:
            register_view_specs(
                ctx,
                views=symtable_resolution_views,
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
            from datafusion_engine.io_adapter import DataFusionIOAdapter

            adapter = DataFusionIOAdapter(ctx=ctx, profile=self)
            with contextlib.suppress(KeyError, RuntimeError, TypeError, ValueError):
                adapter.deregister_table("ast_files_v1")
            self._validate_ast_catalog_autoload(ctx)
        if not bytecode_registration and (
            self.bytecode_catalog_location is not None or self.bytecode_catalog_format is not None
        ):
            from datafusion_engine.io_adapter import DataFusionIOAdapter

            adapter = DataFusionIOAdapter(ctx=ctx, profile=self)
            with contextlib.suppress(KeyError, RuntimeError, TypeError, ValueError):
                adapter.deregister_table("bytecode_files_v1")
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
            ("engine_functions", validate_required_engine_functions),
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
        from datafusion_engine.view_registry import registry_view_specs

        fragment_views = registry_view_specs(ctx, exclude=ast_optional_disabled)
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
        if not self.enable_information_schema:
            statements = [
                statement
                for statement in statements
                if statement.name not in INFO_SCHEMA_STATEMENT_NAMES
            ]
        if self.enable_schema_registry:
            statements.extend(CST_DIAGNOSTIC_STATEMENTS)
        seen: set[str] = set()
        for statement in statements:
            if statement.name in seen:
                continue
            seen.add(statement.name)
            _sql_with_options(
                ctx,
                _prepare_statement_sql(statement),
                sql_options=self._statement_sql_options(),
            )
            self._record_prepared_statement(statement)

    def _record_prepared_statement(self, statement: PreparedStatementSpec) -> None:
        if self.diagnostics_sink is None:
            return
        self.record_artifact(
            "datafusion_prepared_statements_v1",
            {
                "name": statement.name,
                "sql": statement.sql,
                "param_types": list(statement.param_types),
            },
        )

    @staticmethod
    def _install_delta_plan_codecs_extension(
        ctx: SessionContext,
    ) -> tuple[bool, bool]:
        try:
            module = importlib.import_module("datafusion_ext")
        except ImportError:
            return False, False
        installer = getattr(module, "install_delta_plan_codecs", None)
        if not callable(installer):
            return False, False
        try:
            result = installer(ctx)
        except (RuntimeError, TypeError, ValueError):
            return True, False
        return True, bool(result) if result is not None else True

    def _install_delta_plan_codecs_context(self, ctx: SessionContext) -> tuple[bool, bool]:
        register = getattr(ctx, "register_extension_codecs", None)
        if not callable(register):
            return False, False
        try:
            register(self.delta_plan_codec_physical, self.delta_plan_codec_logical)
        except TypeError:
            try:
                register(self.delta_plan_codec_logical, self.delta_plan_codec_physical)
            except TypeError:
                return True, False
        return True, True

    def ensure_delta_plan_codecs(self, ctx: SessionContext) -> bool:
        """Install Delta plan codecs when enabled.

        Returns
        -------
        bool
            True when codecs were installed, otherwise False.
        """
        if not self.enable_delta_plan_codecs:
            return False
        available, installed = self._install_delta_plan_codecs_extension(ctx)
        if not available:
            available, installed = self._install_delta_plan_codecs_context(ctx)
        self._record_delta_plan_codecs(
            available=available,
            installed=installed,
        )
        return installed

    def _record_udf_snapshot(self, snapshot: Mapping[str, object]) -> None:
        if self.diagnostics_sink is None:
            return
        self.record_artifact(
            "datafusion_udf_registry_v1",
            dict(snapshot),
        )

    def _record_delta_plan_codecs(self, *, available: bool, installed: bool) -> None:
        if self.diagnostics_sink is None:
            return
        self.record_artifact(
            "datafusion_delta_plan_codecs_v1",
            {
                "enabled": self.enable_delta_plan_codecs,
                "available": available,
                "installed": installed,
                "physical_codec": self.delta_plan_codec_physical,
                "logical_codec": self.delta_plan_codec_logical,
            },
        )

    def _record_delta_session_defaults(
        self,
        *,
        available: bool,
        installed: bool,
        error: str | None,
    ) -> None:
        if self.diagnostics_sink is None:
            return
        self.record_artifact(
            "datafusion_delta_session_defaults_v1",
            {
                "enabled": self.enable_delta_session_defaults,
                "available": available,
                "installed": installed,
                "error": error,
            },
        )

    def _record_extension_parity_validation(self) -> None:
        if self.diagnostics_sink is None:
            return
        payload = dict(self.validate_named_args_extension_parity())
        payload["event_time_unix_ms"] = int(time.time() * 1000)
        payload["profile_name"] = self.config_policy_name
        payload["settings_hash"] = self.settings_hash()
        self.record_artifact(
            "datafusion_extension_parity_v1",
            payload,
        )

    def _record_cache_diagnostics(self, ctx: SessionContext) -> None:
        """Record cache configuration and state diagnostics.

        Parameters
        ----------
        ctx
            DataFusion session context to introspect.
        """
        if self.diagnostics_sink is None:
            return
        cache_diag = capture_cache_diagnostics(ctx)
        self.record_artifact(
            "datafusion_cache_config_v1",
            cache_diag.get("config", {}),
        )
        cache_snapshots = cache_diag.get("cache_snapshots", [])
        if cache_snapshots:
            self.record_events(
                "datafusion_cache_state_v1",
                cache_snapshots,
            )
        diskcache_profile = self.diskcache_profile
        if diskcache_profile is None:
            return
        diskcache_events: list[dict[str, object]] = []
        for kind in ("plan", "extract", "schema", "repo_scan", "runtime", "coordination"):
            cache = self._diskcache(cast("DiskCacheKind", kind))
            if cache is None:
                continue
            settings = diskcache_profile.settings_for(cast("DiskCacheKind", kind))
            payload = diskcache_stats_snapshot(cache)
            payload.update(
                {
                    "kind": kind,
                    "profile_key": self.context_cache_key(),
                    "size_limit_bytes": settings.size_limit_bytes,
                    "eviction_policy": settings.eviction_policy,
                    "cull_limit": settings.cull_limit,
                    "shards": settings.shards,
                    "statistics": settings.statistics,
                    "tag_index": settings.tag_index,
                    "disk_min_file_size": settings.disk_min_file_size,
                    "sqlite_journal_mode": settings.sqlite_journal_mode,
                    "sqlite_mmap_size": settings.sqlite_mmap_size,
                    "sqlite_synchronous": settings.sqlite_synchronous,
                }
            )
            diskcache_events.append(payload)
        if diskcache_events:
            self.record_events(
                "diskcache_stats_v1",
                diskcache_events,
            )

    def _install_cache_tables(self, ctx: SessionContext) -> None:
        if not (self.enable_cache_manager or self.cache_enabled):
            return
        try:
            register_cache_introspection_functions(ctx)
        except ImportError as exc:
            msg = "Cache table functions require datafusion_ext."
            raise RuntimeError(msg) from exc
        except (RuntimeError, TypeError, ValueError) as exc:
            msg = f"Cache table function registration failed: {exc}"
            raise RuntimeError(msg) from exc

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
        from datafusion_engine.io_adapter import DataFusionIOAdapter

        adapter = DataFusionIOAdapter(ctx=ctx, profile=self)
        adapter.register_object_store(scheme="file://", store=store, host=None)

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
        self.record_artifact(
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
        self.record_artifact(
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

    def _install_tracing(self, ctx: SessionContext) -> None:
        """Enable tracing when configured.

        Raises
        ------
        ValueError
            Raised when tracing is enabled without a hook.
        """
        if not self.enable_tracing:
            return
        if self.tracing_hook is None:
            try:
                module = importlib.import_module("datafusion_ext")
            except ImportError as exc:
                msg = "Tracing enabled but datafusion_ext is unavailable."
                raise ValueError(msg) from exc
            install = getattr(module, "install_tracing", None)
            if not callable(install):
                msg = "Tracing enabled but datafusion_ext.install_tracing is unavailable."
                raise ValueError(msg)
            install(ctx)
            return
        self.tracing_hook(ctx)

    def _resolve_compile_hooks(
        self,
        resolved: DataFusionCompileOptions,
        *,
        capture_explain: bool,
        explain_analyze: bool,
        capture_plan_artifacts: bool,
        capture_semantic_diff: bool,
    ) -> _ResolvedCompileHooks:
        hooks: dict[str, object | None] = {
            "explain": resolved.explain_hook,
            "plan_artifacts": resolved.plan_artifacts_hook,
            "semantic_diff": resolved.semantic_diff_hook,
            "sql_ingest": resolved.sql_ingest_hook,
            "cache_event": resolved.cache_event_hook,
            "substrait_fallback": resolved.substrait_fallback_hook,
        }
        if hooks["explain"] is None and capture_explain and self.explain_collector is not None:
            hooks["explain"] = self.explain_collector.hook
        if (
            hooks["plan_artifacts"] is None
            and capture_plan_artifacts
            and self.plan_collector is not None
        ):
            hooks["plan_artifacts"] = self.plan_collector.hook
        if self.diagnostics_sink is not None:
            if capture_explain or hooks["explain"] is not None:
                hooks["explain"] = _chain_explain_hooks(
                    cast("ExplainHook", hooks["explain"]),
                    diagnostics_explain_hook(
                        self.diagnostics_sink,
                        explain_analyze=explain_analyze,
                    ),
                )
            if capture_plan_artifacts or hooks["plan_artifacts"] is not None:
                hooks["plan_artifacts"] = _chain_plan_artifacts_hooks(
                    cast("PlanArtifactsHook", hooks["plan_artifacts"]),
                    diagnostics_plan_artifacts_hook(self.diagnostics_sink),
                )
            if capture_semantic_diff or hooks["semantic_diff"] is not None:
                hooks["semantic_diff"] = _chain_plan_artifacts_hooks(
                    cast("PlanArtifactsHook", hooks["semantic_diff"]),
                    diagnostics_semantic_diff_hook(self.diagnostics_sink),
                )
            hooks["sql_ingest"] = _chain_sql_ingest_hooks(
                cast("SqlIngestHook", hooks["sql_ingest"]),
                diagnostics_sql_ingest_hook(self.diagnostics_sink),
            )
            hooks["cache_event"] = _chain_cache_hooks(
                cast("CacheEventHook", hooks["cache_event"]),
                diagnostics_cache_hook(self.diagnostics_sink),
            )
            hooks["substrait_fallback"] = _chain_substrait_fallback_hooks(
                cast("SubstraitFallbackHook", hooks["substrait_fallback"]),
                diagnostics_substrait_fallback_hook(self.diagnostics_sink),
            )
        return _ResolvedCompileHooks(
            explain_hook=cast("ExplainHook | None", hooks["explain"]),
            plan_artifacts_hook=cast("PlanArtifactsHook | None", hooks["plan_artifacts"]),
            semantic_diff_hook=cast("SemanticDiffHook | None", hooks["semantic_diff"]),
            sql_ingest_hook=cast("SqlIngestHook | None", hooks["sql_ingest"]),
            cache_event_hook=cast("CacheEventHook | None", hooks["cache_event"]),
            substrait_fallback_hook=cast(
                "SubstraitFallbackHook | None", hooks["substrait_fallback"]
            ),
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
        resolved = options or DataFusionCompileOptions(
            cache=None,
            cache_max_columns=None,
            enforce_preflight=self.enforce_preflight,
        )
        resolved_params = resolved.params if resolved.params is not None else params
        prepared = _resolve_prepared_statement_options(resolved, resolved_params=resolved_params)
        capture_explain = resolved.capture_explain or self.capture_explain
        explain_analyze = resolved.explain_analyze or self.explain_analyze
        substrait_validation = resolved.substrait_validation or self.substrait_validation
        capture_plan_artifacts = (
            resolved.capture_plan_artifacts
            or self.capture_plan_artifacts
            or capture_explain
            or substrait_validation
        )
        capture_semantic_diff = (
            resolved.capture_semantic_diff or self.capture_semantic_diff
        )
        resolution = _CompileOptionResolution(
            cache=resolved.cache if resolved.cache is not None else self.cache_enabled,
            cache_max_columns=(
                resolved.cache_max_columns
                if resolved.cache_max_columns is not None
                else self.cache_max_columns
            ),
            params=resolved_params,
            param_allowlist=(
                resolved.param_identifier_allowlist
                if resolved.param_identifier_allowlist is not None
                else tuple(self.param_identifier_allowlist) or None
            ),
            prepared_param_types=prepared[0],
            prepared_statements=prepared[1],
            dynamic_projection=prepared[2],
            capture_explain=capture_explain,
            explain_analyze=explain_analyze,
            substrait_validation=substrait_validation,
            capture_plan_artifacts=capture_plan_artifacts,
            capture_semantic_diff=capture_semantic_diff,
            sql_policy=self._resolve_sql_policy(resolved),
            sql_policy_name=(
                resolved.sql_policy_name
                if resolved.sql_policy_name is not None
                else self.sql_policy_name
            ),
        )
        hooks = self._resolve_compile_hooks(
            resolved,
            capture_explain=resolution.capture_explain,
            explain_analyze=resolution.explain_analyze,
            capture_plan_artifacts=resolution.capture_plan_artifacts,
            capture_semantic_diff=resolution.capture_semantic_diff,
        )
        unchanged = (
            resolution.cache == resolved.cache,
            resolution.cache_max_columns == resolved.cache_max_columns,
            resolution.params == resolved.params,
            self.enforce_preflight == resolved.enforce_preflight,
            resolution.capture_explain == resolved.capture_explain,
            resolution.explain_analyze == resolved.explain_analyze,
            hooks.explain_hook == resolved.explain_hook,
            resolution.substrait_validation == resolved.substrait_validation,
            resolution.capture_plan_artifacts == resolved.capture_plan_artifacts,
            hooks.plan_artifacts_hook == resolved.plan_artifacts_hook,
            resolution.capture_semantic_diff == resolved.capture_semantic_diff,
            hooks.semantic_diff_hook == resolved.semantic_diff_hook,
            hooks.sql_ingest_hook == resolved.sql_ingest_hook,
            hooks.cache_event_hook == resolved.cache_event_hook,
            hooks.substrait_fallback_hook == resolved.substrait_fallback_hook,
            resolution.sql_policy == resolved.sql_policy,
            resolution.sql_policy_name == resolved.sql_policy_name,
            resolution.param_allowlist == resolved.param_identifier_allowlist,
            resolution.prepared_param_types == resolved.prepared_param_types,
            resolution.prepared_statements == resolved.prepared_statements,
            resolution.dynamic_projection == resolved.dynamic_projection,
        )
        if all(unchanged) and execution_policy is None and execution_label is None:
            return resolved
        updated = replace(
            resolved,
            cache=resolution.cache,
            cache_max_columns=resolution.cache_max_columns,
            params=resolution.params,
            param_identifier_allowlist=resolution.param_allowlist,
            enforce_preflight=resolved.enforce_preflight,
            capture_explain=resolution.capture_explain,
            explain_analyze=resolution.explain_analyze,
            explain_hook=hooks.explain_hook,
            substrait_validation=resolution.substrait_validation,
            capture_plan_artifacts=resolution.capture_plan_artifacts,
            plan_artifacts_hook=hooks.plan_artifacts_hook,
            capture_semantic_diff=resolution.capture_semantic_diff,
            semantic_diff_hook=hooks.semantic_diff_hook,
            sql_ingest_hook=hooks.sql_ingest_hook,
            cache_event_hook=hooks.cache_event_hook,
            substrait_fallback_hook=hooks.substrait_fallback_hook,
            sql_policy=resolution.sql_policy,
            sql_policy_name=resolution.sql_policy_name,
            prepared_param_types=resolution.prepared_param_types,
        )
        if execution_label is not None:
            updated = apply_execution_label(
                updated,
                execution_label=execution_label,
                explain_sink=self.labeled_explains,
            )
        if execution_policy is None:
            return updated
        return apply_execution_policy(
            updated,
            execution_policy=execution_policy,
        )

    def _resolved_sql_policy(self) -> DataFusionSqlPolicy:
        """Return the resolved SQL policy for this runtime profile.

        Returns
        -------
        DataFusionSqlPolicy
            SQL policy derived from the profile configuration.
        """
        if self.sql_policy is not None:
            return self.sql_policy
        if self.sql_policy_name is None:
            return DataFusionSqlPolicy()
        return resolve_sql_policy(self.sql_policy_name)

    def _sql_options(self) -> SQLOptions:
        """Return SQLOptions derived from the resolved SQL policy.

        Returns
        -------
        datafusion.SQLOptions
            SQL options derived from the profile policy.
        """
        return execution_policy_for_profile(self).to_sql_options()

    def sql_options(self) -> SQLOptions:
        """Return SQLOptions derived from the resolved SQL policy.

        Returns
        -------
        datafusion.SQLOptions
            SQL options derived from the profile policy.
        """
        return self._sql_options()

    def _statement_sql_options(self) -> SQLOptions:
        """Return SQLOptions that allow statement execution.

        Returns
        -------
        datafusion.SQLOptions
            SQL options with statement execution enabled.
        """
        return execution_policy_for_profile(self, allow_statements=True).to_sql_options()

    def _diskcache(self, kind: DiskCacheKind) -> Cache | FanoutCache | None:
        """Return a DiskCache instance for the requested kind.

        Returns
        -------
        diskcache.Cache | diskcache.FanoutCache | None
            Cache instance when DiskCache is configured.
        """
        profile = self.diskcache_profile
        if profile is None:
            return None
        return cache_for_kind(profile, kind)

    def _diskcache_ttl_seconds(self, kind: DiskCacheKind) -> float | None:
        """Return the TTL in seconds for a DiskCache kind when configured.

        Returns
        -------
        float | None
            TTL in seconds or None when unset.
        """
        profile = self.diskcache_profile
        if profile is None:
            return None
        return profile.ttl_for(kind)

    def _record_view_definition(self, *, name: str, sql: str | None) -> None:
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

    def _schema_introspector(self, ctx: SessionContext) -> SchemaIntrospector:
        """Return a schema introspector for the session.

        Returns
        -------
        SchemaIntrospector
            Introspector bound to the provided SessionContext.
        """
        return SchemaIntrospector(
            ctx,
            sql_options=self._sql_options(),
            cache=self._diskcache("schema"),
            cache_prefix=self.context_cache_key(),
            cache_ttl=self._diskcache_ttl_seconds("schema"),
        )

    def _settings_snapshot(self, ctx: SessionContext) -> pa.Table:
        """Return a snapshot of DataFusion settings when information_schema is enabled.

        Returns
        -------
        pyarrow.Table
            Table of settings from information_schema.df_settings.
        """
        cache = introspection_cache_for_ctx(ctx, sql_options=self._sql_options())
        return cache.snapshot.settings

    def _catalog_snapshot(self, ctx: SessionContext) -> pa.Table:
        """Return a snapshot of DataFusion catalog tables when available.

        Returns
        -------
        pyarrow.Table
            Table inventory from information_schema.tables.
        """
        cache = introspection_cache_for_ctx(ctx, sql_options=self._sql_options())
        return cache.snapshot.tables

    def _function_catalog_snapshot(
        self,
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
            Sorted function catalog entries from ``information_schema``.
        """
        return self._schema_introspector(ctx).function_catalog_snapshot(
            include_parameters=include_routines,
        )

    def _resolved_config_policy(self) -> DataFusionConfigPolicy | None:
        return _resolved_config_policy_for_profile(self)

    def _resolved_schema_hardening(self) -> SchemaHardeningProfile | None:
        return _resolved_schema_hardening_for_profile(self)

    def _telemetry_payload_row(self) -> dict[str, object]:
        return _build_telemetry_payload_row(self)

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


def collect_datafusion_metrics(
    profile: DataFusionRuntimeProfile,
) -> Mapping[str, object] | None:
    """Return optional DataFusion metrics payload.

    Returns
    -------
    Mapping[str, object] | None
        Metrics payload when enabled and available.
    """
    if not profile.enable_metrics or profile.metrics_collector is None:
        return None
    return profile.metrics_collector()


def schema_introspector_for_profile(
    profile: DataFusionRuntimeProfile,
    ctx: SessionContext,
) -> SchemaIntrospector:
    """Return a schema introspector for a runtime profile.

    Returns
    -------
    SchemaIntrospector
        Introspector configured from the profile.
    """
    cache_profile = profile.diskcache_profile
    cache = cache_for_kind(cache_profile, "schema") if cache_profile is not None else None
    cache_ttl = cache_profile.ttl_for("schema") if cache_profile is not None else None
    return SchemaIntrospector(
        ctx,
        sql_options=profile.sql_options(),
        cache=cache,
        cache_prefix=profile.context_cache_key(),
        cache_ttl=cache_ttl,
    )


def run_diskcache_maintenance(
    profile: DataFusionRuntimeProfile,
    *,
    kinds: tuple[DiskCacheKind, ...] | None = None,
    include_check: bool = False,
    record: bool = True,
) -> list[dict[str, object]]:
    """Run DiskCache maintenance for a runtime profile.

    Returns
    -------
    list[dict[str, object]]
        Maintenance payloads for each cache kind.
    """
    cache_profile = profile.diskcache_profile
    if cache_profile is None:
        return []
    results = run_profile_maintenance(
        cache_profile,
        kinds=kinds,
        include_check=include_check,
    )
    payloads: list[dict[str, object]] = [
        {
            "kind": result.kind,
            "expired": result.expired,
            "culled": result.culled,
            "check_errors": result.check_errors,
        }
        for result in results
    ]
    if record and payloads:
        record_events(profile, "diskcache_maintenance_v1", payloads)
    return payloads


def evict_diskcache_entries(
    profile: DataFusionRuntimeProfile,
    *,
    kind: DiskCacheKind,
    tag: str,
) -> int:
    """Evict DiskCache entries for a runtime profile.

    Returns
    -------
    int
        Count of evicted entries.
    """
    cache_profile = profile.diskcache_profile
    if cache_profile is None:
        return 0
    return evict_cache_tag(cache_profile, kind=kind, tag=tag)


def collect_datafusion_traces(
    profile: DataFusionRuntimeProfile,
) -> Mapping[str, object] | None:
    """Return optional DataFusion tracing payload.

    Returns
    -------
    Mapping[str, object] | None
        Tracing payload when enabled and available.
    """
    if not profile.enable_tracing or profile.tracing_collector is None:
        return None
    return profile.tracing_collector()


def _rulepack_parameter_counts(rows: Sequence[Mapping[str, object]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        name: str | None = None
        for key in ("specific_name", "routine_name", "function_name", "name"):
            value = row.get(key)
            if isinstance(value, str):
                name = value
                break
        if name is None:
            continue
        normalized = name.lower()
        counts[normalized] = counts.get(normalized, 0) + 1
    return counts


def _rulepack_parameter_signatures(
    rows: Sequence[Mapping[str, object]],
) -> dict[str, set[tuple[str, ...]]]:
    signatures: dict[str, dict[str, list[tuple[int, str]]]] = {}
    for row in rows:
        name = _rulepack_row_value(row, ("routine_name", "specific_name", "function_name", "name"))
        if name is None:
            continue
        data_type = _rulepack_row_value(row, ("data_type",))
        if data_type is None:
            continue
        ordinal = row.get("ordinal_position")
        if (isinstance(ordinal, (int, float)) and not isinstance(ordinal, bool)) or (
            isinstance(ordinal, str) and ordinal.isdigit()
        ):
            position = int(ordinal)
        else:
            continue
        specific = _rulepack_row_value(row, ("specific_name",)) or name
        routine_key = name.lower()
        specific_key = specific.lower()
        signatures.setdefault(routine_key, {}).setdefault(specific_key, []).append(
            (position, data_type.lower())
        )
    resolved: dict[str, set[tuple[str, ...]]] = {}
    for routine_name, specifics in signatures.items():
        for values in specifics.values():
            ordered = tuple(dtype for _, dtype in sorted(values, key=lambda item: item[0]))
            resolved.setdefault(routine_name, set()).add(ordered)
    return resolved


def _rulepack_row_value(row: Mapping[str, object], keys: Sequence[str]) -> str | None:
    for key in keys:
        value = row.get(key)
        if isinstance(value, str):
            return value
    return None


def _rulepack_signature_errors(
    required: Mapping[str, int],
    counts: Mapping[str, int],
) -> dict[str, list[str]]:
    missing: list[str] = []
    mismatched: list[str] = []
    for name, min_args in required.items():
        count = counts.get(name.lower())
        if count is None:
            missing.append(name)
            continue
        if count < min_args:
            mismatched.append(f"{name} ({count} < {min_args})")
    details: dict[str, list[str]] = {}
    if missing:
        details["missing"] = missing
    if mismatched:
        details["mismatched"] = mismatched
    return details


def _rulepack_signature_type_errors(
    required: Mapping[str, set[tuple[str, ...]]],
    parameters: Sequence[Mapping[str, object]],
) -> dict[str, list[str]]:
    if not required:
        return {}
    available = _rulepack_parameter_signatures(parameters)
    missing: list[str] = []
    mismatched: list[str] = []
    for name, expected_signatures in required.items():
        actual = available.get(name.lower())
        if not actual:
            missing.append(name)
            continue
        for expected in expected_signatures:
            if expected not in actual:
                sig = ", ".join(expected)
                mismatched.append(f"{name} ({sig})")
    details: dict[str, list[str]] = {}
    if missing:
        details["missing_types"] = missing
    if mismatched:
        details["type_mismatch"] = mismatched
    return details


def _merge_signature_errors(
    counts: Mapping[str, Sequence[str]],
    types: Mapping[str, Sequence[str]],
) -> dict[str, list[str]] | None:
    merged: dict[str, list[str]] = {}
    for errors in (counts, types):
        for key, values in errors.items():
            if values:
                merged.setdefault(key, []).extend(values)
    if not merged:
        return None
    return merged


def _rulepack_required_functions(
    *,
    datafusion_function_catalog: Sequence[Mapping[str, object]] | None = None,
) -> tuple[
    dict[str, set[str]],
    dict[str, int],
    dict[str, set[tuple[str, ...]]],
]:
    _ = datafusion_function_catalog
    return {}, {}, {}


def _rulepack_signature_for_spec(spec: object) -> tuple[str, ...] | None:
    input_types = getattr(spec, "input_types", None)
    if not isinstance(input_types, tuple):
        return None
    try:
        return tuple(_datafusion_type_name(dtype).lower() for dtype in input_types)
    except (RuntimeError, TypeError, ValueError):
        return None


def _rulepack_function_errors(
    ctx: SessionContext,
    *,
    required: Mapping[str, set[str]],
    required_counts: Mapping[str, int],
    required_signatures: Mapping[str, set[tuple[str, ...]]],
    sql_options: SQLOptions | None = None,
) -> dict[str, str]:
    errors: dict[str, str] = {}
    available = _rulepack_available_functions(ctx, errors, sql_options=sql_options)
    missing = _rulepack_missing_functions(required, available)
    if missing:
        errors["missing_functions"] = str(missing)
    signature_errors = _rulepack_signature_validation(
        ctx,
        required_counts,
        required_signatures,
        errors,
        sql_options=sql_options,
    )
    if signature_errors is not None:
        errors["function_signatures"] = str(signature_errors)
    return errors


def _rulepack_available_functions(
    ctx: SessionContext,
    errors: dict[str, str],
    *,
    sql_options: SQLOptions | None = None,
) -> set[str]:
    try:
        return SchemaIntrospector(ctx, sql_options=sql_options).function_names()
    except (RuntimeError, TypeError, ValueError) as exc:
        errors["function_catalog"] = str(exc)
        return set()


def _rulepack_missing_functions(
    required: Mapping[str, set[str]],
    available: set[str],
) -> dict[str, list[str]]:
    available_lower = {name.lower() for name in available}
    return {
        name: sorted(rules)
        for name, rules in required.items()
        if name.lower() not in available_lower
    }


def _rulepack_signature_validation(
    ctx: SessionContext,
    required_counts: Mapping[str, int],
    required_signatures: Mapping[str, set[tuple[str, ...]]],
    errors: dict[str, str],
    *,
    sql_options: SQLOptions | None = None,
) -> dict[str, list[str]] | None:
    if not required_counts and not required_signatures:
        return None
    try:
        parameters = SchemaIntrospector(ctx, sql_options=sql_options).parameters_snapshot()
    except (RuntimeError, TypeError, ValueError) as exc:
        errors["function_parameters"] = str(exc)
        return None
    counts = _rulepack_parameter_counts(parameters)
    count_errors = _rulepack_signature_errors(required_counts, counts)
    type_errors = _rulepack_signature_type_errors(required_signatures, parameters)
    return _merge_signature_errors(count_errors, type_errors)


def apply_execution_label(
    options: DataFusionCompileOptions,
    *,
    execution_label: ExecutionLabel | None,
    explain_sink: list[dict[str, object]] | None,
) -> DataFusionCompileOptions:
    """Return compile options with rule-scoped diagnostics hooks applied.

    Parameters
    ----------
    options:
        Base compile options to update.
    execution_label:
        Optional label used to annotate diagnostics.
    explain_sink:
        Destination list for labeled explain entries.

    Returns
    -------
    DataFusionCompileOptions
        Options updated with labeled diagnostics hooks when configured.
    """
    if execution_label is None:
        return options
    explain_hook = options.explain_hook
    if explain_sink is not None and (options.capture_explain or explain_hook is not None):
        explain_hook = _chain_explain_hooks(
            explain_hook,
            labeled_explain_hook(execution_label, explain_sink),
        )
    if explain_hook is options.explain_hook:
        return options
    return replace(options, explain_hook=explain_hook)


def apply_execution_policy(
    options: DataFusionCompileOptions,
    *,
    execution_policy: AdapterExecutionPolicy | None,
) -> DataFusionCompileOptions:
    """Return compile options with an execution policy enforced.

    Parameters
    ----------
    options:
        Base compile options to update.
    execution_policy:
        Optional execution policy controls execution behaviors.

    Returns
    -------
    DataFusionCompileOptions
        Options updated with execution policy settings when configured.
    """
    _ = execution_policy
    return options


@lru_cache(maxsize=128)
def _datafusion_type_name(dtype: pa.DataType) -> str:
    ctx = DataFusionRuntimeProfile().ephemeral_context()
    table = pa.Table.from_arrays([pa.array([None], type=dtype)], names=["value"])
    from datafusion_engine.io_adapter import DataFusionIOAdapter

    adapter = DataFusionIOAdapter(ctx=ctx, profile=None)
    adapter.register_table_provider("t", ctx.from_arrow(table))
    result = _sql_with_options(ctx, "SELECT arrow_typeof(value) AS dtype FROM t").to_arrow_table()
    value = result["dtype"][0].as_py()
    if not isinstance(value, str):
        msg = "Failed to resolve DataFusion type name."
        raise TypeError(msg)
    return value


def _sql_identifier(name: str) -> str:
    escaped = name.replace('"', '""')
    return f'"{escaped}"'


def _apply_table_schema_metadata(
    table: pa.Table,
    *,
    schema: pa.Schema,
    keep_extra_columns: bool,
) -> pa.Table:
    if not keep_extra_columns:
        return table.cast(schema)
    metadata = dict(table.schema.metadata or {})
    metadata.update(schema.metadata or {})
    fields: list[pa.Field] = []
    for table_field in table.schema:
        try:
            expected = schema.field(table_field.name)
        except KeyError:
            fields.append(table_field)
            continue
        fields.append(
            pa.field(
                table_field.name,
                table_field.type,
                table_field.nullable,
                metadata=expected.metadata,
            )
        )
    return table.cast(pa.schema(fields, metadata=metadata))


def _align_projection_exprs(
    *,
    schema: pa.Schema,
    input_columns: Sequence[str],
    keep_extra_columns: bool,
) -> list[str]:
    selections: list[str] = []
    for schema_field in schema:
        dtype_name = _datafusion_type_name(schema_field.type)
        col_name = _sql_identifier(schema_field.name)
        if schema_field.name in input_columns:
            selections.append(f"arrow_cast({col_name}, '{dtype_name}') AS {col_name}")
        else:
            selections.append(f"arrow_cast(NULL, '{dtype_name}') AS {col_name}")
    if keep_extra_columns:
        for name in input_columns:
            if name in schema.names:
                continue
            selections.append(_sql_identifier(name))
    return selections


def align_table_to_schema(
    table: TableLike | RecordBatchReaderLike,
    *,
    schema: SchemaLike,
    keep_extra_columns: bool = False,
    ctx: SessionContext | None = None,
) -> pa.Table:
    """Align a table to a target schema using DataFusion casts.

    Returns
    -------
    pyarrow.Table
        Table aligned to the provided schema.
    """
    resolved_schema = pa.schema(schema)
    resolved = coerce_table_like(table)
    resolved_table: pa.Table
    if isinstance(resolved, pa.RecordBatchReader):
        reader = cast("pa.RecordBatchReader", resolved)
        resolved_table = cast("pa.Table", reader.read_all())
    else:
        resolved_table = cast("pa.Table", resolved)
    session = ctx or DataFusionRuntimeProfile().session_context()
    temp_name = f"__schema_align_{uuid.uuid4().hex}"
    from datafusion_engine.io_adapter import DataFusionIOAdapter

    adapter = DataFusionIOAdapter(ctx=session, profile=None)
    adapter.register_record_batches(temp_name, resolved_table.to_batches())
    try:
        selections = _align_projection_exprs(
            schema=resolved_schema,
            input_columns=resolved_table.column_names,
            keep_extra_columns=keep_extra_columns,
        )
        select_sql = ", ".join(selections)
        df = _sql_with_options(session, f"SELECT {select_sql} FROM {_sql_identifier(temp_name)}")
        aligned = df.to_arrow_table()
    finally:
        with contextlib.suppress(KeyError, RuntimeError, TypeError, ValueError):
            adapter.deregister_table(temp_name)
    return _apply_table_schema_metadata(
        aligned,
        schema=resolved_schema,
        keep_extra_columns=keep_extra_columns,
    )


def assert_schema_metadata(
    table: TableLike | RecordBatchReaderLike,
    *,
    schema: SchemaLike,
) -> None:
    """Raise when schema metadata does not match the target schema.

    Raises
    ------
    ValueError
        Raised when the schema metadata does not match.
    """
    table_schema = pa.schema(table.schema)
    expected_schema = pa.schema(schema)
    if not table_schema.equals(expected_schema, check_metadata=True):
        msg = "Schema metadata mismatch after finalize."
        raise ValueError(msg)


def dataset_schema_from_context(name: str) -> SchemaLike:
    """Return the dataset schema from the DataFusion SessionContext.

    Parameters
    ----------
    name : str
        Dataset name registered in the SessionContext.

    Returns
    -------
    SchemaLike
        Arrow schema fetched from DataFusion.

    Raises
    ------
    KeyError
        Raised when the dataset is not registered in the SessionContext.
    """
    if is_nested_dataset(name):
        return nested_schema_for(name, allow_derived=True)
    ctx = DataFusionRuntimeProfile().session_context()
    try:
        schema = ctx.table(name).schema()
    except (KeyError, RuntimeError, TypeError, ValueError) as exc:
        msg = f"Dataset schema not registered in DataFusion: {name!r}."
        raise KeyError(msg) from exc
    metadata = table_provider_metadata(id(ctx), table_name=name)
    if metadata is None or not metadata.metadata:
        return schema
    return _schema_with_table_metadata(schema, metadata=metadata.metadata)


def _schema_with_table_metadata(
    schema: SchemaLike,
    *,
    metadata: Mapping[str, str],
) -> SchemaLike:
    if not metadata:
        return schema
    if isinstance(schema, pa.Schema):
        merged = dict(schema.metadata or {})
        for key, value in metadata.items():
            merged.setdefault(key.encode("utf-8"), str(value).encode("utf-8"))
        return schema.with_metadata(merged)
    to_pyarrow = getattr(schema, "to_pyarrow", None)
    if callable(to_pyarrow):
        resolved = to_pyarrow()
        if isinstance(resolved, pa.Schema):
            resolved_schema = cast("pa.Schema", resolved)
            merged = dict(resolved_schema.metadata or {})
            for key, value in metadata.items():
                merged.setdefault(key.encode("utf-8"), str(value).encode("utf-8"))
            return resolved_schema.with_metadata(merged)
    return schema


def read_delta_as_reader(
    path: str,
    *,
    storage_options: Mapping[str, str] | None = None,
    delta_scan: DeltaScanOptions | None = None,
) -> pa.RecordBatchReader:
    """Return a streaming Delta table snapshot using Ibis read_delta.

    Returns
    -------
    pyarrow.RecordBatchReader
        Streaming reader for the Delta table via DataFusion's Delta table provider.

    Raises
    ------
    ValueError
        Raised when DeltaScanConfig overrides are requested.
    """
    if delta_scan is not None:
        msg = "Ibis read_delta does not support DeltaScanConfig overrides."
        raise ValueError(msg)
    from ibis_engine.execution_factory import ibis_backend_from_profile
    from ibis_engine.io_bridge import ibis_table_to_reader
    from ibis_engine.sources import IbisDeltaReadOptions, read_delta_ibis

    runtime_profile = DataFusionRuntimeProfile()
    backend = ibis_backend_from_profile(runtime_profile)
    table = read_delta_ibis(
        backend,
        path,
        options=IbisDeltaReadOptions(storage_options=storage_options),
    )
    return ibis_table_to_reader(table)


def dataset_spec_from_context(name: str) -> DatasetSpec:
    """Return a DatasetSpec derived from the DataFusion schema.

    Parameters
    ----------
    name : str
        Dataset name registered in the SessionContext.

    Returns
    -------
    DatasetSpec
        DatasetSpec derived from the DataFusion schema.
    """
    schema = dataset_schema_from_context(name)
    return dataset_spec_from_schema(name, schema)


__all__ = [
    "DATAFUSION_POLICY_PRESETS",
    "DEFAULT_DF_POLICY",
    "DEV_DF_POLICY",
    "PROD_DF_POLICY",
    "SCHEMA_HARDENING_PRESETS",
    "AdapterExecutionPolicy",
    "DataFusionConfigPolicy",
    "DataFusionExplainCollector",
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
    "align_table_to_schema",
    "apply_execution_label",
    "apply_execution_policy",
    "assert_schema_metadata",
    "collect_datafusion_metrics",
    "collect_datafusion_traces",
    "dataset_schema_from_context",
    "dataset_spec_from_context",
    "diagnostics_arrow_ingest_hook",
    "diagnostics_dml_hook",
    "evict_diskcache_entries",
    "feature_state_snapshot",
    "read_delta_as_reader",
    "register_view_specs",
    "run_diskcache_maintenance",
    "snapshot_plans",
    "sql_options_for_profile",
    "statement_sql_options_for_profile",
]

"""Runtime profile helpers for DataFusion execution."""

from __future__ import annotations

import contextlib
import importlib
import logging
import os
import tempfile
import time
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field, replace
from functools import lru_cache
from pathlib import Path
from typing import TYPE_CHECKING, Final, Literal, cast
from urllib.parse import urlparse
from weakref import WeakKeyDictionary

import datafusion
import msgspec
import pyarrow as pa
from datafusion import (
    RuntimeEnvBuilder,
    SessionConfig,
    SessionContext,
    SQLOptions,
    col,
    lit,
)
from datafusion.dataframe import DataFrame
from datafusion.expr import Expr
from datafusion.object_store import LocalFileSystem

from arrow_utils.core.schema_constants import DEFAULT_VALUE_META
from cache.diskcache_factory import (
    DiskCacheKind,
    DiskCacheProfile,
    cache_for_kind,
    default_diskcache_profile,
    diskcache_stats_snapshot,
    evict_cache_tag,
    run_profile_maintenance,
)
from core.config_base import FingerprintableConfig, config_fingerprint
from core_types import DeterminismTier
from datafusion_engine.arrow.coercion import to_arrow_table
from datafusion_engine.arrow.interop import (
    RecordBatchReaderLike,
    SchemaLike,
    TableLike,
    empty_table_for_schema,
)
from datafusion_engine.arrow.metadata import schema_constraints_from_metadata
from datafusion_engine.arrow.schema import (
    map_entry_type,
    version_field,
    versioned_entries_schema,
)
from datafusion_engine.compile.options import (
    DataFusionCacheEvent,
    DataFusionCompileOptions,
    DataFusionSqlPolicy,
    DataFusionSubstraitFallbackEvent,
    resolve_sql_policy,
)
from datafusion_engine.delta.protocol import DeltaProtocolSupport
from datafusion_engine.delta.store_policy import (
    DeltaStorePolicy,
    apply_delta_store_policy,
    delta_store_policy_hash,
)
from datafusion_engine.expr.cast import safe_cast
from datafusion_engine.expr.planner import expr_planner_payloads, install_expr_planners
from datafusion_engine.identity import schema_identity_hash
from datafusion_engine.lineage.diagnostics import (
    DiagnosticsSink,
    ensure_recorder_sink,
    record_artifact,
    record_events,
)
from serde_schema_registry import ArtifactSpec
from datafusion_engine.plan.cache import PlanCache, PlanProtoCache
from datafusion_engine.registry_facade import RegistrationPhase, RegistrationPhaseOrchestrator
from datafusion_engine.schema.introspection import (
    SchemaIntrospector,
    catalogs_snapshot,
    constraint_rows,
    table_constraint_rows,
)
from datafusion_engine.schema.registry import (
    AST_CORE_VIEW_NAMES,
    AST_OPTIONAL_VIEW_NAMES,
    CST_VIEW_NAMES,
    TREE_SITTER_CHECK_VIEWS,
    TREE_SITTER_VIEW_NAMES,
    extract_nested_dataset_names,
    extract_nested_schema_for,
    missing_schema_names,
    relationship_schema_for,
    relationship_schema_names,
    validate_nested_types,
    validate_required_engine_functions,
    validate_semantic_types,
    validate_udf_info_schema_parity,
)
from datafusion_engine.session.cache_policy import CachePolicyConfig, cache_policy_settings
from datafusion_engine.session.factory import SessionFactory
from datafusion_engine.session.helpers import deregister_table, register_temp_table
from datafusion_engine.sql.options import (
    planning_sql_options,
    sql_options_for_profile,
    statement_sql_options_for_profile,
)
from datafusion_engine.tables.metadata import table_provider_metadata
from datafusion_engine.udf.catalog import get_default_udf_catalog, get_strict_udf_catalog
from datafusion_engine.udf.factory import function_factory_payloads, install_function_factory
from datafusion_engine.views.artifacts import DataFusionViewArtifact
from serde_msgspec import MSGPACK_ENCODER, StructBaseCompat, StructBaseStrict
from storage.deltalake.config import DeltaMutationPolicy
from storage.ipc_utils import payload_hash
from utils.registry_protocol import Registry
from utils.uuid_factory import uuid7_str
from utils.validation import find_missing
from utils.value_coercion import coerce_int

_MISSING = object()

if TYPE_CHECKING:
    from typing import Protocol

    from diskcache import Cache, FanoutCache

    from datafusion_engine.bootstrap.zero_row import ZeroRowBootstrapReport, ZeroRowBootstrapRequest
    from datafusion_engine.catalog.introspection import IntrospectionCache
    from datafusion_engine.delta.service import DeltaService
    from datafusion_engine.session.factory import DataFusionContextPool
    from datafusion_engine.udf.catalog import UdfCatalog
    from obs.datafusion_runs import DataFusionRun
    from semantics.program_manifest import ManifestDatasetResolver
    from storage.deltalake.delta import IdempotentWriteOptions

    class _DeltaRuntimeEnvOptions(Protocol):
        max_spill_size: int | None
        max_temp_directory_size: int | None


from datafusion_engine.dataset.registry import (
    DatasetCatalog,
    DatasetLocation,
)
from datafusion_engine.views.artifacts import CachePolicy
from schema_spec.policies import DataFusionWritePolicy
from schema_spec.system import (
    DatasetSpec,
    DeltaScanOptions,
    ScanPolicyConfig,
    dataset_spec_from_schema,
)
from semantics.incremental.cdf_cursors import CdfCursorStore

if TYPE_CHECKING:
    ExplainRows = TableLike | RecordBatchReaderLike
else:
    ExplainRows = object

ExplainHook = Callable[[str, ExplainRows], None]
PlanArtifactsHook = Callable[[Mapping[str, object]], None]
SemanticDiffHook = Callable[[Mapping[str, object]], None]
SqlIngestHook = Callable[[Mapping[str, object]], None]
CacheEventHook = Callable[[DataFusionCacheEvent], None]
SubstraitFallbackHook = Callable[[DataFusionSubstraitFallbackEvent], None]

_TELEMETRY_MSGPACK_ENCODER = MSGPACK_ENCODER


def _encode_telemetry_msgpack(payload: object) -> bytes:
    buf = bytearray()
    _TELEMETRY_MSGPACK_ENCODER.encode_into(payload, buf)
    return bytes(buf)


MemoryPool = Literal["greedy", "fair", "unbounded"]

logger = logging.getLogger(__name__)
_RUNTIME_SESSION_ID: Final[str] = uuid7_str()

KIB: int = 1024
MIB: int = 1024 * KIB
GIB: int = 1024 * MIB

SETTINGS_HASH_VERSION: int = 1
TELEMETRY_PAYLOAD_VERSION: int = 2

_MAP_ENTRY_SCHEMA = map_entry_type(with_kind=True)
_SETTINGS_HASH_SCHEMA = versioned_entries_schema(_MAP_ENTRY_SCHEMA)
_SESSION_RUNTIME_HASH_SCHEMA = pa.schema(
    [
        version_field(),
        pa.field("profile_context_key", pa.string(), nullable=False),
        pa.field("profile_settings_hash", pa.string(), nullable=False),
        pa.field("udf_snapshot_hash", pa.string(), nullable=False),
        pa.field("udf_rewrite_tags", pa.list_(pa.string()), nullable=False),
        pa.field("domain_planner_names", pa.list_(pa.string()), nullable=False),
        pa.field("df_settings_entries", pa.list_(_MAP_ENTRY_SCHEMA), nullable=False),
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
        pa.field("force_disable_ident_normalization", pa.bool_()),
        pa.field("enable_url_table", pa.bool_()),
        pa.field("sql_parser_dialect", pa.string()),
        pa.field("ansi_mode", pa.bool_()),
    ]
)
_DELTA_RUNTIME_ENV_SCHEMA = pa.struct(
    [
        pa.field("max_spill_size", pa.int64()),
        pa.field("max_temp_directory_size", pa.int64()),
    ]
)
_EXTENSIONS_SCHEMA = pa.struct(
    [
        pa.field("delta_session_defaults_enabled", pa.bool_()),
        pa.field("delta_runtime_env", _DELTA_RUNTIME_ENV_SCHEMA),
        pa.field("delta_querybuilder_enabled", pa.bool_()),
        pa.field("delta_data_checker_enabled", pa.bool_()),
        pa.field("delta_plan_codecs_enabled", pa.bool_()),
        pa.field("delta_plan_codec_physical", pa.string()),
        pa.field("delta_plan_codec_logical", pa.string()),
        pa.field("snapshot_pinned_mode", pa.string()),
        pa.field("delta_ffi_provider_enforced", pa.bool_()),
        pa.field("expr_planners_enabled", pa.bool_()),
        pa.field("expr_planner_names", pa.list_(pa.string())),
        pa.field("physical_expr_adapter_factory", pa.bool_()),
        pa.field("schema_evolution_adapter_enabled", pa.bool_()),
        pa.field("named_args_supported", pa.bool_()),
        pa.field("async_udfs_enabled", pa.bool_()),
        pa.field("async_udf_timeout_ms", pa.int64()),
        pa.field("async_udf_batch_size", pa.int64()),
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
_ZERO_ROW_BOOTSTRAP_SCHEMA = pa.struct(
    [
        pa.field("validation_mode", pa.string()),
        pa.field("include_semantic_outputs", pa.bool_()),
        pa.field("include_internal_tables", pa.bool_()),
        pa.field("strict", pa.bool_()),
        pa.field("allow_semantic_row_probe_fallback", pa.bool_()),
        pa.field("bootstrap_mode", pa.string()),
        pa.field("seeded_datasets", pa.list_(pa.string())),
    ]
)
_DELTA_STORE_POLICY_SCHEMA = pa.struct(
    [
        pa.field("storage_options", pa.list_(_MAP_ENTRY_SCHEMA)),
        pa.field("log_storage_options", pa.list_(_MAP_ENTRY_SCHEMA)),
        pa.field("require_local_paths", pa.bool_()),
    ]
)
_DELTA_RETRY_POLICY_SCHEMA = pa.struct(
    [
        pa.field("max_attempts", pa.int64()),
        pa.field("base_delay_s", pa.float64()),
        pa.field("max_delay_s", pa.float64()),
        pa.field("retryable_errors", pa.list_(pa.string())),
        pa.field("fatal_errors", pa.list_(pa.string())),
    ]
)
_DELTA_MUTATION_POLICY_SCHEMA = pa.struct(
    [
        pa.field("retry_policy", _DELTA_RETRY_POLICY_SCHEMA),
        pa.field("require_locking_provider", pa.bool_()),
        pa.field("locking_option_keys", pa.list_(pa.string())),
        pa.field("append_only", pa.bool_()),
    ]
)
_TELEMETRY_SCHEMA = pa.schema(
    [
        version_field(nullable=True),
        pa.field("profile_name", pa.string()),
        pa.field("datafusion_version", pa.string()),
        pa.field("architecture_version", pa.string()),
        pa.field("sql_policy_name", pa.string()),
        pa.field("cache_profile_name", pa.string()),
        pa.field("session_config", pa.list_(_MAP_ENTRY_SCHEMA)),
        pa.field("settings_hash", pa.string()),
        pa.field("external_table_options", pa.list_(_MAP_ENTRY_SCHEMA)),
        pa.field("delta_store_policy_hash", pa.string()),
        pa.field("delta_store_policy", _DELTA_STORE_POLICY_SCHEMA),
        pa.field("delta_mutation_policy_hash", pa.string()),
        pa.field("delta_mutation_policy", _DELTA_MUTATION_POLICY_SCHEMA),
        pa.field("sql_policy", _SQL_POLICY_SCHEMA),
        pa.field("param_identifier_allowlist", pa.list_(pa.string())),
        pa.field("write_policy", _WRITE_POLICY_SCHEMA),
        pa.field("feature_gates", pa.list_(_MAP_ENTRY_SCHEMA)),
        pa.field("cache_profile_settings", pa.list_(_MAP_ENTRY_SCHEMA)),
        pa.field("join_policy", pa.list_(_MAP_ENTRY_SCHEMA)),
        pa.field("parquet_read", pa.list_(_MAP_ENTRY_SCHEMA)),
        pa.field("listing_table", pa.list_(_MAP_ENTRY_SCHEMA)),
        pa.field("spill", _SPILL_SCHEMA),
        pa.field("execution", _EXECUTION_SCHEMA),
        pa.field("sql_surfaces", _SQL_SURFACES_SCHEMA),
        pa.field("extensions", _EXTENSIONS_SCHEMA),
        pa.field("substrait_validation", pa.bool_()),
        pa.field("output_writes", _OUTPUT_WRITES_SCHEMA),
        pa.field("zero_row_bootstrap", _ZERO_ROW_BOOTSTRAP_SCHEMA),
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


def effective_catalog_autoload(
    profile: DataFusionRuntimeProfile,
) -> tuple[str | None, str | None]:
    """Return effective catalog autoload settings for a profile.

    Returns:
    -------
    tuple[str | None, str | None]
        Catalog location and file format.
    """
    return _effective_catalog_autoload_for_profile(profile)


def effective_ident_normalization(profile: DataFusionRuntimeProfile) -> bool:
    """Return whether identifier normalization is enabled for a profile.

    Returns:
    -------
    bool
        True when identifier normalization is enabled.
    """
    return _effective_ident_normalization(profile)


def supports_explain_analyze_level() -> bool:
    """Return whether explain analyze level is supported.

    Returns:
    -------
    bool
        True when explain analyze level is supported.
    """
    return _supports_explain_analyze_level()


def resolved_config_policy(
    profile: DataFusionRuntimeProfile,
) -> DataFusionConfigPolicy | None:
    """Return resolved config policy for a profile.

    Returns:
    -------
    DataFusionConfigPolicy | None
        Resolved policy or None.
    """
    return _resolved_config_policy_for_profile(profile)


def resolved_schema_hardening(
    profile: DataFusionRuntimeProfile,
) -> SchemaHardeningProfile | None:
    """Return resolved schema hardening profile for a profile.

    Returns:
    -------
    SchemaHardeningProfile | None
        Resolved schema hardening profile or None.
    """
    return _resolved_schema_hardening_for_profile(profile)


def _resolve_extension_module(required_attr: str | None = None) -> object | None:
    for module_name in ("datafusion._internal", "datafusion_ext"):
        try:
            module = importlib.import_module(module_name)
        except ImportError:
            continue
        if required_attr is not None and not hasattr(module, required_attr):
            continue
        return module
    return None


def _resolve_tracing_context(
    ctx: SessionContext,
    module: object,
) -> tuple[object | None, dict[str, object]]:
    module_name = getattr(module, "__name__", "")
    internal_ctx = getattr(ctx, "ctx", None)
    session_type = getattr(module, "SessionContext", None)
    ctx_module = type(ctx).__module__
    internal_module = type(internal_ctx).__module__ if internal_ctx is not None else None
    ctx_arg: object | None = None
    if isinstance(session_type, type):
        if isinstance(ctx, session_type):
            ctx_arg = ctx
        elif internal_ctx is not None and isinstance(internal_ctx, session_type):
            ctx_arg = internal_ctx
    elif (
        internal_ctx is not None
        and module_name
        and internal_module is not None
        and module_name in internal_module
    ):
        ctx_arg = internal_ctx
    elif module_name and module_name in ctx_module:
        ctx_arg = ctx
    details = {
        "module": module_name,
        "context_type": type(ctx).__name__,
        "context_module": ctx_module,
        "internal_context_type": type(internal_ctx).__name__ if internal_ctx is not None else None,
        "internal_context_module": internal_module,
    }
    return ctx_arg, details


def delta_runtime_env_options(
    profile: DataFusionRuntimeProfile,
) -> _DeltaRuntimeEnvOptions | None:
    """Return delta runtime env options for a profile.

    Args:
        profile: Description.

    Raises:
        RuntimeError: If the operation cannot be completed.
        TypeError: If the operation cannot be completed.
    """
    if (
        profile.execution.delta_max_spill_size is None
        and profile.execution.delta_max_temp_directory_size is None
    ):
        return None
    module = _resolve_extension_module(required_attr="DeltaRuntimeEnvOptions")
    if module is None:
        msg = "Delta runtime env options require datafusion._internal or datafusion_ext."
        raise RuntimeError(msg)
    options_cls = getattr(module, "DeltaRuntimeEnvOptions", None)
    if not callable(options_cls):
        msg = "Delta runtime env options type is unavailable in the extension module."
        raise TypeError(msg)
    options = cast("_DeltaRuntimeEnvOptions", options_cls())
    if profile.execution.delta_max_spill_size is not None:
        options.max_spill_size = int(profile.execution.delta_max_spill_size)
    if profile.execution.delta_max_temp_directory_size is not None:
        options.max_temp_directory_size = int(profile.execution.delta_max_temp_directory_size)
    return options


def record_delta_session_defaults(
    profile: DataFusionRuntimeProfile,
    *,
    available: bool,
    installed: bool,
    error: str | None,
    runtime_policy_bridge: Mapping[str, object] | None = None,
) -> None:
    """Record delta session defaults metadata for a profile."""
    if profile.diagnostics.diagnostics_sink is None:
        return
    profile.record_artifact(
        "datafusion_delta_session_defaults_v1",
        {
            "enabled": profile.features.enable_delta_session_defaults,
            "available": available,
            "installed": installed,
            "error": error,
            "runtime_policy_bridge": (
                dict(runtime_policy_bridge) if runtime_policy_bridge is not None else None
            ),
        },
    )


def record_schema_snapshots_for_profile(profile: DataFusionRuntimeProfile) -> None:
    """Record information_schema snapshots to diagnostics when enabled."""
    if profile.diagnostics.diagnostics_sink is None:
        return
    if not profile.catalog.enable_information_schema:
        return
    ctx = profile.session_context()
    introspector = schema_introspector_for_profile(profile, ctx)
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
                    sql_options=sql_options_for_profile(profile),
                ),
                "routines": introspector.routines_snapshot(),
                "parameters": introspector.parameters_snapshot(),
                "settings": introspector.settings_snapshot(),
                "functions": function_catalog_snapshot_for_profile(
                    profile,
                    ctx,
                    include_routines=profile.catalog.enable_information_schema,
                ),
            }
        )
        version = _datafusion_version(ctx)
        if version is not None:
            payload["datafusion_version"] = version
    except (RuntimeError, TypeError, ValueError) as exc:
        payload["error"] = str(exc)
    profile.record_artifact("datafusion_schema_introspection_v1", payload)


def normalize_dataset_locations_for_profile(
    profile: DataFusionRuntimeProfile,
) -> Mapping[str, DatasetLocation]:
    """Return normalize dataset locations derived from the output root.

    Returns:
    -------
    Mapping[str, DatasetLocation]
        Mapping of normalize dataset names to locations, or empty mapping
        when normalize output root is not configured.
    """
    normalize_root = profile.data_sources.semantic_output.normalize_output_root
    if normalize_root is None:
        return {}
    root = Path(normalize_root)
    from schema_spec.dataset_spec_ops import dataset_spec_name
    from semantics.catalog.dataset_specs import dataset_specs

    locations: dict[str, DatasetLocation] = {}
    for spec in dataset_specs():
        name = dataset_spec_name(spec)
        locations[name] = DatasetLocation(
            path=str(root / name),
            format="delta",
            dataset_spec=spec,
        )
    return locations


def extract_output_locations_for_profile(
    profile: DataFusionRuntimeProfile,
) -> Mapping[str, DatasetLocation]:
    """Return extract output dataset locations derived from the output root.

    Returns:
    -------
    Mapping[str, DatasetLocation]
        Mapping of extract dataset names to locations, or empty mapping
        when extract output root/catalog is not configured.
    """
    extract_output = profile.data_sources.extract_output
    if extract_output.dataset_locations:
        return extract_output.dataset_locations
    if extract_output.output_catalog_name is not None:
        catalog = profile.catalog.registry_catalogs.get(extract_output.output_catalog_name)
        if catalog is None:
            return {}
        return {name: catalog.get(name) for name in catalog.names()}
    if extract_output.output_root is None:
        return {}
    from datafusion_engine.extract.output_catalog import build_extract_output_catalog

    catalog = build_extract_output_catalog(output_root=extract_output.output_root)
    return {name: catalog.get(name) for name in catalog.names()}


def semantic_output_locations_for_profile(
    profile: DataFusionRuntimeProfile,
) -> Mapping[str, DatasetLocation]:
    """Return semantic output dataset locations derived from the output root.

    Returns:
    -------
    Mapping[str, DatasetLocation]
        Mapping of semantic output names to locations, or empty mapping
        when semantic output root is not configured and no explicit
        semantic output locations are provided.
    """
    from semantics.registry import SEMANTIC_MODEL

    view_names = [spec.name for spec in SEMANTIC_MODEL.outputs]
    semantic_output = profile.data_sources.semantic_output
    if semantic_output.locations:
        return semantic_output.locations
    if semantic_output.output_catalog_name is not None:
        catalog = profile.catalog.registry_catalogs.get(semantic_output.output_catalog_name)
        if catalog is None:
            return {}
        locations: dict[str, DatasetLocation] = {}
        for name in view_names:
            if catalog.has(name):
                locations[name] = catalog.get(name)
        return locations
    if semantic_output.output_root is None:
        return {}
    root = Path(semantic_output.output_root)
    locations: dict[str, DatasetLocation] = {}
    for name in view_names:
        locations[name] = DatasetLocation(
            path=str(root / name),
            format="delta",
        )
    return locations


def _introspection_cache_for_ctx(
    ctx: SessionContext,
    *,
    sql_options: SQLOptions | None,
) -> IntrospectionCache:
    from datafusion_engine.catalog.introspection import introspection_cache_for_ctx

    return introspection_cache_for_ctx(ctx, sql_options=sql_options)


def _capture_cache_diagnostics(ctx: SessionContext) -> Mapping[str, object]:
    from datafusion_engine.catalog.introspection import capture_cache_diagnostics

    return capture_cache_diagnostics(ctx)


def _register_cache_introspection_functions(ctx: SessionContext) -> None:
    from datafusion_engine.catalog.introspection import register_cache_introspection_functions

    register_cache_introspection_functions(ctx)


def _cache_config_payload(cache_diag: Mapping[str, object]) -> Mapping[str, object]:
    payload = cache_diag.get("config")
    if isinstance(payload, Mapping):
        return payload
    return {}


def _cache_snapshot_rows(cache_diag: Mapping[str, object]) -> list[Mapping[str, object]]:
    payload = cache_diag.get("cache_snapshots")
    if not isinstance(payload, Sequence):
        return []
    rows: list[Mapping[str, object]] = []
    rows.extend(snapshot for snapshot in payload if isinstance(snapshot, Mapping))
    return rows


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


class DataFusionConfigPolicy(StructBaseStrict, frozen=True):
    """Configuration policy for DataFusion SessionConfig."""

    settings: Mapping[str, str]

    def fingerprint_payload(self) -> Mapping[str, object]:
        """Return the fingerprint payload for config settings.

        Returns:
        -------
        Mapping[str, object]
            Payload describing the DataFusion settings.
        """
        return {
            "settings": dict(self.settings),
        }

    def fingerprint(self) -> str:
        """Return a stable fingerprint for the config settings.

        Returns:
        -------
        str
            Stable fingerprint hash.
        """
        return config_fingerprint(self.fingerprint_payload())

    def apply(self, config: SessionConfig) -> SessionConfig:
        """Return a SessionConfig with policy settings applied.

        Returns:
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


class DataFusionFeatureGates(StructBaseStrict, frozen=True):
    """Feature gate toggles for DataFusion optimizer behavior."""

    enable_dynamic_filter_pushdown: bool = True
    enable_join_dynamic_filter_pushdown: bool = True
    enable_aggregate_dynamic_filter_pushdown: bool = True
    enable_topk_dynamic_filter_pushdown: bool = True

    def settings(self) -> dict[str, str]:
        """Return DataFusion config settings for the feature gates.

        Returns:
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

    def fingerprint_payload(self) -> Mapping[str, object]:
        """Return fingerprint payload for feature gate settings.

        Returns:
        -------
        Mapping[str, object]
            Payload describing feature gate settings.
        """
        return {
            "enable_dynamic_filter_pushdown": self.enable_dynamic_filter_pushdown,
            "enable_join_dynamic_filter_pushdown": self.enable_join_dynamic_filter_pushdown,
            "enable_aggregate_dynamic_filter_pushdown": (
                self.enable_aggregate_dynamic_filter_pushdown
            ),
            "enable_topk_dynamic_filter_pushdown": self.enable_topk_dynamic_filter_pushdown,
        }

    def fingerprint(self) -> str:
        """Return fingerprint for feature gate settings.

        Returns:
        -------
        str
            Deterministic fingerprint for the feature gates.
        """
        return config_fingerprint(self.fingerprint_payload())


class DataFusionJoinPolicy(StructBaseStrict, frozen=True):
    """Join algorithm preferences for DataFusion."""

    enable_hash_join: bool = True
    enable_sort_merge_join: bool = True
    enable_nested_loop_join: bool = True
    repartition_joins: bool = True
    enable_round_robin_repartition: bool = True
    perfect_hash_join_small_build_threshold: int | None = None
    perfect_hash_join_min_key_density: float | None = None

    def fingerprint_payload(self) -> Mapping[str, object]:
        """Return the fingerprint payload for join policy settings.

        Returns:
        -------
        Mapping[str, object]
            Payload describing join policy settings.
        """
        return {
            "enable_hash_join": self.enable_hash_join,
            "enable_sort_merge_join": self.enable_sort_merge_join,
            "enable_nested_loop_join": self.enable_nested_loop_join,
            "repartition_joins": self.repartition_joins,
            "enable_round_robin_repartition": self.enable_round_robin_repartition,
            "perfect_hash_join_small_build_threshold": self.perfect_hash_join_small_build_threshold,
            "perfect_hash_join_min_key_density": self.perfect_hash_join_min_key_density,
        }

    def fingerprint(self) -> str:
        """Return a stable fingerprint for join policy settings.

        Returns:
        -------
        str
            Stable fingerprint hash.
        """
        return config_fingerprint(self.fingerprint_payload())

    def settings(self) -> dict[str, str]:
        """Return DataFusion config settings for join preferences.

        Returns:
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


class DataFusionSettingsContract(StructBaseStrict, frozen=True):
    """Settings contract for DataFusion session configuration."""

    settings: Mapping[str, str]
    feature_gates: DataFusionFeatureGates

    def apply(self, config: SessionConfig) -> SessionConfig:
        """Return a SessionConfig with settings and feature gates applied.

        Returns:
        -------
        datafusion.SessionConfig
            Session config with settings applied.
        """
        merged = {**self.settings, **self.feature_gates.settings()}
        for key, value in merged.items():
            config = config.set(key, value)
        return config


class SchemaHardeningProfile(StructBaseStrict, frozen=True):
    """Schema-stability settings for DataFusion SessionConfig."""

    enable_view_types: bool = False
    expand_views_at_output: bool = False
    timezone: str = "UTC"
    parser_dialect: str | None = None
    show_schema_in_explain: bool = True
    explain_format: str = "tree"
    show_types_in_format: bool = True
    strict_aggregate_schema_check: bool = True

    def settings(self) -> dict[str, str]:
        """Return DataFusion settings for schema hardening.

        Returns:
        -------
        dict[str, str]
            Mapping of DataFusion config keys to string values.
        """
        settings = {
            "datafusion.explain.show_schema": str(self.show_schema_in_explain).lower(),
            "datafusion.explain.format": self.explain_format,
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

    def fingerprint_payload(self) -> Mapping[str, object]:
        """Return canonical payload for fingerprinting.

        Returns:
        -------
        Mapping[str, object]
            Payload used for profile fingerprinting.
        """
        return {
            "enable_view_types": self.enable_view_types,
            "expand_views_at_output": self.expand_views_at_output,
            "timezone": self.timezone,
            "parser_dialect": self.parser_dialect,
            "show_schema_in_explain": self.show_schema_in_explain,
            "explain_format": self.explain_format,
            "show_types_in_format": self.show_types_in_format,
            "strict_aggregate_schema_check": self.strict_aggregate_schema_check,
        }

    def fingerprint(self) -> str:
        """Return a stable fingerprint for the profile.

        Returns:
        -------
        str
            Deterministic fingerprint string.
        """
        return config_fingerprint(self.fingerprint_payload())

    def apply(self, config: SessionConfig) -> SessionConfig:
        """Return SessionConfig with schema hardening settings applied.

        Returns:
        -------
        datafusion.SessionConfig
            Updated session config with schema hardening settings.
        """
        for key, value in self.settings().items():
            config = config.set(key, value)
        return config


class FeatureStateSnapshot(
    StructBaseCompat,
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

        Returns:
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

    Returns:
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
    gates = runtime_profile.policies.feature_gates
    dynamic_filters_enabled = (
        gates.enable_dynamic_filter_pushdown
        and gates.enable_join_dynamic_filter_pushdown
        and gates.enable_aggregate_dynamic_filter_pushdown
        and gates.enable_topk_dynamic_filter_pushdown
    )
    spill_enabled = runtime_profile.execution.spill_dir is not None
    return FeatureStateSnapshot(
        profile_name=profile_name,
        determinism_tier=determinism_tier,
        dynamic_filters_enabled=dynamic_filters_enabled,
        spill_enabled=spill_enabled,
        named_args_supported=named_args_supported(runtime_profile),
    )


def named_args_supported(profile: DataFusionRuntimeProfile) -> bool:
    """Return whether named arguments are enabled for SQL execution.

    Parameters
    ----------
    profile
        Runtime profile to evaluate.

    Returns:
    -------
    bool
        ``True`` when named arguments should be supported.
    """
    if not profile.features.enable_expr_planners:
        return False
    if profile.policies.expr_planner_hook is not None:
        return True
    if not profile.policies.expr_planner_names:
        return False
    from datafusion_engine.udf.platform import native_udf_platform_available

    return native_udf_platform_available()


@dataclass
class _DataFusionExplainCollector:
    """Collect EXPLAIN artifacts for diagnostics."""

    entries: list[dict[str, object]] = field(default_factory=list)

    def hook(self, sql: str, rows: ExplainRows) -> None:
        """Collect an explain payload for a single statement."""
        payload = {"sql": sql, "rows": rows}
        self.entries.append(cast("dict[str, object]", payload))

    def snapshot(self) -> list[dict[str, object]]:
        """Return a snapshot of explain artifacts.

        Returns:
        -------
        list[dict[str, object]]
            Collected explain artifacts.
        """
        return list(self.entries)


@dataclass
class _DataFusionPlanCollector:
    """Collect DataFusion plan artifacts."""

    entries: list[dict[str, object]] = field(default_factory=list)

    def hook(self, payload: Mapping[str, object]) -> None:
        """Collect a plan artifact payload."""
        self.entries.append(dict(payload))

    def snapshot(self) -> list[dict[str, object]]:
        """Return a snapshot of plan artifacts.

        Returns:
        -------
        list[dict[str, object]]
            Plan artifact payloads.
        """
        return list(self.entries)


@dataclass
class DataFusionViewRegistry(Registry[str, DataFusionViewArtifact]):
    """Record DataFusion view artifacts for reproducibility."""

    entries: dict[str, DataFusionViewArtifact] = field(default_factory=dict)

    def register(self, key: str, value: DataFusionViewArtifact) -> None:
        """Register a view artifact by name.

        Parameters
        ----------
        key
            View name.
        value
            View artifact payload for the registry.
        """
        self.entries[key] = value

    def record(self, *, name: str, artifact: DataFusionViewArtifact) -> None:
        """Record a view artifact by name.

        Parameters
        ----------
        name
            View name.
        artifact
            View artifact payload for the registry.
        """
        self.register(name, artifact)

    def get(self, key: str) -> DataFusionViewArtifact | None:
        """Return a view artifact when present.

        Parameters
        ----------
        key
            View name.

        Returns:
        -------
        DataFusionViewArtifact | None
            View artifact when registered, otherwise ``None``.
        """
        return self.entries.get(key)

    def __contains__(self, key: str) -> bool:
        """Return True when a view artifact is registered.

        Parameters
        ----------
        key
            View name.

        Returns:
        -------
        bool
            ``True`` when the view is registered.
        """
        return key in self.entries

    def __iter__(self) -> Iterator[str]:
        """Iterate over registered view names.

        Returns:
        -------
        Iterator[str]
            Iterator of registered view names.
        """
        return iter(self.entries)

    def __len__(self) -> int:
        """Return the number of registered views.

        Returns:
        -------
        int
            Count of registered views.
        """
        return len(self.entries)

    def snapshot(self) -> list[dict[str, object]]:
        """Return a stable snapshot of registered view artifacts.

        Returns:
        -------
        list[dict[str, object]]
            Snapshot payloads for registered view artifacts.
        """
        return [
            artifact.payload()
            for _, artifact in sorted(self.entries.items(), key=lambda item: item[0])
        ]

    def diagnostics_snapshot(self, *, event_time_unix_ms: int) -> list[dict[str, object]]:
        """Return diagnostics payloads for registered view artifacts.

        Parameters
        ----------
        event_time_unix_ms
            Event timestamp to attach to each payload.

        Returns:
        -------
        list[dict[str, object]]
            Diagnostics-ready payloads for registered view artifacts.
        """
        return [
            artifact.diagnostics_payload(event_time_unix_ms=event_time_unix_ms)
            for _, artifact in sorted(self.entries.items(), key=lambda item: item[0])
        ]


class PreparedStatementSpec(StructBaseStrict, frozen=True):
    """Prepared statement specification for DataFusion."""

    name: str
    sql: str
    param_types: tuple[str, ...] = ()


@dataclass(frozen=True)
class AdapterExecutionPolicy(FingerprintableConfig):
    """Execution policy for adapterized execution handling."""

    def fingerprint_payload(self) -> Mapping[str, object]:
        """Return the fingerprint payload for adapter execution policy.

        Returns:
        -------
        Mapping[str, object]
            Payload describing adapter execution policy.
        """
        return {"policy": self.__class__.__name__}

    def fingerprint(self) -> str:
        """Return a stable fingerprint for adapter execution policy.

        Returns:
        -------
        str
            Stable fingerprint hash.
        """
        return config_fingerprint(self.fingerprint_payload())


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

CACHE_PROFILES: Mapping[str, Mapping[str, str]] = {
    "snapshot_pinned": {
        "datafusion.runtime.list_files_cache_limit": str(64 * MIB),
        "datafusion.runtime.metadata_cache_limit": str(128 * MIB),
    },
    "always_latest_ttl30s": {
        "datafusion.runtime.list_files_cache_limit": str(64 * MIB),
        "datafusion.runtime.list_files_cache_ttl": "30s",
        "datafusion.runtime.metadata_cache_limit": str(128 * MIB),
    },
    "multi_tenant_strict": {
        "datafusion.runtime.list_files_cache_limit": "0",
        "datafusion.runtime.metadata_cache_limit": "0",
    },
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
            "SELECT specific_name AS routine_name, parameter_name, parameter_mode, "
            "data_type, ordinal_position FROM information_schema.parameters"
        ),
    ),
)
INFO_SCHEMA_STATEMENT_NAMES: frozenset[str] = frozenset(
    spec.name for spec in INFO_SCHEMA_STATEMENTS
)

_SESSION_CONTEXT_CACHE: dict[str, SessionContext] = {}


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
        module = importlib.import_module("datafusion._internal")
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
        module = importlib.import_module("datafusion._internal")
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


def _sql_parse_errors(
    ctx: SessionContext,
    sql: str,
    *,
    sql_options: SQLOptions,
) -> list[dict[str, object]] | None:
    """Parse SQL using DataFusion and collect parsing errors.

    Parameters
    ----------
    ctx
        DataFusion SessionContext used for SQL parsing.
    sql
        SQL string to parse and validate.
    sql_options
        SQL options that gate SQL execution behavior.

    Returns:
    -------
    list[dict[str, object]] | None
        List of parsing errors if any, None if parsing succeeds.
    """
    try:
        _ = _sql_with_options(ctx, sql, sql_options=sql_options)
    except ValueError as exc:
        return [{"message": str(exc)}]
    return None


def _collect_view_sql_parse_errors(
    ctx: SessionContext,
    registry: DataFusionViewRegistry,
    *,
    sql_options: SQLOptions,
) -> dict[str, list[dict[str, object]]] | None:
    errors: dict[str, list[dict[str, object]]] = {}
    for name, entry in registry.entries.items():
        sql = entry if isinstance(entry, str) else getattr(entry, "sql", None)
        if not isinstance(sql, str) or not sql:
            continue
        parse_errors = _sql_parse_errors(ctx, sql, sql_options=sql_options)
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
    schemas: Mapping[str, pa.Schema] | None = None,
) -> list[dict[str, object]]:
    entries: list[dict[str, object]] = []
    schema_map = dict(schemas or {})
    for name in names:
        schema = schema_map.get(name)
        if schema is None:
            continue
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
    session_runtime: SessionRuntime,
    *,
    sql_options: SQLOptions,
) -> Mapping[str, object] | None:
    _ = session_runtime
    _ = sql_options
    return None


@dataclass(frozen=True)
class SchemaRegistryValidationResult:
    """Summary of schema registry validation checks."""

    missing: tuple[str, ...] = ()
    type_errors: dict[str, str] = field(default_factory=dict)
    view_errors: dict[str, str] = field(default_factory=dict)
    constraint_drift: tuple[dict[str, object], ...] = ()
    relationship_constraint_errors: dict[str, object] | None = None

    def has_errors(self) -> bool:
        """Return whether any validation errors are present.

        Returns:
        -------
        bool
            ``True`` when the validation result includes any errors.
        """
        return bool(
            self.missing
            or self.type_errors
            or self.view_errors
            or self.constraint_drift
            or self.relationship_constraint_errors
        )


def _register_schema_table(ctx: SessionContext, name: str, schema: pa.Schema) -> None:
    """Register a schema-only table via an empty table provider."""
    table = empty_table_for_schema(schema)
    from datafusion_engine.io.adapter import DataFusionIOAdapter

    adapter = DataFusionIOAdapter(ctx=ctx, profile=None)
    adapter.register_arrow_table(name, table)


def _load_schema_evolution_adapter_factory() -> object:
    """Return a schema evolution adapter factory from the native extension.

    Raises:
        RuntimeError: If the operation cannot be completed.
        TypeError: If the operation cannot be completed.
    """
    module = _resolve_extension_module(required_attr="schema_evolution_adapter_factory")
    if module is None:  # pragma: no cover - optional dependency
        msg = "Schema evolution adapter requires datafusion._internal or datafusion_ext."
        raise RuntimeError(msg)
    factory = getattr(module, "schema_evolution_adapter_factory", None)
    if not callable(factory):
        msg = "Schema evolution adapter factory is unavailable in the extension module."
        raise TypeError(msg)
    return factory()


def _install_schema_evolution_adapter_factory(ctx: SessionContext) -> None:
    """Install the schema evolution adapter factory via the native extension.

    Args:
        ctx: Description.

    Raises:
        RuntimeError: If the operation cannot be completed.
        TypeError: If the operation cannot be completed.
    """
    module = _resolve_extension_module(
        required_attr="install_schema_evolution_adapter_factory",
    )
    if module is None:  # pragma: no cover - optional dependency
        msg = "Schema evolution adapter requires datafusion._internal or datafusion_ext."
        raise RuntimeError(msg)
    installer = getattr(module, "install_schema_evolution_adapter_factory", None)
    if not callable(installer):
        msg = "Schema evolution adapter installer is unavailable in the extension module."
        raise TypeError(msg)
    try:
        installer(ctx)
    except TypeError as exc:
        if "cannot be converted" in str(exc):
            return
        raise


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
    return planning_sql_options(None)


def _sql_with_options(
    ctx: SessionContext,
    sql: str,
    *,
    sql_options: SQLOptions | None = None,
    allow_statements: bool | None = None,
) -> DataFrame:
    resolved_sql_options = sql_options or _read_only_sql_options()
    if allow_statements:
        allow_statements_flag = True
        resolved_sql_options = resolved_sql_options.with_allow_statements(allow_statements_flag)
    try:
        df = ctx.sql_with_options(sql, resolved_sql_options)
    except (RuntimeError, TypeError, ValueError) as exc:
        msg = "Runtime SQL execution did not return a DataFusion DataFrame."
        raise ValueError(msg) from exc
    if df is None:
        msg = "Runtime SQL execution did not return a DataFusion DataFrame."
        raise ValueError(msg)
    return df


def settings_snapshot_for_profile(
    profile: DataFusionRuntimeProfile, ctx: SessionContext
) -> pa.Table:
    """Return a DataFusion settings snapshot for a runtime profile.

    Returns:
    -------
    pyarrow.Table
        Table of settings from information_schema.df_settings.
    """
    cache = _introspection_cache_for_ctx(ctx, sql_options=profile.sql_options())
    base_table = cache.snapshot.settings
    rows = [dict(row) for row in base_table.to_pylist()]
    rows = _merge_runtime_settings_rows(
        rows,
        schema=base_table.schema,
        profile=profile,
        ctx=ctx,
    )
    rows = sorted(rows, key=lambda row: _settings_row_name(row) or "")
    return pa.Table.from_pylist(rows, schema=base_table.schema)


def catalog_snapshot_for_profile(
    profile: DataFusionRuntimeProfile, ctx: SessionContext
) -> pa.Table:
    """Return a DataFusion catalog snapshot for a runtime profile.

    Returns:
    -------
    pyarrow.Table
        Table inventory from information_schema.tables.
    """
    cache = _introspection_cache_for_ctx(ctx, sql_options=profile.sql_options())
    return cache.snapshot.tables


def function_catalog_snapshot_for_profile(
    profile: DataFusionRuntimeProfile,
    ctx: SessionContext,
    *,
    include_routines: bool = False,
) -> list[dict[str, object]]:
    """Return a function catalog snapshot for a runtime profile.

    Returns:
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
    artifact: DataFusionViewArtifact,
) -> None:
    """Record a view artifact for diagnostics snapshots.

    Parameters
    ----------
    profile
        Runtime profile for recording diagnostics.
    artifact
        View artifact payload for diagnostics.
    """
    if profile.view_registry is None:
        return
    profile.view_registry.record(name=artifact.name, artifact=artifact)
    payload = artifact.diagnostics_payload(event_time_unix_ms=int(time.time() * 1000))
    record_artifact(profile, "datafusion_view_artifacts_v4", payload)


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


def _delta_store_policy_payload(
    policy: DeltaStorePolicy | None,
) -> dict[str, object] | None:
    if policy is None:
        return None
    return {
        "storage_options": _map_entries(policy.storage_options),
        "log_storage_options": _map_entries(policy.log_storage_options),
        "require_local_paths": policy.require_local_paths,
    }


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

    Returns:
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

    Returns:
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
                    "profile_hash": event.profile_hash,
                    "plan_fingerprint": event.plan_fingerprint,
                }
            ],
        )

    return _hook


def diagnostics_substrait_fallback_hook(
    sink: DiagnosticsSink,
) -> Callable[[DataFusionSubstraitFallbackEvent], None]:
    """Return a Substrait fallback hook that records diagnostics rows.

    Returns:
    -------
    Callable[[DataFusionSubstraitFallbackEvent], None]
        Hook that records Substrait fallback events in the diagnostics sink.
    """

    def _hook(event: DataFusionSubstraitFallbackEvent) -> None:
        recorder_sink = ensure_recorder_sink(sink, session_id=_RUNTIME_SESSION_ID)
        recorder_sink.record_events(
            "substrait_fallbacks_v1",
            [
                {
                    "event_time_unix_ms": int(time.time() * 1000),
                    "reason": event.reason,
                    "expr_type": event.expr_type,
                    "profile_hash": event.profile_hash,
                    "run_id": event.run_id,
                    "plan_fingerprint": event.plan_fingerprint,
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

    Returns:
    -------
    Callable[[str, ExplainRows], None]
        Hook that records explain rows in the diagnostics sink.
    """

    def _hook(sql: str, rows: ExplainRows) -> None:
        recorder_sink = ensure_recorder_sink(sink, session_id=_RUNTIME_SESSION_ID)
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

    Returns:
    -------
    Callable[[Mapping[str, object]], None]
        Hook that records plan artifacts in the diagnostics sink.
    """

    def _hook(payload: Mapping[str, object]) -> None:
        recorder_sink = ensure_recorder_sink(sink, session_id=_RUNTIME_SESSION_ID)
        normalized = dict(payload)
        if "plan_identity_hash" not in normalized:
            fingerprint_value = normalized.get("plan_fingerprint")
            if isinstance(fingerprint_value, str) and fingerprint_value:
                normalized["plan_identity_hash"] = fingerprint_value
            else:
                normalized["plan_identity_hash"] = "unknown_plan_identity"
        recorder_sink.record_artifact("datafusion_plan_artifacts_v9", normalized)

    return _hook


def diagnostics_semantic_diff_hook(
    sink: DiagnosticsSink,
) -> Callable[[Mapping[str, object]], None]:
    """Return a semantic diff hook that records diagnostics payloads.

    Returns:
    -------
    Callable[[Mapping[str, object]], None]
        Hook that records semantic diff diagnostics in the sink.
    """

    def _hook(payload: Mapping[str, object]) -> None:
        recorder_sink = ensure_recorder_sink(sink, session_id=_RUNTIME_SESSION_ID)
        recorder_sink.record_artifact("datafusion_semantic_diff_v1", payload)

    return _hook


def diagnostics_sql_ingest_hook(
    sink: DiagnosticsSink,
) -> Callable[[Mapping[str, object]], None]:
    """Return a SQL ingest hook that records diagnostics payloads.

    Returns:
    -------
    Callable[[Mapping[str, object]], None]
        Hook that records SQL ingest artifacts in the diagnostics sink.
    """

    def _hook(payload: Mapping[str, object]) -> None:
        recorder_sink = ensure_recorder_sink(sink, session_id=_RUNTIME_SESSION_ID)
        recorder_sink.record_artifact("datafusion_sql_ingest_v1", payload)

    return _hook


def diagnostics_arrow_ingest_hook(
    sink: DiagnosticsSink,
) -> Callable[[Mapping[str, object]], None]:
    """Return an Arrow ingest hook that records diagnostics payloads.

    Returns:
    -------
    Callable[[Mapping[str, object]], None]
        Hook that records Arrow ingestion artifacts in the diagnostics sink.
    """

    def _hook(payload: Mapping[str, object]) -> None:
        recorder_sink = ensure_recorder_sink(sink, session_id=_RUNTIME_SESSION_ID)
        recorder_sink.record_artifact("datafusion_arrow_ingest_v1", payload)

    return _hook


def diagnostics_dml_hook(
    sink: DiagnosticsSink,
) -> Callable[[Mapping[str, object]], None]:
    """Return a DML hook that records diagnostics payloads.

    Returns:
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
    params: Mapping[str, object] | None
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
    location: DatasetLocation
    expected_fingerprint: str | None
    actual_fingerprint: str | None
    schema_match: bool | None


def _resolve_prepared_statement_options(
    resolved: DataFusionCompileOptions,
) -> tuple[Mapping[str, str] | None, bool, bool | None]:
    prepared_param_types = resolved.prepared_param_types
    prepared_statements = resolved.prepared_statements
    dynamic_projection = resolved.dynamic_projection
    return prepared_param_types, prepared_statements, dynamic_projection


def _resolved_config_policy_for_profile(
    profile: DataFusionRuntimeProfile,
) -> DataFusionConfigPolicy | None:
    if profile.policies.config_policy is not None:
        return profile.policies.config_policy
    if profile.policies.config_policy_name is None:
        return DEFAULT_DF_POLICY
    return DATAFUSION_POLICY_PRESETS.get(
        profile.policies.config_policy_name,
        DEFAULT_DF_POLICY,
    )


def _resolved_schema_hardening_for_profile(
    profile: DataFusionRuntimeProfile,
) -> SchemaHardeningProfile | None:
    if profile.policies.schema_hardening is not None:
        return profile.policies.schema_hardening
    if profile.policies.schema_hardening_name is None:
        return None
    return SCHEMA_HARDENING_PRESETS.get(
        profile.policies.schema_hardening_name,
        SCHEMA_HARDENING_PRESETS["schema_hardening"],
    )


def _effective_catalog_autoload_for_profile(
    profile: DataFusionRuntimeProfile,
) -> tuple[str | None, str | None]:
    if not profile.catalog.catalog_auto_load_enabled:
        return (None, None)
    if (
        profile.catalog.catalog_auto_load_location is not None
        or profile.catalog.catalog_auto_load_format is not None
    ):
        return (
            profile.catalog.catalog_auto_load_location,
            profile.catalog.catalog_auto_load_format,
        )
    env_settings = _catalog_autoload_settings()
    return (
        env_settings.get("datafusion.catalog.location"),
        env_settings.get("datafusion.catalog.format"),
    )


def _delta_protocol_support_payload(
    profile: DataFusionRuntimeProfile,
) -> Mapping[str, object] | None:
    support = profile.policies.delta_protocol_support
    if support is None:
        return None
    return {
        "max_reader_version": support.max_reader_version,
        "max_writer_version": support.max_writer_version,
        "supported_reader_features": list(support.supported_reader_features),
        "supported_writer_features": list(support.supported_writer_features),
    }


def _build_telemetry_payload_row(profile: DataFusionRuntimeProfile) -> dict[str, object]:
    settings = profile.settings_payload()
    sql_policy_payload = None
    if profile.policies.sql_policy is not None:
        sql_policy_payload = {
            "allow_ddl": profile.policies.sql_policy.allow_ddl,
            "allow_dml": profile.policies.sql_policy.allow_dml,
            "allow_statements": profile.policies.sql_policy.allow_statements,
        }
    write_policy_payload = _datafusion_write_policy_payload(profile.policies.write_policy)
    delta_mutation_policy = profile.policies.delta_mutation_policy
    delta_mutation_policy_hash = (
        delta_mutation_policy.fingerprint() if delta_mutation_policy is not None else None
    )
    delta_mutation_policy_payload = None
    if delta_mutation_policy is not None:
        retry_policy = delta_mutation_policy.retry_policy
        delta_mutation_policy_payload = {
            "retry_policy": {
                "max_attempts": retry_policy.max_attempts,
                "base_delay_s": retry_policy.base_delay_s,
                "max_delay_s": retry_policy.max_delay_s,
                "retryable_errors": list(retry_policy.retryable_errors),
                "fatal_errors": list(retry_policy.fatal_errors),
            },
            "require_locking_provider": delta_mutation_policy.require_locking_provider,
            "locking_option_keys": list(delta_mutation_policy.locking_option_keys),
            "append_only": delta_mutation_policy.append_only,
        }
    parquet_read = _settings_by_prefix(settings, "datafusion.execution.parquet.")
    listing_table = _settings_by_prefix(settings, "datafusion.runtime.list_files_")
    parser_dialect = settings.get("datafusion.sql_parser.dialect")
    ansi_mode = _ansi_mode(settings)
    return {
        "version": TELEMETRY_PAYLOAD_VERSION,
        "profile_name": profile.policies.config_policy_name,
        "datafusion_version": datafusion.__version__,
        "architecture_version": profile.architecture_version,
        "sql_policy_name": profile.policies.sql_policy_name,
        "cache_profile_name": profile.policies.cache_profile_name,
        "session_config": _map_entries(settings),
        "settings_hash": profile.settings_hash(),
        "external_table_options": (
            _map_entries(profile.policies.external_table_options)
            if profile.policies.external_table_options
            else None
        ),
        "delta_store_policy_hash": delta_store_policy_hash(profile.policies.delta_store_policy),
        "delta_store_policy": _delta_store_policy_payload(profile.policies.delta_store_policy),
        "delta_mutation_policy_hash": delta_mutation_policy_hash,
        "delta_mutation_policy": delta_mutation_policy_payload,
        "sql_policy": sql_policy_payload,
        "param_identifier_allowlist": (
            list(profile.policies.param_identifier_allowlist)
            if profile.policies.param_identifier_allowlist
            else None
        ),
        "write_policy": write_policy_payload,
        "feature_gates": _map_entries(profile.policies.feature_gates.settings()),
        "cache_profile_settings": (
            _map_entries(_cache_profile_settings(profile))
            if profile.policies.cache_profile_name is not None
            else None
        ),
        "join_policy": (
            _map_entries(profile.policies.join_policy.settings())
            if profile.policies.join_policy is not None
            else None
        ),
        "parquet_read": _map_entries(parquet_read),
        "listing_table": _map_entries(listing_table),
        "spill": {
            "spill_dir": profile.execution.spill_dir,
            "memory_pool": profile.execution.memory_pool,
            "memory_limit_bytes": profile.execution.memory_limit_bytes,
        },
        "execution": {
            "target_partitions": profile.execution.target_partitions,
            "batch_size": profile.execution.batch_size,
            "repartition_aggregations": profile.execution.repartition_aggregations,
            "repartition_windows": profile.execution.repartition_windows,
            "repartition_file_scans": profile.execution.repartition_file_scans,
            "repartition_file_min_size": profile.execution.repartition_file_min_size,
        },
        "sql_surfaces": {
            "enable_information_schema": profile.catalog.enable_information_schema,
            "enable_ident_normalization": _effective_ident_normalization(profile),
            "force_disable_ident_normalization": (
                profile.features.force_disable_ident_normalization
            ),
            "enable_url_table": profile.features.enable_url_table,
            "sql_parser_dialect": parser_dialect,
            "ansi_mode": ansi_mode,
        },
        "extensions": {
            "delta_session_defaults_enabled": profile.features.enable_delta_session_defaults,
            "delta_runtime_env": {
                "max_spill_size": profile.execution.delta_max_spill_size,
                "max_temp_directory_size": profile.execution.delta_max_temp_directory_size,
            },
            "delta_querybuilder_enabled": profile.features.enable_delta_querybuilder,
            "delta_data_checker_enabled": profile.features.enable_delta_data_checker,
            "delta_plan_codecs_enabled": profile.features.enable_delta_plan_codecs,
            "delta_plan_codec_physical": profile.policies.delta_plan_codec_physical,
            "delta_plan_codec_logical": profile.policies.delta_plan_codec_logical,
            "snapshot_pinned_mode": profile.policies.snapshot_pinned_mode,
            "delta_ffi_provider_enforced": profile.features.enforce_delta_ffi_provider,
            "expr_planners_enabled": profile.features.enable_expr_planners,
            "expr_planner_names": list(profile.policies.expr_planner_names),
            "physical_expr_adapter_factory": bool(profile.policies.physical_expr_adapter_factory),
            "schema_evolution_adapter_enabled": (profile.features.enable_schema_evolution_adapter),
            "named_args_supported": named_args_supported(profile),
            "async_udfs_enabled": profile.features.enable_async_udfs,
            "async_udf_timeout_ms": profile.policies.async_udf_timeout_ms,
            "async_udf_batch_size": profile.policies.async_udf_batch_size,
        },
        "substrait_validation": profile.diagnostics.substrait_validation,
        "output_writes": {
            "cache_enabled": profile.features.cache_enabled,
            "cache_max_columns": profile.policies.cache_max_columns,
            "minimum_parallel_output_files": profile.execution.minimum_parallel_output_files,
            "soft_max_rows_per_output_file": profile.execution.soft_max_rows_per_output_file,
            "maximum_parallel_row_group_writers": profile.execution.maximum_parallel_row_group_writers,
            "objectstore_writer_buffer_size": profile.execution.objectstore_writer_buffer_size,
            "datafusion_write_policy": write_policy_payload,
        },
        "zero_row_bootstrap": profile.zero_row_bootstrap.fingerprint_payload(),
    }


def _runtime_settings_payload(profile: DataFusionRuntimeProfile) -> dict[str, str]:
    enable_ident_normalization = _effective_ident_normalization(profile)
    payload: dict[str, str] = {
        "datafusion.sql_parser.enable_ident_normalization": str(enable_ident_normalization).lower()
    }
    optional_values = {
        "datafusion.optimizer.repartition_aggregations": (
            profile.execution.repartition_aggregations
        ),
        "datafusion.optimizer.repartition_windows": profile.execution.repartition_windows,
        "datafusion.execution.repartition_file_scans": (profile.execution.repartition_file_scans),
        "datafusion.execution.repartition_file_min_size": (
            profile.execution.repartition_file_min_size
        ),
        "datafusion.execution.minimum_parallel_output_files": (
            profile.execution.minimum_parallel_output_files
        ),
        "datafusion.execution.soft_max_rows_per_output_file": (
            profile.execution.soft_max_rows_per_output_file
        ),
        "datafusion.execution.maximum_parallel_row_group_writers": (
            profile.execution.maximum_parallel_row_group_writers
        ),
        "datafusion.execution.objectstore_writer_buffer_size": (
            profile.execution.objectstore_writer_buffer_size
        ),
    }
    for key, value in optional_values.items():
        if value is None:
            continue
        if isinstance(value, bool):
            payload[key] = str(value).lower()
        else:
            payload[key] = str(value)
    return payload


def _effective_ident_normalization(profile: DataFusionRuntimeProfile) -> bool:
    if profile.features.force_disable_ident_normalization:
        return False
    if profile.features.enable_delta_session_defaults:
        return False
    return profile.features.enable_ident_normalization


def _extra_settings_payload(profile: DataFusionRuntimeProfile) -> dict[str, str]:
    payload: dict[str, str] = {}
    catalog_location, catalog_format = _effective_catalog_autoload_for_profile(profile)
    if catalog_location is not None:
        payload["datafusion.catalog.location"] = catalog_location
    if catalog_format is not None:
        payload["datafusion.catalog.format"] = catalog_format
    payload.update(profile.policies.feature_gates.settings())
    if profile.policies.join_policy is not None:
        payload.update(profile.policies.join_policy.settings())
    if profile.diagnostics.explain_analyze_level is not None and _supports_explain_analyze_level():
        payload["datafusion.explain.analyze_level"] = profile.diagnostics.explain_analyze_level
    return payload


def _cache_profile_settings(profile: DataFusionRuntimeProfile) -> dict[str, str]:
    name = profile.policies.cache_profile_name
    if name is None:
        return {}
    settings = CACHE_PROFILES.get(name)
    if settings is None:
        msg = f"Unknown cache profile name: {name!r}."
        raise ValueError(msg)
    return dict(settings)


class _RuntimeDiagnosticsMixin:
    def record_artifact(
        self, name: ArtifactSpec | str, payload: Mapping[str, object]
    ) -> None:
        """Record an artifact through DiagnosticsRecorder when configured."""
        profile = cast("DataFusionRuntimeProfile", self)
        record_artifact(profile, name, payload)

    def record_events(self, name: str, rows: Sequence[Mapping[str, object]]) -> None:
        """Record events through DiagnosticsRecorder when configured."""
        profile = cast("DataFusionRuntimeProfile", self)
        record_events(profile, name, rows)

    def view_registry_snapshot(self) -> list[dict[str, object]] | None:
        """Return a stable snapshot of recorded view definitions.

        Returns:
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

        Returns:
        -------
        dict[str, str]
            Resolved DataFusion settings payload.
        """
        profile = cast("DataFusionRuntimeProfile", self)
        resolved_policy = _resolved_config_policy_for_profile(profile)
        payload: dict[str, str] = (
            dict(resolved_policy.settings) if resolved_policy is not None else {}
        )
        if profile.policies.cache_policy is not None:
            payload.update(cache_policy_settings(profile.policies.cache_policy))
        payload.update(_cache_profile_settings(profile))
        resolved_schema_hardening = _resolved_schema_hardening_for_profile(profile)
        if resolved_schema_hardening is not None:
            payload.update(resolved_schema_hardening.settings())
        payload.update(_runtime_settings_payload(profile))
        if profile.policies.settings_overrides:
            payload.update(
                {str(key): str(value) for key, value in profile.policies.settings_overrides.items()}
            )
        payload.update(_extra_settings_payload(profile))
        return payload

    def settings_hash(self) -> str:
        """Return a stable hash for the SessionConfig settings payload.

        Returns:
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

    def fingerprint_payload(self) -> Mapping[str, object]:
        """Return a canonical fingerprint payload for the runtime profile.

        Uses existing settings and telemetry hashes to avoid re-serializing
        the full runtime profile surface.

        Returns:
        -------
        Mapping[str, object]
            Payload describing the runtime profile fingerprint inputs.
        """
        profile = cast("DataFusionRuntimeProfile", self)
        return {
            "version": 2,
            "architecture_version": profile.architecture_version,
            "settings_hash": profile.settings_hash(),
            "telemetry_hash": profile.telemetry_payload_hash(),
            "execution": profile.execution.fingerprint_payload(),
            "catalog": profile.catalog.fingerprint_payload(),
            "zero_row_bootstrap": profile.zero_row_bootstrap.fingerprint_payload(),
            "features": profile.features.fingerprint_payload(),
            "policies": profile.policies.fingerprint_payload(),
        }

    def fingerprint(self) -> str:
        """Return a stable fingerprint for the runtime profile.

        Returns:
        -------
        str
            Stable fingerprint for the runtime profile.
        """
        return config_fingerprint(self.fingerprint_payload())

    def telemetry_payload(self) -> dict[str, object]:
        """Return a diagnostics-friendly payload for the runtime profile.

        Returns:
        -------
        dict[str, object]
            Runtime settings serialized for telemetry/diagnostics.
        """
        profile = cast("DataFusionRuntimeProfile", self)
        resolved_policy = _resolved_config_policy_for_profile(profile)
        execution = profile.execution
        catalog = profile.catalog
        data_sources = profile.data_sources
        features = profile.features
        diagnostics = profile.diagnostics
        policies = profile.policies
        template_payloads: dict[str, object] = {}
        for name, location in sorted(data_sources.dataset_templates.items()):
            resolved = location.resolved
            scan = resolved.datafusion_scan
            scan_payload: dict[str, object] | None = None
            if scan is not None:
                scan_payload = {
                    "file_sort_order": [list(key) for key in scan.file_sort_order],
                    "partition_cols": [
                        {"name": col_name, "dtype": str(dtype)}
                        for col_name, dtype in scan.partition_cols_pyarrow()
                    ],
                    "schema_force_view_types": scan.schema_force_view_types,
                    "skip_arrow_metadata": scan.skip_arrow_metadata,
                    "listing_table_factory_infer_partitions": (
                        scan.listing_table_factory_infer_partitions
                    ),
                    "listing_table_ignore_subdirectory": (scan.listing_table_ignore_subdirectory),
                    "collect_statistics": scan.collect_statistics,
                    "meta_fetch_concurrency": scan.meta_fetch_concurrency,
                    "list_files_cache_limit": scan.list_files_cache_limit,
                    "list_files_cache_ttl": scan.list_files_cache_ttl,
                    "unbounded": scan.unbounded,
                }
            template_payloads[name] = {
                "path": str(location.path),
                "format": location.format,
                "datafusion_provider": resolved.datafusion_provider,
                "delta_version": location.delta_version,
                "delta_timestamp": location.delta_timestamp,
                "delta_constraints": list(resolved.delta_constraints)
                if resolved.delta_constraints
                else None,
                "scan": scan_payload,
            }
        return {
            "datafusion_version": datafusion.__version__,
            "target_partitions": execution.target_partitions,
            "batch_size": execution.batch_size,
            "spill_dir": execution.spill_dir,
            "memory_pool": execution.memory_pool,
            "memory_limit_bytes": execution.memory_limit_bytes,
            "default_catalog": catalog.default_catalog,
            "default_schema": catalog.default_schema,
            "view_catalog": (
                catalog.view_catalog_name or catalog.default_catalog
                if catalog.view_schema_name is not None
                else None
            ),
            "view_schema": catalog.view_schema_name,
            "enable_ident_normalization": _effective_ident_normalization(profile),
            "catalog_auto_load_location": catalog.catalog_auto_load_location,
            "catalog_auto_load_format": catalog.catalog_auto_load_format,
            "delta_store_policy_hash": delta_store_policy_hash(policies.delta_store_policy),
            "delta_store_policy": _delta_store_policy_payload(policies.delta_store_policy),
            "delta_mutation_policy_hash": (
                policies.delta_mutation_policy.fingerprint()
                if policies.delta_mutation_policy is not None
                else None
            ),
            "delta_mutation_policy": (
                {
                    "retry_policy": {
                        "max_attempts": policies.delta_mutation_policy.retry_policy.max_attempts,
                        "base_delay_s": policies.delta_mutation_policy.retry_policy.base_delay_s,
                        "max_delay_s": policies.delta_mutation_policy.retry_policy.max_delay_s,
                        "retryable_errors": list(
                            policies.delta_mutation_policy.retry_policy.retryable_errors
                        ),
                        "fatal_errors": list(
                            policies.delta_mutation_policy.retry_policy.fatal_errors
                        ),
                    },
                    "require_locking_provider": (
                        policies.delta_mutation_policy.require_locking_provider
                    ),
                    "locking_option_keys": list(policies.delta_mutation_policy.locking_option_keys),
                    "append_only": policies.delta_mutation_policy.append_only,
                }
                if policies.delta_mutation_policy is not None
                else None
            ),
            "dataset_templates": template_payloads or None,
            "enable_information_schema": catalog.enable_information_schema,
            "enable_url_table": features.enable_url_table,
            "cache_enabled": features.cache_enabled,
            "cache_max_columns": policies.cache_max_columns,
            "minimum_parallel_output_files": execution.minimum_parallel_output_files,
            "soft_max_rows_per_output_file": execution.soft_max_rows_per_output_file,
            "maximum_parallel_row_group_writers": execution.maximum_parallel_row_group_writers,
            "cache_manager_enabled": features.enable_cache_manager,
            "cache_manager_factory": bool(policies.cache_manager_factory),
            "function_factory_enabled": features.enable_function_factory,
            "function_factory_hook": bool(policies.function_factory_hook),
            "expr_planners_enabled": features.enable_expr_planners,
            "expr_planner_hook": bool(policies.expr_planner_hook),
            "expr_planner_names": list(policies.expr_planner_names),
            "enable_udfs": features.enable_udfs,
            "enable_async_udfs": features.enable_async_udfs,
            "async_udf_timeout_ms": policies.async_udf_timeout_ms,
            "async_udf_batch_size": policies.async_udf_batch_size,
            "physical_expr_adapter_factory": bool(policies.physical_expr_adapter_factory),
            "delta_session_defaults_enabled": features.enable_delta_session_defaults,
            "delta_querybuilder_enabled": features.enable_delta_querybuilder,
            "delta_data_checker_enabled": features.enable_delta_data_checker,
            "delta_plan_codecs_enabled": features.enable_delta_plan_codecs,
            "delta_plan_codec_physical": policies.delta_plan_codec_physical,
            "delta_plan_codec_logical": policies.delta_plan_codec_logical,
            "delta_ffi_provider_enforced": features.enforce_delta_ffi_provider,
            "metrics_enabled": features.enable_metrics,
            "metrics_collector": bool(diagnostics.metrics_collector),
            "tracing_enabled": features.enable_tracing,
            "tracing_hook": bool(diagnostics.tracing_hook),
            "tracing_collector": bool(diagnostics.tracing_collector),
            "capture_explain": diagnostics.capture_explain,
            "explain_verbose": diagnostics.explain_verbose,
            "explain_analyze": diagnostics.explain_analyze,
            "explain_analyze_threshold_ms": diagnostics.explain_analyze_threshold_ms,
            "explain_analyze_level": diagnostics.explain_analyze_level,
            "explain_collector": bool(diagnostics.explain_collector),
            "capture_plan_artifacts": diagnostics.capture_plan_artifacts,
            "capture_semantic_diff": diagnostics.capture_semantic_diff,
            "plan_collector": bool(diagnostics.plan_collector),
            "substrait_validation": diagnostics.substrait_validation,
            "diagnostics_sink": bool(diagnostics.diagnostics_sink),
            "local_filesystem_root": policies.local_filesystem_root,
            "plan_artifacts_root": policies.plan_artifacts_root,
            "input_plugins": len(policies.input_plugins),
            "prepared_statements": [stmt.name for stmt in policies.prepared_statements],
            "runtime_env_hook": bool(policies.runtime_env_hook),
            "config_policy_name": policies.config_policy_name,
            "schema_hardening_name": policies.schema_hardening_name,
            "config_policy": dict(resolved_policy.settings)
            if resolved_policy is not None
            else None,
            "sql_policy_name": policies.sql_policy_name,
            "cache_profile_name": policies.cache_profile_name,
            "sql_policy": (
                {
                    "allow_ddl": policies.sql_policy.allow_ddl,
                    "allow_dml": policies.sql_policy.allow_dml,
                    "allow_statements": policies.sql_policy.allow_statements,
                }
                if policies.sql_policy is not None
                else None
            ),
            "param_identifier_allowlist": (
                list(policies.param_identifier_allowlist)
                if policies.param_identifier_allowlist
                else None
            ),
            "external_table_options": dict(policies.external_table_options)
            if policies.external_table_options
            else None,
            "write_policy": _datafusion_write_policy_payload(policies.write_policy),
            "settings_overrides": dict(policies.settings_overrides),
            "feature_gates": policies.feature_gates.settings(),
            "cache_profile_settings": (
                _cache_profile_settings(profile)
                if policies.cache_profile_name is not None
                else None
            ),
            "join_policy": (
                policies.join_policy.settings() if policies.join_policy is not None else None
            ),
            "settings_hash": profile.settings_hash(),
            "share_context": execution.share_context,
            "session_context_key": execution.session_context_key,
            "zero_row_bootstrap": profile.zero_row_bootstrap.fingerprint_payload(),
        }

    def telemetry_payload_v1(self) -> dict[str, object]:
        """Return a versioned runtime payload for diagnostics.

        Returns:
        -------
        dict[str, object]
            Versioned runtime payload with grouped settings.
        """
        profile = cast("DataFusionRuntimeProfile", self)
        settings = profile.settings_payload()
        ansi_mode = _ansi_mode(settings)
        parser_dialect = settings.get("datafusion.sql_parser.dialect")
        execution = profile.execution
        catalog = profile.catalog
        features = profile.features
        diagnostics = profile.diagnostics
        policies = profile.policies
        return {
            "version": 2,
            "profile_name": policies.config_policy_name,
            "datafusion_version": datafusion.__version__,
            "schema_hardening_name": policies.schema_hardening_name,
            "sql_policy_name": policies.sql_policy_name,
            "cache_profile_name": policies.cache_profile_name,
            "session_config": dict(settings),
            "settings_hash": profile.settings_hash(),
            "external_table_options": dict(policies.external_table_options)
            if policies.external_table_options
            else None,
            "sql_policy": (
                {
                    "allow_ddl": policies.sql_policy.allow_ddl,
                    "allow_dml": policies.sql_policy.allow_dml,
                    "allow_statements": policies.sql_policy.allow_statements,
                }
                if policies.sql_policy is not None
                else None
            ),
            "param_identifier_allowlist": (
                list(policies.param_identifier_allowlist)
                if policies.param_identifier_allowlist
                else None
            ),
            "write_policy": _datafusion_write_policy_payload(policies.write_policy),
            "feature_gates": dict(policies.feature_gates.settings()),
            "cache_profile_settings": (
                _cache_profile_settings(profile)
                if policies.cache_profile_name is not None
                else None
            ),
            "join_policy": policies.join_policy.settings()
            if policies.join_policy is not None
            else None,
            "parquet_read": _settings_by_prefix(settings, "datafusion.execution.parquet."),
            "listing_table": _settings_by_prefix(settings, "datafusion.runtime.list_files_"),
            "spill": {
                "spill_dir": execution.spill_dir,
                "memory_pool": execution.memory_pool,
                "memory_limit_bytes": execution.memory_limit_bytes,
            },
            "execution": {
                "target_partitions": execution.target_partitions,
                "batch_size": execution.batch_size,
                "repartition_aggregations": execution.repartition_aggregations,
                "repartition_windows": execution.repartition_windows,
                "repartition_file_scans": execution.repartition_file_scans,
                "repartition_file_min_size": execution.repartition_file_min_size,
            },
            "sql_surfaces": {
                "enable_information_schema": catalog.enable_information_schema,
                "enable_ident_normalization": _effective_ident_normalization(profile),
                "enable_url_table": features.enable_url_table,
                "sql_parser_dialect": parser_dialect,
                "ansi_mode": ansi_mode,
            },
            "extensions": {
                "delta_session_defaults_enabled": features.enable_delta_session_defaults,
                "delta_runtime_env": {
                    "max_spill_size": execution.delta_max_spill_size,
                    "max_temp_directory_size": execution.delta_max_temp_directory_size,
                },
                "delta_querybuilder_enabled": features.enable_delta_querybuilder,
                "delta_data_checker_enabled": features.enable_delta_data_checker,
                "delta_plan_codecs_enabled": features.enable_delta_plan_codecs,
                "delta_plan_codec_physical": policies.delta_plan_codec_physical,
                "delta_plan_codec_logical": policies.delta_plan_codec_logical,
                "snapshot_pinned_mode": policies.snapshot_pinned_mode,
                "delta_protocol_mode": policies.delta_protocol_mode,
                "delta_protocol_support": _delta_protocol_support_payload(profile),
                "expr_planners_enabled": features.enable_expr_planners,
                "expr_planner_names": list(policies.expr_planner_names),
                "physical_expr_adapter_factory": bool(policies.physical_expr_adapter_factory),
                "schema_evolution_adapter_enabled": features.enable_schema_evolution_adapter,
                "named_args_supported": named_args_supported(profile),
                "async_udfs_enabled": features.enable_async_udfs,
                "async_udf_timeout_ms": policies.async_udf_timeout_ms,
                "async_udf_batch_size": policies.async_udf_batch_size,
            },
            "substrait_validation": diagnostics.substrait_validation,
            "output_writes": {
                "cache_enabled": features.cache_enabled,
                "cache_max_columns": policies.cache_max_columns,
                "minimum_parallel_output_files": execution.minimum_parallel_output_files,
                "soft_max_rows_per_output_file": execution.soft_max_rows_per_output_file,
                "maximum_parallel_row_group_writers": (
                    execution.maximum_parallel_row_group_writers
                ),
                "objectstore_writer_buffer_size": execution.objectstore_writer_buffer_size,
                "datafusion_write_policy": _datafusion_write_policy_payload(policies.write_policy),
            },
            "zero_row_bootstrap": profile.zero_row_bootstrap.fingerprint_payload(),
        }

    def telemetry_payload_msgpack(self) -> bytes:
        """Return a MessagePack-encoded telemetry payload.

        Returns:
        -------
        bytes
            MessagePack payload for runtime telemetry.
        """
        return _encode_telemetry_msgpack(self.telemetry_payload_v1())

    def telemetry_payload_hash(self) -> str:
        """Return a stable hash for the versioned telemetry payload.

        Returns:
        -------
        str
            SHA-256 hash of the telemetry payload.
        """
        profile = cast("DataFusionRuntimeProfile", self)
        return payload_hash(_build_telemetry_payload_row(profile), _TELEMETRY_SCHEMA)


@dataclass(frozen=True)
class SessionRuntime:
    """Authoritative runtime surface for planning and execution."""

    ctx: SessionContext
    profile: DataFusionRuntimeProfile
    udf_snapshot_hash: str
    udf_rewrite_tags: tuple[str, ...]
    domain_planner_names: tuple[str, ...]
    udf_snapshot: Mapping[str, object]
    df_settings: Mapping[str, str]


_SESSION_RUNTIME_CACHE: dict[str, SessionRuntime] = {}
_RUNTIME_SETTINGS_OVERLAY: WeakKeyDictionary[SessionContext, dict[str, str]] = WeakKeyDictionary()

_SESSION_RUNTIME_HASH_VERSION = 1


def _settings_rows_to_mapping(rows: Sequence[Mapping[str, object]]) -> dict[str, str]:
    """Build a name/value settings mapping from introspection rows.

    Parameters
    ----------
    rows
        Settings rows from information_schema snapshots.

    Returns:
    -------
    dict[str, str]
        Mapping of setting names to stringified values.
    """
    mapping: dict[str, str] = {}
    for row in rows:
        name = row.get("name") or row.get("setting_name") or row.get("key")
        if name is None:
            continue
        value = row.get("value")
        mapping[str(name)] = "" if value is None else str(value)
    return mapping


def _build_session_runtime_from_context(
    ctx: SessionContext,
    *,
    profile: DataFusionRuntimeProfile,
) -> SessionRuntime:
    from datafusion_engine.expr.domain_planner import domain_planner_names_from_snapshot
    from datafusion_engine.udf.catalog import rewrite_tag_index
    from datafusion_engine.udf.runtime import rust_udf_snapshot, rust_udf_snapshot_hash

    snapshot = rust_udf_snapshot(ctx)
    snapshot_hash = rust_udf_snapshot_hash(snapshot)
    tag_index = rewrite_tag_index(snapshot)
    rewrite_tags = tuple(sorted(tag_index))
    planner_names = domain_planner_names_from_snapshot(snapshot)
    df_settings: Mapping[str, str]
    try:
        settings_table = settings_snapshot_for_profile(profile, ctx)
        df_settings = _settings_rows_to_mapping(settings_table.to_pylist())
    except (RuntimeError, TypeError, ValueError):
        df_settings = {}
    return SessionRuntime(
        ctx=ctx,
        profile=profile,
        udf_snapshot_hash=snapshot_hash,
        udf_rewrite_tags=rewrite_tags,
        domain_planner_names=planner_names,
        udf_snapshot=snapshot,
        df_settings=df_settings,
    )


def _settings_row_name(row: Mapping[str, object]) -> str | None:
    value = row.get("name") or row.get("setting_name") or row.get("key")
    if value is None:
        return None
    return str(value)


def _settings_name_key(schema: pa.Schema) -> str:
    for name in ("name", "setting_name", "key"):
        if name in schema.names:
            return name
    return "name"


def _settings_value_key(schema: pa.Schema) -> str:
    for name in ("value", "setting_value"):
        if name in schema.names:
            return name
    return "value"


def _settings_row_template(schema: pa.Schema) -> dict[str, object]:
    return {field.name: None for field in schema}


def _runtime_settings_from_profile(
    profile: DataFusionRuntimeProfile | None,
) -> dict[str, str]:
    if profile is None:
        return {}
    return {
        key: value
        for key, value in profile.settings_payload().items()
        if key.startswith("datafusion.runtime.")
    }


def record_runtime_setting_override(
    ctx: SessionContext,
    *,
    key: str,
    value: str,
) -> None:
    """Record runtime settings that DataFusion does not surface via SQL."""
    if not key.startswith("datafusion.runtime."):
        return
    overrides = _RUNTIME_SETTINGS_OVERLAY.setdefault(ctx, {})
    overrides[key] = value


def runtime_setting_overrides(ctx: SessionContext) -> Mapping[str, str]:
    """Return recorded runtime setting overrides for a SessionContext.

    Returns:
    -------
    Mapping[str, str]
        Runtime setting overrides keyed by setting name.
    """
    overrides = _RUNTIME_SETTINGS_OVERLAY.get(ctx)
    return dict(overrides) if overrides else {}


def _merge_runtime_settings_rows(
    rows: list[dict[str, object]],
    *,
    schema: pa.Schema,
    profile: DataFusionRuntimeProfile | None,
    ctx: SessionContext,
) -> list[dict[str, object]]:
    name_key = _settings_name_key(schema)
    value_key = _settings_value_key(schema)
    name_to_row = {name: row for row in rows if (name := _settings_row_name(row)) is not None}
    defaults = _runtime_settings_from_profile(profile)
    for key, value in defaults.items():
        if key in name_to_row:
            continue
        row = _settings_row_template(schema)
        row[name_key] = key
        row[value_key] = value
        rows.append(row)
        name_to_row[key] = row
    overrides = runtime_setting_overrides(ctx)
    for key, value in overrides.items():
        row = name_to_row.get(key)
        if row is None:
            row = _settings_row_template(schema)
            rows.append(row)
        row[name_key] = key
        row[value_key] = value
        name_to_row[key] = row
    return rows


def build_session_runtime(
    profile: DataFusionRuntimeProfile, *, use_cache: bool = True
) -> SessionRuntime:
    """Build and cache a planning-ready SessionRuntime for a profile.

    Parameters
    ----------
    profile
        DataFusion runtime profile to materialize.
    use_cache
        When ``True``, cache the runtime by the profile cache key.

    Returns:
    -------
    SessionRuntime
        Planning-ready runtime with UDF identity and settings snapshots.
    """
    cache_key = profile.context_cache_key()
    cached = _SESSION_RUNTIME_CACHE.get(cache_key)
    ctx = profile.session_context()
    # Guard against cache-key collisions across profile variants.
    if cached is not None and use_cache and cached.profile == profile and cached.ctx is ctx:
        return cached
    from datafusion_engine.expr.domain_planner import domain_planner_names_from_snapshot
    from datafusion_engine.udf.catalog import rewrite_tag_index
    from datafusion_engine.udf.runtime import rust_udf_snapshot, rust_udf_snapshot_hash

    snapshot = rust_udf_snapshot(ctx)
    snapshot_hash = rust_udf_snapshot_hash(snapshot)
    tag_index = rewrite_tag_index(snapshot)
    rewrite_tags = tuple(sorted(tag_index))
    planner_names = domain_planner_names_from_snapshot(snapshot)
    df_settings: Mapping[str, str]
    try:
        settings_table = settings_snapshot_for_profile(profile, ctx)
        df_settings = _settings_rows_to_mapping(settings_table.to_pylist())
    except (RuntimeError, TypeError, ValueError):
        df_settings = {}
    runtime = SessionRuntime(
        ctx=ctx,
        profile=profile,
        udf_snapshot_hash=snapshot_hash,
        udf_rewrite_tags=rewrite_tags,
        domain_planner_names=planner_names,
        udf_snapshot=snapshot,
        df_settings=df_settings,
    )
    if use_cache:
        _SESSION_RUNTIME_CACHE[cache_key] = runtime
    return runtime


def refresh_session_runtime(
    profile: DataFusionRuntimeProfile,
    *,
    ctx: SessionContext | None = None,
) -> SessionRuntime:
    """Rebuild and cache a SessionRuntime for the current SessionContext.

    Args:
        profile: Runtime profile to refresh.
        ctx: Optional context to rebuild for.

    Returns:
        SessionRuntime: Result.

    Raises:
        TypeError: If the profile does not expose a session runtime builder.
    """
    resolved_ctx = ctx or profile.session_context()
    runtime_builder = cast(
        "Callable[[SessionContext], SessionRuntime] | None",
        getattr(profile, "_session_runtime_from_context", None),
    )
    if not callable(runtime_builder):
        msg = "DataFusionRuntimeProfile does not expose a session runtime builder."
        raise TypeError(msg)
    runtime = runtime_builder(resolved_ctx)
    _SESSION_RUNTIME_CACHE[profile.context_cache_key()] = runtime
    return runtime


def _session_runtime_entries(mapping: Mapping[str, str]) -> list[dict[str, str]]:
    entries: list[dict[str, str]] = []
    for key, value in sorted(mapping.items(), key=lambda item: item[0]):
        entries.append(
            {
                "key": str(key),
                "value_kind": type(value).__name__,
                "value": str(value),
            }
        )
    return entries


def session_runtime_hash(runtime: SessionRuntime) -> str:
    """Return a stable hash for session runtime identity.

    Parameters
    ----------
    runtime
        Planning-ready session runtime snapshot.

    Returns:
    -------
    str
        Stable identity hash for runtime-sensitive plan signatures.
    """
    profile_context_key = runtime.profile.context_cache_key()
    profile_settings_hash = runtime.profile.settings_hash()
    df_entries = _session_runtime_entries(runtime.df_settings)
    payload = {
        "version": _SESSION_RUNTIME_HASH_VERSION,
        "profile_context_key": profile_context_key,
        "profile_settings_hash": profile_settings_hash,
        "udf_snapshot_hash": runtime.udf_snapshot_hash,
        "udf_rewrite_tags": list(runtime.udf_rewrite_tags),
        "domain_planner_names": list(runtime.domain_planner_names),
        "df_settings_entries": df_entries,
    }
    return payload_hash(payload, _SESSION_RUNTIME_HASH_SCHEMA)


@dataclass(frozen=True)
class _PlannerRuleInstallers:
    config_installer: Callable[[SessionContext, bool, bool, bool], None]
    physical_config_installer: Callable[[SessionContext, bool], None]
    rule_installer: Callable[[SessionContext], None]
    physical_installer: Callable[[SessionContext], None]


def _resolve_planner_rule_installers() -> _PlannerRuleInstallers | None:
    imported_any = False
    for module_name in ("datafusion._internal", "datafusion_ext"):
        try:
            candidate = importlib.import_module(module_name)
        except ImportError:
            continue
        imported_any = True
        config_installer = getattr(candidate, "install_codeanatomy_policy_config", None)
        physical_config_installer = getattr(candidate, "install_codeanatomy_physical_config", None)
        rule_installer = getattr(candidate, "install_planner_rules", None)
        if not (
            callable(config_installer)
            and callable(physical_config_installer)
            and callable(rule_installer)
        ):
            continue
        physical_installer = getattr(candidate, "install_physical_rules", None)
        if not callable(physical_installer):
            msg = "Physical rule installer is unavailable in the DataFusion extension module."
            raise TypeError(msg)
        return _PlannerRuleInstallers(
            config_installer=cast(
                "Callable[[SessionContext, bool, bool, bool], None]",
                config_installer,
            ),
            physical_config_installer=cast(
                "Callable[[SessionContext, bool], None]",
                physical_config_installer,
            ),
            rule_installer=cast("Callable[[SessionContext], None]", rule_installer),
            physical_installer=cast("Callable[[SessionContext], None]", physical_installer),
        )
    if not imported_any:  # pragma: no cover - optional dependency
        msg = "Planner policy rules require datafusion._internal or datafusion_ext."
        raise RuntimeError(msg)
    return None


class ExecutionConfig(StructBaseStrict, frozen=True):
    """Execution-level DataFusion settings."""

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
    delta_max_spill_size: int | None = None
    delta_max_temp_directory_size: int | None = None
    share_context: bool = True
    session_context_key: str | None = None

    def fingerprint_payload(self) -> Mapping[str, object]:
        """Return fingerprint payload for execution settings.

        Returns:
        -------
        Mapping[str, object]
            Payload describing execution-level configuration values.
        """
        return {
            "target_partitions": self.target_partitions,
            "batch_size": self.batch_size,
            "repartition_aggregations": self.repartition_aggregations,
            "repartition_windows": self.repartition_windows,
            "repartition_file_scans": self.repartition_file_scans,
            "repartition_file_min_size": self.repartition_file_min_size,
            "minimum_parallel_output_files": self.minimum_parallel_output_files,
            "soft_max_rows_per_output_file": self.soft_max_rows_per_output_file,
            "maximum_parallel_row_group_writers": self.maximum_parallel_row_group_writers,
            "objectstore_writer_buffer_size": self.objectstore_writer_buffer_size,
            "spill_dir": self.spill_dir,
            "memory_pool": self.memory_pool,
            "memory_limit_bytes": self.memory_limit_bytes,
            "delta_max_spill_size": self.delta_max_spill_size,
            "delta_max_temp_directory_size": self.delta_max_temp_directory_size,
            "share_context": self.share_context,
            "session_context_key": self.session_context_key,
        }


class CatalogConfig(StructBaseStrict, frozen=True):
    """Catalog and schema configuration."""

    default_catalog: str = "datafusion"
    default_schema: str = "public"
    view_catalog_name: str | None = None
    view_schema_name: str | None = "views"
    registry_catalogs: Mapping[str, DatasetCatalog] = msgspec.field(default_factory=dict)
    registry_catalog_name: str | None = None
    catalog_auto_load_location: str | None = None
    catalog_auto_load_format: str | None = None
    catalog_auto_load_enabled: bool = True
    enable_information_schema: bool = True

    def fingerprint_payload(self) -> Mapping[str, object]:
        """Return fingerprint payload for catalog settings.

        Returns:
        -------
        Mapping[str, object]
            Payload describing catalog configuration values.
        """
        registry_names = tuple(sorted(self.registry_catalogs))
        return {
            "default_catalog": self.default_catalog,
            "default_schema": self.default_schema,
            "view_catalog_name": self.view_catalog_name,
            "view_schema_name": self.view_schema_name,
            "registry_catalogs": list(registry_names),
            "registry_catalog_name": self.registry_catalog_name,
            "catalog_auto_load_location": self.catalog_auto_load_location,
            "catalog_auto_load_format": self.catalog_auto_load_format,
            "catalog_auto_load_enabled": self.catalog_auto_load_enabled,
            "enable_information_schema": self.enable_information_schema,
        }


class ExtractOutputConfig(StructBaseStrict, frozen=True):
    """Extract output configuration."""

    dataset_locations: Mapping[str, DatasetLocation] = msgspec.field(default_factory=dict)
    output_catalog_name: str | None = None
    output_root: str | None = None
    scip_dataset_locations: Mapping[str, DatasetLocation] = msgspec.field(default_factory=dict)


class SemanticOutputConfig(StructBaseStrict, frozen=True):
    """Semantic output configuration."""

    locations: Mapping[str, DatasetLocation] = msgspec.field(default_factory=dict)
    output_catalog_name: str | None = None
    output_root: str | None = None
    normalize_output_root: str | None = None
    cache_overrides: Mapping[str, CachePolicy] = msgspec.field(default_factory=dict)


class DataSourceConfig(StructBaseStrict, frozen=True):
    """Dataset location and output configuration."""

    dataset_templates: Mapping[str, DatasetLocation] = msgspec.field(default_factory=dict)
    extract_output: ExtractOutputConfig = msgspec.field(default_factory=ExtractOutputConfig)
    semantic_output: SemanticOutputConfig = msgspec.field(default_factory=SemanticOutputConfig)
    cdf_cursor_store: CdfCursorStore | None = None


class ZeroRowBootstrapConfig(StructBaseStrict, frozen=True):
    """Zero-row bootstrap configuration for runtime validation and setup."""

    validation_mode: Literal["off", "bootstrap"] = "off"
    include_semantic_outputs: bool = True
    include_internal_tables: bool = True
    strict: bool = True
    allow_semantic_row_probe_fallback: bool = False
    bootstrap_mode: Literal["strict_zero_rows", "seeded_minimal_rows"] = "strict_zero_rows"
    seeded_datasets: tuple[str, ...] = ()

    def fingerprint_payload(self) -> Mapping[str, object]:
        """Return fingerprint payload for zero-row bootstrap settings.

        Returns:
        -------
        Mapping[str, object]
            Payload describing zero-row bootstrap behavior.
        """
        return {
            "validation_mode": self.validation_mode,
            "include_semantic_outputs": self.include_semantic_outputs,
            "include_internal_tables": self.include_internal_tables,
            "strict": self.strict,
            "allow_semantic_row_probe_fallback": self.allow_semantic_row_probe_fallback,
            "bootstrap_mode": self.bootstrap_mode,
            "seeded_datasets": list(self.seeded_datasets),
        }


class FeatureGatesConfig(StructBaseStrict, frozen=True):
    """Feature toggle configuration."""

    enable_ident_normalization: bool = False
    force_disable_ident_normalization: bool = False
    enable_url_table: bool = False
    cache_enabled: bool = False
    enable_cache_manager: bool = False
    enable_function_factory: bool = True
    enable_schema_registry: bool = True
    enable_expr_planners: bool = True
    enable_schema_evolution_adapter: bool = True
    enable_udfs: bool = True
    enable_async_udfs: bool = False
    enable_delta_cdf: bool = False
    enable_delta_session_defaults: bool = False
    enable_delta_querybuilder: bool = False
    enable_delta_data_checker: bool = False
    enable_delta_plan_codecs: bool = False
    enforce_delta_ffi_provider: bool = True
    enable_metrics: bool = False
    enable_tracing: bool = False
    enforce_preflight: bool = True

    def fingerprint_payload(self) -> Mapping[str, object]:
        """Return fingerprint payload for feature gate settings.

        Returns:
        -------
        Mapping[str, object]
            Payload describing feature gate values.
        """
        return {
            "enable_ident_normalization": self.enable_ident_normalization,
            "force_disable_ident_normalization": self.force_disable_ident_normalization,
            "enable_url_table": self.enable_url_table,
            "cache_enabled": self.cache_enabled,
            "enable_cache_manager": self.enable_cache_manager,
            "enable_function_factory": self.enable_function_factory,
            "enable_schema_registry": self.enable_schema_registry,
            "enable_expr_planners": self.enable_expr_planners,
            "enable_schema_evolution_adapter": self.enable_schema_evolution_adapter,
            "enable_udfs": self.enable_udfs,
            "enable_async_udfs": self.enable_async_udfs,
            "enable_delta_cdf": self.enable_delta_cdf,
            "enable_delta_session_defaults": self.enable_delta_session_defaults,
            "enable_delta_querybuilder": self.enable_delta_querybuilder,
            "enable_delta_data_checker": self.enable_delta_data_checker,
            "enable_delta_plan_codecs": self.enable_delta_plan_codecs,
            "enforce_delta_ffi_provider": self.enforce_delta_ffi_provider,
            "enable_metrics": self.enable_metrics,
            "enable_tracing": self.enable_tracing,
            "enforce_preflight": self.enforce_preflight,
        }


class DiagnosticsConfig(StructBaseStrict, frozen=True):
    """Diagnostics, explain, and observability settings."""

    capture_explain: bool = True
    explain_verbose: bool = False
    explain_analyze: bool = True
    explain_analyze_threshold_ms: float | None = None
    explain_analyze_level: str | None = None
    explain_collector: _DataFusionExplainCollector | None = msgspec.field(
        default_factory=_DataFusionExplainCollector
    )
    capture_plan_artifacts: bool = True
    capture_semantic_diff: bool = False
    emit_semantic_quality_diagnostics: bool = True
    plan_collector: _DataFusionPlanCollector | None = msgspec.field(
        default_factory=_DataFusionPlanCollector
    )
    diagnostics_sink: DiagnosticsSink | None = None
    labeled_explains: list[dict[str, object]] = msgspec.field(default_factory=list)
    metrics_collector: Callable[[], Mapping[str, object] | None] | None = None
    tracing_hook: Callable[[SessionContext], None] | None = None
    tracing_collector: Callable[[], Mapping[str, object] | None] | None = None
    substrait_validation: bool = False
    validate_plan_determinism: bool = False
    strict_determinism: bool = False


class PolicyBundleConfig(StructBaseStrict, frozen=True):
    """Policy, hook, and cache configuration."""

    config_policy_name: str | None = "symtable"
    config_policy: DataFusionConfigPolicy | None = None
    cache_policy: CachePolicyConfig | None = None
    cache_profile_name: (
        Literal[
            "snapshot_pinned",
            "always_latest_ttl30s",
            "multi_tenant_strict",
        ]
        | None
    ) = None
    scan_policy: ScanPolicyConfig = msgspec.field(default_factory=ScanPolicyConfig)
    schema_hardening_name: str | None = "schema_hardening"
    schema_hardening: SchemaHardeningProfile | None = None
    sql_policy_name: str | None = "write"
    sql_policy: DataFusionSqlPolicy | None = None
    param_identifier_allowlist: tuple[str, ...] = ()
    external_table_options: Mapping[str, object] = msgspec.field(default_factory=dict)
    write_policy: DataFusionWritePolicy | None = None
    settings_overrides: Mapping[str, str] = msgspec.field(default_factory=dict)
    feature_gates: DataFusionFeatureGates = msgspec.field(default_factory=DataFusionFeatureGates)
    physical_rulepack_enabled: bool = True
    join_policy: DataFusionJoinPolicy | None = None
    cache_max_columns: int | None = 64
    cache_manager_factory: Callable[[], object] | None = None
    function_factory_hook: Callable[[SessionContext], None] | None = None
    expr_planner_names: tuple[str, ...] = ("codeanatomy_domain",)
    expr_planner_hook: Callable[[SessionContext], None] | None = None
    physical_expr_adapter_factory: object | None = None
    schema_adapter_factories: Mapping[str, object] = msgspec.field(default_factory=dict)
    udf_catalog_policy: Literal["default", "strict"] = "default"
    async_udf_timeout_ms: int | None = None
    async_udf_batch_size: int | None = None
    delta_plan_codec_physical: str = "delta_physical"
    delta_plan_codec_logical: str = "delta_logical"
    delta_store_policy: DeltaStorePolicy | None = None
    delta_mutation_policy: DeltaMutationPolicy | None = None
    delta_protocol_support: DeltaProtocolSupport | None = None
    delta_protocol_mode: Literal["error", "warn", "ignore"] = "error"
    diskcache_profile: DiskCacheProfile | None = msgspec.field(
        default_factory=default_diskcache_profile
    )
    snapshot_pinned_mode: Literal["off", "delta_version"] = "off"
    cache_output_root: str | None = None
    runtime_artifact_cache_enabled: bool = False
    runtime_artifact_cache_root: str | None = None
    metadata_cache_snapshot_enabled: bool = False
    local_filesystem_root: str | None = None
    plan_artifacts_root: str | None = None
    input_plugins: tuple[Callable[[SessionContext], None], ...] = ()
    prepared_statements: tuple[PreparedStatementSpec, ...] = INFO_SCHEMA_STATEMENTS
    runtime_env_hook: Callable[[RuntimeEnvBuilder], RuntimeEnvBuilder] | None = None

    @staticmethod
    def _callable_identity(value: object | None) -> str | None:
        if value is None:
            return None
        qualname = getattr(value, "__qualname__", None) or getattr(value, "__name__", None)
        module = getattr(value, "__module__", None)
        if qualname and module:
            return f"{module}.{qualname}"
        cls = getattr(value, "__class__", None)
        if cls is None:
            return None
        return f"{cls.__module__}.{cls.__name__}"

    @staticmethod
    def _diskcache_profile_payload(
        profile: DiskCacheProfile | None,
    ) -> Mapping[str, object] | None:
        if profile is None:
            return None
        overrides = {
            str(kind): settings.fingerprint_payload()
            for kind, settings in sorted(profile.overrides.items(), key=lambda item: str(item[0]))
        }
        ttl_seconds = {
            str(kind): profile.ttl_seconds.get(kind)
            for kind in sorted(profile.ttl_seconds, key=str)
        }
        return {
            "root": str(profile.root),
            "base_settings": profile.base_settings.fingerprint_payload(),
            "overrides": overrides,
            "ttl_seconds": ttl_seconds,
        }

    @staticmethod
    def _schema_adapter_payload(
        factories: Mapping[str, object],
    ) -> Mapping[str, object]:
        return {
            str(name): PolicyBundleConfig._callable_identity(factory)
            for name, factory in sorted(factories.items(), key=lambda item: str(item[0]))
        }

    @staticmethod
    def _prepared_statement_payload(
        statements: tuple[PreparedStatementSpec, ...],
    ) -> list[dict[str, object]]:
        return [
            {
                "name": statement.name,
                "sql": statement.sql,
                "param_types": list(statement.param_types),
            }
            for statement in statements
        ]

    def fingerprint_payload(self) -> Mapping[str, object]:
        """Return fingerprint payload for policy bundle settings.

        Returns:
        -------
        Mapping[str, object]
            Payload describing policy bundle settings.
        """
        external_table_options = {
            str(key): str(value) for key, value in self.external_table_options.items()
        }
        settings_overrides = {
            str(key): str(value) for key, value in self.settings_overrides.items()
        }
        schema_hardening_payload = (
            self.schema_hardening.fingerprint_payload()
            if self.schema_hardening is not None
            else None
        )
        return {
            "config_policy_name": self.config_policy_name,
            "config_policy": (
                self.config_policy.fingerprint() if self.config_policy is not None else None
            ),
            "scan_policy": self.scan_policy.fingerprint(),
            "cache_policy": (
                self.cache_policy.fingerprint() if self.cache_policy is not None else None
            ),
            "cache_profile_name": self.cache_profile_name,
            "schema_hardening_name": self.schema_hardening_name,
            "schema_hardening": schema_hardening_payload,
            "sql_policy_name": self.sql_policy_name,
            "sql_policy": self.sql_policy.fingerprint() if self.sql_policy is not None else None,
            "param_identifier_allowlist": list(self.param_identifier_allowlist),
            "external_table_options": external_table_options,
            "write_policy": (
                self.write_policy.fingerprint() if self.write_policy is not None else None
            ),
            "settings_overrides": settings_overrides,
            "feature_gates": self.feature_gates.fingerprint_payload(),
            "physical_rulepack_enabled": self.physical_rulepack_enabled,
            "join_policy": self.join_policy.fingerprint() if self.join_policy is not None else None,
            "cache_max_columns": self.cache_max_columns,
            "cache_manager_factory": self._callable_identity(self.cache_manager_factory),
            "function_factory_hook": self._callable_identity(self.function_factory_hook),
            "expr_planner_names": list(self.expr_planner_names),
            "expr_planner_hook": self._callable_identity(self.expr_planner_hook),
            "physical_expr_adapter_factory": self._callable_identity(
                self.physical_expr_adapter_factory
            ),
            "schema_adapter_factories": self._schema_adapter_payload(self.schema_adapter_factories),
            "udf_catalog_policy": self.udf_catalog_policy,
            "async_udf_timeout_ms": self.async_udf_timeout_ms,
            "async_udf_batch_size": self.async_udf_batch_size,
            "delta_plan_codec_physical": self.delta_plan_codec_physical,
            "delta_plan_codec_logical": self.delta_plan_codec_logical,
            "delta_store_policy": (
                self.delta_store_policy.fingerprint()
                if self.delta_store_policy is not None
                else None
            ),
            "delta_mutation_policy": (
                self.delta_mutation_policy.fingerprint()
                if self.delta_mutation_policy is not None
                else None
            ),
            "delta_protocol_support": (
                {
                    "max_reader_version": self.delta_protocol_support.max_reader_version,
                    "max_writer_version": self.delta_protocol_support.max_writer_version,
                    "supported_reader_features": list(
                        self.delta_protocol_support.supported_reader_features
                    ),
                    "supported_writer_features": list(
                        self.delta_protocol_support.supported_writer_features
                    ),
                }
                if self.delta_protocol_support is not None
                else None
            ),
            "delta_protocol_mode": self.delta_protocol_mode,
            "diskcache_profile": self._diskcache_profile_payload(self.diskcache_profile),
            "snapshot_pinned_mode": self.snapshot_pinned_mode,
            "cache_output_root": self.cache_output_root,
            "runtime_artifact_cache_enabled": self.runtime_artifact_cache_enabled,
            "runtime_artifact_cache_root": self.runtime_artifact_cache_root,
            "metadata_cache_snapshot_enabled": self.metadata_cache_snapshot_enabled,
            "local_filesystem_root": self.local_filesystem_root,
            "plan_artifacts_root": self.plan_artifacts_root,
            "input_plugins": [self._callable_identity(plugin) for plugin in self.input_plugins],
            "prepared_statements": self._prepared_statement_payload(self.prepared_statements),
            "runtime_env_hook": self._callable_identity(self.runtime_env_hook),
        }

    def fingerprint(self) -> str:
        """Return fingerprint for policy bundle settings.

        Returns:
        -------
        str
            Deterministic fingerprint for the policy bundle.
        """
        return config_fingerprint(self.fingerprint_payload())


@dataclass(frozen=True)
class RuntimeProfileDeltaOps:
    """Delta-specific runtime operations bound to a profile."""

    profile: DataFusionRuntimeProfile

    def delta_runtime_ctx(self) -> SessionContext:
        """Return a SessionContext configured for Delta operations.

        Returns:
        -------
        SessionContext
            Session context configured for Delta operations.
        """
        return self.profile.session_context()

    def delta_service(self) -> DeltaService:
        """Return the Delta service bound to this runtime profile.

        Returns:
        -------
        DeltaService
            Delta service bound to the runtime profile.
        """
        from datafusion_engine.delta.service import DeltaService

        return DeltaService(profile=self.profile)

    def reserve_delta_commit(
        self,
        *,
        key: str,
        metadata: Mapping[str, object] | None = None,
        commit_metadata: Mapping[str, str] | None = None,
    ) -> tuple[IdempotentWriteOptions, DataFusionRun]:
        """Reserve the next idempotent commit version for a Delta write.

        Returns:
        -------
        tuple[IdempotentWriteOptions, DataFusionRun]
            Commit options plus the updated run context.
        """
        run = self.profile.delta_commit_runs.get(key)
        if run is None:
            from obs.datafusion_runs import create_run_context

            base_metadata: dict[str, str] = {"key": key}
            if metadata:
                base_metadata.update(
                    {str(item_key): str(item_value) for item_key, item_value in metadata.items()}
                )
            run = create_run_context(
                label="delta_commit",
                sink=self.profile.diagnostics.diagnostics_sink,
                metadata=base_metadata,
            )
            self.profile.delta_commit_runs[key] = run
        elif metadata:
            run.metadata.update(dict(metadata))
        if commit_metadata:
            run.metadata["commit_metadata"] = dict(commit_metadata)
        options, updated = run.next_commit_version()
        if self.profile.diagnostics.diagnostics_sink is not None:
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
            self.profile.record_artifact("datafusion_delta_commit_v1", payload)
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
        self.profile.delta_commit_runs[key] = run
        if self.profile.diagnostics.diagnostics_sink is None:
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
        self.profile.record_artifact("datafusion_delta_commit_v1", payload)

    def ensure_delta_plan_codecs(self, ctx: SessionContext) -> bool:
        """Install Delta plan codecs when enabled.

        Returns:
        -------
        bool
            True when codecs are installed.
        """
        if not self.profile.features.enable_delta_plan_codecs:
            return False
        available, installed = self.profile.install_delta_plan_codecs(ctx)
        self.profile.record_delta_plan_codecs_event(
            available=available,
            installed=installed,
        )
        return installed


@dataclass(frozen=True)
class RuntimeProfileIO:
    """Runtime I/O helpers for cache and ephemeral contexts."""

    profile: DataFusionRuntimeProfile

    def cache_root(self) -> str:
        """Return the root directory for Delta-backed caches.

        Returns:
        -------
        str
            Cache root directory.
        """
        if self.profile.policies.cache_output_root is not None:
            return self.profile.policies.cache_output_root
        return str(Path(tempfile.gettempdir()) / "datafusion_cache")

    def runtime_artifact_root(self) -> str:
        """Return the root directory for runtime artifact cache tables.

        Returns:
        -------
        str
            Runtime artifact cache root.
        """
        if self.profile.policies.runtime_artifact_cache_root is not None:
            return self.profile.policies.runtime_artifact_cache_root
        return str(Path(self.cache_root()) / "runtime_artifacts")

    def metadata_cache_snapshot_root(self) -> str:
        """Return the root directory for metadata cache snapshots.

        Returns:
        -------
        str
            Metadata cache snapshot root.
        """
        return str(Path(self.cache_root()) / "metadata_cache_snapshots")

    def ephemeral_context(self) -> SessionContext:
        """Return a non-cached SessionContext configured from the profile.

        Returns:
        -------
        SessionContext
            Ephemeral session context configured for this profile.
        """
        ctx = self.profile.build_ephemeral_context()
        RegistrationPhaseOrchestrator().run(self.profile.ephemeral_context_phases(ctx))
        return ctx


@dataclass(frozen=True)
class RuntimeProfileCatalog:
    """Catalog helpers bound to a runtime profile."""

    profile: DataFusionRuntimeProfile

    def ast_dataset_location(self) -> DatasetLocation | None:
        """Return the configured AST dataset location, when available.

        Returns:
        -------
        DatasetLocation | None
            AST dataset location when configured.
        """
        return self.profile.resolve_ast_dataset_location()

    def bytecode_dataset_location(self) -> DatasetLocation | None:
        """Return the configured bytecode dataset location, when available.

        Returns:
        -------
        DatasetLocation | None
            Bytecode dataset location when configured.
        """
        return self.profile.resolve_bytecode_dataset_location()

    def extract_dataset_location(self, name: str) -> DatasetLocation | None:
        """Return a configured extract dataset location for the dataset name.

        Returns:
        -------
        DatasetLocation | None
            Dataset location when configured.
        """
        location = self.profile.resolve_dataset_template(name)
        if location is None:
            location = extract_output_locations_for_profile(self.profile).get(name)
        if location is None:
            location = self.profile.data_sources.extract_output.scip_dataset_locations.get(name)
        if location is None:
            return None
        if location.dataset_spec is None:
            from datafusion_engine.extract.registry import dataset_spec as extract_dataset_spec

            try:
                spec = extract_dataset_spec(name)
            except KeyError:
                spec = None
            if spec is not None:
                location = msgspec.structs.replace(location, dataset_spec=spec)
        return location

    def dataset_location(self, name: str) -> DatasetLocation | None:
        """Return a configured dataset location for the dataset name.

        Returns:
        -------
        DatasetLocation | None
            Dataset location when configured.
        """
        location = self.extract_dataset_location(name)
        if location is not None:
            resolved = apply_delta_store_policy(
                location,
                policy=self.profile.policies.delta_store_policy,
            )
            if self.profile.policies.scan_policy is None:
                return resolved
            from datafusion_engine.dataset.policies import apply_scan_policy_defaults
            from datafusion_engine.dataset.registry import (
                DatasetLocationOverrides,
                resolve_dataset_policies,
            )

            policies = resolve_dataset_policies(resolved, overrides=resolved.overrides)
            datafusion_scan, delta_scan = apply_scan_policy_defaults(
                dataset_format=resolved.format or "delta",
                datafusion_scan=policies.datafusion_scan,
                delta_scan=policies.delta_scan,
                policy=self.profile.policies.scan_policy,
            )
            if datafusion_scan is None and delta_scan is None:
                return resolved
            overrides = resolved.overrides or DatasetLocationOverrides()
            if datafusion_scan is not None:
                overrides = msgspec.structs.replace(overrides, datafusion_scan=datafusion_scan)
            if delta_scan is not None:
                delta_bundle = policies.delta_bundle
                if delta_bundle is None:
                    from schema_spec.system import DeltaPolicyBundle as _DeltaPolicyBundle

                    delta_bundle = _DeltaPolicyBundle(scan=delta_scan)
                else:
                    delta_bundle = msgspec.structs.replace(delta_bundle, scan=delta_scan)
                overrides = msgspec.structs.replace(overrides, delta=delta_bundle)
            return msgspec.structs.replace(resolved, overrides=overrides)
        from semantics.compile_context import dataset_bindings_for_profile

        bindings = dataset_bindings_for_profile(self.profile)
        return bindings.location(name)

    def dataset_location_or_raise(self, name: str) -> DatasetLocation:
        """Return a configured dataset location for the dataset name.

        Args:
            name: Description.

        Raises:
            KeyError: If the operation cannot be completed.
        """
        location = self.dataset_location(name)
        if location is None:
            msg = f"No dataset location configured for {name!r}."
            raise KeyError(msg)
        return location


class _RuntimeProfileIOFacadeMixin:
    """Facade methods for runtime-profile I/O operations."""

    def cache_root(self) -> str:
        """Return the configured cache root directory.

        Returns:
        -------
        str
            Cache root directory.
        """
        profile = cast("DataFusionRuntimeProfile", self)
        return profile.io_ops.cache_root()


class _RuntimeProfileCatalogFacadeMixin:
    """Facade methods for runtime-profile catalog operations."""

    def dataset_location(self, name: str) -> DatasetLocation | None:
        """Return a configured dataset location for the dataset name.

        Returns:
        -------
        DatasetLocation | None
            Dataset location when configured.
        """
        profile = cast("DataFusionRuntimeProfile", self)
        from semantics.compile_context import dataset_bindings_for_profile

        return dataset_bindings_for_profile(profile).location(name)


class _RuntimeProfileDeltaFacadeMixin:
    """Facade methods for runtime-profile Delta operations."""

    def delta_service(self) -> DeltaService:
        """Return a DeltaService bound to this runtime profile.

        Returns:
        -------
        DeltaService
            DeltaService instance bound to this profile.
        """
        from datafusion_engine.delta.service import delta_service_for_profile

        return delta_service_for_profile(cast("DataFusionRuntimeProfile", self))


class DataFusionRuntimeProfile(
    _RuntimeProfileIOFacadeMixin,
    _RuntimeProfileCatalogFacadeMixin,
    _RuntimeProfileDeltaFacadeMixin,
    _RuntimeDiagnosticsMixin,
    StructBaseStrict,
    frozen=True,
):
    """DataFusion runtime configuration.

    Identifier normalization is disabled by default to preserve case-sensitive
    identifiers, and URL-table support is disabled unless explicitly enabled
    for development or controlled file-path queries.
    """

    architecture_version: str = "v2"
    execution: ExecutionConfig = msgspec.field(default_factory=ExecutionConfig)
    catalog: CatalogConfig = msgspec.field(default_factory=CatalogConfig)
    data_sources: DataSourceConfig = msgspec.field(default_factory=DataSourceConfig)
    zero_row_bootstrap: ZeroRowBootstrapConfig = msgspec.field(
        default_factory=ZeroRowBootstrapConfig
    )
    features: FeatureGatesConfig = msgspec.field(default_factory=FeatureGatesConfig)
    diagnostics: DiagnosticsConfig = msgspec.field(default_factory=DiagnosticsConfig)
    policies: PolicyBundleConfig = msgspec.field(default_factory=PolicyBundleConfig)
    view_registry: DataFusionViewRegistry | None = msgspec.field(
        default_factory=DataFusionViewRegistry
    )
    plan_cache: PlanCache | None = None
    plan_proto_cache: PlanProtoCache | None = None
    udf_catalog_cache: dict[int, UdfCatalog] = msgspec.field(default_factory=dict)
    delta_commit_runs: dict[str, DataFusionRun] = msgspec.field(default_factory=dict)

    @property
    def delta_ops(self) -> RuntimeProfileDeltaOps:
        """Return Delta runtime operations bound to this profile.

        Returns:
        -------
        RuntimeProfileDeltaOps
            Delta operations helper.
        """
        return RuntimeProfileDeltaOps(self)

    @property
    def io_ops(self) -> RuntimeProfileIO:
        """Return I/O helpers bound to this profile.

        Returns:
        -------
        RuntimeProfileIO
            I/O operations helper.
        """
        return RuntimeProfileIO(self)

    @property
    def catalog_ops(self) -> RuntimeProfileCatalog:
        """Return catalog helpers bound to this profile.

        Returns:
        -------
        RuntimeProfileCatalog
            Catalog operations helper.
        """
        return RuntimeProfileCatalog(self)

    def _validate_information_schema(self) -> None:
        if not self.catalog.enable_information_schema:
            msg = "information_schema must be enabled for DataFusion sessions."
            raise ValueError(msg)

    def _validate_catalog_names(self) -> None:
        if (
            self.catalog.registry_catalog_name is not None
            and self.catalog.registry_catalog_name != self.catalog.default_catalog
        ):
            msg = (
                "registry_catalog_name must match default_catalog; "
                "custom catalog inference is not supported."
            )
            raise ValueError(msg)
        if (
            self.catalog.view_catalog_name is not None
            and self.catalog.view_catalog_name != self.catalog.default_catalog
        ):
            msg = (
                "view_catalog_name must match default_catalog; "
                "custom catalog inference is not supported."
            )
            raise ValueError(msg)

    def _resolve_plan_cache(self) -> PlanCache:
        if self.plan_cache is not None:
            return self.plan_cache
        return PlanCache(cache_profile=self.policies.diskcache_profile)

    def _resolve_plan_proto_cache(self) -> PlanProtoCache:
        if self.plan_proto_cache is not None:
            return self.plan_proto_cache
        return PlanProtoCache(cache_profile=self.policies.diskcache_profile)

    def _resolve_diagnostics_sink(self) -> DiagnosticsSink | None:
        if self.diagnostics.diagnostics_sink is None:
            return None
        return ensure_recorder_sink(
            self.diagnostics.diagnostics_sink,
            session_id=self.context_cache_key(),
        )

    def __post_init__(self) -> None:
        """Initialize defaults after dataclass construction.

        Raises:
            ValueError: If the operation cannot be completed.
        """
        self._validate_information_schema()
        self._validate_catalog_names()
        plan_cache = self._resolve_plan_cache()
        if self.plan_cache is None:
            object.__setattr__(self, "plan_cache", plan_cache)
        plan_proto_cache = self._resolve_plan_proto_cache()
        if self.plan_proto_cache is None:
            object.__setattr__(self, "plan_proto_cache", plan_proto_cache)
        diagnostics_sink = self._resolve_diagnostics_sink()
        if diagnostics_sink is not None:
            object.__setattr__(
                self,
                "diagnostics",
                msgspec.structs.replace(
                    self.diagnostics,
                    diagnostics_sink=diagnostics_sink,
                ),
            )
        async_policy = self._validate_async_udf_policy()
        if not async_policy["valid"]:
            msg = f"Async UDF policy invalid: {async_policy['errors']}."
            raise ValueError(msg)

    def _session_config(self) -> SessionConfig:
        """Return a SessionConfig configured from the profile.

        Returns:
        -------
        datafusion.SessionConfig
            Session configuration for the profile.
        """
        return SessionFactory(self).build_config()

    def _effective_catalog_autoload(self) -> tuple[str | None, str | None]:
        return _effective_catalog_autoload_for_profile(self)

    def _effective_ident_normalization(self) -> bool:
        return _effective_ident_normalization(self)

    @staticmethod
    def _supports_explain_analyze_level() -> bool:
        return _supports_explain_analyze_level()

    def runtime_env_builder(self) -> RuntimeEnvBuilder:
        """Return a RuntimeEnvBuilder configured from the profile.

        Returns:
        -------
        datafusion.RuntimeEnvBuilder
            Runtime environment builder for the profile.
        """
        builder = RuntimeEnvBuilder()
        if self.execution.spill_dir is not None:
            builder = _apply_builder(
                builder,
                method="with_disk_manager_specified",
                args=(self.execution.spill_dir,),
            )
            builder = _apply_builder(
                builder,
                method="with_temp_file_path",
                args=(self.execution.spill_dir,),
            )
        if self.execution.memory_limit_bytes is not None:
            limit = int(self.execution.memory_limit_bytes)
            if self.execution.memory_pool == "fair":
                builder = _apply_builder(
                    builder,
                    method="with_fair_spill_pool",
                    args=(limit,),
                )
            elif self.execution.memory_pool == "greedy":
                builder = _apply_builder(
                    builder,
                    method="with_greedy_memory_pool",
                    args=(limit,),
                )
        builder = _attach_cache_manager(
            builder,
            enabled=self.features.enable_cache_manager,
            factory=self.policies.cache_manager_factory,
        )
        if self.policies.runtime_env_hook is not None:
            builder = self.policies.runtime_env_hook(builder)
        return builder

    def _delta_runtime_env_options(self) -> _DeltaRuntimeEnvOptions | None:
        """Return delta-specific RuntimeEnv options when configured.

        Raises:
            RuntimeError: If the operation cannot be completed.
            TypeError: If the operation cannot be completed.
        """
        if (
            self.execution.delta_max_spill_size is None
            and self.execution.delta_max_temp_directory_size is None
        ):
            return None
        module = _resolve_extension_module(required_attr="DeltaRuntimeEnvOptions")
        if module is None:
            msg = "Delta runtime env options require datafusion._internal or datafusion_ext."
            raise RuntimeError(msg)
        options_cls = getattr(module, "DeltaRuntimeEnvOptions", None)
        if not callable(options_cls):
            msg = "Delta runtime env options type is unavailable in the extension module."
            raise TypeError(msg)
        options = cast("_DeltaRuntimeEnvOptions", options_cls())
        if self.execution.delta_max_spill_size is not None:
            options.max_spill_size = int(self.execution.delta_max_spill_size)
        if self.execution.delta_max_temp_directory_size is not None:
            options.max_temp_directory_size = int(self.execution.delta_max_temp_directory_size)
        return options

    def session_context(self) -> SessionContext:
        """Return a SessionContext configured from the profile.

        Use session_runtime() for planning to ensure UDF and settings
        snapshots are captured deterministically.

        Returns:
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
        self._install_view_schema(ctx)
        self._install_udf_platform(ctx)
        self._install_planner_rules(ctx)
        self._install_schema_registry(ctx)
        self._validate_rule_function_allowlist(ctx)
        self._prepare_statements(ctx)
        self.delta_ops.ensure_delta_plan_codecs(ctx)
        self._record_extension_parity_validation(ctx)
        self._install_physical_expr_adapter_factory(ctx)
        self._install_tracing(ctx)
        self._install_cache_tables(ctx)
        self._record_cache_diagnostics(ctx)
        self._cache_context(ctx)
        return ctx

    def build_ephemeral_context(self) -> SessionContext:
        """Return a non-cached SessionContext configured from the profile.

        Returns:
        -------
        SessionContext
            Ephemeral session context configured for this profile.
        """
        return self._apply_url_table(self._build_session_context())

    def ephemeral_context_phases(
        self,
        ctx: SessionContext,
    ) -> tuple[RegistrationPhase, ...]:
        """Return registration phases for ephemeral contexts.

        Returns:
        -------
        tuple[RegistrationPhase, ...]
            Registration phases for ephemeral contexts.
        """
        return self._ephemeral_context_phases(ctx)

    def install_delta_plan_codecs(self, ctx: SessionContext) -> tuple[bool, bool]:
        """Install Delta plan codecs using the extension or context fallback.

        Returns:
        -------
        tuple[bool, bool]
            Tuple of (available, installed) flags.
        """
        available, installed = self._install_delta_plan_codecs_extension(ctx)
        if not available:
            available, installed = self._install_delta_plan_codecs_context(ctx)
        return available, installed

    def record_delta_plan_codecs_event(self, *, available: bool, installed: bool) -> None:
        """Record the Delta plan codecs install status."""
        self._record_delta_plan_codecs(available=available, installed=installed)

    def resolve_dataset_template(self, name: str) -> DatasetLocation | None:
        """Return a dataset location template for the name.

        Returns:
        -------
        DatasetLocation | None
            Template dataset location when configured.
        """
        return self._dataset_template(name)

    def resolve_ast_dataset_location(self) -> DatasetLocation | None:
        """Return the configured AST dataset location.

        Returns:
        -------
        DatasetLocation | None
            AST dataset location when configured.
        """
        return self._ast_dataset_location()

    def resolve_bytecode_dataset_location(self) -> DatasetLocation | None:
        """Return the configured bytecode dataset location.

        Returns:
        -------
        DatasetLocation | None
            Bytecode dataset location when configured.
        """
        return self._bytecode_dataset_location()

    def _delta_runtime_profile_ctx(
        self,
        *,
        storage_options: Mapping[str, str] | None = None,
    ) -> SessionContext:
        """Return a SessionContext for Delta operations with storage overrides.

        Parameters
        ----------
        storage_options
            Optional storage options used to disable shared context reuse.

        Returns:
        -------
        datafusion.SessionContext
            Session context configured for Delta operations.
        """
        if storage_options:
            return msgspec.structs.replace(
                self,
                execution=msgspec.structs.replace(
                    self.execution,
                    share_context=False,
                ),
            ).delta_ops.delta_runtime_ctx()
        return self.delta_ops.delta_runtime_ctx()

    def _session_runtime_from_context(self, ctx: SessionContext) -> SessionRuntime:
        """Build a SessionRuntime from an existing SessionContext.

        Avoids re-entering session_context while still capturing snapshots.

        Returns:
        -------
        SessionRuntime
            Planning-ready session runtime for the provided context.
        """
        return _build_session_runtime_from_context(ctx, profile=self)

    def session_runtime(self) -> SessionRuntime:
        """Return a planning-ready SessionRuntime for the profile.

        Returns:
        -------
        SessionRuntime
            Planning-ready session runtime.
        """
        return build_session_runtime(self, use_cache=True)

    def run_zero_row_bootstrap_validation(
        self,
        request: ZeroRowBootstrapRequest | None = None,
        *,
        ctx: SessionContext | None = None,
    ) -> ZeroRowBootstrapReport:
        """Run zero-row bootstrap materialization and validation.

        Parameters
        ----------
        request
            Optional explicit bootstrap request. When omitted, runtime
            configuration from ``zero_row_bootstrap`` is used.
        ctx
            Optional context override. When omitted, the profile session
            context is created and reused for bootstrap operations.

        Returns:
        -------
        ZeroRowBootstrapReport
            Structured report describing bootstrap execution and validation.
        """
        from datafusion_engine.bootstrap.zero_row import (
            ZeroRowBootstrapRequest as BootstrapRequest,
        )
        from datafusion_engine.bootstrap.zero_row import (
            run_zero_row_bootstrap_validation as run_bootstrap_validation,
        )
        from semantics.compile_context import build_semantic_execution_context

        resolved_request = request or BootstrapRequest(
            include_semantic_outputs=self.zero_row_bootstrap.include_semantic_outputs,
            include_internal_tables=self.zero_row_bootstrap.include_internal_tables,
            strict=self.zero_row_bootstrap.strict,
            allow_semantic_row_probe_fallback=(
                self.zero_row_bootstrap.allow_semantic_row_probe_fallback
            ),
            bootstrap_mode=self.zero_row_bootstrap.bootstrap_mode,
            seeded_datasets=self.zero_row_bootstrap.seeded_datasets,
        )
        active_ctx = ctx or self.session_context()
        semantic_ctx = build_semantic_execution_context(
            runtime_profile=self,
            ctx=active_ctx,
            policy=(
                "schema_plus_runtime_probe"
                if resolved_request.allow_semantic_row_probe_fallback
                else "schema_plus_optional_probe"
            ),
        )
        manifest = semantic_ctx.manifest
        self.record_artifact("semantic_program_manifest_v1", manifest.payload())
        report = run_bootstrap_validation(
            self,
            request=resolved_request,
            ctx=active_ctx,
            manifest=manifest,
        )
        self.record_artifact("zero_row_bootstrap_validation_v1", report.payload())
        if report.events:
            self.record_events(
                "zero_row_bootstrap_events_v1",
                [event.payload() for event in report.events],
            )
        return report

    def context_pool(
        self,
        *,
        size: int = 1,
        run_name_prefix: str = "__run",
    ) -> DataFusionContextPool:
        """Return a pooled SessionContext manager for isolated run execution.

        Returns:
        -------
        DataFusionContextPool
            Reusable context pool configured for this runtime profile.
        """
        from datafusion_engine.session.factory import DataFusionContextPool

        return DataFusionContextPool(
            self,
            size=size,
            run_name_prefix=run_name_prefix,
        )

    def _ephemeral_context_phases(
        self,
        ctx: SessionContext,
    ) -> tuple[RegistrationPhase, ...]:
        return (
            RegistrationPhase(name="context", validate=lambda: None),
            RegistrationPhase(
                name="filesystems",
                requires=("context",),
                validate=lambda: self._register_local_filesystem(ctx),
            ),
            RegistrationPhase(
                name="catalogs",
                requires=("filesystems",),
                validate=lambda: self._install_catalogs_for_context(ctx),
            ),
            RegistrationPhase(
                name="udf_stack",
                requires=("catalogs",),
                validate=lambda: self._install_udf_stack_for_context(ctx),
            ),
            RegistrationPhase(
                name="schema_guards",
                requires=("udf_stack",),
                validate=lambda: self._install_schema_guards_for_context(ctx),
            ),
            RegistrationPhase(
                name="planning_extensions",
                requires=("schema_guards",),
                validate=lambda: self._install_planning_extensions_for_context(ctx),
            ),
            RegistrationPhase(
                name="extension_hooks",
                requires=("planning_extensions",),
                validate=lambda: self._install_extension_hooks_for_context(ctx),
            ),
            RegistrationPhase(
                name="observability",
                requires=("extension_hooks",),
                validate=lambda: self._install_observability_for_context(ctx),
            ),
        )

    def _install_catalogs_for_context(self, ctx: SessionContext) -> None:
        self._install_input_plugins(ctx)
        self._install_registry_catalogs(ctx)
        self._install_view_schema(ctx)

    def _install_udf_stack_for_context(self, ctx: SessionContext) -> None:
        self._install_udf_platform(ctx)
        self._install_planner_rules(ctx)

    def _install_schema_guards_for_context(self, ctx: SessionContext) -> None:
        self._install_schema_registry(ctx)
        self._validate_rule_function_allowlist(ctx)

    def _install_planning_extensions_for_context(self, ctx: SessionContext) -> None:
        self._prepare_statements(ctx)
        self.delta_ops.ensure_delta_plan_codecs(ctx)

    def _install_extension_hooks_for_context(self, ctx: SessionContext) -> None:
        self._record_extension_parity_validation(ctx)
        self._install_physical_expr_adapter_factory(ctx)

    def _install_observability_for_context(self, ctx: SessionContext) -> None:
        self._install_tracing(ctx)
        self._install_cache_tables(ctx)
        self._record_cache_diagnostics(ctx)

    def _validate_async_udf_policy(self) -> dict[str, object]:
        """Validate async UDF policy configuration.

        Returns:
        -------
        dict[str, object]
            Validation report with status and configuration details.
        """
        errors: list[str] = []
        if self.features.enable_async_udfs and not self.features.enable_udfs:
            errors.append("Async UDFs require enable_udfs to be True.")
        if not self.features.enable_async_udfs and (
            self.policies.async_udf_timeout_ms is not None
            or self.policies.async_udf_batch_size is not None
        ):
            errors.append("Async UDF settings provided while async UDFs are disabled.")
        if self.features.enable_async_udfs:
            if (
                self.policies.async_udf_timeout_ms is None
                or self.policies.async_udf_timeout_ms <= 0
            ):
                errors.append("async_udf_timeout_ms must be a positive integer.")
            if (
                self.policies.async_udf_batch_size is None
                or self.policies.async_udf_batch_size <= 0
            ):
                errors.append("async_udf_batch_size must be a positive integer.")
        return {
            "valid": not errors,
            "enable_async_udfs": self.features.enable_async_udfs,
            "async_udf_timeout_ms": self.policies.async_udf_timeout_ms,
            "async_udf_batch_size": self.policies.async_udf_batch_size,
            "errors": errors,
        }

    def _validate_named_args_extension_parity(self) -> dict[str, object]:
        """Validate that named-arg support aligns with extension capabilities.

        This method checks whether the Python-side configuration for named arguments
        is consistent with the available Rust extension capabilities.

        Returns:
        -------
        dict[str, object]
            Validation report with status and details.
        """
        warnings: list[str] = []

        # Check if function factory is enabled but expr planners are not
        if self.features.enable_function_factory and not self.features.enable_expr_planners:
            warnings.append(
                "FunctionFactory enabled without ExprPlanners; "
                "named arguments may not be supported in SQL."
            )

        # Check if expr planners are configured
        valid = True
        if (
            self.features.enable_expr_planners
            and not self.policies.expr_planner_names
            and not self.policies.expr_planner_hook
        ):
            valid = False
            warnings.append("ExprPlanners enabled but no planner names or hook configured.")

        return {
            "valid": valid,
            "enable_function_factory": self.features.enable_function_factory,
            "enable_expr_planners": self.features.enable_expr_planners,
            "expr_planner_names": list(self.policies.expr_planner_names),
            "named_args_supported": named_args_supported(self),
            "warnings": warnings,
        }

    def _validate_udf_info_schema_parity(self, ctx: SessionContext) -> dict[str, object]:
        """Validate that Rust UDFs appear in information_schema.

        Args:
            ctx: Description.

        Returns:
            dict[str, object]: Result.

        Raises:
            ValueError: If the operation cannot be completed.
        """
        if not self.catalog.enable_information_schema:
            return {
                "missing_in_information_schema": [],
                "routines_available": False,
                "error": "information_schema disabled",
            }
        from datafusion_engine.udf.parity import udf_info_schema_parity_report
        from datafusion_engine.udf.runtime import fallback_udfs_active

        if fallback_udfs_active(ctx):
            return {
                "missing_in_information_schema": [],
                "routines_available": False,
                "error": None,
            }

        report = udf_info_schema_parity_report(ctx)
        if report.error is not None:
            msg = f"information_schema parity check failed: {report.error}"
            raise ValueError(msg)
        if report.missing_in_information_schema:
            msg = (
                "information_schema parity check failed; "
                f"missing routines: {list(report.missing_in_information_schema)}"
            )
            raise ValueError(msg)
        return report.payload()

    def _install_input_plugins(self, ctx: SessionContext) -> None:
        """Install input plugins on the session context."""
        for plugin in self.policies.input_plugins:
            plugin(ctx)

    def _install_registry_catalogs(self, ctx: SessionContext) -> None:
        """Install registry-backed catalog providers on the session context."""
        if not self.catalog.registry_catalogs:
            return
        from datafusion_engine.catalog.provider import (
            register_registry_catalogs,
        )

        catalog_name = self.catalog.registry_catalog_name or self.catalog.default_catalog
        register_registry_catalogs(
            ctx,
            catalogs=self.catalog.registry_catalogs,
            catalog_name=catalog_name,
            default_schema=self.catalog.default_schema,
            runtime_profile=self,
        )

    def _install_view_schema(self, ctx: SessionContext) -> None:
        """Install the view schema namespace when configured."""
        if self.catalog.view_schema_name is None:
            return
        catalog_name = self.catalog.view_catalog_name or self.catalog.default_catalog
        try:
            catalog = ctx.catalog(catalog_name)
        except (KeyError, RuntimeError, TypeError, ValueError):
            return
        try:
            existing_schema = catalog.schema(self.catalog.view_schema_name)
        except KeyError:
            existing_schema = None
        if existing_schema is not None:
            return
        from datafusion.catalog import Schema

        catalog.register_schema(self.catalog.view_schema_name, Schema.memory_schema())

    def _install_udf_platform(self, ctx: SessionContext) -> None:
        """Install the unified Rust UDF platform on the session context."""
        from datafusion_engine.udf.platform import (
            RustUdfPlatformOptions,
            install_rust_udf_platform,
        )
        from datafusion_engine.udf.runtime import udf_backend_available

        options = RustUdfPlatformOptions(
            enable_udfs=self.features.enable_udfs,
            enable_async_udfs=self.features.enable_async_udfs,
            async_udf_timeout_ms=self.policies.async_udf_timeout_ms,
            async_udf_batch_size=self.policies.async_udf_batch_size,
            enable_function_factory=self.features.enable_function_factory,
            enable_expr_planners=self.features.enable_expr_planners,
            function_factory_hook=self.policies.function_factory_hook,
            expr_planner_hook=self.policies.expr_planner_hook,
            expr_planner_names=self.policies.expr_planner_names,
            strict=udf_backend_available(),
        )
        platform = install_rust_udf_platform(ctx, options=options)
        if platform.snapshot is not None:
            self._record_udf_snapshot(platform.snapshot)
        if platform.docs is not None and self.diagnostics.diagnostics_sink is not None:
            self._record_udf_docs(platform.docs)
        if platform.function_factory is not None:
            self._record_function_factory(
                available=platform.function_factory.available,
                installed=platform.function_factory.installed,
                error=platform.function_factory.error,
                policy=platform.function_factory_policy,
            )
        if platform.expr_planners is not None:
            self._record_expr_planners(
                available=platform.expr_planners.available,
                installed=platform.expr_planners.installed,
                error=platform.expr_planners.error,
                policy=platform.expr_planner_policy,
            )
        if (
            self.catalog.enable_information_schema
            and platform.snapshot is not None
            and platform.function_factory is not None
            and platform.function_factory.installed
        ):
            from datafusion_engine.udf.factory import function_factory_fallback_active
            from datafusion_engine.udf.runtime import register_udfs_via_ddl

            if function_factory_fallback_active(ctx):
                logger.warning(
                    "FunctionFactory fallback active; skipping CREATE FUNCTION registration."
                )
            else:
                register_udfs_via_ddl(ctx, snapshot=platform.snapshot)
        self._refresh_udf_catalog(ctx)

    def _install_planner_rules(self, ctx: SessionContext) -> None:
        """Install Rust planner policy rules for the session context.

        Args:
            ctx: Description.

        Raises:
            TypeError: If the operation cannot be completed.
        """
        policy = self._resolved_sql_policy()
        installers = _resolve_planner_rule_installers()
        if installers is None:
            return
        try:
            installers.config_installer(
                ctx,
                policy.allow_ddl,
                policy.allow_dml,
                policy.allow_statements,
            )
            installers.physical_config_installer(ctx, self.policies.physical_rulepack_enabled)
            installers.rule_installer(ctx)
            installers.physical_installer(ctx)
        except TypeError as exc:
            if "cannot be converted" in str(exc):
                return
            raise

    def _refresh_udf_catalog(self, ctx: SessionContext) -> None:
        if not self.catalog.enable_information_schema:
            msg = "UdfCatalog requires information_schema to be enabled."
            raise ValueError(msg)
        introspector = self._schema_introspector(ctx)
        if self.policies.udf_catalog_policy == "strict":
            catalog = get_strict_udf_catalog(introspector=introspector)
        else:
            catalog = get_default_udf_catalog(introspector=introspector)
        self._validate_udf_specs(catalog, introspector=introspector)
        self.udf_catalog_cache[id(ctx)] = catalog

    def _validate_udf_specs(
        self,
        catalog: UdfCatalog,
        *,
        introspector: SchemaIntrospector,
    ) -> None:
        """Validate Rust UDF snapshot coverage against the runtime catalog.

        Args:
            catalog: Description.
            introspector: Description.

        Raises:
            ValueError: If the operation cannot be completed.
        """
        from datafusion_engine.udf.runtime import (
            fallback_udfs_active,
            rust_udf_snapshot,
            udf_names_from_snapshot,
        )

        if fallback_udfs_active(introspector.ctx):
            return
        registry_snapshot = rust_udf_snapshot(introspector.ctx)
        registered_udfs = self._registered_udf_names(registry_snapshot)
        required_builtins = self._required_builtin_udfs(
            registry_snapshot,
            registered_udfs=registered_udfs,
            udf_names_from_snapshot=udf_names_from_snapshot,
        )
        missing = self._missing_udf_names(catalog, required_builtins)
        if missing:
            if self.diagnostics.diagnostics_sink is not None:
                self.record_artifact(
                    "datafusion_udf_validation_v1",
                    {
                        "event_time_unix_ms": int(time.time() * 1000),
                        "udf_catalog_policy": self.policies.udf_catalog_policy,
                        "missing_udfs": sorted(missing),
                        "missing_count": len(missing),
                    },
                )
            msg = f"Rust UDFs missing in DataFusion: {sorted(missing)}."
            raise ValueError(msg)
        from datafusion_engine.udf.parity import udf_info_schema_parity_report

        parity = udf_info_schema_parity_report(introspector.ctx)
        if parity.error is not None:
            msg = f"UDF information_schema parity failed: {parity.error}"
            raise ValueError(msg)
        if parity.missing_in_information_schema:
            msg = (
                "UDF information_schema parity failed: "
                f"missing={list(parity.missing_in_information_schema)}, "
                f"param_mismatches={len(parity.param_name_mismatches)}"
            )
            raise ValueError(msg)

    @staticmethod
    def _iter_snapshot_names(values: object) -> set[str]:
        if isinstance(values, Iterable) and not isinstance(values, (str, bytes)):
            return {str(name) for name in values if name is not None}
        return set()

    def _registered_udf_names(self, snapshot: Mapping[str, object]) -> set[str]:
        names: set[str] = set()
        for key in ("scalar", "aggregate", "window", "table"):
            names.update(self._iter_snapshot_names(snapshot.get(key)))
        return names

    def _required_builtin_udfs(
        self,
        snapshot: Mapping[str, object],
        *,
        registered_udfs: set[str],
        udf_names_from_snapshot: Callable[[Mapping[str, object]], Iterable[str]],
    ) -> set[str]:
        required = set(udf_names_from_snapshot(snapshot))
        custom_udfs = self._iter_snapshot_names(snapshot.get("custom_udfs"))
        required.difference_update(custom_udfs - registered_udfs)
        required.difference_update(self._iter_snapshot_names(snapshot.get("table")))
        return required

    @staticmethod
    def _missing_udf_names(catalog: UdfCatalog, required: Iterable[str]) -> list[str]:
        missing: list[str] = []
        for name in sorted(set(required)):
            try:
                if catalog.is_builtin_from_runtime(name):
                    continue
            except (RuntimeError, TypeError, ValueError):
                pass
            missing.append(name)
        return missing

    def udf_catalog(self, ctx: SessionContext) -> UdfCatalog:
        """Return the cached UDF catalog for a session context.

        Returns:
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

    def function_factory_policy_hash(self, ctx: SessionContext) -> str | None:
        """Return the FunctionFactory policy hash for a session context.

        Returns:
        -------
        str | None
            Policy hash when enabled, otherwise ``None``.
        """
        if not self.features.enable_function_factory:
            return None
        from datafusion_engine.udf.factory import function_factory_policy_hash
        from datafusion_engine.udf.runtime import rust_udf_snapshot

        snapshot = rust_udf_snapshot(ctx)
        return function_factory_policy_hash(
            snapshot,
            allow_async=self.features.enable_async_udfs,
        )

    def _validate_rule_function_allowlist(self, ctx: SessionContext) -> None:
        """Validate rulepack function demands against information_schema.

        Args:
            ctx: Description.

        Raises:
            ValueError: If the operation cannot be completed.
        """
        if not self.catalog.enable_information_schema:
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
        expected_names: Sequence[str] | None = None,
        expected_schemas: Mapping[str, pa.Schema] | None = None,
        view_errors: Mapping[str, str] | None = None,
        tree_sitter_checks: Mapping[str, object] | None = None,
    ) -> SchemaRegistryValidationResult:
        if not self.catalog.enable_information_schema:
            return SchemaRegistryValidationResult()
        expected = tuple(sorted(set(expected_names or ())))
        missing = missing_schema_names(ctx, expected=expected) if expected else ()
        type_errors: dict[str, str] = {}
        introspector = self._schema_introspector(ctx)
        constraint_drift = _constraint_drift_entries(
            introspector,
            names=expected,
            schemas=expected_schemas,
        )
        relationship_errors = _relationship_constraint_errors(
            self._session_runtime_from_context(ctx),
            sql_options=self._sql_options(),
        )
        result = SchemaRegistryValidationResult(
            missing=tuple(missing),
            type_errors=dict(type_errors),
            view_errors=dict(view_errors) if view_errors else {},
            constraint_drift=tuple(constraint_drift),
            relationship_constraint_errors=dict(relationship_errors)
            if relationship_errors
            else None,
        )
        if self.diagnostics.diagnostics_sink is None:
            return result
        if (
            not result.missing
            and not result.type_errors
            and not result.view_errors
            and not result.constraint_drift
            and result.relationship_constraint_errors is None
        ):
            return result
        payload: dict[str, object] = {
            "event_time_unix_ms": int(time.time() * 1000),
            "missing": list(result.missing),
            "type_errors": dict(result.type_errors),
            "view_errors": dict(result.view_errors) if result.view_errors else None,
            "constraint_drift": list(result.constraint_drift) if result.constraint_drift else None,
            "relationship_constraint_errors": dict(result.relationship_constraint_errors)
            if result.relationship_constraint_errors
            else None,
        }
        if tree_sitter_checks is not None:
            import json

            payload["tree_sitter_checks"] = json.dumps(tree_sitter_checks, default=str)
        if result.view_errors and self.view_registry is not None:
            parse_errors = _collect_view_sql_parse_errors(
                ctx,
                self.view_registry,
                sql_options=self._sql_options(),
            )
            if parse_errors:
                import json

                payload["sql_parse_errors"] = json.dumps(parse_errors, default=str)
        self.record_artifact("datafusion_schema_registry_validation_v1", payload)
        return result

    def _record_catalog_autoload_snapshot(self, ctx: SessionContext) -> None:
        if self.diagnostics.diagnostics_sink is None:
            return
        if not self.catalog.enable_information_schema:
            return
        catalog_location, catalog_format = self._effective_catalog_autoload()
        if catalog_location is None and catalog_format is None:
            return
        introspector = self._schema_introspector(ctx)
        template_names = sorted(self.data_sources.dataset_templates)
        payload: dict[str, object] = {
            "event_time_unix_ms": int(time.time() * 1000),
            "catalog_auto_load_location": catalog_location,
            "catalog_auto_load_format": catalog_format,
            "dataset_templates": template_names or None,
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
            missing = find_missing(required, functions)
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
        if self.diagnostics.diagnostics_sink is None:
            return
        self.record_artifact("datafusion_ast_feature_gates_v1", payload)

    def _record_ast_span_metadata(self, ctx: SessionContext) -> None:
        if self.diagnostics.diagnostics_sink is None:
            return
        payload: dict[str, object] = {
            "event_time_unix_ms": int(time.time() * 1000),
            "dataset": "ast_files_v1",
        }
        try:
            table = ctx.table("ast_span_metadata").to_arrow_table()
            rows = table.to_pylist()
            schema = self._resolved_table_schema(ctx, "ast_files_v1")
            if schema is not None:
                payload["schema_identity_hash"] = schema_identity_hash(schema)
            payload["metadata"] = rows[0] if rows else None
        except (KeyError, RuntimeError, TypeError, ValueError) as exc:
            payload["error"] = str(exc)
        version = _datafusion_version(ctx)
        if version is not None:
            payload["datafusion_version"] = version
        self.record_artifact("datafusion_ast_span_metadata_v1", payload)

    def _dataset_template(self, name: str) -> DatasetLocation | None:
        templates = self.data_sources.dataset_templates
        if not templates:
            return None
        template = templates.get(name)
        if template is not None:
            return template
        if name.endswith("_files_v1"):
            short_name = name.removesuffix("_files_v1")
            return templates.get(short_name)
        return None

    def _ast_dataset_location(self) -> DatasetLocation | None:
        return self._dataset_template("ast_files_v1")

    def _register_ast_dataset(self, ctx: SessionContext) -> None:
        location = self._ast_dataset_location()
        if location is None:
            return
        from datafusion_engine.io.adapter import DataFusionIOAdapter

        adapter = DataFusionIOAdapter(ctx=ctx, profile=self)
        with contextlib.suppress(KeyError, RuntimeError, TypeError, ValueError):
            adapter.deregister_table("ast_files_v1")
        from datafusion_engine.session.facade import DataFusionExecutionFacade

        facade = DataFusionExecutionFacade(ctx=ctx, runtime_profile=self)
        facade.register_dataset(name="ast_files_v1", location=location)
        self._record_ast_registration(location=location)

    def _bytecode_dataset_location(self) -> DatasetLocation | None:
        return self._dataset_template("bytecode_files_v1")

    def _register_bytecode_dataset(self, ctx: SessionContext) -> None:
        location = self._bytecode_dataset_location()
        if location is None:
            return
        from datafusion_engine.io.adapter import DataFusionIOAdapter

        adapter = DataFusionIOAdapter(ctx=ctx, profile=self)
        with contextlib.suppress(KeyError, RuntimeError, TypeError, ValueError):
            adapter.deregister_table("bytecode_files_v1")
        from datafusion_engine.session.facade import DataFusionExecutionFacade

        facade = DataFusionExecutionFacade(ctx=ctx, runtime_profile=self)
        facade.register_dataset(name="bytecode_files_v1", location=location)
        self._record_bytecode_registration(location=location)

    def _register_scip_datasets(self, ctx: SessionContext) -> None:
        scip_locations = self.data_sources.extract_output.scip_dataset_locations
        if not scip_locations:
            return
        from datafusion_engine.io.adapter import DataFusionIOAdapter

        adapter = DataFusionIOAdapter(ctx=ctx, profile=self)
        for name, location in sorted(scip_locations.items()):
            resolved = location
            with contextlib.suppress(KeyError, RuntimeError, TypeError, ValueError):
                adapter.deregister_table(name)
            from datafusion_engine.session.facade import DataFusionExecutionFacade

            facade = DataFusionExecutionFacade(ctx=ctx, runtime_profile=self)
            df = facade.register_dataset(name=name, location=resolved)
            actual_schema = df.schema()
            actual_fingerprint = None
            if isinstance(actual_schema, pa.Schema):
                actual_fingerprint = schema_identity_hash(actual_schema.remove_metadata())
            snapshot = _ScipRegistrationSnapshot(
                name=name,
                location=resolved,
                expected_fingerprint=None,
                actual_fingerprint=actual_fingerprint,
                schema_match=None,
            )
            self._record_scip_registration(snapshot=snapshot)

    def _record_ast_registration(self, *, location: DatasetLocation) -> None:
        if self.diagnostics.diagnostics_sink is None:
            return
        resolved = location.resolved
        scan = resolved.datafusion_scan
        payload = {
            "event_time_unix_ms": int(time.time() * 1000),
            "name": "ast_files_v1",
            "location": str(location.path),
            "format": location.format,
            "datafusion_provider": resolved.datafusion_provider,
            "file_sort_order": (
                [list(key) for key in scan.file_sort_order] if scan is not None else None
            ),
            "partition_cols": [
                {"name": name, "dtype": str(dtype)}
                for name, dtype in (scan.partition_cols_pyarrow() if scan is not None else ())
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
            "delta_constraints": (
                list(resolved.delta_constraints) if resolved.delta_constraints else None
            ),
        }
        self.record_artifact("datafusion_ast_dataset_v1", payload)

    def _record_bytecode_registration(self, *, location: DatasetLocation) -> None:
        if self.diagnostics.diagnostics_sink is None:
            return
        resolved = location.resolved
        scan = resolved.datafusion_scan
        payload = {
            "event_time_unix_ms": int(time.time() * 1000),
            "name": "bytecode_files_v1",
            "location": str(location.path),
            "format": location.format,
            "datafusion_provider": resolved.datafusion_provider,
            "file_sort_order": (
                [list(key) for key in scan.file_sort_order] if scan is not None else None
            ),
            "partition_cols": [
                {"name": name, "dtype": str(dtype)}
                for name, dtype in (scan.partition_cols_pyarrow() if scan is not None else ())
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
            "delta_constraints": (
                list(resolved.delta_constraints) if resolved.delta_constraints else None
            ),
        }
        self.record_artifact("datafusion_bytecode_dataset_v1", payload)

    def _record_scip_registration(
        self,
        *,
        snapshot: _ScipRegistrationSnapshot,
    ) -> None:
        if self.diagnostics.diagnostics_sink is None:
            return
        location = snapshot.location
        resolved = location.resolved
        scan = resolved.datafusion_scan
        payload = {
            "event_time_unix_ms": int(time.time() * 1000),
            "name": snapshot.name,
            "location": str(location.path),
            "format": location.format,
            "datafusion_provider": resolved.datafusion_provider,
            "file_sort_order": (
                [list(key) for key in scan.file_sort_order] if scan is not None else None
            ),
            "partition_cols": [
                {"name": col_name, "dtype": str(dtype)}
                for col_name, dtype in (scan.partition_cols_pyarrow() if scan is not None else ())
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
            "delta_constraints": (
                list(resolved.delta_constraints) if resolved.delta_constraints else None
            ),
            "expected_schema_identity_hash": snapshot.expected_fingerprint,
            "observed_schema_identity_hash": snapshot.actual_fingerprint,
            "schema_match": snapshot.schema_match,
        }
        self.record_artifact("datafusion_scip_datasets_v1", payload)

    def _validate_ast_catalog_autoload(self, ctx: SessionContext) -> None:
        catalog_location, catalog_format = self._effective_catalog_autoload()
        if catalog_location is None and catalog_format is None:
            return
        try:
            ctx.table("ast_files_v1")
        except (KeyError, RuntimeError, TypeError, ValueError) as exc:
            msg = f"AST catalog autoload failed: {exc}."
            raise ValueError(msg) from exc
        if not self.catalog.enable_information_schema:
            return
        try:
            self._schema_introspector(ctx).table_column_names("ast_files_v1")
        except (RuntimeError, TypeError, ValueError) as exc:
            msg = f"AST catalog column introspection failed: {exc}."
            raise ValueError(msg) from exc

    def _validate_bytecode_catalog_autoload(self, ctx: SessionContext) -> None:
        catalog_location, catalog_format = self._effective_catalog_autoload()
        if catalog_location is None and catalog_format is None:
            return
        try:
            ctx.table("bytecode_files_v1")
        except (KeyError, RuntimeError, TypeError, ValueError) as exc:
            msg = f"Bytecode catalog autoload failed: {exc}."
            raise ValueError(msg) from exc
        if not self.catalog.enable_information_schema:
            return
        try:
            self._schema_introspector(ctx).table_column_names("bytecode_files_v1")
        except (RuntimeError, TypeError, ValueError) as exc:
            msg = f"Bytecode catalog column introspection failed: {exc}."
            raise ValueError(msg) from exc

    def _record_cst_schema_diagnostics(self, ctx: SessionContext) -> None:
        if self.diagnostics.diagnostics_sink is None:
            return
        payload: dict[str, object] = {
            "event_time_unix_ms": int(time.time() * 1000),
            "dataset": "libcst_files_v1",
        }
        if not ctx.table_exist("cst_schema_diagnostics"):
            payload["available"] = False
            self.record_artifact("datafusion_cst_schema_diagnostics_v1", payload)
            return
        try:
            table = ctx.table("cst_schema_diagnostics").to_arrow_table()
            rows = table.to_pylist()
            schema = self._resolved_table_schema(ctx, "libcst_files_v1")
            if schema is not None:
                payload["schema_identity_hash"] = schema_identity_hash(schema)
            default_entries = _default_value_entries(schema) if schema is not None else None
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
        if self.diagnostics.diagnostics_sink is None:
            return
        payload: dict[str, object] = {
            "event_time_unix_ms": int(time.time() * 1000),
            "dataset": "tree_sitter_files_v1",
        }
        try:
            table = ctx.table("ts_stats").to_arrow_table()
            rows = table.to_pylist()
            schema = self._resolved_table_schema(ctx, "tree_sitter_files_v1")
            if schema is not None:
                payload["schema_identity_hash"] = schema_identity_hash(schema)
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
        if self.diagnostics.diagnostics_sink is None:
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
        if self.diagnostics.diagnostics_sink is None:
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
                if not ctx.table_exist(name):
                    errors[name] = "table not found"
                    continue
                summary_sql = (
                    "SELECT count(*) AS row_count, "
                    "sum(CASE WHEN mismatch THEN 1 ELSE 0 END) AS mismatch_count "
                    f"FROM {name}"
                )
                summary_rows = (
                    _sql_with_options(
                        ctx,
                        summary_sql,
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
                    sample_sql = f"SELECT * FROM {name} WHERE mismatch LIMIT 25"
                    sample_rows = (
                        _sql_with_options(
                            ctx,
                            sample_sql,
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
        if self.diagnostics.diagnostics_sink is None:
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
        if self.diagnostics.diagnostics_sink is None:
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
        if self.diagnostics.diagnostics_sink is None:
            return
        if not self.catalog.enable_information_schema:
            return
        payload: dict[str, object] = {
            "event_time_unix_ms": int(time.time() * 1000),
            "dataset": "bytecode_files_v1",
        }
        try:
            table = ctx.table("py_bc_metadata").to_arrow_table()
            rows = table.to_pylist()
            schema = self._resolved_table_schema(ctx, "bytecode_files_v1")
            if schema is not None:
                payload["schema_identity_hash"] = schema_identity_hash(schema)
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
        if self.diagnostics.diagnostics_sink is None:
            return
        if not self.catalog.enable_information_schema:
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
                        include_routines=self.catalog.enable_information_schema,
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

    def _validate_catalog_autoloads(
        self,
        ctx: SessionContext,
        *,
        ast_registration: bool,
        bytecode_registration: bool,
    ) -> None:
        catalog_location, catalog_format = self._effective_catalog_autoload()
        if catalog_location is None and catalog_format is None:
            return
        if not ast_registration:
            from datafusion_engine.io.adapter import DataFusionIOAdapter

            adapter = DataFusionIOAdapter(ctx=ctx, profile=self)
            with contextlib.suppress(KeyError, RuntimeError, TypeError, ValueError):
                adapter.deregister_table("ast_files_v1")
            self._validate_ast_catalog_autoload(ctx)
        if not bytecode_registration:
            from datafusion_engine.io.adapter import DataFusionIOAdapter

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
        allow_semantic_row_probe_fallback: bool = True,
    ) -> dict[str, str]:
        _ = ast_view_names
        view_errors: dict[str, str] = {}
        for label, validator in (
            ("udf_info_schema_parity", validate_udf_info_schema_parity),
            ("engine_functions", validate_required_engine_functions),
        ):
            try:
                validator(ctx)
            except (RuntimeError, TypeError, ValueError) as exc:
                view_errors[label] = str(exc)
        for name in extract_nested_dataset_names():
            if not ctx.table_exist(name):
                continue
            try:
                validate_nested_types(ctx, name)
            except (RuntimeError, TypeError, ValueError) as exc:
                view_errors[f"nested_types:{name}"] = str(exc)
        try:
            validate_semantic_types(
                ctx,
                allow_row_probe_fallback=allow_semantic_row_probe_fallback,
            )
        except (RuntimeError, TypeError, ValueError) as exc:
            view_errors["semantic_types"] = str(exc)
        return view_errors

    @staticmethod
    def _register_schema_tables(
        ctx: SessionContext,
        *,
        names: Sequence[str],
        resolver: Callable[[str], pa.Schema],
    ) -> None:
        for name in names:
            if ctx.table_exist(name):
                continue
            schema = resolver(name)
            _register_schema_table(ctx, name, schema)

    @staticmethod
    def _schema_registry_issues(
        validation: SchemaRegistryValidationResult,
        *,
        zero_row_bootstrap: ZeroRowBootstrapConfig,
    ) -> tuple[dict[str, object], dict[str, object] | None]:
        issues: dict[str, object] = {}
        advisory: dict[str, object] = {}
        if validation.missing:
            issues["missing"] = list(validation.missing)
        if validation.type_errors:
            issues["type_errors"] = dict(validation.type_errors)
        if validation.view_errors:
            if zero_row_bootstrap.validation_mode == "bootstrap" and not zero_row_bootstrap.strict:
                advisory["view_errors"] = dict(validation.view_errors)
            else:
                issues["view_errors"] = dict(validation.view_errors)
        if validation.constraint_drift:
            issues["constraint_drift"] = list(validation.constraint_drift)
        if validation.relationship_constraint_errors:
            issues["relationship_constraint_errors"] = dict(
                validation.relationship_constraint_errors
            )
        return issues, advisory or None

    def _install_schema_registry(self, ctx: SessionContext) -> None:
        """Register canonical nested schemas on the session context.

        Args:
            ctx: Description.

        Raises:
            ValueError: If the operation cannot be completed.
        """
        if not self.features.enable_schema_registry:
            return
        self._record_catalog_autoload_snapshot(ctx)
        ast_view_names, _, ast_gate_payload = self._ast_feature_gates(ctx)
        self._record_ast_feature_gates(ast_gate_payload)
        ast_registration = self._ast_dataset_location() is not None
        if ast_registration:
            self._register_ast_dataset(ctx)
        bytecode_registration = self._bytecode_dataset_location() is not None
        if bytecode_registration:
            self._register_bytecode_dataset(ctx)
        self._register_scip_datasets(ctx)
        self._register_schema_tables(
            ctx,
            names=extract_nested_dataset_names(),
            resolver=extract_nested_schema_for,
        )
        from datafusion_engine.extract.registry import dataset_schema as extract_dataset_schema
        from semantics.input_registry import SEMANTIC_INPUT_SPECS

        semantic_input_names = tuple(
            dict.fromkeys(spec.extraction_source for spec in SEMANTIC_INPUT_SPECS)
        )
        self._register_schema_tables(
            ctx,
            names=semantic_input_names,
            resolver=extract_dataset_schema,
        )
        self._register_schema_tables(
            ctx,
            names=relationship_schema_names(),
            resolver=relationship_schema_for,
        )
        self._validate_catalog_autoloads(
            ctx,
            ast_registration=ast_registration,
            bytecode_registration=bytecode_registration,
        )
        tree_sitter_checks = self._record_schema_diagnostics(
            ctx,
            ast_view_names=ast_view_names,
        )
        view_errors = self._validate_schema_views(
            ctx,
            ast_view_names=ast_view_names,
            allow_semantic_row_probe_fallback=(
                self.zero_row_bootstrap.allow_semantic_row_probe_fallback
            ),
        )
        validation = self._record_schema_registry_validation(
            ctx,
            expected_names=(),
            expected_schemas=None,
            view_errors=view_errors or None,
            tree_sitter_checks=tree_sitter_checks,
        )
        issues, advisory = self._schema_registry_issues(
            validation,
            zero_row_bootstrap=self.zero_row_bootstrap,
        )
        if advisory:
            self.record_artifact(
                "schema_registry_validation_advisory_v1",
                {
                    "event_time_unix_ms": int(time.time() * 1000),
                    "issues": advisory,
                    "validation_mode": self.zero_row_bootstrap.validation_mode,
                    "strict": self.zero_row_bootstrap.strict,
                },
            )
        if issues:
            msg = f"Schema registry validation failed: {issues}."
            raise ValueError(msg)
        self._record_schema_snapshots(ctx)

    def _prepare_statements(self, ctx: SessionContext) -> None:
        """Prepare SQL statements when configured."""
        statements = list(self.policies.prepared_statements)
        if not self.catalog.enable_information_schema:
            statements = [
                statement
                for statement in statements
                if statement.name not in INFO_SCHEMA_STATEMENT_NAMES
            ]
        seen: set[str] = set()
        for statement in statements:
            if statement.name in seen:
                continue
            seen.add(statement.name)
            _sql_with_options(
                ctx,
                _prepare_statement_sql(statement),
                sql_options=self._statement_sql_options(),
                allow_statements=True,
            )
            self._record_prepared_statement(statement)

    def _record_prepared_statement(self, statement: PreparedStatementSpec) -> None:
        if self.diagnostics.diagnostics_sink is None:
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
        module = _resolve_extension_module(required_attr="install_delta_plan_codecs")
        if module is None:
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
            register(
                self.policies.delta_plan_codec_physical,
                self.policies.delta_plan_codec_logical,
            )
        except TypeError:
            try:
                register(
                    self.policies.delta_plan_codec_logical,
                    self.policies.delta_plan_codec_physical,
                )
            except TypeError:
                return True, False
        return True, True

    def _record_udf_snapshot(self, snapshot: Mapping[str, object]) -> None:
        if self.diagnostics.diagnostics_sink is None:
            return
        self.record_artifact(
            "datafusion_udf_registry_v1",
            dict(snapshot),
        )

    def _record_udf_docs(self, docs: Mapping[str, object]) -> None:
        if self.diagnostics.diagnostics_sink is None:
            return
        self.record_artifact(
            "datafusion_udf_docs_v1",
            dict(docs),
        )

    def _record_delta_plan_codecs(self, *, available: bool, installed: bool) -> None:
        if self.diagnostics.diagnostics_sink is None:
            return
        self.record_artifact(
            "datafusion_delta_plan_codecs_v1",
            {
                "enabled": self.features.enable_delta_plan_codecs,
                "available": available,
                "installed": installed,
                "physical_codec": self.policies.delta_plan_codec_physical,
                "logical_codec": self.policies.delta_plan_codec_logical,
            },
        )

    def _record_delta_session_defaults(
        self,
        *,
        available: bool,
        installed: bool,
        error: str | None,
    ) -> None:
        if self.diagnostics.diagnostics_sink is None:
            return
        self.record_artifact(
            "datafusion_delta_session_defaults_v1",
            {
                "enabled": self.features.enable_delta_session_defaults,
                "available": available,
                "installed": installed,
                "error": error,
            },
        )

    def _record_extension_parity_validation(self, ctx: SessionContext) -> None:
        payload = dict(self._validate_named_args_extension_parity())
        payload["async_udf_policy"] = self._validate_async_udf_policy()
        payload["udf_info_schema_parity"] = self._validate_udf_info_schema_parity(ctx)
        if self.diagnostics.diagnostics_sink is None:
            return
        from datafusion_engine.udf.runtime import extension_capabilities_report

        payload["extension_capabilities"] = extension_capabilities_report()
        runtime_capabilities = self._runtime_capabilities_payload(ctx)
        payload["runtime_capabilities"] = runtime_capabilities
        payload["event_time_unix_ms"] = int(time.time() * 1000)
        payload["profile_name"] = self.policies.config_policy_name
        payload["settings_hash"] = self.settings_hash()
        self.record_artifact(
            "datafusion_extension_parity_v1",
            payload,
        )
        self.record_artifact(
            "datafusion_runtime_capabilities_v1",
            runtime_capabilities,
        )

    def _runtime_capabilities_payload(self, ctx: SessionContext) -> dict[str, object]:
        from datafusion_engine.extensions.runtime_capabilities import (
            build_runtime_capabilities_snapshot,
            runtime_capabilities_payload,
        )

        snapshot = build_runtime_capabilities_snapshot(
            ctx,
            profile_name=self.policies.config_policy_name,
            settings_hash=self.settings_hash(),
            strict_native_provider_enabled=self.features.enforce_delta_ffi_provider,
        )
        return runtime_capabilities_payload(snapshot)

    def _record_cache_diagnostics(self, ctx: SessionContext) -> None:
        """Record cache configuration and state diagnostics.

        Parameters
        ----------
        ctx
            DataFusion session context to introspect.
        """
        self._snapshot_metadata_caches(ctx)
        if self.diagnostics.diagnostics_sink is None:
            return
        cache_diag = _capture_cache_diagnostics(ctx)
        config_payload = _cache_config_payload(cache_diag)
        self.record_artifact("datafusion_cache_config_v1", config_payload)
        self.record_artifact(
            "datafusion_cache_root_v1",
            {"cache_root": self.io_ops.cache_root()},
        )
        if self.policies.cache_policy is not None:
            self.record_artifact(
                "cache_policy_v1",
                cache_policy_settings(self.policies.cache_policy),
            )
        cache_snapshots = _cache_snapshot_rows(cache_diag)
        if cache_snapshots:
            self.record_events(
                "datafusion_cache_state_v1",
                cache_snapshots,
            )
        diskcache_profile = self.policies.diskcache_profile
        if diskcache_profile is None:
            return
        diskcache_events = self._diskcache_event_rows(diskcache_profile)
        if diskcache_events:
            self.record_events(
                "diskcache_stats_v1",
                diskcache_events,
            )

    def _snapshot_metadata_caches(self, ctx: SessionContext) -> None:
        if not self.policies.metadata_cache_snapshot_enabled:
            return
        from datafusion_engine.cache.metadata_snapshots import snapshot_datafusion_caches

        try:
            snapshots = snapshot_datafusion_caches(ctx, runtime_profile=self)
        except (RuntimeError, TypeError, ValueError) as exc:
            if self.diagnostics.diagnostics_sink is None:
                return
            self.record_artifact(
                "datafusion_cache_snapshot_error_v1",
                {
                    "event_time_unix_ms": int(time.time() * 1000),
                    "error": str(exc),
                },
            )
            return
        if self.diagnostics.diagnostics_sink is None:
            return
        if snapshots:
            self.record_events(
                "datafusion_cache_snapshot_v1",
                snapshots,
            )

    def _diskcache_event_rows(self, diskcache_profile: DiskCacheProfile) -> list[dict[str, object]]:
        rows: list[dict[str, object]] = []
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
                    "ttl_seconds": diskcache_profile.ttl_for(cast("DiskCacheKind", kind)),
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
            rows.append(payload)
        return rows

    def _install_cache_tables(self, ctx: SessionContext) -> None:
        if not (
            self.features.enable_cache_manager
            or self.features.cache_enabled
            or self.policies.metadata_cache_snapshot_enabled
        ):
            return
        try:
            _register_cache_introspection_functions(ctx)
        except ImportError as exc:
            msg = "Cache table functions require datafusion._internal or datafusion_ext."
            raise RuntimeError(msg) from exc
        except (RuntimeError, TypeError, ValueError) as exc:
            msg = f"Cache table function registration failed: {exc}"
            raise RuntimeError(msg) from exc

    def _build_session_context(self) -> SessionContext:
        """Create the SessionContext base for this runtime profile.

        Returns:
        -------
        datafusion.SessionContext
            Base session context for this profile.
        """
        return SessionFactory(self).build()

    def _apply_url_table(self, ctx: SessionContext) -> SessionContext:
        return ctx.enable_url_table() if self.features.enable_url_table else ctx

    def _register_local_filesystem(self, ctx: SessionContext) -> None:
        if self.policies.local_filesystem_root is None:
            return
        store = LocalFileSystem(prefix=self.policies.local_filesystem_root)
        from datafusion_engine.io.adapter import DataFusionIOAdapter

        adapter = DataFusionIOAdapter(ctx=ctx, profile=self)
        adapter.register_object_store(scheme="file://", store=store, host=None)

    def _install_function_factory(self, ctx: SessionContext) -> None:
        if not self.features.enable_function_factory:
            return
        available = True
        installed = False
        error: str | None = None
        cause: Exception | None = None
        try:
            if self.policies.function_factory_hook is None:
                install_function_factory(ctx)
            else:
                self.policies.function_factory_hook(ctx)
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
        if not self.features.enable_expr_planners:
            return
        available = True
        installed = False
        error: str | None = None
        cause: Exception | None = None
        try:
            if self.policies.expr_planner_hook is None:
                install_expr_planners(ctx, planner_names=self.policies.expr_planner_names)
            else:
                self.policies.expr_planner_hook(ctx)
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

        Args:
            ctx: Description.

        Raises:
            TypeError: If the operation cannot be completed.
        """
        factory = self.policies.physical_expr_adapter_factory
        uses_default_adapter = False
        if factory is None and self.features.enable_schema_evolution_adapter:
            try:
                factory = _load_schema_evolution_adapter_factory()
                uses_default_adapter = True
            except (RuntimeError, TypeError):
                return
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
        policy: Mapping[str, object] | None = None,
    ) -> None:
        if self.diagnostics.diagnostics_sink is None:
            return
        self.record_artifact(
            "datafusion_expr_planners_v1",
            {
                "enabled": self.features.enable_expr_planners,
                "available": available,
                "installed": installed,
                "hook_enabled": bool(self.policies.expr_planner_hook),
                "planner_names": list(self.policies.expr_planner_names),
                "policy": policy or expr_planner_payloads(self.policies.expr_planner_names),
                "error": error,
            },
        )

    def _record_function_factory(
        self,
        *,
        available: bool,
        installed: bool,
        error: str | None,
        policy: Mapping[str, object] | None = None,
    ) -> None:
        if self.diagnostics.diagnostics_sink is None:
            return
        self.record_artifact(
            "datafusion_function_factory_v1",
            {
                "enabled": self.features.enable_function_factory,
                "available": available,
                "installed": installed,
                "hook_enabled": bool(self.policies.function_factory_hook),
                "policy": policy or function_factory_payloads(),
                "error": error,
            },
        )

    def _record_tracing_install(self, error: str, details: Mapping[str, object]) -> None:
        if self.diagnostics.diagnostics_sink is None:
            return
        payload = {"error": error, **details}
        self.record_artifact("datafusion_tracing_install_v1", payload)

    def _install_tracing(self, ctx: SessionContext) -> None:
        """Enable tracing when configured.

        Args:
            ctx: Description.

        Raises:
            ValueError: If the operation cannot be completed.
        """
        if not self.features.enable_tracing:
            return
        from datafusion_engine.delta.observability import ensure_delta_tracing

        delta_installed, delta_error = ensure_delta_tracing()
        if self.diagnostics.diagnostics_sink is not None:
            self.record_artifact(
                "datafusion_delta_tracing_v1",
                {
                    "enabled": self.features.enable_tracing,
                    "installed": delta_installed,
                    "error": delta_error,
                },
            )
        if self.diagnostics.tracing_hook is None:
            module = _resolve_extension_module(required_attr="install_tracing")
            if module is None:
                msg = "Tracing enabled but datafusion._internal or datafusion_ext is unavailable."
                raise ValueError(msg)
            install = getattr(module, "install_tracing", None)
            if not callable(install):
                msg = "Tracing enabled but DataFusion extension install_tracing is unavailable."
                raise ValueError(msg)
            ctx_arg, details = _resolve_tracing_context(ctx, module)
            if ctx_arg is None:
                logger.info(
                    "Tracing install skipped: no compatible SessionContext for %s",
                    details.get("module") or "unknown",
                )
                self._record_tracing_install("no_compatible_session_context", details)
                return
            try:
                install(ctx_arg)
            except TypeError as exc:
                logger.info("Tracing install failed: %s", exc)
                error_details = {
                    "module": details.get("module"),
                    "context_type": type(ctx_arg).__name__,
                }
                self._record_tracing_install(str(exc), error_details)
            return
        self.diagnostics.tracing_hook(ctx)

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
        if (
            hooks["explain"] is None
            and capture_explain
            and self.diagnostics.explain_collector is not None
        ):
            hooks["explain"] = self.diagnostics.explain_collector.hook
        if (
            hooks["plan_artifacts"] is None
            and capture_plan_artifacts
            and self.diagnostics.plan_collector is not None
        ):
            hooks["plan_artifacts"] = self.diagnostics.plan_collector.hook
        if self.diagnostics.diagnostics_sink is not None:
            if capture_explain or hooks["explain"] is not None:
                hooks["explain"] = _chain_explain_hooks(
                    cast("ExplainHook", hooks["explain"]),
                    diagnostics_explain_hook(
                        self.diagnostics.diagnostics_sink,
                        explain_analyze=explain_analyze,
                    ),
                )
            if capture_plan_artifacts or hooks["plan_artifacts"] is not None:
                hooks["plan_artifacts"] = _chain_plan_artifacts_hooks(
                    cast("PlanArtifactsHook", hooks["plan_artifacts"]),
                    diagnostics_plan_artifacts_hook(self.diagnostics.diagnostics_sink),
                )
            if capture_semantic_diff or hooks["semantic_diff"] is not None:
                hooks["semantic_diff"] = _chain_plan_artifacts_hooks(
                    cast("PlanArtifactsHook", hooks["semantic_diff"]),
                    diagnostics_semantic_diff_hook(self.diagnostics.diagnostics_sink),
                )
            hooks["sql_ingest"] = _chain_sql_ingest_hooks(
                cast("SqlIngestHook", hooks["sql_ingest"]),
                diagnostics_sql_ingest_hook(self.diagnostics.diagnostics_sink),
            )
            hooks["cache_event"] = _chain_cache_hooks(
                cast("CacheEventHook", hooks["cache_event"]),
                diagnostics_cache_hook(self.diagnostics.diagnostics_sink),
            )
            hooks["substrait_fallback"] = _chain_substrait_fallback_hooks(
                cast("SubstraitFallbackHook", hooks["substrait_fallback"]),
                diagnostics_substrait_fallback_hook(self.diagnostics.diagnostics_sink),
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
        if self.policies.sql_policy is None and self.policies.sql_policy_name is None:
            return None
        return self.policies.sql_policy or resolve_sql_policy(self.policies.sql_policy_name)

    def _compile_options(
        self,
        *,
        options: DataFusionCompileOptions | None = None,
        params: Mapping[str, object] | None = None,
        execution_policy: AdapterExecutionPolicy | None = None,
        execution_label: ExecutionLabel | None = None,
    ) -> DataFusionCompileOptions:
        """Return DataFusion compile options derived from the profile.

        Returns:
        -------
        DataFusionCompileOptions
            Compile options aligned with this runtime profile.
        """
        resolved = options or DataFusionCompileOptions(
            cache=None,
            cache_max_columns=None,
            enforce_preflight=self.features.enforce_preflight,
        )
        resolved_params = resolved.params if resolved.params is not None else params
        prepared = _resolve_prepared_statement_options(resolved)
        capture_explain = resolved.capture_explain or self.diagnostics.capture_explain
        explain_analyze = resolved.explain_analyze or self.diagnostics.explain_analyze
        substrait_validation = (
            resolved.substrait_validation or self.diagnostics.substrait_validation
        )
        capture_plan_artifacts = (
            resolved.capture_plan_artifacts
            or self.diagnostics.capture_plan_artifacts
            or capture_explain
            or substrait_validation
        )
        capture_semantic_diff = (
            resolved.capture_semantic_diff or self.diagnostics.capture_semantic_diff
        )
        resolution = _CompileOptionResolution(
            cache=resolved.cache if resolved.cache is not None else self.features.cache_enabled,
            cache_max_columns=(
                resolved.cache_max_columns
                if resolved.cache_max_columns is not None
                else self.policies.cache_max_columns
            ),
            params=resolved_params,
            param_allowlist=(
                resolved.param_identifier_allowlist
                if resolved.param_identifier_allowlist is not None
                else tuple(self.policies.param_identifier_allowlist) or None
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
                else self.policies.sql_policy_name
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
            self.features.enforce_preflight == resolved.enforce_preflight,
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
                explain_sink=self.diagnostics.labeled_explains,
            )
        if execution_policy is None:
            return updated
        return apply_execution_policy(
            updated,
            execution_policy=execution_policy,
        )

    def _resolved_sql_policy(self) -> DataFusionSqlPolicy:
        """Return the resolved SQL policy for this runtime profile.

        Returns:
        -------
        DataFusionSqlPolicy
            SQL policy derived from the profile configuration.
        """
        if self.policies.sql_policy is not None:
            return self.policies.sql_policy
        if self.policies.sql_policy_name is None:
            return DataFusionSqlPolicy()
        return resolve_sql_policy(self.policies.sql_policy_name)

    def _sql_options(self) -> SQLOptions:
        """Return SQLOptions for SQL execution.

        Returns:
        -------
        datafusion.SQLOptions
            SQL options for use with DataFusion contexts.
        """
        return sql_options_for_profile(self)

    def sql_options(self) -> SQLOptions:
        """Return SQLOptions derived from the resolved SQL policy.

        Returns:
        -------
        datafusion.SQLOptions
            SQL options derived from the profile policy.
        """
        return self._sql_options()

    def _statement_sql_options(self) -> SQLOptions:
        """Return SQLOptions that allow statement execution.

        Returns:
        -------
        datafusion.SQLOptions
            SQL options with statement execution enabled.
        """
        return statement_sql_options_for_profile(self)

    def _diskcache(self, kind: DiskCacheKind) -> Cache | FanoutCache | None:
        """Return a DiskCache instance for the requested kind.

        Returns:
        -------
        diskcache.Cache | diskcache.FanoutCache | None
            Cache instance when DiskCache is configured.
        """
        profile = self.policies.diskcache_profile
        if profile is None:
            return None
        return cache_for_kind(profile, kind)

    def _diskcache_ttl_seconds(self, kind: DiskCacheKind) -> float | None:
        """Return the TTL in seconds for a DiskCache kind when configured.

        Returns:
        -------
        float | None
            TTL in seconds or None when unset.
        """
        profile = self.policies.diskcache_profile
        if profile is None:
            return None
        return profile.ttl_for(kind)

    def _record_view_definition(self, *, artifact: DataFusionViewArtifact) -> None:
        """Record a view artifact for diagnostics snapshots.

        Parameters
        ----------
        artifact:
            View artifact payload for diagnostics.
        """
        record_view_definition(self, artifact=artifact)

    def _schema_introspector(self, ctx: SessionContext) -> SchemaIntrospector:
        """Return a schema introspector for the session.

        Returns:
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

    @staticmethod
    def _resolved_table_schema(ctx: SessionContext, name: str) -> pa.Schema | None:
        try:
            schema = ctx.table(name).schema()
        except (KeyError, RuntimeError, TypeError, ValueError):
            return None
        if isinstance(schema, pa.Schema):
            return schema
        to_arrow = getattr(schema, "to_arrow", None)
        if callable(to_arrow):
            resolved = to_arrow()
            if isinstance(resolved, pa.Schema):
                return resolved
        return None

    def _settings_snapshot(self, ctx: SessionContext) -> pa.Table:
        """Return a snapshot of DataFusion settings when information_schema is enabled.

        Returns:
        -------
        pyarrow.Table
            Table of settings from information_schema.df_settings.
        """
        cache = _introspection_cache_for_ctx(ctx, sql_options=self._sql_options())
        return cache.snapshot.settings

    def _catalog_snapshot(self, ctx: SessionContext) -> pa.Table:
        """Return a snapshot of DataFusion catalog tables when available.

        Returns:
        -------
        pyarrow.Table
            Table inventory from information_schema.tables.
        """
        cache = _introspection_cache_for_ctx(ctx, sql_options=self._sql_options())
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

        Returns:
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
        if self.execution.session_context_key:
            return self.execution.session_context_key
        # Use the full runtime fingerprint so distinct profiles do not alias.
        return self.fingerprint()

    def context_cache_key(self) -> str:
        """Return a stable cache key for the session context.

        Returns:
        -------
        str
            Stable cache key derived from the runtime profile.
        """
        return self._cache_key()

    def _cached_context(self) -> SessionContext | None:
        if not self.execution.share_context or self.diagnostics.diagnostics_sink is not None:
            return None
        return _SESSION_CONTEXT_CACHE.get(self._cache_key())

    def _cache_context(self, ctx: SessionContext) -> None:
        if not self.execution.share_context:
            return
        _SESSION_CONTEXT_CACHE[self._cache_key()] = ctx


def cache_prefix_for_delta_snapshot(
    profile: DataFusionRuntimeProfile,
    *,
    dataset_name: str,
    snapshot: Mapping[str, object] | None = None,
    delta_version: int | None = None,
    delta_timestamp: str | int | None = None,
) -> str | None:
    """Return a cache prefix that pins caches to a Delta snapshot.

    Returns:
    -------
    str | None
        Cache prefix when snapshot identifiers are available; otherwise ``None``.
    """
    version = coerce_int(snapshot.get("version")) if snapshot is not None else None
    if version is None:
        version = delta_version
    timestamp_value = None
    if snapshot is not None:
        timestamp_value = coerce_int(snapshot.get("snapshot_timestamp"))
        if timestamp_value is None:
            timestamp_value = snapshot.get("timestamp")
    if timestamp_value is None and delta_timestamp is not None:
        timestamp_value = delta_timestamp
    if version is None and timestamp_value is None:
        return None
    suffix = version if version is not None else timestamp_value
    return f"{profile.context_cache_key()}::{dataset_name}::{suffix}"


def collect_datafusion_metrics(
    profile: DataFusionRuntimeProfile,
) -> Mapping[str, object] | None:
    """Return optional DataFusion metrics payload.

    Returns:
    -------
    Mapping[str, object] | None
        Metrics payload when enabled and available.
    """
    if not profile.features.enable_metrics or profile.diagnostics.metrics_collector is None:
        return None
    return profile.diagnostics.metrics_collector()


def schema_introspector_for_profile(
    profile: DataFusionRuntimeProfile,
    ctx: SessionContext,
    *,
    cache_prefix: str | None = None,
) -> SchemaIntrospector:
    """Return a schema introspector for a runtime profile.

    Returns:
    -------
    SchemaIntrospector
        Introspector configured from the profile.
    """
    cache_profile = profile.policies.diskcache_profile
    cache = cache_for_kind(cache_profile, "schema") if cache_profile is not None else None
    cache_ttl = cache_profile.ttl_for("schema") if cache_profile is not None else None
    resolved_prefix = cache_prefix or profile.context_cache_key()
    return SchemaIntrospector(
        ctx,
        sql_options=profile.sql_options(),
        cache=cache,
        cache_prefix=resolved_prefix,
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

    Returns:
    -------
    list[dict[str, object]]
        Maintenance payloads for each cache kind.
    """
    cache_profile = profile.policies.diskcache_profile
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

    Returns:
    -------
    int
        Count of evicted entries.
    """
    cache_profile = profile.policies.diskcache_profile
    if cache_profile is None:
        return 0
    return evict_cache_tag(cache_profile, kind=kind, tag=tag)


def register_cdf_inputs_for_profile(
    profile: DataFusionRuntimeProfile,
    ctx: SessionContext,
    *,
    table_names: Sequence[str],
    dataset_resolver: ManifestDatasetResolver | None = None,
) -> Mapping[str, str]:
    """Register Delta CDF inputs for the requested tables.

    Returns:
    -------
    Mapping[str, str]
        Mapping of base table names to registered CDF view names.
    """
    from datafusion_engine.delta.cdf import register_cdf_inputs

    return register_cdf_inputs(
        ctx, profile, table_names=table_names, dataset_resolver=dataset_resolver
    )


def collect_datafusion_traces(
    profile: DataFusionRuntimeProfile,
) -> Mapping[str, object] | None:
    """Return optional DataFusion tracing payload.

    Returns:
    -------
    Mapping[str, object] | None
        Tracing payload when enabled and available.
    """
    if not profile.features.enable_tracing or profile.diagnostics.tracing_collector is None:
        return None
    return profile.diagnostics.tracing_collector()


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

    Returns:
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

    Returns:
    -------
    DataFusionCompileOptions
        Options updated with execution policy settings when configured.
    """
    _ = execution_policy
    return options


@lru_cache(maxsize=128)
def _datafusion_type_name(dtype: pa.DataType) -> str:
    ctx = DataFusionRuntimeProfile().io_ops.ephemeral_context()
    table = pa.Table.from_arrays([pa.array([None], type=dtype)], names=["value"])
    from datafusion_engine.io.adapter import DataFusionIOAdapter

    adapter = DataFusionIOAdapter(ctx=ctx, profile=None)
    adapter.register_table("t", ctx.from_arrow(table))
    result = _sql_with_options(
        ctx,
        "SELECT arrow_typeof(value) AS dtype FROM t LIMIT 1",
    ).to_arrow_table()
    value = result["dtype"][0].as_py()
    if not isinstance(value, str):
        msg = "Failed to resolve DataFusion type name."
        raise TypeError(msg)
    return value


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


def session_runtime_for_context(
    profile: DataFusionRuntimeProfile,
    ctx: SessionContext,
) -> SessionRuntime | None:
    """Return a SessionRuntime for the provided context when compatible.

    Returns:
    -------
    SessionRuntime | None
        Session runtime for the context when compatible, otherwise ``None``.
    """
    try:
        return _build_session_runtime_from_context(ctx, profile=profile)
    except (RuntimeError, TypeError, ValueError):
        return None


def _align_projection_exprs(
    *,
    schema: pa.Schema,
    input_columns: Sequence[str],
    keep_extra_columns: bool,
) -> list[Expr]:
    selections: list[Expr] = []
    for schema_field in schema:
        col_name = schema_field.name
        if schema_field.name in input_columns:
            selections.append(safe_cast(col(col_name), schema_field.type).alias(col_name))
        else:
            selections.append(safe_cast(lit(None), schema_field.type).alias(col_name))
    if keep_extra_columns:
        for name in input_columns:
            if name in schema.names:
                continue
            selections.append(col(name))
    return selections


def align_table_to_schema(
    table: TableLike | RecordBatchReaderLike,
    *,
    schema: SchemaLike,
    keep_extra_columns: bool = False,
    ctx: SessionContext | None = None,
) -> pa.Table:
    """Align a table to a target schema using DataFusion casts.

    Returns:
    -------
    pyarrow.Table
        Table aligned to the provided schema.
    """
    resolved_schema = pa.schema(schema)
    resolved_table = to_arrow_table(table)
    if _schema_has_extension(resolved_schema):
        return _align_table_with_arrow(
            resolved_table,
            schema=resolved_schema,
            keep_extra_columns=keep_extra_columns,
        )
    session = ctx or DataFusionRuntimeProfile().session_context()
    temp_name = register_temp_table(session, resolved_table, prefix="__schema_align_")
    try:
        selections = _align_projection_exprs(
            schema=resolved_schema,
            input_columns=resolved_table.column_names,
            keep_extra_columns=keep_extra_columns,
        )
        aligned = session.table(temp_name).select(*selections).to_arrow_table()
    finally:
        deregister_table(session, temp_name)
    return _apply_table_schema_metadata(
        aligned,
        schema=resolved_schema,
        keep_extra_columns=keep_extra_columns,
    )


def _schema_has_extension(schema: pa.Schema) -> bool:
    return any(_type_has_extension(field.type) for field in schema)


def _type_has_extension(data_type: pa.DataType) -> bool:
    if isinstance(data_type, pa.ExtensionType):
        return True
    if pa.types.is_struct(data_type):
        return any(_type_has_extension(field.type) for field in data_type)
    if pa.types.is_list(data_type) or pa.types.is_large_list(data_type):
        return _type_has_extension(data_type.value_field.type)
    if pa.types.is_map(data_type):
        return _type_has_extension(data_type.key_field.type) or _type_has_extension(
            data_type.item_field.type
        )
    if pa.types.is_union(data_type):
        return any(_type_has_extension(field.type) for field in data_type)
    return False


def _align_table_with_arrow(
    table: pa.Table,
    *,
    schema: pa.Schema,
    keep_extra_columns: bool,
) -> pa.Table:
    arrays: list[pa.Array | pa.ChunkedArray] = []
    fields: list[pa.Field] = []
    num_rows = int(table.num_rows)
    table_fields = {field.name: field for field in table.schema}
    for schema_field in schema:
        if schema_field.name in table.column_names:
            column = table[schema_field.name]
            if column.type != schema_field.type:
                column = column.cast(schema_field.type)
            arrays.append(column)
        else:
            arrays.append(pa.nulls(num_rows, type=schema_field.type))
        fields.append(schema_field)
    if keep_extra_columns:
        for name in table.column_names:
            if name in schema.names:
                continue
            arrays.append(table[name])
            fields.append(table_fields[name])
    resolved_schema = pa.schema(fields, metadata=schema.metadata)
    return pa.Table.from_arrays(arrays, schema=resolved_schema)


def assert_schema_metadata(
    table: TableLike | RecordBatchReaderLike,
    *,
    schema: SchemaLike,
) -> None:
    """Raise when schema metadata does not match the target schema.

    Args:
        table: Description.
        schema: Description.

    Raises:
        ValueError: If the operation cannot be completed.
    """
    table_schema = pa.schema(table.schema)
    expected_schema = pa.schema(schema)
    if not table_schema.equals(expected_schema, check_metadata=True):
        msg = "Schema metadata mismatch after finalize."
        raise ValueError(msg)


def dataset_schema_from_context(
    name: str,
    *,
    ctx: SessionContext | None = None,
) -> SchemaLike:
    """Return the dataset schema from the DataFusion SessionContext.

    Args:
        name: Description.
        ctx: Description.

    Raises:
        KeyError: If the operation cannot be completed.
    """
    session_ctx = ctx or DataFusionRuntimeProfile().session_context()
    try:
        schema = session_ctx.table(name).schema()
    except (KeyError, RuntimeError, TypeError, ValueError) as exc:
        msg = f"Dataset schema not registered in DataFusion: {name!r}."
        raise KeyError(msg) from exc
    metadata = table_provider_metadata(id(session_ctx), table_name=name)
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
    log_storage_options: Mapping[str, str] | None = None,
    delta_scan: DeltaScanOptions | None = None,
) -> pa.RecordBatchReader:
    """Return a streaming Delta table snapshot using the Delta TableProvider.

    Returns:
    -------
    pyarrow.RecordBatchReader
        Streaming reader for the Delta table via DataFusion's Delta table provider.
    """
    if delta_scan is None:
        delta_scan = DeltaScanOptions(schema_force_view_types=False)
    elif delta_scan.schema_force_view_types is None:
        delta_scan = msgspec.structs.replace(delta_scan, schema_force_view_types=False)
    profile = DataFusionRuntimeProfile()
    ctx = profile.session_context()
    from datafusion_engine.dataset.registry import DatasetLocation, DatasetLocationOverrides
    from datafusion_engine.dataset.resolution import (
        DatasetResolutionRequest,
        resolve_dataset_provider,
    )

    overrides = None
    if delta_scan is not None:
        from schema_spec.system import DeltaPolicyBundle

        overrides = DatasetLocationOverrides(delta=DeltaPolicyBundle(scan=delta_scan))
    location = DatasetLocation(
        path=path,
        format="delta",
        storage_options=dict(storage_options or {}),
        delta_log_storage_options=dict(log_storage_options or {}),
        overrides=overrides,
    )
    resolution = resolve_dataset_provider(
        DatasetResolutionRequest(
            ctx=ctx,
            location=location,
            runtime_profile=profile,
        )
    )
    from datafusion_engine.tables.metadata import TableProviderCapsule

    df = ctx.read_table(TableProviderCapsule(resolution.provider))
    batches = df.collect()
    if batches:
        table = pa.Table.from_batches(batches)
    else:
        schema = df.schema()
        table = pa.Table.from_batches([], schema=schema)
    if "__delta_rs_path" in table.column_names:
        table = table.drop(["__delta_rs_path"])
    return pa.RecordBatchReader.from_batches(table.schema, table.to_batches())


def dataset_spec_from_context(
    name: str,
    *,
    ctx: SessionContext | None = None,
) -> DatasetSpec:
    """Return a DatasetSpec derived from the DataFusion schema.

    Parameters
    ----------
    name : str
        Dataset name registered in the SessionContext.
    ctx : SessionContext | None
        Optional SessionContext override for schema resolution.

    Returns:
    -------
    DatasetSpec
        DatasetSpec derived from the DataFusion schema.
    """
    schema = dataset_schema_from_context(name, ctx=ctx)
    return dataset_spec_from_schema(name, schema)


def record_dataset_readiness(
    profile: DataFusionRuntimeProfile,
    *,
    dataset_names: Sequence[str],
) -> None:
    """Record readiness diagnostics for configured dataset locations."""
    if profile.diagnostics.diagnostics_sink is None:
        return
    from datafusion_engine.lineage.diagnostics import record_artifact
    from obs.otel.heartbeat import set_heartbeat_blockers
    from semantics.compile_context import dataset_bindings_for_profile

    bindings = dataset_bindings_for_profile(profile)
    blockers: list[str] = []
    for name in dataset_names:
        location = bindings.location(name)
        if location is None:
            record_artifact(
                profile,
                "dataset_readiness_v1",
                {
                    "dataset": name,
                    "status": "missing_location",
                    "reason": "no_dataset_location_configured",
                },
            )
            blockers.append(name)
            continue
        readiness = _dataset_readiness_payload(name, location)
        record_artifact(profile, "dataset_readiness_v1", readiness)
        status = readiness.get("status")
        if isinstance(status, str) and status not in {"ok", "remote_path"}:
            blockers.append(name)
    if blockers:
        set_heartbeat_blockers(sorted(set(blockers)))


def _dataset_readiness_payload(name: str, location: DatasetLocation) -> dict[str, object]:
    path_value = str(location.path)
    payload = _base_readiness_payload(name, location, path_value)
    parsed = urlparse(path_value)
    if _is_remote_scheme(parsed.scheme):
        return _remote_readiness_payload(payload)
    path = _resolve_local_path(parsed, path_value)
    return _local_readiness_payload(payload, path, location.format)


def _base_readiness_payload(
    name: str,
    location: DatasetLocation,
    path_value: str,
) -> dict[str, object]:
    return {
        "dataset": name,
        "path": path_value,
        "format": location.format,
        "status": "ok",
    }


def _is_remote_scheme(scheme: str) -> bool:
    return scheme not in {"", "file"}


def _remote_readiness_payload(payload: dict[str, object]) -> dict[str, object]:
    payload.update(
        {
            "status": "remote_path",
            "reason": "remote_scheme",
            "path_exists": None,
            "delta_log_present": None,
        }
    )
    return payload


def _resolve_local_path(parsed: object, path_value: str) -> Path:
    scheme = getattr(parsed, "scheme", "")
    parsed_path = getattr(parsed, "path", "")
    if scheme == "file" and parsed_path:
        return Path(parsed_path)
    return Path(path_value)


def _local_readiness_payload(
    payload: dict[str, object],
    path: Path,
    format_name: str,
) -> dict[str, object]:
    exists = path.exists()
    payload["path_exists"] = exists
    if not exists:
        return _apply_readiness_status(payload, "missing_path", "path_not_found")
    if format_name == "delta":
        return _delta_readiness_payload(payload, path)
    return _file_readiness_payload(payload, path)


def _delta_readiness_payload(payload: dict[str, object], path: Path) -> dict[str, object]:
    delta_log = path / "_delta_log"
    delta_exists = delta_log.exists()
    payload["delta_log_present"] = delta_exists
    if not delta_exists:
        return _apply_readiness_status(payload, "missing_delta_log", "delta_log_missing")
    delta_count = _count_dir_entries(delta_log)
    payload["delta_log_file_count"] = delta_count
    if delta_count == 0:
        return _apply_readiness_status(payload, "empty_delta_log", "delta_log_empty")
    return payload


def _file_readiness_payload(payload: dict[str, object], path: Path) -> dict[str, object]:
    payload["delta_log_present"] = None
    if path.is_dir():
        file_count = _count_dir_entries(path)
        payload["file_count"] = file_count
        if file_count == 0:
            return _apply_readiness_status(payload, "empty_path", "no_files_found")
        return payload
    payload["file_count"] = 1
    return payload


def _count_dir_entries(path: Path) -> int:
    try:
        return sum(1 for _ in path.iterdir())
    except OSError:
        return 0


def _apply_readiness_status(
    payload: dict[str, object],
    status: str,
    reason: str,
) -> dict[str, object]:
    payload["status"] = status
    payload["reason"] = reason
    return payload


__all__ = [
    "CACHE_PROFILES",
    "DATAFUSION_POLICY_PRESETS",
    "DEFAULT_DF_POLICY",
    "DEV_DF_POLICY",
    "PROD_DF_POLICY",
    "SCHEMA_HARDENING_PRESETS",
    "AdapterExecutionPolicy",
    "CatalogConfig",
    "DataFusionConfigPolicy",
    "DataFusionFeatureGates",
    "DataFusionJoinPolicy",
    "DataFusionRuntimeProfile",
    "DataFusionSettingsContract",
    "DataSourceConfig",
    "DiagnosticsConfig",
    "ExecutionConfig",
    "ExecutionLabel",
    "FeatureGatesConfig",
    "FeatureStateSnapshot",
    "MemoryPool",
    "PolicyBundleConfig",
    "PreparedStatementSpec",
    "SchemaHardeningProfile",
    "SessionRuntime",
    "ZeroRowBootstrapConfig",
    "align_table_to_schema",
    "apply_execution_label",
    "apply_execution_policy",
    "assert_schema_metadata",
    "build_session_runtime",
    "cache_prefix_for_delta_snapshot",
    "collect_datafusion_metrics",
    "collect_datafusion_traces",
    "dataset_schema_from_context",
    "dataset_spec_from_context",
    "diagnostics_arrow_ingest_hook",
    "diagnostics_dml_hook",
    "evict_diskcache_entries",
    "extract_output_locations_for_profile",
    "feature_state_snapshot",
    "normalize_dataset_locations_for_profile",
    "read_delta_as_reader",
    "record_dataset_readiness",
    "record_runtime_setting_override",
    "record_schema_snapshots_for_profile",
    "refresh_session_runtime",
    "register_cdf_inputs_for_profile",
    "run_diskcache_maintenance",
    "runtime_setting_overrides",
    "semantic_output_locations_for_profile",
    "session_runtime_for_context",
    "session_runtime_hash",
    "settings_snapshot_for_profile",
    "sql_options_for_profile",
    "statement_sql_options_for_profile",
]

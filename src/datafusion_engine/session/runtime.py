"""Runtime profile helpers for DataFusion execution."""

from __future__ import annotations

import contextlib
import logging
import tempfile
import time
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, replace
from pathlib import Path
from typing import TYPE_CHECKING, cast
from weakref import WeakKeyDictionary

import datafusion
import msgspec
import pyarrow as pa
from datafusion import (
    RuntimeEnvBuilder,
    SessionConfig,
    SessionContext,
    SQLOptions,
)
from datafusion.object_store import LocalFileSystem

from cache.diskcache_factory import (
    DiskCacheKind,
    DiskCacheProfile,
    cache_for_kind,
    diskcache_stats_snapshot,
)
from core.config_base import config_fingerprint
from datafusion_engine.arrow.interop import (
    RecordBatchReaderLike,
    TableLike,
)
from datafusion_engine.compile.options import (
    DataFusionCompileOptions,
    DataFusionSqlPolicy,
    resolve_sql_policy,
)
from datafusion_engine.delta.store_policy import (
    apply_delta_store_policy,
    delta_store_policy_hash,
)
from datafusion_engine.expr.planner import expr_planner_payloads, install_expr_planners
from datafusion_engine.extensions.context_adaptation import (
    resolve_extension_module as _resolve_extension_module_contract,
)
from datafusion_engine.identity import schema_identity_hash
from datafusion_engine.lineage.diagnostics import (
    DiagnosticsSink,
    ensure_recorder_sink,
    record_events,
)
from datafusion_engine.plan.cache import PlanCache, PlanProtoCache
from datafusion_engine.plan.perf_policy import (
    PerformancePolicy,
    performance_policy_artifact_payload,
)
from datafusion_engine.registry_facade import RegistrationPhase, RegistrationPhaseOrchestrator
from datafusion_engine.schema import (
    AST_CORE_VIEW_NAMES,
    AST_OPTIONAL_VIEW_NAMES,
    CST_VIEW_NAMES,
    TREE_SITTER_CHECK_VIEWS,
    TREE_SITTER_VIEW_NAMES,
    extract_nested_dataset_names,
    extract_schema_for,
    missing_schema_names,
    relationship_schema_for,
    relationship_schema_names,
    validate_nested_types,
    validate_required_engine_functions,
    validate_semantic_types,
    validate_udf_info_schema_parity,
)
from datafusion_engine.schema.introspection import (
    SchemaIntrospector,
    catalogs_snapshot,
    constraint_rows,
)
from datafusion_engine.session.cache_policy import cache_policy_settings
from datafusion_engine.session.context_pool import SessionFactory
from datafusion_engine.session.features import (
    FeatureStateSnapshot,
    feature_state_snapshot,
    named_args_supported,
)
from datafusion_engine.session.introspection import register_cdf_inputs_for_profile
from datafusion_engine.session.runtime_compile import (
    _CompileOptionResolution,
    _resolve_prepared_statement_options,
    _ResolvedCompileHooks,
    _supports_explain_analyze_level,
    compile_resolver_invariant_artifact_payload,
    compile_resolver_invariants_strict_mode,
    effective_catalog_autoload,
    effective_ident_normalization,
    record_artifact,
    record_compile_resolver_invariants,
    supports_explain_analyze_level,
)

# ---------------------------------------------------------------------------
# Imports from extracted runtime sub-modules.
#
# These modules were extracted from this file to reduce its size. All public
# and private names are imported here so that existing ``from
# datafusion_engine.session.runtime import X`` patterns continue to work
# without modification.
# ---------------------------------------------------------------------------
from datafusion_engine.session.runtime_config_policies import (
    CACHE_PROFILES,
    CST_AUTOLOAD_DF_POLICY,
    DATAFUSION_MAJOR_VERSION,
    DATAFUSION_OPTIMIZER_DYNAMIC_FILTER_SKIP_VERSION,
    DATAFUSION_POLICY_PRESETS,
    DATAFUSION_RUNTIME_SETTINGS_SKIP_VERSION,
    DEFAULT_DF_POLICY,
    DEV_DF_POLICY,
    GIB,
    KIB,
    MIB,
    PROD_DF_POLICY,
    SCHEMA_HARDENING_PRESETS,
    SYMTABLE_DF_POLICY,
    DataFusionConfigPolicy,
    DataFusionFeatureGates,
    DataFusionJoinPolicy,
    DataFusionSettingsContract,
    SchemaHardeningProfile,
    _ansi_mode,
    _effective_catalog_autoload_for_profile,
    _parse_major_version,
    _resolved_config_policy_for_profile,
    _resolved_schema_hardening_for_profile,
)
from datafusion_engine.session.runtime_dataset_io import (
    _cache_config_payload,
    _cache_snapshot_rows,
    _capture_cache_diagnostics,
    _introspection_cache_for_ctx,
    _register_cache_introspection_functions,
    align_table_to_schema,
    assert_schema_metadata,
    cache_prefix_for_delta_snapshot,
    dataset_schema_from_context,
    dataset_spec_from_context,
    datasource_config_from_manifest,
    datasource_config_from_profile,
    extract_output_locations_for_profile,
    normalize_dataset_locations_for_profile,
    read_delta_as_reader,
    record_dataset_readiness,
    semantic_output_locations_for_profile,
)
from datafusion_engine.session.runtime_hooks import (
    CacheEventHook,
    ExplainHook,
    PlanArtifactsHook,
    SemanticDiffHook,
    SqlIngestHook,
    SubstraitFallbackHook,
    _apply_builder,
    _attach_cache_manager,
    _chain_cache_hooks,
    _chain_explain_hooks,
    _chain_plan_artifacts_hooks,
    _chain_sql_ingest_hooks,
    _chain_substrait_fallback_hooks,
    apply_execution_label,
    apply_execution_policy,
    diagnostics_arrow_ingest_hook,
    diagnostics_cache_hook,
    diagnostics_dml_hook,
    diagnostics_explain_hook,
    diagnostics_plan_artifacts_hook,
    diagnostics_semantic_diff_hook,
    diagnostics_sql_ingest_hook,
    diagnostics_substrait_fallback_hook,
    labeled_explain_hook,
)
from datafusion_engine.session.runtime_profile_config import (
    _AST_OPTIONAL_VIEW_FUNCTIONS,
    CST_DIAGNOSTIC_STATEMENTS,
    INFO_SCHEMA_STATEMENT_NAMES,
    INFO_SCHEMA_STATEMENTS,
    AdapterExecutionPolicy,
    CatalogConfig,
    DataSourceConfig,
    DiagnosticsConfig,
    ExecutionConfig,
    ExecutionLabel,
    ExtractOutputConfig,
    FeatureGatesConfig,
    MemoryPool,
    PolicyBundleConfig,
    PreparedStatementSpec,
    SemanticOutputConfig,
    ZeroRowBootstrapConfig,
)
from datafusion_engine.session.runtime_session import (
    DataFusionViewRegistry,
    SessionRuntime,
    _build_session_runtime_from_context,
    _datafusion_function_names,
    _datafusion_version,
    _ScipRegistrationSnapshot,
    _sql_with_options,
    build_session_runtime,
    catalog_snapshot_for_profile,
    function_catalog_snapshot_for_profile,
    record_runtime_setting_override,
    record_view_definition,
    refresh_session_runtime,
    runtime_setting_overrides,
    session_runtime_for_context,
    session_runtime_hash,
    settings_snapshot_for_profile,
)
from datafusion_engine.session.runtime_telemetry import (
    _SETTINGS_HASH_SCHEMA,
    _TELEMETRY_SCHEMA,
    SETTINGS_HASH_VERSION,
    TELEMETRY_PAYLOAD_VERSION,
    _build_telemetry_payload_row,
    _cache_profile_settings,
    _datafusion_write_policy_payload,
    _default_value_entries,
    _delta_protocol_support_payload,
    _delta_store_policy_payload,
    _encode_telemetry_msgpack,
    _enrich_query_telemetry,
    _extra_settings_payload,
    _map_entries,
    _runtime_settings_payload,
    _settings_by_prefix,
    _telemetry_common_payload,
    _telemetry_enrichment_policy_for_profile,
    performance_policy_applied_knobs,
    performance_policy_settings,
)
from datafusion_engine.session.runtime_telemetry import (
    _effective_ident_normalization as _telemetry_effective_ident_normalization,
)
from datafusion_engine.session.runtime_telemetry import (
    _identifier_normalization_mode as _telemetry_identifier_normalization_mode,
)
from datafusion_engine.session.runtime_udf import (
    SchemaRegistryValidationResult,
    _collect_view_sql_parse_errors,
    _constraint_drift_entries,
    _install_schema_evolution_adapter_factory,
    _load_schema_evolution_adapter_factory,
    _prepare_statement_sql,
    _register_schema_table,
    _relationship_constraint_errors,
    _resolve_planner_rule_installers,
    _rulepack_function_errors,
    _rulepack_required_functions,
    _table_dfschema_tree,
    _table_logical_plan,
)
from datafusion_engine.sql.options import (
    sql_options_for_profile,
    statement_sql_options_for_profile,
)
from datafusion_engine.udf.extension_runtime import ExtensionRegistries
from datafusion_engine.udf.factory import function_factory_payloads, install_function_factory
from datafusion_engine.udf.metadata import get_default_udf_catalog, get_strict_udf_catalog
from datafusion_engine.udf.platform import RustUdfPlatformRegistries
from datafusion_engine.views.artifacts import DataFusionViewArtifact
from serde_msgspec import MSGPACK_ENCODER, StructBaseStrict
from storage.ipc_utils import payload_hash
from utils.validation import find_missing

# Use the telemetry versions for _RuntimeDiagnosticsMixin compatibility.
_identifier_normalization_mode = _telemetry_identifier_normalization_mode
_effective_ident_normalization = _telemetry_effective_ident_normalization

_MISSING = object()
_COMPILE_RESOLVER_STRICT_ENV = "CODEANATOMY_COMPILE_RESOLVER_INVARIANTS_STRICT"
_CI_ENV = "CI"
_DEFAULT_PERFORMANCE_POLICY = PerformancePolicy()
_EXTENSION_MODULE_NAMES: tuple[str, ...] = ("datafusion_engine.extensions.datafusion_ext",)
# DataFusion Python currently raises plain ``Exception`` for many SQL/plan failures.
_DATAFUSION_SQL_ERROR = Exception

if TYPE_CHECKING:
    from typing import Protocol

    from diskcache import Cache, FanoutCache

    from datafusion_engine.bootstrap.zero_row import ZeroRowBootstrapReport, ZeroRowBootstrapRequest
    from datafusion_engine.delta.service import DeltaService
    from datafusion_engine.session.context_pool import DataFusionContextPool
    from datafusion_engine.udf.metadata import UdfCatalog
    from obs.datafusion_runs import DataFusionRun
    from semantics.program_manifest import ManifestDatasetResolver
    from serde_schema_registry import ArtifactSpec
    from storage.deltalake.delta import IdempotentWriteOptions

    class _DeltaRuntimeEnvOptions(Protocol):
        max_spill_size: int | None
        max_temp_directory_size: int | None


from datafusion_engine.dataset.registry import (
    DatasetLocation,
)

if TYPE_CHECKING:
    ExplainRows = TableLike | RecordBatchReaderLike
else:
    ExplainRows = object

_TELEMETRY_MSGPACK_ENCODER = MSGPACK_ENCODER

logger = logging.getLogger(__name__)

_SESSION_CONTEXT_CACHE: dict[str, SessionContext] = {}


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


def _resolve_runtime_extension_module(required_attr: str | None = None) -> object | None:
    resolved = _resolve_extension_module_contract(
        _EXTENSION_MODULE_NAMES,
        required_attr=required_attr,
    )
    if resolved is None:
        return None
    _module_name, module = resolved
    return module


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
    module = _resolve_runtime_extension_module(required_attr="DeltaRuntimeEnvOptions")
    if module is None:
        msg = "Delta runtime env options require datafusion_ext."
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
        DATAFUSION_DELTA_SESSION_DEFAULTS_SPEC,
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
    profile.record_artifact(
        DATAFUSION_SCHEMA_INTROSPECTION_SPEC,
        payload,
    )


class _RuntimeDiagnosticsMixin:
    def record_artifact(self, name: ArtifactSpec, payload: Mapping[str, object]) -> None:
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
        payload.update(performance_policy_settings(profile))
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
        common_payload = _telemetry_common_payload(profile)
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
        payload: dict[str, object] = {
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
            "identifier_normalization_mode": _identifier_normalization_mode(profile).value,
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
            "settings_overrides": dict(policies.settings_overrides),
            **common_payload,
            "share_context": execution.share_context,
            "session_context_key": execution.session_context_key,
            "zero_row_bootstrap": profile.zero_row_bootstrap.fingerprint_payload(),
        }
        return _enrich_query_telemetry(
            payload,
            profile=profile,
            policy=_telemetry_enrichment_policy_for_profile(profile),
        )

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
        common_payload = _telemetry_common_payload(profile)
        return _enrich_query_telemetry(
            {
                "version": 2,
                "datafusion_version": datafusion.__version__,
                "schema_hardening_name": policies.schema_hardening_name,
                "session_config": dict(settings),
                **common_payload,
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
                    "identifier_normalization_mode": _identifier_normalization_mode(profile).value,
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
                    "datafusion_write_policy": _datafusion_write_policy_payload(
                        policies.write_policy
                    ),
                },
                "zero_row_bootstrap": profile.zero_row_bootstrap.fingerprint_payload(),
            },
            profile=profile,
            policy=_telemetry_enrichment_policy_for_profile(profile),
        )

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
            self.profile.record_artifact(
                DATAFUSION_DELTA_COMMIT_SPEC,
                payload,
            )
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
        self.profile.record_artifact(
            DATAFUSION_DELTA_COMMIT_SPEC,
            payload,
        )

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

    def dataset_location(
        self,
        name: str,
        *,
        dataset_resolver: ManifestDatasetResolver,
    ) -> DatasetLocation | None:
        """Return a configured dataset location for the dataset name.

        Parameters
        ----------
        name
            Dataset name to resolve.
        dataset_resolver
            Pre-resolved manifest resolver.

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
                    from schema_spec.contracts import DeltaPolicyBundle as _DeltaPolicyBundle

                    delta_bundle = _DeltaPolicyBundle(scan=delta_scan)
                else:
                    delta_bundle = msgspec.structs.replace(delta_bundle, scan=delta_scan)
                overrides = msgspec.structs.replace(overrides, delta=delta_bundle)
            return msgspec.structs.replace(resolved, overrides=overrides)
        return dataset_resolver.location(name)

    def dataset_location_or_raise(
        self,
        name: str,
        *,
        dataset_resolver: ManifestDatasetResolver,
    ) -> DatasetLocation:
        """Return a configured dataset location for the dataset name.

        Args:
            name: Description.
            dataset_resolver: Pre-resolved manifest resolver.

        Raises:
            KeyError: If the operation cannot be completed.
        """
        location = self.dataset_location(name, dataset_resolver=dataset_resolver)
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

    @staticmethod
    def dataset_location(
        name: str,
        *,
        dataset_resolver: ManifestDatasetResolver,
    ) -> DatasetLocation | None:
        """Return a configured dataset location for the dataset name.

        Parameters
        ----------
        name
            Dataset name to resolve.
        dataset_resolver
            Pre-resolved manifest resolver.

        Returns:
        -------
        DatasetLocation | None
            Dataset location when configured.
        """
        return dataset_resolver.location(name)


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
    udf_extension_registries: ExtensionRegistries = msgspec.field(
        default_factory=ExtensionRegistries
    )
    udf_platform_registries: RustUdfPlatformRegistries = msgspec.field(
        default_factory=RustUdfPlatformRegistries
    )
    view_registry: DataFusionViewRegistry | None = msgspec.field(
        default_factory=DataFusionViewRegistry
    )
    plan_cache: PlanCache | None = None
    plan_proto_cache: PlanProtoCache | None = None
    udf_catalog_cache: WeakKeyDictionary[SessionContext, UdfCatalog] = msgspec.field(
        default_factory=WeakKeyDictionary
    )
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
        module = _resolve_runtime_extension_module(required_attr="DeltaRuntimeEnvOptions")
        if module is None:
            msg = "Delta runtime env options require datafusion_ext."
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
        """Install Delta plan codecs using the extension entrypoint.

        Returns:
        -------
        tuple[bool, bool]
            Tuple of (available, installed) flags.
        """
        return self._install_delta_plan_codecs_extension(ctx)

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
        self.record_artifact(
            SEMANTIC_PROGRAM_MANIFEST_SPEC,
            manifest.payload(),
        )
        report = run_bootstrap_validation(
            self,
            request=resolved_request,
            ctx=active_ctx,
            manifest=manifest,
        )
        self.record_artifact(
            ZERO_ROW_BOOTSTRAP_VALIDATION_SPEC,
            report.payload(),
        )
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
        from datafusion_engine.session.context_pool import DataFusionContextPool

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
        from datafusion_engine.udf.extension_runtime import extension_capabilities_report

        try:
            capabilities = extension_capabilities_report()
        except (RuntimeError, TypeError, ValueError):
            capabilities = {"available": False, "compatible": False}
        if not (bool(capabilities.get("available")) and bool(capabilities.get("compatible"))):
            return {
                "missing_in_information_schema": [],
                "routines_available": False,
                "error": "native_udf_platform unavailable",
            }
        from datafusion_engine.udf.extension_runtime import rust_runtime_install_payload
        from datafusion_engine.udf.parity import udf_info_schema_parity_report

        report = udf_info_schema_parity_report(ctx)
        _runtime_payload = rust_runtime_install_payload(
            ctx,
            registries=self.udf_extension_registries,
        )
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
        from datafusion_engine.udf.contracts import InstallRustUdfPlatformRequestV1
        from datafusion_engine.udf.extension_runtime import extension_capabilities_report
        from datafusion_engine.udf.platform import (
            RustUdfPlatformOptions,
            install_rust_udf_platform,
        )

        try:
            capabilities = extension_capabilities_report()
        except (RuntimeError, TypeError, ValueError):
            capabilities = {"available": False, "compatible": False}

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
            strict=bool(capabilities.get("available")) and bool(capabilities.get("compatible")),
        )
        platform = install_rust_udf_platform(
            InstallRustUdfPlatformRequestV1(options=msgspec.to_builtins(options)),
            ctx=ctx,
            registries=self.udf_platform_registries,
            extension_registries=self.udf_extension_registries,
        )
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
            from datafusion_engine.udf.extension_runtime import register_udfs_via_ddl

            try:
                register_udfs_via_ddl(
                    ctx,
                    snapshot=platform.snapshot,
                    registries=self.udf_extension_registries,
                )
            except _DATAFUSION_SQL_ERROR as exc:
                logging.getLogger(__name__).warning(
                    "Skipping UDF DDL catalog registration due to extension SQL incompatibility: %s",
                    exc,
                )
        if platform.snapshot is not None:
            self._refresh_udf_catalog(ctx)
        else:
            self.udf_catalog_cache.pop(ctx, None)

    def _install_planner_rules(self, ctx: SessionContext) -> None:
        """Install Rust planner policy rules for the session context.

        Args:
            ctx: Description.

        Raises:
            RuntimeError: If the operation cannot be completed.
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
            installers.physical_config_installer(
                ctx,
                self.policies.physical_rulepack_enabled,
            )
            installers.rule_installer(ctx)
            installers.physical_installer(ctx)
        except (RuntimeError, TypeError, ValueError) as exc:
            msg = (
                "Planner-rule install failed due to SessionContext ABI mismatch. "
                "Rebuild and install matching datafusion/datafusion_ext wheels "
                "(scripts/build_datafusion_wheels.sh + uv sync)."
            )
            raise RuntimeError(msg) from exc
        return

    def _refresh_udf_catalog(self, ctx: SessionContext) -> None:
        if not self.catalog.enable_information_schema:
            msg = "UdfCatalog requires information_schema to be enabled."
            raise ValueError(msg)
        cache_key = ctx
        try:
            introspector = self._schema_introspector(ctx)
            if self.policies.udf_catalog_policy == "strict":
                catalog = get_strict_udf_catalog(
                    introspector=introspector,
                    registries=self.udf_extension_registries,
                )
            else:
                catalog = get_default_udf_catalog(
                    introspector=introspector,
                    registries=self.udf_extension_registries,
                )
        except (RuntimeError, TypeError, ValueError) as exc:
            logging.getLogger(__name__).warning(
                "Skipping UDF catalog refresh due to DataFusion expression ABI mismatch: %s",
                exc,
            )
            self.udf_catalog_cache.pop(cache_key, None)
            return
        try:
            self._validate_udf_specs(catalog, introspector=introspector)
        except (RuntimeError, TypeError, ValueError) as exc:
            logging.getLogger(__name__).warning(
                "Using degraded UDF catalog snapshot due to DataFusion expression ABI mismatch: %s",
                exc,
            )
        self.udf_catalog_cache[cache_key] = catalog

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
        from datafusion_engine.udf.extension_runtime import (
            rust_udf_snapshot,
            udf_names_from_snapshot,
        )

        registry_snapshot = rust_udf_snapshot(
            introspector.ctx,
            registries=self.udf_extension_registries,
        )
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
                    DATAFUSION_UDF_VALIDATION_SPEC,
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

        Args:
            ctx: DataFusion session context.

        Returns:
            Cached UDF catalog for the session.

        Raises:
            RuntimeError: If the UDF catalog cannot be resolved for the session context.
        """
        cache_key = ctx
        catalog = self.udf_catalog_cache.get(cache_key)
        if catalog is None:
            self._refresh_udf_catalog(ctx)
            catalog = self.udf_catalog_cache.get(cache_key)
        if catalog is None:
            msg = "UDF catalog is unavailable for the current DataFusion session context."
            raise RuntimeError(msg)
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
        from datafusion_engine.udf.extension_runtime import rust_udf_snapshot
        from datafusion_engine.udf.factory import function_factory_policy_hash

        snapshot = rust_udf_snapshot(ctx, registries=self.udf_extension_registries)
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
        self.record_artifact(
            DATAFUSION_SCHEMA_REGISTRY_VALIDATION_SPEC,
            payload,
        )
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
        self.record_artifact(DATAFUSION_CATALOG_AUTOLOAD_SPEC, payload)

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
        self.record_artifact(DATAFUSION_AST_FEATURE_GATES_SPEC, payload)

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
        self.record_artifact(DATAFUSION_AST_SPAN_METADATA_SPEC, payload)

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

    def _registration_artifact_payload(  # noqa: PLR6301
        self,
        *,
        name: str,
        location: DatasetLocation,
        extra: Mapping[str, object] | None = None,
    ) -> dict[str, object]:
        resolved = location.resolved
        scan = resolved.datafusion_scan
        payload: dict[str, object] = {
            "event_time_unix_ms": int(time.time() * 1000),
            "name": name,
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
        }
        if extra is not None:
            payload.update(extra)
        return payload

    def _record_ast_registration(self, *, location: DatasetLocation) -> None:
        if self.diagnostics.diagnostics_sink is None:
            return
        payload = self._registration_artifact_payload(name="ast_files_v1", location=location)
        self.record_artifact(DATAFUSION_AST_DATASET_SPEC, payload)

    def _record_bytecode_registration(self, *, location: DatasetLocation) -> None:
        if self.diagnostics.diagnostics_sink is None:
            return
        payload = self._registration_artifact_payload(name="bytecode_files_v1", location=location)
        self.record_artifact(DATAFUSION_BYTECODE_DATASET_SPEC, payload)

    def _record_scip_registration(
        self,
        *,
        snapshot: _ScipRegistrationSnapshot,
    ) -> None:
        if self.diagnostics.diagnostics_sink is None:
            return
        payload = self._registration_artifact_payload(
            name=snapshot.name,
            location=snapshot.location,
            extra={
                "expected_schema_identity_hash": snapshot.expected_fingerprint,
                "observed_schema_identity_hash": snapshot.actual_fingerprint,
                "schema_match": snapshot.schema_match,
            },
        )
        self.record_artifact(DATAFUSION_SCIP_DATASETS_SPEC, payload)

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
            self.record_artifact(
                DATAFUSION_CST_SCHEMA_DIAGNOSTICS_SPEC,
                payload,
            )
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
        self.record_artifact(
            DATAFUSION_CST_SCHEMA_DIAGNOSTICS_SPEC,
            payload,
        )

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
        self.record_artifact(DATAFUSION_TREE_SITTER_STATS_SPEC, payload)

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
            DATAFUSION_TREE_SITTER_PLAN_SCHEMA_SPEC,
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
        self.record_artifact(
            DATAFUSION_TREE_SITTER_CROSS_CHECKS_SPEC,
            payload,
        )
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
        self.record_artifact(DATAFUSION_CST_VIEW_PLANS_SPEC, payload)

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
        self.record_artifact(DATAFUSION_CST_DFSCHEMA_SPEC, payload)

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
        self.record_artifact(DATAFUSION_BYTECODE_METADATA_SPEC, payload)

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
            DATAFUSION_SCHEMA_INTROSPECTION_SPEC,
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
        from datafusion_engine.udf.extension_runtime import extension_capabilities_report

        _ = ast_view_names
        view_errors: dict[str, str] = {}
        try:
            capabilities = extension_capabilities_report()
        except (RuntimeError, TypeError, ValueError):
            capabilities = {"available": False, "compatible": False}
        if bool(capabilities.get("available")) and bool(capabilities.get("compatible")):
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

    @staticmethod
    def _semantic_input_schema_names() -> tuple[str, ...]:
        from semantics.input_registry import SEMANTIC_INPUT_SPECS

        return tuple(dict.fromkeys(spec.extraction_source for spec in SEMANTIC_INPUT_SPECS))

    def _register_schema_registry_tables(self, ctx: SessionContext) -> None:
        from datafusion_engine.extract.registry import dataset_schema as extract_dataset_schema

        self._register_schema_tables(
            ctx,
            names=extract_nested_dataset_names(),
            resolver=extract_schema_for,
        )
        self._register_schema_tables(
            ctx,
            names=self._semantic_input_schema_names(),
            resolver=extract_dataset_schema,
        )
        self._register_schema_tables(
            ctx,
            names=relationship_schema_names(),
            resolver=relationship_schema_for,
        )

    def _prepare_schema_registry_context(
        self,
        ctx: SessionContext,
    ) -> tuple[Sequence[str], bool, bool]:
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
        self._register_schema_registry_tables(ctx)
        return ast_view_names, ast_registration, bytecode_registration

    def _install_schema_registry(self, ctx: SessionContext) -> None:
        """Register canonical nested schemas on the session context.

        Args:
            ctx: Description.

        Raises:
            ValueError: If the operation cannot be completed.
        """
        if not self.features.enable_schema_registry:
            return
        ast_view_names, ast_registration, bytecode_registration = (
            self._prepare_schema_registry_context(ctx)
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
                SCHEMA_REGISTRY_VALIDATION_ADVISORY_SPEC,
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
            DATAFUSION_PREPARED_STATEMENTS_SPEC,
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
        module = _resolve_runtime_extension_module(required_attr="install_delta_plan_codecs")
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

    def _record_udf_snapshot(self, snapshot: Mapping[str, object]) -> None:
        if self.diagnostics.diagnostics_sink is None:
            return
        self.record_artifact(
            DATAFUSION_UDF_REGISTRY_SPEC,
            dict(snapshot),
        )

    def _record_udf_docs(self, docs: Mapping[str, object]) -> None:
        if self.diagnostics.diagnostics_sink is None:
            return
        self.record_artifact(
            DATAFUSION_UDF_DOCS_SPEC,
            dict(docs),
        )

    def _record_delta_plan_codecs(self, *, available: bool, installed: bool) -> None:
        if self.diagnostics.diagnostics_sink is None:
            return
        self.record_artifact(
            DATAFUSION_DELTA_PLAN_CODECS_SPEC,
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
            DATAFUSION_DELTA_SESSION_DEFAULTS_SPEC,
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
        from datafusion_engine.udf.extension_runtime import extension_capabilities_report

        payload["extension_capabilities"] = extension_capabilities_report()
        runtime_capabilities = self._runtime_capabilities_payload(ctx)
        payload["runtime_capabilities"] = runtime_capabilities
        payload["event_time_unix_ms"] = int(time.time() * 1000)
        payload["profile_name"] = self.policies.config_policy_name
        payload["settings_hash"] = self.settings_hash()
        self.record_artifact(
            DATAFUSION_EXTENSION_PARITY_SPEC,
            payload,
        )
        self.record_artifact(
            DATAFUSION_RUNTIME_CAPABILITIES_SPEC,
            runtime_capabilities,
        )
        self._record_performance_policy(runtime_capabilities=runtime_capabilities)

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

    def _record_performance_policy(
        self,
        *,
        runtime_capabilities: Mapping[str, object] | None,
    ) -> None:
        if self.diagnostics.diagnostics_sink is None:
            return
        policy_payload = performance_policy_artifact_payload(
            self.policies.performance_policy,
            applied_knobs=performance_policy_applied_knobs(
                self,
                runtime_capabilities=runtime_capabilities,
            ),
        )
        self.record_artifact(
            PERFORMANCE_POLICY_SPEC,
            policy_payload,
        )

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
        self.record_artifact(DATAFUSION_CACHE_CONFIG_SPEC, config_payload)
        self.record_artifact(
            DATAFUSION_CACHE_ROOT_SPEC,
            {"cache_root": self.io_ops.cache_root()},
        )
        if self.policies.cache_policy is not None:
            self.record_artifact(
                CACHE_POLICY_SPEC,
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
                DATAFUSION_CACHE_SNAPSHOT_ERROR_SPEC,
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
            msg = "Cache table functions require datafusion_ext."
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
            DATAFUSION_EXPR_PLANNERS_SPEC,
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
            DATAFUSION_FUNCTION_FACTORY_SPEC,
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
        self.record_artifact(DATAFUSION_TRACING_INSTALL_SPEC, payload)

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
                DATAFUSION_DELTA_TRACING_SPEC,
                {
                    "enabled": self.features.enable_tracing,
                    "installed": delta_installed,
                    "error": delta_error,
                },
            )
        if self.diagnostics.tracing_hook is None:
            module = _resolve_runtime_extension_module(required_attr="install_tracing")
            if module is None:
                msg = "Tracing enabled but datafusion_ext is unavailable."
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


from datafusion_engine.session.introspection import (
    schema_introspector_for_profile,
)

__all__ = [
    "CACHE_PROFILES",
    "CST_AUTOLOAD_DF_POLICY",
    "CST_DIAGNOSTIC_STATEMENTS",
    "DATAFUSION_MAJOR_VERSION",
    "DATAFUSION_OPTIMIZER_DYNAMIC_FILTER_SKIP_VERSION",
    "DATAFUSION_POLICY_PRESETS",
    "DATAFUSION_RUNTIME_SETTINGS_SKIP_VERSION",
    "DEFAULT_DF_POLICY",
    "DEV_DF_POLICY",
    "GIB",
    "INFO_SCHEMA_STATEMENTS",
    "INFO_SCHEMA_STATEMENT_NAMES",
    "KIB",
    "MIB",
    "PROD_DF_POLICY",
    "SCHEMA_HARDENING_PRESETS",
    # Telemetry
    "SETTINGS_HASH_VERSION",
    "SYMTABLE_DF_POLICY",
    "TELEMETRY_PAYLOAD_VERSION",
    "AdapterExecutionPolicy",
    "CacheEventHook",
    "CatalogConfig",
    # Config policies
    "DataFusionConfigPolicy",
    "DataFusionFeatureGates",
    "DataFusionJoinPolicy",
    # Core types
    "DataFusionRuntimeProfile",
    "DataFusionSettingsContract",
    "DataFusionViewRegistry",
    "DataSourceConfig",
    "DiagnosticsConfig",
    # Config structs
    "ExecutionConfig",
    "ExecutionLabel",
    # Hooks
    "ExplainHook",
    "ExtractOutputConfig",
    "FeatureGatesConfig",
    # Features
    "FeatureStateSnapshot",
    "MemoryPool",
    "PlanArtifactsHook",
    "PolicyBundleConfig",
    "PreparedStatementSpec",
    "SchemaHardeningProfile",
    # UDF helpers
    "SchemaRegistryValidationResult",
    "SemanticDiffHook",
    "SemanticOutputConfig",
    "SessionRuntime",
    "SqlIngestHook",
    "SubstraitFallbackHook",
    "ZeroRowBootstrapConfig",
    # Dataset IO
    "align_table_to_schema",
    "apply_execution_label",
    "apply_execution_policy",
    "assert_schema_metadata",
    # Session runtime
    "build_session_runtime",
    "cache_prefix_for_delta_snapshot",
    "catalog_snapshot_for_profile",
    "compile_resolver_invariant_artifact_payload",
    "compile_resolver_invariants_strict_mode",
    "dataset_schema_from_context",
    "dataset_spec_from_context",
    "datasource_config_from_manifest",
    "datasource_config_from_profile",
    "diagnostics_arrow_ingest_hook",
    "diagnostics_cache_hook",
    "diagnostics_dml_hook",
    "diagnostics_explain_hook",
    "diagnostics_plan_artifacts_hook",
    "diagnostics_semantic_diff_hook",
    "diagnostics_sql_ingest_hook",
    "diagnostics_substrait_fallback_hook",
    "effective_catalog_autoload",
    "effective_ident_normalization",
    "extract_output_locations_for_profile",
    "feature_state_snapshot",
    "function_catalog_snapshot_for_profile",
    "labeled_explain_hook",
    "normalize_dataset_locations_for_profile",
    "performance_policy_applied_knobs",
    "performance_policy_settings",
    "read_delta_as_reader",
    # Compile helpers
    "record_artifact",
    "record_compile_resolver_invariants",
    "record_dataset_readiness",
    "record_runtime_setting_override",
    "record_view_definition",
    "refresh_session_runtime",
    # Introspection
    "register_cdf_inputs_for_profile",
    "runtime_setting_overrides",
    "semantic_output_locations_for_profile",
    "session_runtime_for_context",
    "session_runtime_hash",
    "settings_snapshot_for_profile",
    # SQL options (re-exports)
    "sql_options_for_profile",
    "statement_sql_options_for_profile",
    "supports_explain_analyze_level",
]

# ---------------------------------------------------------------------------
# Deferred import of artifact spec constants.
#
# ``serde_artifact_specs`` transitively imports modules that depend on symbols
# defined *above* in this very file (``SessionRuntime``, ``dataset_spec_from_context``,
# etc.).  Importing the module at the top of the file would create a circular
# import chain.  By placing the import here -- after all definitions are
# complete and ``__all__`` is declared -- every name that the downstream
# modules need is already in scope.
# ---------------------------------------------------------------------------
from serde_artifact_specs import (
    CACHE_POLICY_SPEC,
    DATAFUSION_AST_DATASET_SPEC,
    DATAFUSION_AST_FEATURE_GATES_SPEC,
    DATAFUSION_AST_SPAN_METADATA_SPEC,
    DATAFUSION_BYTECODE_DATASET_SPEC,
    DATAFUSION_BYTECODE_METADATA_SPEC,
    DATAFUSION_CACHE_CONFIG_SPEC,
    DATAFUSION_CACHE_ROOT_SPEC,
    DATAFUSION_CACHE_SNAPSHOT_ERROR_SPEC,
    DATAFUSION_CATALOG_AUTOLOAD_SPEC,
    DATAFUSION_CST_DFSCHEMA_SPEC,
    DATAFUSION_CST_SCHEMA_DIAGNOSTICS_SPEC,
    DATAFUSION_CST_VIEW_PLANS_SPEC,
    DATAFUSION_DELTA_COMMIT_SPEC,
    DATAFUSION_DELTA_PLAN_CODECS_SPEC,
    DATAFUSION_DELTA_SESSION_DEFAULTS_SPEC,
    DATAFUSION_DELTA_TRACING_SPEC,
    DATAFUSION_EXPR_PLANNERS_SPEC,
    DATAFUSION_EXTENSION_PARITY_SPEC,
    DATAFUSION_FUNCTION_FACTORY_SPEC,
    DATAFUSION_PREPARED_STATEMENTS_SPEC,
    DATAFUSION_RUNTIME_CAPABILITIES_SPEC,
    DATAFUSION_SCHEMA_INTROSPECTION_SPEC,
    DATAFUSION_SCHEMA_REGISTRY_VALIDATION_SPEC,
    DATAFUSION_SCIP_DATASETS_SPEC,
    DATAFUSION_TRACING_INSTALL_SPEC,
    DATAFUSION_TREE_SITTER_CROSS_CHECKS_SPEC,
    DATAFUSION_TREE_SITTER_PLAN_SCHEMA_SPEC,
    DATAFUSION_TREE_SITTER_STATS_SPEC,
    DATAFUSION_UDF_DOCS_SPEC,
    DATAFUSION_UDF_REGISTRY_SPEC,
    DATAFUSION_UDF_VALIDATION_SPEC,
    PERFORMANCE_POLICY_SPEC,
    SCHEMA_REGISTRY_VALIDATION_ADVISORY_SPEC,
    SEMANTIC_PROGRAM_MANIFEST_SPEC,
    ZERO_ROW_BOOTSTRAP_VALIDATION_SPEC,
)

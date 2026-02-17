"""Extension installation, UDF platform, tracing, and cache management for runtime profiles."""

from __future__ import annotations

import importlib
import logging
import time
from collections.abc import Callable, Iterable, Mapping
from typing import TYPE_CHECKING, cast

import msgspec
from datafusion import SessionContext

from cache.diskcache_factory import (
    DiskCacheKind,
    DiskCacheProfile,
    cache_for_kind,
    diskcache_stats_snapshot,
)
from datafusion_engine.compile.options import DataFusionSqlPolicy, resolve_sql_policy
from datafusion_engine.expr.planner import expr_planner_payloads, install_expr_planners
from datafusion_engine.schema.introspection_core import SchemaIntrospector
from datafusion_engine.session._session_constants import (
    DATAFUSION_SQL_ERROR,
    create_schema_introspector,
)
from datafusion_engine.session.runtime_dataset_io import (
    _cache_config_payload,
    _cache_snapshot_rows,
    _capture_cache_diagnostics,
    _register_cache_introspection_functions,
)
from datafusion_engine.session.runtime_ops import _resolve_runtime_extension_module
from datafusion_engine.session.runtime_session import (
    function_catalog_snapshot_for_profile,
)
from datafusion_engine.session.runtime_udf import (
    _install_schema_evolution_adapter_factory,
    _load_schema_evolution_adapter_factory,
    _resolve_planner_rule_installers,
    _rulepack_function_errors,
    _rulepack_required_functions,
)
from datafusion_engine.sql.options import sql_options_for_profile
from datafusion_engine.udf.extension_validation import extension_capabilities_report
from datafusion_engine.udf.factory import (
    function_factory_payloads,
    install_function_factory,
)
from datafusion_engine.udf.metadata import (
    UdfCatalog,
    get_default_udf_catalog,
    get_strict_udf_catalog,
)
from datafusion_engine.udf.parity import udf_info_schema_parity_report

if TYPE_CHECKING:
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile
    from serde_schema_registry import ArtifactSpec

logger = logging.getLogger(__name__)
_DDL_CATALOG_WARNING_STATE: dict[str, bool] = {"emitted": False}


def _deferred_import(module_path: str, attr_name: str) -> object:
    module = importlib.import_module(module_path)
    return getattr(module, attr_name)


def _artifact_spec(name: str) -> ArtifactSpec:
    return cast("ArtifactSpec", _deferred_import("serde_artifact_specs", name))


def _resolve_tracing_context(
    ctx: SessionContext,
    module: object,
) -> tuple[object | None, dict[str, object]]:
    """Resolve the appropriate SessionContext for tracing install.

    Args:
        ctx: DataFusion session context.
        module: Extension module providing tracing install function.

    Returns:
        Tuple of (resolved context argument or None, details dict).
    """
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


def _install_udf_platform(profile: DataFusionRuntimeProfile, ctx: SessionContext) -> None:
    """Install the unified Rust UDF platform on the session context.

    Args:
        profile: Runtime profile.
        ctx: DataFusion session context.
    """
    from datafusion_engine.udf.contracts import InstallRustUdfPlatformRequestV1
    from datafusion_engine.udf.platform import (
        RustUdfPlatformOptions,
        install_rust_udf_platform,
    )

    try:
        capabilities = extension_capabilities_report()
    except (RuntimeError, TypeError, ValueError):
        capabilities = {"available": False, "compatible": False}

    options = RustUdfPlatformOptions(
        enable_udfs=profile.features.enable_udfs,
        enable_async_udfs=profile.features.enable_async_udfs,
        async_udf_timeout_ms=profile.policies.async_udf_timeout_ms,
        async_udf_batch_size=profile.policies.async_udf_batch_size,
        enable_function_factory=profile.features.enable_function_factory,
        enable_expr_planners=profile.features.enable_expr_planners,
        function_factory_hook=profile.policies.function_factory_hook,
        expr_planner_hook=profile.policies.expr_planner_hook,
        expr_planner_names=profile.policies.expr_planner_names,
        strict=bool(capabilities.get("available")) and bool(capabilities.get("compatible")),
    )
    platform = install_rust_udf_platform(
        InstallRustUdfPlatformRequestV1(options=msgspec.to_builtins(options)),
        ctx=ctx,
        registries=profile.udf_platform_registries,
        extension_registries=profile.udf_extension_registries,
    )
    if platform.snapshot is not None:
        _record_udf_snapshot(profile, platform.snapshot)
    if platform.docs is not None and profile.diagnostics.diagnostics_sink is not None:
        _record_udf_docs(profile, platform.docs)
    if platform.function_factory is not None:
        _record_function_factory(
            profile,
            available=platform.function_factory.available,
            installed=platform.function_factory.installed,
            error=platform.function_factory.error,
            policy=platform.function_factory_policy,
        )
    if platform.expr_planners is not None:
        _record_expr_planners(
            profile,
            available=platform.expr_planners.available,
            installed=platform.expr_planners.installed,
            error=platform.expr_planners.error,
            policy=platform.expr_planner_policy,
        )
    if (
        profile.catalog.enable_information_schema
        and platform.snapshot is not None
        and platform.function_factory is not None
        and platform.function_factory.installed
    ):
        from datafusion_engine.udf.extension_core import register_udfs_via_ddl

        try:
            register_udfs_via_ddl(
                ctx,
                snapshot=platform.snapshot,
                registries=profile.udf_extension_registries,
            )
        except DATAFUSION_SQL_ERROR as exc:
            if not _DDL_CATALOG_WARNING_STATE["emitted"]:
                logger.warning(
                    "Skipping UDF DDL catalog registration due to extension SQL incompatibility: %s",
                    exc,
                )
                _DDL_CATALOG_WARNING_STATE["emitted"] = True
    if platform.snapshot is not None:
        _refresh_udf_catalog(profile, ctx)
    else:
        profile.udf_catalog_cache.pop(ctx, None)


def _install_planner_rules(profile: DataFusionRuntimeProfile, ctx: SessionContext) -> None:
    """Install Rust planner policy rules for the session context.

    Args:
        profile: Runtime profile.
        ctx: DataFusion session context.

    Raises:
        RuntimeError: If the operation cannot be completed.
    """
    # Resolve SQL policy from profile configuration
    sql_policy = profile.policies.sql_policy
    if sql_policy is None and profile.policies.sql_policy_name is not None:
        sql_policy = resolve_sql_policy(profile.policies.sql_policy_name)
    if sql_policy is None:
        sql_policy = DataFusionSqlPolicy()

    installers = _resolve_planner_rule_installers()
    if installers is None:
        return
    try:
        installers.config_installer(
            ctx,
            sql_policy.allow_ddl,
            sql_policy.allow_dml,
            sql_policy.allow_statements,
        )
        installers.physical_config_installer(
            ctx,
            profile.policies.physical_rulepack_enabled,
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


def _refresh_udf_catalog(profile: DataFusionRuntimeProfile, ctx: SessionContext) -> None:
    """Refresh the UDF catalog cache for a session context.

    Args:
        profile: Runtime profile.
        ctx: DataFusion session context.

    Raises:
        ValueError: If information_schema is not enabled.
    """
    if not profile.catalog.enable_information_schema:
        msg = "UdfCatalog requires information_schema to be enabled."
        raise ValueError(msg)
    cache_key = ctx
    try:
        introspector = create_schema_introspector(profile, ctx)
        if profile.policies.udf_catalog_policy == "strict":
            catalog = get_strict_udf_catalog(
                introspector=introspector,
                registries=profile.udf_extension_registries,
            )
        else:
            catalog = get_default_udf_catalog(
                introspector=introspector,
                registries=profile.udf_extension_registries,
            )
    except (RuntimeError, TypeError, ValueError) as exc:
        logging.getLogger(__name__).warning(
            "Skipping UDF catalog refresh due to DataFusion expression ABI mismatch: %s",
            exc,
        )
        profile.udf_catalog_cache.pop(cache_key, None)
        return
    try:
        _validate_udf_specs(profile, catalog, introspector=introspector)
    except (RuntimeError, TypeError, ValueError) as exc:
        logging.getLogger(__name__).warning(
            "Using degraded UDF catalog snapshot due to DataFusion expression ABI mismatch: %s",
            exc,
        )
    profile.udf_catalog_cache[cache_key] = catalog


def _validate_udf_specs(
    profile: DataFusionRuntimeProfile,
    catalog: UdfCatalog,
    *,
    introspector: SchemaIntrospector,
) -> None:
    """Validate Rust UDF snapshot coverage against the runtime catalog.

    Args:
        profile: Runtime profile.
        catalog: UDF catalog.
        introspector: Schema introspector.

    Raises:
        ValueError: If the tracing hook is unavailable.
    """
    from datafusion_engine.udf.extension_core import (
        rust_udf_snapshot,
        udf_names_from_snapshot,
    )

    registry_snapshot = rust_udf_snapshot(
        introspector.ctx,
        registries=profile.udf_extension_registries,
    )
    registered_udfs = _registered_udf_names(profile, registry_snapshot)
    required_builtins = _required_builtin_udfs(
        profile,
        registry_snapshot,
        registered_udfs=registered_udfs,
        udf_names_from_snapshot=udf_names_from_snapshot,
    )
    missing = _missing_udf_names(catalog, required_builtins)
    if missing:
        if profile.diagnostics.diagnostics_sink is not None:
            from datafusion_engine.session.runtime_compile import record_artifact

            record_artifact(
                profile,
                _artifact_spec("DATAFUSION_UDF_VALIDATION_SPEC"),
                {
                    "event_time_unix_ms": int(time.time() * 1000),
                    "udf_catalog_policy": profile.policies.udf_catalog_policy,
                    "missing_udfs": sorted(missing),
                    "missing_count": len(missing),
                },
            )
        msg = f"Rust UDFs missing in DataFusion: {sorted(missing)}."
        raise ValueError(msg)

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


def _iter_snapshot_names(values: object) -> set[str]:
    """Iterate snapshot values and extract names as strings.

    Args:
        values: Snapshot values.

    Returns:
        Set of extracted names.
    """
    if isinstance(values, Iterable) and not isinstance(values, (str, bytes)):
        return {str(name) for name in values if name is not None}
    return set()


def _registered_udf_names(
    profile: DataFusionRuntimeProfile, snapshot: Mapping[str, object]
) -> set[str]:
    """Extract registered UDF names from snapshot.

    Args:
        profile: Runtime profile (unused but kept for consistency).
        snapshot: UDF snapshot.

    Returns:
        Set of registered UDF names.
    """
    _ = profile
    names: set[str] = set()
    for key in ("scalar", "aggregate", "window", "table"):
        names.update(_iter_snapshot_names(snapshot.get(key)))
    return names


def _required_builtin_udfs(
    profile: DataFusionRuntimeProfile,
    snapshot: Mapping[str, object],
    *,
    registered_udfs: set[str],
    udf_names_from_snapshot: Callable[[Mapping[str, object]], Iterable[str]],
) -> set[str]:
    """Compute required builtin UDF names.

    Args:
        profile: Runtime profile (unused but kept for consistency).
        snapshot: UDF snapshot.
        registered_udfs: Set of registered UDF names.
        udf_names_from_snapshot: Function to extract names from snapshot.

    Returns:
        Set of required builtin UDF names.
    """
    _ = profile
    required = set(udf_names_from_snapshot(snapshot))
    custom_udfs = _iter_snapshot_names(snapshot.get("custom_udfs"))
    required.difference_update(custom_udfs - registered_udfs)
    required.difference_update(_iter_snapshot_names(snapshot.get("table")))
    return required


def _missing_udf_names(catalog: UdfCatalog, required: Iterable[str]) -> list[str]:
    """Find UDF names that are required but missing from catalog.

    Args:
        catalog: UDF catalog.
        required: Required UDF names.

    Returns:
        List of missing UDF names.
    """
    missing: list[str] = []
    for name in sorted(set(required)):
        try:
            if catalog.is_builtin_from_runtime(name):
                continue
        except (RuntimeError, TypeError, ValueError):
            pass
        missing.append(name)
    return missing


def _validate_rule_function_allowlist(
    profile: DataFusionRuntimeProfile, ctx: SessionContext
) -> None:
    """Validate rulepack function demands against information_schema.

    Args:
        profile: Runtime profile.
        ctx: DataFusion session context.

    Raises:
        ValueError: If the operation cannot be completed.
    """
    if not profile.catalog.enable_information_schema:
        return
    try:
        function_catalog = function_catalog_snapshot_for_profile(
            profile,
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
        sql_options=sql_options_for_profile(profile),
    )
    if errors:
        msg = f"Rulepack function validation failed: {errors}."
        raise ValueError(msg)


def _validate_async_udf_policy(profile: DataFusionRuntimeProfile) -> dict[str, object]:
    """Validate async UDF policy configuration.

    Args:
        profile: Runtime profile.

    Returns:
        Validation report with status and configuration details.
    """
    errors: list[str] = []
    if profile.features.enable_async_udfs and not profile.features.enable_udfs:
        errors.append("Async UDFs require enable_udfs to be True.")
    if not profile.features.enable_async_udfs and (
        profile.policies.async_udf_timeout_ms is not None
        or profile.policies.async_udf_batch_size is not None
    ):
        errors.append("Async UDF settings provided while async UDFs are disabled.")
    if profile.features.enable_async_udfs:
        if (
            profile.policies.async_udf_timeout_ms is None
            or profile.policies.async_udf_timeout_ms <= 0
        ):
            errors.append("async_udf_timeout_ms must be a positive integer.")
        if (
            profile.policies.async_udf_batch_size is None
            or profile.policies.async_udf_batch_size <= 0
        ):
            errors.append("async_udf_batch_size must be a positive integer.")
    return {
        "valid": not errors,
        "enable_async_udfs": profile.features.enable_async_udfs,
        "async_udf_timeout_ms": profile.policies.async_udf_timeout_ms,
        "async_udf_batch_size": profile.policies.async_udf_batch_size,
        "errors": errors,
    }


def _validate_named_args_extension_parity(profile: DataFusionRuntimeProfile) -> dict[str, object]:
    """Validate that named-arg support aligns with extension capabilities.

    This function checks whether the Python-side configuration for named arguments
    is consistent with the available Rust extension capabilities.

    Args:
        profile: Runtime profile.

    Returns:
        Validation report with status and details.
    """
    from datafusion_engine.session.features import named_args_supported

    warnings: list[str] = []

    # Check if function factory is enabled but expr planners are not
    if profile.features.enable_function_factory and not profile.features.enable_expr_planners:
        warnings.append(
            "FunctionFactory enabled without ExprPlanners; "
            "named arguments may not be supported in SQL."
        )

    # Check if expr planners are configured
    valid = True
    if (
        profile.features.enable_expr_planners
        and not profile.policies.expr_planner_names
        and not profile.policies.expr_planner_hook
    ):
        valid = False
        warnings.append("ExprPlanners enabled but no planner names or hook configured.")

    return {
        "valid": valid,
        "enable_function_factory": profile.features.enable_function_factory,
        "enable_expr_planners": profile.features.enable_expr_planners,
        "expr_planner_names": list(profile.policies.expr_planner_names),
        "named_args_supported": named_args_supported(profile),
        "warnings": warnings,
    }


def _validate_udf_info_schema_parity(
    profile: DataFusionRuntimeProfile, ctx: SessionContext
) -> dict[str, object]:
    """Validate that Rust UDFs appear in information_schema.

    Args:
        profile: Runtime profile.
        ctx: DataFusion session context.

    Returns:
        Result dict.

    Raises:
        ValueError: If the operation cannot be completed.
    """
    if not profile.catalog.enable_information_schema:
        return {
            "missing_in_information_schema": [],
            "routines_available": False,
            "error": "information_schema disabled",
        }

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
    from datafusion_engine.udf.extension_core import rust_runtime_install_payload
    from datafusion_engine.udf.parity import udf_info_schema_parity_report

    report = udf_info_schema_parity_report(ctx)
    _runtime_payload = rust_runtime_install_payload(
        ctx,
        registries=profile.udf_extension_registries,
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


def _record_extension_parity_validation(
    profile: DataFusionRuntimeProfile, ctx: SessionContext
) -> None:
    """Record extension parity validation artifacts.

    Args:
        profile: Runtime profile.
        ctx: DataFusion session context.
    """
    from datafusion_engine.session.runtime_compile import record_artifact

    payload = dict(_validate_named_args_extension_parity(profile))
    payload["async_udf_policy"] = _validate_async_udf_policy(profile)
    payload["udf_info_schema_parity"] = _validate_udf_info_schema_parity(profile, ctx)
    if profile.diagnostics.diagnostics_sink is None:
        return

    payload["extension_capabilities"] = extension_capabilities_report()
    runtime_capabilities = _runtime_capabilities_payload(profile, ctx)
    payload["runtime_capabilities"] = runtime_capabilities
    payload["event_time_unix_ms"] = int(time.time() * 1000)
    payload["profile_name"] = profile.policies.config_policy_name
    payload["settings_hash"] = profile.settings_hash()
    record_artifact(
        profile,
        _artifact_spec("DATAFUSION_EXTENSION_PARITY_SPEC"),
        payload,
    )
    record_artifact(
        profile,
        _artifact_spec("DATAFUSION_RUNTIME_CAPABILITIES_SPEC"),
        runtime_capabilities,
    )
    _record_performance_policy(profile, runtime_capabilities=runtime_capabilities)


def _runtime_capabilities_payload(
    profile: DataFusionRuntimeProfile, ctx: SessionContext
) -> dict[str, object]:
    """Build runtime capabilities payload.

    Args:
        profile: Runtime profile.
        ctx: DataFusion session context.

    Returns:
        Runtime capabilities payload.
    """
    from datafusion_engine.extensions.runtime_capabilities import (
        build_runtime_capabilities_snapshot,
        runtime_capabilities_payload,
    )

    snapshot = build_runtime_capabilities_snapshot(
        ctx,
        profile_name=profile.policies.config_policy_name,
        settings_hash=profile.settings_hash(),
        strict_native_provider_enabled=profile.features.enforce_delta_ffi_provider,
    )
    return runtime_capabilities_payload(snapshot)


def _record_performance_policy(
    profile: DataFusionRuntimeProfile,
    *,
    runtime_capabilities: Mapping[str, object] | None,
) -> None:
    """Record performance policy artifact.

    Args:
        profile: Runtime profile.
        runtime_capabilities: Runtime capabilities payload.
    """
    from datafusion_engine.plan.perf_policy import performance_policy_artifact_payload
    from datafusion_engine.session.runtime_compile import record_artifact
    from datafusion_engine.session.runtime_telemetry import performance_policy_applied_knobs

    if profile.diagnostics.diagnostics_sink is None:
        return
    policy_payload = performance_policy_artifact_payload(
        profile.policies.performance_policy,
        applied_knobs=performance_policy_applied_knobs(
            profile,
            runtime_capabilities=runtime_capabilities,
        ),
    )
    record_artifact(
        profile,
        _artifact_spec("PERFORMANCE_POLICY_SPEC"),
        policy_payload,
    )


def _record_cache_diagnostics(profile: DataFusionRuntimeProfile, ctx: SessionContext) -> None:
    """Record cache configuration and state diagnostics.

    Args:
        profile: Runtime profile.
        ctx: DataFusion session context to introspect.
    """
    from datafusion_engine.session.cache_policy import cache_policy_settings
    from datafusion_engine.session.runtime_compile import record_artifact

    _snapshot_metadata_caches(profile, ctx)
    if profile.diagnostics.diagnostics_sink is None:
        return
    cache_diag = _capture_cache_diagnostics(ctx)
    config_payload = _cache_config_payload(cache_diag)
    record_artifact(profile, _artifact_spec("DATAFUSION_CACHE_CONFIG_SPEC"), config_payload)
    record_artifact(
        profile,
        _artifact_spec("DATAFUSION_CACHE_ROOT_SPEC"),
        {"cache_root": profile.io_ops.cache_root()},
    )
    if profile.policies.cache_policy is not None:
        record_artifact(
            profile,
            _artifact_spec("CACHE_POLICY_SPEC"),
            cache_policy_settings(profile.policies.cache_policy),
        )
    cache_snapshots = _cache_snapshot_rows(cache_diag)
    if cache_snapshots:
        profile.record_events(
            "datafusion_cache_state_v1",
            cache_snapshots,
        )
    diskcache_profile = profile.policies.diskcache_profile
    if diskcache_profile is None:
        return
    diskcache_events = _diskcache_event_rows(profile, diskcache_profile)
    if diskcache_events:
        profile.record_events(
            "diskcache_stats_v1",
            diskcache_events,
        )


def _snapshot_metadata_caches(profile: DataFusionRuntimeProfile, ctx: SessionContext) -> None:
    """Snapshot metadata caches when enabled.

    Args:
        profile: Runtime profile.
        ctx: DataFusion session context.
    """
    from datafusion_engine.session.runtime_compile import record_artifact

    if not profile.policies.metadata_cache_snapshot_enabled:
        return
    from datafusion_engine.cache.metadata_snapshots import snapshot_datafusion_caches

    try:
        snapshots = snapshot_datafusion_caches(ctx, runtime_profile=profile)
    except (RuntimeError, TypeError, ValueError) as exc:
        if profile.diagnostics.diagnostics_sink is None:
            return
        record_artifact(
            profile,
            _artifact_spec("DATAFUSION_CACHE_SNAPSHOT_ERROR_SPEC"),
            {
                "event_time_unix_ms": int(time.time() * 1000),
                "error": str(exc),
            },
        )
        return
    if profile.diagnostics.diagnostics_sink is None:
        return
    if snapshots:
        profile.record_events(
            "datafusion_cache_snapshot_v1",
            snapshots,
        )


def _diskcache_event_rows(
    profile: DataFusionRuntimeProfile, diskcache_profile: DiskCacheProfile
) -> list[dict[str, object]]:
    """Build diskcache event rows.

    Args:
        profile: Runtime profile.
        diskcache_profile: Diskcache profile.

    Returns:
        List of diskcache event rows.
    """
    rows: list[dict[str, object]] = []
    for kind in ("plan", "extract", "schema", "repo_scan", "runtime", "coordination"):
        cache = cache_for_kind(diskcache_profile, cast("DiskCacheKind", kind))
        if cache is None:
            continue
        settings = diskcache_profile.settings_for(cast("DiskCacheKind", kind))
        payload = diskcache_stats_snapshot(cache)
        payload.update(
            {
                "kind": kind,
                "profile_key": profile.context_cache_key(),
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


def _install_cache_tables(profile: DataFusionRuntimeProfile, ctx: SessionContext) -> None:
    """Install cache introspection tables.

    Args:
        profile: Runtime profile.
        ctx: DataFusion session context.

    Raises:
        RuntimeError: If cache table function registration fails.
    """
    if not (
        profile.features.enable_cache_manager
        or profile.features.cache_enabled
        or profile.policies.metadata_cache_snapshot_enabled
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


def _install_tracing(profile: DataFusionRuntimeProfile, ctx: SessionContext) -> None:
    """Enable tracing when configured.

    Args:
        profile: Runtime profile.
        ctx: DataFusion session context.

    Raises:
        RuntimeError: If tracing installation fails.
        ValueError: If tracing is enabled but extension entrypoints are unavailable.
    """
    from datafusion_engine.session.runtime_compile import record_artifact

    if not profile.features.enable_tracing:
        return
    from datafusion_engine.delta.observability import ensure_delta_tracing

    delta_installed, delta_error = ensure_delta_tracing()
    if profile.diagnostics.diagnostics_sink is not None:
        record_artifact(
            profile,
            _artifact_spec("DATAFUSION_DELTA_TRACING_SPEC"),
            {
                "enabled": profile.features.enable_tracing,
                "installed": delta_installed,
                "error": delta_error,
            },
        )
    if profile.diagnostics.tracing_hook is None:
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
            msg = (
                "Tracing install failed: no compatible SessionContext was resolved for "
                f"{details.get('module') or 'unknown'}."
            )
            raise RuntimeError(msg)
        try:
            install(ctx_arg)
        except TypeError as exc:
            msg = (
                "Tracing install failed due to SessionContext ABI mismatch. "
                f"module={details.get('module')}, context_type={type(ctx_arg).__name__}"
            )
            raise RuntimeError(msg) from exc
        return
    profile.diagnostics.tracing_hook(ctx)


def _install_function_factory(profile: DataFusionRuntimeProfile, ctx: SessionContext) -> None:
    """Install function factory when enabled.

    Args:
        profile: Runtime profile.
        ctx: DataFusion session context.

    Raises:
        RuntimeError: If installation fails.
    """
    if not profile.features.enable_function_factory:
        return
    available = True
    installed = False
    error: str | None = None
    cause: Exception | None = None
    try:
        if profile.policies.function_factory_hook is None:
            install_function_factory(ctx)
        else:
            profile.policies.function_factory_hook(ctx)
        installed = True
    except ImportError as exc:
        available = False
        error = str(exc)
        cause = exc
    except (RuntimeError, TypeError) as exc:
        error = str(exc)
        cause = exc
    _record_function_factory(
        profile,
        available=available,
        installed=installed,
        error=error,
    )
    if error is not None:
        msg = "FunctionFactory installation failed; native extension is required."
        raise RuntimeError(msg) from cause


def _install_expr_planners(profile: DataFusionRuntimeProfile, ctx: SessionContext) -> None:
    """Install expression planners when enabled.

    Args:
        profile: Runtime profile.
        ctx: DataFusion session context.

    Raises:
        RuntimeError: If installation fails.
    """
    if not profile.features.enable_expr_planners:
        return
    available = True
    installed = False
    error: str | None = None
    cause: Exception | None = None
    try:
        if profile.policies.expr_planner_hook is None:
            install_expr_planners(ctx, planner_names=profile.policies.expr_planner_names)
        else:
            profile.policies.expr_planner_hook(ctx)
        installed = True
    except ImportError as exc:
        available = False
        error = str(exc)
        cause = exc
    except (RuntimeError, TypeError, ValueError) as exc:
        error = str(exc)
        cause = exc
    _record_expr_planners(
        profile,
        available=available,
        installed=installed,
        error=error,
    )
    if error is not None:
        msg = "ExprPlanner installation failed; native extension is required."
        raise RuntimeError(msg) from cause


def _install_physical_expr_adapter_factory(
    profile: DataFusionRuntimeProfile, ctx: SessionContext
) -> None:
    """Install a physical expression adapter factory when available.

    Args:
        profile: Runtime profile.
        ctx: DataFusion session context.

    Raises:
        TypeError: If the operation cannot be completed.
    """
    factory = profile.policies.physical_expr_adapter_factory
    uses_default_adapter = False
    if factory is None and profile.features.enable_schema_evolution_adapter:
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


def _install_delta_plan_codecs_extension(
    ctx: SessionContext,
) -> tuple[bool, bool]:
    """Install delta plan codecs extension.

    Args:
        ctx: DataFusion session context.

    Returns:
        Tuple of (available, installed).
    """
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


def _record_udf_snapshot(profile: DataFusionRuntimeProfile, snapshot: Mapping[str, object]) -> None:
    """Record UDF snapshot artifact.

    Args:
        profile: Runtime profile.
        snapshot: UDF snapshot.
    """
    from datafusion_engine.session.runtime_compile import record_artifact

    if profile.diagnostics.diagnostics_sink is None:
        return
    record_artifact(
        profile,
        _artifact_spec("DATAFUSION_UDF_REGISTRY_SPEC"),
        dict(snapshot),
    )


def _record_udf_docs(profile: DataFusionRuntimeProfile, docs: Mapping[str, object]) -> None:
    """Record UDF docs artifact.

    Args:
        profile: Runtime profile.
        docs: UDF docs.
    """
    from datafusion_engine.session.runtime_compile import record_artifact

    if profile.diagnostics.diagnostics_sink is None:
        return
    record_artifact(
        profile,
        _artifact_spec("DATAFUSION_UDF_DOCS_SPEC"),
        dict(docs),
    )


def _record_delta_plan_codecs(
    profile: DataFusionRuntimeProfile, *, available: bool, installed: bool
) -> None:
    """Record delta plan codecs artifact.

    Args:
        profile: Runtime profile.
        available: Whether codecs are available.
        installed: Whether codecs are installed.
    """
    from datafusion_engine.session.runtime_compile import record_artifact

    if profile.diagnostics.diagnostics_sink is None:
        return
    record_artifact(
        profile,
        _artifact_spec("DATAFUSION_DELTA_PLAN_CODECS_SPEC"),
        {
            "enabled": profile.features.enable_delta_plan_codecs,
            "available": available,
            "installed": installed,
            "physical_codec": profile.policies.delta_plan_codec_physical,
            "logical_codec": profile.policies.delta_plan_codec_logical,
        },
    )


def record_delta_session_defaults(
    profile: DataFusionRuntimeProfile,
    *,
    available: bool,
    installed: bool,
    error: str | None,
    runtime_policy_bridge: object | None = None,
) -> None:
    """Record delta session defaults artifact.

    Args:
        profile: Runtime profile.
        available: Whether defaults are available.
        installed: Whether defaults are installed.
        error: Error message if any.
        runtime_policy_bridge: Optional policy bridge metadata.
    """
    from datafusion_engine.session.runtime_compile import record_artifact

    if profile.diagnostics.diagnostics_sink is None:
        return
    record_artifact(
        profile,
        _artifact_spec("DATAFUSION_DELTA_SESSION_DEFAULTS_SPEC"),
        {
            "enabled": profile.features.enable_delta_session_defaults,
            "available": available,
            "installed": installed,
            "error": error,
            "runtime_policy_bridge": runtime_policy_bridge,
        },
    )


def _record_tracing_install(
    profile: DataFusionRuntimeProfile, error: str, details: Mapping[str, object]
) -> None:
    """Record tracing install artifact.

    Args:
        profile: Runtime profile.
        error: Error message.
        details: Additional details.
    """
    from datafusion_engine.session.runtime_compile import record_artifact

    if profile.diagnostics.diagnostics_sink is None:
        return
    payload = {"error": error, **details}
    record_artifact(profile, _artifact_spec("DATAFUSION_TRACING_INSTALL_SPEC"), payload)


def _record_function_factory(
    profile: DataFusionRuntimeProfile,
    *,
    available: bool,
    installed: bool,
    error: str | None,
    policy: Mapping[str, object] | None = None,
) -> None:
    """Record function factory artifact.

    Args:
        profile: Runtime profile.
        available: Whether function factory is available.
        installed: Whether function factory is installed.
        error: Error message if any.
        policy: Policy details.
    """
    from datafusion_engine.session.runtime_compile import record_artifact

    if profile.diagnostics.diagnostics_sink is None:
        return
    record_artifact(
        profile,
        _artifact_spec("DATAFUSION_FUNCTION_FACTORY_SPEC"),
        {
            "enabled": profile.features.enable_function_factory,
            "available": available,
            "installed": installed,
            "hook_enabled": bool(profile.policies.function_factory_hook),
            "policy": policy or function_factory_payloads(),
            "error": error,
        },
    )


def _record_expr_planners(
    profile: DataFusionRuntimeProfile,
    *,
    available: bool,
    installed: bool,
    error: str | None,
    policy: Mapping[str, object] | None = None,
) -> None:
    """Record expression planners artifact.

    Args:
        profile: Runtime profile.
        available: Whether expr planners are available.
        installed: Whether expr planners are installed.
        error: Error message if any.
        policy: Policy details.
    """
    from datafusion_engine.session.runtime_compile import record_artifact

    if profile.diagnostics.diagnostics_sink is None:
        return
    record_artifact(
        profile,
        _artifact_spec("DATAFUSION_EXPR_PLANNERS_SPEC"),
        {
            "enabled": profile.features.enable_expr_planners,
            "available": available,
            "installed": installed,
            "hook_enabled": bool(profile.policies.expr_planner_hook),
            "planner_names": list(profile.policies.expr_planner_names),
            "policy": policy or expr_planner_payloads(profile.policies.expr_planner_names),
            "error": error,
        },
    )

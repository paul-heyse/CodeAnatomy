"""Compile option resolution and hook wiring for runtime profiles."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import replace
from typing import TYPE_CHECKING, cast

from datafusion_engine.compile.options import (
    DataFusionCompileOptions,
    DataFusionSqlPolicy,
    resolve_sql_policy,
)
from datafusion_engine.session.runtime_compile import (
    _CompileOptionResolution,
    _resolve_prepared_statement_options,
    _ResolvedCompileHooks,
)
from datafusion_engine.session.runtime_hooks import (
    CacheEventHook,
    ExplainHook,
    PlanArtifactsHook,
    SemanticDiffHook,
    SqlIngestHook,
    SubstraitFallbackHook,
    _chain_cache_hooks,
    _chain_explain_hooks,
    _chain_plan_artifacts_hooks,
    _chain_sql_ingest_hooks,
    _chain_substrait_fallback_hooks,
    apply_execution_label,
    apply_execution_policy,
    diagnostics_cache_hook,
    diagnostics_explain_hook,
    diagnostics_plan_artifacts_hook,
    diagnostics_semantic_diff_hook,
    diagnostics_sql_ingest_hook,
    diagnostics_substrait_fallback_hook,
)

if TYPE_CHECKING:
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile
    from datafusion_engine.session.runtime_profile_config import (
        AdapterExecutionPolicy,
        ExecutionLabel,
    )


def resolve_compile_hooks(
    profile: DataFusionRuntimeProfile,
    resolved: DataFusionCompileOptions,
    *,
    capture_explain: bool,
    explain_analyze: bool,
    capture_plan_artifacts: bool,
    capture_semantic_diff: bool,
) -> _ResolvedCompileHooks:
    """Resolve compile hooks from the profile diagnostics configuration.

    Parameters
    ----------
    profile
        Runtime profile with diagnostics configuration.
    resolved
        Base compile options to derive hooks from.
    capture_explain
        Whether explain output should be captured.
    explain_analyze
        Whether explain-analyze is enabled.
    capture_plan_artifacts
        Whether plan artifacts should be captured.
    capture_semantic_diff
        Whether semantic diff should be captured.

    Returns:
    -------
    _ResolvedCompileHooks
        Resolved hook functions.
    """
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
        and profile.diagnostics.explain_collector is not None
    ):
        hooks["explain"] = profile.diagnostics.explain_collector.hook
    if (
        hooks["plan_artifacts"] is None
        and capture_plan_artifacts
        and profile.diagnostics.plan_collector is not None
    ):
        hooks["plan_artifacts"] = profile.diagnostics.plan_collector.hook
    if profile.diagnostics.diagnostics_sink is not None:
        if capture_explain or hooks["explain"] is not None:
            hooks["explain"] = _chain_explain_hooks(
                cast("ExplainHook", hooks["explain"]),
                diagnostics_explain_hook(
                    profile.diagnostics.diagnostics_sink,
                    explain_analyze=explain_analyze,
                ),
            )
        if capture_plan_artifacts or hooks["plan_artifacts"] is not None:
            hooks["plan_artifacts"] = _chain_plan_artifacts_hooks(
                cast("PlanArtifactsHook", hooks["plan_artifacts"]),
                diagnostics_plan_artifacts_hook(profile.diagnostics.diagnostics_sink),
            )
        if capture_semantic_diff or hooks["semantic_diff"] is not None:
            hooks["semantic_diff"] = _chain_plan_artifacts_hooks(
                cast("PlanArtifactsHook", hooks["semantic_diff"]),
                diagnostics_semantic_diff_hook(profile.diagnostics.diagnostics_sink),
            )
        hooks["sql_ingest"] = _chain_sql_ingest_hooks(
            cast("SqlIngestHook", hooks["sql_ingest"]),
            diagnostics_sql_ingest_hook(profile.diagnostics.diagnostics_sink),
        )
        hooks["cache_event"] = _chain_cache_hooks(
            cast("CacheEventHook", hooks["cache_event"]),
            diagnostics_cache_hook(profile.diagnostics.diagnostics_sink),
        )
        hooks["substrait_fallback"] = _chain_substrait_fallback_hooks(
            cast("SubstraitFallbackHook", hooks["substrait_fallback"]),
            diagnostics_substrait_fallback_hook(profile.diagnostics.diagnostics_sink),
        )
    return _ResolvedCompileHooks(
        explain_hook=cast("ExplainHook | None", hooks["explain"]),
        plan_artifacts_hook=cast("PlanArtifactsHook | None", hooks["plan_artifacts"]),
        semantic_diff_hook=cast("SemanticDiffHook | None", hooks["semantic_diff"]),
        sql_ingest_hook=cast("SqlIngestHook | None", hooks["sql_ingest"]),
        cache_event_hook=cast("CacheEventHook | None", hooks["cache_event"]),
        substrait_fallback_hook=cast("SubstraitFallbackHook | None", hooks["substrait_fallback"]),
    )


def resolve_compile_sql_policy(
    profile: DataFusionRuntimeProfile,
    resolved: DataFusionCompileOptions,
) -> DataFusionSqlPolicy | None:
    """Resolve SQL policy from compile options and profile.

    Parameters
    ----------
    profile
        Runtime profile with policy configuration.
    resolved
        Compile options that may override the SQL policy.

    Returns:
    -------
    DataFusionSqlPolicy | None
        Resolved SQL policy.
    """
    if resolved.sql_policy is not None:
        return resolved.sql_policy
    if profile.policies.sql_policy is None and profile.policies.sql_policy_name is None:
        return None
    return profile.policies.sql_policy or resolve_sql_policy(profile.policies.sql_policy_name)


def compile_options_for_profile(
    profile: DataFusionRuntimeProfile,
    *,
    options: DataFusionCompileOptions | None = None,
    params: Mapping[str, object] | None = None,
    execution_policy: AdapterExecutionPolicy | None = None,
    execution_label: ExecutionLabel | None = None,
) -> DataFusionCompileOptions:
    """Return DataFusion compile options derived from the profile.

    Parameters
    ----------
    profile
        Runtime profile to derive options from.
    options
        Optional base compile options to merge with.
    params
        Optional query parameters.
    execution_policy
        Optional execution policy override.
    execution_label
        Optional execution label.

    Returns:
    -------
    DataFusionCompileOptions
        Compile options aligned with this runtime profile.
    """
    resolved = options or DataFusionCompileOptions(
        cache=None,
        cache_max_columns=None,
        enforce_preflight=profile.features.enforce_preflight,
    )
    resolved_params = resolved.params if resolved.params is not None else params
    prepared = _resolve_prepared_statement_options(resolved)
    capture_explain = resolved.capture_explain or profile.diagnostics.capture_explain
    explain_analyze = resolved.explain_analyze or profile.diagnostics.explain_analyze
    substrait_validation = resolved.substrait_validation or profile.diagnostics.substrait_validation
    capture_plan_artifacts = (
        resolved.capture_plan_artifacts
        or profile.diagnostics.capture_plan_artifacts
        or capture_explain
        or substrait_validation
    )
    capture_semantic_diff = (
        resolved.capture_semantic_diff or profile.diagnostics.capture_semantic_diff
    )
    resolution = _CompileOptionResolution(
        cache=resolved.cache if resolved.cache is not None else profile.features.cache_enabled,
        cache_max_columns=(
            resolved.cache_max_columns
            if resolved.cache_max_columns is not None
            else profile.policies.cache_max_columns
        ),
        params=resolved_params,
        param_allowlist=(
            resolved.param_identifier_allowlist
            if resolved.param_identifier_allowlist is not None
            else tuple(profile.policies.param_identifier_allowlist) or None
        ),
        prepared_param_types=prepared[0],
        prepared_statements=prepared[1],
        dynamic_projection=prepared[2],
        capture_explain=capture_explain,
        explain_analyze=explain_analyze,
        substrait_validation=substrait_validation,
        capture_plan_artifacts=capture_plan_artifacts,
        capture_semantic_diff=capture_semantic_diff,
        sql_policy=resolve_compile_sql_policy(profile, resolved),
        sql_policy_name=(
            resolved.sql_policy_name
            if resolved.sql_policy_name is not None
            else profile.policies.sql_policy_name
        ),
    )
    hooks = resolve_compile_hooks(
        profile,
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
        profile.features.enforce_preflight == resolved.enforce_preflight,
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
            explain_sink=profile.diagnostics.labeled_explains,
        )
    if execution_policy is None:
        return updated
    return apply_execution_policy(
        updated,
        execution_policy=execution_policy,
    )

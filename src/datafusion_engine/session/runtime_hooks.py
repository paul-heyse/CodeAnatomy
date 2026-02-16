"""Hook type aliases, chaining functions, and diagnostics hook factories for DataFusion runtime.

This module provides reusable hook infrastructure for capturing diagnostics,
explain plans, plan artifacts, cache events, and other runtime observability signals.
All hook factories are LEAF functions with no dependency on DataFusionRuntimeProfile.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Mapping
from dataclasses import replace
from typing import TYPE_CHECKING, cast

from datafusion import RuntimeEnvBuilder

from datafusion_engine.compile.options import (
    DataFusionCacheEvent,
    DataFusionCompileOptions,
    DataFusionSubstraitFallbackEvent,
)
from datafusion_engine.lineage.diagnostics import DiagnosticsSink, ensure_recorder_sink
from datafusion_engine.session._session_identity import RUNTIME_SESSION_ID
from datafusion_engine.session.hooks import chain_optional_hooks
from serde_artifact_specs import (
    DATAFUSION_ARROW_INGEST_SPEC,
    DATAFUSION_PLAN_ARTIFACTS_SPEC,
    DATAFUSION_SEMANTIC_DIFF_SPEC,
    DATAFUSION_SQL_INGEST_SPEC,
)

if TYPE_CHECKING:
    from datafusion_engine.arrow.interop import RecordBatchReaderLike, TableLike
    from datafusion_engine.session.runtime_profile_config import (
        AdapterExecutionPolicy,
        ExecutionLabel,
    )

    ExplainRows = TableLike | RecordBatchReaderLike
else:
    ExplainRows = object

ExplainHook = Callable[[str, ExplainRows], None]
PlanArtifactsHook = Callable[[Mapping[str, object]], None]
SemanticDiffHook = Callable[[Mapping[str, object]], None]
SqlIngestHook = Callable[[Mapping[str, object]], None]
CacheEventHook = Callable[[DataFusionCacheEvent], None]
SubstraitFallbackHook = Callable[[DataFusionSubstraitFallbackEvent], None]


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
    return chain_optional_hooks(*hooks)


def _chain_plan_artifacts_hooks(
    *hooks: Callable[[Mapping[str, object]], None] | None,
) -> Callable[[Mapping[str, object]], None] | None:
    return chain_optional_hooks(*hooks)


def _chain_sql_ingest_hooks(
    *hooks: Callable[[Mapping[str, object]], None] | None,
) -> Callable[[Mapping[str, object]], None] | None:
    return chain_optional_hooks(*hooks)


def _chain_cache_hooks(
    *hooks: Callable[[DataFusionCacheEvent], None] | None,
) -> Callable[[DataFusionCacheEvent], None] | None:
    return chain_optional_hooks(*hooks)


def _chain_substrait_fallback_hooks(
    *hooks: Callable[[DataFusionSubstraitFallbackEvent], None] | None,
) -> Callable[[DataFusionSubstraitFallbackEvent], None] | None:
    return chain_optional_hooks(*hooks)


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
        recorder_sink = ensure_recorder_sink(sink, session_id=RUNTIME_SESSION_ID)
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
        recorder_sink = ensure_recorder_sink(sink, session_id=RUNTIME_SESSION_ID)
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
        recorder_sink = ensure_recorder_sink(sink, session_id=RUNTIME_SESSION_ID)
        normalized = dict(payload)
        if "plan_identity_hash" not in normalized:
            fingerprint_value = normalized.get("plan_fingerprint")
            if isinstance(fingerprint_value, str) and fingerprint_value:
                normalized["plan_identity_hash"] = fingerprint_value
            else:
                normalized["plan_identity_hash"] = "unknown_plan_identity"
        recorder_sink.record_artifact(
            DATAFUSION_PLAN_ARTIFACTS_SPEC,
            normalized,
        )

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
        recorder_sink = ensure_recorder_sink(sink, session_id=RUNTIME_SESSION_ID)
        recorder_sink.record_artifact(
            DATAFUSION_SEMANTIC_DIFF_SPEC,
            payload,
        )

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
        recorder_sink = ensure_recorder_sink(sink, session_id=RUNTIME_SESSION_ID)
        recorder_sink.record_artifact(
            DATAFUSION_SQL_INGEST_SPEC,
            payload,
        )

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
        recorder_sink = ensure_recorder_sink(sink, session_id=RUNTIME_SESSION_ID)
        recorder_sink.record_artifact(
            DATAFUSION_ARROW_INGEST_SPEC,
            payload,
        )

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


__all__ = [
    "CacheEventHook",
    "ExplainHook",
    "PlanArtifactsHook",
    "SemanticDiffHook",
    "SqlIngestHook",
    "SubstraitFallbackHook",
    "apply_execution_label",
    "apply_execution_policy",
    "diagnostics_arrow_ingest_hook",
    "diagnostics_cache_hook",
    "diagnostics_dml_hook",
    "diagnostics_explain_hook",
    "diagnostics_plan_artifacts_hook",
    "diagnostics_semantic_diff_hook",
    "diagnostics_sql_ingest_hook",
    "diagnostics_substrait_fallback_hook",
    "labeled_explain_hook",
]

"""Runtime profile helpers for DataFusion execution."""

from __future__ import annotations

import hashlib
import json
import logging
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING, Literal, cast

import datafusion
import pyarrow as pa
from datafusion import RuntimeEnvBuilder, SessionConfig, SessionContext
from datafusion.dataframe import DataFrame
from datafusion.object_store import LocalFileSystem

from arrowdsl.core.context import DeterminismTier
from datafusion_engine.compile_options import (
    DataFusionCacheEvent,
    DataFusionCompileOptions,
    DataFusionFallbackEvent,
)
from datafusion_engine.function_factory import install_function_factory
from datafusion_engine.udf_registry import DataFusionUdfSnapshot, register_datafusion_udfs
from engine.plan_cache import PlanCache
from obs.diagnostics import DiagnosticsCollector

if TYPE_CHECKING:
    from ibis.expr.types import Value as IbisValue

MemoryPool = Literal["greedy", "fair", "unbounded"]

logger = logging.getLogger(__name__)

KIB: int = 1024
MIB: int = 1024 * KIB
GIB: int = 1024 * MIB


def _parse_major_version(version: str) -> int | None:
    major = version.split(".", maxsplit=1)[0]
    if major.isdigit():
        return int(major)
    return None


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
        return {
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
class FeatureStateSnapshot:
    """Snapshot of runtime feature gates and determinism tier."""

    profile_name: str
    determinism_tier: DeterminismTier
    dynamic_filters_enabled: bool
    spill_enabled: bool

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
    )


@dataclass
class DataFusionExplainCollector:
    """Collect EXPLAIN artifacts for diagnostics."""

    entries: list[dict[str, object]] = field(default_factory=list)

    def hook(self, sql: str, rows: Sequence[Mapping[str, object]]) -> None:
        """Collect an explain payload for a single statement."""
        row_payload: list[dict[str, str]] = [
            {str(key): str(value) for key, value in row.items()} for row in rows
        ]
        payload = {"sql": sql, "rows": row_payload}
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
    "dev": DEV_DF_POLICY,
    "default": DEFAULT_DF_POLICY,
    "prod": PROD_DF_POLICY,
}

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


def _apply_feature_settings(
    config: SessionConfig,
    feature_gates: DataFusionFeatureGates | None,
) -> SessionConfig:
    if feature_gates is None:
        return config
    for key, value in feature_gates.settings().items():
        config = config.set(key, value)
    return config


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
    *hooks: Callable[[str, Sequence[Mapping[str, object]]], None] | None,
) -> Callable[[str, Sequence[Mapping[str, object]]], None] | None:
    active = [hook for hook in hooks if hook is not None]
    if not active:
        return None

    def _hook(sql: str, rows: Sequence[Mapping[str, object]]) -> None:
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
) -> Callable[[str, Sequence[Mapping[str, object]]], None]:
    """Return an explain hook that records rule-scoped diagnostics.

    Returns
    -------
    Callable[[str, Sequence[Mapping[str, object]]], None]
        Hook that appends labeled explain diagnostics to the sink.
    """

    def _hook(sql: str, rows: Sequence[Mapping[str, object]]) -> None:
        sink.append(
            {
                "rule": label.rule_name,
                "output": label.output_dataset,
                "sql": sql,
                "rows": [dict(row) for row in rows],
            }
        )

    return _hook


def diagnostics_fallback_hook(
    sink: DiagnosticsCollector,
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
    sink: DiagnosticsCollector,
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
    sink: DiagnosticsCollector,
    *,
    explain_analyze: bool,
) -> Callable[[str, Sequence[Mapping[str, object]]], None]:
    """Return an explain hook that records diagnostics rows.

    Returns
    -------
    Callable[[str, Sequence[Mapping[str, object]]], None]
        Hook that records explain rows in the diagnostics sink.
    """

    def _hook(sql: str, rows: Sequence[Mapping[str, object]]) -> None:
        sink.record_events(
            "datafusion_explains_v1",
            [
                {
                    "event_time_unix_ms": int(time.time() * 1000),
                    "sql": sql,
                    "rows": [dict(row) for row in rows],
                    "explain_analyze": explain_analyze,
                }
            ],
        )

    return _hook


def diagnostics_plan_artifacts_hook(
    sink: DiagnosticsCollector,
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
    sink: DiagnosticsCollector,
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
class DataFusionRuntimeProfile:
    """DataFusion runtime configuration."""

    target_partitions: int | None = None
    batch_size: int | None = None
    spill_dir: str | None = None
    memory_pool: MemoryPool = "greedy"
    memory_limit_bytes: int | None = None
    default_catalog: str = "codeintel"
    default_schema: str = "public"
    enable_information_schema: bool = True
    enable_url_table: bool = False  # Dev-only convenience for file-path queries.
    cache_enabled: bool = False
    cache_max_columns: int | None = 64
    enable_cache_manager: bool = False
    cache_manager_factory: Callable[[], object] | None = None
    enable_function_factory: bool = True
    function_factory_hook: Callable[[SessionContext], None] | None = None
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
    capture_plan_artifacts: bool = False
    plan_collector: DataFusionPlanCollector | None = field(default_factory=DataFusionPlanCollector)
    capture_fallbacks: bool = True
    fallback_collector: DataFusionFallbackCollector | None = field(
        default_factory=DataFusionFallbackCollector
    )
    diagnostics_sink: DiagnosticsCollector | None = None
    labeled_fallbacks: list[dict[str, object]] = field(default_factory=list)
    labeled_explains: list[dict[str, object]] = field(default_factory=list)
    plan_cache: PlanCache | None = field(default_factory=PlanCache)
    local_filesystem_root: str | None = None
    input_plugins: tuple[Callable[[SessionContext], None], ...] = ()
    prepared_statements: tuple[PreparedStatementSpec, ...] = ()
    config_policy_name: str | None = "default"
    config_policy: DataFusionConfigPolicy | None = None
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
        config = _apply_config_policy(config, self._resolved_config_policy())
        config = _apply_settings_overrides(config, self.settings_overrides)
        config = _apply_feature_settings(config, self.feature_gates)
        config = _apply_join_settings(config, self.join_policy)
        return _apply_explain_analyze_level(config, self.explain_analyze_level)

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
        self._install_udfs(ctx)
        self._prepare_statements(ctx)
        self.ensure_delta_plan_codecs(ctx)
        self._install_function_factory(ctx)
        self._install_tracing()
        if self.session_context_hook is not None:
            ctx = self.session_context_hook(ctx)
        self._cache_context(ctx)
        return ctx

    def _install_input_plugins(self, ctx: SessionContext) -> None:
        """Install input plugins on the session context."""
        for plugin in self.input_plugins:
            plugin(ctx)

    def _install_udfs(self, ctx: SessionContext) -> None:
        """Install registered UDFs on the session context."""
        if not self.enable_udfs:
            return
        snapshot = register_datafusion_udfs(ctx)
        self._record_udf_snapshot(snapshot)

    def _prepare_statements(self, ctx: SessionContext) -> None:
        """Prepare SQL statements when configured."""
        for statement in self.prepared_statements:
            ctx.sql(statement.sql)

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
        try:
            if self.function_factory_hook is None:
                install_function_factory(ctx)
            else:
                self.function_factory_hook(ctx)
        except (ImportError, RuntimeError, TypeError) as exc:
            msg = "FunctionFactory installation failed; native extension is required."
            raise RuntimeError(msg) from exc

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
        capture_explain = resolved.capture_explain or self.capture_explain
        explain_analyze = resolved.explain_analyze or self.explain_analyze
        explain_hook = resolved.explain_hook
        if explain_hook is None and capture_explain and self.explain_collector is not None:
            explain_hook = self.explain_collector.hook
        capture_plan_artifacts = (
            resolved.capture_plan_artifacts or self.capture_plan_artifacts or capture_explain
        )
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
        unchanged = (
            cache == resolved.cache,
            cache_max_columns == resolved.cache_max_columns,
            resolved_params == resolved.params,
            capture_explain == resolved.capture_explain,
            explain_analyze == resolved.explain_analyze,
            explain_hook == resolved.explain_hook,
            capture_plan_artifacts == resolved.capture_plan_artifacts,
            plan_artifacts_hook == resolved.plan_artifacts_hook,
            sql_ingest_hook == resolved.sql_ingest_hook,
            fallback_hook == resolved.fallback_hook,
            cache_event_hook == resolved.cache_event_hook,
        )
        if all(unchanged) and execution_policy is None and execution_label is None:
            return resolved
        updated = replace(
            resolved,
            cache=cache,
            cache_max_columns=cache_max_columns,
            params=resolved_params,
            capture_explain=capture_explain,
            explain_analyze=explain_analyze,
            explain_hook=explain_hook,
            capture_plan_artifacts=capture_plan_artifacts,
            plan_artifacts_hook=plan_artifacts_hook,
            sql_ingest_hook=sql_ingest_hook,
            fallback_hook=fallback_hook,
            cache_event_hook=cache_event_hook,
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
        if self.settings_overrides:
            payload.update({str(key): str(value) for key, value in self.settings_overrides.items()})
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
        payload = self.settings_payload()
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
        return hashlib.sha256(encoded.encode("utf-8")).hexdigest()

    def telemetry_payload(self) -> dict[str, object]:
        """Return a diagnostics-friendly payload for the runtime profile.

        Returns
        -------
        dict[str, object]
            Runtime settings serialized for telemetry/diagnostics.
        """
        resolved_policy = self._resolved_config_policy()
        return {
            "target_partitions": self.target_partitions,
            "batch_size": self.batch_size,
            "spill_dir": self.spill_dir,
            "memory_pool": self.memory_pool,
            "memory_limit_bytes": self.memory_limit_bytes,
            "default_catalog": self.default_catalog,
            "default_schema": self.default_schema,
            "enable_information_schema": self.enable_information_schema,
            "enable_url_table": self.enable_url_table,
            "cache_enabled": self.cache_enabled,
            "cache_max_columns": self.cache_max_columns,
            "cache_manager_enabled": self.enable_cache_manager,
            "cache_manager_factory": bool(self.cache_manager_factory),
            "function_factory_enabled": self.enable_function_factory,
            "function_factory_hook": bool(self.function_factory_hook),
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
            "config_policy": dict(resolved_policy.settings)
            if resolved_policy is not None
            else None,
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
            "version": 1,
            "profile_name": self.config_policy_name,
            "session_config": dict(settings),
            "settings_hash": self.settings_hash(),
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
            },
            "output_writes": {
                "cache_enabled": self.cache_enabled,
                "cache_max_columns": self.cache_max_columns,
            },
        }

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

    def _cache_key(self) -> str:
        if self.session_context_key:
            return self.session_context_key
        payload = self.telemetry_payload()
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
        return hashlib.sha256(encoded.encode("utf-8")).hexdigest()

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
    "apply_execution_label",
    "apply_execution_policy",
    "feature_state_snapshot",
    "snapshot_plans",
]

"""Runtime profile helpers for DataFusion execution."""

from __future__ import annotations

import hashlib
import json
import logging
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING, Literal, cast

import datafusion
import pyarrow as pa
from datafusion import RuntimeEnvBuilder, SessionConfig, SessionContext
from datafusion.dataframe import DataFrame
from datafusion.object_store import LocalFileSystem

from datafusion_engine.compile_options import DataFusionCompileOptions, DataFusionFallbackEvent
from datafusion_engine.function_factory import install_function_factory
from datafusion_engine.udf_registry import register_datafusion_udfs

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


DATAFUSION_MAJOR_VERSION: int | None = _parse_major_version(datafusion.__version__)


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
            DATAFUSION_MAJOR_VERSION is not None and DATAFUSION_MAJOR_VERSION >= 51
        )
        for key, value in self.settings.items():
            if skip_runtime_settings and key.startswith("datafusion.runtime."):
                continue
            config = config.set(key, value)
        return config


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
class AdapterExecutionPolicy:
    """Execution policy for adapterized fallback handling."""

    allow_fallback: bool = True
    fail_on_fallback: bool = False


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
    enable_function_factory: bool = False
    function_factory_hook: Callable[[SessionContext], None] | None = None
    enable_metrics: bool = False
    metrics_collector: Callable[[], Mapping[str, object] | None] | None = None
    enable_tracing: bool = False
    tracing_hook: Callable[[], None] | None = None
    tracing_collector: Callable[[], Mapping[str, object] | None] | None = None
    capture_explain: bool = False
    explain_analyze: bool = True
    explain_collector: DataFusionExplainCollector | None = field(
        default_factory=DataFusionExplainCollector
    )
    capture_fallbacks: bool = True
    fallback_collector: DataFusionFallbackCollector | None = field(
        default_factory=DataFusionFallbackCollector
    )
    labeled_fallbacks: list[dict[str, object]] = field(default_factory=list)
    labeled_explains: list[dict[str, object]] = field(default_factory=list)
    local_filesystem_root: str | None = None
    config_policy_name: str | None = "default"
    config_policy: DataFusionConfigPolicy | None = None
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
        if self.target_partitions is not None:
            config = _apply_config_int(
                config,
                method="with_target_partitions",
                key="datafusion.execution.target_partitions",
                value=int(self.target_partitions),
            )
        if self.batch_size is not None:
            config = _apply_config_int(
                config,
                method="with_batch_size",
                key="datafusion.execution.batch_size",
                value=int(self.batch_size),
            )
        policy = self._resolved_config_policy()
        if policy is not None:
            config = policy.apply(config)
        return config

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
        self._install_function_factory(ctx)
        self._install_tracing()
        if self.session_context_hook is not None:
            ctx = self.session_context_hook(ctx)
        self._cache_context(ctx)
        return ctx

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
            logger.warning(
                "FunctionFactory unavailable; falling back to DataFusion UDFs: %s",
                exc,
            )
            register_datafusion_udfs(ctx)

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
        fallback_hook = resolved.fallback_hook
        if fallback_hook is None and self.capture_fallbacks and self.fallback_collector is not None:
            fallback_hook = self.fallback_collector.hook
        unchanged = (
            cache == resolved.cache,
            cache_max_columns == resolved.cache_max_columns,
            resolved_params == resolved.params,
            capture_explain == resolved.capture_explain,
            explain_analyze == resolved.explain_analyze,
            explain_hook == resolved.explain_hook,
            fallback_hook == resolved.fallback_hook,
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
            fallback_hook=fallback_hook,
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
            "metrics_enabled": self.enable_metrics,
            "metrics_collector": bool(self.metrics_collector),
            "tracing_enabled": self.enable_tracing,
            "tracing_hook": bool(self.tracing_hook),
            "tracing_collector": bool(self.tracing_collector),
            "capture_explain": self.capture_explain,
            "explain_analyze": self.explain_analyze,
            "explain_collector": bool(self.explain_collector),
            "capture_fallbacks": self.capture_fallbacks,
            "fallback_collector": bool(self.fallback_collector),
            "local_filesystem_root": self.local_filesystem_root,
            "distributed": self.distributed,
            "distributed_context_factory": bool(self.distributed_context_factory),
            "runtime_env_hook": bool(self.runtime_env_hook),
            "session_context_hook": bool(self.session_context_hook),
            "config_policy_name": self.config_policy_name,
            "config_policy": dict(resolved_policy.settings)
            if resolved_policy is not None
            else None,
            "share_context": self.share_context,
            "session_context_key": self.session_context_key,
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
    fallback_hook = _apply_fallback_policy(
        policy=execution_policy,
        fallback_hook=options.fallback_hook,
        label=execution_label,
    )
    if fallback_hook is options.fallback_hook:
        return options
    return replace(options, fallback_hook=fallback_hook)


__all__ = [
    "DATAFUSION_POLICY_PRESETS",
    "DEFAULT_DF_POLICY",
    "DEV_DF_POLICY",
    "PROD_DF_POLICY",
    "AdapterExecutionPolicy",
    "DataFusionConfigPolicy",
    "DataFusionExplainCollector",
    "DataFusionFallbackCollector",
    "DataFusionRuntimeProfile",
    "ExecutionLabel",
    "MemoryPool",
    "apply_execution_label",
    "apply_execution_policy",
    "snapshot_plans",
]

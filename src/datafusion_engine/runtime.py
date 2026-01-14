"""Runtime profile helpers for DataFusion execution."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Literal, cast

import pyarrow as pa
from datafusion import RuntimeEnvBuilder, SessionConfig, SessionContext
from datafusion.dataframe import DataFrame
from datafusion.object_store import LocalFileSystem

from datafusion_engine.compile_options import DataFusionCompileOptions

if TYPE_CHECKING:
    from ibis.expr.types import Value as IbisValue

MemoryPool = Literal["greedy", "fair", "unbounded"]


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
        for key, value in self.settings.items():
            config = config.set(key, value)
        return config


DEFAULT_DF_POLICY = DataFusionConfigPolicy(
    settings={
        "datafusion.execution.collect_statistics": "true",
        "datafusion.execution.meta_fetch_concurrency": "32",
        "datafusion.execution.planning_concurrency": "0",
        "datafusion.execution.parquet.pushdown_filters": "true",
        "datafusion.execution.parquet.max_predicate_cache_size": "64M",
        "datafusion.runtime.list_files_cache_limit": "16M",
        "datafusion.runtime.list_files_cache_ttl": "2m",
        "datafusion.runtime.metadata_cache_limit": "128M",
        "datafusion.runtime.memory_limit": "8G",
        "datafusion.runtime.temp_directory": "/tmp/datafusion",
        "datafusion.runtime.max_temp_directory_size": "100G",
        "datafusion.execution.parquet.enable_page_index": "true",
        "datafusion.execution.parquet.metadata_size_hint": "524288",
    }
)


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
    local_filesystem_root: str | None = None
    config_policy: DataFusionConfigPolicy | None = DEFAULT_DF_POLICY
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
        if self.config_policy is not None:
            config = self.config_policy.apply(config)
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

        Raises
        ------
        ValueError
            Raised when distributed execution is enabled without a factory.
        """
        if self.distributed:
            if self.distributed_context_factory is None:
                msg = "Distributed execution requires distributed_context_factory."
                raise ValueError(msg)
            ctx = self.distributed_context_factory()
        else:
            ctx = SessionContext(self.session_config(), self.runtime_env_builder())
        if self.enable_url_table:
            ctx = ctx.enable_url_table()
        if self.local_filesystem_root is not None:
            store = LocalFileSystem(prefix=self.local_filesystem_root)
            ctx.register_object_store("file://", store, None)
        if self.enable_function_factory:
            if self.function_factory_hook is None:
                msg = "Function factory enabled but function_factory_hook is not set."
                raise ValueError(msg)
            self.function_factory_hook(ctx)
        if self.enable_tracing:
            if self.tracing_hook is None:
                msg = "Tracing enabled but tracing_hook is not set."
                raise ValueError(msg)
            self.tracing_hook()
        if self.session_context_hook is not None:
            ctx = self.session_context_hook(ctx)
        return ctx

    def compile_options(
        self,
        *,
        options: DataFusionCompileOptions | None = None,
        params: Mapping[str, object] | Mapping[IbisValue, object] | None = None,
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
        if (
            cache == resolved.cache
            and cache_max_columns == resolved.cache_max_columns
            and resolved_params == resolved.params
        ):
            return resolved
        return replace(
            resolved,
            cache=cache,
            cache_max_columns=cache_max_columns,
            params=resolved_params,
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

    def telemetry_payload(self) -> dict[str, object]:
        """Return a diagnostics-friendly payload for the runtime profile.

        Returns
        -------
        dict[str, object]
            Runtime settings serialized for telemetry/diagnostics.
        """
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
            "local_filesystem_root": self.local_filesystem_root,
            "distributed": self.distributed,
            "distributed_context_factory": bool(self.distributed_context_factory),
            "runtime_env_hook": bool(self.runtime_env_hook),
            "session_context_hook": bool(self.session_context_hook),
            "config_policy": dict(self.config_policy.settings)
            if self.config_policy is not None
            else None,
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


__all__ = [
    "DEFAULT_DF_POLICY",
    "DataFusionConfigPolicy",
    "DataFusionRuntimeProfile",
    "MemoryPool",
    "snapshot_plans",
]

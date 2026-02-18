"""SessionConfig and SessionContext factory helpers."""

from __future__ import annotations

import time
from collections import deque
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Protocol

from datafusion import RuntimeEnvBuilder, SessionConfig, SessionContext

if TYPE_CHECKING:
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile

from datafusion_engine.session.cache_policy import cache_policy_settings
from datafusion_engine.session.delta_session_builder import (
    build_delta_session_context as _build_delta_session_context_impl,
)
from datafusion_engine.session.delta_session_builder import (
    build_runtime_policy_options as _build_runtime_policy_options_impl,
)
from datafusion_engine.session.delta_session_builder import (
    parse_runtime_size as _parse_runtime_size_impl,
)
from datafusion_engine.session.delta_session_builder import (
    split_runtime_settings as _split_runtime_settings_impl,
)
from datafusion_engine.session.helpers import deregister_table


class _SettingsProvider(Protocol):
    def settings(self) -> Mapping[str, str]:
        """Return a mapping of DataFusion config settings."""
        ...


def _set_config_if_supported(
    config: SessionConfig,
    *,
    key: str,
    value: str,
) -> SessionConfig:
    """Apply a config setting and skip keys unsupported by the active binding.

    Returns:
        SessionConfig: Updated config when the key is supported, else unchanged config.
    """
    if key.startswith("datafusion.runtime."):
        return config
    try:
        return config.set(key, value)
    except BaseException as exc:
        message = str(exc)
        if "Config value" in message and "not found" in message:
            return config
        raise


def _apply_settings_overrides(
    config: SessionConfig,
    overrides: Mapping[str, str],
) -> SessionConfig:
    for key, value in overrides.items():
        config = _set_config_if_supported(config, key=key, value=str(value))
    return config


def _apply_catalog_autoload(
    config: SessionConfig,
    *,
    location: str | None,
    file_format: str | None,
) -> SessionConfig:
    if location is not None:
        config = _set_config_if_supported(
            config,
            key="datafusion.catalog.location",
            value=location,
        )
    if file_format is not None:
        config = _set_config_if_supported(
            config,
            key="datafusion.catalog.format",
            value=file_format,
        )
    return config


def _apply_identifier_settings(
    config: SessionConfig,
    *,
    enable_ident_normalization: bool,
) -> SessionConfig:
    return _set_config_if_supported(
        config,
        key="datafusion.sql_parser.enable_ident_normalization",
        value=str(enable_ident_normalization).lower(),
    )


def _apply_feature_settings(
    config: SessionConfig,
    feature_gates: _SettingsProvider | None,
) -> SessionConfig:
    if feature_gates is None:
        return config
    settings_map = feature_gates.settings()
    for key, value in settings_map.items():
        config = _set_config_if_supported(config, key=key, value=value)
    return config


def _apply_join_settings(
    config: SessionConfig,
    join_policy: _SettingsProvider | None,
) -> SessionConfig:
    if join_policy is None:
        return config
    settings_map = join_policy.settings()
    for key, value in settings_map.items():
        config = _set_config_if_supported(config, key=key, value=value)
    return config


def _apply_explain_analyze_level(
    config: SessionConfig,
    *,
    level: str | None,
    supported: bool,
) -> SessionConfig:
    if level is None or not supported:
        return config
    return _set_config_if_supported(
        config,
        key="datafusion.explain.analyze_level",
        value=level,
    )


def _split_runtime_settings(
    settings: Mapping[str, str],
) -> tuple[dict[str, str], dict[str, str]]:
    return _split_runtime_settings_impl(settings)


def _parse_runtime_size(value: object) -> int | None:
    return _parse_runtime_size_impl(value)


def _build_delta_runtime_policy_options(
    module: object | None,
    runtime_settings: Mapping[str, str],
) -> tuple[object | None, dict[str, object] | None]:
    bridge = _build_runtime_policy_options_impl(module, runtime_settings)
    return bridge.options, bridge.payload


@dataclass(frozen=True)
class _DeltaSessionBuildResult:
    ctx: SessionContext | None
    available: bool
    installed: bool
    error: str | None
    cause: Exception | None
    runtime_policy_bridge: Mapping[str, object] | None = None


@dataclass
class DataFusionContextPool:
    """Pool of SessionContext instances with deterministic cleanup semantics."""

    profile: DataFusionRuntimeProfile
    size: int = 1
    run_name_prefix: str = "__run"
    _queue: deque[SessionContext] = field(default_factory=deque, init=False, repr=False)

    def __post_init__(self) -> None:
        """Normalize pool size and eagerly allocate pooled contexts."""
        resolved_size = max(1, int(self.size))
        object.__setattr__(self, "size", resolved_size)
        for _ in range(resolved_size):
            self._queue.append(SessionFactory(self.profile).build())

    @contextmanager
    def checkout(
        self,
        *,
        run_prefix: str | None = None,
    ) -> Iterator[SessionContext]:
        """Yield a pooled SessionContext and clean up run-scoped artifacts.

        Yields:
        ------
        SessionContext
            Session context borrowed from the pool for the caller's work.
        """
        ctx = self._queue.popleft() if self._queue else SessionFactory(self.profile).build()
        resolved_prefix = run_prefix or self.next_run_prefix()
        try:
            yield ctx
        finally:
            type(self).cleanup_ephemeral_objects(ctx, prefix=resolved_prefix)
            self._queue.append(ctx)

    def next_run_prefix(self) -> str:
        """Return a deterministic run-scoped object prefix.

        Returns:
        -------
        str
            Prefix used to identify temporary run-scoped objects.
        """
        return f"{self.run_name_prefix}_{int(time.time() * 1000)}"

    @staticmethod
    def cleanup_ephemeral_objects(ctx: SessionContext, *, prefix: str) -> None:
        """Deregister run-scoped tables from the session context."""
        try:
            rows = ctx.sql("SHOW TABLES").to_arrow_table().to_pylist()
        except (RuntimeError, TypeError, ValueError):
            return
        for row in rows:
            name = (
                row.get("table_name") or row.get("name") or row.get("table") or row.get("tableName")
            )
            if not isinstance(name, str):
                continue
            if not name.startswith(prefix):
                continue
            deregister_table(ctx, name)


def _build_delta_session_context(
    profile: DataFusionRuntimeProfile,
    runtime_env: RuntimeEnvBuilder,
) -> _DeltaSessionBuildResult:
    result = _build_delta_session_context_impl(profile, runtime_env)
    return _DeltaSessionBuildResult(
        ctx=result.ctx,
        available=result.available,
        installed=result.installed,
        error=result.error,
        cause=result.cause,
        runtime_policy_bridge=result.runtime_policy_bridge,
    )


@dataclass(frozen=True)
class SessionFactory:
    """Build SessionConfig and SessionContext from a runtime profile."""

    profile: DataFusionRuntimeProfile

    def build_config(self) -> SessionConfig:
        """Return a SessionConfig configured from the runtime profile.

        Returns:
        -------
        SessionConfig
            Configured SessionConfig instance.
        """
        from datafusion_engine.session.runtime_compile import (
            effective_catalog_autoload,
            effective_ident_normalization,
            supports_explain_analyze_level,
        )
        from datafusion_engine.session.runtime_config_policies import (
            resolved_config_policy,
            resolved_schema_hardening,
        )
        from datafusion_engine.session.runtime_telemetry import performance_policy_settings

        profile = self.profile
        config = SessionConfig()
        config = config.with_default_catalog_and_schema(
            profile.catalog.default_catalog,
            profile.catalog.default_schema,
        )
        config = config.with_create_default_catalog_and_schema(enabled=True)
        config = config.with_information_schema(profile.catalog.enable_information_schema)
        config = _apply_identifier_settings(
            config,
            enable_ident_normalization=effective_ident_normalization(profile),
        )
        target_partitions = profile.execution.target_partitions
        if target_partitions is not None:
            config = _set_config_if_supported(
                config,
                key="datafusion.execution.target_partitions",
                value=str(target_partitions),
            )
        batch_size = profile.execution.batch_size
        if batch_size is not None:
            config = _set_config_if_supported(
                config,
                key="datafusion.execution.batch_size",
                value=str(batch_size),
            )
        repartition_aggregations = profile.execution.repartition_aggregations
        if repartition_aggregations is not None:
            config = _set_config_if_supported(
                config,
                key="datafusion.optimizer.repartition_aggregations",
                value=str(repartition_aggregations).lower(),
            )
        repartition_windows = profile.execution.repartition_windows
        if repartition_windows is not None:
            config = _set_config_if_supported(
                config,
                key="datafusion.optimizer.repartition_windows",
                value=str(repartition_windows).lower(),
            )
        repartition_file_scans = profile.execution.repartition_file_scans
        if repartition_file_scans is not None:
            config = _set_config_if_supported(
                config,
                key="datafusion.execution.repartition_file_scans",
                value=str(repartition_file_scans).lower(),
            )
        repartition_file_min_size = profile.execution.repartition_file_min_size
        if repartition_file_min_size is not None:
            config = _set_config_if_supported(
                config,
                key="datafusion.execution.repartition_file_min_size",
                value=str(repartition_file_min_size),
            )
        minimum_parallel_output_files = profile.execution.minimum_parallel_output_files
        if minimum_parallel_output_files is not None:
            config = _set_config_if_supported(
                config,
                key="datafusion.execution.minimum_parallel_output_files",
                value=str(minimum_parallel_output_files),
            )
        soft_max_rows_per_output_file = profile.execution.soft_max_rows_per_output_file
        if soft_max_rows_per_output_file is not None:
            config = _set_config_if_supported(
                config,
                key="datafusion.execution.soft_max_rows_per_output_file",
                value=str(soft_max_rows_per_output_file),
            )
        maximum_parallel_row_group_writers = profile.execution.maximum_parallel_row_group_writers
        if maximum_parallel_row_group_writers is not None:
            config = _set_config_if_supported(
                config,
                key="datafusion.execution.maximum_parallel_row_group_writers",
                value=str(maximum_parallel_row_group_writers),
            )
        objectstore_writer_buffer_size = profile.execution.objectstore_writer_buffer_size
        if objectstore_writer_buffer_size is not None:
            config = _set_config_if_supported(
                config,
                key="datafusion.execution.objectstore_writer_buffer_size",
                value=str(objectstore_writer_buffer_size),
            )
        catalog_location, catalog_format = effective_catalog_autoload(profile)
        config = _apply_catalog_autoload(
            config,
            location=catalog_location,
            file_format=catalog_format,
        )
        config_policy = resolved_config_policy(profile)
        if config_policy is not None:
            config = config_policy.apply(config)
        if profile.policies.cache_policy is not None:
            config = _apply_settings_overrides(
                config,
                cache_policy_settings(profile.policies.cache_policy),
            )
        config = _apply_settings_overrides(
            config,
            performance_policy_settings(profile),
        )
        schema_hardening = resolved_schema_hardening(profile)
        if schema_hardening is not None:
            config = schema_hardening.apply(config)
        config = _apply_settings_overrides(config, profile.policies.settings_overrides)
        config = _apply_feature_settings(config, profile.policies.feature_gates)
        config = _apply_join_settings(config, profile.policies.join_policy)
        return _apply_explain_analyze_level(
            config,
            level=profile.diagnostics.explain_analyze_level,
            supported=supports_explain_analyze_level(),
        )

    def build_runtime_env(self) -> RuntimeEnvBuilder:
        """Return a RuntimeEnvBuilder configured from the runtime profile.

        Returns:
        -------
        RuntimeEnvBuilder
            Configured runtime environment builder.
        """
        return self.profile.runtime_env_builder()

    def build(self) -> SessionContext:
        """Return a SessionContext configured from the runtime profile.

        Returns:
        -------
        SessionContext
            Configured SessionContext instance.
        """
        return self._build_local_context()

    def _build_local_context(self) -> SessionContext:
        profile = self.profile
        from datafusion_engine.session.runtime_extensions import record_delta_session_defaults

        if not profile.features.enable_delta_session_defaults:
            return SessionContext(self.build_config(), self.build_runtime_env())
        result = _build_delta_session_context(profile, self.build_runtime_env())
        record_delta_session_defaults(
            profile,
            available=result.available,
            installed=result.installed,
            error=result.error,
            runtime_policy_bridge=result.runtime_policy_bridge,
        )
        if result.error is not None:
            msg = "Delta session defaults require datafusion_ext."
            raise RuntimeError(msg) from result.cause
        if result.ctx is None:
            msg = "Delta session context construction failed."
            raise RuntimeError(msg)
        return result.ctx


__all__ = ["DataFusionContextPool", "SessionFactory"]

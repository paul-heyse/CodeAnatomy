"""SessionConfig and SessionContext factory helpers."""

from __future__ import annotations

import importlib
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol, cast

from datafusion import RuntimeEnvBuilder, SessionConfig, SessionContext

if TYPE_CHECKING:
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile

from datafusion_engine.session.cache_policy import cache_policy_settings


class _SettingsProvider(Protocol):
    def settings(self) -> Mapping[str, str]:
        """Return a mapping of DataFusion config settings."""
        ...


def _apply_setting(
    config: SessionConfig,
    *,
    method: str | None,
    key: str,
    value: int | bool | str | None,
) -> SessionConfig:
    if value is None:
        return config
    if method is not None:
        updater = getattr(config, method, None)
        if callable(updater):
            updated = updater(value)
            return cast("SessionConfig", updated)
    setter = getattr(config, "set", None)
    if callable(setter):
        str_value = str(value).lower() if isinstance(value, bool) else str(value)
        updated = setter(key, str_value)
        return cast("SessionConfig", updated)
    return config


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


def _apply_feature_settings(
    config: SessionConfig,
    feature_gates: _SettingsProvider | None,
) -> SessionConfig:
    if feature_gates is None:
        return config
    settings_map = feature_gates.settings()
    for key, value in settings_map.items():
        try:
            config = config.set(key, value)
        except (RuntimeError, TypeError, ValueError) as exc:
            message = str(exc)
            if "Config value" in message and "not found" in message:
                continue
            raise
    return config


def _apply_join_settings(
    config: SessionConfig,
    join_policy: _SettingsProvider | None,
) -> SessionConfig:
    if join_policy is None:
        return config
    settings_map = join_policy.settings()
    for key, value in settings_map.items():
        config = config.set(key, value)
    return config


def _apply_explain_analyze_level(
    config: SessionConfig,
    *,
    level: str | None,
    supported: bool,
) -> SessionConfig:
    if level is None or not supported:
        return config
    return config.set("datafusion.explain.analyze_level", level)


@dataclass(frozen=True)
class _DeltaSessionBuildResult:
    ctx: SessionContext | None
    available: bool
    installed: bool
    error: str | None
    cause: Exception | None


def _build_delta_session_context(
    profile: DataFusionRuntimeProfile,
    runtime_env: RuntimeEnvBuilder,
) -> _DeltaSessionBuildResult:
    from datafusion_engine.session.runtime import delta_runtime_env_options

    available = True
    error: str | None = None
    cause: Exception | None = None
    builder: object | None = None
    builder_module: str | None = None
    for module_name in ("datafusion._internal", "datafusion_ext"):
        try:
            module = importlib.import_module(module_name)
        except ImportError as exc:
            available = False
            error = str(exc)
            cause = exc
            continue
        builder = getattr(module, "delta_session_context", None)
        if callable(builder):
            builder_module = module_name
            break
        error = f"{module_name}.delta_session_context is unavailable."
        cause = TypeError(error)
        builder = None
    if builder is None:
        return _DeltaSessionBuildResult(
            ctx=None,
            available=available,
            installed=False,
            error=error,
            cause=cause,
        )
    builder_fn = cast(
        "Callable[[list[tuple[str, str]], RuntimeEnvBuilder | None, object | None], SessionContext]",
        builder,
    )
    try:
        settings = profile.settings_payload()
        if builder_module == "datafusion_ext":
            settings = {
                key: value
                for key, value in settings.items()
                if not key.startswith("datafusion.runtime.")
            }
        settings["datafusion.catalog.information_schema"] = str(
            profile.enable_information_schema
        ).lower()
        delta_runtime = delta_runtime_env_options(profile)
        runtime_arg: RuntimeEnvBuilder | None = runtime_env
        if builder_module == "datafusion_ext":
            runtime_arg = None
        ctx = builder_fn(
            list(settings.items()),
            runtime_arg,
            delta_runtime,
        )
    except (RuntimeError, TypeError, ValueError) as exc:
        return _DeltaSessionBuildResult(
            ctx=None,
            available=available,
            installed=False,
            error=str(exc),
            cause=exc,
        )
    if not isinstance(ctx, SessionContext) and hasattr(ctx, "table") and not hasattr(ctx, "ctx"):
        wrapper = SessionContext.__new__(SessionContext)
        wrapper.ctx = ctx
        ctx = wrapper
    if not isinstance(ctx, SessionContext):
        message = "Delta session context must return a SessionContext."
        return _DeltaSessionBuildResult(
            ctx=None,
            available=available,
            installed=False,
            error=message,
            cause=TypeError(message),
        )
    return _DeltaSessionBuildResult(
        ctx=ctx,
        available=available,
        installed=True,
        error=None,
        cause=None,
    )


@dataclass(frozen=True)
class SessionFactory:
    """Build SessionConfig and SessionContext from a runtime profile."""

    profile: DataFusionRuntimeProfile

    def build_config(self) -> SessionConfig:
        """Return a SessionConfig configured from the runtime profile.

        Returns
        -------
        SessionConfig
            Configured SessionConfig instance.
        """
        from datafusion_engine.session.runtime import (
            effective_catalog_autoload,
            effective_ident_normalization,
            resolved_config_policy,
            resolved_schema_hardening,
            supports_explain_analyze_level,
        )

        profile = self.profile
        config = SessionConfig()
        config = config.with_default_catalog_and_schema(
            profile.default_catalog,
            profile.default_schema,
        )
        config = config.with_create_default_catalog_and_schema(enabled=True)
        config = config.with_information_schema(profile.enable_information_schema)
        config = _apply_identifier_settings(
            config,
            enable_ident_normalization=effective_ident_normalization(profile),
        )
        config = _apply_setting(
            config,
            method="with_target_partitions",
            key="datafusion.execution.target_partitions",
            value=profile.target_partitions,
        )
        config = _apply_setting(
            config,
            method="with_batch_size",
            key="datafusion.execution.batch_size",
            value=profile.batch_size,
        )
        config = _apply_setting(
            config,
            method="with_repartition_aggregations",
            key="datafusion.optimizer.repartition_aggregations",
            value=profile.repartition_aggregations,
        )
        config = _apply_setting(
            config,
            method="with_repartition_windows",
            key="datafusion.optimizer.repartition_windows",
            value=profile.repartition_windows,
        )
        config = _apply_setting(
            config,
            method="with_repartition_file_scans",
            key="datafusion.execution.repartition_file_scans",
            value=profile.repartition_file_scans,
        )
        config = _apply_setting(
            config,
            method=None,
            key="datafusion.execution.repartition_file_min_size",
            value=profile.repartition_file_min_size,
        )
        config = _apply_setting(
            config,
            method=None,
            key="datafusion.execution.minimum_parallel_output_files",
            value=profile.minimum_parallel_output_files,
        )
        config = _apply_setting(
            config,
            method=None,
            key="datafusion.execution.soft_max_rows_per_output_file",
            value=profile.soft_max_rows_per_output_file,
        )
        config = _apply_setting(
            config,
            method=None,
            key="datafusion.execution.maximum_parallel_row_group_writers",
            value=profile.maximum_parallel_row_group_writers,
        )
        config = _apply_setting(
            config,
            method=None,
            key="datafusion.execution.objectstore_writer_buffer_size",
            value=profile.objectstore_writer_buffer_size,
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
        if profile.cache_policy is not None:
            config = _apply_settings_overrides(
                config,
                cache_policy_settings(profile.cache_policy),
            )
        schema_hardening = resolved_schema_hardening(profile)
        if schema_hardening is not None:
            config = schema_hardening.apply(config)
        config = _apply_settings_overrides(config, profile.settings_overrides)
        config = _apply_feature_settings(config, profile.feature_gates)
        config = _apply_join_settings(config, profile.join_policy)
        return _apply_explain_analyze_level(
            config,
            level=profile.explain_analyze_level,
            supported=supports_explain_analyze_level(),
        )

    def build_runtime_env(self) -> RuntimeEnvBuilder:
        """Return a RuntimeEnvBuilder configured from the runtime profile.

        Returns
        -------
        RuntimeEnvBuilder
            Configured runtime environment builder.
        """
        return self.profile.runtime_env_builder()

    def build(self) -> SessionContext:
        """Return a SessionContext configured from the runtime profile.

        Returns
        -------
        SessionContext
            Configured SessionContext instance.
        """
        return self._build_local_context()

    def _build_local_context(self) -> SessionContext:
        profile = self.profile
        from datafusion_engine.session.runtime import record_delta_session_defaults

        if not profile.enable_delta_session_defaults:
            return SessionContext(self.build_config(), self.build_runtime_env())
        result = _build_delta_session_context(profile, self.build_runtime_env())
        record_delta_session_defaults(
            profile,
            available=result.available,
            installed=result.installed,
            error=result.error,
        )
        if result.error is not None:
            msg = "Delta session defaults require datafusion._internal or datafusion_ext."
            raise RuntimeError(msg) from result.cause
        if result.ctx is None:
            msg = "Delta session context construction failed."
            raise RuntimeError(msg)
        return result.ctx


__all__ = ["SessionFactory"]

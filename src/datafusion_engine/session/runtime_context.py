"""Runtime profile context-installation and access helpers."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

import pyarrow as pa
from datafusion import SessionContext, SQLOptions
from datafusion.object_store import LocalFileSystem

from cache.diskcache_factory import DiskCacheKind, cache_for_kind
from datafusion_engine.compile.options import DataFusionSqlPolicy, resolve_sql_policy
from datafusion_engine.registry_facade import RegistrationPhase
from datafusion_engine.schema.introspection_core import SchemaIntrospector
from datafusion_engine.schema.introspection_routines import _introspection_cache_for_ctx
from datafusion_engine.session.runtime_config_policies import (
    DataFusionConfigPolicy,
    SchemaHardeningProfile,
    _resolved_config_policy_for_profile,
    _resolved_schema_hardening_for_profile,
)
from datafusion_engine.session.runtime_extensions import (
    _install_cache_tables,
    _install_physical_expr_adapter_factory,
    _install_planner_rules,
    _install_tracing,
    _install_udf_platform,
    _record_cache_diagnostics,
    _record_extension_parity_validation,
    _refresh_udf_catalog,
    _validate_rule_function_allowlist,
)
from datafusion_engine.session.runtime_schema_registry import (
    _install_schema_registry,
    _prepare_statements,
)
from datafusion_engine.session.runtime_session import record_view_definition
from datafusion_engine.session.runtime_telemetry import _build_telemetry_payload_row
from datafusion_engine.sql.options import sql_options_for_profile, statement_sql_options_for_profile

if TYPE_CHECKING:
    from diskcache import Cache, FanoutCache

    from datafusion_engine.session.runtime import DataFusionRuntimeProfile
    from datafusion_engine.udf.metadata import UdfCatalog
    from datafusion_engine.views.artifacts import DataFusionViewArtifact


def _as_runtime_profile(profile: _RuntimeContextMixin) -> DataFusionRuntimeProfile:
    """Narrow runtime mixin receiver to canonical runtime profile type.

    Returns:
        DataFusionRuntimeProfile: Runtime profile view of the mixin receiver.
    """
    return cast("DataFusionRuntimeProfile", profile)


class _RuntimeContextMixin:
    def __getattr__(self, name: str) -> Any:
        msg = f"{type(self).__name__!s} has no attribute {name!r}"
        raise AttributeError(msg)

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
        _install_udf_platform(_as_runtime_profile(self), ctx)
        _install_planner_rules(_as_runtime_profile(self), ctx)

    def _install_schema_guards_for_context(self, ctx: SessionContext) -> None:
        _install_schema_registry(_as_runtime_profile(self), ctx)
        _validate_rule_function_allowlist(_as_runtime_profile(self), ctx)

    def _install_planning_extensions_for_context(self, ctx: SessionContext) -> None:
        _prepare_statements(_as_runtime_profile(self), ctx)
        self.delta_ops.ensure_delta_plan_codecs(ctx)

    def _install_extension_hooks_for_context(self, ctx: SessionContext) -> None:
        _record_extension_parity_validation(_as_runtime_profile(self), ctx)
        _install_physical_expr_adapter_factory(_as_runtime_profile(self), ctx)

    def _install_observability_for_context(self, ctx: SessionContext) -> None:
        _install_tracing(_as_runtime_profile(self), ctx)
        _install_cache_tables(_as_runtime_profile(self), ctx)
        _record_cache_diagnostics(_as_runtime_profile(self), ctx)

    def _install_input_plugins(self, ctx: SessionContext) -> None:
        """Install input plugins on the session context."""
        for plugin in self.policies.input_plugins:
            plugin(ctx)

    def _install_registry_catalogs(self, ctx: SessionContext) -> None:
        """Install registry-backed catalog providers on the session context."""
        if not self.catalog.registry_catalogs:
            return
        from datafusion_engine.catalog.provider import register_registry_catalogs

        catalog_name = self.catalog.registry_catalog_name or self.catalog.default_catalog
        register_registry_catalogs(
            ctx,
            catalogs=self.catalog.registry_catalogs,
            catalog_name=catalog_name,
            default_schema=self.catalog.default_schema,
            runtime_profile=_as_runtime_profile(self),
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
            _refresh_udf_catalog(_as_runtime_profile(self), ctx)
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
        return sql_options_for_profile(_as_runtime_profile(self))

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
        return statement_sql_options_for_profile(_as_runtime_profile(self))

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
        record_view_definition(_as_runtime_profile(self), artifact=artifact)

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
        return _resolved_config_policy_for_profile(_as_runtime_profile(self))

    def _resolved_schema_hardening(self) -> SchemaHardeningProfile | None:
        return _resolved_schema_hardening_for_profile(_as_runtime_profile(self))

    def _telemetry_payload_row(self) -> dict[str, object]:
        return _build_telemetry_payload_row(_as_runtime_profile(self))

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
        return self.session_context_cache.get(self._cache_key())

    def _cache_context(self, ctx: SessionContext) -> None:
        if not self.execution.share_context:
            return
        self.session_context_cache[self._cache_key()] = ctx

    def _build_session_context(self) -> SessionContext:
        """Create the SessionContext base for this runtime profile.

        Returns:
        -------
        datafusion.SessionContext
            Base session context for this profile.
        """
        from datafusion_engine.session.context_pool import SessionFactory

        return SessionFactory(_as_runtime_profile(self)).build()

    def _apply_url_table(self, ctx: SessionContext) -> SessionContext:
        return ctx.enable_url_table() if self.features.enable_url_table else ctx

    def _register_local_filesystem(self, ctx: SessionContext) -> None:
        if self.policies.local_filesystem_root is None:
            return
        store = LocalFileSystem(prefix=self.policies.local_filesystem_root)
        from datafusion_engine.io.adapter import DataFusionIOAdapter

        adapter = DataFusionIOAdapter(ctx=ctx, profile=_as_runtime_profile(self))
        adapter.register_object_store(scheme="file://", store=store, host=None)


__all__: list[str] = []

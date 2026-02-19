"""Runtime profile helpers for DataFusion execution."""

from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import TYPE_CHECKING, cast
from weakref import WeakKeyDictionary

import msgspec
from datafusion import (
    RuntimeEnvBuilder,
    SessionConfig,
    SessionContext,
)

from datafusion_engine.lineage.diagnostics import (
    DiagnosticsSink,
    ensure_recorder_sink,
)
from datafusion_engine.plan.cache import PlanCache, PlanProtoCache
from datafusion_engine.plan.perf_policy import (
    PerformancePolicy,
)
from datafusion_engine.registry_facade import RegistrationPhase
from datafusion_engine.session.context_pool import SessionFactory
from datafusion_engine.session.runtime_compile import (
    _supports_explain_analyze_level,
)

# Runtime helpers are imported from authority submodules; external callers
# should import those authority modules directly instead of this file.
from datafusion_engine.session.runtime_config_policies import (
    DataFusionConfigPolicy,
    SchemaHardeningProfile,
    _effective_catalog_autoload_for_profile,
    _resolved_config_policy_for_profile,
    _resolved_schema_hardening_for_profile,
)
from datafusion_engine.session.runtime_context import _RuntimeContextMixin
from datafusion_engine.session.runtime_diagnostics_mixin import _RuntimeDiagnosticsMixin

# Delegation imports for extracted runtime modules.
from datafusion_engine.session.runtime_extensions import (
    _install_delta_plan_codecs_extension,
    _record_delta_plan_codecs,
    _validate_async_udf_policy,
)
from datafusion_engine.session.runtime_hooks import (
    _apply_builder,
    _attach_cache_manager,
)
from datafusion_engine.session.runtime_ops import (
    RuntimeProfileCatalog,
    RuntimeProfileDeltaOps,
    RuntimeProfileIO,
    _RuntimeProfileCatalogFacadeMixin,
    _RuntimeProfileDeltaFacadeMixin,
    _RuntimeProfileIOFacadeMixin,
    delta_runtime_env_options,
)
from datafusion_engine.session.runtime_profile_config import (
    CatalogConfig,
    DataSourceConfig,
    DiagnosticsConfig,
    ExecutionConfig,
    FeatureGatesConfig,
    PolicyBundleConfig,
    ZeroRowBootstrapConfig,
)
from datafusion_engine.session.runtime_schema_registry import (
    _ast_dataset_location,
    _bytecode_dataset_location,
    _dataset_template,
)
from datafusion_engine.session.runtime_session import (
    DataFusionViewRegistry,
    SessionRuntime,
    _build_session_runtime_from_context,
    build_session_runtime,
)
from datafusion_engine.session.runtime_telemetry import _effective_ident_normalization
from datafusion_engine.sql.options import sql_options_for_profile
from datafusion_engine.udf.extension_runtime import ExtensionRegistries
from datafusion_engine.udf.platform import RustUdfPlatformRegistries
from serde_msgspec import MSGPACK_ENCODER, StructBaseStrict

_MISSING = object()
_COMPILE_RESOLVER_STRICT_ENV = "CODEANATOMY_COMPILE_RESOLVER_INVARIANTS_STRICT"
_CI_ENV = "CI"
_DEFAULT_PERFORMANCE_POLICY = PerformancePolicy()
_MIN_TARGET_PARTITIONS_FOR_JOIN_REPARTITION = 2

if TYPE_CHECKING:
    from typing import Protocol

    from datafusion_engine.bootstrap.zero_row import ZeroRowBootstrapReport, ZeroRowBootstrapRequest
    from datafusion_engine.obs.datafusion_runs import DataFusionRun
    from datafusion_engine.session.context_pool import DataFusionContextPool
    from datafusion_engine.udf.metadata import UdfCatalog
    from datafusion_engine.views.artifacts import CachePolicy
    from storage.cdf_cursor_protocol import CdfCursorStoreLike

    class _DeltaRuntimeEnvOptions(Protocol):
        max_spill_size: int | None
        max_temp_directory_size: int | None


from datafusion_engine.dataset.registry import (
    DatasetLocation,
)

_TELEMETRY_MSGPACK_ENCODER = MSGPACK_ENCODER

logger = logging.getLogger(__name__)


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


class _RuntimeProfileIdentityMixin:
    """Identity behavior for runtime profile instances."""

    def __hash__(self) -> int:
        """Use identity hashing for weak-key runtime registries.

        Returns:
        -------
        int
            Identity-based hash value.
        """
        return id(self)


class _RuntimeProfileQueryMixin:
    """Read-oriented runtime profile helper methods."""

    def delta_runtime_ctx(self) -> SessionContext:
        """Return canonical SessionContext used for Delta runtime operations."""
        profile = cast("DataFusionRuntimeProfile", self)
        return profile.session_context()

    def semantic_cache_overrides(self) -> Mapping[str, CachePolicy]:
        """Return explicit semantic view cache policy overrides."""
        profile = cast("DataFusionRuntimeProfile", self)
        return dict(profile.data_sources.semantic_output.cache_overrides)

    def dataset_candidates(
        self,
        destination: str,
    ) -> tuple[tuple[str, DatasetLocation], ...]:
        """Return dataset binding candidates for destination resolution."""
        profile = cast("DataFusionRuntimeProfile", self)
        candidates = dict(profile.data_sources.dataset_templates)
        candidates.update(profile.data_sources.extract_output.dataset_locations)
        direct = candidates.get(destination)
        if direct is not None:
            return ((destination, direct),)
        return tuple(candidates.items())

    def join_repartition_enabled(self, keys: list[str]) -> bool:
        """Return whether join repartitioning should be applied for keys."""
        profile = cast("DataFusionRuntimeProfile", self)
        if not keys:
            return False
        if (
            profile.execution.target_partitions is None
            or profile.execution.target_partitions < _MIN_TARGET_PARTITIONS_FOR_JOIN_REPARTITION
        ):
            return False
        join_policy = profile.policies.join_policy
        if join_policy is None:
            return True
        return bool(join_policy.repartition_joins)

    def effective_sql_options(self) -> object:
        """Return effective SQL options for the runtime profile."""
        profile = cast("DataFusionRuntimeProfile", self)
        return sql_options_for_profile(profile)

    def schema_hardening_view_types(self) -> frozenset[str]:
        """Return enabled schema-hardening view type labels."""
        profile = cast("DataFusionRuntimeProfile", self)
        schema_hardening = resolved_schema_hardening(profile)
        if schema_hardening is None or not schema_hardening.enable_view_types:
            return frozenset()
        return frozenset({"view"})

    def cdf_cursor_store(self) -> CdfCursorStoreLike | None:
        """Return configured CDF cursor store."""
        profile = cast("DataFusionRuntimeProfile", self)
        return profile.data_sources.cdf_cursor_store

    def diagnostics_sink(self) -> DiagnosticsSink | None:
        """Return configured diagnostics sink when available."""
        profile = cast("DataFusionRuntimeProfile", self)
        return profile.diagnostics.diagnostics_sink


class DataFusionRuntimeProfile(
    _RuntimeProfileIdentityMixin,
    _RuntimeProfileQueryMixin,
    _RuntimeProfileIOFacadeMixin,
    _RuntimeProfileCatalogFacadeMixin,
    _RuntimeProfileDeltaFacadeMixin,
    _RuntimeDiagnosticsMixin,
    _RuntimeContextMixin,
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
    session_context_cache: dict[str, SessionContext] = msgspec.field(default_factory=dict)
    session_runtime_cache: dict[str, object] = msgspec.field(default_factory=dict)
    runtime_settings_overlay: WeakKeyDictionary[SessionContext, dict[str, str]] = msgspec.field(
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
        # Runtime caches are ephemeral process-local state and must not be
        # shared when profiles are cloned via msgspec.structs.replace().
        object.__setattr__(self, "session_context_cache", {})
        object.__setattr__(self, "session_runtime_cache", {})
        object.__setattr__(self, "runtime_settings_overlay", WeakKeyDictionary())
        async_policy = _validate_async_udf_policy(self)
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

        Returns:
        -------
        _DeltaRuntimeEnvOptions | None
            Delta-specific runtime environment options or None if not configured.
        """
        return delta_runtime_env_options(self)

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
        from datafusion_engine.registry_facade import RegistrationPhaseOrchestrator

        RegistrationPhaseOrchestrator.run(self._ephemeral_context_phases(ctx))
        self._cache_context(ctx)
        return ctx

    def build_ephemeral_context(self) -> SessionContext:
        """Return a non-cached SessionContext configured from the profile.

        Returns:
        -------
        SessionContext
            Ephemeral session context configured for this profile.
        """
        from datafusion_engine.registry_facade import RegistrationPhaseOrchestrator

        ctx = self._apply_url_table(self._build_session_context())
        RegistrationPhaseOrchestrator.run(self._ephemeral_context_phases(ctx))
        return ctx

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

    @staticmethod
    def install_delta_plan_codecs(ctx: SessionContext) -> tuple[bool, bool]:
        """Install Delta plan codecs using the extension entrypoint.

        Returns:
        -------
        tuple[bool, bool]
            Tuple of (available, installed) flags.
        """
        return _install_delta_plan_codecs_extension(ctx)

    def record_delta_plan_codecs_event(self, *, available: bool, installed: bool) -> None:
        """Record the Delta plan codecs install status."""
        _record_delta_plan_codecs(self, available=available, installed=installed)

    def resolve_dataset_template(self, name: str) -> DatasetLocation | None:
        """Return a dataset location template for the name.

        Returns:
        -------
        DatasetLocation | None
            Template dataset location when configured.
        """
        return _dataset_template(self, name)

    def resolve_ast_dataset_location(self) -> DatasetLocation | None:
        """Return the configured AST dataset location.

        Returns:
        -------
        DatasetLocation | None
            AST dataset location when configured.
        """
        return _ast_dataset_location(self)

    def resolve_bytecode_dataset_location(self) -> DatasetLocation | None:
        """Return the configured bytecode dataset location.

        Returns:
        -------
        DatasetLocation | None
            Bytecode dataset location when configured.
        """
        return _bytecode_dataset_location(self)

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
        return build_session_runtime(self)

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


__all__ = ["DataFusionRuntimeProfile"]

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
    SEMANTIC_PROGRAM_MANIFEST_SPEC,
    ZERO_ROW_BOOTSTRAP_VALIDATION_SPEC,
)

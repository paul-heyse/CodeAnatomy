"""Runtime profile helpers for DataFusion execution."""

from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import TYPE_CHECKING
from weakref import WeakKeyDictionary

import msgspec
import pyarrow as pa
from datafusion import (
    RuntimeEnvBuilder,
    SessionConfig,
    SessionContext,
    SQLOptions,
)
from datafusion.object_store import LocalFileSystem

from cache.diskcache_factory import (
    DiskCacheKind,
    cache_for_kind,
)
from datafusion_engine.arrow.interop import (
    RecordBatchReaderLike,
    TableLike,
)
from datafusion_engine.compile.options import (
    DataFusionSqlPolicy,
    resolve_sql_policy,
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
from datafusion_engine.schema.introspection import (
    SchemaIntrospector,
)
from datafusion_engine.session.context_pool import SessionFactory
from datafusion_engine.session.features import (
    FeatureStateSnapshot,
    feature_state_snapshot,
)
from datafusion_engine.session.introspection import (
    register_cdf_inputs_for_profile,
    schema_introspector_for_profile,
)
from datafusion_engine.session.runtime_compile import (
    _supports_explain_analyze_level,
    compile_resolver_invariant_artifact_payload,
    compile_resolver_invariants_strict_mode,
    effective_catalog_autoload,
    effective_ident_normalization,
    record_artifact,
    record_compile_resolver_invariants,
    supports_explain_analyze_level,
)

# ---------------------------------------------------------------------------
# Imports from extracted runtime sub-modules.
#
# These modules were extracted from this file to reduce its size. All public
# and private names are imported here so that existing ``from
# datafusion_engine.session.runtime import X`` patterns continue to work
# without modification.
# ---------------------------------------------------------------------------
from datafusion_engine.session.runtime_compile_options import (
    compile_options_for_profile,
    resolve_compile_hooks,
    resolve_compile_sql_policy,
)
from datafusion_engine.session.runtime_config_policies import (
    CACHE_PROFILES,
    CST_AUTOLOAD_DF_POLICY,
    DATAFUSION_MAJOR_VERSION,
    DATAFUSION_OPTIMIZER_DYNAMIC_FILTER_SKIP_VERSION,
    DATAFUSION_POLICY_PRESETS,
    DATAFUSION_RUNTIME_SETTINGS_SKIP_VERSION,
    DEFAULT_DF_POLICY,
    DEV_DF_POLICY,
    GIB,
    KIB,
    MIB,
    PROD_DF_POLICY,
    SCHEMA_HARDENING_PRESETS,
    SYMTABLE_DF_POLICY,
    DataFusionConfigPolicy,
    DataFusionFeatureGates,
    DataFusionJoinPolicy,
    DataFusionSettingsContract,
    SchemaHardeningProfile,
    _effective_catalog_autoload_for_profile,
    _resolved_config_policy_for_profile,
    _resolved_schema_hardening_for_profile,
)
from datafusion_engine.session.runtime_dataset_io import (
    _introspection_cache_for_ctx,
    align_table_to_schema,
    assert_schema_metadata,
    cache_prefix_for_delta_snapshot,
    dataset_schema_from_context,
    dataset_spec_from_context,
    datasource_config_from_manifest,
    datasource_config_from_profile,
    extract_output_locations_for_profile,
    normalize_dataset_locations_for_profile,
    read_delta_as_reader,
    record_dataset_readiness,
    semantic_output_locations_for_profile,
)
from datafusion_engine.session.runtime_diagnostics_mixin import _RuntimeDiagnosticsMixin

# Delegation imports for extracted runtime modules.
from datafusion_engine.session.runtime_extensions import (
    _install_cache_tables as _ext_install_cache_tables,
)
from datafusion_engine.session.runtime_extensions import (
    _install_delta_plan_codecs_extension as _ext_install_delta_plan_codecs,
)
from datafusion_engine.session.runtime_extensions import (
    _install_physical_expr_adapter_factory as _ext_install_physical_expr_adapter,
)
from datafusion_engine.session.runtime_extensions import (
    _install_planner_rules as _ext_install_planner_rules,
)
from datafusion_engine.session.runtime_extensions import (
    _install_tracing as _ext_install_tracing,
)
from datafusion_engine.session.runtime_extensions import (
    _install_udf_platform as _ext_install_udf_platform,
)
from datafusion_engine.session.runtime_extensions import (
    _record_cache_diagnostics as _ext_record_cache_diagnostics,
)
from datafusion_engine.session.runtime_extensions import (
    _record_delta_plan_codecs as _ext_record_delta_plan_codecs,
)
from datafusion_engine.session.runtime_extensions import (
    _record_extension_parity_validation as _ext_record_extension_parity,
)
from datafusion_engine.session.runtime_extensions import (
    _refresh_udf_catalog as _ext_refresh_udf_catalog,
)
from datafusion_engine.session.runtime_extensions import (
    _validate_async_udf_policy as _ext_validate_async_udf_policy,
)
from datafusion_engine.session.runtime_extensions import (
    _validate_rule_function_allowlist as _ext_validate_rule_function_allowlist,
)
from datafusion_engine.session.runtime_extensions import (
    record_delta_session_defaults,
)
from datafusion_engine.session.runtime_hooks import (
    CacheEventHook,
    ExplainHook,
    PlanArtifactsHook,
    SemanticDiffHook,
    SqlIngestHook,
    SubstraitFallbackHook,
    _apply_builder,
    _attach_cache_manager,
    apply_execution_label,
    apply_execution_policy,
    diagnostics_arrow_ingest_hook,
    diagnostics_cache_hook,
    diagnostics_dml_hook,
    diagnostics_explain_hook,
    diagnostics_plan_artifacts_hook,
    diagnostics_semantic_diff_hook,
    diagnostics_sql_ingest_hook,
    diagnostics_substrait_fallback_hook,
    labeled_explain_hook,
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
    CST_DIAGNOSTIC_STATEMENTS,
    INFO_SCHEMA_STATEMENT_NAMES,
    INFO_SCHEMA_STATEMENTS,
    AdapterExecutionPolicy,
    CatalogConfig,
    DataSourceConfig,
    DiagnosticsConfig,
    ExecutionConfig,
    ExecutionLabel,
    ExtractOutputConfig,
    FeatureGatesConfig,
    MemoryPool,
    PolicyBundleConfig,
    PreparedStatementSpec,
    SemanticOutputConfig,
    ZeroRowBootstrapConfig,
)
from datafusion_engine.session.runtime_schema_registry import (
    _ast_dataset_location as _schema_ast_dataset_location,
)
from datafusion_engine.session.runtime_schema_registry import (
    _bytecode_dataset_location as _schema_bytecode_dataset_location,
)
from datafusion_engine.session.runtime_schema_registry import (
    _dataset_template as _schema_dataset_template,
)
from datafusion_engine.session.runtime_schema_registry import (
    _install_schema_registry as _schema_install_schema_registry,
)
from datafusion_engine.session.runtime_schema_registry import (
    _prepare_statements as _schema_prepare_statements,
)
from datafusion_engine.session.runtime_schema_registry import record_schema_snapshots_for_profile
from datafusion_engine.session.runtime_session import (
    DataFusionViewRegistry,
    SessionRuntime,
    _build_session_runtime_from_context,
    build_session_runtime,
    catalog_snapshot_for_profile,
    function_catalog_snapshot_for_profile,
    record_runtime_setting_override,
    record_view_definition,
    refresh_session_runtime,
    runtime_setting_overrides,
    session_runtime_for_context,
    session_runtime_hash,
    settings_snapshot_for_profile,
)
from datafusion_engine.session.runtime_telemetry import (
    SETTINGS_HASH_VERSION,
    TELEMETRY_PAYLOAD_VERSION,
    _build_telemetry_payload_row,
    performance_policy_applied_knobs,
    performance_policy_settings,
)
from datafusion_engine.session.runtime_telemetry import (
    _effective_ident_normalization as _telemetry_effective_ident_normalization,
)
from datafusion_engine.session.runtime_telemetry import (
    _identifier_normalization_mode as _telemetry_identifier_normalization_mode,
)
from datafusion_engine.session.runtime_udf import (
    SchemaRegistryValidationResult,
)
from datafusion_engine.sql.options import (
    sql_options_for_profile,
    statement_sql_options_for_profile,
)
from datafusion_engine.udf.extension_runtime import ExtensionRegistries
from datafusion_engine.udf.platform import RustUdfPlatformRegistries
from datafusion_engine.views.artifacts import DataFusionViewArtifact
from serde_msgspec import MSGPACK_ENCODER, StructBaseStrict

# Use the telemetry versions for _RuntimeDiagnosticsMixin compatibility.
_identifier_normalization_mode = _telemetry_identifier_normalization_mode
_effective_ident_normalization = _telemetry_effective_ident_normalization

_MISSING = object()
_COMPILE_RESOLVER_STRICT_ENV = "CODEANATOMY_COMPILE_RESOLVER_INVARIANTS_STRICT"
_CI_ENV = "CI"
_DEFAULT_PERFORMANCE_POLICY = PerformancePolicy()
_EXTENSION_MODULE_NAMES: tuple[str, ...] = ("datafusion_engine.extensions.datafusion_ext",)
# DataFusion Python currently raises plain ``Exception`` for many SQL/plan failures.
_DATAFUSION_SQL_ERROR = Exception

if TYPE_CHECKING:
    from typing import Protocol

    from diskcache import Cache, FanoutCache

    from datafusion_engine.bootstrap.zero_row import ZeroRowBootstrapReport, ZeroRowBootstrapRequest
    from datafusion_engine.session.context_pool import DataFusionContextPool
    from datafusion_engine.udf.metadata import UdfCatalog
    from obs.datafusion_runs import DataFusionRun

    class _DeltaRuntimeEnvOptions(Protocol):
        max_spill_size: int | None
        max_temp_directory_size: int | None


from datafusion_engine.dataset.registry import (
    DatasetLocation,
)

if TYPE_CHECKING:
    ExplainRows = TableLike | RecordBatchReaderLike
else:
    ExplainRows = object

_TELEMETRY_MSGPACK_ENCODER = MSGPACK_ENCODER

logger = logging.getLogger(__name__)

_SESSION_CONTEXT_CACHE: dict[str, SessionContext] = {}


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


class DataFusionRuntimeProfile(
    _RuntimeProfileIOFacadeMixin,
    _RuntimeProfileCatalogFacadeMixin,
    _RuntimeProfileDeltaFacadeMixin,
    _RuntimeDiagnosticsMixin,
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
        async_policy = _ext_validate_async_udf_policy(self)
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
        self._register_local_filesystem(ctx)
        self._install_input_plugins(ctx)
        self._install_registry_catalogs(ctx)
        self._install_view_schema(ctx)
        _ext_install_udf_platform(self, ctx)
        _ext_install_planner_rules(self, ctx)
        _schema_install_schema_registry(self, ctx)
        _ext_validate_rule_function_allowlist(self, ctx)
        _schema_prepare_statements(self, ctx)
        self.delta_ops.ensure_delta_plan_codecs(ctx)
        _ext_record_extension_parity(self, ctx)
        _ext_install_physical_expr_adapter(self, ctx)
        _ext_install_tracing(self, ctx)
        _ext_install_cache_tables(self, ctx)
        _ext_record_cache_diagnostics(self, ctx)
        self._cache_context(ctx)
        return ctx

    def build_ephemeral_context(self) -> SessionContext:
        """Return a non-cached SessionContext configured from the profile.

        Returns:
        -------
        SessionContext
            Ephemeral session context configured for this profile.
        """
        return self._apply_url_table(self._build_session_context())

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
        return _ext_install_delta_plan_codecs(ctx)

    def record_delta_plan_codecs_event(self, *, available: bool, installed: bool) -> None:
        """Record the Delta plan codecs install status."""
        _ext_record_delta_plan_codecs(self, available=available, installed=installed)

    def resolve_dataset_template(self, name: str) -> DatasetLocation | None:
        """Return a dataset location template for the name.

        Returns:
        -------
        DatasetLocation | None
            Template dataset location when configured.
        """
        return _schema_dataset_template(self, name)

    def resolve_ast_dataset_location(self) -> DatasetLocation | None:
        """Return the configured AST dataset location.

        Returns:
        -------
        DatasetLocation | None
            AST dataset location when configured.
        """
        return _schema_ast_dataset_location(self)

    def resolve_bytecode_dataset_location(self) -> DatasetLocation | None:
        """Return the configured bytecode dataset location.

        Returns:
        -------
        DatasetLocation | None
            Bytecode dataset location when configured.
        """
        return _schema_bytecode_dataset_location(self)

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
        return build_session_runtime(self, use_cache=True)

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
        _ext_install_udf_platform(self, ctx)
        _ext_install_planner_rules(self, ctx)

    def _install_schema_guards_for_context(self, ctx: SessionContext) -> None:
        _schema_install_schema_registry(self, ctx)
        _ext_validate_rule_function_allowlist(self, ctx)

    def _install_planning_extensions_for_context(self, ctx: SessionContext) -> None:
        _schema_prepare_statements(self, ctx)
        self.delta_ops.ensure_delta_plan_codecs(ctx)

    def _install_extension_hooks_for_context(self, ctx: SessionContext) -> None:
        _ext_record_extension_parity(self, ctx)
        _ext_install_physical_expr_adapter(self, ctx)

    def _install_observability_for_context(self, ctx: SessionContext) -> None:
        _ext_install_tracing(self, ctx)
        _ext_install_cache_tables(self, ctx)
        _ext_record_cache_diagnostics(self, ctx)

    def _install_input_plugins(self, ctx: SessionContext) -> None:
        """Install input plugins on the session context."""
        for plugin in self.policies.input_plugins:
            plugin(ctx)

    def _install_registry_catalogs(self, ctx: SessionContext) -> None:
        """Install registry-backed catalog providers on the session context."""
        if not self.catalog.registry_catalogs:
            return
        from datafusion_engine.catalog.provider import (
            register_registry_catalogs,
        )

        catalog_name = self.catalog.registry_catalog_name or self.catalog.default_catalog
        register_registry_catalogs(
            ctx,
            catalogs=self.catalog.registry_catalogs,
            catalog_name=catalog_name,
            default_schema=self.catalog.default_schema,
            runtime_profile=self,
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
            _ext_refresh_udf_catalog(self, ctx)
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
        return sql_options_for_profile(self)

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
        return statement_sql_options_for_profile(self)

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
        record_view_definition(self, artifact=artifact)

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
        return _resolved_config_policy_for_profile(self)

    def _resolved_schema_hardening(self) -> SchemaHardeningProfile | None:
        return _resolved_schema_hardening_for_profile(self)

    def _telemetry_payload_row(self) -> dict[str, object]:
        return _build_telemetry_payload_row(self)

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
        return _SESSION_CONTEXT_CACHE.get(self._cache_key())

    def _cache_context(self, ctx: SessionContext) -> None:
        if not self.execution.share_context:
            return
        _SESSION_CONTEXT_CACHE[self._cache_key()] = ctx

    def _build_session_context(self) -> SessionContext:
        """Create the SessionContext base for this runtime profile.

        Returns:
        -------
        datafusion.SessionContext
            Base session context for this profile.
        """
        return SessionFactory(self).build()

    def _apply_url_table(self, ctx: SessionContext) -> SessionContext:
        return ctx.enable_url_table() if self.features.enable_url_table else ctx

    def _register_local_filesystem(self, ctx: SessionContext) -> None:
        if self.policies.local_filesystem_root is None:
            return
        store = LocalFileSystem(prefix=self.policies.local_filesystem_root)
        from datafusion_engine.io.adapter import DataFusionIOAdapter

        adapter = DataFusionIOAdapter(ctx=ctx, profile=self)
        adapter.register_object_store(scheme="file://", store=store, host=None)


__all__ = [
    "CACHE_PROFILES",
    "CST_AUTOLOAD_DF_POLICY",
    "CST_DIAGNOSTIC_STATEMENTS",
    "DATAFUSION_MAJOR_VERSION",
    "DATAFUSION_OPTIMIZER_DYNAMIC_FILTER_SKIP_VERSION",
    "DATAFUSION_POLICY_PRESETS",
    "DATAFUSION_RUNTIME_SETTINGS_SKIP_VERSION",
    "DEFAULT_DF_POLICY",
    "DEV_DF_POLICY",
    "GIB",
    "INFO_SCHEMA_STATEMENTS",
    "INFO_SCHEMA_STATEMENT_NAMES",
    "KIB",
    "MIB",
    "PROD_DF_POLICY",
    "SCHEMA_HARDENING_PRESETS",
    # Telemetry
    "SETTINGS_HASH_VERSION",
    "SYMTABLE_DF_POLICY",
    "TELEMETRY_PAYLOAD_VERSION",
    "AdapterExecutionPolicy",
    "CacheEventHook",
    "CatalogConfig",
    # Config policies
    "DataFusionConfigPolicy",
    "DataFusionFeatureGates",
    "DataFusionJoinPolicy",
    # Core types
    "DataFusionRuntimeProfile",
    "DataFusionSettingsContract",
    "DataFusionViewRegistry",
    "DataSourceConfig",
    "DiagnosticsConfig",
    # Config structs
    "ExecutionConfig",
    "ExecutionLabel",
    # Hooks
    "ExplainHook",
    "ExtractOutputConfig",
    "FeatureGatesConfig",
    # Features
    "FeatureStateSnapshot",
    "MemoryPool",
    "PlanArtifactsHook",
    "PolicyBundleConfig",
    "PreparedStatementSpec",
    "SchemaHardeningProfile",
    # UDF helpers
    "SchemaRegistryValidationResult",
    "SemanticDiffHook",
    "SemanticOutputConfig",
    "SessionRuntime",
    "SqlIngestHook",
    "SubstraitFallbackHook",
    "ZeroRowBootstrapConfig",
    # Dataset IO
    "align_table_to_schema",
    "apply_execution_label",
    "apply_execution_policy",
    "assert_schema_metadata",
    # Session runtime
    "build_session_runtime",
    "cache_prefix_for_delta_snapshot",
    "catalog_snapshot_for_profile",
    "compile_options_for_profile",
    "compile_resolver_invariant_artifact_payload",
    "compile_resolver_invariants_strict_mode",
    "dataset_schema_from_context",
    "dataset_spec_from_context",
    "datasource_config_from_manifest",
    "datasource_config_from_profile",
    "diagnostics_arrow_ingest_hook",
    "diagnostics_cache_hook",
    "diagnostics_dml_hook",
    "diagnostics_explain_hook",
    "diagnostics_plan_artifacts_hook",
    "diagnostics_semantic_diff_hook",
    "diagnostics_sql_ingest_hook",
    "diagnostics_substrait_fallback_hook",
    "effective_catalog_autoload",
    "effective_ident_normalization",
    "extract_output_locations_for_profile",
    "feature_state_snapshot",
    "function_catalog_snapshot_for_profile",
    "labeled_explain_hook",
    "normalize_dataset_locations_for_profile",
    "performance_policy_applied_knobs",
    "performance_policy_settings",
    "read_delta_as_reader",
    # Compile helpers
    "record_artifact",
    "record_compile_resolver_invariants",
    "record_dataset_readiness",
    "record_delta_session_defaults",
    "record_runtime_setting_override",
    "record_schema_snapshots_for_profile",
    "record_view_definition",
    "refresh_session_runtime",
    # Introspection
    "register_cdf_inputs_for_profile",
    "resolve_compile_hooks",
    "resolve_compile_sql_policy",
    "resolved_config_policy",
    "resolved_schema_hardening",
    "runtime_setting_overrides",
    "schema_introspector_for_profile",
    "semantic_output_locations_for_profile",
    "session_runtime_for_context",
    "session_runtime_hash",
    "settings_snapshot_for_profile",
    # SQL options (re-exports)
    "sql_options_for_profile",
    "statement_sql_options_for_profile",
    "supports_explain_analyze_level",
]

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

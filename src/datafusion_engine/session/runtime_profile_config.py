"""Runtime profile configuration structs.

This module contains data-only configuration structures for DataFusion runtime profiles.
These structs have NO dependency on DataFusionRuntimeProfile.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Literal, cast

import msgspec

from cache.diskcache_factory import DiskCacheProfile, default_diskcache_profile
from core.config_base import FingerprintableConfig, config_fingerprint
from datafusion_engine.compile.options import DataFusionSqlPolicy
from datafusion_engine.dataset.registry import DatasetCatalog, DatasetLocation
from datafusion_engine.delta.protocol import DeltaProtocolSupport
from datafusion_engine.delta.store_policy import DeltaStorePolicy
from datafusion_engine.lineage.diagnostics import DiagnosticsSink
from datafusion_engine.plan.perf_policy import (
    PerformancePolicy,
    performance_policy_artifact_payload,
)
from datafusion_engine.session.cache_policy import CachePolicyConfig
from datafusion_engine.session.contracts import IdentifierNormalizationMode
from datafusion_engine.session.policy_groups import (
    CachePolicyGroup,
    DeltaPolicyGroup,
    RuntimeArtifactPolicyGroup,
    SqlPolicyGroup,
)
from datafusion_engine.session.runtime_config_policies import (
    DataFusionConfigPolicy,
    DataFusionFeatureGates,
    DataFusionJoinPolicy,
    SchemaHardeningProfile,
)
from datafusion_engine.views.artifacts import CachePolicy
from schema_spec.policies import DataFusionWritePolicy
from schema_spec.scan_policy import ScanPolicyConfig
from serde_msgspec import StructBaseStrict
from storage.cdf_cursor_protocol import CdfCursorStoreLike
from storage.deltalake.config import DeltaMutationPolicy

if TYPE_CHECKING:
    from datafusion import RuntimeEnvBuilder, SessionContext

    from datafusion_engine.arrow.interop import RecordBatchReaderLike, TableLike

    ExplainRows = TableLike | RecordBatchReaderLike
else:
    ExplainRows = object

MemoryPool = Literal["greedy", "fair", "unbounded"]

logger = logging.getLogger(__name__)

__all__ = [
    "CST_DIAGNOSTIC_STATEMENTS",
    "INFO_SCHEMA_STATEMENTS",
    "INFO_SCHEMA_STATEMENT_NAMES",
    "AdapterExecutionPolicy",
    "CatalogConfig",
    "DataSourceConfig",
    "DiagnosticsConfig",
    "ExecutionConfig",
    "ExecutionLabel",
    "ExtractOutputConfig",
    "FeatureGatesConfig",
    "MemoryPool",
    "PolicyBundleConfig",
    "PreparedStatementSpec",
    "SemanticOutputConfig",
    "ZeroRowBootstrapConfig",
]


_AST_OPTIONAL_VIEW_FUNCTIONS: dict[str, tuple[str, ...]] = {
    "ast_node_attrs": ("map_entries",),
    "ast_def_attrs": ("map_entries",),
    "ast_call_attrs": ("map_entries",),
    "ast_edge_attrs": ("map_entries",),
    "ast_span_metadata": ("arrow_metadata",),
}


@dataclass
class _DataFusionExplainCollector:
    """Collect EXPLAIN artifacts for diagnostics."""

    entries: list[dict[str, object]] = field(default_factory=list)

    def hook(self, sql: str, rows: ExplainRows) -> None:
        """Collect an explain payload for a single statement."""
        payload = {"sql": sql, "rows": rows}
        self.entries.append(cast("dict[str, object]", payload))

    def snapshot(self) -> list[dict[str, object]]:
        """Return a snapshot of explain artifacts.

        Returns:
        -------
        list[dict[str, object]]
            Collected explain artifacts.
        """
        return list(self.entries)


@dataclass
class _DataFusionPlanCollector:
    """Collect DataFusion plan artifacts."""

    entries: list[dict[str, object]] = field(default_factory=list)

    def hook(self, payload: Mapping[str, object]) -> None:
        """Collect a plan artifact payload."""
        self.entries.append(dict(payload))

    def snapshot(self) -> list[dict[str, object]]:
        """Return a snapshot of plan artifacts.

        Returns:
        -------
        list[dict[str, object]]
            Plan artifact payloads.
        """
        return list(self.entries)


class PreparedStatementSpec(StructBaseStrict, frozen=True):
    """Prepared statement specification for DataFusion."""

    name: str
    sql: str
    param_types: tuple[str, ...] = ()


@dataclass(frozen=True)
class AdapterExecutionPolicy(FingerprintableConfig):
    """Execution policy for adapterized execution handling."""

    def fingerprint_payload(self) -> Mapping[str, object]:
        """Return the fingerprint payload for adapter execution policy.

        Returns:
        -------
        Mapping[str, object]
            Payload describing adapter execution policy.
        """
        return {"policy": self.__class__.__name__}

    def fingerprint(self) -> str:
        """Return a stable fingerprint for adapter execution policy.

        Returns:
        -------
        str
            Stable fingerprint hash.
        """
        return config_fingerprint(self.fingerprint_payload())


@dataclass(frozen=True)
class ExecutionLabel:
    """Execution label for task-scoped diagnostics."""

    task_name: str
    output_dataset: str


CST_DIAGNOSTIC_STATEMENTS: tuple[PreparedStatementSpec, ...] = (
    PreparedStatementSpec(
        name="cst_refs_by_file",
        sql="SELECT * FROM cst_refs WHERE file_id = $1",
        param_types=("Utf8",),
    ),
    PreparedStatementSpec(
        name="cst_defs_by_file",
        sql="SELECT * FROM cst_defs WHERE file_id = $1",
        param_types=("Utf8",),
    ),
    PreparedStatementSpec(
        name="cst_callsites_by_file",
        sql="SELECT * FROM cst_callsites WHERE file_id = $1",
        param_types=("Utf8",),
    ),
)

INFO_SCHEMA_STATEMENTS: tuple[PreparedStatementSpec, ...] = (
    PreparedStatementSpec(
        name="table_names_snapshot",
        sql="SELECT table_name FROM information_schema.tables",
    ),
    PreparedStatementSpec(
        name="tables_snapshot",
        sql="SELECT table_catalog, table_schema, table_name, table_type FROM information_schema.tables",
    ),
    PreparedStatementSpec(
        name="df_settings_snapshot",
        sql="SELECT name, value FROM information_schema.df_settings",
    ),
    PreparedStatementSpec(
        name="routines_snapshot",
        sql="SELECT * FROM information_schema.routines",
    ),
    PreparedStatementSpec(
        name="parameters_snapshot",
        sql=(
            "SELECT specific_name AS routine_name, parameter_name, parameter_mode, "
            "data_type, ordinal_position FROM information_schema.parameters"
        ),
    ),
)
INFO_SCHEMA_STATEMENT_NAMES: frozenset[str] = frozenset(
    spec.name for spec in INFO_SCHEMA_STATEMENTS
)


class ExecutionConfig(StructBaseStrict, frozen=True):
    """Execution-level DataFusion settings."""

    target_partitions: int | None = None
    batch_size: int | None = None
    repartition_aggregations: bool | None = None
    repartition_windows: bool | None = None
    repartition_file_scans: bool | None = None
    repartition_file_min_size: int | None = None
    minimum_parallel_output_files: int | None = None
    soft_max_rows_per_output_file: int | None = None
    maximum_parallel_row_group_writers: int | None = None
    objectstore_writer_buffer_size: int | None = None
    spill_dir: str | None = None
    memory_pool: MemoryPool = "greedy"
    memory_limit_bytes: int | None = None
    delta_max_spill_size: int | None = None
    delta_max_temp_directory_size: int | None = None
    share_context: bool = True
    session_context_key: str | None = None

    def fingerprint_payload(self) -> Mapping[str, object]:
        """Return fingerprint payload for execution settings.

        Returns:
        -------
        Mapping[str, object]
            Payload describing execution-level configuration values.
        """
        return {
            "target_partitions": self.target_partitions,
            "batch_size": self.batch_size,
            "repartition_aggregations": self.repartition_aggregations,
            "repartition_windows": self.repartition_windows,
            "repartition_file_scans": self.repartition_file_scans,
            "repartition_file_min_size": self.repartition_file_min_size,
            "minimum_parallel_output_files": self.minimum_parallel_output_files,
            "soft_max_rows_per_output_file": self.soft_max_rows_per_output_file,
            "maximum_parallel_row_group_writers": self.maximum_parallel_row_group_writers,
            "objectstore_writer_buffer_size": self.objectstore_writer_buffer_size,
            "spill_dir": self.spill_dir,
            "memory_pool": self.memory_pool,
            "memory_limit_bytes": self.memory_limit_bytes,
            "delta_max_spill_size": self.delta_max_spill_size,
            "delta_max_temp_directory_size": self.delta_max_temp_directory_size,
            "share_context": self.share_context,
            "session_context_key": self.session_context_key,
        }


class CatalogConfig(StructBaseStrict, frozen=True):
    """Catalog and schema configuration."""

    default_catalog: str = "datafusion"
    default_schema: str = "public"
    view_catalog_name: str | None = None
    view_schema_name: str | None = "views"
    registry_catalogs: Mapping[str, DatasetCatalog] = msgspec.field(default_factory=dict)
    registry_catalog_name: str | None = None
    catalog_auto_load_location: str | None = None
    catalog_auto_load_format: str | None = None
    catalog_auto_load_enabled: bool = True
    enable_information_schema: bool = True

    def fingerprint_payload(self) -> Mapping[str, object]:
        """Return fingerprint payload for catalog settings.

        Returns:
        -------
        Mapping[str, object]
            Payload describing catalog configuration values.
        """
        registry_names = tuple(sorted(self.registry_catalogs))
        return {
            "default_catalog": self.default_catalog,
            "default_schema": self.default_schema,
            "view_catalog_name": self.view_catalog_name,
            "view_schema_name": self.view_schema_name,
            "registry_catalogs": list(registry_names),
            "registry_catalog_name": self.registry_catalog_name,
            "catalog_auto_load_location": self.catalog_auto_load_location,
            "catalog_auto_load_format": self.catalog_auto_load_format,
            "catalog_auto_load_enabled": self.catalog_auto_load_enabled,
            "enable_information_schema": self.enable_information_schema,
        }


class ExtractOutputConfig(StructBaseStrict, frozen=True):
    """Extract output configuration."""

    dataset_locations: Mapping[str, DatasetLocation] = msgspec.field(default_factory=dict)
    output_catalog_name: str | None = None
    output_root: str | None = None
    scip_dataset_locations: Mapping[str, DatasetLocation] = msgspec.field(default_factory=dict)


class SemanticOutputConfig(StructBaseStrict, frozen=True):
    """Semantic output configuration."""

    locations: Mapping[str, DatasetLocation] = msgspec.field(default_factory=dict)
    output_catalog_name: str | None = None
    output_root: str | None = None
    normalize_output_root: str | None = None
    cache_overrides: Mapping[str, CachePolicy] = msgspec.field(default_factory=dict)


class DataSourceConfig(StructBaseStrict, frozen=True):
    """Dataset location and output configuration."""

    dataset_templates: Mapping[str, DatasetLocation] = msgspec.field(default_factory=dict)
    extract_output: ExtractOutputConfig = msgspec.field(default_factory=ExtractOutputConfig)
    semantic_output: SemanticOutputConfig = msgspec.field(default_factory=SemanticOutputConfig)
    cdf_cursor_store: CdfCursorStoreLike | None = None


class ZeroRowBootstrapConfig(StructBaseStrict, frozen=True):
    """Zero-row bootstrap configuration for runtime validation and setup."""

    validation_mode: Literal["off", "bootstrap"] = "off"
    include_semantic_outputs: bool = True
    include_internal_tables: bool = True
    strict: bool = True
    allow_semantic_row_probe_fallback: bool = False
    bootstrap_mode: Literal["strict_zero_rows", "seeded_minimal_rows"] = "strict_zero_rows"
    seeded_datasets: tuple[str, ...] = ()

    def fingerprint_payload(self) -> Mapping[str, object]:
        """Return fingerprint payload for zero-row bootstrap settings.

        Returns:
        -------
        Mapping[str, object]
            Payload describing zero-row bootstrap behavior.
        """
        return {
            "validation_mode": self.validation_mode,
            "include_semantic_outputs": self.include_semantic_outputs,
            "include_internal_tables": self.include_internal_tables,
            "strict": self.strict,
            "allow_semantic_row_probe_fallback": self.allow_semantic_row_probe_fallback,
            "bootstrap_mode": self.bootstrap_mode,
            "seeded_datasets": list(self.seeded_datasets),
        }


class FeatureGatesConfig(StructBaseStrict, frozen=True):
    """Feature toggle configuration."""

    identifier_normalization_mode: IdentifierNormalizationMode = IdentifierNormalizationMode.RAW
    enable_url_table: bool = False
    cache_enabled: bool = False
    enable_cache_manager: bool = False
    enable_function_factory: bool = True
    enable_schema_registry: bool = True
    enable_expr_planners: bool = True
    enable_schema_evolution_adapter: bool = True
    enable_udfs: bool = True
    enable_async_udfs: bool = False
    enable_delta_cdf: bool = False
    enable_delta_session_defaults: bool = False
    enable_delta_querybuilder: bool = False
    enable_delta_plan_codecs: bool = False
    enforce_delta_ffi_provider: bool = True
    enable_metrics: bool = False
    enable_tracing: bool = False
    enforce_preflight: bool = True

    def fingerprint_payload(self) -> Mapping[str, object]:
        """Return fingerprint payload for feature gate settings.

        Returns:
        -------
        Mapping[str, object]
            Payload describing feature gate values.
        """
        return {
            "identifier_normalization_mode": self.identifier_normalization_mode.value,
            "enable_url_table": self.enable_url_table,
            "cache_enabled": self.cache_enabled,
            "enable_cache_manager": self.enable_cache_manager,
            "enable_function_factory": self.enable_function_factory,
            "enable_schema_registry": self.enable_schema_registry,
            "enable_expr_planners": self.enable_expr_planners,
            "enable_schema_evolution_adapter": self.enable_schema_evolution_adapter,
            "enable_udfs": self.enable_udfs,
            "enable_async_udfs": self.enable_async_udfs,
            "enable_delta_cdf": self.enable_delta_cdf,
            "enable_delta_session_defaults": self.enable_delta_session_defaults,
            "enable_delta_querybuilder": self.enable_delta_querybuilder,
            "enable_delta_plan_codecs": self.enable_delta_plan_codecs,
            "enforce_delta_ffi_provider": self.enforce_delta_ffi_provider,
            "enable_metrics": self.enable_metrics,
            "enable_tracing": self.enable_tracing,
            "enforce_preflight": self.enforce_preflight,
        }


class DiagnosticsConfig(StructBaseStrict, frozen=True):
    """Diagnostics, explain, and observability settings."""

    capture_explain: bool = True
    explain_verbose: bool = False
    explain_analyze: bool = True
    explain_analyze_threshold_ms: float | None = None
    explain_analyze_level: str | None = None
    explain_collector: _DataFusionExplainCollector | None = msgspec.field(
        default_factory=_DataFusionExplainCollector
    )
    capture_plan_artifacts: bool = True
    capture_semantic_diff: bool = False
    emit_semantic_quality_diagnostics: bool = True
    plan_collector: _DataFusionPlanCollector | None = msgspec.field(
        default_factory=_DataFusionPlanCollector
    )
    diagnostics_sink: DiagnosticsSink | None = None
    labeled_explains: list[dict[str, object]] = msgspec.field(default_factory=list)
    metrics_collector: Callable[[], Mapping[str, object] | None] | None = None
    tracing_hook: Callable[[SessionContext], None] | None = None
    tracing_collector: Callable[[], Mapping[str, object] | None] | None = None
    substrait_validation: bool = False
    validate_plan_determinism: bool = False
    strict_determinism: bool = False


class PolicyBundleConfig(StructBaseStrict, frozen=True):
    """Policy, hook, and cache configuration."""

    config_policy_name: str | None = "symtable"
    config_policy: DataFusionConfigPolicy | None = None
    cache_policy: CachePolicyConfig | None = None
    cache_profile_name: (
        Literal[
            "snapshot_pinned",
            "always_latest_ttl30s",
            "multi_tenant_strict",
        ]
        | None
    ) = None
    performance_policy: PerformancePolicy = msgspec.field(default_factory=PerformancePolicy)
    scan_policy: ScanPolicyConfig = msgspec.field(default_factory=ScanPolicyConfig)
    schema_hardening_name: str | None = "schema_hardening"
    schema_hardening: SchemaHardeningProfile | None = None
    sql_policy_name: str | None = "write"
    sql_policy: DataFusionSqlPolicy | None = None
    param_identifier_allowlist: tuple[str, ...] = ()
    external_table_options: Mapping[str, object] = msgspec.field(default_factory=dict)
    write_policy: DataFusionWritePolicy | None = None
    settings_overrides: Mapping[str, str] = msgspec.field(default_factory=dict)
    feature_gates: DataFusionFeatureGates = msgspec.field(default_factory=DataFusionFeatureGates)
    physical_rulepack_enabled: bool = True
    join_policy: DataFusionJoinPolicy | None = None
    cache_max_columns: int | None = 64
    cache_manager_factory: Callable[[], object] | None = None
    function_factory_hook: Callable[[SessionContext], None] | None = None
    expr_planner_names: tuple[str, ...] = ("codeanatomy_domain",)
    expr_planner_hook: Callable[[SessionContext], None] | None = None
    physical_expr_adapter_factory: object | None = None
    schema_adapter_factories: Mapping[str, object] = msgspec.field(default_factory=dict)
    udf_catalog_policy: Literal["default", "strict"] = "default"
    async_udf_timeout_ms: int | None = None
    async_udf_batch_size: int | None = None
    delta_plan_codec_physical: str = "delta_physical"
    delta_plan_codec_logical: str = "delta_logical"
    delta_store_policy: DeltaStorePolicy | None = None
    delta_mutation_policy: DeltaMutationPolicy | None = None
    delta_protocol_support: DeltaProtocolSupport | None = None
    delta_protocol_mode: Literal["error", "warn", "ignore"] = "error"
    diskcache_profile: DiskCacheProfile | None = msgspec.field(
        default_factory=default_diskcache_profile
    )
    snapshot_pinned_mode: Literal["off", "delta_version"] = "off"
    cache_output_root: str | None = None
    runtime_artifact_cache_enabled: bool = False
    runtime_artifact_cache_root: str | None = None
    metadata_cache_snapshot_enabled: bool = False
    local_filesystem_root: str | None = None
    plan_artifacts_root: str | None = None
    input_plugins: tuple[Callable[[SessionContext], None], ...] = ()
    prepared_statements: tuple[PreparedStatementSpec, ...] = INFO_SCHEMA_STATEMENTS
    runtime_env_hook: Callable[[RuntimeEnvBuilder], RuntimeEnvBuilder] | None = None

    def cache_policy_group(self) -> CachePolicyGroup:
        """Return cache-focused policy settings."""
        return CachePolicyGroup(
            cache_profile_name=self.cache_profile_name,
            cache_max_columns=self.cache_max_columns,
            snapshot_pinned_mode=self.snapshot_pinned_mode,
        )

    def sql_policy_group(self) -> SqlPolicyGroup:
        """Return SQL-focused policy settings."""
        return SqlPolicyGroup(
            sql_policy_name=self.sql_policy_name,
            param_identifier_allowlist=self.param_identifier_allowlist,
        )

    def delta_policy_group(self) -> DeltaPolicyGroup:
        """Return Delta-focused policy settings."""
        return DeltaPolicyGroup(
            delta_plan_codec_physical=self.delta_plan_codec_physical,
            delta_plan_codec_logical=self.delta_plan_codec_logical,
            delta_protocol_mode=self.delta_protocol_mode,
        )

    def runtime_artifact_policy_group(self) -> RuntimeArtifactPolicyGroup:
        """Return runtime artifact/cache policy settings."""
        return RuntimeArtifactPolicyGroup(
            runtime_artifact_cache_enabled=self.runtime_artifact_cache_enabled,
            runtime_artifact_cache_root=self.runtime_artifact_cache_root,
            metadata_cache_snapshot_enabled=self.metadata_cache_snapshot_enabled,
            plan_artifacts_root=self.plan_artifacts_root,
        )

    @staticmethod
    def _callable_identity(value: object | None) -> str | None:
        if value is None:
            return None
        qualname = getattr(value, "__qualname__", None) or getattr(value, "__name__", None)
        module = getattr(value, "__module__", None)
        if qualname and module:
            return f"{module}.{qualname}"
        cls = getattr(value, "__class__", None)
        if cls is None:
            return None
        return f"{cls.__module__}.{cls.__name__}"

    @staticmethod
    def _diskcache_profile_payload(
        profile: DiskCacheProfile | None,
    ) -> Mapping[str, object] | None:
        if profile is None:
            return None
        overrides = {
            str(kind): settings.fingerprint_payload()
            for kind, settings in sorted(profile.overrides.items(), key=lambda item: str(item[0]))
        }
        ttl_seconds = {
            str(kind): profile.ttl_seconds.get(kind)
            for kind in sorted(profile.ttl_seconds, key=str)
        }
        return {
            "root": str(profile.root),
            "base_settings": profile.base_settings.fingerprint_payload(),
            "overrides": overrides,
            "ttl_seconds": ttl_seconds,
        }

    @staticmethod
    def _schema_adapter_payload(
        factories: Mapping[str, object],
    ) -> Mapping[str, object]:
        return {
            str(name): PolicyBundleConfig._callable_identity(factory)
            for name, factory in sorted(factories.items(), key=lambda item: str(item[0]))
        }

    @staticmethod
    def _prepared_statement_payload(
        statements: tuple[PreparedStatementSpec, ...],
    ) -> list[dict[str, object]]:
        return [
            {
                "name": statement.name,
                "sql": statement.sql,
                "param_types": list(statement.param_types),
            }
            for statement in statements
        ]

    def fingerprint_payload(self) -> Mapping[str, object]:
        """Return fingerprint payload for policy bundle settings.

        Returns:
        -------
        Mapping[str, object]
            Payload describing policy bundle settings.
        """
        external_table_options = {
            str(key): str(value) for key, value in self.external_table_options.items()
        }
        settings_overrides = {
            str(key): str(value) for key, value in self.settings_overrides.items()
        }
        schema_hardening_payload = (
            self.schema_hardening.fingerprint_payload()
            if self.schema_hardening is not None
            else None
        )
        return {
            "config_policy_name": self.config_policy_name,
            "config_policy": (
                self.config_policy.fingerprint() if self.config_policy is not None else None
            ),
            "scan_policy": self.scan_policy.fingerprint(),
            "cache_policy": (
                self.cache_policy.fingerprint() if self.cache_policy is not None else None
            ),
            "cache_profile_name": self.cache_profile_name,
            "performance_policy": performance_policy_artifact_payload(self.performance_policy),
            "schema_hardening_name": self.schema_hardening_name,
            "schema_hardening": schema_hardening_payload,
            "sql_policy_name": self.sql_policy_name,
            "sql_policy": self.sql_policy.fingerprint() if self.sql_policy is not None else None,
            "param_identifier_allowlist": list(self.param_identifier_allowlist),
            "external_table_options": external_table_options,
            "write_policy": (
                self.write_policy.fingerprint() if self.write_policy is not None else None
            ),
            "settings_overrides": settings_overrides,
            "feature_gates": self.feature_gates.fingerprint_payload(),
            "physical_rulepack_enabled": self.physical_rulepack_enabled,
            "join_policy": self.join_policy.fingerprint() if self.join_policy is not None else None,
            "cache_max_columns": self.cache_max_columns,
            "cache_manager_factory": self._callable_identity(self.cache_manager_factory),
            "function_factory_hook": self._callable_identity(self.function_factory_hook),
            "expr_planner_names": list(self.expr_planner_names),
            "expr_planner_hook": self._callable_identity(self.expr_planner_hook),
            "physical_expr_adapter_factory": self._callable_identity(
                self.physical_expr_adapter_factory
            ),
            "schema_adapter_factories": self._schema_adapter_payload(self.schema_adapter_factories),
            "udf_catalog_policy": self.udf_catalog_policy,
            "async_udf_timeout_ms": self.async_udf_timeout_ms,
            "async_udf_batch_size": self.async_udf_batch_size,
            "delta_plan_codec_physical": self.delta_plan_codec_physical,
            "delta_plan_codec_logical": self.delta_plan_codec_logical,
            "delta_store_policy": (
                self.delta_store_policy.fingerprint()
                if self.delta_store_policy is not None
                else None
            ),
            "delta_mutation_policy": (
                self.delta_mutation_policy.fingerprint()
                if self.delta_mutation_policy is not None
                else None
            ),
            "delta_protocol_support": (
                {
                    "max_reader_version": self.delta_protocol_support.max_reader_version,
                    "max_writer_version": self.delta_protocol_support.max_writer_version,
                    "supported_reader_features": list(
                        self.delta_protocol_support.supported_reader_features
                    ),
                    "supported_writer_features": list(
                        self.delta_protocol_support.supported_writer_features
                    ),
                }
                if self.delta_protocol_support is not None
                else None
            ),
            "delta_protocol_mode": self.delta_protocol_mode,
            "diskcache_profile": self._diskcache_profile_payload(self.diskcache_profile),
            "snapshot_pinned_mode": self.snapshot_pinned_mode,
            "cache_output_root": self.cache_output_root,
            "runtime_artifact_cache_enabled": self.runtime_artifact_cache_enabled,
            "runtime_artifact_cache_root": self.runtime_artifact_cache_root,
            "metadata_cache_snapshot_enabled": self.metadata_cache_snapshot_enabled,
            "local_filesystem_root": self.local_filesystem_root,
            "plan_artifacts_root": self.plan_artifacts_root,
            "input_plugins": [self._callable_identity(plugin) for plugin in self.input_plugins],
            "prepared_statements": self._prepared_statement_payload(self.prepared_statements),
            "runtime_env_hook": self._callable_identity(self.runtime_env_hook),
        }

    def fingerprint(self) -> str:
        """Return fingerprint for policy bundle settings.

        Returns:
        -------
        str
            Deterministic fingerprint for the policy bundle.
        """
        return config_fingerprint(self.fingerprint_payload())

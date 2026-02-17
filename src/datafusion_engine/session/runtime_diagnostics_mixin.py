"""Runtime profile diagnostics mixin for telemetry and fingerprint computation."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, cast

import datafusion

from core.config_base import config_fingerprint
from datafusion_engine.delta.store_policy import delta_store_policy_hash
from datafusion_engine.lineage.diagnostics import record_events
from datafusion_engine.session.cache_policy import cache_policy_settings
from datafusion_engine.session.features import named_args_supported
from datafusion_engine.session.runtime_compile import record_artifact
from datafusion_engine.session.runtime_config_policies import (
    _ansi_mode,
    _resolved_config_policy_for_profile,
    _resolved_schema_hardening_for_profile,
)
from datafusion_engine.session.runtime_telemetry import (
    _SETTINGS_HASH_SCHEMA,
    _TELEMETRY_SCHEMA,
    SETTINGS_HASH_VERSION,
    _build_telemetry_payload_row,
    _cache_profile_settings,
    _datafusion_write_policy_payload,
    _delta_protocol_support_payload,
    _delta_store_policy_payload,
    _encode_telemetry_msgpack,
    _enrich_query_telemetry,
    _extra_settings_payload,
    _map_entries,
    _runtime_settings_payload,
    _settings_by_prefix,
    _telemetry_common_payload,
    _telemetry_enrichment_policy_for_profile,
    performance_policy_settings,
)
from datafusion_engine.session.runtime_telemetry import (
    _effective_ident_normalization as _telemetry_effective_ident_normalization,
)
from datafusion_engine.session.runtime_telemetry import (
    _identifier_normalization_mode as _telemetry_identifier_normalization_mode,
)
from storage.ipc_utils import payload_hash

if TYPE_CHECKING:
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile
    from serde_schema_registry import ArtifactSpec

# Re-export for backward compat with runtime.py aliases.
_identifier_normalization_mode = _telemetry_identifier_normalization_mode
_effective_ident_normalization = _telemetry_effective_ident_normalization


class _RuntimeDiagnosticsMixin:
    def _diagnostics_profile(self: object) -> DataFusionRuntimeProfile:
        return cast("DataFusionRuntimeProfile", self)

    def record_artifact(self, name: ArtifactSpec, payload: Mapping[str, object]) -> None:
        """Record an artifact through DiagnosticsRecorder when configured."""
        profile = self._diagnostics_profile()
        record_artifact(profile, name, payload)

    def record_events(self, name: str, rows: Sequence[Mapping[str, object]]) -> None:
        """Record events through DiagnosticsRecorder when configured."""
        profile = self._diagnostics_profile()
        record_events(profile, name, rows)

    def view_registry_snapshot(self) -> list[dict[str, object]] | None:
        """Return a stable snapshot of recorded view definitions.

        Returns:
        -------
        list[dict[str, object]] | None
            Snapshot payload or ``None`` when registry tracking is disabled.
        """
        profile = self._diagnostics_profile()
        if profile.view_registry is None:
            return None
        return profile.view_registry.snapshot()

    def settings_payload(self) -> dict[str, str]:
        """Return resolved settings applied to DataFusion SessionConfig.

        Returns:
        -------
        dict[str, str]
            Resolved DataFusion settings payload.
        """
        profile = self._diagnostics_profile()
        resolved_policy = _resolved_config_policy_for_profile(profile)
        payload: dict[str, str] = (
            dict(resolved_policy.settings) if resolved_policy is not None else {}
        )
        if profile.policies.cache_policy is not None:
            payload.update(cache_policy_settings(profile.policies.cache_policy))
        payload.update(_cache_profile_settings(profile))
        resolved_schema_hardening = _resolved_schema_hardening_for_profile(profile)
        if resolved_schema_hardening is not None:
            payload.update(resolved_schema_hardening.settings())
        payload.update(_runtime_settings_payload(profile))
        payload.update(performance_policy_settings(profile))
        if profile.policies.settings_overrides:
            payload.update(
                {str(key): str(value) for key, value in profile.policies.settings_overrides.items()}
            )
        payload.update(_extra_settings_payload(profile))
        return payload

    def settings_hash(self) -> str:
        """Return a stable hash for the SessionConfig settings payload.

        Returns:
        -------
        str
            SHA-256 hash for the settings payload.
        """
        profile = self._diagnostics_profile()
        payload = {
            "version": SETTINGS_HASH_VERSION,
            "entries": _map_entries(profile.settings_payload()),
        }
        return payload_hash(payload, _SETTINGS_HASH_SCHEMA)

    def fingerprint_payload(self) -> Mapping[str, object]:
        """Return a canonical fingerprint payload for the runtime profile.

        Returns:
        -------
        Mapping[str, object]
            Payload describing the runtime profile fingerprint inputs.
        """
        profile = self._diagnostics_profile()
        return {
            "version": 2,
            "architecture_version": profile.architecture_version,
            "settings_hash": profile.settings_hash(),
            "telemetry_hash": profile.telemetry_payload_hash(),
            "execution": profile.execution.fingerprint_payload(),
            "catalog": profile.catalog.fingerprint_payload(),
            "zero_row_bootstrap": profile.zero_row_bootstrap.fingerprint_payload(),
            "features": profile.features.fingerprint_payload(),
            "policies": profile.policies.fingerprint_payload(),
        }

    def fingerprint(self) -> str:
        """Return a stable fingerprint for the runtime profile.

        Returns:
        -------
        str
            Stable fingerprint for the runtime profile.
        """
        return config_fingerprint(self.fingerprint_payload())

    def telemetry_payload(self) -> dict[str, object]:
        """Return a diagnostics-friendly payload for the runtime profile.

        Returns:
        -------
        dict[str, object]
            Runtime settings serialized for telemetry/diagnostics.
        """
        profile = self._diagnostics_profile()
        resolved_policy = _resolved_config_policy_for_profile(profile)
        execution = profile.execution
        catalog = profile.catalog
        data_sources = profile.data_sources
        features = profile.features
        diagnostics = profile.diagnostics
        policies = profile.policies
        common_payload = _telemetry_common_payload(profile)
        template_payloads: dict[str, object] = {}
        for name, location in sorted(data_sources.dataset_templates.items()):
            resolved = location.resolved
            scan = resolved.datafusion_scan
            scan_payload: dict[str, object] | None = None
            if scan is not None:
                scan_payload = {
                    "file_sort_order": [list(key) for key in scan.file_sort_order],
                    "partition_cols": [
                        {"name": col_name, "dtype": str(dtype)}
                        for col_name, dtype in scan.partition_cols_pyarrow()
                    ],
                    "schema_force_view_types": scan.schema_force_view_types,
                    "skip_arrow_metadata": scan.skip_arrow_metadata,
                    "listing_table_factory_infer_partitions": (
                        scan.listing_table_factory_infer_partitions
                    ),
                    "listing_table_ignore_subdirectory": (scan.listing_table_ignore_subdirectory),
                    "collect_statistics": scan.collect_statistics,
                    "meta_fetch_concurrency": scan.meta_fetch_concurrency,
                    "list_files_cache_limit": scan.list_files_cache_limit,
                    "list_files_cache_ttl": scan.list_files_cache_ttl,
                    "unbounded": scan.unbounded,
                }
            template_payloads[name] = {
                "path": str(location.path),
                "format": location.format,
                "datafusion_provider": resolved.datafusion_provider,
                "delta_version": location.delta_version,
                "delta_timestamp": location.delta_timestamp,
                "delta_constraints": list(resolved.delta_constraints)
                if resolved.delta_constraints
                else None,
                "scan": scan_payload,
            }
        payload: dict[str, object] = {
            "datafusion_version": datafusion.__version__,
            "target_partitions": execution.target_partitions,
            "batch_size": execution.batch_size,
            "spill_dir": execution.spill_dir,
            "memory_pool": execution.memory_pool,
            "memory_limit_bytes": execution.memory_limit_bytes,
            "default_catalog": catalog.default_catalog,
            "default_schema": catalog.default_schema,
            "view_catalog": (
                catalog.view_catalog_name or catalog.default_catalog
                if catalog.view_schema_name is not None
                else None
            ),
            "view_schema": catalog.view_schema_name,
            "identifier_normalization_mode": _identifier_normalization_mode(profile).value,
            "enable_ident_normalization": _effective_ident_normalization(profile),
            "catalog_auto_load_location": catalog.catalog_auto_load_location,
            "catalog_auto_load_format": catalog.catalog_auto_load_format,
            "delta_store_policy_hash": delta_store_policy_hash(policies.delta_store_policy),
            "delta_store_policy": _delta_store_policy_payload(policies.delta_store_policy),
            "delta_mutation_policy_hash": (
                policies.delta_mutation_policy.fingerprint()
                if policies.delta_mutation_policy is not None
                else None
            ),
            "delta_mutation_policy": (
                {
                    "retry_policy": {
                        "max_attempts": policies.delta_mutation_policy.retry_policy.max_attempts,
                        "base_delay_s": policies.delta_mutation_policy.retry_policy.base_delay_s,
                        "max_delay_s": policies.delta_mutation_policy.retry_policy.max_delay_s,
                        "retryable_errors": list(
                            policies.delta_mutation_policy.retry_policy.retryable_errors
                        ),
                        "fatal_errors": list(
                            policies.delta_mutation_policy.retry_policy.fatal_errors
                        ),
                    },
                    "require_locking_provider": (
                        policies.delta_mutation_policy.require_locking_provider
                    ),
                    "locking_option_keys": list(policies.delta_mutation_policy.locking_option_keys),
                    "append_only": policies.delta_mutation_policy.append_only,
                }
                if policies.delta_mutation_policy is not None
                else None
            ),
            "dataset_templates": template_payloads or None,
            "enable_information_schema": catalog.enable_information_schema,
            "enable_url_table": features.enable_url_table,
            "cache_enabled": features.cache_enabled,
            "cache_max_columns": policies.cache_max_columns,
            "minimum_parallel_output_files": execution.minimum_parallel_output_files,
            "soft_max_rows_per_output_file": execution.soft_max_rows_per_output_file,
            "maximum_parallel_row_group_writers": execution.maximum_parallel_row_group_writers,
            "cache_manager_enabled": features.enable_cache_manager,
            "cache_manager_factory": bool(policies.cache_manager_factory),
            "function_factory_enabled": features.enable_function_factory,
            "function_factory_hook": bool(policies.function_factory_hook),
            "expr_planners_enabled": features.enable_expr_planners,
            "expr_planner_hook": bool(policies.expr_planner_hook),
            "expr_planner_names": list(policies.expr_planner_names),
            "enable_udfs": features.enable_udfs,
            "enable_async_udfs": features.enable_async_udfs,
            "async_udf_timeout_ms": policies.async_udf_timeout_ms,
            "async_udf_batch_size": policies.async_udf_batch_size,
            "physical_expr_adapter_factory": bool(policies.physical_expr_adapter_factory),
            "delta_session_defaults_enabled": features.enable_delta_session_defaults,
            "delta_querybuilder_enabled": features.enable_delta_querybuilder,
            "delta_plan_codecs_enabled": features.enable_delta_plan_codecs,
            "delta_plan_codec_physical": policies.delta_plan_codec_physical,
            "delta_plan_codec_logical": policies.delta_plan_codec_logical,
            "delta_ffi_provider_enforced": features.enforce_delta_ffi_provider,
            "metrics_enabled": features.enable_metrics,
            "metrics_collector": bool(diagnostics.metrics_collector),
            "tracing_enabled": features.enable_tracing,
            "tracing_hook": bool(diagnostics.tracing_hook),
            "tracing_collector": bool(diagnostics.tracing_collector),
            "capture_explain": diagnostics.capture_explain,
            "explain_verbose": diagnostics.explain_verbose,
            "explain_analyze": diagnostics.explain_analyze,
            "explain_analyze_threshold_ms": diagnostics.explain_analyze_threshold_ms,
            "explain_analyze_level": diagnostics.explain_analyze_level,
            "explain_collector": bool(diagnostics.explain_collector),
            "capture_plan_artifacts": diagnostics.capture_plan_artifacts,
            "capture_semantic_diff": diagnostics.capture_semantic_diff,
            "plan_collector": bool(diagnostics.plan_collector),
            "substrait_validation": diagnostics.substrait_validation,
            "diagnostics_sink": bool(diagnostics.diagnostics_sink),
            "local_filesystem_root": policies.local_filesystem_root,
            "plan_artifacts_root": policies.plan_artifacts_root,
            "input_plugins": len(policies.input_plugins),
            "prepared_statements": [stmt.name for stmt in policies.prepared_statements],
            "runtime_env_hook": bool(policies.runtime_env_hook),
            "config_policy_name": policies.config_policy_name,
            "schema_hardening_name": policies.schema_hardening_name,
            "config_policy": dict(resolved_policy.settings)
            if resolved_policy is not None
            else None,
            "settings_overrides": dict(policies.settings_overrides),
            **common_payload,
            "share_context": execution.share_context,
            "session_context_key": execution.session_context_key,
            "zero_row_bootstrap": profile.zero_row_bootstrap.fingerprint_payload(),
        }
        return _enrich_query_telemetry(
            payload,
            profile=profile,
            policy=_telemetry_enrichment_policy_for_profile(profile),
        )

    def telemetry_payload_v1(self) -> dict[str, object]:
        """Return a versioned runtime payload for diagnostics.

        Returns:
        -------
        dict[str, object]
            Versioned runtime payload with grouped settings.
        """
        profile = self._diagnostics_profile()
        settings = profile.settings_payload()
        ansi_mode = _ansi_mode(settings)
        parser_dialect = settings.get("datafusion.sql_parser.dialect")
        execution = profile.execution
        catalog = profile.catalog
        features = profile.features
        diagnostics = profile.diagnostics
        policies = profile.policies
        common_payload = _telemetry_common_payload(profile)
        return _enrich_query_telemetry(
            {
                "version": 2,
                "datafusion_version": datafusion.__version__,
                "schema_hardening_name": policies.schema_hardening_name,
                "session_config": dict(settings),
                **common_payload,
                "parquet_read": _settings_by_prefix(settings, "datafusion.execution.parquet."),
                "listing_table": _settings_by_prefix(settings, "datafusion.runtime.list_files_"),
                "spill": {
                    "spill_dir": execution.spill_dir,
                    "memory_pool": execution.memory_pool,
                    "memory_limit_bytes": execution.memory_limit_bytes,
                },
                "execution": {
                    "target_partitions": execution.target_partitions,
                    "batch_size": execution.batch_size,
                    "repartition_aggregations": execution.repartition_aggregations,
                    "repartition_windows": execution.repartition_windows,
                    "repartition_file_scans": execution.repartition_file_scans,
                    "repartition_file_min_size": execution.repartition_file_min_size,
                },
                "sql_surfaces": {
                    "enable_information_schema": catalog.enable_information_schema,
                    "identifier_normalization_mode": _identifier_normalization_mode(profile).value,
                    "enable_ident_normalization": _effective_ident_normalization(profile),
                    "enable_url_table": features.enable_url_table,
                    "sql_parser_dialect": parser_dialect,
                    "ansi_mode": ansi_mode,
                },
                "extensions": {
                    "delta_session_defaults_enabled": features.enable_delta_session_defaults,
                    "delta_runtime_env": {
                        "max_spill_size": execution.delta_max_spill_size,
                        "max_temp_directory_size": execution.delta_max_temp_directory_size,
                    },
                    "delta_querybuilder_enabled": features.enable_delta_querybuilder,
                    "delta_plan_codecs_enabled": features.enable_delta_plan_codecs,
                    "delta_plan_codec_physical": policies.delta_plan_codec_physical,
                    "delta_plan_codec_logical": policies.delta_plan_codec_logical,
                    "snapshot_pinned_mode": policies.snapshot_pinned_mode,
                    "delta_protocol_mode": policies.delta_protocol_mode,
                    "delta_protocol_support": _delta_protocol_support_payload(profile),
                    "expr_planners_enabled": features.enable_expr_planners,
                    "expr_planner_names": list(policies.expr_planner_names),
                    "physical_expr_adapter_factory": bool(policies.physical_expr_adapter_factory),
                    "schema_evolution_adapter_enabled": features.enable_schema_evolution_adapter,
                    "named_args_supported": named_args_supported(profile),
                    "async_udfs_enabled": features.enable_async_udfs,
                    "async_udf_timeout_ms": policies.async_udf_timeout_ms,
                    "async_udf_batch_size": policies.async_udf_batch_size,
                },
                "substrait_validation": diagnostics.substrait_validation,
                "output_writes": {
                    "cache_enabled": features.cache_enabled,
                    "cache_max_columns": policies.cache_max_columns,
                    "minimum_parallel_output_files": execution.minimum_parallel_output_files,
                    "soft_max_rows_per_output_file": execution.soft_max_rows_per_output_file,
                    "maximum_parallel_row_group_writers": (
                        execution.maximum_parallel_row_group_writers
                    ),
                    "objectstore_writer_buffer_size": execution.objectstore_writer_buffer_size,
                    "datafusion_write_policy": _datafusion_write_policy_payload(
                        policies.write_policy
                    ),
                },
                "zero_row_bootstrap": profile.zero_row_bootstrap.fingerprint_payload(),
            },
            profile=profile,
            policy=_telemetry_enrichment_policy_for_profile(profile),
        )

    def telemetry_payload_msgpack(self) -> bytes:
        """Return a MessagePack-encoded telemetry payload.

        Returns:
        -------
        bytes
            MessagePack payload for runtime telemetry.
        """
        return _encode_telemetry_msgpack(self.telemetry_payload_v1())

    def telemetry_payload_hash(self) -> str:
        """Return a stable hash for the versioned telemetry payload.

        Returns:
        -------
        str
            SHA-256 hash of the telemetry payload.
        """
        profile = self._diagnostics_profile()
        return payload_hash(_build_telemetry_payload_row(profile), _TELEMETRY_SCHEMA)


# Avoid circular import: DataFusionRuntimeProfile is used by type annotations only.
if TYPE_CHECKING:
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile

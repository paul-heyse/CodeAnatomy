"""Telemetry and performance policy helpers for DataFusion runtime profiles."""

from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import TYPE_CHECKING

import datafusion
import pyarrow as pa

from arrow_utils.core.schema_constants import DEFAULT_VALUE_META
from datafusion_engine.arrow.schema import map_entry_type, version_field, versioned_entries_schema
from datafusion_engine.delta.store_policy import DeltaStorePolicy, delta_store_policy_hash
from datafusion_engine.plan.perf_policy import (
    PerformancePolicy,
)
from datafusion_engine.session._session_constants import CACHE_PROFILES
from datafusion_engine.session.contracts import (
    TelemetryEnrichmentPolicy,
)
from datafusion_engine.session.features import named_args_supported
from datafusion_engine.session.runtime_compile import (
    _effective_ident_normalization,
    _identifier_normalization_mode,
)
from datafusion_engine.session.runtime_config_policies import (
    DATAFUSION_MAJOR_VERSION,
    DATAFUSION_RUNTIME_SETTINGS_SKIP_VERSION,
    _ansi_mode,
    _effective_catalog_autoload_for_profile,
)
from schema_spec.policies import DataFusionWritePolicy
from serde_msgspec import MSGPACK_ENCODER

if TYPE_CHECKING:
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile

__all__ = [
    "SETTINGS_HASH_VERSION",
    "TELEMETRY_PAYLOAD_VERSION",
    "performance_policy_applied_knobs",
    "performance_policy_settings",
]

_log = logging.getLogger(__name__)

SETTINGS_HASH_VERSION: int = 1
TELEMETRY_PAYLOAD_VERSION: int = 2

_TELEMETRY_MSGPACK_ENCODER = MSGPACK_ENCODER

_DEFAULT_PERFORMANCE_POLICY = PerformancePolicy()

# ---------------------------------------------------------------------------
# PyArrow Schema Constants
# ---------------------------------------------------------------------------
_MAP_ENTRY_SCHEMA = map_entry_type(with_kind=True)
_SETTINGS_HASH_SCHEMA = versioned_entries_schema(_MAP_ENTRY_SCHEMA)
_SESSION_RUNTIME_HASH_SCHEMA = pa.schema(
    [
        version_field(),
        pa.field("profile_context_key", pa.string(), nullable=False),
        pa.field("profile_settings_hash", pa.string(), nullable=False),
        pa.field("udf_snapshot_hash", pa.string(), nullable=False),
        pa.field("udf_rewrite_tags", pa.list_(pa.string()), nullable=False),
        pa.field("domain_planner_names", pa.list_(pa.string()), nullable=False),
        pa.field("df_settings_entries", pa.list_(_MAP_ENTRY_SCHEMA), nullable=False),
    ]
)
_SQL_POLICY_SCHEMA = pa.struct(
    [
        pa.field("allow_ddl", pa.bool_()),
        pa.field("allow_dml", pa.bool_()),
        pa.field("allow_statements", pa.bool_()),
    ]
)
_WRITE_POLICY_SCHEMA = pa.struct(
    [
        pa.field("partition_by", pa.list_(pa.string())),
        pa.field("single_file_output", pa.bool_()),
        pa.field("sort_by", pa.list_(pa.string())),
    ]
)
_SPILL_SCHEMA = pa.struct(
    [
        pa.field("spill_dir", pa.string()),
        pa.field("memory_pool", pa.string()),
        pa.field("memory_limit_bytes", pa.int64()),
    ]
)
_EXECUTION_SCHEMA = pa.struct(
    [
        pa.field("target_partitions", pa.int64()),
        pa.field("batch_size", pa.int64()),
        pa.field("repartition_aggregations", pa.bool_()),
        pa.field("repartition_windows", pa.bool_()),
        pa.field("repartition_file_scans", pa.bool_()),
        pa.field("repartition_file_min_size", pa.int64()),
    ]
)
_SQL_SURFACES_SCHEMA = pa.struct(
    [
        pa.field("enable_information_schema", pa.bool_()),
        pa.field("identifier_normalization_mode", pa.string()),
        pa.field("enable_ident_normalization", pa.bool_()),
        pa.field("enable_url_table", pa.bool_()),
        pa.field("sql_parser_dialect", pa.string()),
        pa.field("ansi_mode", pa.bool_()),
    ]
)
_DELTA_RUNTIME_ENV_SCHEMA = pa.struct(
    [
        pa.field("max_spill_size", pa.int64()),
        pa.field("max_temp_directory_size", pa.int64()),
    ]
)
_EXTENSIONS_SCHEMA = pa.struct(
    [
        pa.field("delta_session_defaults_enabled", pa.bool_()),
        pa.field("delta_runtime_env", _DELTA_RUNTIME_ENV_SCHEMA),
        pa.field("delta_querybuilder_enabled", pa.bool_()),
        pa.field("delta_plan_codecs_enabled", pa.bool_()),
        pa.field("delta_plan_codec_physical", pa.string()),
        pa.field("delta_plan_codec_logical", pa.string()),
        pa.field("snapshot_pinned_mode", pa.string()),
        pa.field("delta_ffi_provider_enforced", pa.bool_()),
        pa.field("expr_planners_enabled", pa.bool_()),
        pa.field("expr_planner_names", pa.list_(pa.string())),
        pa.field("physical_expr_adapter_factory", pa.bool_()),
        pa.field("schema_evolution_adapter_enabled", pa.bool_()),
        pa.field("named_args_supported", pa.bool_()),
        pa.field("async_udfs_enabled", pa.bool_()),
        pa.field("async_udf_timeout_ms", pa.int64()),
        pa.field("async_udf_batch_size", pa.int64()),
    ]
)
_OUTPUT_WRITES_SCHEMA = pa.struct(
    [
        pa.field("cache_enabled", pa.bool_()),
        pa.field("cache_max_columns", pa.int64()),
        pa.field("minimum_parallel_output_files", pa.int64()),
        pa.field("soft_max_rows_per_output_file", pa.int64()),
        pa.field("maximum_parallel_row_group_writers", pa.int64()),
        pa.field("objectstore_writer_buffer_size", pa.int64()),
        pa.field("datafusion_write_policy", _WRITE_POLICY_SCHEMA),
    ]
)
_ZERO_ROW_BOOTSTRAP_SCHEMA = pa.struct(
    [
        pa.field("validation_mode", pa.string()),
        pa.field("include_semantic_outputs", pa.bool_()),
        pa.field("include_internal_tables", pa.bool_()),
        pa.field("strict", pa.bool_()),
        pa.field("allow_semantic_row_probe_fallback", pa.bool_()),
        pa.field("bootstrap_mode", pa.string()),
        pa.field("seeded_datasets", pa.list_(pa.string())),
    ]
)
_DELTA_STORE_POLICY_SCHEMA = pa.struct(
    [
        pa.field("storage_options", pa.list_(_MAP_ENTRY_SCHEMA)),
        pa.field("log_storage_options", pa.list_(_MAP_ENTRY_SCHEMA)),
        pa.field("require_local_paths", pa.bool_()),
    ]
)
_DELTA_RETRY_POLICY_SCHEMA = pa.struct(
    [
        pa.field("max_attempts", pa.int64()),
        pa.field("base_delay_s", pa.float64()),
        pa.field("max_delay_s", pa.float64()),
        pa.field("retryable_errors", pa.list_(pa.string())),
        pa.field("fatal_errors", pa.list_(pa.string())),
    ]
)
_DELTA_MUTATION_POLICY_SCHEMA = pa.struct(
    [
        pa.field("retry_policy", _DELTA_RETRY_POLICY_SCHEMA),
        pa.field("require_locking_provider", pa.bool_()),
        pa.field("locking_option_keys", pa.list_(pa.string())),
        pa.field("append_only", pa.bool_()),
    ]
)
_TELEMETRY_SCHEMA = pa.schema(
    [
        version_field(nullable=True),
        pa.field("profile_name", pa.string()),
        pa.field("datafusion_version", pa.string()),
        pa.field("architecture_version", pa.string()),
        pa.field("sql_policy_name", pa.string()),
        pa.field("cache_profile_name", pa.string()),
        pa.field("session_config", pa.list_(_MAP_ENTRY_SCHEMA)),
        pa.field("settings_hash", pa.string()),
        pa.field("external_table_options", pa.list_(_MAP_ENTRY_SCHEMA)),
        pa.field("delta_store_policy_hash", pa.string()),
        pa.field("delta_store_policy", _DELTA_STORE_POLICY_SCHEMA),
        pa.field("delta_mutation_policy_hash", pa.string()),
        pa.field("delta_mutation_policy", _DELTA_MUTATION_POLICY_SCHEMA),
        pa.field("sql_policy", _SQL_POLICY_SCHEMA),
        pa.field("param_identifier_allowlist", pa.list_(pa.string())),
        pa.field("write_policy", _WRITE_POLICY_SCHEMA),
        pa.field("feature_gates", pa.list_(_MAP_ENTRY_SCHEMA)),
        pa.field("cache_profile_settings", pa.list_(_MAP_ENTRY_SCHEMA)),
        pa.field("join_policy", pa.list_(_MAP_ENTRY_SCHEMA)),
        pa.field("parquet_read", pa.list_(_MAP_ENTRY_SCHEMA)),
        pa.field("listing_table", pa.list_(_MAP_ENTRY_SCHEMA)),
        pa.field("spill", _SPILL_SCHEMA),
        pa.field("execution", _EXECUTION_SCHEMA),
        pa.field("sql_surfaces", _SQL_SURFACES_SCHEMA),
        pa.field("extensions", _EXTENSIONS_SCHEMA),
        pa.field("substrait_validation", pa.bool_()),
        pa.field("output_writes", _OUTPUT_WRITES_SCHEMA),
        pa.field("zero_row_bootstrap", _ZERO_ROW_BOOTSTRAP_SCHEMA),
    ]
)


# ---------------------------------------------------------------------------
# Telemetry Encoding
# ---------------------------------------------------------------------------
def _encode_telemetry_msgpack(payload: object) -> bytes:
    buf = bytearray()
    _TELEMETRY_MSGPACK_ENCODER.encode_into(payload, buf)
    return bytes(buf)


# ---------------------------------------------------------------------------
# Settings Helpers
# ---------------------------------------------------------------------------
def _settings_by_prefix(payload: Mapping[str, str], prefix: str) -> dict[str, str]:
    return {key: value for key, value in payload.items() if key.startswith(prefix)}


def _map_entries(payload: Mapping[str, object]) -> list[dict[str, object]]:
    return [
        _map_entry(key, value)
        for key, value in sorted(payload.items(), key=lambda item: str(item[0]))
    ]


def _map_entry(key: object, value: object) -> dict[str, object]:
    return {
        "key": str(key),
        "value_kind": _value_kind(value),
        "value": _value_text(value),
    }


def _value_kind(value: object) -> str:
    kind = "string"
    if value is None:
        kind = "null"
    elif isinstance(value, bool):
        kind = "bool"
    elif isinstance(value, int):
        kind = "int64"
    elif isinstance(value, float):
        kind = "float64"
    elif isinstance(value, bytes):
        kind = "binary"
    return kind


def _value_text(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, bytes):
        return value.hex()
    if isinstance(value, str):
        return value
    if isinstance(value, (bool, int, float)):
        return str(value)
    return _stable_repr(value)


def _stable_repr(value: object) -> str:
    if isinstance(value, Mapping):
        items = ", ".join(
            f"{_stable_repr(key)}:{_stable_repr(val)}"
            for key, val in sorted(value.items(), key=lambda item: str(item[0]))
        )
        return f"{{{items}}}"
    if isinstance(value, (list, tuple, set)):
        rendered = [_stable_repr(item) for item in value]
        if isinstance(value, set):
            rendered = sorted(rendered)
        items = ", ".join(rendered)
        bracket = "()" if isinstance(value, tuple) else "[]"
        return f"{bracket[0]}{items}{bracket[1]}"
    return repr(value)


def _default_value_entries(schema: pa.Schema) -> list[dict[str, str]]:
    entries: list[dict[str, str]] = []

    def _walk_field(field: pa.Field, *, prefix: str) -> None:
        path = f"{prefix}.{field.name}" if prefix else field.name
        meta = field.metadata or {}
        default_value = meta.get(DEFAULT_VALUE_META)
        if default_value is not None:
            entries.append(
                {
                    "path": path,
                    "default_value": default_value.decode("utf-8", errors="replace"),
                }
            )
        _walk_dtype(field.type, prefix=path)

    def _walk_dtype(dtype: pa.DataType, *, prefix: str) -> None:
        if pa.types.is_struct(dtype):
            for child in dtype:
                _walk_field(child, prefix=prefix)
            return
        if (
            pa.types.is_list(dtype)
            or pa.types.is_large_list(dtype)
            or pa.types.is_list_view(dtype)
            or pa.types.is_large_list_view(dtype)
        ):
            _walk_dtype(dtype.value_type, prefix=prefix)
            return
        if pa.types.is_map(dtype):
            _walk_dtype(dtype.item_type, prefix=prefix)

    for schema_field in schema:
        _walk_field(schema_field, prefix="")
    return entries


# ---------------------------------------------------------------------------
# Write Policy Helpers
# ---------------------------------------------------------------------------
def _datafusion_write_policy_payload(
    policy: DataFusionWritePolicy | None,
) -> dict[str, object] | None:
    if policy is None:
        return None
    return policy.payload()


def _delta_store_policy_payload(
    policy: DeltaStorePolicy | None,
) -> dict[str, object] | None:
    if policy is None:
        return None
    return {
        "storage_options": _map_entries(policy.storage_options),
        "log_storage_options": _map_entries(policy.log_storage_options),
        "require_local_paths": policy.require_local_paths,
    }


# ---------------------------------------------------------------------------
# Delta Protocol Support
# ---------------------------------------------------------------------------
def _delta_protocol_support_payload(
    profile: DataFusionRuntimeProfile,
) -> Mapping[str, object] | None:
    support = profile.policies.delta_protocol_support
    if support is None:
        return None
    return {
        "max_reader_version": support.max_reader_version,
        "max_writer_version": support.max_writer_version,
        "supported_reader_features": list(support.supported_reader_features),
        "supported_writer_features": list(support.supported_writer_features),
    }


def _supports_explain_analyze_level() -> bool:
    if DATAFUSION_MAJOR_VERSION is None:
        return False
    return DATAFUSION_MAJOR_VERSION >= DATAFUSION_RUNTIME_SETTINGS_SKIP_VERSION


def _sql_policy_payload(profile: DataFusionRuntimeProfile) -> dict[str, bool] | None:
    sql_policy = profile.policies.sql_policy
    if sql_policy is None:
        return None
    return {
        "allow_ddl": sql_policy.allow_ddl,
        "allow_dml": sql_policy.allow_dml,
        "allow_statements": sql_policy.allow_statements,
    }


def _feature_gate_settings(profile: DataFusionRuntimeProfile) -> dict[str, str]:
    feature_gates = profile.policies.feature_gates
    return dict(feature_gates.settings())


def _join_policy_settings(profile: DataFusionRuntimeProfile) -> dict[str, str] | None:
    join_policy = profile.policies.join_policy
    if join_policy is None:
        return None
    return dict(join_policy.settings())


def _external_table_options_payload(
    profile: DataFusionRuntimeProfile,
) -> dict[str, object] | None:
    options = profile.policies.external_table_options
    if not options:
        return None
    return {str(key): value for key, value in options.items()}


# ---------------------------------------------------------------------------
# Telemetry Builder Functions
# ---------------------------------------------------------------------------
def _telemetry_common_payload(profile: DataFusionRuntimeProfile) -> dict[str, object]:
    policies = profile.policies
    sql_policy_payload = _sql_policy_payload(profile)
    return {
        "profile_name": policies.config_policy_name,
        "sql_policy_name": policies.sql_policy_name,
        "cache_profile_name": policies.cache_profile_name,
        "settings_hash": profile.settings_hash(),
        "external_table_options": _external_table_options_payload(profile),
        "sql_policy": sql_policy_payload,
        "param_identifier_allowlist": (
            list(policies.param_identifier_allowlist)
            if policies.param_identifier_allowlist
            else None
        ),
        "write_policy": _datafusion_write_policy_payload(policies.write_policy),
        "feature_gates": _feature_gate_settings(profile),
        "cache_profile_settings": (
            _cache_profile_settings(profile) if policies.cache_profile_name is not None else None
        ),
        "join_policy": _join_policy_settings(profile),
    }


def _build_telemetry_payload_row(profile: DataFusionRuntimeProfile) -> dict[str, object]:
    settings = profile.settings_payload()
    sql_policy_payload = _sql_policy_payload(profile)
    write_policy_payload = _datafusion_write_policy_payload(profile.policies.write_policy)
    external_table_options = _external_table_options_payload(profile)
    join_policy_settings = _join_policy_settings(profile)
    delta_mutation_policy = profile.policies.delta_mutation_policy
    delta_mutation_policy_hash = (
        delta_mutation_policy.fingerprint() if delta_mutation_policy is not None else None
    )
    delta_mutation_policy_payload = None
    if delta_mutation_policy is not None:
        retry_policy = delta_mutation_policy.retry_policy
        delta_mutation_policy_payload = {
            "retry_policy": {
                "max_attempts": retry_policy.max_attempts,
                "base_delay_s": retry_policy.base_delay_s,
                "max_delay_s": retry_policy.max_delay_s,
                "retryable_errors": list(retry_policy.retryable_errors),
                "fatal_errors": list(retry_policy.fatal_errors),
            },
            "require_locking_provider": delta_mutation_policy.require_locking_provider,
            "locking_option_keys": list(delta_mutation_policy.locking_option_keys),
            "append_only": delta_mutation_policy.append_only,
        }
    parquet_read = _settings_by_prefix(settings, "datafusion.execution.parquet.")
    listing_table = _settings_by_prefix(settings, "datafusion.runtime.list_files_")
    parser_dialect = settings.get("datafusion.sql_parser.dialect")
    ansi_mode = _ansi_mode(settings)
    return {
        "version": TELEMETRY_PAYLOAD_VERSION,
        "profile_name": profile.policies.config_policy_name,
        "datafusion_version": datafusion.__version__,
        "architecture_version": profile.architecture_version,
        "sql_policy_name": profile.policies.sql_policy_name,
        "cache_profile_name": profile.policies.cache_profile_name,
        "session_config": _map_entries(settings),
        "settings_hash": profile.settings_hash(),
        "external_table_options": (
            _map_entries(external_table_options) if external_table_options is not None else None
        ),
        "delta_store_policy_hash": delta_store_policy_hash(profile.policies.delta_store_policy),
        "delta_store_policy": _delta_store_policy_payload(profile.policies.delta_store_policy),
        "delta_mutation_policy_hash": delta_mutation_policy_hash,
        "delta_mutation_policy": delta_mutation_policy_payload,
        "sql_policy": sql_policy_payload,
        "param_identifier_allowlist": (
            list(profile.policies.param_identifier_allowlist)
            if profile.policies.param_identifier_allowlist
            else None
        ),
        "write_policy": write_policy_payload,
        "feature_gates": _map_entries(_feature_gate_settings(profile)),
        "cache_profile_settings": (
            _map_entries(_cache_profile_settings(profile))
            if profile.policies.cache_profile_name is not None
            else None
        ),
        "join_policy": _map_entries(join_policy_settings)
        if join_policy_settings is not None
        else None,
        "parquet_read": _map_entries(parquet_read),
        "listing_table": _map_entries(listing_table),
        "spill": {
            "spill_dir": profile.execution.spill_dir,
            "memory_pool": profile.execution.memory_pool,
            "memory_limit_bytes": profile.execution.memory_limit_bytes,
        },
        "execution": {
            "target_partitions": profile.execution.target_partitions,
            "batch_size": profile.execution.batch_size,
            "repartition_aggregations": profile.execution.repartition_aggregations,
            "repartition_windows": profile.execution.repartition_windows,
            "repartition_file_scans": profile.execution.repartition_file_scans,
            "repartition_file_min_size": profile.execution.repartition_file_min_size,
        },
        "sql_surfaces": {
            "enable_information_schema": profile.catalog.enable_information_schema,
            "identifier_normalization_mode": _identifier_normalization_mode(profile).value,
            "enable_ident_normalization": _effective_ident_normalization(profile),
            "enable_url_table": profile.features.enable_url_table,
            "sql_parser_dialect": parser_dialect,
            "ansi_mode": ansi_mode,
        },
        "extensions": {
            "delta_session_defaults_enabled": profile.features.enable_delta_session_defaults,
            "delta_runtime_env": {
                "max_spill_size": profile.execution.delta_max_spill_size,
                "max_temp_directory_size": profile.execution.delta_max_temp_directory_size,
            },
            "delta_querybuilder_enabled": profile.features.enable_delta_querybuilder,
            "delta_plan_codecs_enabled": profile.features.enable_delta_plan_codecs,
            "delta_plan_codec_physical": profile.policies.delta_plan_codec_physical,
            "delta_plan_codec_logical": profile.policies.delta_plan_codec_logical,
            "snapshot_pinned_mode": profile.policies.snapshot_pinned_mode,
            "delta_ffi_provider_enforced": profile.features.enforce_delta_ffi_provider,
            "expr_planners_enabled": profile.features.enable_expr_planners,
            "expr_planner_names": list(profile.policies.expr_planner_names),
            "physical_expr_adapter_factory": bool(profile.policies.physical_expr_adapter_factory),
            "schema_evolution_adapter_enabled": (profile.features.enable_schema_evolution_adapter),
            "named_args_supported": named_args_supported(profile),
            "async_udfs_enabled": profile.features.enable_async_udfs,
            "async_udf_timeout_ms": profile.policies.async_udf_timeout_ms,
            "async_udf_batch_size": profile.policies.async_udf_batch_size,
        },
        "substrait_validation": profile.diagnostics.substrait_validation,
        "output_writes": {
            "cache_enabled": profile.features.cache_enabled,
            "cache_max_columns": profile.policies.cache_max_columns,
            "minimum_parallel_output_files": profile.execution.minimum_parallel_output_files,
            "soft_max_rows_per_output_file": profile.execution.soft_max_rows_per_output_file,
            "maximum_parallel_row_group_writers": profile.execution.maximum_parallel_row_group_writers,
            "objectstore_writer_buffer_size": profile.execution.objectstore_writer_buffer_size,
            "datafusion_write_policy": write_policy_payload,
        },
        "zero_row_bootstrap": profile.zero_row_bootstrap.fingerprint_payload(),
    }


def _runtime_settings_payload(profile: DataFusionRuntimeProfile) -> dict[str, str]:
    enable_ident_normalization = _effective_ident_normalization(profile)
    payload: dict[str, str] = {
        "datafusion.sql_parser.enable_ident_normalization": str(enable_ident_normalization).lower()
    }
    optional_values = {
        "datafusion.optimizer.repartition_aggregations": (
            profile.execution.repartition_aggregations
        ),
        "datafusion.optimizer.repartition_windows": profile.execution.repartition_windows,
        "datafusion.execution.repartition_file_scans": (profile.execution.repartition_file_scans),
        "datafusion.execution.repartition_file_min_size": (
            profile.execution.repartition_file_min_size
        ),
        "datafusion.execution.minimum_parallel_output_files": (
            profile.execution.minimum_parallel_output_files
        ),
        "datafusion.execution.soft_max_rows_per_output_file": (
            profile.execution.soft_max_rows_per_output_file
        ),
        "datafusion.execution.maximum_parallel_row_group_writers": (
            profile.execution.maximum_parallel_row_group_writers
        ),
        "datafusion.execution.objectstore_writer_buffer_size": (
            profile.execution.objectstore_writer_buffer_size
        ),
    }
    for key, value in optional_values.items():
        if value is None:
            continue
        if isinstance(value, bool):
            payload[key] = str(value).lower()
        else:
            payload[key] = str(value)
    return payload


def _telemetry_enrichment_policy_for_profile(
    _profile: DataFusionRuntimeProfile,
) -> TelemetryEnrichmentPolicy:
    return TelemetryEnrichmentPolicy()


def _enrich_query_telemetry(
    payload: Mapping[str, object],
    *,
    profile: DataFusionRuntimeProfile,
    policy: TelemetryEnrichmentPolicy,
) -> dict[str, object]:
    """Apply canonical telemetry enrichment policy to a payload.

    Returns:
    -------
    dict[str, object]
        Enriched telemetry payload.
    """
    _ = profile
    enriched = dict(payload)
    if not policy.include_profile_name:
        enriched.pop("profile_name", None)
    if not policy.include_plan_hash:
        enriched.pop("settings_hash", None)
    if not policy.include_query_text:
        enriched.pop("session_config", None)
    return enriched


def _extra_settings_payload(profile: DataFusionRuntimeProfile) -> dict[str, str]:
    payload: dict[str, str] = {}
    catalog_location, catalog_format = _effective_catalog_autoload_for_profile(profile)
    if catalog_location is not None:
        payload["datafusion.catalog.location"] = catalog_location
    if catalog_format is not None:
        payload["datafusion.catalog.format"] = catalog_format
    payload.update(_feature_gate_settings(profile))
    join_policy_settings = _join_policy_settings(profile)
    if join_policy_settings is not None:
        payload.update(join_policy_settings)
    if profile.diagnostics.explain_analyze_level is not None and _supports_explain_analyze_level():
        payload["datafusion.explain.analyze_level"] = profile.diagnostics.explain_analyze_level
    return payload


def _cache_profile_settings(profile: DataFusionRuntimeProfile) -> dict[str, str]:
    name = profile.policies.cache_profile_name
    if name is None:
        return {}
    settings = CACHE_PROFILES.get(name)
    if settings is None:
        msg = f"Unknown cache profile name: {name!r}."
        raise ValueError(msg)
    return dict(settings)


def _seconds_ttl(value: int | None) -> str | None:
    if value is None:
        return None
    return f"{int(value)}s"


# ---------------------------------------------------------------------------
# Performance Policy Functions
# ---------------------------------------------------------------------------
def _plan_statistics_capability_available(
    runtime_capabilities: Mapping[str, object] | None,
) -> bool | None:
    if runtime_capabilities is None:
        return None
    plan_caps = runtime_capabilities.get("plan_capabilities")
    if not isinstance(plan_caps, Mapping):
        return None
    has_stats = plan_caps.get("has_execution_plan_statistics")
    if isinstance(has_stats, bool):
        return has_stats
    return None


def _resolved_collect_statistics_setting(
    policy: PerformancePolicy,
    *,
    has_plan_statistics: bool | None,
) -> tuple[bool, str]:
    requested = bool(policy.statistics.collect_statistics)
    fallback = policy.statistics.fallback_when_unavailable
    if has_plan_statistics is False and requested:
        if fallback == "skip":
            return False, "fallback_skip"
        if fallback == "estimate":
            return True, "fallback_estimate"
        if fallback == "error":
            return False, "fallback_error"
    return requested, "native"


def _performance_policy_settings(
    policy: PerformancePolicy,
    *,
    collect_statistics_override: bool | None = None,
) -> dict[str, str]:
    collect_statistics = (
        collect_statistics_override
        if collect_statistics_override is not None
        else policy.statistics.collect_statistics
    )
    settings: dict[str, str] = {
        "datafusion.execution.collect_statistics": str(bool(collect_statistics)).lower(),
        "datafusion.execution.meta_fetch_concurrency": str(
            int(policy.statistics.meta_fetch_concurrency)
        ),
    }
    listing_limit = policy.cache.listing_cache_max_entries
    if listing_limit is not None:
        settings["datafusion.runtime.list_files_cache_limit"] = str(int(listing_limit))
    metadata_limit = policy.cache.metadata_cache_max_entries
    if metadata_limit is not None:
        settings["datafusion.runtime.metadata_cache_limit"] = str(int(metadata_limit))
    listing_ttl = _seconds_ttl(policy.cache.listing_cache_ttl_seconds)
    if listing_ttl is not None:
        settings["datafusion.runtime.list_files_cache_ttl"] = listing_ttl
    return settings


def performance_policy_settings(
    profile: DataFusionRuntimeProfile,
    *,
    runtime_capabilities: Mapping[str, object] | None = None,
) -> dict[str, str]:
    """Return DataFusion settings derived from the runtime performance policy."""
    policy = profile.policies.performance_policy
    if policy == _DEFAULT_PERFORMANCE_POLICY:
        return {}
    has_plan_stats = _plan_statistics_capability_available(runtime_capabilities)
    collect_statistics, _ = _resolved_collect_statistics_setting(
        policy,
        has_plan_statistics=has_plan_stats,
    )
    return _performance_policy_settings(
        policy,
        collect_statistics_override=collect_statistics,
    )


def performance_policy_applied_knobs(
    profile: DataFusionRuntimeProfile,
    *,
    runtime_capabilities: Mapping[str, object] | None = None,
) -> dict[str, object]:
    """Return applied performance-policy knobs including capability gating details."""
    policy = profile.policies.performance_policy
    has_plan_stats = _plan_statistics_capability_available(runtime_capabilities)
    collect_statistics, stats_mode = _resolved_collect_statistics_setting(
        policy,
        has_plan_statistics=has_plan_stats,
    )
    settings = (
        _performance_policy_settings(
            policy,
            collect_statistics_override=collect_statistics,
        )
        if policy != _DEFAULT_PERFORMANCE_POLICY
        else {}
    )
    knobs: dict[str, object] = {
        "enabled": policy != _DEFAULT_PERFORMANCE_POLICY,
        "statistics_mode": stats_mode,
        "statistics_fallback_when_unavailable": policy.statistics.fallback_when_unavailable,
        "statistics_capability_available": has_plan_stats,
        "cache_enable_dataframe_cache": policy.cache.enable_dataframe_cache,
        "comparison_enable_diff_gates": policy.comparison.enable_diff_gates,
        "comparison_retain_p0_artifacts": policy.comparison.retain_p0_artifacts,
        "comparison_retain_p1_artifacts": policy.comparison.retain_p1_artifacts,
        "comparison_retain_p2_artifacts": policy.comparison.retain_p2_artifacts,
    }
    knobs.update(settings)
    return knobs

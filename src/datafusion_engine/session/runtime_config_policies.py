"""DataFusion config policy presets and schema hardening settings."""

from __future__ import annotations

import logging
import os
from collections.abc import Mapping
from typing import TYPE_CHECKING

import datafusion
from datafusion import SessionConfig

from core.config_base import config_fingerprint
from datafusion_engine.session._session_constants import (
    CACHE_PROFILES,
    GIB,
    KIB,
    MIB,
    parse_major_version,
)
from serde_msgspec import StructBaseStrict
from utils.hashing import hash_sha256_hex

if TYPE_CHECKING:
    from datafusion_engine.session.planning_surface_policy import PlanningSurfacePolicyV1
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile

__all__ = [
    "CACHE_PROFILES",
    "CST_AUTOLOAD_DF_POLICY",
    "DATAFUSION_MAJOR_VERSION",
    "DATAFUSION_POLICY_PRESETS",
    "DEFAULT_DF_POLICY",
    "DEV_DF_POLICY",
    "GIB",
    "KIB",
    "MIB",
    "PROD_DF_POLICY",
    "SCHEMA_HARDENING_PRESETS",
    "SYMTABLE_DF_POLICY",
    "DataFusionConfigPolicy",
    "DataFusionFeatureGates",
    "DataFusionJoinPolicy",
    "DataFusionSettingsContract",
    "SchemaHardeningProfile",
    "cache_manager_contract_for_profile",
    "effective_datafusion_engine_major_version",
    "effective_datafusion_engine_version",
    "feature_gate_parity_hash",
    "feature_gate_parity_settings",
    "planning_surface_policy_for_profile",
    "resolved_config_policy",
    "resolved_schema_hardening",
]

# Note: Helper functions (_catalog_autoload_settings, _ansi_mode,
# _resolved_config_policy_for_profile, _resolved_schema_hardening_for_profile,
# _effective_catalog_autoload_for_profile) are intentionally private (not in __all__)

_LOG = logging.getLogger(__name__)


def _set_config_if_supported(config: SessionConfig, *, key: str, value: str) -> SessionConfig:
    if key.startswith("datafusion.runtime."):
        return config
    try:
        return config.set(key, value)
    except BaseException as exc:
        message = str(exc)
        if "Config value" in message and "not found on ConfigOptions" in message:
            _LOG.debug("Skipping unsupported DataFusion config key: %s", key)
            return config
        raise


def _catalog_autoload_settings() -> dict[str, str]:
    location = os.environ.get("CODEANATOMY_DATAFUSION_CATALOG_LOCATION", "").strip()
    file_format = os.environ.get("CODEANATOMY_DATAFUSION_CATALOG_FORMAT", "").strip()
    settings: dict[str, str] = {}
    if location:
        settings["datafusion.catalog.location"] = location
    if file_format:
        settings["datafusion.catalog.format"] = file_format
    return settings


def _ansi_mode(settings: Mapping[str, str]) -> bool | None:
    dialect = settings.get("datafusion.sql_parser.dialect")
    if dialect is None:
        return None
    return str(dialect).lower() == "ansi"


# Version constants for DataFusion
DATAFUSION_MAJOR_VERSION: int | None = parse_major_version(datafusion.__version__)


def effective_datafusion_engine_version(
    capabilities_report: Mapping[str, object] | None = None,
) -> str | None:
    """Return engine version using extension capabilities first, package version second.

    Parameters
    ----------
    capabilities_report
        Optional extension capability report payload returned by
        ``extension_capabilities_report``. When omitted, the helper probes
        the runtime extension.

    Returns:
    -------
    str | None
        Effective DataFusion engine version.
    """
    report = capabilities_report
    if report is None:
        try:
            from datafusion_engine.udf.extension_runtime import extension_capabilities_report
        except ImportError:
            report = None
        else:
            try:
                report = extension_capabilities_report()
            except (RuntimeError, TypeError, ValueError):
                report = None
    if isinstance(report, Mapping):
        snapshot = report.get("snapshot")
        if isinstance(snapshot, Mapping):
            version = snapshot.get("datafusion_version")
            if isinstance(version, str) and version:
                return version
    package_version = getattr(datafusion, "__version__", None)
    if isinstance(package_version, str) and package_version:
        return package_version
    return None


def effective_datafusion_engine_major_version(
    capabilities_report: Mapping[str, object] | None = None,
) -> int | None:
    """Return major DataFusion engine version from capability-first resolution."""
    version = effective_datafusion_engine_version(capabilities_report)
    if version is None:
        return None
    return parse_major_version(version)


def planning_surface_policy_for_profile(
    profile: DataFusionRuntimeProfile,
) -> PlanningSurfacePolicyV1:
    """Compile typed planning policy payload from runtime profile policies.

    Returns:
        PlanningSurfacePolicyV1: Typed planning policy payload.
    """
    from datafusion_engine.session.planning_surface_policy import (
        planning_surface_policy_from_bundle,
    )

    return planning_surface_policy_from_bundle(profile.policies)


def cache_manager_contract_for_profile(
    profile: DataFusionRuntimeProfile,
) -> Mapping[str, object]:
    """Compile typed cache-manager contract payload from a runtime profile.

    Returns:
        Mapping[str, object]: Cache-manager contract payload.
    """
    from datafusion_engine.session.cache_manager_contract import (
        cache_manager_contract_from_settings,
    )

    contract = cache_manager_contract_from_settings(
        enabled=profile.features.enable_cache_manager,
        settings=profile.settings_payload(),
    )
    return contract.payload()


class DataFusionConfigPolicy(StructBaseStrict, frozen=True):
    """Configuration policy for DataFusion SessionConfig."""

    settings: Mapping[str, str]

    def fingerprint_payload(self) -> Mapping[str, object]:
        """Return the fingerprint payload for config settings.

        Returns:
        -------
        Mapping[str, object]
            Payload describing the DataFusion settings.
        """
        return {"settings": dict(self.settings)}

    def fingerprint(self) -> str:
        """Return a stable fingerprint for the config settings.

        Returns:
        -------
        str
            Stable fingerprint hash.
        """
        return config_fingerprint(self.fingerprint_payload())

    def apply(self, config: SessionConfig) -> SessionConfig:
        """Return a SessionConfig with policy settings applied.

        Returns:
        -------
        datafusion.SessionConfig
            Session config with policy settings applied.
        """
        for key, value in self.settings.items():
            config = _set_config_if_supported(config, key=key, value=value)
        return config


class DataFusionFeatureGates(StructBaseStrict, frozen=True):
    """Feature gate toggles for DataFusion optimizer behavior."""

    enable_dynamic_filter_pushdown: bool = True
    enable_join_dynamic_filter_pushdown: bool = True
    enable_aggregate_dynamic_filter_pushdown: bool = True
    enable_topk_dynamic_filter_pushdown: bool = True
    enable_sort_pushdown: bool = True
    allow_symmetric_joins_without_pruning: bool = True

    def settings(self) -> dict[str, str]:
        """Return DataFusion config settings for the feature gates.

        Returns:
        -------
        dict[str, str]
            Mapping of DataFusion config keys to string values.
        """
        return {
            "datafusion.optimizer.enable_dynamic_filter_pushdown": str(
                self.enable_dynamic_filter_pushdown
            ).lower(),
            "datafusion.optimizer.enable_join_dynamic_filter_pushdown": str(
                self.enable_join_dynamic_filter_pushdown
            ).lower(),
            "datafusion.optimizer.enable_aggregate_dynamic_filter_pushdown": str(
                self.enable_aggregate_dynamic_filter_pushdown
            ).lower(),
            "datafusion.optimizer.enable_topk_dynamic_filter_pushdown": str(
                self.enable_topk_dynamic_filter_pushdown
            ).lower(),
            "datafusion.optimizer.enable_sort_pushdown": str(self.enable_sort_pushdown).lower(),
            "datafusion.optimizer.allow_symmetric_joins_without_pruning": str(
                self.allow_symmetric_joins_without_pruning
            ).lower(),
        }

    def fingerprint_payload(self) -> Mapping[str, object]:
        """Return fingerprint payload for feature gate settings.

        Returns:
        -------
        Mapping[str, object]
            Payload describing feature gate settings.
        """
        return {
            "enable_dynamic_filter_pushdown": self.enable_dynamic_filter_pushdown,
            "enable_join_dynamic_filter_pushdown": self.enable_join_dynamic_filter_pushdown,
            "enable_aggregate_dynamic_filter_pushdown": (
                self.enable_aggregate_dynamic_filter_pushdown
            ),
            "enable_topk_dynamic_filter_pushdown": self.enable_topk_dynamic_filter_pushdown,
            "enable_sort_pushdown": self.enable_sort_pushdown,
            "allow_symmetric_joins_without_pruning": (self.allow_symmetric_joins_without_pruning),
        }

    def fingerprint(self) -> str:
        """Return fingerprint for feature gate settings.

        Returns:
        -------
        str
            Deterministic fingerprint for the feature gates.
        """
        return config_fingerprint(self.fingerprint_payload())


_FEATURE_GATE_PARITY_KEYS: tuple[str, ...] = (
    "datafusion.optimizer.enable_dynamic_filter_pushdown",
    "datafusion.optimizer.enable_join_dynamic_filter_pushdown",
    "datafusion.optimizer.enable_aggregate_dynamic_filter_pushdown",
    "datafusion.optimizer.enable_topk_dynamic_filter_pushdown",
    "datafusion.optimizer.enable_sort_pushdown",
    "datafusion.optimizer.allow_symmetric_joins_without_pruning",
)


def feature_gate_parity_settings(feature_gates: DataFusionFeatureGates) -> dict[str, str]:
    """Return the cross-language parity subset for feature-gate settings."""
    settings = feature_gates.settings()
    return {key: settings[key] for key in _FEATURE_GATE_PARITY_KEYS}


def feature_gate_parity_hash(feature_gates: DataFusionFeatureGates) -> str:
    """Return deterministic parity hash for feature-gate contract checks."""
    settings = feature_gate_parity_settings(feature_gates)
    payload = "|".join(f"{key}={settings[key]}" for key in sorted(settings))
    return hash_sha256_hex(payload.encode("utf-8"))


class DataFusionJoinPolicy(StructBaseStrict, frozen=True):
    """Join algorithm preferences for DataFusion."""

    enable_hash_join: bool = True
    enable_sort_merge_join: bool = True
    enable_nested_loop_join: bool = True
    repartition_joins: bool = True
    enable_round_robin_repartition: bool = True
    perfect_hash_join_small_build_threshold: int | None = None
    perfect_hash_join_min_key_density: float | None = None

    def fingerprint_payload(self) -> Mapping[str, object]:
        """Return the fingerprint payload for join policy settings.

        Returns:
        -------
        Mapping[str, object]
            Payload describing join policy settings.
        """
        return {
            "enable_hash_join": self.enable_hash_join,
            "enable_sort_merge_join": self.enable_sort_merge_join,
            "enable_nested_loop_join": self.enable_nested_loop_join,
            "repartition_joins": self.repartition_joins,
            "enable_round_robin_repartition": self.enable_round_robin_repartition,
            "perfect_hash_join_small_build_threshold": self.perfect_hash_join_small_build_threshold,
            "perfect_hash_join_min_key_density": self.perfect_hash_join_min_key_density,
        }

    def fingerprint(self) -> str:
        """Return a stable fingerprint for join policy settings.

        Returns:
        -------
        str
            Stable fingerprint hash.
        """
        return config_fingerprint(self.fingerprint_payload())

    def settings(self) -> dict[str, str]:
        """Return DataFusion config settings for join preferences.

        Returns:
        -------
        dict[str, str]
            Mapping of DataFusion config keys to string values.
        """
        settings = {
            "datafusion.optimizer.enable_hash_join": str(self.enable_hash_join).lower(),
            "datafusion.optimizer.enable_sort_merge_join": str(self.enable_sort_merge_join).lower(),
            "datafusion.optimizer.enable_nested_loop_join": str(
                self.enable_nested_loop_join
            ).lower(),
            "datafusion.optimizer.repartition_joins": str(self.repartition_joins).lower(),
            "datafusion.optimizer.enable_round_robin_repartition": str(
                self.enable_round_robin_repartition
            ).lower(),
        }
        if self.perfect_hash_join_small_build_threshold is not None:
            settings["datafusion.execution.perfect_hash_join_small_build_threshold"] = str(
                self.perfect_hash_join_small_build_threshold
            )
        if self.perfect_hash_join_min_key_density is not None:
            settings["datafusion.execution.perfect_hash_join_min_key_density"] = str(
                self.perfect_hash_join_min_key_density
            )
        return settings


class DataFusionSettingsContract(StructBaseStrict, frozen=True):
    """Settings contract for DataFusion session configuration."""

    settings: Mapping[str, str]
    feature_gates: DataFusionFeatureGates

    def apply(self, config: SessionConfig) -> SessionConfig:
        """Return a SessionConfig with settings and feature gates applied.

        Returns:
        -------
        datafusion.SessionConfig
            Session config with settings applied.
        """
        merged = {**self.settings, **self.feature_gates.settings()}
        for key, value in merged.items():
            config = _set_config_if_supported(config, key=key, value=value)
        return config


class SchemaHardeningProfile(StructBaseStrict, frozen=True):
    """Schema-stability settings for DataFusion SessionConfig."""

    enable_view_types: bool = False
    expand_views_at_output: bool = False
    timezone: str = "UTC"
    parser_dialect: str | None = None
    show_schema_in_explain: bool = True
    explain_format: str = "tree"
    show_types_in_format: bool = True
    strict_aggregate_schema_check: bool = True

    def settings(self) -> dict[str, str]:
        """Return DataFusion settings for schema hardening.

        Returns:
        -------
        dict[str, str]
            Mapping of DataFusion config keys to string values.
        """
        settings = {
            "datafusion.explain.show_schema": str(self.show_schema_in_explain).lower(),
            "datafusion.explain.format": self.explain_format,
            "datafusion.format.types_info": str(self.show_types_in_format).lower(),
            "datafusion.execution.time_zone": str(self.timezone),
            "datafusion.execution.skip_physical_aggregate_schema_check": str(
                not self.strict_aggregate_schema_check
            ).lower(),
            "datafusion.sql_parser.map_string_types_to_utf8view": str(
                self.enable_view_types
            ).lower(),
            "datafusion.execution.parquet.schema_force_view_types": str(
                self.enable_view_types
            ).lower(),
            "datafusion.optimizer.expand_views_at_output": str(self.expand_views_at_output).lower(),
        }
        if self.parser_dialect is not None:
            settings["datafusion.sql_parser.dialect"] = self.parser_dialect
        return settings

    def fingerprint_payload(self) -> Mapping[str, object]:
        """Return canonical payload for fingerprinting.

        Returns:
        -------
        Mapping[str, object]
            Payload used for profile fingerprinting.
        """
        return {
            "enable_view_types": self.enable_view_types,
            "expand_views_at_output": self.expand_views_at_output,
            "timezone": self.timezone,
            "parser_dialect": self.parser_dialect,
            "show_schema_in_explain": self.show_schema_in_explain,
            "explain_format": self.explain_format,
            "show_types_in_format": self.show_types_in_format,
            "strict_aggregate_schema_check": self.strict_aggregate_schema_check,
        }

    def fingerprint(self) -> str:
        """Return a stable fingerprint for the profile.

        Returns:
        -------
        str
            Deterministic fingerprint string.
        """
        return config_fingerprint(self.fingerprint_payload())

    def apply(self, config: SessionConfig) -> SessionConfig:
        """Return SessionConfig with schema hardening settings applied.

        Returns:
        -------
        datafusion.SessionConfig
            Updated session config with schema hardening settings.
        """
        for key, value in self.settings().items():
            config = config.set(key, value)
        return config


# Policy preset constants
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

CST_AUTOLOAD_DF_POLICY = DataFusionConfigPolicy(
    settings={**DEFAULT_DF_POLICY.settings, **_catalog_autoload_settings()}
)

SYMTABLE_DF_POLICY = DataFusionConfigPolicy(
    settings={
        **DEFAULT_DF_POLICY.settings,
        "datafusion.execution.collect_statistics": "false",
        "datafusion.execution.meta_fetch_concurrency": "8",
        "datafusion.runtime.list_files_cache_limit": str(64 * MIB),
        "datafusion.runtime.list_files_cache_ttl": "1m",
        "datafusion.execution.listing_table_factory_infer_partitions": "false",
        "datafusion.explain.show_schema": "true",
        "datafusion.format.types_info": "true",
        "datafusion.execution.time_zone": "UTC",
        "datafusion.sql_parser.map_string_types_to_utf8view": "false",
        "datafusion.execution.parquet.schema_force_view_types": "false",
        "datafusion.optimizer.expand_views_at_output": "false",
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

# Preset registries
DATAFUSION_POLICY_PRESETS: Mapping[str, DataFusionConfigPolicy] = {
    "cst_autoload": CST_AUTOLOAD_DF_POLICY,
    "dev": DEV_DF_POLICY,
    "default": DEFAULT_DF_POLICY,
    "prod": PROD_DF_POLICY,
    "symtable": SYMTABLE_DF_POLICY,
}

SCHEMA_HARDENING_PRESETS: Mapping[str, SchemaHardeningProfile] = {
    "schema_hardening": SchemaHardeningProfile(),
    "arrow_performance": SchemaHardeningProfile(enable_view_types=True),
}


def _resolved_config_policy_for_profile(
    profile: DataFusionRuntimeProfile,
) -> DataFusionConfigPolicy | None:
    if profile.policies.config_policy is not None:
        return profile.policies.config_policy
    if profile.policies.config_policy_name is None:
        return DEFAULT_DF_POLICY
    return DATAFUSION_POLICY_PRESETS.get(
        profile.policies.config_policy_name,
        DEFAULT_DF_POLICY,
    )


def _resolved_schema_hardening_for_profile(
    profile: DataFusionRuntimeProfile,
) -> SchemaHardeningProfile | None:
    if profile.policies.schema_hardening is not None:
        return profile.policies.schema_hardening
    if profile.policies.schema_hardening_name is None:
        return None
    return SCHEMA_HARDENING_PRESETS.get(
        profile.policies.schema_hardening_name,
        SCHEMA_HARDENING_PRESETS["schema_hardening"],
    )


def resolved_config_policy(profile: DataFusionRuntimeProfile) -> DataFusionConfigPolicy | None:
    """Return resolved config policy for a runtime profile."""
    return _resolved_config_policy_for_profile(profile)


def resolved_schema_hardening(profile: DataFusionRuntimeProfile) -> SchemaHardeningProfile | None:
    """Return resolved schema-hardening profile for a runtime profile."""
    return _resolved_schema_hardening_for_profile(profile)


def _effective_catalog_autoload_for_profile(
    profile: DataFusionRuntimeProfile,
) -> tuple[str | None, str | None]:
    if not profile.catalog.catalog_auto_load_enabled:
        return (None, None)
    if (
        profile.catalog.catalog_auto_load_location is not None
        or profile.catalog.catalog_auto_load_format is not None
    ):
        return (
            profile.catalog.catalog_auto_load_location,
            profile.catalog.catalog_auto_load_format,
        )
    env_settings = _catalog_autoload_settings()
    return (
        env_settings.get("datafusion.catalog.location"),
        env_settings.get("datafusion.catalog.format"),
    )

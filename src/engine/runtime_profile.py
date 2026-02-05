"""Runtime profile presets and helpers."""

from __future__ import annotations

import os
import time
from collections.abc import Mapping
from typing import TYPE_CHECKING, cast

import msgspec
import pyarrow as pa
from pydantic import BaseModel, ConfigDict, TypeAdapter

from core_types import DeterminismTier
from datafusion_engine.arrow.schema import version_field
from datafusion_engine.session.runtime import DataFusionRuntimeProfile, PolicyBundleConfig
from engine.telemetry.hamilton import HamiltonTelemetryProfile, HamiltonTrackerConfig
from serde_artifacts import RuntimeProfileSnapshot
from serde_msgspec import StructBaseStrict, coalesce_unset, dumps_msgpack, to_builtins
from storage.ipc_utils import payload_hash

if TYPE_CHECKING:
    from datafusion_engine.lineage.diagnostics import DiagnosticsSink
    from datafusion_engine.udf.runtime import RustUdfSnapshot


PROFILE_HASH_VERSION: int = 3
_PROFILE_HASH_SCHEMA = pa.schema(
    [
        version_field(),
        pa.field("profile_name", pa.string(), nullable=False),
        pa.field("determinism_tier", pa.string(), nullable=False),
        pa.field("telemetry_hash", pa.string(), nullable=False),
    ]
)


_ENV_TRUE_VALUES = frozenset({"1", "true", "yes", "y"})
_ENV_FALSE_VALUES = frozenset({"0", "false", "no", "n"})


class RuntimeProfileSpec(StructBaseStrict, frozen=True):
    """Resolved runtime profile and determinism tier."""

    name: str
    datafusion: DataFusionRuntimeProfile
    determinism_tier: DeterminismTier = DeterminismTier.BEST_EFFORT
    tracker_config: HamiltonTrackerConfig | None = None
    hamilton_telemetry: HamiltonTelemetryProfile | None = None

    @property
    def datafusion_settings_hash(self) -> str:
        """Return DataFusion settings hash when configured."""
        return self.datafusion.settings_hash()

    def runtime_profile_snapshot(self) -> RuntimeProfileSnapshot:
        """Return a unified runtime profile snapshot.

        Returns
        -------
        RuntimeProfileSnapshot
            Snapshot combining deterministic profile metadata.
        """
        return runtime_profile_snapshot(
            self.datafusion,
            name=self.name,
            determinism_tier=self.determinism_tier,
        )

    @property
    def runtime_profile_hash(self) -> str:
        """Return a stable hash of the unified runtime profile.

        Returns
        -------
        str
            Hash for the runtime profile snapshot.
        """
        return self.runtime_profile_snapshot().profile_hash


class RuntimeProfileEnvPatch(StructBaseStrict, frozen=True):
    """Patch payload for runtime profile environment overrides."""

    config_policy_name: str | msgspec.UnsetType | None = msgspec.UNSET
    catalog_auto_load_location: str | msgspec.UnsetType | None = msgspec.UNSET
    catalog_auto_load_format: str | msgspec.UnsetType | None = msgspec.UNSET
    cache_output_root: str | msgspec.UnsetType | None = msgspec.UNSET
    runtime_artifact_cache_root: str | msgspec.UnsetType | None = msgspec.UNSET
    runtime_artifact_cache_enabled: bool | msgspec.UnsetType = msgspec.UNSET
    metadata_cache_snapshot_enabled: bool | msgspec.UnsetType = msgspec.UNSET
    diagnostics_sink: object | msgspec.UnsetType | None = msgspec.UNSET


class _RuntimeProfileEnvPatchRuntime(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        validate_default=True,
        frozen=True,
        arbitrary_types_allowed=True,
        revalidate_instances="always",
    )

    config_policy_name: str | msgspec.UnsetType | None = msgspec.UNSET
    catalog_auto_load_location: str | msgspec.UnsetType | None = msgspec.UNSET
    catalog_auto_load_format: str | msgspec.UnsetType | None = msgspec.UNSET
    cache_output_root: str | msgspec.UnsetType | None = msgspec.UNSET
    runtime_artifact_cache_root: str | msgspec.UnsetType | None = msgspec.UNSET
    runtime_artifact_cache_enabled: bool | msgspec.UnsetType = msgspec.UNSET
    metadata_cache_snapshot_enabled: bool | msgspec.UnsetType = msgspec.UNSET
    diagnostics_sink: object | msgspec.UnsetType | None = msgspec.UNSET


_RUNTIME_PROFILE_ENV_ADAPTER = TypeAdapter(_RuntimeProfileEnvPatchRuntime)


def _cpu_count() -> int:
    count = os.cpu_count()
    return count if count is not None and count > 0 else 1


def _settings_int(value: str | None) -> int | None:
    if not value:
        return None
    try:
        return int(value)
    except ValueError:
        return None


def _env_patch_text(name: str) -> str | msgspec.UnsetType | None:
    raw = os.environ.get(name)
    if raw is None:
        return msgspec.UNSET
    value = raw.strip()
    if not value:
        return msgspec.UNSET
    lowered = value.lower()
    if lowered in {"none", "null"}:
        return None
    return value


def _env_patch_bool(name: str) -> bool | msgspec.UnsetType:
    raw = os.environ.get(name)
    if raw is None:
        return msgspec.UNSET
    value = raw.strip().lower()
    if not value or value in {"none", "null"}:
        return msgspec.UNSET
    if value in _ENV_TRUE_VALUES:
        return True
    if value in _ENV_FALSE_VALUES:
        return False
    return msgspec.UNSET


def _env_patch_diagnostics_sink(name: str) -> object | msgspec.UnsetType | None:
    raw = os.environ.get(name)
    if raw is None:
        return msgspec.UNSET
    value = raw.strip()
    if not value:
        return msgspec.UNSET
    return _diagnostics_sink_from_value(value)


def _runtime_profile_env_patch() -> RuntimeProfileEnvPatch:
    payload = {
        "config_policy_name": _env_patch_text("CODEANATOMY_DATAFUSION_POLICY"),
        "catalog_auto_load_location": _env_patch_text("CODEANATOMY_DATAFUSION_CATALOG_LOCATION"),
        "catalog_auto_load_format": _env_patch_text("CODEANATOMY_DATAFUSION_CATALOG_FORMAT"),
        "cache_output_root": _env_patch_text("CODEANATOMY_CACHE_OUTPUT_ROOT"),
        "runtime_artifact_cache_root": _env_patch_text("CODEANATOMY_RUNTIME_ARTIFACT_CACHE_ROOT"),
        "runtime_artifact_cache_enabled": _env_patch_bool(
            "CODEANATOMY_RUNTIME_ARTIFACT_CACHE_ENABLED",
        ),
        "metadata_cache_snapshot_enabled": _env_patch_bool(
            "CODEANATOMY_METADATA_CACHE_SNAPSHOT_ENABLED",
        ),
        "diagnostics_sink": _env_patch_diagnostics_sink("CODEANATOMY_DIAGNOSTICS_SINK"),
    }
    validated = _RUNTIME_PROFILE_ENV_ADAPTER.validate_strings(payload)
    return RuntimeProfileEnvPatch(**validated.model_dump())


def _coalesce_diagnostics_sink(
    value: object | msgspec.UnsetType | None,
    default: DiagnosticsSink | None,
) -> DiagnosticsSink | None:
    if value is msgspec.UNSET:
        return default
    return cast("DiagnosticsSink | None", value)


def _coalesce_unset_text(
    value: str | msgspec.UnsetType | None,
    default: str | None,
) -> str | None:
    if value is msgspec.UNSET:
        return default
    return value


def _profile_hash_payload(
    *,
    name: str,
    determinism: DeterminismTier,
    telemetry_hash: str,
) -> dict[str, object]:
    return {
        "version": PROFILE_HASH_VERSION,
        "profile_name": name,
        "determinism_tier": determinism.value,
        "telemetry_hash": telemetry_hash,
    }


def _runtime_profile_hash(
    *,
    name: str,
    determinism: DeterminismTier,
    telemetry_hash: str,
) -> str:
    payload = _profile_hash_payload(
        name=name,
        determinism=determinism,
        telemetry_hash=telemetry_hash,
    )
    return payload_hash(payload, _PROFILE_HASH_SCHEMA)


def runtime_profile_snapshot(
    profile: DataFusionRuntimeProfile,
    *,
    name: str | None = None,
    determinism_tier: DeterminismTier = DeterminismTier.BEST_EFFORT,
) -> RuntimeProfileSnapshot:
    """Return a runtime profile snapshot for diagnostics and metadata.

    Returns
    -------
    RuntimeProfileSnapshot
        Snapshot describing DataFusion runtime settings.
    """
    profile_name = name or profile.policies.config_policy_name or "default"
    telemetry_payload = profile.telemetry_payload_v1()
    telemetry_hash = profile.telemetry_payload_hash()
    profile_hash = _runtime_profile_hash(
        name=profile_name,
        determinism=determinism_tier,
        telemetry_hash=telemetry_hash,
    )
    return RuntimeProfileSnapshot(
        version=PROFILE_HASH_VERSION,
        name=profile_name,
        determinism_tier=determinism_tier.value,
        datafusion_settings_hash=profile.settings_hash(),
        datafusion_settings=profile.settings_payload(),
        telemetry_payload=telemetry_payload,
        profile_hash=profile_hash,
    )


def engine_runtime_artifact(
    profile: DataFusionRuntimeProfile,
    *,
    name: str | None = None,
    determinism_tier: DeterminismTier = DeterminismTier.BEST_EFFORT,
) -> dict[str, object]:
    """Return an engine runtime artifact payload for diagnostics.

    Returns
    -------
    dict[str, object]
        Diagnostics payload for engine runtime settings.
    """
    snapshot = runtime_profile_snapshot(
        profile,
        name=name,
        determinism_tier=determinism_tier,
    )
    registry_snapshot: RustUdfSnapshot | None = None
    if profile.catalog.enable_information_schema:
        try:
            session = profile.session_runtime().ctx
        except (RuntimeError, TypeError, ValueError):
            session = None
        if session is not None:
            from datafusion_engine.udf.runtime import rust_udf_snapshot

            registry_snapshot = rust_udf_snapshot(session)
    registry_hash = None
    if registry_snapshot is not None:
        from datafusion_engine.udf.runtime import rust_udf_snapshot_hash

        registry_hash = rust_udf_snapshot_hash(registry_snapshot)
    datafusion_settings = profile.settings_payload()
    return {
        "event_time_unix_ms": int(time.time() * 1000),
        "runtime_profile_name": snapshot.name,
        "determinism_tier": snapshot.determinism_tier,
        "runtime_profile_hash": snapshot.profile_hash,
        "runtime_profile_snapshot": dumps_msgpack(snapshot.payload()),
        "function_registry_hash": registry_hash,
        "datafusion_settings_hash": snapshot.datafusion_settings_hash,
        "datafusion_settings": dumps_msgpack(datafusion_settings),
    }


def _apply_named_profile_overrides(
    name: str,
    profile: DataFusionRuntimeProfile,
) -> DataFusionRuntimeProfile:
    cpu_count = _cpu_count()
    if name == "dev_debug":
        return msgspec.structs.replace(
            profile,
            policies=msgspec.structs.replace(profile.policies, config_policy_name="dev"),
            execution=msgspec.structs.replace(
                profile.execution,
                target_partitions=min(cpu_count, 8),
                batch_size=4096,
            ),
            diagnostics=msgspec.structs.replace(
                profile.diagnostics,
                capture_explain=True,
                explain_verbose=True,
                explain_analyze=True,
                explain_analyze_level="dev",
            ),
        )
    if name == "prod_fast":
        return msgspec.structs.replace(
            profile,
            policies=msgspec.structs.replace(profile.policies, config_policy_name="prod"),
            diagnostics=msgspec.structs.replace(
                profile.diagnostics,
                capture_explain=False,
                explain_verbose=False,
                explain_analyze=False,
                explain_analyze_level=None,
            ),
        )
    if name == "memory_tight":
        return msgspec.structs.replace(
            profile,
            policies=msgspec.structs.replace(profile.policies, config_policy_name="symtable"),
            execution=msgspec.structs.replace(
                profile.execution,
                target_partitions=min(cpu_count, 4),
                batch_size=4096,
            ),
            diagnostics=msgspec.structs.replace(
                profile.diagnostics,
                capture_explain=False,
                explain_verbose=False,
                explain_analyze=False,
                explain_analyze_level="summary",
            ),
        )
    return profile


def _apply_memory_overrides(
    name: str,
    profile: DataFusionRuntimeProfile,
    settings: Mapping[str, str],
) -> DataFusionRuntimeProfile:
    if name == "dev_debug":
        return profile
    spill_dir = profile.execution.spill_dir or settings.get("datafusion.runtime.temp_directory")
    memory_limit = profile.execution.memory_limit_bytes or _settings_int(
        settings.get("datafusion.runtime.memory_limit")
    )
    memory_pool = profile.execution.memory_pool
    if memory_limit is not None and memory_pool == "greedy":
        memory_pool = "fair"
    return msgspec.structs.replace(
        profile,
        execution=msgspec.structs.replace(
            profile.execution,
            spill_dir=spill_dir,
            memory_limit_bytes=memory_limit,
            memory_pool=memory_pool,
        ),
    )


def _apply_env_overrides(profile: DataFusionRuntimeProfile) -> DataFusionRuntimeProfile:
    patch = _runtime_profile_env_patch()
    return msgspec.structs.replace(
        profile,
        policies=msgspec.structs.replace(
            profile.policies,
            config_policy_name=_coalesce_unset_text(
                patch.config_policy_name,
                profile.policies.config_policy_name,
            ),
            cache_output_root=_coalesce_unset_text(
                patch.cache_output_root,
                profile.policies.cache_output_root,
            ),
            runtime_artifact_cache_root=_coalesce_unset_text(
                patch.runtime_artifact_cache_root,
                profile.policies.runtime_artifact_cache_root,
            ),
            runtime_artifact_cache_enabled=coalesce_unset(
                patch.runtime_artifact_cache_enabled,
                profile.policies.runtime_artifact_cache_enabled,
            ),
            metadata_cache_snapshot_enabled=coalesce_unset(
                patch.metadata_cache_snapshot_enabled,
                profile.policies.metadata_cache_snapshot_enabled,
            ),
        ),
        catalog=msgspec.structs.replace(
            profile.catalog,
            catalog_auto_load_location=_coalesce_unset_text(
                patch.catalog_auto_load_location,
                profile.catalog.catalog_auto_load_location,
            ),
            catalog_auto_load_format=_coalesce_unset_text(
                patch.catalog_auto_load_format,
                profile.catalog.catalog_auto_load_format,
            ),
        ),
        diagnostics=msgspec.structs.replace(
            profile.diagnostics,
            diagnostics_sink=_coalesce_diagnostics_sink(
                patch.diagnostics_sink,
                profile.diagnostics.diagnostics_sink,
            ),
        ),
    )


def _diagnostics_sink_from_value(value: str) -> DiagnosticsSink | None:
    normalized = value.strip().lower()
    if not normalized or normalized in {"none", "off", "disabled"}:
        return None
    if normalized in {"memory", "in_memory", "in-memory", "test", "testing"}:
        from datafusion_engine.lineage.diagnostics import InMemoryDiagnosticsSink

        return InMemoryDiagnosticsSink()
    if normalized in {"otel", "otlp", "opentelemetry"}:
        from obs.otel.logs import OtelDiagnosticsSink

        return OtelDiagnosticsSink()
    msg = f"Unsupported diagnostics sink: {value!r}."
    raise ValueError(msg)


def resolve_runtime_profile(
    profile: str,
    *,
    determinism: DeterminismTier | None = None,
) -> RuntimeProfileSpec:
    """Return a runtime profile spec for the requested profile name.

    Returns
    -------
    RuntimeProfileSpec
        Resolved runtime profile spec.
    """
    df_profile = DataFusionRuntimeProfile(
        policies=PolicyBundleConfig(config_policy_name=profile),
    )
    df_profile = _apply_named_profile_overrides(profile, df_profile)
    df_profile = _apply_memory_overrides(profile, df_profile, df_profile.settings_payload())
    df_profile = _apply_env_overrides(df_profile)
    tracker_config = HamiltonTrackerConfig.from_env()
    telemetry_profile = HamiltonTelemetryProfile.resolve()
    return RuntimeProfileSpec(
        name=profile,
        datafusion=df_profile,
        determinism_tier=determinism or DeterminismTier.BEST_EFFORT,
        tracker_config=tracker_config,
        hamilton_telemetry=telemetry_profile,
    )


def runtime_profile_snapshot_payload(profile: DataFusionRuntimeProfile) -> dict[str, object]:
    """Return a snapshot payload for profile diagnostics.

    Returns
    -------
    dict[str, object]
        Builtins-only payload suitable for diagnostics.
    """
    try:
        payload = to_builtins(profile.telemetry_payload_v1())
    except (msgspec.EncodeError, TypeError):
        return {"profile_name": profile.policies.config_policy_name}
    if isinstance(payload, dict):
        return cast("dict[str, object]", payload)
    return {"profile_name": profile.policies.config_policy_name}


__all__ = [
    "HamiltonTelemetryProfile",
    "HamiltonTrackerConfig",
    "RuntimeProfileSnapshot",
    "RuntimeProfileSpec",
    "engine_runtime_artifact",
    "resolve_runtime_profile",
    "runtime_profile_snapshot",
]

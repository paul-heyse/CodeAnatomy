"""Runtime profile presets and helpers."""

from __future__ import annotations

import os
import time
from collections.abc import Mapping
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, cast

import msgspec
import pyarrow as pa

from core_types import DeterminismTier
from datafusion_engine.arrow.schema import version_field
from datafusion_engine.session.runtime import DataFusionRuntimeProfile
from engine.telemetry.hamilton import HamiltonTelemetryProfile, HamiltonTrackerConfig
from serde_artifacts import RuntimeProfileSnapshot
from serde_msgspec import dumps_msgpack, to_builtins
from storage.ipc_utils import payload_hash
from utils.env_utils import env_bool, env_value

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


@dataclass(frozen=True)
class RuntimeProfileSpec:
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
    profile_name = name or profile.config_policy_name or "default"
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
    if profile.enable_information_schema:
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
        return replace(
            profile,
            config_policy_name="dev",
            target_partitions=min(cpu_count, 8),
            batch_size=4096,
            capture_explain=True,
            explain_verbose=True,
            explain_analyze=True,
            explain_analyze_level="dev",
        )
    if name == "prod_fast":
        return replace(
            profile,
            config_policy_name="prod",
            capture_explain=False,
            explain_verbose=False,
            explain_analyze=False,
            explain_analyze_level=None,
        )
    if name == "memory_tight":
        return replace(
            profile,
            config_policy_name="symtable",
            target_partitions=min(cpu_count, 4),
            batch_size=4096,
            capture_explain=False,
            explain_verbose=False,
            explain_analyze=False,
            explain_analyze_level="summary",
        )
    return profile


def _apply_memory_overrides(
    name: str,
    profile: DataFusionRuntimeProfile,
    settings: Mapping[str, str],
) -> DataFusionRuntimeProfile:
    if name == "dev_debug":
        return profile
    spill_dir = profile.spill_dir or settings.get("datafusion.runtime.temp_directory")
    memory_limit = profile.memory_limit_bytes or _settings_int(
        settings.get("datafusion.runtime.memory_limit")
    )
    memory_pool = profile.memory_pool
    if memory_limit is not None and memory_pool == "greedy":
        memory_pool = "fair"
    return replace(
        profile,
        spill_dir=spill_dir,
        memory_limit_bytes=memory_limit,
        memory_pool=memory_pool,
    )


def _apply_env_overrides(profile: DataFusionRuntimeProfile) -> DataFusionRuntimeProfile:
    policy_override = env_value("CODEANATOMY_DATAFUSION_POLICY")
    if policy_override is not None:
        profile = replace(profile, config_policy_name=policy_override)
    catalog_location = env_value("CODEANATOMY_DATAFUSION_CATALOG_LOCATION")
    if catalog_location is not None:
        profile = replace(profile, catalog_auto_load_location=catalog_location)
    catalog_format = env_value("CODEANATOMY_DATAFUSION_CATALOG_FORMAT")
    if catalog_format is not None:
        profile = replace(profile, catalog_auto_load_format=catalog_format)
    cache_output_root = env_value("CODEANATOMY_CACHE_OUTPUT_ROOT")
    if cache_output_root is not None:
        profile = replace(profile, cache_output_root=cache_output_root)
    runtime_artifact_cache_root = env_value("CODEANATOMY_RUNTIME_ARTIFACT_CACHE_ROOT")
    if runtime_artifact_cache_root is not None:
        profile = replace(profile, runtime_artifact_cache_root=runtime_artifact_cache_root)
    runtime_artifact_cache_enabled = env_bool(
        "CODEANATOMY_RUNTIME_ARTIFACT_CACHE_ENABLED",
    )
    if runtime_artifact_cache_enabled is not None:
        profile = replace(
            profile,
            runtime_artifact_cache_enabled=runtime_artifact_cache_enabled,
        )
    metadata_cache_snapshot_enabled = env_bool(
        "CODEANATOMY_METADATA_CACHE_SNAPSHOT_ENABLED",
    )
    if metadata_cache_snapshot_enabled is not None:
        profile = replace(
            profile,
            metadata_cache_snapshot_enabled=metadata_cache_snapshot_enabled,
        )
    diagnostics_sink_value = env_value("CODEANATOMY_DIAGNOSTICS_SINK")
    if diagnostics_sink_value is not None:
        diagnostics_sink = _diagnostics_sink_from_value(diagnostics_sink_value)
        profile = replace(profile, diagnostics_sink=diagnostics_sink)
    return profile


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
    df_profile = DataFusionRuntimeProfile(config_policy_name=profile)
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
        return {"profile_name": profile.config_policy_name}
    if isinstance(payload, dict):
        return cast("dict[str, object]", payload)
    return {"profile_name": profile.config_policy_name}


__all__ = [
    "HamiltonTelemetryProfile",
    "HamiltonTrackerConfig",
    "RuntimeProfileSnapshot",
    "RuntimeProfileSpec",
    "engine_runtime_artifact",
    "resolve_runtime_profile",
    "runtime_profile_snapshot",
]

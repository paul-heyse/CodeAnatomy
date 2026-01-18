"""Runtime profile presets and helpers."""

from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Mapping
from dataclasses import dataclass, replace

from arrowdsl.core.context import (
    DeterminismTier,
    RuntimeProfile,
    ScanProfile,
    runtime_profile_factory,
)
from sqlglot_tools.optimizer import sqlglot_policy_snapshot


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


@dataclass(frozen=True)
class RuntimeProfileSpec:
    """Runtime profile plus compiler/engine preferences."""

    name: str
    runtime: RuntimeProfile
    ibis_fuse_selects: bool
    ibis_default_limit: int | None
    ibis_default_dialect: str | None
    ibis_interactive: bool | None

    @property
    def datafusion_settings_hash(self) -> str | None:
        """Return DataFusion settings hash when configured."""
        if self.runtime.datafusion is None:
            return None
        return self.runtime.datafusion.settings_hash()

    def runtime_profile_snapshot(self) -> RuntimeProfileSnapshot:
        """Return a unified runtime profile snapshot.

        Returns
        -------
        RuntimeProfileSnapshot
            Snapshot combining runtime, compiler, and engine policies.
        """
        return runtime_profile_snapshot(self.runtime)

    @property
    def runtime_profile_hash(self) -> str:
        """Return a stable hash of the unified runtime profile.

        Returns
        -------
        str
            Hash for the runtime profile snapshot.
        """
        return self.runtime_profile_snapshot().profile_hash


@dataclass(frozen=True)
class RuntimeProfileSnapshot:
    """Unified runtime profile snapshot for reproducibility."""

    version: int
    name: str
    determinism_tier: str
    scan_profile: dict[str, object]
    plan_use_threads: bool
    ibis_options: dict[str, object]
    arrow_resources: dict[str, object]
    sqlglot_policy: dict[str, object] | None
    datafusion: dict[str, object] | None
    profile_hash: str

    def payload(self) -> dict[str, object]:
        """Return the snapshot payload for serialization.

        Returns
        -------
        dict[str, object]
            Serialized payload for the runtime profile snapshot.
        """
        return {
            "version": self.version,
            "name": self.name,
            "determinism_tier": self.determinism_tier,
            "scan_profile": self.scan_profile,
            "plan_use_threads": self.plan_use_threads,
            "ibis_options": self.ibis_options,
            "arrow_resources": self.arrow_resources,
            "sqlglot_policy": self.sqlglot_policy,
            "datafusion": self.datafusion,
            "profile_hash": self.profile_hash,
        }


def runtime_profile_snapshot(runtime: RuntimeProfile) -> RuntimeProfileSnapshot:
    """Return a unified runtime profile snapshot.

    Returns
    -------
    RuntimeProfileSnapshot
        Snapshot combining runtime, compiler, and engine policies.
    """
    scan_payload = _scan_profile_payload(runtime.scan)
    arrow_payload = runtime.arrow_resource_snapshot().to_payload()
    ibis_payload = runtime.ibis_options_payload()
    sqlglot_snapshot = sqlglot_policy_snapshot()
    datafusion_payload = (
        runtime.datafusion.telemetry_payload_v1() if runtime.datafusion is not None else None
    )
    payload = {
        "name": runtime.name,
        "determinism_tier": runtime.determinism.value,
        "scan_profile": scan_payload,
        "plan_use_threads": runtime.plan_use_threads,
        "ibis_options": ibis_payload,
        "arrow_resources": arrow_payload,
        "sqlglot_policy": sqlglot_snapshot.payload() if sqlglot_snapshot is not None else None,
        "datafusion": datafusion_payload,
    }
    profile_hash = _hash_payload(payload)
    return RuntimeProfileSnapshot(
        version=1,
        name=runtime.name,
        determinism_tier=runtime.determinism.value,
        scan_profile=scan_payload,
        plan_use_threads=runtime.plan_use_threads,
        ibis_options=ibis_payload,
        arrow_resources=arrow_payload,
        sqlglot_policy=payload["sqlglot_policy"],
        datafusion=datafusion_payload,
        profile_hash=profile_hash,
    )


def _scan_profile_payload(scan: ScanProfile) -> dict[str, object]:
    """Return a scan profile payload for runtime snapshots.

    Returns
    -------
    dict[str, object]
        Serialized scan profile payload.
    """
    return {
        "name": scan.name,
        "batch_size": scan.batch_size,
        "batch_readahead": scan.batch_readahead,
        "fragment_readahead": scan.fragment_readahead,
        "fragment_scan_options": scan.fragment_scan_options,
        "cache_metadata": scan.cache_metadata,
        "use_threads": scan.use_threads,
        "require_sequenced_output": scan.require_sequenced_output,
        "implicit_ordering": scan.implicit_ordering,
        "scan_provenance_columns": list(scan.scan_provenance_columns),
    }


def _hash_payload(payload: Mapping[str, object]) -> str:
    """Return a stable hash for a runtime profile payload.

    Returns
    -------
    str
        SHA-256 hash of the payload.
    """
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _apply_profile_overrides(name: str, runtime: RuntimeProfile) -> RuntimeProfile:
    df_profile = runtime.datafusion
    if df_profile is None:
        return runtime
    settings = df_profile.settings_payload()
    if name == "dev_debug":
        runtime = replace(
            runtime,
            cpu_threads=min(_cpu_count(), 4),
            io_threads=min(_cpu_count() * 2, 8),
        )
        df_profile = replace(
            df_profile,
            config_policy_name="dev",
            target_partitions=min(_cpu_count(), 8),
            batch_size=4096,
            capture_explain=True,
            explain_analyze=True,
            explain_analyze_level="dev",
        )
    elif name == "prod_fast":
        runtime = replace(
            runtime,
            cpu_threads=_cpu_count(),
            io_threads=_cpu_count() * 2,
        )
        df_profile = replace(
            df_profile,
            config_policy_name="prod",
            capture_explain=False,
            explain_analyze=False,
            explain_analyze_level=None,
        )
    elif name == "memory_tight":
        runtime = replace(
            runtime,
            cpu_threads=min(_cpu_count(), 2),
            io_threads=min(_cpu_count(), 4),
        )
        df_profile = replace(
            df_profile,
            config_policy_name="default",
            target_partitions=min(_cpu_count(), 4),
            batch_size=4096,
            capture_explain=False,
            explain_analyze=False,
            explain_analyze_level="summary",
        )
    if name != "dev_debug":
        spill_dir = df_profile.spill_dir or settings.get("datafusion.runtime.temp_directory")
        memory_limit = df_profile.memory_limit_bytes or _settings_int(
            settings.get("datafusion.runtime.memory_limit")
        )
        memory_pool = df_profile.memory_pool
        if memory_limit is not None and memory_pool == "greedy":
            memory_pool = "fair"
        df_profile = replace(
            df_profile,
            spill_dir=spill_dir,
            memory_limit_bytes=memory_limit,
            memory_pool=memory_pool,
        )
    return runtime.with_datafusion(df_profile)


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
    runtime = runtime_profile_factory(profile)
    if determinism is not None:
        runtime = runtime.with_determinism(determinism)
    runtime = _apply_profile_overrides(profile, runtime)
    return RuntimeProfileSpec(
        name=profile,
        runtime=runtime,
        ibis_fuse_selects=runtime.ibis_fuse_selects,
        ibis_default_limit=runtime.ibis_default_limit,
        ibis_default_dialect=runtime.ibis_default_dialect,
        ibis_interactive=runtime.ibis_interactive,
    )


__all__ = [
    "RuntimeProfileSnapshot",
    "RuntimeProfileSpec",
    "resolve_runtime_profile",
    "runtime_profile_snapshot",
]

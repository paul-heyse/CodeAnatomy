"""Runtime profile helpers for execution configuration."""

from __future__ import annotations

import importlib
from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING, cast

import pyarrow as pa

from arrowdsl.core.determinism import DeterminismTier
from arrowdsl.core.scan_profiles import ScanProfile, normalize_profile_name, scan_profile_factory

if TYPE_CHECKING:
    from datafusion_engine.runtime import DataFusionRuntimeProfile


def _default_datafusion_profile() -> DataFusionRuntimeProfile:
    """Return the default DataFusion runtime profile.

    Returns
    -------
    DataFusionRuntimeProfile
        Default runtime profile instance.
    """
    module = importlib.import_module("datafusion_engine.runtime")
    profile_cls = cast("type[DataFusionRuntimeProfile]", module.DataFusionRuntimeProfile)
    return profile_cls()


@dataclass(frozen=True)
class ArrowResourceSnapshot:
    """Snapshot of Arrow thread pools and memory pool metrics."""

    pyarrow_version: str
    cpu_threads: int
    io_threads: int
    memory_pool: str | None
    bytes_allocated: int | None
    max_memory: int | None

    def to_payload(self) -> dict[str, object]:
        """Return a diagnostics payload for resource snapshots.

        Returns
        -------
        dict[str, object]
            Arrow resource telemetry payload.
        """
        return {
            "pyarrow_version": self.pyarrow_version,
            "cpu_threads": self.cpu_threads,
            "io_threads": self.io_threads,
            "memory_pool": self.memory_pool,
            "bytes_allocated": self.bytes_allocated,
            "max_memory": self.max_memory,
        }


@dataclass(frozen=True)
class RuntimeProfile:
    """Global runtime threading + scan policy + determinism defaults."""

    name: str

    cpu_threads: int | None = None
    io_threads: int | None = None

    scan: ScanProfile = field(default_factory=lambda: ScanProfile(name="DEFAULT"))
    plan_use_threads: bool = True
    ibis_fuse_selects: bool = True
    ibis_default_limit: int | None = None
    ibis_default_dialect: str | None = None
    ibis_interactive: bool | None = None

    determinism: DeterminismTier = DeterminismTier.BEST_EFFORT
    datafusion: DataFusionRuntimeProfile | None = field(
        default_factory=_default_datafusion_profile
    )

    def apply_global_thread_pools(self) -> None:
        """Set Arrow CPU + IO thread pools."""
        if self.cpu_threads is not None:
            pa.set_cpu_count(int(self.cpu_threads))
        if self.io_threads is not None:
            pa.set_io_thread_count(int(self.io_threads))

    @staticmethod
    def arrow_resource_snapshot() -> ArrowResourceSnapshot:
        """Return a snapshot of Arrow runtime resources.

        Returns
        -------
        ArrowResourceSnapshot
            Snapshot of Arrow resource configuration and memory state.
        """
        pool = pa.default_memory_pool()
        backend_name = pool.backend_name if hasattr(pool, "backend_name") else None
        return ArrowResourceSnapshot(
            pyarrow_version=str(pa.__version__),
            cpu_threads=int(pa.cpu_count()),
            io_threads=int(pa.io_thread_count()),
            memory_pool=str(backend_name) if backend_name is not None else None,
            bytes_allocated=pool.bytes_allocated(),
            max_memory=pool.max_memory(),
        )

    def ibis_options_payload(self) -> dict[str, object]:
        """Return Ibis options payload for diagnostics.

        Returns
        -------
        dict[str, object]
            Ibis options payload.
        """
        return {
            "fuse_selects": self.ibis_fuse_selects,
            "default_limit": self.ibis_default_limit,
            "default_dialect": self.ibis_default_dialect,
            "interactive": self.ibis_interactive,
        }

    def with_determinism(self, tier: DeterminismTier) -> RuntimeProfile:
        """Return a copy with the specified determinism tier.

        Parameters
        ----------
        tier:
            Determinism tier to apply.

        Returns
        -------
        RuntimeProfile
            Updated profile.
        """
        scan = self.scan
        if tier in {DeterminismTier.CANONICAL, DeterminismTier.STABLE_SET} and (
            not scan.implicit_ordering or not scan.require_sequenced_output
        ):
            scan = replace(
                scan,
                require_sequenced_output=True,
                implicit_ordering=True,
            )
        return replace(
            self,
            scan=scan,
            determinism=tier,
        )

    def with_datafusion(
        self,
        profile: DataFusionRuntimeProfile | None,
    ) -> RuntimeProfile:
        """Return a copy with the DataFusion runtime profile attached.

        Parameters
        ----------
        profile:
            DataFusion runtime profile to attach.

        Returns
        -------
        RuntimeProfile
            Updated runtime profile.
        """
        return replace(self, datafusion=profile)


def runtime_profile_factory(profile: str) -> RuntimeProfile:
    """Return a RuntimeProfile for the named execution profile.

    Returns
    -------
    RuntimeProfile
        Runtime profile matching the named profile.
    """
    profile_key = normalize_profile_name(profile)
    scan = scan_profile_factory(profile_key)
    plan_use_threads = profile_key not in {"deterministic", "dev_debug"}
    if profile_key in {"deterministic", "dev_debug"}:
        determinism = DeterminismTier.CANONICAL
    elif profile_key == "memory_tight":
        determinism = DeterminismTier.STABLE_SET
    else:
        determinism = DeterminismTier.BEST_EFFORT
    name = scan.name
    runtime = RuntimeProfile(
        name=name,
        scan=scan,
        plan_use_threads=plan_use_threads,
        ibis_fuse_selects=profile_key != "dev_debug",
        ibis_interactive=profile_key == "dev_debug",
        determinism=determinism,
    )
    if determinism in {DeterminismTier.CANONICAL, DeterminismTier.STABLE_SET}:
        return runtime.with_determinism(determinism)
    return runtime


__all__ = ["ArrowResourceSnapshot", "RuntimeProfile", "runtime_profile_factory"]

"""Runtime profile helpers for execution configuration."""

from __future__ import annotations

import importlib
from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING, Literal, cast

import pyarrow as pa

from arrowdsl.core.determinism import DeterminismTier

if TYPE_CHECKING:
    from datafusion_engine.runtime import DataFusionRuntimeProfile


type ExecutionProfileName = Literal[
    "bulk",
    "default",
    "deterministic",
    "streaming",
    "dev_debug",
    "memory_tight",
    "prod_fast",
]


@dataclass(frozen=True)
class ScanProfile:
    """Dataset scan policy for Arrow scanners and Acero scans."""

    name: str
    batch_size: int | None = None
    batch_readahead: int | None = None
    fragment_readahead: int | None = None
    fragment_scan_options: object | None = None
    cache_metadata: bool = True
    use_threads: bool = True

    require_sequenced_output: bool = False
    implicit_ordering: bool = False
    scan_provenance_columns: tuple[str, ...] = ()

    def scanner_kwargs(self) -> dict[str, object]:
        """Return kwargs for ``ds.Scanner.from_dataset``.

        Returns
        -------
        dict[str, object]
            Scanner keyword arguments without columns or filters.
        """
        kw: dict[str, object] = {"use_threads": self.use_threads}
        if self.batch_size is not None:
            kw["batch_size"] = self.batch_size
        if self.batch_readahead is not None:
            kw["batch_readahead"] = self.batch_readahead
        if self.fragment_readahead is not None:
            kw["fragment_readahead"] = self.fragment_readahead
        if self.fragment_scan_options is not None:
            kw["fragment_scan_options"] = self.fragment_scan_options
        if self.cache_metadata:
            kw["cache_metadata"] = True
        return kw

    def scan_node_kwargs(self) -> dict[str, object]:
        """Return kwargs for ``acero.ScanNodeOptions``.

        Returns
        -------
        dict[str, object]
            Scan node keyword arguments without dataset or filters.
        """
        kw: dict[str, object] = {}
        if self.require_sequenced_output:
            kw["require_sequenced_output"] = True
        if self.implicit_ordering:
            kw["implicit_ordering"] = True
        return kw


def _normalize_profile(profile: str) -> ExecutionProfileName:
    """Normalize an execution profile name to the canonical token.

    Parameters
    ----------
    profile
        User-provided profile name.

    Returns
    -------
    ExecutionProfileName
        Canonical profile token.

    Raises
    ------
    ValueError
        Raised when the profile name is unknown.
    """
    key = profile.strip().lower()
    mapping: dict[str, ExecutionProfileName] = {
        "default": "default",
        "streaming": "streaming",
        "bulk": "bulk",
        "deterministic": "deterministic",
        "dev_debug": "dev_debug",
        "prod_fast": "prod_fast",
        "memory_tight": "memory_tight",
    }
    resolved = mapping.get(key)
    if resolved is None:
        msg = f"Unknown execution profile: {profile!r}."
        raise ValueError(msg)
    return resolved


def normalize_profile_name(profile: str) -> ExecutionProfileName:
    """Return the canonical profile name token.

    Returns
    -------
    ExecutionProfileName
        Canonical profile token.
    """
    return _normalize_profile(profile)


def scan_profile_factory(profile: str) -> ScanProfile:
    """Return a ScanProfile for the named execution profile.

    Returns
    -------
    ScanProfile
        Scan profile matching the named profile.
    """
    profile_key = _normalize_profile(profile)
    profiles: dict[ExecutionProfileName, ScanProfile] = {
        "streaming": ScanProfile(
            name="STREAM",
            batch_size=4096,
            batch_readahead=1,
            fragment_readahead=1,
            use_threads=True,
        ),
        "bulk": ScanProfile(
            name="BULK",
            batch_size=16384,
            batch_readahead=4,
            fragment_readahead=2,
            use_threads=True,
        ),
        "deterministic": ScanProfile(
            name="DETERMINISTIC",
            batch_size=4096,
            batch_readahead=1,
            fragment_readahead=1,
            use_threads=False,
            require_sequenced_output=True,
            implicit_ordering=True,
        ),
        "dev_debug": ScanProfile(
            name="DEV_DEBUG",
            batch_size=4096,
            batch_readahead=1,
            fragment_readahead=1,
            use_threads=False,
            require_sequenced_output=True,
            implicit_ordering=True,
        ),
        "prod_fast": ScanProfile(
            name="PROD_FAST",
            batch_size=16384,
            batch_readahead=4,
            fragment_readahead=2,
            use_threads=True,
        ),
        "memory_tight": ScanProfile(
            name="MEMORY_TIGHT",
            batch_size=4096,
            batch_readahead=1,
            fragment_readahead=1,
            use_threads=True,
        ),
        "default": ScanProfile(name="DEFAULT"),
    }
    return profiles[profile_key]


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

    determinism: DeterminismTier = DeterminismTier.BEST_EFFORT
    datafusion: DataFusionRuntimeProfile | None = field(default_factory=_default_datafusion_profile)

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
        updated = replace(self, datafusion=profile)
        return _align_datafusion_profile(updated)


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
        determinism=determinism,
    )
    if determinism in {DeterminismTier.CANONICAL, DeterminismTier.STABLE_SET}:
        runtime = runtime.with_determinism(determinism)
    return _align_datafusion_profile(runtime)


_DATAFUSION_POLICY_BY_PROFILE: dict[str, str] = {
    "DEV_DEBUG": "dev",
    "PROD_FAST": "prod",
    "MEMORY_TIGHT": "dev",
}


def _align_datafusion_profile(runtime: RuntimeProfile) -> RuntimeProfile:
    profile = runtime.datafusion
    if profile is None:
        return runtime
    batch_size = profile.batch_size
    if batch_size is None:
        batch_size = runtime.scan.batch_size
    target_partitions = profile.target_partitions
    if target_partitions is None and runtime.cpu_threads is not None:
        target_partitions = runtime.cpu_threads
    config_policy_name = profile.config_policy_name
    if config_policy_name in {None, "symtable"}:
        suggested = _DATAFUSION_POLICY_BY_PROFILE.get(runtime.name)
        if suggested is not None:
            config_policy_name = suggested
    if (
        batch_size == profile.batch_size
        and target_partitions == profile.target_partitions
        and config_policy_name == profile.config_policy_name
    ):
        return runtime
    updated_profile = replace(
        profile,
        batch_size=batch_size,
        target_partitions=target_partitions,
        config_policy_name=config_policy_name,
    )
    return replace(runtime, datafusion=updated_profile)


__all__ = [
    "ArrowResourceSnapshot",
    "ExecutionProfileName",
    "RuntimeProfile",
    "ScanProfile",
    "normalize_profile_name",
    "runtime_profile_factory",
    "scan_profile_factory",
]

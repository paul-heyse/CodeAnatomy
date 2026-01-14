"""Execution context, runtime profiles, and scan helpers."""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from enum import StrEnum
from typing import Literal

import arrowdsl.core.interop as pa
from datafusion_engine.runtime import DataFusionRuntimeProfile


def _default_datafusion_profile() -> DataFusionRuntimeProfile:
    """Return the default DataFusion runtime profile.

    Returns
    -------
    DataFusionRuntimeProfile
        Default runtime profile instance.
    """
    return DataFusionRuntimeProfile()


type OrderingKey = tuple[str, str]
type ExecutionProfileName = Literal["bulk", "default", "deterministic", "streaming"]


class DeterminismTier(StrEnum):
    """Determinism budgets for the pipeline."""

    CANONICAL = "canonical"
    STABLE_SET = "stable_set"
    BEST_EFFORT = "best_effort"


class OrderingLevel(StrEnum):
    """Ordering metadata levels."""

    UNORDERED = "unordered"
    IMPLICIT = "implicit"
    EXPLICIT = "explicit"


class OrderingEffect(StrEnum):
    """Ordering effect classification for operations."""

    PRESERVE = "preserve"
    UNORDERED = "unordered"
    IMPLICIT = "implicit"
    EXPLICIT = "explicit"


@dataclass(frozen=True)
class Ordering:
    """Ordering metadata propagated through Plan operations.

    Parameters
    ----------
    level:
        Ordering level classification.
    keys:
        Tuple of (column, order) pairs.
    """

    level: OrderingLevel = OrderingLevel.UNORDERED
    keys: tuple[OrderingKey, ...] = ()

    @staticmethod
    def unordered() -> Ordering:
        """Return an unordered ordering marker.

        Returns
        -------
        Ordering
            Unordered marker.
        """
        return Ordering(OrderingLevel.UNORDERED, ())

    @staticmethod
    def implicit() -> Ordering:
        """Return an implicit ordering marker.

        Returns
        -------
        Ordering
            Implicit ordering marker.
        """
        return Ordering(OrderingLevel.IMPLICIT, ())

    @staticmethod
    def explicit(keys: tuple[OrderingKey, ...]) -> Ordering:
        """Return an explicit ordering marker.

        Parameters
        ----------
        keys:
            Explicit ordering keys.

        Returns
        -------
        Ordering
            Explicit ordering marker.
        """
        return Ordering(OrderingLevel.EXPLICIT, tuple(keys))


@dataclass(frozen=True)
class ScanProfile:
    """Dataset scan policy for Arrow scanners and Acero scans."""

    name: str
    batch_size: int | None = None
    batch_readahead: int | None = None
    fragment_readahead: int | None = None
    fragment_scan_options: object | None = None
    cache_metadata: bool = False
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
        if tier == DeterminismTier.CANONICAL and (
            not scan.implicit_ordering or not scan.require_sequenced_output
        ):
            scan = ScanProfile(
                name=scan.name,
                batch_size=scan.batch_size,
                batch_readahead=scan.batch_readahead,
                fragment_readahead=scan.fragment_readahead,
                fragment_scan_options=scan.fragment_scan_options,
                cache_metadata=scan.cache_metadata,
                use_threads=scan.use_threads,
                require_sequenced_output=True,
                implicit_ordering=True,
                scan_provenance_columns=scan.scan_provenance_columns,
            )
        return RuntimeProfile(
            name=self.name,
            cpu_threads=self.cpu_threads,
            io_threads=self.io_threads,
            scan=scan,
            plan_use_threads=self.plan_use_threads,
            determinism=tier,
            datafusion=self.datafusion,
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
        return RuntimeProfile(
            name=self.name,
            cpu_threads=self.cpu_threads,
            io_threads=self.io_threads,
            scan=self.scan,
            plan_use_threads=self.plan_use_threads,
            determinism=self.determinism,
            datafusion=profile,
        )


@dataclass(frozen=True)
class SchemaValidationPolicy:
    """Schema validation settings for contract boundaries."""

    enabled: bool = False
    strict: bool | Literal["filter"] = "filter"
    coerce: bool = False
    lazy: bool = True


@dataclass(frozen=True)
class ExecutionContext:
    """Execution-time knobs passed through the DSL."""

    runtime: RuntimeProfile
    mode: Literal["strict", "tolerant"] = "tolerant"
    provenance: bool = False
    safe_cast: bool = True
    debug: bool = False
    schema_validation: SchemaValidationPolicy = field(default_factory=SchemaValidationPolicy)

    @property
    def determinism(self) -> DeterminismTier:
        """Return the active determinism tier.

        Returns
        -------
        DeterminismTier
            Determinism tier for this context.
        """
        return self.runtime.determinism

    @property
    def use_threads(self) -> bool:
        """Return whether to enable plan execution threads.

        Returns
        -------
        bool
            ``True`` when plan execution should use threads.
        """
        return self.runtime.plan_use_threads

    @property
    def scan_use_threads(self) -> bool:
        """Return whether dataset scanning should use threads.

        Returns
        -------
        bool
            ``True`` when dataset scanning should use threads.
        """
        return self.runtime.scan.use_threads

    def with_mode(self, mode: Literal["strict", "tolerant"]) -> ExecutionContext:
        """Return a copy with a different finalize mode.

        Parameters
        ----------
        mode:
            New finalize mode.

        Returns
        -------
        ExecutionContext
            Updated execution context.
        """
        return ExecutionContext(
            runtime=self.runtime,
            mode=mode,
            provenance=self.provenance,
            safe_cast=self.safe_cast,
            debug=self.debug,
            schema_validation=self.schema_validation,
        )

    def with_provenance(self, *, provenance: bool) -> ExecutionContext:
        """Return a copy with provenance toggled.

        Parameters
        ----------
        provenance:
            When ``True``, include provenance columns in scans.

        Returns
        -------
        ExecutionContext
            Updated execution context.
        """
        return ExecutionContext(
            runtime=self.runtime,
            mode=self.mode,
            provenance=provenance,
            safe_cast=self.safe_cast,
            debug=self.debug,
            schema_validation=self.schema_validation,
        )

    def with_determinism(self, tier: DeterminismTier) -> ExecutionContext:
        """Return a copy with a determinism tier override applied.

        Parameters
        ----------
        tier:
            Determinism tier override.

        Returns
        -------
        ExecutionContext
            Updated execution context.
        """
        runtime = self.runtime.with_determinism(tier)
        return ExecutionContext(
            runtime=runtime,
            mode=self.mode,
            provenance=self.provenance,
            safe_cast=self.safe_cast,
            debug=self.debug,
            schema_validation=self.schema_validation,
        )


@dataclass(frozen=True)
class ExecutionContextOptions:
    """Execution context option bundle for factory construction."""

    mode: Literal["strict", "tolerant"] = "tolerant"
    provenance: bool = False
    safe_cast: bool = True
    debug: bool = False
    schema_validation: SchemaValidationPolicy = field(default_factory=SchemaValidationPolicy)


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
    if key == "default":
        return "default"
    if key == "streaming":
        return "streaming"
    if key == "bulk":
        return "bulk"
    if key == "deterministic":
        return "deterministic"
    msg = f"Unknown execution profile: {profile!r}."
    raise ValueError(msg)


def scan_profile_factory(profile: str) -> ScanProfile:
    """Return a ScanProfile for the named execution profile.

    Returns
    -------
    ScanProfile
        Scan profile matching the named profile.
    """
    profile_key = _normalize_profile(profile)
    if profile_key == "streaming":
        return ScanProfile(
            name="STREAM",
            batch_size=4096,
            batch_readahead=1,
            fragment_readahead=1,
            use_threads=True,
        )
    if profile_key == "bulk":
        return ScanProfile(
            name="BULK",
            batch_size=16384,
            batch_readahead=4,
            fragment_readahead=2,
            use_threads=True,
        )
    if profile_key == "deterministic":
        return ScanProfile(
            name="DETERMINISTIC",
            batch_size=4096,
            batch_readahead=1,
            fragment_readahead=1,
            use_threads=False,
            require_sequenced_output=True,
            implicit_ordering=True,
        )
    return ScanProfile(name="DEFAULT")


def runtime_profile_factory(profile: str) -> RuntimeProfile:
    """Return a RuntimeProfile for the named execution profile.

    Returns
    -------
    RuntimeProfile
        Runtime profile matching the named profile.
    """
    profile_key = _normalize_profile(profile)
    scan = scan_profile_factory(profile_key)
    plan_use_threads = profile_key != "deterministic"
    if profile_key == "deterministic":
        determinism = DeterminismTier.CANONICAL
    else:
        determinism = DeterminismTier.BEST_EFFORT
    name = scan.name
    runtime = RuntimeProfile(
        name=name,
        scan=scan,
        plan_use_threads=plan_use_threads,
        determinism=determinism,
    )
    if determinism == DeterminismTier.CANONICAL:
        return runtime.with_determinism(determinism)
    return runtime


def execution_context_factory(
    profile: str,
    *,
    options: ExecutionContextOptions | None = None,
) -> ExecutionContext:
    """Return an ExecutionContext for the named profile.

    Returns
    -------
    ExecutionContext
        Execution context with profile defaults applied.
    """
    runtime = runtime_profile_factory(profile)
    options = options or ExecutionContextOptions()
    if options.debug and runtime.datafusion is not None:
        datafusion_profile = replace(runtime.datafusion, capture_explain=True)
        runtime = runtime.with_datafusion(datafusion_profile)
    return ExecutionContext(
        runtime=runtime,
        mode=options.mode,
        provenance=options.provenance,
        safe_cast=options.safe_cast,
        debug=options.debug,
        schema_validation=options.schema_validation,
    )

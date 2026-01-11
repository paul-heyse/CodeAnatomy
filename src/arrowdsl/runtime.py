"""Runtime execution profiles and context settings."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Literal

import pyarrow as pa

type OrderingKey = tuple[str, str]


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
    use_threads: bool = True

    require_sequenced_output: bool = False
    implicit_ordering: bool = False

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
                use_threads=scan.use_threads,
                require_sequenced_output=True,
                implicit_ordering=True,
            )
        return RuntimeProfile(
            name=self.name,
            cpu_threads=self.cpu_threads,
            io_threads=self.io_threads,
            scan=scan,
            plan_use_threads=self.plan_use_threads,
            determinism=tier,
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

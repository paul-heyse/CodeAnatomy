from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, Optional, Tuple


class DeterminismTier(str, Enum):
    """Determinism budgets for the pipeline.

    CANONICAL: deterministic row order + winner selection (suitable for snapshots/caching).
    STABLE_SET: stable set of rows, but ordering may differ unless a contract requires sorting.
    BEST_EFFORT: prioritize throughput; only enforce ordering if explicitly requested by contract.
    """

    CANONICAL = "canonical"
    STABLE_SET = "stable_set"
    BEST_EFFORT = "best_effort"


class OrderingLevel(str, Enum):
    UNORDERED = "unordered"
    IMPLICIT = "implicit"
    EXPLICIT = "explicit"


@dataclass(frozen=True)
class Ordering:
    """Lightweight ordering metadata propagated through Plan operations.

    keys is a tuple of (column_name, order) pairs.
    """

    level: OrderingLevel = OrderingLevel.UNORDERED
    keys: Tuple[Tuple[str, str], ...] = ()

    @staticmethod
    def unordered() -> "Ordering":
        return Ordering(OrderingLevel.UNORDERED, ())

    @staticmethod
    def implicit() -> "Ordering":
        return Ordering(OrderingLevel.IMPLICIT, ())

    @staticmethod
    def explicit(keys: Tuple[Tuple[str, str], ...]) -> "Ordering":
        return Ordering(OrderingLevel.EXPLICIT, tuple(keys))


@dataclass(frozen=True)
class ScanProfile:
    """Central policy for dataset scanning (Scanner/ScanNodeOptions surface)."""

    name: str
    batch_size: Optional[int] = None
    batch_readahead: Optional[int] = None
    fragment_readahead: Optional[int] = None
    use_threads: bool = True

    # Determinism-oriented scan knobs (Acero scan node options)
    require_sequenced_output: bool = False
    implicit_ordering: bool = False

    def scanner_kwargs(self) -> Dict[str, Any]:
        """Kwargs for ds.Scanner.from_dataset (excluding columns/filter)."""
        kw: Dict[str, Any] = {"use_threads": self.use_threads}
        if self.batch_size is not None:
            kw["batch_size"] = self.batch_size
        if self.batch_readahead is not None:
            kw["batch_readahead"] = self.batch_readahead
        if self.fragment_readahead is not None:
            kw["fragment_readahead"] = self.fragment_readahead
        return kw

    def scan_node_kwargs(self) -> Dict[str, Any]:
        """Kwargs for acero.ScanNodeOptions (excluding dataset/columns/filter)."""
        kw: Dict[str, Any] = {}
        if self.require_sequenced_output:
            kw["require_sequenced_output"] = True
        if self.implicit_ordering:
            kw["implicit_ordering"] = True
        return kw


@dataclass(frozen=True)
class RuntimeProfile:
    """Bundles global threading + scan policy + plan threading + determinism defaults."""

    name: str

    # Global Arrow thread pools (process scope). None => leave default.
    cpu_threads: Optional[int] = None
    io_threads: Optional[int] = None

    scan: ScanProfile = ScanProfile(name="DEFAULT")
    plan_use_threads: bool = True

    determinism: DeterminismTier = DeterminismTier.BEST_EFFORT

    def apply_global_thread_pools(self) -> None:
        """Set Arrow CPU + IO pools (recommended once at process startup)."""
        try:
            import pyarrow as pa
        except Exception:
            return

        if self.cpu_threads is not None:
            pa.set_cpu_count(int(self.cpu_threads))
        if self.io_threads is not None:
            pa.set_io_thread_count(int(self.io_threads))

    def with_determinism(self, tier: DeterminismTier) -> "RuntimeProfile":
        return RuntimeProfile(
            name=self.name,
            cpu_threads=self.cpu_threads,
            io_threads=self.io_threads,
            scan=self.scan,
            plan_use_threads=self.plan_use_threads,
            determinism=tier,
        )


@dataclass(frozen=True)
class ExecutionContext:
    """Execution-time knobs passed through the DSL."""

    runtime: RuntimeProfile

    # Finalize behavior
    mode: str = "tolerant"  # "strict" | "tolerant"

    # Include provenance fields in scans when possible
    provenance: bool = False

    # Schema alignment casting
    safe_cast: bool = True

    # Debug toggles
    debug: bool = False

    @property
    def determinism(self) -> DeterminismTier:
        return self.runtime.determinism

    @property
    def use_threads(self) -> bool:
        """Default plan execution threading (Declaration.to_table / to_reader)."""
        return self.runtime.plan_use_threads

    @property
    def scan_use_threads(self) -> bool:
        """Dataset scanning threading (Scanner.from_dataset)."""
        return self.runtime.scan.use_threads

    def with_mode(self, mode: str) -> "ExecutionContext":
        return ExecutionContext(
            runtime=self.runtime,
            mode=mode,
            provenance=self.provenance,
            safe_cast=self.safe_cast,
            debug=self.debug,
        )

    def with_provenance(self, provenance: bool) -> "ExecutionContext":
        return ExecutionContext(
            runtime=self.runtime,
            mode=self.mode,
            provenance=provenance,
            safe_cast=self.safe_cast,
            debug=self.debug,
        )

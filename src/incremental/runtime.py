"""Shared runtime helpers for incremental pipeline execution."""

from __future__ import annotations

import contextlib
import uuid
from dataclasses import dataclass
from typing import TYPE_CHECKING, Self

import pyarrow as pa

from core_types import DeterminismTier
from datafusion_engine.introspection import invalidate_introspection_cache
from datafusion_engine.io_adapter import DataFusionIOAdapter
from datafusion_engine.runtime import DataFusionRuntimeProfile, SessionRuntime

if TYPE_CHECKING:
    from datafusion import SessionContext


@dataclass
class IncrementalRuntime:
    """Runtime container for DataFusion incremental execution."""

    profile: DataFusionRuntimeProfile
    _session_runtime: SessionRuntime
    determinism_tier: DeterminismTier = DeterminismTier.BEST_EFFORT

    @classmethod
    def build(
        cls,
        *,
        profile: DataFusionRuntimeProfile | None = None,
        profile_name: str = "default",
        determinism_tier: DeterminismTier = DeterminismTier.BEST_EFFORT,
    ) -> IncrementalRuntime:
        """Create a runtime with default DataFusion profile.

        Returns
        -------
        IncrementalRuntime
            Newly constructed runtime instance.
        """
        runtime_profile = profile or DataFusionRuntimeProfile(config_policy_name=profile_name)
        return cls(
            profile=runtime_profile,
            _session_runtime=runtime_profile.session_runtime(),
            determinism_tier=determinism_tier,
        )

    def session_runtime(self) -> SessionRuntime:
        """Return the cached DataFusion SessionRuntime.

        Returns
        -------
        SessionRuntime
            Cached DataFusion session runtime.
        """
        return self._session_runtime

    def session_context(self) -> SessionContext:
        """Return the DataFusion SessionContext for compatibility.

        Returns
        -------
        SessionContext
            Session context bound to the incremental SessionRuntime.
        """
        return self._session_runtime.ctx

    def io_adapter(self) -> DataFusionIOAdapter:
        """Return a DataFusion IO adapter bound to this runtime.

        Returns
        -------
        DataFusionIOAdapter
            IO adapter bound to the runtime session.
        """
        return DataFusionIOAdapter(ctx=self._session_runtime.ctx, profile=self.profile)


class TempTableRegistry:
    """Track and cleanup temporary DataFusion tables."""

    def __init__(self, runtime: IncrementalRuntime) -> None:
        self._runtime = runtime
        self._ctx = runtime.session_runtime().ctx
        self._names: list[str] = []

    def register_table(self, table: pa.Table, *, prefix: str) -> str:
        """Register an Arrow table and track its name.

        Returns
        -------
        str
            Registered table name.
        """
        name = f"__incremental_{prefix}_{uuid.uuid4().hex}"
        self._runtime.io_adapter().register_arrow_table(name, table, overwrite=True)
        self._names.append(name)
        return name

    def register_batches(self, batches: list[pa.RecordBatch], *, prefix: str) -> str:
        """Register Arrow record batches and track their name.

        Returns
        -------
        str
            Registered table name.
        """
        name = f"__incremental_{prefix}_{uuid.uuid4().hex}"
        self._runtime.io_adapter().register_record_batches(name, batches, overwrite=True)
        self._names.append(name)
        return name

    def track(self, name: str) -> str:
        """Track a table name registered elsewhere.

        Returns
        -------
        str
            Tracked table name.
        """
        if name not in self._names:
            self._names.append(name)
        return name

    def deregister(self, name: str) -> None:
        """Deregister a tracked table name if possible."""
        deregister = getattr(self._ctx, "deregister_table", None)
        if callable(deregister):
            with contextlib.suppress(KeyError, RuntimeError, TypeError, ValueError):
                deregister(name)
                invalidate_introspection_cache(self._ctx)
        if name in self._names:
            self._names.remove(name)

    def close(self) -> None:
        """Deregister all tracked tables."""
        for name in list(self._names):
            self.deregister(name)

    def __enter__(self) -> Self:
        """Enter the context and return self.

        Returns
        -------
        TempTableRegistry
            Context manager instance.
        """
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        """Exit the context and cleanup tracked tables."""
        self.close()


__all__ = ["IncrementalRuntime", "TempTableRegistry"]

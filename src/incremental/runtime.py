"""Shared runtime helpers for incremental pipeline execution."""

from __future__ import annotations

import contextlib
import uuid
from dataclasses import dataclass
from typing import Self

import pyarrow as pa
from datafusion import SessionContext

from arrowdsl.core.execution_context import ExecutionContext, execution_context_factory
from datafusion_engine.introspection import invalidate_introspection_cache
from datafusion_engine.io_adapter import DataFusionIOAdapter
from datafusion_engine.runtime import DataFusionRuntimeProfile


@dataclass
class IncrementalRuntime:
    """Runtime container for DataFusion incremental execution."""

    execution_ctx: ExecutionContext
    profile: DataFusionRuntimeProfile
    _ctx: SessionContext | None = None

    @classmethod
    def build(
        cls,
        *,
        ctx: ExecutionContext | None = None,
        profile: str = "default",
    ) -> IncrementalRuntime:
        """Create a runtime with default DataFusion profile.

        Returns
        -------
        IncrementalRuntime
            Newly constructed runtime instance.

        Raises
        ------
        ValueError
            Raised when the execution context lacks a DataFusion profile.
        """
        exec_ctx = ctx or execution_context_factory(profile)
        runtime_profile = exec_ctx.runtime.datafusion
        if runtime_profile is None:
            msg = "Incremental runtime requires a DataFusion profile."
            raise ValueError(msg)
        return cls(
            execution_ctx=exec_ctx,
            profile=runtime_profile,
        )

    def session_context(self) -> SessionContext:
        """Return the cached DataFusion SessionContext.

        Returns
        -------
        SessionContext
            Cached or newly created DataFusion session.
        """
        if self._ctx is None:
            self._ctx = self.profile.session_context()
        return self._ctx

    def execution_context(self) -> ExecutionContext:
        """Return the cached ExecutionContext for incremental work.

        Returns
        -------
        ExecutionContext
            Cached or newly created execution context.
        """
        return self.execution_ctx

    def io_adapter(self) -> DataFusionIOAdapter:
        """Return a DataFusion IO adapter bound to this runtime.

        Returns
        -------
        DataFusionIOAdapter
            IO adapter bound to the runtime session.
        """
        return DataFusionIOAdapter(ctx=self.session_context(), profile=self.profile)


class TempTableRegistry:
    """Track and cleanup temporary DataFusion tables."""

    def __init__(self, runtime: IncrementalRuntime) -> None:
        self._runtime = runtime
        self._ctx = runtime.session_context()
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

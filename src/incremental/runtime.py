"""Shared runtime helpers for incremental pipeline execution."""

from __future__ import annotations

import contextlib
import uuid
from dataclasses import dataclass
from typing import TYPE_CHECKING, Self

import pyarrow as pa
from datafusion import SessionContext

from arrowdsl.core.execution_context import ExecutionContext, execution_context_factory
from arrowdsl.core.ordering import Ordering
from datafusion_engine.introspection import invalidate_introspection_cache
from datafusion_engine.runtime import DataFusionRuntimeProfile
from ibis_engine.execution_factory import ibis_backend_from_ctx, ibis_execution_from_ctx
from ibis_engine.sources import (
    SourceToIbisOptions,
    register_ibis_record_batches,
    register_ibis_table,
)
from sqlglot_tools.optimizer import SqlGlotPolicy, resolve_sqlglot_policy

if TYPE_CHECKING:
    from ibis.backends import BaseBackend

    from ibis_engine.execution import IbisExecutionContext


@dataclass
class IncrementalRuntime:
    """Runtime container for DataFusion/Ibis/SQLGlot integration."""

    execution_ctx: ExecutionContext
    profile: DataFusionRuntimeProfile
    sqlglot_policy: SqlGlotPolicy
    _ctx: SessionContext | None = None
    _ibis_backend: BaseBackend | None = None
    _ibis_execution: IbisExecutionContext | None = None

    @classmethod
    def build(
        cls,
        *,
        ctx: ExecutionContext | None = None,
        profile: str = "default",
        sqlglot_policy: SqlGlotPolicy | None = None,
    ) -> IncrementalRuntime:
        """Create a runtime with default DataFusion profile and SQLGlot policy.

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
        policy = resolve_sqlglot_policy(name="datafusion_compile", policy=sqlglot_policy)
        return cls(
            execution_ctx=exec_ctx,
            profile=runtime_profile,
            sqlglot_policy=policy,
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

    def ibis_backend(self) -> BaseBackend:
        """Return the cached Ibis backend bound to this runtime.

        Returns
        -------
        ibis.backends.BaseBackend
            Cached or newly created Ibis backend.
        """
        backend = self._ibis_backend
        if backend is None:
            backend = ibis_backend_from_ctx(self.execution_ctx)
            self._ibis_backend = backend
        return backend

    def execution_context(self) -> ExecutionContext:
        """Return the cached ExecutionContext for incremental work.

        Returns
        -------
        ExecutionContext
            Cached or newly created execution context.
        """
        return self.execution_ctx

    def ibis_execution(self) -> IbisExecutionContext:
        """Return the cached Ibis execution context.

        Returns
        -------
        IbisExecutionContext
            Cached or newly created Ibis execution context.
        """
        execution = self._ibis_execution
        if execution is None:
            execution = ibis_execution_from_ctx(
                self.execution_ctx,
                backend=self.ibis_backend(),
            )
            self._ibis_execution = execution
        return execution


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
        register_ibis_table(
            table,
            options=SourceToIbisOptions(
                backend=self._runtime.ibis_backend(),
                name=name,
                ordering=Ordering.unordered(),
                runtime_profile=self._runtime.profile,
            ),
        )
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
        register_ibis_record_batches(
            batches,
            options=SourceToIbisOptions(
                backend=self._runtime.ibis_backend(),
                name=name,
                ordering=Ordering.unordered(),
                runtime_profile=self._runtime.profile,
            ),
        )
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

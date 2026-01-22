"""Shared runtime helpers for incremental pipeline execution."""

from __future__ import annotations

import contextlib
import uuid
from dataclasses import dataclass
from typing import TYPE_CHECKING, Self

import pyarrow as pa
from datafusion import SessionContext

from arrowdsl.core.execution_context import ExecutionContext
from arrowdsl.core.runtime_profiles import runtime_profile_factory
from datafusion_engine.runtime import DataFusionRuntimeProfile
from ibis_engine.execution_factory import ibis_backend_from_profile, ibis_execution_from_ctx
from sqlglot_tools.optimizer import SqlGlotPolicy, default_sqlglot_policy

if TYPE_CHECKING:
    from ibis.backends import BaseBackend

    from ibis_engine.execution import IbisExecutionContext


@dataclass
class IncrementalRuntime:
    """Runtime container for DataFusion/Ibis/SQLGlot integration."""

    profile: DataFusionRuntimeProfile
    sqlglot_policy: SqlGlotPolicy
    _ctx: SessionContext | None = None
    _ibis_backend: BaseBackend | None = None
    _execution_ctx: ExecutionContext | None = None
    _ibis_execution: IbisExecutionContext | None = None

    @classmethod
    def build(cls, *, sqlglot_policy: SqlGlotPolicy | None = None) -> IncrementalRuntime:
        """Create a runtime with default DataFusion profile and SQLGlot policy.

        Returns
        -------
        IncrementalRuntime
            Newly constructed runtime instance.
        """
        policy = sqlglot_policy or default_sqlglot_policy()
        return cls(profile=DataFusionRuntimeProfile(), sqlglot_policy=policy)

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
            backend = ibis_backend_from_profile(self.profile)
            self._ibis_backend = backend
        return backend

    def execution_context(self) -> ExecutionContext:
        """Return the cached ExecutionContext for incremental work.

        Returns
        -------
        ExecutionContext
            Cached or newly created execution context.
        """
        ctx = self._execution_ctx
        if ctx is None:
            runtime_profile = runtime_profile_factory("default").with_datafusion(self.profile)
            ctx = ExecutionContext(runtime=runtime_profile)
            self._execution_ctx = ctx
        return ctx

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
                self.execution_context(),
                backend=self.ibis_backend(),
            )
            self._ibis_execution = execution
        return execution


class TempTableRegistry:
    """Track and cleanup temporary DataFusion tables."""

    def __init__(self, ctx: SessionContext) -> None:
        self._ctx = ctx
        self._names: list[str] = []

    def register_table(self, table: pa.Table, *, prefix: str) -> str:
        """Register an Arrow table and track its name.

        Returns
        -------
        str
            Registered table name.
        """
        name = f"__incremental_{prefix}_{uuid.uuid4().hex}"
        self._ctx.register_record_batches(name, table.to_batches())
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
        self._ctx.register_record_batches(name, batches)
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

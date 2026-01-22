"""Shared runtime helpers for incremental pipeline execution."""

from __future__ import annotations

import contextlib
import uuid
from dataclasses import dataclass
from typing import TYPE_CHECKING, Self

import pyarrow as pa
from datafusion import SessionContext

from datafusion_engine.runtime import DataFusionRuntimeProfile
from ibis_engine.backend import build_backend
from ibis_engine.config import IbisBackendConfig
from sqlglot_tools.optimizer import SqlGlotPolicy, default_sqlglot_policy

if TYPE_CHECKING:
    from ibis.backends import BaseBackend


@dataclass
class IncrementalRuntime:
    """Runtime container for DataFusion/Ibis/SQLGlot integration."""

    profile: DataFusionRuntimeProfile
    sqlglot_policy: SqlGlotPolicy
    _ctx: SessionContext | None = None
    _ibis_backend: BaseBackend | None = None

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
            backend = build_backend(IbisBackendConfig(datafusion_profile=self.profile))
            self._ibis_backend = backend
        return backend


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

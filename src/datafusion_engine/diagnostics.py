"""Diagnostics protocol and recording infrastructure.

This module provides a unified diagnostics recording system for DataFusion
operations. All diagnostics emission should go through DiagnosticsRecorder
to ensure consistent payload shapes.

Examples
--------
>>> from datafusion_engine.diagnostics import (
...     InMemoryDiagnosticsSink,
...     DiagnosticsContext,
...     DiagnosticsRecorder,
... )
>>> sink = InMemoryDiagnosticsSink()
>>> context = DiagnosticsContext(session_id="test", operation_id="op1")
>>> recorder = DiagnosticsRecorder(sink, context)
>>> recorder.record_compilation(
...     input_sql="SELECT * FROM table",
...     output_sql="SELECT * FROM table",
...     ast_fingerprint="abc123",
...     policy_fingerprint="def456",
...     dialect="postgres",
...     duration_ms=10.5,
... )
>>> sink.get_artifacts("sql_compilation")
[{'session_id': 'test', 'operation_id': 'op1', ...}]
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from datafusion_engine.runtime import DataFusionRuntimeProfile


class DiagnosticsSink(Protocol):
    """Protocol for diagnostics sinks.

    All diagnostics sinks must implement these three methods to capture
    artifacts, metrics, and events from DataFusion operations.
    """

    def record_artifact(self, name: str, payload: dict[str, Any]) -> None:
        """Record a named artifact.

        Parameters
        ----------
        name : str
            Artifact type identifier (e.g., "sql_compilation", "write_operation").
        payload : dict[str, Any]
            Artifact payload with type-specific fields.
        """
        ...

    def record_metric(self, name: str, value: float, tags: dict[str, str]) -> None:
        """Record a metric value.

        Parameters
        ----------
        name : str
            Metric name (e.g., "cache_hit_rate", "execution_duration_ms").
        value : float
            Metric value.
        tags : dict[str, str]
            Tags for metric categorization.
        """
        ...

    def record_event(self, name: str, properties: dict[str, Any]) -> None:
        """Record an event.

        Parameters
        ----------
        name : str
            Event type identifier (e.g., "cache_access", "schema_validation").
        properties : dict[str, Any]
            Event properties.
        """
        ...


@dataclass
class InMemoryDiagnosticsSink:
    """In-memory diagnostics sink for testing and debugging.

    Stores all artifacts, metrics, and events in memory for inspection.
    Useful for unit tests and local debugging.

    Attributes
    ----------
    artifacts : list[tuple[str, dict]]
        List of (name, payload) tuples for recorded artifacts.
    metrics : list[tuple[str, float, dict]]
        List of (name, value, tags) tuples for recorded metrics.
    events : list[tuple[str, dict]]
        List of (name, properties) tuples for recorded events.
    """

    artifacts: list[tuple[str, dict[str, Any]]] = field(default_factory=list)
    metrics: list[tuple[str, float, dict[str, str]]] = field(default_factory=list)
    events: list[tuple[str, dict[str, Any]]] = field(default_factory=list)

    def record_artifact(self, name: str, payload: dict[str, Any]) -> None:
        """Record a named artifact.

        Parameters
        ----------
        name : str
            Artifact type identifier.
        payload : dict[str, Any]
            Artifact payload.
        """
        self.artifacts.append((name, payload))

    def record_metric(self, name: str, value: float, tags: dict[str, str]) -> None:
        """Record a metric value.

        Parameters
        ----------
        name : str
            Metric name.
        value : float
            Metric value.
        tags : dict[str, str]
            Tags for metric categorization.
        """
        self.metrics.append((name, value, tags))

    def record_event(self, name: str, properties: dict[str, Any]) -> None:
        """Record an event.

        Parameters
        ----------
        name : str
            Event type identifier.
        properties : dict[str, Any]
            Event properties.
        """
        self.events.append((name, properties))

    def get_artifacts(self, name: str) -> list[dict[str, Any]]:
        """Get all artifacts with given name.

        Parameters
        ----------
        name : str
            Artifact type identifier to filter by.

        Returns
        -------
        list[dict]
            List of artifact payloads matching the given name.
        """
        return [p for n, p in self.artifacts if n == name]


@dataclass(frozen=True)
class DiagnosticsContext:
    """Context for diagnostics recording.

    Provides session-level context information that is automatically included
    in all diagnostics artifacts.

    Attributes
    ----------
    session_id : str
        Unique identifier for the DataFusion session.
    operation_id : str
        Unique identifier for the current operation.
    start_time : datetime
        Timestamp when the context was created (defaults to current UTC time).
    tags : dict[str, str]
        Additional tags for categorization (defaults to empty dict).
    """

    session_id: str
    operation_id: str
    start_time: datetime = field(default_factory=lambda: datetime.now().astimezone())
    tags: dict[str, str] = field(default_factory=dict)


class DiagnosticsRecorder:
    """Centralized diagnostics recorder.

    All diagnostics emission should go through this class to ensure
    consistent payload shapes across different artifact types.

    Parameters
    ----------
    sink : DiagnosticsSink | None
        Diagnostics sink to record to. If None, recording is disabled.
    context : DiagnosticsContext | None
        Context information for this recorder. If None, uses default context.

    Attributes
    ----------
    _sink : DiagnosticsSink | None
        The underlying diagnostics sink.
    _context : DiagnosticsContext
        The diagnostics context for this recorder.
    """

    def __init__(
        self,
        sink: DiagnosticsSink | None,
        context: DiagnosticsContext | None = None,
    ) -> None:
        """Initialize diagnostics recorder.

        Parameters
        ----------
        sink : DiagnosticsSink | None
            Diagnostics sink to record to.
        context : DiagnosticsContext | None
            Context information for this recorder.
        """
        self._sink = sink
        self._context = context or DiagnosticsContext(
            session_id="default",
            operation_id="default",
        )

    @property
    def enabled(self) -> bool:
        """Check if diagnostics are enabled.

        Returns
        -------
        bool
            True if sink is configured and recording is enabled.
        """
        return self._sink is not None

    def record_compilation(  # noqa: PLR0913
        self,
        *,
        input_sql: str,
        output_sql: str,
        ast_fingerprint: str,
        policy_fingerprint: str,
        dialect: str,
        duration_ms: float,
        lineage: dict[str, Any] | None = None,
    ) -> None:
        """Record SQL compilation diagnostics.

        Parameters
        ----------
        input_sql : str
            Original SQL input.
        output_sql : str
            Compiled/normalized SQL output.
        ast_fingerprint : str
            Fingerprint of the SQL AST.
        policy_fingerprint : str
            Fingerprint of the safety policy applied.
        dialect : str
            SQL dialect used for compilation.
        duration_ms : float
            Compilation duration in milliseconds.
        lineage : dict[str, Any] | None, optional
            Column-level lineage information.
        """
        if not self.enabled or self._sink is None:
            return

        self._sink.record_artifact(
            "sql_compilation",
            {
                "session_id": self._context.session_id,
                "operation_id": self._context.operation_id,
                "timestamp": datetime.now().astimezone().isoformat(),
                "input_sql": input_sql,
                "output_sql": output_sql,
                "ast_fingerprint": ast_fingerprint,
                "policy_fingerprint": policy_fingerprint,
                "dialect": dialect,
                "duration_ms": duration_ms,
                "lineage": lineage,
                "tags": self._context.tags,
            },
        )

    def record_execution(  # noqa: PLR0913
        self,
        *,
        sql: str,
        ast_fingerprint: str,
        duration_ms: float,
        rows_produced: int | None = None,
        bytes_scanned: int | None = None,
        error: str | None = None,
    ) -> None:
        """Record SQL execution diagnostics.

        Parameters
        ----------
        sql : str
            SQL statement executed.
        ast_fingerprint : str
            Fingerprint of the SQL AST.
        duration_ms : float
            Execution duration in milliseconds.
        rows_produced : int | None, optional
            Number of rows produced by the query.
        bytes_scanned : int | None, optional
            Number of bytes scanned during execution.
        error : str | None, optional
            Error message if execution failed.
        """
        if not self.enabled or self._sink is None:
            return

        self._sink.record_artifact(
            "sql_execution",
            {
                "session_id": self._context.session_id,
                "operation_id": self._context.operation_id,
                "timestamp": datetime.now().astimezone().isoformat(),
                "sql": sql,
                "ast_fingerprint": ast_fingerprint,
                "duration_ms": duration_ms,
                "rows_produced": rows_produced,
                "bytes_scanned": bytes_scanned,
                "error": error,
                "success": error is None,
                "tags": self._context.tags,
            },
        )

    def record_write(  # noqa: PLR0913
        self,
        *,
        destination: str,
        format_: str,  # "parquet", "csv", "json"
        method: str,  # "copy", "streaming", "insert"
        rows_written: int | None = None,
        bytes_written: int | None = None,
        partitions: int | None = None,
        duration_ms: float,
    ) -> None:
        """Record write operation diagnostics.

        Parameters
        ----------
        destination : str
            Write destination path or table name.
        format_ : str
            Output format ("parquet", "csv", "json").
        method : str
            Write method ("copy", "streaming", "insert").
        rows_written : int | None, optional
            Number of rows written.
        bytes_written : int | None, optional
            Number of bytes written.
        partitions : int | None, optional
            Number of partitions written.
        duration_ms : float
            Write operation duration in milliseconds.
        """
        if not self.enabled or self._sink is None:
            return

        self._sink.record_artifact(
            "write_operation",
            {
                "session_id": self._context.session_id,
                "operation_id": self._context.operation_id,
                "timestamp": datetime.now().astimezone().isoformat(),
                "destination": destination,
                "format": format_,
                "method": method,
                "rows_written": rows_written,
                "bytes_written": bytes_written,
                "partitions": partitions,
                "duration_ms": duration_ms,
                "tags": self._context.tags,
            },
        )

    def record_registration(
        self,
        *,
        name: str,
        registration_type: str,  # "table", "view", "object_store"
        location: str | None = None,
        schema: dict[str, Any] | None = None,
    ) -> None:
        """Record table/view/store registration diagnostics.

        Parameters
        ----------
        name : str
            Name of the registered entity.
        registration_type : str
            Type of registration ("table", "view", "object_store").
        location : str | None, optional
            Physical location if applicable.
        schema : dict | None, optional
            Schema definition if applicable.
        """
        if not self.enabled or self._sink is None:
            return

        self._sink.record_artifact(
            "registration",
            {
                "session_id": self._context.session_id,
                "operation_id": self._context.operation_id,
                "timestamp": datetime.now().astimezone().isoformat(),
                "name": name,
                "type": registration_type,
                "location": location,
                "schema": schema,
                "tags": self._context.tags,
            },
        )

    def record_cache_event(
        self,
        *,
        key: str,
        hit: bool,
        fingerprint: str | None = None,
    ) -> None:
        """Record cache hit/miss event.

        Parameters
        ----------
        key : str
            Cache key.
        hit : bool
            True if cache hit, False if cache miss.
        fingerprint : str | None, optional
            Fingerprint of the cached artifact.
        """
        if not self.enabled or self._sink is None:
            return

        self._sink.record_event(
            "cache_access",
            {
                "session_id": self._context.session_id,
                "key": key,
                "hit": hit,
                "fingerprint": fingerprint,
            },
        )

        self._sink.record_metric(
            "cache_hit_rate",
            1.0 if hit else 0.0,
            self._context.tags,
        )


def record_artifact(
    profile: DataFusionRuntimeProfile | None,
    name: str,
    payload: dict[str, Any],
) -> None:
    """Record an artifact directly from a runtime profile.

    Use this when you don't have a DiagnosticsRecorder instance and need
    to record a single artifact directly from a runtime profile.

    Parameters
    ----------
    profile : DataFusionRuntimeProfile | None
        Runtime profile containing diagnostics sink. If None or sink is None,
        no recording occurs.
    name : str
        Artifact type identifier.
    payload : dict[str, Any]
        Artifact payload.
    """
    if profile is None or profile.diagnostics_sink is None:
        return
    profile.diagnostics_sink.record_artifact(name, payload)

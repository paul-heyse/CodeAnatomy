"""Diagnostics protocol and recording infrastructure.

This module provides a unified diagnostics recording system for DataFusion
operations. All diagnostics emission should go through DiagnosticsRecorder
to ensure consistent payload shapes.

Examples:
--------
>>> from datafusion_engine.lineage.diagnostics import (
...     CompilationRecord,
...     InMemoryDiagnosticsSink,
...     DiagnosticsContext,
...     DiagnosticsRecorder,
... )
>>> sink = InMemoryDiagnosticsSink()
>>> context = DiagnosticsContext(session_id="test", operation_id="op1")
>>> recorder = DiagnosticsRecorder(sink, context)
>>> record = CompilationRecord(
...     input_sql="SELECT * FROM table",
...     output_sql="SELECT * FROM table",
...     plan_fingerprint="plan_abc123",
...     dialect="postgres",
...     duration_ms=10.5,
... )
>>> recorder.record_compilation(record)
>>> sink.get_artifacts("sql_compilation")
[{'session_id': 'test', 'operation_id': 'op1', ...}]
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, Any, Protocol, cast

from utils.hashing import hash_json_canonical
from utils.uuid_factory import uuid7_str

if TYPE_CHECKING:
    from datafusion import SessionContext

    from datafusion_engine.session.runtime import DataFusionRuntimeProfile
    from datafusion_engine.views.graph import ViewNode
    from serde_schema_registry import ArtifactSpec


class DiagnosticsSink(Protocol):
    """Protocol for diagnostics sinks.

    All diagnostics sinks must implement these methods to capture
    artifacts and events from DataFusion operations.
    """

    def record_artifact(self, name: ArtifactSpec, payload: Mapping[str, Any]) -> None:
        """Record a named artifact.

        Parameters
        ----------
        name : ArtifactSpec
            Artifact spec identifier.
        payload : Mapping[str, Any]
            Artifact payload with type-specific fields.
        """
        ...

    def record_event(self, name: str, properties: Mapping[str, Any]) -> None:
        """Record an event.

        Parameters
        ----------
        name : str
            Event type identifier (e.g., "cache_access", "schema_validation").
        properties : Mapping[str, Any]
            Event properties.
        """
        ...

    def record_events(self, name: str, rows: Sequence[Mapping[str, Any]]) -> None:
        """Record a batch of events.

        Parameters
        ----------
        name : str
            Event type identifier.
        rows : Sequence[Mapping[str, Any]]
            Event payload rows.
        """
        ...

    def events_snapshot(self) -> dict[str, list[Mapping[str, Any]]]:
        """Return collected event rows by name."""
        ...

    def artifacts_snapshot(self) -> dict[str, list[Mapping[str, Any]]]:
        """Return collected artifact payloads by name."""
        ...


@dataclass(frozen=True)
class CompilationRecord:
    """Payload for SQL compilation diagnostics.

    Uses DataFusion plan fingerprints for compilation tracking.
    """

    input_sql: str
    output_sql: str
    dialect: str
    duration_ms: float
    lineage: dict[str, Any] | None = None
    plan_fingerprint: str | None = None


@dataclass(frozen=True)
class ExecutionRecord:
    """Payload for SQL execution diagnostics.

    Uses DataFusion plan fingerprints for execution tracking.
    """

    sql: str
    duration_ms: float
    rows_produced: int | None = None
    bytes_scanned: int | None = None
    error: str | None = None
    plan_fingerprint: str | None = None


@dataclass(frozen=True)
class WriteRecord:
    """Payload for write operation diagnostics."""

    destination: str
    format_: str
    method: str
    duration_ms: float
    rows_written: int | None = None
    bytes_written: int | None = None
    partitions: int | None = None
    sql: str | None = None
    delta_features: Mapping[str, str] | None = None


@dataclass
class InMemoryDiagnosticsSink:
    """In-memory diagnostics sink for testing and debugging.

    Stores all artifacts and events in memory for inspection.
    Useful for unit tests and local debugging.

    Attributes:
    ----------
    artifacts : list[tuple[str, dict]]
        List of (name, payload) tuples for recorded artifacts.
    events : list[tuple[str, dict]]
        List of (name, properties) tuples for recorded events.
    """

    artifacts: list[tuple[str, dict[str, Any]]] = field(default_factory=list)
    events: list[tuple[str, dict[str, Any]]] = field(default_factory=list)

    def record_artifact(self, name: ArtifactSpec, payload: Mapping[str, Any]) -> None:
        """Record a named artifact.

        Parameters
        ----------
        name : ArtifactSpec
            Artifact spec identifier.
        payload : dict[str, Any]
            Artifact payload.
        """
        self.artifacts.append((name.canonical_name, dict(payload)))

    def record_event(self, name: str, properties: Mapping[str, Any]) -> None:
        """Record an event.

        Parameters
        ----------
        name : str
            Event type identifier.
        properties : dict[str, Any]
            Event properties.
        """
        self.events.append((name, dict(properties)))

    def record_events(self, name: str, rows: Sequence[Mapping[str, Any]]) -> None:
        """Record a batch of events.

        Parameters
        ----------
        name : str
            Event type identifier.
        rows : Sequence[Mapping[str, Any]]
            Event payload rows.
        """
        for row in rows:
            self.record_event(name, dict(row))

    def events_snapshot(self) -> dict[str, list[Mapping[str, Any]]]:
        """Return collected event rows by name.

        Returns:
        -------
        dict[str, list[Mapping[str, Any]]]
            Mapping of event names to collected payloads.
        """
        snapshot: dict[str, list[Mapping[str, Any]]] = {}
        for name, payload in self.events:
            snapshot.setdefault(name, []).append(dict(payload))
        return snapshot

    def artifacts_snapshot(self) -> dict[str, list[Mapping[str, Any]]]:
        """Return collected artifact payloads by name.

        Returns:
        -------
        dict[str, list[Mapping[str, Any]]]
            Mapping of artifact names to collected payloads.
        """
        snapshot: dict[str, list[Mapping[str, Any]]] = {}
        for name, payload in self.artifacts:
            snapshot.setdefault(name, []).append(dict(payload))
        return snapshot

    def get_artifacts(self, name: str) -> list[dict[str, Any]]:
        """Get all artifacts with given name.

        Parameters
        ----------
        name : str
            Artifact type identifier to filter by.

        Returns:
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

    Attributes:
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

    session_id: str = field(default_factory=uuid7_str)
    operation_id: str = field(default_factory=uuid7_str)
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

    Attributes:
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
        self._context = context or DiagnosticsContext()

    @property
    def enabled(self) -> bool:
        """Check if diagnostics are enabled.

        Returns:
        -------
        bool
            True if sink is configured and recording is enabled.
        """
        return self._sink is not None

    def record_artifact(self, name: ArtifactSpec, payload: Mapping[str, Any]) -> None:
        """Record a named artifact through the recorder.

        Parameters
        ----------
        name : ArtifactSpec
            Artifact spec identifier.
        payload : dict[str, Any]
            Artifact payload.
        """
        if not self.enabled or self._sink is None:
            return
        self._sink.record_artifact(name, payload)

    def record_event(self, name: str, properties: Mapping[str, Any]) -> None:
        """Record a single event through the recorder.

        Parameters
        ----------
        name : str
            Event type identifier.
        properties : dict[str, Any]
            Event properties.
        """
        if not self.enabled or self._sink is None:
            return
        self._sink.record_event(name, properties)

    def record_events(self, name: str, rows: Sequence[Mapping[str, Any]]) -> None:
        """Record a batch of event rows through the recorder.

        Parameters
        ----------
        name : str
            Event type identifier.
        rows : Sequence[Mapping[str, Any]]
            Event payload rows.
        """
        if not self.enabled or self._sink is None:
            return
        self._sink.record_events(name, rows)

    def record_compilation(self, record: CompilationRecord) -> None:
        """Record SQL compilation diagnostics.

        Parameters
        ----------
        record
            Compilation diagnostics payload.
        """
        if not self.enabled or self._sink is None:
            return
        from serde_schema_registry import ArtifactSpec

        self._sink.record_artifact(
            ArtifactSpec(
                canonical_name="sql_compilation",
                description="SQL compilation diagnostics artifact.",
            ),
            {
                "session_id": self._context.session_id,
                "operation_id": self._context.operation_id,
                "timestamp": datetime.now().astimezone().isoformat(),
                "input_sql": record.input_sql,
                "output_sql": record.output_sql,
                "plan_fingerprint": record.plan_fingerprint,
                "dialect": record.dialect,
                "duration_ms": record.duration_ms,
                "lineage": record.lineage,
                "tags": self._context.tags,
            },
        )

    def record_execution(self, record: ExecutionRecord) -> None:
        """Record SQL execution diagnostics.

        Parameters
        ----------
        record
            Execution diagnostics payload.
        """
        if not self.enabled or self._sink is None:
            return
        from serde_schema_registry import ArtifactSpec

        self._sink.record_artifact(
            ArtifactSpec(
                canonical_name="sql_execution",
                description="SQL execution diagnostics artifact.",
            ),
            {
                "session_id": self._context.session_id,
                "operation_id": self._context.operation_id,
                "timestamp": datetime.now().astimezone().isoformat(),
                "sql": record.sql,
                "plan_fingerprint": record.plan_fingerprint,
                "duration_ms": record.duration_ms,
                "rows_produced": record.rows_produced,
                "bytes_scanned": record.bytes_scanned,
                "error": record.error,
                "success": record.error is None,
                "tags": self._context.tags,
            },
        )

    def record_write(self, record: WriteRecord) -> None:
        """Record write operation diagnostics.

        Parameters
        ----------
        record
            Write diagnostics payload.
        """
        if not self.enabled or self._sink is None:
            return
        from serde_schema_registry import ArtifactSpec

        self._sink.record_artifact(
            ArtifactSpec(
                canonical_name="write_operation",
                description="Write operation diagnostics artifact.",
            ),
            {
                "session_id": self._context.session_id,
                "operation_id": self._context.operation_id,
                "timestamp": datetime.now().astimezone().isoformat(),
                "destination": record.destination,
                "format": record.format_,
                "method": record.method,
                "rows_written": record.rows_written,
                "bytes_written": record.bytes_written,
                "partitions": record.partitions,
                "duration_ms": record.duration_ms,
                "sql": record.sql,
                "delta_features": (
                    dict(record.delta_features) if record.delta_features is not None else None
                ),
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
        from serde_schema_registry import ArtifactSpec

        self._sink.record_artifact(
            ArtifactSpec(
                canonical_name="registration",
                description="Registration diagnostics artifact.",
            ),
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

    def record_namespace_action(
        self,
        *,
        action: str,
        name: str,
        database: Mapping[str, Any],
        overwrite: bool,
    ) -> None:
        """Record namespace actions from DataFusion integrations.

        Parameters
        ----------
        action : str
            Action name (create_table, create_view, insert, etc.).
        name : str
            Target object name.
        database : Mapping[str, Any]
            Catalog/database payload for the target.
        overwrite : bool
            Whether the action overwrites existing objects.
        """
        if not self.enabled or self._sink is None:
            return
        from serde_schema_registry import ArtifactSpec

        self._sink.record_artifact(
            ArtifactSpec(
                canonical_name="datafusion_namespace_actions_v1",
                description="DataFusion namespace actions diagnostics artifact.",
            ),
            {
                "session_id": self._context.session_id,
                "operation_id": self._context.operation_id,
                "timestamp": datetime.now().astimezone().isoformat(),
                "action": action,
                "name": name,
                "overwrite": overwrite,
                **dict(database),
                "tags": self._context.tags,
            },
        )


@dataclass(frozen=True)
class DiagnosticsRecorderAdapter:
    """Adapter that routes DiagnosticsSink calls through DiagnosticsRecorder."""

    sink: DiagnosticsSink
    session_id: str
    tags: dict[str, str] = field(default_factory=dict)

    def _recorder(self, operation_id: str) -> DiagnosticsRecorder:
        context = DiagnosticsContext(
            session_id=self.session_id,
            operation_id=operation_id,
            tags=self.tags,
        )
        return DiagnosticsRecorder(self.sink, context)

    def record_artifact(self, name: ArtifactSpec, payload: Mapping[str, Any]) -> None:
        """Record a named artifact via DiagnosticsRecorder."""
        self._recorder(name.canonical_name).record_artifact(name, payload)

    def record_event(self, name: str, properties: Mapping[str, Any]) -> None:
        """Record a single event via DiagnosticsRecorder."""
        self._recorder(name).record_event(name, properties)

    def record_events(self, name: str, rows: Sequence[Mapping[str, Any]]) -> None:
        """Record a batch of events via DiagnosticsRecorder."""
        self._recorder(name).record_events(name, rows)

    def events_snapshot(self) -> dict[str, list[Mapping[str, Any]]]:
        """Return collected event rows from the underlying sink when available.

        Returns:
        -------
        dict[str, list[Mapping[str, Any]]]
            Mapping of event names to collected payloads.
        """
        snapshot = getattr(self.sink, "events_snapshot", None)
        if callable(snapshot):
            return cast("dict[str, list[Mapping[str, Any]]]", snapshot())
        return {}

    def artifacts_snapshot(self) -> dict[str, list[Mapping[str, Any]]]:
        """Return collected artifact payloads from the underlying sink when available.

        Returns:
        -------
        dict[str, list[Mapping[str, Any]]]
            Mapping of artifact names to collected payloads.
        """
        snapshot = getattr(self.sink, "artifacts_snapshot", None)
        if callable(snapshot):
            return cast("dict[str, list[Mapping[str, Any]]]", snapshot())
        return {}

    def __getattr__(self, name: str) -> object:
        """Delegate unknown attributes to the underlying sink.

        Returns:
        -------
        object
            Attribute resolved from the underlying sink.
        """
        return getattr(self.sink, name)


def record_artifact(
    profile: DataFusionRuntimeProfile | None,
    name: ArtifactSpec,
    payload: Mapping[str, Any],
) -> None:
    """Record an artifact directly from a runtime profile.

    Use this when you don't have a DiagnosticsRecorder instance and need
    to record a single artifact directly from a runtime profile.

    Parameters
    ----------
    profile : DataFusionRuntimeProfile | None
        Runtime profile containing diagnostics sink. If None or sink is None,
        no recording occurs.
    name : ArtifactSpec
        Artifact spec identifier.
    payload : dict[str, Any]
        Artifact payload.
    """
    if profile is None or profile.diagnostics.diagnostics_sink is None:
        return
    recorder = recorder_for_profile(profile, operation_id=name.canonical_name)
    if recorder is None:
        return
    recorder.record_artifact(name, payload)


def record_events(
    profile: DataFusionRuntimeProfile | None,
    name: str,
    rows: Sequence[Mapping[str, Any]],
) -> None:
    """Record event rows directly from a runtime profile.

    Parameters
    ----------
    profile : DataFusionRuntimeProfile | None
        Runtime profile containing diagnostics sink. If None or sink is None,
        no recording occurs.
    name : str
        Event type identifier.
    rows : Sequence[Mapping[str, Any]]
        Event payload rows.
    """
    if profile is None or profile.diagnostics.diagnostics_sink is None:
        return
    recorder = recorder_for_profile(profile, operation_id=name)
    if recorder is None:
        return
    recorder.record_events(name, rows)


def record_cache_lineage(
    profile: DataFusionRuntimeProfile | None,
    *,
    summary: Mapping[str, object],
    rows: Sequence[Mapping[str, object]],
) -> None:
    """Record cache lineage summaries and per-node facts.

    Parameters
    ----------
    profile : DataFusionRuntimeProfile | None
        Runtime profile for diagnostics emission.
    summary : Mapping[str, object]
        Summary payload containing run metadata and counts.
    rows : Sequence[Mapping[str, object]]
        Per-node cache lineage rows to emit as events.
    """
    from serde_artifact_specs import HAMILTON_CACHE_LINEAGE_SPEC

    summary_payload = dict(summary)
    record_artifact(profile, HAMILTON_CACHE_LINEAGE_SPEC, summary_payload)
    node_rows = [_normalize_diagnostics_row(row) for row in rows]
    if node_rows:
        record_events(profile, "hamilton_cache_lineage_nodes_v1", node_rows)


def _normalize_diagnostics_row(row: Mapping[str, object]) -> dict[str, Any]:
    normalized: dict[str, Any] = {}
    for key, value in row.items():
        if isinstance(value, Mapping):
            normalized[key] = {
                str(inner_key): inner_value
                for inner_key, inner_value in value.items()
                if isinstance(inner_key, str)
            }
            continue
        if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
            normalized[key] = list(value)
            continue
        normalized[key] = value
    return normalized


def ensure_recorder_sink(
    sink: DiagnosticsSink,
    *,
    session_id: str,
) -> DiagnosticsSink:
    """Wrap a diagnostics sink with a recorder adapter when needed.

    Returns:
    -------
    DiagnosticsSink
        Recorder adapter when wrapping is required.
    """
    if isinstance(sink, DiagnosticsRecorderAdapter):
        return sink
    return DiagnosticsRecorderAdapter(sink=sink, session_id=session_id)


def recorder_for_profile(
    profile: DataFusionRuntimeProfile | None,
    *,
    operation_id: str,
    session_id: str | None = None,
) -> DiagnosticsRecorder | None:
    """Return a DiagnosticsRecorder for a runtime profile.

    Parameters
    ----------
    profile : DataFusionRuntimeProfile | None
        Runtime profile containing diagnostics sink.
    operation_id : str
        Operation identifier for the recorder context.
    session_id : str | None
        Optional session identifier override.

    Returns:
    -------
    DiagnosticsRecorder | None
        Recorder instance when diagnostics are enabled, otherwise ``None``.
    """
    if profile is None or profile.diagnostics.diagnostics_sink is None:
        return None
    resolved_session = session_id or profile.context_cache_key()
    context = DiagnosticsContext(session_id=resolved_session, operation_id=operation_id)
    sink = profile.diagnostics.diagnostics_sink
    return DiagnosticsRecorder(sink, context)


def view_udf_parity_payload(
    *,
    snapshot: Mapping[str, object],
    view_nodes: Sequence[ViewNode],
    ctx: SessionContext | None = None,
) -> dict[str, object]:
    """Return a diagnostics payload describing view/UDF parity.

    Parameters
    ----------
    snapshot
        Rust UDF snapshot used for parity comparison.
    view_nodes
        View nodes containing UDF requirements.
    ctx
        Optional session context for information_schema parity checks.

    Returns:
    -------
    dict[str, object]
        Diagnostics payload describing required/missing UDFs per view.
    """
    from datafusion_engine.udf.runtime import udf_names_from_snapshot

    def _required_udfs(node: ViewNode) -> tuple[str, ...]:
        return tuple(node.required_udfs)

    available = udf_names_from_snapshot(snapshot)
    info_available: set[str] | None = None
    if ctx is not None:
        from datafusion_engine.schema.introspection import SchemaIntrospector

        introspector = SchemaIntrospector(ctx)
        catalog = introspector.function_catalog_snapshot(include_parameters=False)
        info_available = {
            name.lower()
            for row in catalog
            for name in (
                row.get("function_name"),
                row.get("routine_name"),
                row.get("name"),
            )
            if isinstance(name, str)
        }
    rows: list[dict[str, object]] = []
    missing_snapshot_views = 0
    missing_info_views = 0
    for node in view_nodes:
        required = _required_udfs(node)
        missing_snapshot = [name for name in required if name not in available]
        missing_info: list[str] | None = None
        if info_available is not None:
            missing_info = [name for name in required if name.lower() not in info_available]
        if missing_snapshot:
            missing_snapshot_views += 1
        if missing_info:
            missing_info_views += 1
        rows.append(
            {
                "view": node.name,
                "required_udfs": list(required) or None,
                "missing_snapshot_udfs": missing_snapshot or None,
                "missing_information_schema_udfs": missing_info or None,
            }
        )
    return {
        "total_views": len(view_nodes),
        "views_with_requirements": sum(1 for node in view_nodes if _required_udfs(node)),
        "views_missing_snapshot_udfs": missing_snapshot_views,
        "views_missing_information_schema_udfs": missing_info_views,
        "rows": rows,
    }


def view_fingerprint_payload(
    *,
    view_nodes: Sequence[ViewNode],
) -> dict[str, object]:
    """Return a diagnostics payload describing view fingerprints.

    Parameters
    ----------
    view_nodes
        View nodes to fingerprint.

    Returns:
    -------
    dict[str, object]
        Diagnostics payload containing per-view fingerprints.
    """
    rows = [
        {
            "view": node.name,
            "plan_fingerprint": (
                node.plan_bundle.plan_fingerprint if node.plan_bundle is not None else None
            ),
        }
        for node in view_nodes
    ]
    return {
        "total_views": len(view_nodes),
        "rows": rows,
    }


def rust_udf_snapshot_payload(snapshot: Mapping[str, object]) -> dict[str, object]:
    """Return a diagnostics payload summarizing a Rust UDF snapshot.

    Parameters
    ----------
    snapshot
        Rust UDF registry snapshot payload.

    Returns:
    -------
    dict[str, object]
        Summary payload with counts and metadata coverage.
    """
    from datafusion_engine.udf.runtime import rust_udf_snapshot_hash, udf_names_from_snapshot

    def _plugin_manifest() -> Mapping[str, object] | None:
        from datafusion_engine.extensions.plugin_manifest import resolve_plugin_manifest

        return resolve_plugin_manifest("datafusion_ext").manifest

    def _count_seq(key: str) -> int:
        value = snapshot.get(key, ())
        if isinstance(value, (str, bytes)) or not isinstance(value, Sequence):
            return 0
        return len(value)

    def _count_map(key: str) -> int:
        value = snapshot.get(key, {})
        if not isinstance(value, Mapping):
            return 0
        return len(value)

    manifest = _plugin_manifest()
    return {
        "snapshot_hash": rust_udf_snapshot_hash(snapshot),
        "manifest_hash": (
            hash_json_canonical(manifest, str_keys=True) if manifest is not None else None
        ),
        "manifest": manifest,
        "total_udfs": len(udf_names_from_snapshot(snapshot)),
        "scalar_udfs": _count_seq("scalar"),
        "aggregate_udfs": _count_seq("aggregate"),
        "window_udfs": _count_seq("window"),
        "table_udfs": _count_seq("table"),
        "custom_udfs": _count_seq("custom_udfs"),
        "aliases": _count_map("aliases"),
        "signature_inputs": _count_map("signature_inputs"),
        "return_types": _count_map("return_types"),
        "parameter_names": _count_map("parameter_names"),
        "volatility": _count_map("volatility"),
        "rewrite_tags": _count_map("rewrite_tags"),
    }


def otel_diagnostics_sink() -> DiagnosticsSink:
    """Return an OTel-backed diagnostics sink.

    Returns:
    -------
    DiagnosticsSink
        Diagnostics sink that emits OpenTelemetry logs.
    """
    from obs.otel.logs import OtelDiagnosticsSink

    return OtelDiagnosticsSink()

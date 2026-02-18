"""Diagnostics sink helpers for runtime and rule events."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import datafusion_ext as _datafusion_ext
from obs.otel.logs import emit_diagnostics_event
from obs.otel.metrics import record_artifact_count
from obs.ports import DiagnosticsPort
from serde_artifact_specs import (
    DATAFUSION_PREPARED_STATEMENTS_SPEC,
    SEMANTIC_QUALITY_ARTIFACT_SPEC,
)
from serde_msgspec import StructBaseCompat

if TYPE_CHECKING:
    from serde_schema_registry import ArtifactSpec

_ = _datafusion_ext


@dataclass
class DiagnosticsCollector:
    """Collect diagnostics events and artifacts in-memory."""

    events: dict[str, list[Mapping[str, object]]] = field(default_factory=dict)
    artifacts: dict[str, list[Mapping[str, object]]] = field(default_factory=dict)

    def record_events(self, name: str, rows: Sequence[Mapping[str, object]]) -> None:
        """Append event rows under a logical name."""
        bucket = self.events.setdefault(name, [])
        normalized = [dict(row) for row in rows]
        bucket.extend(normalized)
        for row in normalized:
            emit_diagnostics_event(name, payload=row, event_kind="event")
        record_artifact_count(name, status="ok", attributes={"artifact.type": "event"})

    def record_artifact(self, name: ArtifactSpec, payload: Mapping[str, object]) -> None:
        """Append an artifact payload under a logical name."""
        resolved = name.canonical_name
        bucket = self.artifacts.setdefault(resolved, [])
        normalized = dict(payload)
        bucket.append(normalized)
        emit_diagnostics_event(resolved, payload=normalized, event_kind="artifact")
        record_artifact_count(resolved, status="ok", attributes={"artifact.type": "artifact"})

    def record_event(self, name: str, properties: Mapping[str, object]) -> None:
        """Append an event payload under a logical name."""
        bucket = self.events.setdefault(name, [])
        normalized = dict(properties)
        bucket.append(normalized)
        emit_diagnostics_event(name, payload=normalized, event_kind="event")
        record_artifact_count(name, status="ok", attributes={"artifact.type": "event"})

    def events_snapshot(self) -> dict[str, list[Mapping[str, object]]]:
        """Return a shallow copy of collected event rows."""
        return {name: list(rows) for name, rows in self.events.items()}

    def artifacts_snapshot(self) -> dict[str, list[Mapping[str, object]]]:
        """Return a shallow copy of collected artifacts."""
        return {name: list(rows) for name, rows in self.artifacts.items()}


class PreparedStatementSpec(StructBaseCompat, frozen=True):
    """Prepared statement metadata for diagnostics reporting."""

    name: str
    sql: str
    param_types: tuple[str, ...]

    def payload(self) -> Mapping[str, object]:
        """Return a JSON-ready payload for diagnostics sinks."""
        return {
            "name": self.name,
            "sql": self.sql,
            "param_types": list(self.param_types),
        }


class SemanticQualityArtifact(StructBaseCompat, frozen=True):
    """Summary payload for a semantic diagnostics artifact."""

    name: str
    row_count: int
    schema_hash: str | None
    artifact_uri: str | None
    run_id: str | None

    def payload(self) -> Mapping[str, object]:
        """Return a JSON-ready payload for diagnostics sinks."""
        return {
            "name": self.name,
            "row_count": self.row_count,
            "schema_hash": self.schema_hash,
            "artifact_uri": self.artifact_uri,
            "run_id": self.run_id,
        }


def prepared_statement_hook(
    sink: DiagnosticsPort,
) -> Callable[[PreparedStatementSpec], None]:
    """Return a hook that records prepared statements in diagnostics."""

    def _hook(spec: PreparedStatementSpec) -> None:
        sink.record_artifact(DATAFUSION_PREPARED_STATEMENTS_SPEC, spec.payload())

    return _hook


def record_semantic_quality_artifact(
    sink: DiagnosticsPort,
    *,
    artifact: SemanticQualityArtifact,
) -> None:
    """Record a semantic quality artifact payload."""
    sink.record_artifact(SEMANTIC_QUALITY_ARTIFACT_SPEC, artifact.payload())


def record_semantic_quality_events(
    sink: DiagnosticsPort,
    *,
    name: str,
    rows: Sequence[Mapping[str, object]],
) -> None:
    """Record semantic quality event rows."""
    sink.record_events(name, rows)


__all__ = [
    "DiagnosticsCollector",
    "PreparedStatementSpec",
    "SemanticQualityArtifact",
    "prepared_statement_hook",
    "record_semantic_quality_artifact",
    "record_semantic_quality_events",
]

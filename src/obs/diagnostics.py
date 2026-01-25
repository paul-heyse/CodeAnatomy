"""Diagnostics sink helpers for runtime and rule events."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field

from datafusion_engine.diagnostics import ensure_recorder_sink
from serde_msgspec import StructBase


@dataclass
class DiagnosticsCollector:
    """Collect diagnostics events and artifacts in-memory."""

    events: dict[str, list[Mapping[str, object]]] = field(default_factory=dict)
    artifacts: dict[str, list[Mapping[str, object]]] = field(default_factory=dict)
    metrics: list[tuple[str, float, dict[str, str]]] = field(default_factory=list)

    def record_events(self, name: str, rows: Sequence[Mapping[str, object]]) -> None:
        """Append event rows under a logical name."""
        bucket = self.events.setdefault(name, [])
        bucket.extend([dict(row) for row in rows])

    def record_artifact(self, name: str, payload: Mapping[str, object]) -> None:
        """Append an artifact payload under a logical name."""
        bucket = self.artifacts.setdefault(name, [])
        bucket.append(dict(payload))

    def record_event(self, name: str, properties: Mapping[str, object]) -> None:
        """Append an event payload under a logical name."""
        bucket = self.events.setdefault(name, [])
        bucket.append(dict(properties))

    def record_metric(self, name: str, value: float, tags: Mapping[str, str]) -> None:
        """Record a metric value."""
        self.metrics.append((name, value, dict(tags)))

    def events_snapshot(self) -> dict[str, list[Mapping[str, object]]]:
        """Return a shallow copy of collected event rows.

        Returns
        -------
        dict[str, list[Mapping[str, object]]]
            Mapping of event names to collected rows.
        """
        return {name: list(rows) for name, rows in self.events.items()}

    def artifacts_snapshot(self) -> dict[str, list[Mapping[str, object]]]:
        """Return a shallow copy of collected artifacts.

        Returns
        -------
        dict[str, list[Mapping[str, object]]]
            Mapping of artifact names to collected payloads.
        """
        return {name: list(rows) for name, rows in self.artifacts.items()}


class PreparedStatementSpec(StructBase, frozen=True):
    """Prepared statement metadata for diagnostics reporting."""

    name: str
    sql: str
    param_types: tuple[str, ...]

    def payload(self) -> Mapping[str, object]:
        """Return a JSON-ready payload for diagnostics sinks.

        Returns
        -------
        Mapping[str, object]
            JSON-ready payload describing the prepared statement.
        """
        return {
            "name": self.name,
            "sql": self.sql,
            "param_types": list(self.param_types),
        }


def prepared_statement_hook(
    sink: DiagnosticsCollector,
) -> Callable[[PreparedStatementSpec], None]:
    """Return a hook that records prepared statements in diagnostics.

    Returns
    -------
    Callable[[PreparedStatementSpec], None]
        Hook that records prepared statement metadata.
    """

    def _hook(spec: PreparedStatementSpec) -> None:
        recorder_sink = ensure_recorder_sink(sink, session_id="obs")
        recorder_sink.record_artifact("datafusion_prepared_statements_v1", spec.payload())

    return _hook


__all__ = ["DiagnosticsCollector", "PreparedStatementSpec", "prepared_statement_hook"]

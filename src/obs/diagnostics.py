"""Diagnostics sink helpers for runtime and rule events."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field


@dataclass
class DiagnosticsCollector:
    """Collect diagnostics events and artifacts in-memory."""

    events: dict[str, list[Mapping[str, object]]] = field(default_factory=dict)
    artifacts: dict[str, list[Mapping[str, object]]] = field(default_factory=dict)

    def record_events(self, name: str, rows: Sequence[Mapping[str, object]]) -> None:
        """Append event rows under a logical name."""
        bucket = self.events.setdefault(name, [])
        bucket.extend([dict(row) for row in rows])

    def record_artifact(self, name: str, payload: Mapping[str, object]) -> None:
        """Append an artifact payload under a logical name."""
        bucket = self.artifacts.setdefault(name, [])
        bucket.append(dict(payload))

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


__all__ = ["DiagnosticsCollector"]

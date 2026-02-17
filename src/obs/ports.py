"""Observation port protocols for engine-agnostic consumers."""

from __future__ import annotations

from typing import Protocol

import pyarrow as pa


class DiagnosticsPort(Protocol):
    """Port for recording diagnostics events and artifacts."""

    def record_event(self, name: str, properties: dict[str, object]) -> None:
        """Record one diagnostics event payload."""
        ...

    def record_events(self, name: str, rows: list[dict[str, object]]) -> None:
        """Record multiple diagnostics event payloads."""
        ...


class MetricsSchemaPort(Protocol):
    """Port for schema-aware metrics payload creation."""

    def schema_to_dict(self, schema: pa.Schema) -> dict[str, object]:
        """Convert an Arrow schema into a serializable mapping payload."""
        ...

    def empty_table(self, schema: pa.Schema) -> pa.Table:
        """Build an empty table for the provided schema."""
        ...

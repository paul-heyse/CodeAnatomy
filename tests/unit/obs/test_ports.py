"""Unit tests for observation port protocols."""

from __future__ import annotations

from typing import cast

import pyarrow as pa

from obs.ports import DiagnosticsPort, MetricsSchemaPort


class _DiagnosticsImpl:
    def __init__(self) -> None:
        self.events: list[tuple[str, dict[str, object]]] = []

    def record_event(self, name: str, properties: dict[str, object]) -> None:
        self.events.append((name, properties))

    def record_events(self, name: str, rows: list[dict[str, object]]) -> None:
        for row in rows:
            self.events.append((name, row))


class _MetricsSchemaImpl:
    @staticmethod
    def schema_to_dict(schema: pa.Schema) -> dict[str, object]:
        return {"fields": list(schema.names)}

    @staticmethod
    def empty_table(schema: pa.Schema) -> pa.Table:
        return pa.table(
            {
                name: pa.array([], type=field.type)
                for name, field in zip(schema.names, schema, strict=False)
            }
        )


def test_diagnostics_port_protocol_compatibility() -> None:
    """Diagnostics protocol accepts single and batched event emissions."""
    impl = cast("DiagnosticsPort", _DiagnosticsImpl())
    impl.record_event("evt", {"a": 1})
    impl.record_events("evt_many", [{"b": 2}])


def test_metrics_schema_port_protocol_compatibility() -> None:
    """Metrics schema protocol implementation returns expected payload/table."""
    impl = cast("MetricsSchemaPort", _MetricsSchemaImpl())
    schema = pa.schema([("a", pa.int64())])
    payload = impl.schema_to_dict(schema)
    assert payload == {"fields": ["a"]}
    assert impl.empty_table(schema).num_rows == 0

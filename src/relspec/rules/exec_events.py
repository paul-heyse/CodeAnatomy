"""Rule execution event capture helpers."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Protocol

import pyarrow as pa

from arrowdsl.core.interop import TableLike
from arrowdsl.schema.serialization import schema_fingerprint

RULE_EXEC_EVENTS_V1 = pa.schema(
    [
        pa.field("rule_name", pa.string(), nullable=False),
        pa.field("output_dataset", pa.string(), nullable=False),
        pa.field("rows_out", pa.int64(), nullable=True),
        pa.field("schema_fingerprint", pa.string(), nullable=True),
        pa.field("duration_ms", pa.float64(), nullable=True),
    ],
    metadata={b"schema_name": b"relspec_rule_exec_events_v1"},
)


@dataclass(frozen=True)
class RuleExecutionEvent:
    """Execution metrics for a single rule."""

    rule_name: str
    output_dataset: str
    rows_out: int | None = None
    schema_fingerprint: str | None = None
    duration_ms: float | None = None

    def to_row(self) -> dict[str, object]:
        """Return a row mapping for this event.

        Returns
        -------
        dict[str, object]
            Row mapping for this execution event.
        """
        return {
            "rule_name": self.rule_name,
            "output_dataset": self.output_dataset,
            "rows_out": self.rows_out,
            "schema_fingerprint": self.schema_fingerprint,
            "duration_ms": self.duration_ms,
        }


def rule_execution_event_from_table(
    *,
    rule_name: str,
    output_dataset: str,
    table: TableLike,
    duration_ms: float | None,
) -> RuleExecutionEvent:
    """Build a rule execution event from an output table.

    Returns
    -------
    RuleExecutionEvent
        Execution event populated from the output table.
    """
    return RuleExecutionEvent(
        rule_name=rule_name,
        output_dataset=output_dataset,
        rows_out=int(table.num_rows),
        schema_fingerprint=schema_fingerprint(table.schema),
        duration_ms=duration_ms,
    )


def rule_execution_events_table(events: Sequence[RuleExecutionEvent]) -> pa.Table:
    """Return an Arrow table for rule execution events.

    Returns
    -------
    pyarrow.Table
        Arrow table of rule execution events.
    """
    return pa.Table.from_pylist([event.to_row() for event in events], schema=RULE_EXEC_EVENTS_V1)


class RuleExecutionObserver(Protocol):
    """Protocol for observing rule execution events."""

    def record(self, event: RuleExecutionEvent) -> None:
        """Record a rule execution event."""


@dataclass
class RuleExecutionEventCollector:
    """Collect rule execution events during execution."""

    events: list[RuleExecutionEvent] = field(default_factory=list)

    def record(self, event: RuleExecutionEvent) -> None:
        """Record an execution event."""
        self.events.append(event)

    def table(self) -> pa.Table:
        """Return the collected events as a table.

        Returns
        -------
        pyarrow.Table
            Arrow table with collected execution events.
        """
        return rule_execution_events_table(self.events)


__all__ = [
    "RULE_EXEC_EVENTS_V1",
    "RuleExecutionEvent",
    "RuleExecutionEventCollector",
    "RuleExecutionObserver",
    "rule_execution_event_from_table",
    "rule_execution_events_table",
]

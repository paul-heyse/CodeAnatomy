"""DataFusion run envelope tracking with correlation IDs."""

from __future__ import annotations

import time
import uuid
from collections.abc import Mapping
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from collections.abc import Iterator


class DiagnosticsSink(Protocol):
    """Protocol for diagnostics recording backends."""

    def record_events(self, name: str, rows: list[Mapping[str, object]]) -> None:
        """Record event rows under a logical name."""
        ...

    def record_artifact(self, name: str, payload: Mapping[str, object]) -> None:
        """Record an artifact payload under a logical name."""
        ...


@dataclass
class DataFusionRun:
    """Run envelope for DataFusion execution with correlation ID."""

    run_id: str
    label: str
    start_time_unix_ms: int
    end_time_unix_ms: int | None = None
    status: str = "running"
    metadata: dict[str, object] = field(default_factory=dict)

    def payload(self) -> dict[str, object]:
        """Return a JSON-ready payload for diagnostics sinks.

        Returns
        -------
        dict[str, object]
            JSON-ready payload describing the run envelope.
        """
        return {
            "run_id": self.run_id,
            "label": self.label,
            "start_time_unix_ms": self.start_time_unix_ms,
            "end_time_unix_ms": self.end_time_unix_ms,
            "status": self.status,
            "duration_ms": (
                self.end_time_unix_ms - self.start_time_unix_ms
                if self.end_time_unix_ms is not None
                else None
            ),
            "metadata": dict(self.metadata),
        }


def start_run(
    *,
    label: str,
    sink: DiagnosticsSink | None = None,
    metadata: Mapping[str, object] | None = None,
) -> DataFusionRun:
    """Start a new DataFusion run with correlation ID.

    Parameters
    ----------
    label:
        Human-readable label for the run (e.g., "incremental_rebuild", "full_query").
    sink:
        Optional diagnostics sink for recording run lifecycle events.
    metadata:
        Optional metadata to attach to the run envelope.

    Returns
    -------
    DataFusionRun
        Run envelope with a unique run_id and start timestamp.
    """
    run_id = str(uuid.uuid4())
    start_time = int(time.time() * 1000)
    run = DataFusionRun(
        run_id=run_id,
        label=label,
        start_time_unix_ms=start_time,
        metadata=dict(metadata) if metadata else {},
    )
    if sink is not None:
        sink.record_artifact("datafusion_run_started_v1", run.payload())
    return run


def finish_run(
    run: DataFusionRun,
    *,
    sink: DiagnosticsSink | None = None,
    status: str = "completed",
    metadata: Mapping[str, object] | None = None,
) -> DataFusionRun:
    """Finish a DataFusion run and record completion.

    Parameters
    ----------
    run:
        Run envelope to finish.
    sink:
        Optional diagnostics sink for recording run completion.
    status:
        Status string for the run (default: "completed").
    metadata:
        Optional additional metadata to merge into the run envelope.

    Returns
    -------
    DataFusionRun
        Updated run envelope with end timestamp and status.
    """
    end_time = int(time.time() * 1000)
    run.end_time_unix_ms = end_time
    run.status = status
    if metadata:
        run.metadata.update(metadata)
    if sink is not None:
        sink.record_artifact("datafusion_run_finished_v1", run.payload())
    return run


@contextmanager
def tracked_run(
    *,
    label: str,
    sink: DiagnosticsSink | None = None,
    metadata: Mapping[str, object] | None = None,
) -> Iterator[DataFusionRun]:
    """Context manager for automatic run lifecycle tracking.

    Parameters
    ----------
    label:
        Human-readable label for the run.
    sink:
        Optional diagnostics sink for recording run lifecycle events.
    metadata:
        Optional metadata to attach to the run envelope.

    Yields
    ------
    DataFusionRun
        Run envelope with correlation ID.

    Examples
    --------
    >>> with tracked_run(label="query_execution", sink=diagnostics) as run:
    ...     df = ctx.sql("SELECT * FROM table")
    ...     result = df.to_arrow_table()
    """
    run = start_run(label=label, sink=sink, metadata=metadata)
    try:
        yield run
        finish_run(run, sink=sink, status="completed")
    except Exception as exc:
        finish_run(
            run,
            sink=sink,
            status="failed",
            metadata={"error": str(exc), "error_type": type(exc).__name__},
        )
        raise


__all__ = [
    "DataFusionRun",
    "DiagnosticsSink",
    "finish_run",
    "start_run",
    "tracked_run",
]

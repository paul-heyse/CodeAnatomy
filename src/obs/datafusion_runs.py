"""DataFusion run envelope tracking with correlation IDs."""

from __future__ import annotations

import time
from collections.abc import Mapping
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from utils.uuid_factory import uuid7_str

if TYPE_CHECKING:
    from collections.abc import Iterator

    from storage.deltalake.delta import IdempotentWriteOptions

from datafusion_engine.diagnostics import DiagnosticsSink, ensure_recorder_sink


@dataclass
class DataFusionRun:
    """Run envelope for DataFusion execution with correlation ID.

    Tracks both run lifecycle (start/end times, status) and commit sequence
    for idempotent Delta writes.
    """

    run_id: str
    label: str
    start_time_unix_ms: int
    end_time_unix_ms: int | None = None
    status: str = "running"
    metadata: dict[str, object] = field(default_factory=dict)
    _commit_sequence: int = 0

    @property
    def commit_sequence(self) -> int:
        """Current commit sequence number for idempotent writes."""
        return self._commit_sequence

    def next_commit_version(self) -> tuple[IdempotentWriteOptions, DataFusionRun]:
        """Return idempotent options and updated run for next commit.

        Returns
        -------
        tuple[IdempotentWriteOptions, DataFusionRun]
            Tuple of (idempotent_options, updated_run) where the run has
            an incremented commit sequence.

        Examples
        --------
        >>> run = start_run(label="test")
        >>> options, run = run.next_commit_version()
        >>> print(options.version)
        0
        >>> options, run = run.next_commit_version()
        >>> print(options.version)
        1
        """
        from storage.deltalake.delta import IdempotentWriteOptions

        options = IdempotentWriteOptions(
            app_id=self.run_id,
            version=self._commit_sequence,
        )
        # Return updated run with incremented sequence
        updated = DataFusionRun(
            run_id=self.run_id,
            label=self.label,
            start_time_unix_ms=self.start_time_unix_ms,
            end_time_unix_ms=self.end_time_unix_ms,
            status=self.status,
            metadata=self.metadata.copy(),
            _commit_sequence=self._commit_sequence + 1,
        )
        return options, updated

    def with_sequence(self, sequence: int) -> DataFusionRun:
        """Return a run with a specific commit sequence.

        Useful for resuming from a known checkpoint.

        Parameters
        ----------
        sequence : int
            Commit sequence number to set.

        Returns
        -------
        DataFusionRun
            New run instance with the specified commit sequence.
        """
        return DataFusionRun(
            run_id=self.run_id,
            label=self.label,
            start_time_unix_ms=self.start_time_unix_ms,
            end_time_unix_ms=self.end_time_unix_ms,
            status=self.status,
            metadata=self.metadata.copy(),
            _commit_sequence=sequence,
        )

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
            "commit_sequence": self._commit_sequence,
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
    run_id = uuid7_str()
    start_time = int(time.time() * 1000)
    run = DataFusionRun(
        run_id=run_id,
        label=label,
        start_time_unix_ms=start_time,
        metadata=dict(metadata) if metadata else {},
    )
    if sink is not None:
        recorder_sink = ensure_recorder_sink(sink, session_id=run_id)
        recorder_sink.record_artifact("datafusion_run_started_v1", run.payload())
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
        recorder_sink = ensure_recorder_sink(sink, session_id=run.run_id)
        recorder_sink.record_artifact("datafusion_run_finished_v1", run.payload())
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


def create_run_context(
    *,
    label: str = "idempotent_run",
    run_id: str | None = None,
    sink: DiagnosticsSink | None = None,
    metadata: Mapping[str, object] | None = None,
) -> DataFusionRun:
    """Create a run context for idempotent writes.

    Parameters
    ----------
    label : str
        Human-readable label for the run (default: "idempotent_run").
    run_id : str | None
        Optional existing run ID. If None, generates a new one.
    sink : DiagnosticsSink | None
        Optional diagnostics sink for recording run lifecycle events.
    metadata : Mapping[str, object] | None
        Optional metadata to attach to the run envelope.

    Returns
    -------
    DataFusionRun
        Run context for tracking commits.

    Examples
    --------
    >>> # Create a new run context
    >>> run = create_run_context(label="data_pipeline")
    >>> # Get idempotent write options for first commit
    >>> options, run = run.next_commit_version()
    >>> # Use options to populate IbisDeltaWriteOptions(app_id=..., version=...)
    """
    if run_id is not None:
        # Resume existing run
        return DataFusionRun(
            run_id=run_id,
            label=label,
            start_time_unix_ms=int(time.time() * 1000),
            metadata=dict(metadata) if metadata else {},
        )
    # Create new run
    return start_run(label=label, sink=sink, metadata=metadata)


__all__ = [
    "DataFusionRun",
    "DiagnosticsSink",
    "create_run_context",
    "finish_run",
    "start_run",
    "tracked_run",
]

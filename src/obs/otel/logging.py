"""Logging helpers for trace correlation."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, cast

from opentelemetry import trace

if TYPE_CHECKING:

    class _TraceRecord(logging.LogRecord):
        trace_id: str | None
        span_id: str | None


class TraceContextFilter(logging.Filter):
    """Attach trace/span IDs to log records when available."""

    @staticmethod
    def filter(record: logging.LogRecord) -> bool:
        """Inject trace/span IDs into the log record when available.

        Returns
        -------
        bool
            True to keep the log record.
        """
        span = trace.get_current_span()
        context = span.get_span_context()
        trace_record = cast("_TraceRecord", record)
        if context is None or not context.is_valid:
            trace_record.trace_id = None
            trace_record.span_id = None
            return True
        trace_record.trace_id = f"{context.trace_id:032x}"
        trace_record.span_id = f"{context.span_id:016x}"
        return True


def install_trace_context_filter(logger: logging.Logger | None = None) -> None:
    """Install the trace context filter on the provided logger."""
    target = logger or logging.getLogger()
    if not target.handlers:
        target.addFilter(TraceContextFilter())
        return
    for handler in target.handlers:
        for existing in handler.filters:
            if isinstance(existing, TraceContextFilter):
                return
        handler.addFilter(TraceContextFilter())


__all__ = ["TraceContextFilter", "install_trace_context_filter"]

"""Logging helpers for trace correlation."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, cast

from opentelemetry import trace

if TYPE_CHECKING:

    class _TraceRecord(logging.LogRecord):
        trace_id: str | None
        span_id: str | None


TRACE_LOG_FORMAT = (
    "%(asctime)s %(levelname)s [trace_id=%(trace_id)s span_id=%(span_id)s] %(name)s: %(message)s"
)


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


class TraceContextFormatter(logging.Formatter):
    """Formatter that ensures trace/span IDs are present on log records."""

    def format(self, record: logging.LogRecord) -> str:
        """Format a log record with ensured trace/span fields.

        Returns
        -------
        str
            Formatted log record string.
        """
        if not hasattr(record, "trace_id"):
            record.trace_id = None
        if not hasattr(record, "span_id"):
            record.span_id = None
        return super().format(record)


def _formatter_includes_trace(formatter: logging.Formatter | None) -> bool:
    if formatter is None:
        return False
    style = getattr(formatter, "_style", None)
    fmt = getattr(style, "_fmt", None)
    if not isinstance(fmt, str):
        return False
    return "trace_id" in fmt and "span_id" in fmt


def apply_trace_context_formatter(
    logger: logging.Logger | None = None,
    *,
    force: bool = False,
    fmt: str | None = None,
    datefmt: str | None = None,
) -> None:
    """Ensure log handlers include trace/span IDs in their formatter."""
    target = logger or logging.getLogger()
    formatter = TraceContextFormatter(fmt or TRACE_LOG_FORMAT, datefmt=datefmt)
    if not target.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(formatter)
        target.addHandler(handler)
        return
    for handler in target.handlers:
        if handler.__class__.__name__ == "LoggingHandler":
            continue
        if not force and _formatter_includes_trace(handler.formatter):
            continue
        handler.setFormatter(formatter)


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


__all__ = [
    "TRACE_LOG_FORMAT",
    "TraceContextFilter",
    "TraceContextFormatter",
    "apply_trace_context_formatter",
    "install_trace_context_filter",
]

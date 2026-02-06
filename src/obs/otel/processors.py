"""Custom OpenTelemetry processors for CodeAnatomy."""

from __future__ import annotations

from opentelemetry.sdk._logs import LogRecordProcessor, ReadWriteLogRecord
from opentelemetry.sdk.trace import ReadableSpan, Span, SpanProcessor

from obs.otel.attributes import normalize_log_attributes
from obs.otel.run_context import get_run_id


class RunIdSpanProcessor(SpanProcessor):
    """Attach run_id to every started span when available."""

    _RUN_ID_KEY = "codeanatomy.run_id"

    def on_start(self, span: Span, parent_context: object | None = None) -> None:
        """Handle span start events for run_id tagging.

        Parameters
        ----------
        span
            Span being started.
        parent_context
            Optional parent context for the span.
        """
        _ = parent_context
        run_id = get_run_id()
        if run_id:
            span.set_attribute(self._RUN_ID_KEY, run_id)

    def on_end(self, span: ReadableSpan) -> None:
        """Handle span end events.

        Parameters
        ----------
        span
            Span being ended.
        """
        _ = self._RUN_ID_KEY
        _ = span

    def shutdown(self) -> None:
        """Shutdown the span processor."""
        _ = self._RUN_ID_KEY

    def force_flush(self, timeout_millis: int = 30000) -> bool:
        """Flush any buffered spans.

        Parameters
        ----------
        timeout_millis
            Timeout in milliseconds.

        Returns:
        -------
        bool
            True if flush succeeded.
        """
        _ = self._RUN_ID_KEY
        _ = timeout_millis
        return True


class RunIdLogRecordProcessor(LogRecordProcessor):
    """Attach run_id to every log record when available."""

    _RUN_ID_KEY = "codeanatomy.run_id"

    def on_emit(self, log_record: ReadWriteLogRecord) -> None:
        """Handle log record emission and attach run_id.

        Parameters
        ----------
        log_record
            Log record being emitted.
        """
        run_id = get_run_id()
        if not run_id:
            return
        attributes = log_record.log_record.attributes
        if attributes is None:
            log_record.log_record.attributes = {self._RUN_ID_KEY: run_id}
            return
        updated = dict(attributes)
        updated[self._RUN_ID_KEY] = run_id
        log_record.log_record.attributes = updated

    def shutdown(self) -> None:
        """Shutdown the log record processor."""
        _ = self._RUN_ID_KEY

    def force_flush(self, timeout_millis: int = 30000) -> bool:
        """Flush any buffered log records.

        Parameters
        ----------
        timeout_millis
            Timeout in milliseconds.

        Returns:
        -------
        bool
            True if flush succeeded.
        """
        _ = self._RUN_ID_KEY
        _ = timeout_millis
        return True


class LogRecordAttributeLimitProcessor(LogRecordProcessor):
    """Normalize and limit log record attributes before export."""

    _RUN_ID_KEY = "codeanatomy.run_id"

    def on_emit(self, log_record: ReadWriteLogRecord) -> None:
        """Normalize log record attributes using configured limits."""
        _ = self
        attributes = log_record.log_record.attributes
        if attributes is None:
            return
        run_id = attributes.get(self._RUN_ID_KEY)
        normalized = normalize_log_attributes(attributes)
        if run_id is not None:
            normalized[self._RUN_ID_KEY] = str(run_id)
        log_record.log_record.attributes = normalized

    def shutdown(self) -> None:
        """Shutdown the log record processor."""
        _ = self

    def force_flush(self, timeout_millis: int = 30000) -> bool:
        """Flush any buffered log records.

        Parameters
        ----------
        timeout_millis
            Timeout in milliseconds.

        Returns:
        -------
        bool
            True if flush succeeded.
        """
        _ = self
        _ = timeout_millis
        return True


__all__ = [
    "LogRecordAttributeLimitProcessor",
    "RunIdLogRecordProcessor",
    "RunIdSpanProcessor",
]

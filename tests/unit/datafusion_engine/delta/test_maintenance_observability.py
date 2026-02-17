"""Tests for delta maintenance observability behavior."""

from __future__ import annotations

from types import TracebackType
from typing import Self

import pytest

from datafusion_engine.delta import maintenance


class _Span:
    def __init__(self) -> None:
        self.attributes: dict[str, object] = {}

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        _ = (exc_type, exc, tb)

    def set_attribute(self, key: str, value: object) -> None:
        self.attributes[key] = value


class _Tracer:
    def __init__(self, span: _Span) -> None:
        self._span = span

    def start_as_current_span(self, _name: str) -> _Span:
        return self._span


def test_run_maintenance_operation_records_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Maintenance helper records success metrics and span attributes."""
    span = _Span()
    metric_calls: list[tuple[str, str, float]] = []

    monkeypatch.setattr(maintenance, "maintenance_tracer", lambda: _Tracer(span))
    monkeypatch.setattr(
        maintenance,
        "record_delta_maintenance_run",
        lambda *, operation, status, duration_s: metric_calls.append(
            (operation, status, duration_s)
        ),
    )

    run_maintenance_operation = maintenance.__dict__["_run_maintenance_operation"]
    result = run_maintenance_operation(
        operation="optimize",
        table_uri="s3://bucket/table",
        execute=lambda: {"status": "ok"},
    )

    assert result == {"status": "ok"}
    assert metric_calls
    assert metric_calls[0][0:2] == ("optimize", "ok")
    assert span.attributes["delta.operation"] == "optimize"
    assert span.attributes["status"] == "ok"


def test_run_maintenance_operation_records_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Maintenance helper records error metrics and status attributes."""
    span = _Span()
    metric_calls: list[tuple[str, str, float]] = []

    monkeypatch.setattr(maintenance, "maintenance_tracer", lambda: _Tracer(span))
    monkeypatch.setattr(
        maintenance,
        "record_delta_maintenance_run",
        lambda *, operation, status, duration_s: metric_calls.append(
            (operation, status, duration_s)
        ),
    )

    with pytest.raises(ValueError, match="boom"):
        maintenance.__dict__["_run_maintenance_operation"](
            operation="vacuum",
            table_uri="s3://bucket/table",
            execute=lambda: (_ for _ in ()).throw(ValueError("boom")),
        )

    assert metric_calls
    assert metric_calls[0][0:2] == ("vacuum", "error")
    assert span.attributes["status"] == "error"

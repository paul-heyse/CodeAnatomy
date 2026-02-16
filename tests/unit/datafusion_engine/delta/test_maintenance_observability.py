# ruff: noqa: D100, D103, ANN001, PT018, SLF001, PYI034
from __future__ import annotations

import pytest

from datafusion_engine.delta import maintenance


class _Span:
    def __init__(self) -> None:
        self.attributes: dict[str, object] = {}

    def __enter__(self) -> _Span:
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def set_attribute(self, key: str, value: object) -> None:
        self.attributes[key] = value


class _Tracer:
    def __init__(self, span: _Span) -> None:
        self._span = span

    def start_as_current_span(self, _name: str) -> _Span:
        return self._span


def test_run_maintenance_operation_records_success(monkeypatch) -> None:
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

    result = maintenance._run_maintenance_operation(
        operation="optimize",
        table_uri="s3://bucket/table",
        execute=lambda: {"status": "ok"},
    )

    assert result == {"status": "ok"}
    assert metric_calls and metric_calls[0][0:2] == ("optimize", "ok")
    assert span.attributes["delta.operation"] == "optimize"
    assert span.attributes["status"] == "ok"


def test_run_maintenance_operation_records_error(monkeypatch) -> None:
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
        maintenance._run_maintenance_operation(
            operation="vacuum",
            table_uri="s3://bucket/table",
            execute=lambda: (_ for _ in ()).throw(ValueError("boom")),
        )

    assert metric_calls and metric_calls[0][0:2] == ("vacuum", "error")
    assert span.attributes["status"] == "error"

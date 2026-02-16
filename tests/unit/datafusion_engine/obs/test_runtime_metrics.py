# ruff: noqa: D100, D103, ANN001, INP001
from __future__ import annotations

import obs.datafusion_engine_runtime_metrics as runtime_metrics


class _Counter:
    def __init__(self) -> None:
        self.calls: list[tuple[int, dict[str, str]]] = []

    def add(self, value: int, attrs: dict[str, str]) -> None:
        self.calls.append((value, dict(attrs)))


class _Histogram:
    def __init__(self) -> None:
        self.calls: list[tuple[float, dict[str, str]]] = []

    def record(self, value: float, attrs: dict[str, str]) -> None:
        self.calls.append((value, dict(attrs)))


def test_record_delta_maintenance_run_emits_counter_and_histogram(monkeypatch) -> None:
    counter = _Counter()
    histogram = _Histogram()
    monkeypatch.setattr(runtime_metrics, "_DELTA_MAINTENANCE_RUNS", counter)
    monkeypatch.setattr(runtime_metrics, "_DELTA_MAINTENANCE_DURATION", histogram)

    runtime_metrics.record_delta_maintenance_run(
        operation="optimize",
        status="ok",
        duration_s=1.25,
        extra_attributes={"dataset": "foo"},
    )

    assert counter.calls == [(1, {"operation": "optimize", "status": "ok", "dataset": "foo"})]
    assert histogram.calls == [(1.25, {"operation": "optimize", "status": "ok", "dataset": "foo"})]


def test_maintenance_tracer_returns_tracer() -> None:
    tracer = runtime_metrics.maintenance_tracer()
    assert hasattr(tracer, "start_as_current_span")

"""Bridge RunResult metrics to OTel instruments.

Extract per-operator timings and execution metrics from the Rust engine's
RunResult and record them via the OTel metrics registry.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


def record_engine_metrics(run_result: dict[str, object]) -> None:
    """Extract metrics from RunResult and record to OTel instruments.

    Parameters
    ----------
    run_result
        The raw RunResult envelope from the Rust engine.
    """
    from obs.otel.metrics import record_stage_duration, record_task_duration

    trace_summary = run_result.get("trace_metrics_summary")
    if isinstance(trace_summary, dict):
        elapsed_nanos = trace_summary.get("elapsed_compute_nanos")
        if isinstance(elapsed_nanos, (int, float)) and elapsed_nanos > 0:
            elapsed_seconds = float(elapsed_nanos) / 1_000_000_000
            record_stage_duration("engine_execute", elapsed_seconds, status="ok")

        operator_metrics = trace_summary.get("operator_metrics")
        if isinstance(operator_metrics, (list, tuple)):
            for op_metric in operator_metrics:
                if not isinstance(op_metric, dict):
                    continue
                op_name = op_metric.get("operator_name", "unknown")
                op_elapsed = op_metric.get("elapsed_nanos", 0)
                if isinstance(op_elapsed, (int, float)) and op_elapsed > 0:
                    record_task_duration(
                        str(op_name),
                        float(op_elapsed) / 1_000_000_000,
                        status="ok",
                    )

    warnings = run_result.get("warnings")
    if isinstance(warnings, (list, tuple)):
        for warning in warnings:
            if isinstance(warning, dict):
                logger.info(
                    "Engine warning: code=%s stage=%s message=%s",
                    warning.get("code", "unknown"),
                    warning.get("stage", "unknown"),
                    warning.get("message", ""),
                )


__all__ = ["record_engine_metrics"]

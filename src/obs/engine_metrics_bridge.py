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

    _record_trace_summary_metrics(run_result, record_stage_duration)
    _record_operator_metrics(run_result, record_task_duration)
    _log_run_warnings(run_result)


def _record_trace_summary_metrics(
    run_result: dict[str, object],
    record_stage_duration: object,
) -> None:
    if not callable(record_stage_duration):
        return

    trace_summary = run_result.get("trace_metrics_summary")
    if not isinstance(trace_summary, dict):
        return

    elapsed_nanos = trace_summary.get("elapsed_compute_nanos")
    if isinstance(elapsed_nanos, (int, float)) and elapsed_nanos > 0:
        elapsed_seconds = float(elapsed_nanos) / 1_000_000_000
        record_stage_duration("engine_execute", elapsed_seconds, status="ok")

    warning_total = trace_summary.get("warning_count_total", 0)
    if isinstance(warning_total, (int, float)) and warning_total:
        logger.info("Engine warning summary count=%s", int(warning_total))
    warning_by_code = trace_summary.get("warning_counts_by_code")
    if not isinstance(warning_by_code, dict):
        return
    for code, count in warning_by_code.items():
        if isinstance(code, str) and isinstance(count, (int, float)):
            logger.info("Engine warning summary code=%s count=%s", code, int(count))


def _record_operator_metrics(
    run_result: dict[str, object],
    record_task_duration: object,
) -> None:
    if not callable(record_task_duration):
        return

    collected_metrics = run_result.get("collected_metrics")
    if not isinstance(collected_metrics, dict):
        return

    operator_metrics = collected_metrics.get("operator_metrics")
    if not isinstance(operator_metrics, (list, tuple)):
        return

    for op_metric in operator_metrics:
        if not isinstance(op_metric, dict):
            continue
        op_name = op_metric.get("operator_name", "unknown")
        op_elapsed = op_metric.get("elapsed_compute_nanos", 0)
        if isinstance(op_elapsed, (int, float)) and op_elapsed > 0:
            record_task_duration(
                str(op_name),
                float(op_elapsed) / 1_000_000_000,
                status="ok",
            )


def _log_run_warnings(run_result: dict[str, object]) -> None:
    warnings = run_result.get("warnings")
    if not isinstance(warnings, (list, tuple)):
        return
    for warning in warnings:
        if isinstance(warning, dict):
            logger.info(
                "Engine warning: code=%s stage=%s message=%s",
                warning.get("code", "unknown"),
                warning.get("stage", "unknown"),
                warning.get("message", ""),
            )


__all__ = ["record_engine_metrics"]

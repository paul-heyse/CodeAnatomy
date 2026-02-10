"""Generate a post-run diagnostics summary report."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from statistics import mean
from typing import TextIO

from obs.runtime_capabilities_summary import (
    collect_runtime_capability_events,
    runtime_capability_summary_payload,
)


@dataclass(frozen=True)
class DiagnosticsReport:
    """Structured diagnostics report payload."""

    slow_spans: Sequence[Mapping[str, object]]
    stage_breakdown: Sequence[Mapping[str, object]]
    idle_gaps: Sequence[Mapping[str, object]]
    metrics: Mapping[str, object]
    log_summary: Mapping[str, object]
    dataset_readiness: Mapping[str, object]
    provider_modes: Mapping[str, object]
    delta_log_health: Mapping[str, object]
    plan_execution_diff: Mapping[str, object]
    plan_phase_summary: Mapping[str, object]
    datafusion_execution: Mapping[str, object]
    scan_pruning: Mapping[str, object]
    runtime_capabilities: Mapping[str, object]
    diagnostic_categories: Mapping[str, object]

    def to_dict(self) -> dict[str, object]:
        return {
            "slow_spans": list(self.slow_spans),
            "stage_breakdown": list(self.stage_breakdown),
            "idle_gaps": list(self.idle_gaps),
            "metrics": dict(self.metrics),
            "log_summary": dict(self.log_summary),
            "dataset_readiness": dict(self.dataset_readiness),
            "provider_modes": dict(self.provider_modes),
            "delta_log_health": dict(self.delta_log_health),
            "plan_execution_diff": dict(self.plan_execution_diff),
            "plan_phase_summary": dict(self.plan_phase_summary),
            "datafusion_execution": dict(self.datafusion_execution),
            "scan_pruning": dict(self.scan_pruning),
            "runtime_capabilities": dict(self.runtime_capabilities),
            "diagnostic_categories": dict(self.diagnostic_categories),
        }


@dataclass(frozen=True)
class _LogSections:
    log_summary: Mapping[str, object]
    dataset_readiness: Mapping[str, object]
    provider_modes: Mapping[str, object]
    delta_log_health: Mapping[str, object]
    plan_execution_diff: Mapping[str, object]
    plan_phase_summary: Mapping[str, object]
    datafusion_execution: Mapping[str, object]
    scan_pruning: Mapping[str, object]
    runtime_capabilities: Mapping[str, object]
    diagnostic_categories: Mapping[str, object]


def _span_stage(span: Mapping[str, object]) -> str | None:
    attrs = span.get("attributes")
    if isinstance(attrs, Mapping):
        value = attrs.get("codeanatomy.stage")
        if isinstance(value, str):
            return value
    return None


def _span_duration(span: Mapping[str, object]) -> float:
    value = span.get("duration_s")
    if isinstance(value, (int, float)):
        return float(value)
    return 0.0


def _span_start(span: Mapping[str, object]) -> int:
    value = span.get("start_time_unix_nano")
    if isinstance(value, int):
        return value
    return 0


def _span_end(span: Mapping[str, object]) -> int:
    value = span.get("end_time_unix_nano")
    if isinstance(value, int):
        return value
    return 0


def build_diagnostics_report(snapshot: Mapping[str, object]) -> DiagnosticsReport:
    """Build a diagnostics report from a snapshot payload.

    Returns:
    -------
    DiagnosticsReport
        Structured report data derived from the snapshot.
    """
    spans = snapshot.get("spans")
    span_rows: list[Mapping[str, object]] = list(spans) if isinstance(spans, Sequence) else []
    slow_spans, stage_breakdown, idle_gaps = _span_sections(span_rows)
    metrics_payload = (
        dict(metrics) if isinstance(metrics := snapshot.get("gauges"), Mapping) else {}
    )
    log_rows = list(logs) if isinstance(logs := snapshot.get("logs"), Sequence) else []
    log_sections = _build_log_sections(log_rows)
    return DiagnosticsReport(
        slow_spans=slow_spans,
        stage_breakdown=stage_breakdown,
        idle_gaps=idle_gaps,
        metrics=metrics_payload,
        log_summary=log_sections.log_summary,
        dataset_readiness=log_sections.dataset_readiness,
        provider_modes=log_sections.provider_modes,
        delta_log_health=log_sections.delta_log_health,
        plan_execution_diff=log_sections.plan_execution_diff,
        plan_phase_summary=log_sections.plan_phase_summary,
        datafusion_execution=log_sections.datafusion_execution,
        scan_pruning=log_sections.scan_pruning,
        runtime_capabilities=log_sections.runtime_capabilities,
        diagnostic_categories=log_sections.diagnostic_categories,
    )


def write_run_diagnostics_report(
    *,
    snapshot: Mapping[str, object],
    run_bundle_dir: Path,
) -> Path:
    """Write a diagnostics report to the run bundle directory.

    Returns:
    -------
    Path
        Path to the rendered Markdown report.
    """
    report = build_diagnostics_report(snapshot)
    payload = report.to_dict()
    run_bundle_dir.mkdir(parents=True, exist_ok=True)
    json_path = run_bundle_dir / "run_diagnostics_report.json"
    md_path = run_bundle_dir / "run_diagnostics_report.md"
    with json_path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
    with md_path.open("w", encoding="utf-8") as handle:
        _write_report_header(handle)
        _write_slow_spans(handle, report.slow_spans)
        _write_stage_breakdown(handle, report.stage_breakdown)
        _write_idle_gaps(handle, report.idle_gaps)
        _write_metrics_snapshot(handle, report.metrics)
        _write_log_summary(handle, report.log_summary)
        _write_dataset_readiness(handle, report.dataset_readiness)
        _write_provider_modes(handle, report.provider_modes)
        _write_delta_log_health(handle, report.delta_log_health)
        _write_plan_execution(handle, report.plan_execution_diff)
        _write_plan_phase_summary(handle, report.plan_phase_summary)
        _write_datafusion_execution(handle, report.datafusion_execution)
        _write_scan_pruning(handle, report.scan_pruning)
        _write_runtime_capabilities(handle, report.runtime_capabilities)
        _write_diagnostic_categories(handle, report.diagnostic_categories)
    return md_path


def _write_report_header(handle: TextIO) -> None:
    handle.write("# Run Diagnostics Report\n\n")


def _write_slow_spans(
    handle: TextIO,
    spans: Sequence[Mapping[str, object]],
) -> None:
    handle.write("## Slowest Spans\n")
    for span in spans:
        handle.write(
            f"- {span.get('name')} (stage={span.get('stage')}, "
            f"duration_s={span.get('duration_s')})\n"
        )


def _write_stage_breakdown(
    handle: TextIO,
    stage_breakdown: Sequence[Mapping[str, object]],
) -> None:
    handle.write("\n## Stage Breakdown\n")
    for row in stage_breakdown:
        handle.write(f"- {row.get('stage')}: {row.get('duration_s')}s\n")


def _write_idle_gaps(
    handle: TextIO,
    idle_gaps: Sequence[Mapping[str, object]],
) -> None:
    handle.write("\n## Idle Gaps\n")
    if not idle_gaps:
        handle.write("- None detected\n")
        return
    for gap in idle_gaps:
        handle.write(
            f"- {gap.get('gap_s')}s between {gap.get('prev_span')} and {gap.get('next_span')}\n"
        )


def _write_metrics_snapshot(handle: TextIO, metrics: Mapping[str, object]) -> None:
    handle.write("\n## Metrics Snapshot\n")
    for key, value in metrics.items():
        handle.write(f"- {key}: {value}\n")


def _write_log_summary(handle: TextIO, log_summary: Mapping[str, object]) -> None:
    handle.write("\n## Log Summary\n")
    handle.write(f"- count: {log_summary.get('count')}\n")


def _write_dataset_readiness(handle: TextIO, dataset_readiness: Mapping[str, object]) -> None:
    handle.write("\n## Dataset Readiness\n")
    handle.write(f"- total: {dataset_readiness.get('total', 0)}\n")
    handle.write(f"- issues: {dataset_readiness.get('issues', 0)}\n")
    handle.write("\n### Issue Breakdown\n")
    issue_counts = dataset_readiness.get("issue_counts", {})
    if isinstance(issue_counts, Mapping) and issue_counts:
        for status, count in issue_counts.items():
            handle.write(f"- {status}: {count}\n")
        return
    handle.write("- None detected\n")


def _write_provider_modes(handle: TextIO, provider_modes: Mapping[str, object]) -> None:
    handle.write("\n## Provider Modes\n")
    handle.write(f"- total: {provider_modes.get('total', 0)}\n")
    handle.write(f"- warnings: {provider_modes.get('warnings', 0)}\n")
    strict_enabled = provider_modes.get("strict_native_provider_enabled")
    if isinstance(strict_enabled, bool):
        handle.write(f"- strict_native_provider_enabled: {strict_enabled}\n")
    strict_violations = provider_modes.get("strict_native_provider_violations")
    if isinstance(strict_violations, int):
        handle.write(f"- strict_native_provider_violations: {strict_violations}\n")
    modes = provider_modes.get("modes")
    if isinstance(modes, Mapping) and modes:
        for name, count in sorted(modes.items()):
            handle.write(f"- {name}: {count}\n")
        return
    handle.write("- None detected\n")


def _write_runtime_capabilities(
    handle: TextIO,
    runtime_capabilities: Mapping[str, object],
) -> None:
    handle.write("\n## Runtime Capabilities\n")
    handle.write(f"- total: {runtime_capabilities.get('total', 0)}\n")
    for key in (
        "strict_native_provider_enabled",
        "delta_available",
        "delta_compatible",
        "delta_probe_result",
        "delta_ctx_kind",
        "delta_session_defaults_enabled",
        "delta_session_defaults_available",
        "delta_session_defaults_installed",
        "runtime_policy_bridge_enabled",
        "runtime_policy_bridge_reason",
        "runtime_policy_bridge_consumed_settings",
        "runtime_policy_bridge_unsupported_settings",
        "execution_metrics_rows",
        "execution_memory_reserved_bytes",
        "execution_metadata_cache_entries",
        "execution_metadata_cache_hits",
        "execution_list_files_cache_entries",
        "execution_statistics_cache_entries",
    ):
        value = runtime_capabilities.get(key)
        if value is None:
            continue
        handle.write(f"- {key}: {value}\n")
    plugin_path = runtime_capabilities.get("plugin_path")
    if isinstance(plugin_path, str) and plugin_path:
        handle.write(f"- plugin_path: {plugin_path}\n")


def _write_delta_log_health(handle: TextIO, delta_log_health: Mapping[str, object]) -> None:
    handle.write("\n## Delta Log Health\n")
    handle.write(f"- total: {delta_log_health.get('total', 0)}\n")
    handle.write(f"- missing_log: {delta_log_health.get('missing_log', 0)}\n")
    handle.write(f"- protocol_incompatible: {delta_log_health.get('protocol_incompatible', 0)}\n")


def _write_plan_execution(handle: TextIO, plan_execution_diff: Mapping[str, object]) -> None:
    handle.write("\n## Plan vs Execution\n")
    handle.write(f"- expected_tasks: {plan_execution_diff.get('expected_task_count', 0)}\n")
    handle.write(f"- executed_tasks: {plan_execution_diff.get('executed_task_count', 0)}\n")
    handle.write(f"- missing_tasks: {plan_execution_diff.get('missing_task_count', 0)}\n")
    handle.write(f"- unexpected_tasks: {plan_execution_diff.get('unexpected_task_count', 0)}\n")
    plan_signature = plan_execution_diff.get("plan_signature")
    if isinstance(plan_signature, str) and plan_signature:
        handle.write(f"- plan_signature: {plan_signature}\n")
    blocked_datasets = plan_execution_diff.get("blocked_datasets")
    if isinstance(blocked_datasets, Sequence) and not isinstance(
        blocked_datasets, (str, bytes, bytearray)
    ):
        handle.write(f"- blocked_datasets: {len(blocked_datasets)}\n")
    blocked_scan_units = plan_execution_diff.get("blocked_scan_units")
    if isinstance(blocked_scan_units, Sequence) and not isinstance(
        blocked_scan_units, (str, bytes, bytearray)
    ):
        handle.write(f"- blocked_scan_units: {len(blocked_scan_units)}\n")


def _write_plan_phase_summary(handle: TextIO, plan_phase_summary: Mapping[str, object]) -> None:
    handle.write("\n## DataFusion Plan Phases\n")
    phases = plan_phase_summary.get("phases")
    if not isinstance(phases, Mapping) or not phases:
        handle.write("- None detected\n")
        return
    for phase, stats in sorted(phases.items()):
        if not isinstance(stats, Mapping):
            continue
        avg_ms = stats.get("avg_duration_ms")
        count = stats.get("count", 0)
        if isinstance(avg_ms, (int, float)):
            handle.write(f"- {phase}: count={count}, avg_ms={avg_ms:.2f}\n")
        else:
            handle.write(f"- {phase}: count={count}\n")


def _write_datafusion_execution(
    handle: TextIO,
    datafusion_execution: Mapping[str, object],
) -> None:
    handle.write("\n## DataFusion Execution\n")
    handle.write(f"- executions: {datafusion_execution.get('total', 0)}\n")
    handle.write(f"- errors: {datafusion_execution.get('errors', 0)}\n")
    avg_ms = datafusion_execution.get("avg_duration_ms")
    if isinstance(avg_ms, (int, float)):
        handle.write(f"- avg_duration_ms: {avg_ms:.2f}\n")


def _write_scan_pruning(handle: TextIO, scan_pruning: Mapping[str, object]) -> None:
    handle.write("\n## Scan Pruning\n")
    handle.write(f"- total_scans: {scan_pruning.get('total', 0)}\n")
    avg_ratio = scan_pruning.get("avg_pruned_ratio")
    if isinstance(avg_ratio, (int, float)):
        handle.write(f"- avg_pruned_ratio: {avg_ratio:.2f}\n")
    top = scan_pruning.get("top_pruned")
    if isinstance(top, Sequence) and top:
        handle.write("### Top Pruned Datasets\n")
        for row in top:
            if not isinstance(row, Mapping):
                continue
            dataset = row.get("dataset")
            ratio = row.get("pruned_ratio")
            if isinstance(dataset, str) and isinstance(ratio, (int, float)):
                handle.write(f"- {dataset}: {ratio:.2f}\n")


def _write_diagnostic_categories(
    handle: TextIO,
    diagnostic_categories: Mapping[str, object],
) -> None:
    handle.write("\n## Diagnostic Categories\n")
    severity_counts = diagnostic_categories.get("severity_counts")
    if isinstance(severity_counts, Mapping) and severity_counts:
        for severity, count in sorted(severity_counts.items()):
            handle.write(f"- {severity}: {count}\n")
    category_counts = diagnostic_categories.get("category_counts")
    if isinstance(category_counts, Mapping) and category_counts:
        handle.write("### By Category\n")
        for category, count in sorted(category_counts.items()):
            handle.write(f"- {category}: {count}\n")


def _slow_span_payload(span: Mapping[str, object]) -> dict[str, object]:
    return {
        "name": span.get("name"),
        "stage": _span_stage(span),
        "duration_s": _span_duration(span),
        "trace_id": span.get("trace_id"),
        "span_id": span.get("span_id"),
    }


def _accumulate_stage_duration(stage_totals: dict[str, float], span: Mapping[str, object]) -> None:
    stage = _span_stage(span)
    if stage is None:
        return
    stage_totals[stage] = stage_totals.get(stage, 0.0) + _span_duration(span)


def _idle_gaps(
    ordered_spans: Sequence[Mapping[str, object]],
) -> list[dict[str, object] | None]:
    from itertools import pairwise

    min_gap_s = 5.0
    gaps: list[dict[str, object] | None] = []
    for prev, current in pairwise(ordered_spans):
        prev_end = _span_end(prev)
        current_start = _span_start(current)
        if not (prev_end and current_start and current_start > prev_end):
            continue
        gap_s = (current_start - prev_end) / 1_000_000_000
        if gap_s < min_gap_s:
            continue
        gaps.append(
            {
                "gap_s": gap_s,
                "prev_span": prev.get("name"),
                "next_span": current.get("name"),
            }
        )
    return gaps


def _span_sections(
    spans: Sequence[Mapping[str, object]],
) -> tuple[list[dict[str, object]], list[dict[str, object]], list[dict[str, object]]]:
    sorted_spans = sorted(spans, key=_span_duration, reverse=True)
    slow_spans: list[dict[str, object]] = [_slow_span_payload(span) for span in sorted_spans[:10]]
    stage_totals: dict[str, float] = {}
    for span in spans:
        _accumulate_stage_duration(stage_totals, span)
    stage_breakdown: list[dict[str, object]] = [
        {"stage": stage, "duration_s": duration}
        for stage, duration in sorted(stage_totals.items(), key=lambda item: item[1], reverse=True)
    ]
    ordered = sorted(spans, key=_span_start)
    idle_gaps: list[dict[str, object]] = [gap for gap in _idle_gaps(ordered) if gap is not None]
    return slow_spans, stage_breakdown, idle_gaps


def _build_log_sections(logs: Sequence[Mapping[str, object]]) -> _LogSections:
    (
        log_summary,
        dataset_readiness,
        provider_modes,
        delta_log_health,
        plan_execution_diff,
        plan_phase_summary,
        datafusion_execution,
        scan_pruning,
        runtime_capabilities,
        diagnostic_categories,
    ) = _log_sections(logs)
    return _LogSections(
        log_summary=log_summary,
        dataset_readiness=dataset_readiness,
        provider_modes=provider_modes,
        delta_log_health=delta_log_health,
        plan_execution_diff=plan_execution_diff,
        plan_phase_summary=plan_phase_summary,
        datafusion_execution=datafusion_execution,
        scan_pruning=scan_pruning,
        runtime_capabilities=runtime_capabilities,
        diagnostic_categories=diagnostic_categories,
    )


def _log_sections(
    logs: Sequence[Mapping[str, object]],
) -> tuple[
    dict[str, object],
    dict[str, object],
    dict[str, object],
    dict[str, object],
    dict[str, object],
    dict[str, object],
    dict[str, object],
    dict[str, object],
    dict[str, object],
    dict[str, object],
]:
    log_summary: dict[str, object] = {"count": len(logs)}
    dataset_readiness = _dataset_readiness_summary(logs)
    provider_modes = _provider_mode_summary(logs)
    delta_log_health = _delta_log_health_summary(logs)
    plan_execution_diff = _plan_execution_diff_summary(logs, dataset_readiness)
    plan_phase_summary = _plan_phase_summary(logs)
    datafusion_execution = _datafusion_execution_summary(logs)
    scan_pruning = _scan_pruning_summary(logs)
    runtime_capabilities = _runtime_capability_summary(logs)
    diagnostic_categories = _diagnostic_category_summary(logs)
    return (
        log_summary,
        dataset_readiness,
        provider_modes,
        delta_log_health,
        plan_execution_diff,
        plan_phase_summary,
        datafusion_execution,
        scan_pruning,
        runtime_capabilities,
        diagnostic_categories,
    )


def _event_rows(logs: Sequence[Mapping[str, object]], name: str) -> list[Mapping[str, object]]:
    rows: list[Mapping[str, object]] = []
    for record in logs:
        attrs = record.get("attributes")
        if not isinstance(attrs, Mapping):
            continue
        if attrs.get("event.name") != name:
            continue
        rows.append(attrs)
    return rows


def _dataset_readiness_summary(logs: Sequence[Mapping[str, object]]) -> dict[str, object]:
    rows = _event_rows(logs, "dataset_readiness_v1")
    issues: list[Mapping[str, object]] = []
    for row in rows:
        status = row.get("status")
        if status not in {"ok", "remote_path"}:
            issues.append(row)
    issue_counts: dict[str, int] = {}
    for row in issues:
        status = row.get("status")
        if isinstance(status, str):
            issue_counts[status] = issue_counts.get(status, 0) + 1
    issue_datasets = [row.get("dataset") for row in issues if isinstance(row.get("dataset"), str)]
    return {
        "total": len(rows),
        "issues": len(issues),
        "issue_counts": issue_counts,
        "issue_datasets": issue_datasets,
    }


def _provider_mode_summary(logs: Sequence[Mapping[str, object]]) -> dict[str, object]:
    rows = _event_rows(logs, "dataset_provider_mode_v1")
    modes: dict[str, int] = {}
    warnings = 0
    strict_native_provider_enabled: bool | None = None
    strict_native_provider_violations = 0
    for row in rows:
        mode = row.get("provider_mode")
        if isinstance(mode, str):
            modes[mode] = modes.get(mode, 0) + 1
        severity = row.get("diagnostic.severity")
        if severity == "warn":
            warnings += 1
        strict_value = row.get("strict_native_provider_enabled")
        if isinstance(strict_value, bool):
            strict_native_provider_enabled = strict_value
        if row.get("strict_native_provider_violation") is True:
            strict_native_provider_violations += 1
    return {
        "total": len(rows),
        "warnings": warnings,
        "modes": modes,
        "strict_native_provider_enabled": strict_native_provider_enabled,
        "strict_native_provider_violations": strict_native_provider_violations,
    }


def _runtime_capability_summary(logs: Sequence[Mapping[str, object]]) -> dict[str, object]:
    events = collect_runtime_capability_events(logs)
    payload = runtime_capability_summary_payload(events)
    payload.update(_delta_session_defaults_summary(logs))
    return payload


def _delta_session_defaults_summary(logs: Sequence[Mapping[str, object]]) -> dict[str, object]:
    rows = _event_rows(logs, "datafusion_delta_session_defaults_v1")
    if not rows:
        return {}
    latest = rows[-1]
    bridge = latest.get("runtime_policy_bridge")
    bridge_mapping = bridge if isinstance(bridge, Mapping) else None

    bridge_enabled: bool | None = None
    bridge_reason: str | None = None
    bridge_consumed_settings: int | None = None
    bridge_unsupported_settings: int | None = None

    if bridge_mapping is not None:
        enabled_value = bridge_mapping.get("enabled")
        if isinstance(enabled_value, bool):
            bridge_enabled = enabled_value
        reason_value = bridge_mapping.get("reason")
        if isinstance(reason_value, str):
            bridge_reason = reason_value
        consumed = bridge_mapping.get("consumed_runtime_settings")
        if isinstance(consumed, Mapping):
            bridge_consumed_settings = len(consumed)
        unsupported = bridge_mapping.get("unsupported_runtime_settings")
        if isinstance(unsupported, Mapping):
            bridge_unsupported_settings = len(unsupported)

    return {
        "delta_session_defaults_enabled": latest.get("enabled"),
        "delta_session_defaults_available": latest.get("available"),
        "delta_session_defaults_installed": latest.get("installed"),
        "runtime_policy_bridge_enabled": bridge_enabled,
        "runtime_policy_bridge_reason": bridge_reason,
        "runtime_policy_bridge_consumed_settings": bridge_consumed_settings,
        "runtime_policy_bridge_unsupported_settings": bridge_unsupported_settings,
    }


def _delta_log_health_summary(logs: Sequence[Mapping[str, object]]) -> dict[str, object]:
    rows = _event_rows(logs, "delta_log_health_v1")
    missing_log = 0
    protocol_incompatible = 0
    for row in rows:
        if row.get("delta_log_present") is False:
            missing_log += 1
        if row.get("protocol_compatible") is False:
            protocol_incompatible += 1
    return {
        "total": len(rows),
        "missing_log": missing_log,
        "protocol_incompatible": protocol_incompatible,
    }


def _plan_phase_summary(logs: Sequence[Mapping[str, object]]) -> dict[str, object]:
    rows = _event_rows(logs, "datafusion_plan_phase_v1")
    durations_by_phase: dict[str, list[float]] = {}
    for row in rows:
        phase = row.get("phase")
        if not isinstance(phase, str):
            continue
        duration = row.get("duration_ms")
        if not isinstance(duration, (int, float)):
            continue
        durations_by_phase.setdefault(phase, []).append(float(duration))
    phases: dict[str, dict[str, object]] = {}
    for phase, durations in durations_by_phase.items():
        avg = mean(durations) if durations else None
        phases[phase] = {
            "count": len(durations),
            "avg_duration_ms": avg,
        }
    return {"total": len(rows), "phases": phases}


def _scan_pruning_summary(logs: Sequence[Mapping[str, object]]) -> dict[str, object]:
    rows = _event_rows(logs, "scan_unit_pruning_v1")
    ratios: list[tuple[str, float]] = []
    ratio_values: list[float] = []
    for row in rows:
        dataset = row.get("dataset")
        total_files = row.get("total_files")
        pruned_files = row.get("pruned_files")
        if not isinstance(dataset, str):
            continue
        if not isinstance(total_files, (int, float)) or total_files <= 0:
            continue
        if not isinstance(pruned_files, (int, float)):
            continue
        ratio = float(pruned_files) / float(total_files)
        ratios.append((dataset, ratio))
        ratio_values.append(ratio)
    top = [
        {"dataset": dataset, "pruned_ratio": ratio}
        for dataset, ratio in sorted(ratios, key=lambda item: item[1], reverse=True)[:5]
    ]
    avg_ratio = mean(ratio_values) if ratio_values else None
    return {"total": len(rows), "avg_pruned_ratio": avg_ratio, "top_pruned": top}


def _diagnostic_category_summary(logs: Sequence[Mapping[str, object]]) -> dict[str, object]:
    severity_counts: dict[str, int] = {}
    category_counts: dict[str, int] = {}
    category_by_severity: dict[str, dict[str, int]] = {}
    for record in logs:
        attrs = record.get("attributes")
        if not isinstance(attrs, Mapping):
            continue
        severity = attrs.get("diagnostic.severity")
        category = attrs.get("diagnostic.category")
        if not isinstance(severity, str) or not isinstance(category, str):
            continue
        severity_counts[severity] = severity_counts.get(severity, 0) + 1
        category_counts[category] = category_counts.get(category, 0) + 1
        by_severity = category_by_severity.setdefault(category, {})
        by_severity[severity] = by_severity.get(severity, 0) + 1
    return {
        "severity_counts": severity_counts,
        "category_counts": category_counts,
        "category_by_severity": category_by_severity,
    }


def _plan_execution_diff_summary(
    logs: Sequence[Mapping[str, object]],
    dataset_readiness: Mapping[str, object],
) -> dict[str, object]:
    from_event = _plan_execution_diff_from_event(logs, dataset_readiness)
    if from_event is not None:
        return from_event
    return _plan_execution_diff_from_tasks(logs, dataset_readiness)


def _plan_execution_diff_from_event(
    logs: Sequence[Mapping[str, object]],
    dataset_readiness: Mapping[str, object],
) -> dict[str, object] | None:
    diff_rows = _event_rows(logs, "plan_execution_diff_v1")
    if not diff_rows:
        return None
    latest = diff_rows[-1]
    blocked_datasets = _coerce_str_list(latest.get("blocked_datasets"))
    if not blocked_datasets:
        blocked_datasets = _coerce_str_list(dataset_readiness.get("issue_datasets"))
    blocked_scan_units = _coerce_str_list(latest.get("blocked_scan_units"))
    missing_task_fingerprints = _coerce_str_mapping(latest.get("missing_task_fingerprints"))
    missing_task_signatures = _coerce_str_mapping(latest.get("missing_task_signatures"))
    return {
        "expected_task_count": latest.get("expected_task_count", 0),
        "executed_task_count": latest.get("executed_task_count", 0),
        "missing_task_count": latest.get("missing_task_count", 0),
        "unexpected_task_count": latest.get("unexpected_task_count", 0),
        "missing_tasks": latest.get("missing_tasks", []),
        "unexpected_tasks": latest.get("unexpected_tasks", []),
        "blocked_datasets": blocked_datasets,
        "blocked_scan_units": blocked_scan_units,
        "missing_task_fingerprints": missing_task_fingerprints,
        "missing_task_signatures": missing_task_signatures,
        "plan_signature": latest.get("plan_signature"),
        "task_dependency_signature": latest.get("task_dependency_signature"),
        "reduced_task_dependency_signature": latest.get("reduced_task_dependency_signature"),
    }


def _plan_execution_diff_from_tasks(
    logs: Sequence[Mapping[str, object]],
    dataset_readiness: Mapping[str, object],
) -> dict[str, object]:
    """Derive plan vs execution diff from engine-native sources.

    Expected tasks are sourced from engine spec view definitions.
    Executed tasks are sourced from engine run result outputs.
    This replaces the legacy Hamilton task tracking approach.

    Returns:
    -------
    dict[str, object]
        Plan execution diff summary with expected/executed task counts.
    """
    expected_tasks = _expected_tasks_from_engine_spec(logs)
    executed_tasks = _executed_tasks_from_engine_outputs(logs)
    missing = sorted(expected_tasks - executed_tasks)
    unexpected = sorted(executed_tasks - expected_tasks)
    blocked_by = dataset_readiness.get("issue_datasets")
    return {
        "expected_task_count": len(expected_tasks),
        "executed_task_count": len(executed_tasks),
        "missing_task_count": len(missing),
        "unexpected_task_count": len(unexpected),
        "missing_tasks": missing,
        "unexpected_tasks": unexpected,
        "blocked_datasets": blocked_by if isinstance(blocked_by, list) else [],
        "blocked_scan_units": [],
    }


def _expected_tasks_from_engine_spec(logs: Sequence[Mapping[str, object]]) -> set[str]:
    """Extract expected task names from engine spec view definitions.

    Searches for engine_spec_summary_v1 events and extracts view definition names
    as the expected task set. This replaces legacy Hamilton plan_expected_tasks_v1.

    Returns:
    -------
    set[str]
        Expected task names from engine spec view definitions.
    """
    spec_rows = _event_rows(logs, "engine_spec_summary_v1")
    if not spec_rows:
        return set()
    latest = spec_rows[-1]
    view_count = latest.get("view_count")
    if isinstance(view_count, int) and view_count > 0:
        view_names = latest.get("view_names")
        if isinstance(view_names, Sequence) and not isinstance(view_names, (str, bytes, bytearray)):
            return {name for name in view_names if isinstance(name, str)}
    return set()


def _executed_tasks_from_engine_outputs(logs: Sequence[Mapping[str, object]]) -> set[str]:
    """Extract executed task names from engine run result outputs.

    Searches for engine_output_v1 events and extracts table names as the executed
    task set. This replaces legacy Hamilton task_execution_v1 tracking.

    Returns:
    -------
    set[str]
        Executed task names from engine run result outputs.
    """
    output_rows = _event_rows(logs, "engine_output_v1")
    executed_tasks: set[str] = set()
    for row in output_rows:
        table_name = row.get("table_name")
        if isinstance(table_name, str):
            executed_tasks.add(table_name)
    return executed_tasks


def _coerce_str_list(value: object) -> list[str]:
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [item for item in value if isinstance(item, str)]
    return []


def _coerce_str_mapping(value: object) -> dict[str, str]:
    if not isinstance(value, Mapping):
        return {}
    return {
        key: item for key, item in value.items() if isinstance(key, str) and isinstance(item, str)
    }


def _datafusion_execution_summary(
    logs: Sequence[Mapping[str, object]],
) -> dict[str, object]:
    rows = _event_rows(logs, "datafusion_plan_execution_v1")
    durations: list[float] = []
    errors = 0
    for row in rows:
        duration = row.get("duration_ms")
        if isinstance(duration, (int, float)):
            durations.append(float(duration))
        status = row.get("status")
        if status == "error":
            errors += 1
    avg_duration = mean(durations) if durations else None
    return {
        "total": len(rows),
        "errors": errors,
        "avg_duration_ms": avg_duration,
    }


__all__ = ["build_diagnostics_report", "write_run_diagnostics_report"]

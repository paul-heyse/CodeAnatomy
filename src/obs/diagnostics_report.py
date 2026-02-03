"""Generate a post-run diagnostics summary report."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class DiagnosticsReport:
    """Structured diagnostics report payload."""

    slow_spans: Sequence[Mapping[str, object]]
    stage_breakdown: Sequence[Mapping[str, object]]
    idle_gaps: Sequence[Mapping[str, object]]
    metrics: Mapping[str, object]
    log_summary: Mapping[str, object]

    def to_dict(self) -> dict[str, object]:
        return {
            "slow_spans": list(self.slow_spans),
            "stage_breakdown": list(self.stage_breakdown),
            "idle_gaps": list(self.idle_gaps),
            "metrics": dict(self.metrics),
            "log_summary": dict(self.log_summary),
        }


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

    Returns
    -------
    DiagnosticsReport
        Structured report data derived from the snapshot.
    """
    spans = snapshot.get("spans")
    span_rows: list[Mapping[str, object]] = list(spans) if isinstance(spans, Sequence) else []
    sorted_spans = sorted(span_rows, key=_span_duration, reverse=True)
    slow_spans = [_slow_span_payload(span) for span in sorted_spans[:10]]
    stage_totals: dict[str, float] = {}
    for span in span_rows:
        _accumulate_stage_duration(stage_totals, span)
    stage_breakdown = [
        {"stage": stage, "duration_s": duration}
        for stage, duration in sorted(stage_totals.items(), key=lambda item: item[1], reverse=True)
    ]
    ordered = sorted(span_rows, key=_span_start)
    idle_gaps = [gap for gap in _idle_gaps(ordered) if gap is not None]
    metrics = snapshot.get("gauges")
    metrics_payload = dict(metrics) if isinstance(metrics, Mapping) else {}
    logs = snapshot.get("logs")
    log_summary = {
        "count": len(logs) if isinstance(logs, Sequence) else 0,
    }
    return DiagnosticsReport(
        slow_spans=slow_spans,
        stage_breakdown=stage_breakdown,
        idle_gaps=idle_gaps,
        metrics=metrics_payload,
        log_summary=log_summary,
    )


def write_run_diagnostics_report(
    *,
    snapshot: Mapping[str, object],
    run_bundle_dir: Path,
) -> Path:
    """Write a diagnostics report to the run bundle directory.

    Returns
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
        handle.write("# Run Diagnostics Report\n\n")
        handle.write("## Slowest Spans\n")
        for span in report.slow_spans:
            handle.write(
                f"- {span.get('name')} (stage={span.get('stage')}, "
                f"duration_s={span.get('duration_s')})\n"
            )
        handle.write("\n## Stage Breakdown\n")
        for row in report.stage_breakdown:
            handle.write(f"- {row.get('stage')}: {row.get('duration_s')}s\n")
        handle.write("\n## Idle Gaps\n")
        if report.idle_gaps:
            for gap in report.idle_gaps:
                handle.write(
                    f"- {gap.get('gap_s')}s between {gap.get('prev_span')} "
                    f"and {gap.get('next_span')}\n"
                )
        else:
            handle.write("- None detected\n")
        handle.write("\n## Metrics Snapshot\n")
        for key, value in report.metrics.items():
            handle.write(f"- {key}: {value}\n")
        handle.write("\n## Log Summary\n")
        handle.write(f"- count: {report.log_summary.get('count')}\n")
    return md_path


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


__all__ = ["build_diagnostics_report", "write_run_diagnostics_report"]

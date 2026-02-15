"""Benchmark smoke harness for CQ cache/parallel throughput checks."""

from __future__ import annotations

import argparse
import sys
import time
from collections.abc import Callable
from pathlib import Path

import msgspec

from tools.cq.core.contract_codec import dumps_json_value
from tools.cq.core.toolchain import Toolchain
from tools.cq.macros.calls import cmd_calls
from tools.cq.query.executor import ExecutePlanRequestV1, execute_plan
from tools.cq.query.parser import parse_query
from tools.cq.query.planner import compile_query
from tools.cq.search.pipeline.smart_search import smart_search


class PerfMeasurement(msgspec.Struct, frozen=True):
    """One benchmark timing sample."""

    first_run_ms: float
    second_run_ms: float
    speedup_ratio: float


class PerfSmokeReport(msgspec.Struct, frozen=True):
    """Serialized performance smoke report payload."""

    generated_at_epoch_s: float
    workspace: str
    search_python: PerfMeasurement
    calls_python: PerfMeasurement
    query_entity_auto: PerfMeasurement


def _time_call(fn: Callable[[], object], /) -> float:
    start = time.perf_counter()
    fn()
    return (time.perf_counter() - start) * 1000.0


def _measure_pair(fn: Callable[[], object], /) -> PerfMeasurement:
    first = _time_call(fn)
    second = _time_call(fn)
    ratio = (first / second) if second > 0 else 0.0
    return PerfMeasurement(
        first_run_ms=first,
        second_run_ms=second,
        speedup_ratio=ratio,
    )


def build_perf_smoke_report(*, workspace: Path) -> PerfSmokeReport:
    """Run representative CQ commands twice and return timing report.

    Returns:
        Performance smoke report with first/second run timings.
    """
    tc = Toolchain.detect()

    search_measurement = _measure_pair(
        lambda: smart_search(
            root=workspace,
            query="build_graph",
            lang_scope="python",
            tc=tc,
            argv=[],
        )
    )
    calls_measurement = _measure_pair(
        lambda: cmd_calls(
            tc=tc,
            root=workspace,
            argv=[],
            function_name="build_graph",
        )
    )
    query_obj = parse_query("entity=function name=build_graph lang=auto")
    query_plan = compile_query(query_obj)
    query_measurement = _measure_pair(
        lambda: execute_plan(
            ExecutePlanRequestV1(
                plan=query_plan,
                query=query_obj,
                root=str(workspace),
                argv=(),
            ),
            tc=tc,
        )
    )

    return PerfSmokeReport(
        generated_at_epoch_s=time.time(),
        workspace=str(workspace),
        search_python=search_measurement,
        calls_python=calls_measurement,
        query_entity_auto=query_measurement,
    )


def _default_workspace() -> Path:
    return (
        Path(__file__).resolve().parents[3]
        / "tests"
        / "e2e"
        / "cq"
        / "_golden_workspace"
        / "python_project"
    )


def main() -> int:
    """Run performance smoke harness and write JSON report.

    Returns:
        Process exit code.
    """
    parser = argparse.ArgumentParser(description="CQ performance smoke benchmark")
    parser.add_argument(
        "--workspace",
        type=Path,
        default=_default_workspace(),
        help="Workspace root to benchmark against",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("build/perf/cq_smoke_report.json"),
        help="JSON output path",
    )
    args = parser.parse_args()
    workspace = args.workspace.resolve()
    output = args.output

    report = build_perf_smoke_report(workspace=workspace)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        dumps_json_value(msgspec.to_builtins(report), indent=2),
        encoding="utf-8",
    )
    sys.stdout.write(f"Wrote performance smoke report: {output}\n")
    sys.stdout.write(
        "Speedup ratios "
        f"search={report.search_python.speedup_ratio:.2f}x "
        f"calls={report.calls_python.speedup_ratio:.2f}x "
        f"query={report.query_entity_auto.speedup_ratio:.2f}x\n"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""Tests for CQ markdown report rendering."""

from __future__ import annotations

import importlib
import os
import time
from collections.abc import Callable
from pathlib import Path

import pytest
from tools.cq.core.report import (
    RenderEnrichmentResult,
    RenderEnrichmentTask,
    render_markdown,
)
from tools.cq.core.runtime.worker_scheduler import WorkerBatchResult
from tools.cq.core.schema import Anchor, CqResult, DetailPayload, Finding, RunMeta, Section

_RenderEnrichTask = RenderEnrichmentTask
_RenderEnrichResult = RenderEnrichmentResult

EXPECTED_ENRICHMENT_TASKS = 6
EXPECTED_TIMEOUT_SECONDS = 6.0
EXPECTED_MAX_WORKERS = 4
MIN_PARALLEL_PIDS = 2


def _run_meta() -> RunMeta:
    return RunMeta(
        macro="search",
        argv=["cq", "search", "build_graph"],
        root=".",
        started_ms=0.0,
        elapsed_ms=12.0,
        toolchain={},
    )


def _build_enrichment_result(root: Path, file_count: int) -> CqResult:
    findings = [
        Finding(
            category="callsite",
            message=f"target in module_{idx}",
            anchor=Anchor(file=f"src/module_{idx}.py", line=1, col=0),
            details=DetailPayload.from_legacy(
                {
                    "language": "python",
                    "name": "target",
                }
            ),
        )
        for idx in range(file_count)
    ]
    run = RunMeta(
        macro="search",
        argv=["cq", "search", "target"],
        root=str(root),
        started_ms=0.0,
        elapsed_ms=12.0,
        toolchain={},
    )
    return CqResult(run=run, key_findings=findings)


def _extract_python_enrichment_pids(result: CqResult) -> set[int]:
    pids: set[int] = set()
    for finding in result.key_findings:
        payloads: list[dict[str, object]] = []
        direct_python = finding.details.get("python")
        if isinstance(direct_python, dict):
            payloads.append(direct_python)
        python_enrichment = finding.details.get("python_enrichment")
        if isinstance(python_enrichment, dict):
            payloads.append(python_enrichment)
        enrichment = finding.details.get("enrichment")
        if isinstance(enrichment, dict):
            nested_python = enrichment.get("python")
            if isinstance(nested_python, dict):
                payloads.append(nested_python)
        for payload in payloads:
            pid = payload.get("pid")
            if isinstance(pid, int):
                pids.add(pid)
    return pids


def _sleeping_pid_worker(
    task: _RenderEnrichTask,
) -> _RenderEnrichResult:
    time.sleep(0.2)
    return RenderEnrichmentResult(
        file=task.file,
        line=task.line,
        col=task.col,
        language=task.language,
        payload={
            "language": task.language,
            "python": {"pid": os.getpid()},
        },
    )


def test_render_summary_compacts_output() -> None:
    """Test render summary compacts output."""
    run = RunMeta(
        macro="q",
        argv=["cq", "q", "entity=function name=build_graph"],
        root=".",
        started_ms=0.0,
        elapsed_ms=12.0,
        toolchain={},
    )
    result = CqResult(
        run=run,
        summary={
            "query": "build_graph",
            "mode": "identifier",
            "lang_scope": "auto",
            "language_order": ["python", "rust"],
            "languages": {"python": {"total_matches": 1}, "rust": {"total_matches": 0}},
            "cross_language_diagnostics": [],
            "language_capabilities": {"python": {}, "rust": {}, "shared": {}},
            "pattern": r"\bbuild_graph\b",
        },
    )

    output = render_markdown(result)
    assert "## Summary" in output
    assert "- {" in output
    assert '"query":"build_graph"' in output
    assert '"mode":"identifier"' in output
    assert "- **query**:" not in output


def test_render_search_hides_summary_and_context_blocks() -> None:
    """Test render search hides summary and context blocks."""
    finding = Finding(
        category="definition",
        message="build_graph (src/module.py)",
        anchor=Anchor(file="src/module.py", line=10),
        details=DetailPayload.from_legacy(
            {
                "language": "python",
                "context_window": {"start_line": 5, "end_line": 20},
                "context_snippet": "def build_graph():\n    target = 1",
            }
        ),
    )
    result = CqResult(
        run=_run_meta(),
        summary={"query": "build_graph", "mode": "identifier"},
        key_findings=[finding],
        sections=[Section(title="Resolved Objects", findings=[finding])],
    )
    output = render_markdown(result)
    assert "## Summary" not in output
    assert "Context (lines" not in output


def test_render_finding_includes_enrichment_tables() -> None:
    """Test render finding includes enrichment tables."""
    finding = Finding(
        category="definition",
        message="build_graph (src/module.py)",
        anchor=Anchor(file="src/module.py", line=10),
        details=DetailPayload.from_legacy(
            {
                "language": "python",
                "context_window": {"start_line": 5, "end_line": 20},
                "context_snippet": "def build_graph():\n    target = 1",
                "enrichment": {
                    "language": "python",
                    "python": {
                        "item_role": "free_function",
                        "enrichment_status": "applied",
                    },
                },
            }
        ),
    )
    result = CqResult(
        run=_run_meta(),
        key_findings=[finding],
        sections=[Section(title="Top Contexts", findings=[finding])],
    )

    output = render_markdown(result)
    assert "Code Facts:" in output
    assert "Identity" in output
    assert "free_function" in output


def test_render_includes_python_semantic_overview_and_code_facts() -> None:
    """Test render includes python semantic overview and code facts."""
    finding = Finding(
        category="callsite",
        message="target call",
        anchor=Anchor(file="src/module.py", line=10, col=4),
        details=DetailPayload.from_legacy(
            {
                "language": "python",
                "enrichment": {
                    "language": "python",
                    "python": {
                        "meta": {"language": "python"},
                        "structural": {"node_kind": "function_definition"},
                        "python_semantic": {
                            "type_contract": {
                                "resolved_type": "(x: int) -> int",
                                "callable_signature": "target(x: int) -> int",
                            },
                            "call_graph": {"incoming_total": 1, "outgoing_total": 2},
                            "anchor_diagnostics": [],
                        },
                    },
                    "python_semantic": {
                        "type_contract": {
                            "resolved_type": "(x: int) -> int",
                        }
                    },
                },
            }
        ),
    )
    result = CqResult(
        run=_run_meta(),
        summary={
            "query": "target",
            "mode": "identifier",
            "lang_scope": "python",
            "language_order": ["python"],
            "languages": {"python": {"total_matches": 1}},
            "cross_language_diagnostics": [],
            "language_capabilities": {"python": {}, "rust": {}, "shared": {}},
            "python_semantic_overview": {"primary_symbol": "target", "matches_enriched": 1},
        },
        key_findings=[finding],
    )

    output = render_markdown(result)
    assert "Python semantic overview:" in output
    assert "Resolved Type: (x: int) -> int" in output
    assert "Incoming Callers: 1" in output


def test_render_enrichment_parameters_uses_params_alias() -> None:
    """Test render enrichment parameters uses params alias."""
    finding = Finding(
        category="definition",
        message="function: target",
        anchor=Anchor(file="src/module.py", line=10, col=4),
        details=DetailPayload.from_legacy(
            {
                "language": "python",
                "enrichment": {
                    "language": "python",
                    "python": {
                        "meta": {"language": "python"},
                        "resolution": {"symbol_role": "write"},
                        "behavior": {"is_async": False},
                        "structural": {
                            "node_kind": "function_definition",
                            "signature": "def target(value: int) -> int",
                            "params": ["value: int"],
                            "return_type": "int",
                        },
                    },
                },
            }
        ),
    )
    result = CqResult(run=_run_meta(), key_findings=[finding])

    output = render_markdown(result)
    assert "Parameters: value: int" in output
    assert "Language: python" in output


def test_render_hides_unresolved_facts_by_default() -> None:
    """Test render hides unresolved facts by default."""
    finding = Finding(
        category="definition",
        message="function: target",
        anchor=Anchor(file="src/module.py", line=10, col=4),
        details=DetailPayload.from_legacy(
            {
                "language": "python",
                "enrichment": {
                    "language": "python",
                    "python": {
                        "meta": {"language": "python"},
                        "structural": {"node_kind": "function_definition"},
                    },
                },
            }
        ),
    )
    output = render_markdown(CqResult(run=_run_meta(), key_findings=[finding]))
    assert "N/A — not resolved" not in output
    assert "N/A — not applicable" not in output
    assert "Identity" in output


def test_render_can_show_unresolved_facts_with_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test render can show unresolved facts with env."""
    monkeypatch.setenv("CQ_SHOW_UNRESOLVED_FACTS", "1")
    finding = Finding(
        category="definition",
        message="function: target",
        anchor=Anchor(file="src/module.py", line=10, col=4),
        details=DetailPayload.from_legacy(
            {
                "language": "python",
                "enrichment": {
                    "language": "python",
                    "python": {
                        "meta": {"language": "python"},
                        "structural": {"node_kind": "function_definition"},
                    },
                },
            }
        ),
    )
    output = render_markdown(CqResult(run=_run_meta(), key_findings=[finding]))
    assert "N/A — not resolved" in output
    assert "N/A — not applicable" not in output


def test_render_falls_back_to_top_level_enrichment_when_nested_language_payload_empty() -> None:
    """Test render falls back to top level enrichment when nested language payload empty."""
    finding = Finding(
        category="reference",
        message="reference: stable_id",
        anchor=Anchor(file="src/module.py", line=12, col=8),
        details=DetailPayload.from_legacy(
            {
                "language": "python",
                "enrichment": {
                    "language": "python",
                    "python": {},
                    "item_role": "callsite",
                },
            }
        ),
    )
    output = render_markdown(CqResult(run=_run_meta(), key_findings=[finding]))
    assert "Language: python" in output
    assert "Symbol Role: callsite" in output


def test_render_query_import_finding_attaches_code_facts(tmp_path: Path) -> None:
    """Test render query import finding attaches code facts."""
    repo = tmp_path / "repo"
    src = repo / "src"
    src.mkdir(parents=True)
    (src / "module.py").write_text("import os\n", encoding="utf-8")
    run = RunMeta(
        macro="q",
        argv=["cq", "q", "entity=import in=src"],
        root=str(repo),
        started_ms=0.0,
        elapsed_ms=8.0,
        toolchain={},
    )
    finding = Finding(
        category="import",
        message="import: os",
        anchor=Anchor(file="src/module.py", line=1, col=7),
        details=DetailPayload.from_legacy(
            {"language": "python", "line_text": "import os", "match_text": "os"}
        ),
    )
    result = CqResult(run=run, key_findings=[finding])

    output = render_markdown(result)
    assert "Code Facts:" in output
    assert "Symbol Role:" in output
    assert "Symbol Role: N/A — enrichment unavailable" not in output


def test_render_query_finding_attaches_enrichment_without_context_by_default(
    tmp_path: Path,
) -> None:
    """Test render query finding attaches enrichment without context by default."""
    repo = tmp_path / "repo"
    src = repo / "src"
    src.mkdir(parents=True)
    (src / "module.py").write_text(
        "def helper():\n"
        "    return 1\n\n"
        "def target(value: int) -> int:\n"
        "    return helper() + value\n",
        encoding="utf-8",
    )
    run = RunMeta(
        macro="q",
        argv=["cq", "q", "entity=function name=target"],
        root=str(repo),
        started_ms=0.0,
        elapsed_ms=12.0,
        toolchain={},
    )
    finding = Finding(
        category="definition",
        message="function: target",
        anchor=Anchor(file="src/module.py", line=4, col=4),
        details=DetailPayload.from_legacy({"name": "target"}),
    )
    result = CqResult(run=run, key_findings=[finding])
    output = render_markdown(result)
    assert "Context (lines" not in output
    assert "Code Facts:" in output
    assert "Details:" not in output


def test_render_code_overview_falls_back_for_run_query_mode() -> None:
    """Test render code overview falls back for run query mode."""
    run = RunMeta(
        macro="run",
        argv=["cq", "run"],
        root=".",
        started_ms=0.0,
        elapsed_ms=5.0,
        toolchain={},
    )
    result = CqResult(
        run=run,
        summary={"steps": ["q_0", "search_1"]},
    )
    output = render_markdown(result)
    assert "- Query: `multi-step plan (2 steps)`" in output
    assert "- Mode: `run`" in output


def test_render_code_overview_derives_language_scope_from_step_summaries() -> None:
    """Test render code overview derives language scope from step summaries."""
    run = RunMeta(
        macro="run",
        argv=["cq", "run"],
        root=".",
        started_ms=0.0,
        elapsed_ms=5.0,
        toolchain={},
    )
    result = CqResult(
        run=run,
        summary={
            "step_summaries": {
                "q_0": {"lang_scope": "python"},
                "q_1": {"lang_scope": "python"},
            }
        },
    )
    output = render_markdown(result)
    assert "- Language Scope: `python`" in output


def test_render_code_overview_falls_back_for_macro_query_mode() -> None:
    """Test render code overview falls back for macro query mode."""
    run = RunMeta(
        macro="calls",
        argv=["cq", "calls", "build_graph"],
        root=".",
        started_ms=0.0,
        elapsed_ms=5.0,
        toolchain={},
    )
    result = CqResult(run=run, summary={})
    output = render_markdown(result)
    assert "- Query: `build_graph`" in output
    assert "- Mode: `macro:calls`" in output


def test_render_enrichment_uses_fixed_process_pool_workers(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test render enrichment uses fixed process pool workers."""
    report_module = importlib.import_module("tools.cq.core.report")
    result = _build_enrichment_result(tmp_path, file_count=6)

    submit_count = 0
    collect_called = False
    timeout_seconds_seen = 0.0

    class FakeScheduler:
        def submit_cpu(
            self,
            fn: Callable[[_RenderEnrichTask], _RenderEnrichResult],
            task: _RenderEnrichTask,
        ) -> _RenderEnrichResult:
            nonlocal submit_count
            submit_count += 1
            return fn(task)

        def collect_bounded(
            self,
            futures: list[_RenderEnrichResult],
            *,
            timeout_seconds: float,
        ) -> WorkerBatchResult[_RenderEnrichResult]:
            nonlocal collect_called, timeout_seconds_seen
            collect_called = True
            timeout_seconds_seen = timeout_seconds
            done = list(futures)
            return WorkerBatchResult(done=done, timed_out=0)

    def _fake_worker(
        task: _RenderEnrichTask,
    ) -> _RenderEnrichResult:
        return RenderEnrichmentResult(
            file=task.file,
            line=task.line,
            col=task.col,
            language=task.language,
            payload={
                "language": task.language,
                "python": {"pid": 7},
            },
        )

    def _fake_get_worker_scheduler() -> FakeScheduler:
        return FakeScheduler()

    monkeypatch.setattr(report_module, "get_worker_scheduler", _fake_get_worker_scheduler)
    monkeypatch.setattr(report_module, "_compute_render_enrichment_worker", _fake_worker)

    render_markdown(result)

    assert collect_called
    assert submit_count == EXPECTED_ENRICHMENT_TASKS
    assert timeout_seconds_seen == EXPECTED_TIMEOUT_SECONDS
    assert (
        min(EXPECTED_ENRICHMENT_TASKS, report_module.MAX_RENDER_ENRICH_WORKERS)
        == EXPECTED_MAX_WORKERS
    )


def test_render_enrichment_parallelization_workers_1_vs_4(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test render enrichment parallelization workers 1 vs 4."""
    report_module = importlib.import_module("tools.cq.core.report")

    monkeypatch.setattr(report_module, "_compute_render_enrichment_worker", _sleeping_pid_worker)

    serial_result = _build_enrichment_result(tmp_path, file_count=8)
    monkeypatch.setattr(report_module, "MAX_RENDER_ENRICH_WORKERS", 1)
    serial_start = time.perf_counter()
    render_markdown(serial_result)
    serial_elapsed = time.perf_counter() - serial_start
    serial_pids = _extract_python_enrichment_pids(serial_result)

    parallel_result = _build_enrichment_result(tmp_path, file_count=8)
    monkeypatch.setattr(report_module, "MAX_RENDER_ENRICH_WORKERS", 4)
    parallel_start = time.perf_counter()
    render_markdown(parallel_result)
    parallel_elapsed = time.perf_counter() - parallel_start
    parallel_pids = _extract_python_enrichment_pids(parallel_result)

    assert len(serial_pids) == 1
    assert len(parallel_pids) >= MIN_PARALLEL_PIDS
    assert parallel_elapsed < serial_elapsed * 0.8


def test_render_markdown_keeps_compact_diagnostics_without_payload_dump() -> None:
    """Test render markdown keeps compact diagnostics without payload dump."""
    run = RunMeta(
        macro="q",
        argv=["cq", "q", "entity=function name=target"],
        root=".",
        started_ms=0.0,
        elapsed_ms=12.0,
        toolchain={},
    )
    result = CqResult(
        run=run,
        summary={
            "query": "target",
            "mode": "identifier",
            "python_semantic_diagnostics": [{"message": "diag"}],
            "cross_language_diagnostics": [{"code": "ML001"}],
        },
        key_findings=[Finding(category="definition", message="function: target")],
    )
    output = render_markdown(result)
    assert "Python semantic diagnostics: 1 items" in output
    assert "Cross-lang: 1 diagnostics" in output
    assert "Diagnostic Details" not in output
    assert '"message": "diag"' not in output

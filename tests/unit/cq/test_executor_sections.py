"""Unit tests for cq executor section builders."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import msgspec
from tools.cq.core.bootstrap import resolve_runtime_services
from tools.cq.core.front_door_contracts import FrontDoorInsightV1, InsightTargetV1
from tools.cq.core.summary_contract import SemanticTelemetryV1, summary_from_mapping
from tools.cq.core.toolchain import Toolchain
from tools.cq.query.executor_runtime import (
    ExecutePlanRequestV1,
    execute_plan,
)
from tools.cq.query.merge import _mark_entity_insight_partial_from_summary
from tools.cq.query.parser import parse_query
from tools.cq.query.planner import compile_query
from tools.cq.query.section_builders import (
    build_callers_section,
)
from tools.cq.query.sg_parser import SgRecord
from tools.cq.utils.interval_index import FileIntervalIndex


def _def_record(file: str, name: str, start: int, end: int) -> SgRecord:
    return SgRecord(
        record="def",
        kind="function",
        file=file,
        start_line=start,
        start_col=0,
        end_line=end,
        end_col=0,
        text=f"def {name}(): pass",
        rule_id="py_def_function",
    )


def _call_record(file: str, line: int, text: str) -> SgRecord:
    return SgRecord(
        record="call",
        kind="name_call",
        file=file,
        start_line=line,
        start_col=4,
        end_line=line,
        end_col=4 + len(text),
        text=text,
        rule_id="py_call_name",
    )


def _execute_query(
    *,
    plan: Any,
    query: Any,
    tc: Toolchain,
    root: Path,
    argv: tuple[str, ...] = ("cq", "q"),
    query_text: str | None = None,
) -> Any:
    return execute_plan(
        ExecutePlanRequestV1(
            plan=plan,
            query=query,
            root=str(root),
            services=resolve_runtime_services(root),
            argv=argv,
            query_text=query_text,
        ),
        tc=tc,
    )


def test_callers_section_respects_file_boundaries() -> None:
    """Ensure callers are attributed within the correct file."""
    target_def = _def_record("a.py", "target", 1, 10)
    caller_def = _def_record("b.py", "caller", 1, 10)
    call = _call_record("b.py", 5, "target()")

    index = FileIntervalIndex.from_records([target_def, caller_def])
    section = build_callers_section([target_def], [call], index, Path())

    assert len(section.findings) == 1
    finding = section.findings[0]
    assert finding.details.get("caller") == "caller"
    assert finding.anchor is not None
    assert finding.anchor.file == "b.py"


def test_auto_scope_summary_uses_multilang_partitions(tmp_path: Path) -> None:
    """Auto-scope query summary should expose per-language stats partitions only."""
    (tmp_path / "a.py").write_text("def target():\n    return 1\n", encoding="utf-8")
    (tmp_path / "b.rs").write_text("fn target() -> i32 { 1 }\n", encoding="utf-8")
    tc = Toolchain.detect()
    query = parse_query("entity=function name=target lang=auto")
    plan = compile_query(query)
    result = _execute_query(plan=plan, query=query, tc=tc, root=tmp_path)

    assert result.summary["query"] == "entity=function name=target"
    assert result.summary["mode"] == "entity"
    assert "python_semantic_overview" in result.summary
    assert "python_semantic_telemetry" in result.summary
    assert "rust_semantic_telemetry" in result.summary
    assert "python_semantic_diagnostics" in result.summary
    assert result.summary["lang_scope"] == "auto"
    assert result.summary["language_order"] == ["python", "rust"]
    languages = result.summary["languages"]
    assert isinstance(languages, dict)
    assert "python" in languages
    assert "rust" in languages
    assert isinstance(languages["python"], dict)
    assert "query" not in languages["python"]
    assert "front_door_insight" in result.summary


def test_single_scope_summary_uses_canonical_multilang_keys(tmp_path: Path) -> None:
    """Explicit language scope should keep canonical multilang summary fields."""
    (tmp_path / "a.py").write_text("def target():\n    return 1\n", encoding="utf-8")
    tc = Toolchain.detect()
    query = parse_query("entity=function name=target lang=python")
    plan = compile_query(query)
    result = _execute_query(plan=plan, query=query, tc=tc, root=tmp_path)

    assert result.summary["lang_scope"] == "python"
    assert result.summary["language_order"] == ["python"]
    assert "python_semantic_overview" in result.summary
    assert "python_semantic_telemetry" in result.summary
    assert "rust_semantic_telemetry" in result.summary
    assert "python_semantic_diagnostics" in result.summary
    languages = result.summary["languages"]
    assert isinstance(languages, dict)
    assert set(languages) == {"python"}
    assert isinstance(languages["python"], dict)


def test_entity_definition_finding_includes_counts_and_scope(tmp_path: Path) -> None:
    """Definition findings should include caller/callee counts and enclosing scope."""
    (tmp_path / "a.py").write_text(
        "def helper():\n"
        "    return 1\n\n"
        "def target(x):\n"
        "    helper()\n"
        "    return x\n\n"
        "def caller():\n"
        "    return target(1)\n",
        encoding="utf-8",
    )
    tc = Toolchain.detect()
    query = parse_query("entity=function name=target lang=python")
    plan = compile_query(query)
    result = _execute_query(plan=plan, query=query, tc=tc, root=tmp_path)

    definitions = [finding for finding in result.key_findings if finding.category == "definition"]
    assert definitions
    payload = definitions[0].details
    assert isinstance(payload.get("caller_count"), int)
    assert isinstance(payload.get("callee_count"), int)
    assert isinstance(payload.get("calls_within"), int)
    assert isinstance(payload.get("enclosing_scope"), str)


def test_auto_scope_entity_insight_marks_missing_language_partial(tmp_path: Path) -> None:
    """Merged auto-scope entity insight should mark missing language partitions."""
    (tmp_path / "only_rust.rs").write_text(
        "fn compile_target() -> i32 { 1 }\n",
        encoding="utf-8",
    )
    tc = Toolchain.detect()
    query = parse_query("entity=function name=compile_target lang=auto")
    plan = compile_query(query)
    result = _execute_query(plan=plan, query=query, tc=tc, root=tmp_path)

    insight = result.summary.get("front_door_insight")
    assert isinstance(insight, dict)
    degradation = insight.get("degradation")
    assert isinstance(degradation, dict)
    assert degradation.get("scope_filter") == "partial"
    notes = degradation.get("notes")
    assert isinstance(notes, list)
    assert any("missing_languages=python" in str(note) for note in notes)


def test_query_text_preserved_when_provided(tmp_path: Path) -> None:
    """Execute plan should preserve caller-provided query text in summary."""
    (tmp_path / "a.py").write_text("def target():\n    return 1\n", encoding="utf-8")
    tc = Toolchain.detect()
    query_text = "entity=function name=target lang=python in=a.py"
    query = parse_query(query_text)
    plan = compile_query(query)
    result = _execute_query(
        plan=plan,
        query=query,
        tc=tc,
        root=tmp_path,
        argv=("cq", "q", query_text),
        query_text=query_text,
    )

    assert result.summary["query"] == query_text
    assert result.summary["mode"] == "entity"


def test_entity_insight_skips_semantic_for_high_cardinality_query(tmp_path: Path) -> None:
    """Broad entity queries should skip semantic augmentation within front-door budget."""
    lines = []
    for idx in range(60):
        lines.append(f"def fn_{idx}():\n")
        lines.append(f"    return {idx}\n\n")
    (tmp_path / "many.py").write_text("".join(lines), encoding="utf-8")

    tc = Toolchain.detect()
    query = parse_query("entity=function lang=python")
    plan = compile_query(query)
    result = _execute_query(plan=plan, query=query, tc=tc, root=tmp_path)

    insight = result.summary.get("front_door_insight")
    assert isinstance(insight, dict)
    degradation = insight.get("degradation")
    assert isinstance(degradation, dict)
    assert degradation.get("semantic") == "skipped"
    notes = degradation.get("notes")
    assert isinstance(notes, list)
    assert any("not_attempted_by_budget" in str(note) for note in notes)

    python_semantic_telemetry = result.summary.get("python_semantic_telemetry")
    assert isinstance(python_semantic_telemetry, SemanticTelemetryV1)
    assert python_semantic_telemetry.attempted == 0


def test_mark_entity_insight_semantic_from_merged_telemetry() -> None:
    """Merged telemetry should drive deterministic front-door semantic status."""
    from tools.cq.core.schema import CqResult, RunMeta

    run = RunMeta(
        macro="q",
        argv=["cq", "q"],
        root=".",
        started_ms=0.0,
        elapsed_ms=1.0,
        toolchain={},
    )
    insight = FrontDoorInsightV1(
        source="entity",
        target=InsightTargetV1(symbol="compile_target", kind="function"),
    )
    result = CqResult(
        run=run,
        summary=summary_from_mapping(
            {
                "lang_scope": "auto",
                "front_door_insight": msgspec.to_builtins(insight),
                "python_semantic_telemetry": {
                    "attempted": 2,
                    "applied": 1,
                    "failed": 1,
                    "skipped": 0,
                    "timed_out": 0,
                },
                "rust_semantic_telemetry": {
                    "attempted": 0,
                    "applied": 0,
                    "failed": 0,
                    "skipped": 0,
                    "timed_out": 0,
                },
                "languages": {
                    "python": {"matches": 1, "total_matches": 1},
                    "rust": {"matches": 0, "total_matches": 0},
                },
            }
        ),
    )

    _mark_entity_insight_partial_from_summary(result)
    updated = result.summary.get("front_door_insight")
    assert isinstance(updated, dict)
    degradation = updated.get("degradation")
    assert isinstance(degradation, dict)
    assert degradation.get("semantic") == "partial"

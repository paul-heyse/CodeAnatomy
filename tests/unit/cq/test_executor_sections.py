"""Unit tests for cq executor section builders."""

from __future__ import annotations

from pathlib import Path

from tools.cq.core.toolchain import Toolchain
from tools.cq.query.executor import FileIntervalIndex, _build_callers_section, execute_plan
from tools.cq.query.parser import parse_query
from tools.cq.query.planner import compile_query
from tools.cq.query.sg_parser import SgRecord


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


def test_callers_section_respects_file_boundaries() -> None:
    """Ensure callers are attributed within the correct file."""
    target_def = _def_record("a.py", "target", 1, 10)
    caller_def = _def_record("b.py", "caller", 1, 10)
    call = _call_record("b.py", 5, "target()")

    index = FileIntervalIndex.from_records([target_def, caller_def])
    section = _build_callers_section([target_def], [call], index, Path())

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
    result = execute_plan(plan, query, tc, tmp_path, ["cq", "q"])

    assert result.summary["query"] == "entity=function name=target"
    assert result.summary["mode"] == "entity"
    assert "pyrefly_overview" in result.summary
    assert "pyrefly_telemetry" in result.summary
    assert "pyrefly_diagnostics" in result.summary
    assert result.summary["lang_scope"] == "auto"
    assert result.summary["language_order"] == ["python", "rust"]
    languages = result.summary["languages"]
    assert isinstance(languages, dict)
    assert "python" in languages
    assert "rust" in languages
    assert isinstance(languages["python"], dict)
    assert "query" not in languages["python"]


def test_single_scope_summary_uses_canonical_multilang_keys(tmp_path: Path) -> None:
    """Explicit language scope should keep canonical multilang summary fields."""
    (tmp_path / "a.py").write_text("def target():\n    return 1\n", encoding="utf-8")
    tc = Toolchain.detect()
    query = parse_query("entity=function name=target lang=python")
    plan = compile_query(query)
    result = execute_plan(plan, query, tc, tmp_path, ["cq", "q"])

    assert result.summary["lang_scope"] == "python"
    assert result.summary["language_order"] == ["python"]
    assert "pyrefly_overview" in result.summary
    assert "pyrefly_telemetry" in result.summary
    assert "pyrefly_diagnostics" in result.summary
    languages = result.summary["languages"]
    assert isinstance(languages, dict)
    assert set(languages) == {"python"}
    assert isinstance(languages["python"], dict)


def test_query_text_preserved_when_provided(tmp_path: Path) -> None:
    """Execute plan should preserve caller-provided query text in summary."""
    (tmp_path / "a.py").write_text("def target():\n    return 1\n", encoding="utf-8")
    tc = Toolchain.detect()
    query_text = "entity=function name=target lang=python in=a.py"
    query = parse_query(query_text)
    plan = compile_query(query)
    result = execute_plan(
        plan=plan,
        query=query,
        tc=tc,
        root=tmp_path,
        argv=["cq", "q", query_text],
        query_text=query_text,
    )

    assert result.summary["query"] == query_text
    assert result.summary["mode"] == "entity"

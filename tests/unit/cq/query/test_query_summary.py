"""Tests for query summary helper extraction."""

from __future__ import annotations

from pathlib import Path

from tools.cq.core.bootstrap import resolve_runtime_services
from tools.cq.core.schema import Anchor, Finding, mk_result
from tools.cq.core.summary_contract import apply_summary_mapping
from tools.cq.core.toolchain import Toolchain
from tools.cq.query.execution_context import QueryExecutionContext
from tools.cq.query.parser import parse_query
from tools.cq.query.planner import compile_query
from tools.cq.query.query_summary import (
    build_runmeta,
    finalize_single_scope_summary,
    query_text,
    summary_common_for_context,
)


def test_query_text_formats_entity_and_pattern_queries() -> None:
    """Query text should render entity and pattern queries consistently."""
    entity_query = parse_query("entity=function name=target")
    pattern_query = parse_query("pattern='target($$$)'")

    assert query_text(entity_query) == "entity=function name=target"
    assert query_text(pattern_query) == "target($$$)"


def test_finalize_single_scope_summary_builds_language_partition(tmp_path: Path) -> None:
    """Single-language summaries should include canonical language partitions."""
    query = parse_query("entity=function name=target lang=python")
    plan = compile_query(query)
    ctx = QueryExecutionContext(
        query=query,
        plan=plan,
        tc=Toolchain.detect(),
        root=tmp_path,
        argv=["cq", "q"],
        started_ms=0.0,
        run_id="run-1",
        services=resolve_runtime_services(tmp_path),
    )

    result = mk_result(build_runmeta(ctx))
    result.summary = apply_summary_mapping(result.summary, summary_common_for_context(ctx))
    result.key_findings.append(
        Finding(
            category="definition",
            message="target definition",
            anchor=Anchor(file="a.py", line=1),
        )
    )

    finalized = finalize_single_scope_summary(ctx, result)

    assert finalized.summary["lang_scope"] == "python"
    assert finalized.summary["language_order"] == ["python"]
    languages = finalized.summary["languages"]
    assert isinstance(languages, dict)
    assert "python" in languages

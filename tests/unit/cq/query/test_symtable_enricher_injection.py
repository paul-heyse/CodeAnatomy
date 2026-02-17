"""Tests for explicit SymtableEnricher injection seams in query runtime."""

from __future__ import annotations

from pathlib import Path

import pytest
from tools.cq.core.bootstrap import resolve_runtime_services
from tools.cq.core.toolchain import Toolchain
from tools.cq.query.batch import build_batch_session
from tools.cq.query.enrichment import SymtableEnricher
from tools.cq.query.executor_plan_dispatch import ExecutePlanRequestV1
from tools.cq.query.parser import parse_query
from tools.cq.query.planner import compile_query


def test_execute_plan_request_requires_injected_symtable(tmp_path: Path) -> None:
    """Verify execute-plan request rejects missing explicit symtable dependency."""
    query = parse_query("entity=function name=target")
    plan = compile_query(query)
    services = resolve_runtime_services(tmp_path)

    with pytest.raises(TypeError):
        ExecutePlanRequestV1(  # type: ignore[call-arg]
            plan=plan,
            query=query,
            root=str(tmp_path),
            services=services,
            argv=("cq", "q"),
        )


def test_batch_session_uses_injected_symtable(tmp_path: Path) -> None:
    """Verify batch session preserves and exposes injected symtable enricher."""
    (tmp_path / "a.py").write_text("def target():\n    return 1\n", encoding="utf-8")
    symtable = SymtableEnricher(tmp_path)

    session = build_batch_session(
        root=tmp_path,
        tc=Toolchain.detect(),
        paths=[tmp_path],
        record_types={"def"},
        symtable=symtable,
    )

    assert session.symtable is symtable

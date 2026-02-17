"""Tests for query scan orchestration helpers."""

from __future__ import annotations

from types import SimpleNamespace
from typing import cast

import pytest
from tools.cq.astgrep.sgpy_scanner import SgRecord
from tools.cq.query.execution_context import QueryExecutionContext
from tools.cq.query.query_scan import scan_entity_records


def _record(*, line: int, text: str) -> SgRecord:
    return SgRecord(
        record="def",
        kind="function",
        file="a.py",
        start_line=line,
        start_col=0,
        end_line=line + 1,
        end_col=0,
        text=text,
        rule_id="py_def_function",
    )


def test_scan_entity_records_returns_empty_for_empty_fragment_files(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Scanning should short-circuit when fragment context has no files."""
    fragment_ctx = SimpleNamespace(cache_ctx=SimpleNamespace(files=[], root=None))
    monkeypatch.setattr(
        "tools.cq.query.query_scan.build_entity_fragment_context",
        lambda *_args, **_kwargs: fragment_ctx,
    )

    called = {"run": False}

    def _fail_run_query_fragment_scan(**_kwargs: object) -> object:
        called["run"] = True
        return SimpleNamespace(hits=(), miss_payload=None)

    monkeypatch.setattr(
        "tools.cq.query.query_scan.run_query_fragment_scan",
        _fail_run_query_fragment_scan,
    )

    result = scan_entity_records(
        ctx=cast("QueryExecutionContext", SimpleNamespace(run_id="r")),
        paths=[],
        scope_globs=None,
    )

    assert result == []
    assert called["run"] is False


def test_scan_entity_records_applies_miss_payload_after_hits(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Miss payloads should replace stale hit payloads for the same file key."""
    hit_record = _record(line=1, text="def stale(): pass")
    miss_record = _record(line=2, text="def fresh(): pass")
    fragment_ctx = SimpleNamespace(cache_ctx=SimpleNamespace(files=["a.py"], root="."))

    monkeypatch.setattr(
        "tools.cq.query.query_scan.build_entity_fragment_context",
        lambda *_args, **_kwargs: fragment_ctx,
    )
    monkeypatch.setattr(
        "tools.cq.query.query_scan.entity_fragment_entries",
        lambda _fragment_ctx: [],
    )
    monkeypatch.setattr(
        "tools.cq.query.query_scan.entity_records_from_hits",
        lambda _hits: {"a.py": [hit_record]},
    )
    monkeypatch.setattr(
        "tools.cq.query.query_scan.run_query_fragment_scan",
        lambda **_kwargs: SimpleNamespace(hits=(), miss_payload={"a.py": [miss_record]}),
    )
    monkeypatch.setattr(
        "tools.cq.query.query_scan.assemble_entity_records",
        lambda _files, _root, records_by_rel: records_by_rel["a.py"],
    )

    result = scan_entity_records(
        ctx=cast("QueryExecutionContext", SimpleNamespace(run_id="run-1")),
        paths=[],
        scope_globs=None,
    )

    assert result == [miss_record]

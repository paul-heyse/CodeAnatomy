"""Tests for rust-lane query orchestration helpers."""

from __future__ import annotations

from tools.cq.search.tree_sitter.contracts.core_models import QueryExecutionSettingsV1
from tools.cq.search.tree_sitter.rust_lane.query_orchestration import orchestrate_query_packs


class _Node:
    pass


def test_orchestrate_query_packs_delegates_to_runtime(monkeypatch) -> None:
    expected = ({"defs": []}, (), (), {"telemetry": 1}, (), ())

    def _fake_collect(**kwargs: object) -> tuple[object, ...]:
        assert isinstance(kwargs.get("root"), _Node)
        return expected

    monkeypatch.setattr(
        "tools.cq.search.tree_sitter.rust_lane.runtime._collect_query_pack_captures",
        _fake_collect,
    )

    result = orchestrate_query_packs(
        root=_Node(),
        source_bytes=b"fn x() {}",
        windows=(),
        settings=QueryExecutionSettingsV1(),
    )

    assert result == expected

"""Tests for Rust lane query-pack execution wrapper delegates."""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

import pytest
from tools.cq.search.tree_sitter.rust_lane import query_pack_execution

if TYPE_CHECKING:
    from tools.cq.search.tree_sitter.contracts.core_models import QueryExecutionSettingsV1
    from tree_sitter import Node


def test_collect_query_pack_payload_delegates(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify payload-collection wrapper delegates to runtime implementation."""
    sentinel: dict[str, object] = {"ok": True}

    def fake_collect_query_pack_payload(**_kwargs: object) -> dict[str, object]:
        return sentinel

    monkeypatch.setattr(
        "tools.cq.search.tree_sitter.rust_lane.runtime_engine._collect_query_pack_payload",
        fake_collect_query_pack_payload,
    )

    out = query_pack_execution.collect_query_pack_payload(
        root=cast("Node", object()),
        source_bytes=b"",
        byte_span=(1, 2),
    )

    assert out is sentinel


def test_collect_query_pack_captures_delegates(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify capture-collection wrapper delegates to runtime implementation."""
    captures: dict[str, list[object]] = {}
    object_rows: tuple[object, ...] = ()
    query_hits: tuple[object, ...] = ()
    telemetry: dict[str, object] = {}
    injections: tuple[object, ...] = ()
    tags: tuple[object, ...] = ()
    sentinel = (captures, object_rows, query_hits, telemetry, injections, tags)

    def fake_collect_query_pack_captures(
        **_kwargs: object,
    ) -> tuple[
        dict[str, list[object]],
        tuple[object, ...],
        tuple[object, ...],
        dict[str, object],
        tuple[object, ...],
        tuple[object, ...],
    ]:
        return sentinel

    monkeypatch.setattr(
        "tools.cq.search.tree_sitter.rust_lane.runtime_engine._collect_query_pack_captures",
        fake_collect_query_pack_captures,
    )

    out = query_pack_execution.collect_query_pack_captures(
        root=cast("Node", object()),
        source_bytes=b"",
        windows=(),
        settings=cast("QueryExecutionSettingsV1", object()),
    )

    assert out is sentinel

"""Tests for shared tree-sitter query pack executor helpers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, cast

import pytest
from tools.cq.search.tree_sitter.contracts.core_models import (
    QueryExecutionSettingsV1,
    QueryExecutionTelemetryV1,
    QueryWindowV1,
)
from tools.cq.search.tree_sitter.core import query_pack_executor


@dataclass(frozen=True)
class _Node:
    start_byte: int = 0
    end_byte: int = 1


def test_execute_pack_rows_builds_rows_and_match_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: dict[str, object] = {}
    node = _Node()

    def _captures(*_args: object, **_kwargs: object) -> tuple[dict[str, list[object]], object]:
        return {"capture": [node]}, QueryExecutionTelemetryV1()

    def _matches(
        *_args: object, **kwargs: object
    ) -> tuple[list[tuple[int, dict[str, list[object]]]], object]:
        calls["settings"] = kwargs["settings"]
        return [(0, {"capture": [node]})], QueryExecutionTelemetryV1()

    monkeypatch.setattr(query_pack_executor, "run_bounded_query_captures", _captures)
    monkeypatch.setattr(query_pack_executor, "run_bounded_query_matches", _matches)
    monkeypatch.setattr(
        query_pack_executor,
        "build_match_rows_with_query_hits",
        lambda **_kwargs: ((), ()),
    )

    settings = QueryExecutionSettingsV1(match_limit=10, require_containment=False)
    windows = (QueryWindowV1(start_byte=0, end_byte=1),)
    _captures_out, _rows, _hits, _capture_t, _match_t = query_pack_executor.execute_pack_rows(
        query=cast("Any", object()),
        query_name="00_defs.scm",
        root=cast("Any", object()),
        source_bytes=b"a",
        windows=windows,
        settings=settings,
        callbacks=None,
    )
    match_settings = calls.get("settings")
    assert isinstance(match_settings, QueryExecutionSettingsV1)
    assert match_settings.require_containment is True
    assert match_settings.window_mode == "containment_preferred"

"""Tests for `Node.has_changes` filtering in tree-sitter query runtime lanes."""

from __future__ import annotations

from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any, cast

import pytest
from tools.cq.search.tree_sitter.contracts.core_models import (
    QueryExecutionSettingsV1,
    QueryWindowV1,
)
from tools.cq.search.tree_sitter.core import runtime_engine as runtime_module

FILTERED_CAPTURE_COUNT = 2


@dataclass(frozen=True)
class _Node:
    start_byte: int
    end_byte: int
    has_changes: bool | None = None


def test_run_bounded_query_captures_filters_unchanged_nodes_in_change_context(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test run bounded query captures filters unchanged nodes in change context."""
    window = QueryWindowV1(start_byte=0, end_byte=20)
    changed = _Node(start_byte=0, end_byte=4, has_changes=True)
    unchanged = _Node(start_byte=5, end_byte=9, has_changes=False)
    unknown = SimpleNamespace(start_byte=10, end_byte=14)

    monkeypatch.setattr(
        runtime_module,
        "_autotuned_settings",
        lambda settings, **_kwargs: (settings, SimpleNamespace(window_split_target=1)),
    )
    monkeypatch.setattr(
        runtime_module,
        "_build_cursor",
        lambda *_args, **_kwargs: SimpleNamespace(did_exceed_match_limit=False),
    )
    monkeypatch.setattr(
        runtime_module,
        "_window_plan",
        lambda **_kwargs: ((window, None),),
    )
    monkeypatch.setattr(
        runtime_module,
        "_base_window_count",
        lambda **_kwargs: 1,
    )
    monkeypatch.setattr(
        runtime_module,
        "_combined_progress_callback",
        lambda **_kwargs: None,
    )
    monkeypatch.setattr(
        runtime_module,
        "_apply_window",
        lambda **_kwargs: True,
    )
    monkeypatch.setattr(
        runtime_module,
        "_progress_allows",
        lambda _progress: True,
    )
    monkeypatch.setattr(
        runtime_module,
        "_cursor_captures",
        lambda **_kwargs: {"capture": [changed, unchanged, unknown]},
    )
    monkeypatch.setattr(runtime_module, "record_runtime_sample", lambda *_args, **_kwargs: None)

    captures, telemetry = runtime_module.run_bounded_query_captures(
        query=cast("Any", object()),
        root=cast("Any", SimpleNamespace(start_byte=0, end_byte=20)),
        windows=(window,),
        settings=QueryExecutionSettingsV1(has_change_context=True),
    )
    assert captures == {"capture": [changed, unknown]}
    assert telemetry.capture_count == FILTERED_CAPTURE_COUNT

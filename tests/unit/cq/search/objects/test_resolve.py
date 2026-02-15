"""Tests for object resolution wrappers."""

from __future__ import annotations

from typing import Any, cast

import pytest
from tools.cq.search.objects import resolve as resolve_module
from tools.cq.search.pipeline.smart_search import EnrichedMatch


def test_resolve_objects_delegates(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        resolve_module,
        "build_object_resolved_view",
        lambda matches, *, query: {"matches": matches, "query": query},
    )
    payload = cast(
        "Any",
        resolve_module.resolve_objects(
            cast("list[EnrichedMatch]", [cast("Any", object()), cast("Any", object())]),
            query="foo",
        ),
    )
    assert payload["query"] == "foo"

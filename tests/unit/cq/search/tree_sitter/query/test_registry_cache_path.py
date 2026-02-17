"""Tests for query-pack registry cache-path behavior."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pytest
from tools.cq.search.tree_sitter.query import registry


def test_stamped_loader_uses_single_memoization_layer(monkeypatch: pytest.MonkeyPatch) -> None:
    """Stamped loader should call uncached source loader directly."""
    calls: list[tuple[str, bool]] = []

    monkeypatch.setattr(registry, "_fanout_cache", object)

    def _fake_memoize_stampede(_cache: object, **_kwargs: Any) -> Callable[[Any], Any]:
        def _decorator(fn: Any) -> Any:
            return fn

        return _decorator

    monkeypatch.setattr(registry, "memoize_stampede", _fake_memoize_stampede)
    monkeypatch.setattr(
        registry,
        "_load_sources_uncached",
        lambda *, language, include_distribution: (
            calls.append((language, include_distribution)) or ()
        ),
    )

    stamped_loader = registry.__dict__["_stamped_loader"]
    loader = stamped_loader("python")
    assert callable(loader)

    loaded = loader(include_distribution=True, local_hash="hash")

    assert loaded == ()
    assert calls == [("python", True)]

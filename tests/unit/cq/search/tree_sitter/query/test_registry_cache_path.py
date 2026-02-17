"""Tests for query-pack registry cache-path behavior."""

from __future__ import annotations

from tools.cq.search.tree_sitter.query import registry


def test_stamped_loader_uses_single_memoization_layer(monkeypatch) -> None:
    """Stamped loader should call uncached source loader directly."""
    calls: list[tuple[str, bool]] = []

    monkeypatch.setattr(registry, "_fanout_cache", lambda: object())

    def _fake_memoize_stampede(_cache, **_kwargs):
        def _decorator(fn):
            return fn

        return _decorator

    monkeypatch.setattr(registry, "memoize_stampede", _fake_memoize_stampede)
    monkeypatch.setattr(
        registry,
        "_load_sources_uncached",
        lambda *, language, include_distribution: calls.append((language, include_distribution))
        or (),
    )

    loader = registry._stamped_loader("python")
    assert callable(loader)

    loaded = loader(include_distribution=True, local_hash="hash")

    assert loaded == ()
    assert calls == [("python", True)]

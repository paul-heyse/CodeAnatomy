"""Tests for tree-sitter query registry cache adapter."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pytest
from tools.cq.core.cache import diskcache_backend
from tools.cq.search.tree_sitter.query.cache_adapter import query_registry_cache


class _CacheWithGetSet:
    def get(
        self,
        _key: str,
        default: object | None = None,
        *,
        _retry: bool = True,
    ) -> object | None:
        return default

    def set(
        self,
        _key: str,
        _value: object,
        *,
        _expire: int | None = None,
        _tag: str | None = None,
        _retry: bool = True,
    ) -> bool:
        return True


def test_query_registry_cache_returns_none_when_backend_has_no_cache(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    @dataclass
    class _Backend:
        cache: object | None = None

    monkeypatch.setattr(diskcache_backend, "get_cq_cache_backend", lambda **_kwargs: _Backend())
    assert query_registry_cache(root=Path()) is None


def test_query_registry_cache_returns_cache_when_backend_exposes_get_set(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cache = _CacheWithGetSet()

    @dataclass
    class _Backend:
        cache: _CacheWithGetSet

    monkeypatch.setattr(
        diskcache_backend,
        "get_cq_cache_backend",
        lambda **_kwargs: _Backend(cache),
    )
    assert query_registry_cache(root=Path()) is cache

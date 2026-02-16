"""Tests for tree-sitter query registry wrappers."""

from __future__ import annotations

import pytest
from tools.cq.search.tree_sitter.query import registry as registry_module


def test_query_registry_exports_symbols() -> None:
    """Test query registry exports symbols."""
    assert hasattr(registry_module, "QueryPackSourceV1")
    assert callable(registry_module.load_query_pack_sources)


def test_load_distribution_query_source_normalizes_language(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test load distribution query source normalizes language."""
    calls: list[str] = []

    def _load_distribution(language: str) -> list[registry_module.QueryPackSourceV1]:
        calls.append(language)
        return [
            registry_module.QueryPackSourceV1(
                language=language,
                pack_name="highlights.scm",
                source="(identifier) @variable",
            )
        ]

    monkeypatch.setattr(registry_module, "_load_distribution_queries", _load_distribution)
    source = registry_module.load_distribution_query_source(" PYTHON ", "highlights.scm")
    assert source == "(identifier) @variable"
    assert calls == ["python"]


def test_load_query_pack_sources_prefers_local_pack_over_distribution_duplicate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Prefer local query packs over distribution duplicates for same pack name."""
    def _no_cache_loader(_language: str) -> object | None:
        return None

    monkeypatch.setattr(registry_module, "_stamped_loader", _no_cache_loader)
    monkeypatch.setattr(
        registry_module,
        "_load_local_query_sources",
        lambda _language: [
            registry_module.QueryPackSourceV1(
                language="python",
                pack_name="tags.scm",
                source="local",
            )
        ],
    )
    monkeypatch.setattr(
        registry_module,
        "_load_distribution_queries",
        lambda _language: [
            registry_module.QueryPackSourceV1(
                language="python",
                pack_name="tags.scm",
                source="dist-tags",
            ),
            registry_module.QueryPackSourceV1(
                language="python",
                pack_name="highlights.scm",
                source="dist-highlights",
            ),
        ],
    )
    rows = registry_module.load_query_pack_sources("python", include_distribution=True)
    assert tuple(row.pack_name for row in rows) == ("tags.scm", "highlights.scm")
    assert rows[0].source == "local"

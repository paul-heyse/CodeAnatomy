"""Tests for tree-sitter ABI compatibility gating."""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from tools.cq.search.tree_sitter.core import language_registry as registry


def _python_capsule() -> object:
    return object()


def test_load_tree_sitter_language_raises_for_out_of_range_abi(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test load tree sitter language raises for out of range abi."""
    registry.load_tree_sitter_language.cache_clear()
    monkeypatch.setattr(
        registry,
        "_ts",
        SimpleNamespace(
            LANGUAGE_VERSION=15,
            MIN_COMPATIBLE_LANGUAGE_VERSION=13,
        ),
    )

    def _make_language(_capsule: object) -> SimpleNamespace:
        return SimpleNamespace(abi_version=12)

    monkeypatch.setattr(registry, "_TreeSitterLanguage", _make_language)
    monkeypatch.setattr(
        registry,
        "_tree_sitter_python",
        SimpleNamespace(language=_python_capsule),
    )
    with pytest.raises(RuntimeError, match="tree-sitter ABI mismatch"):
        registry.load_tree_sitter_language("python")


def test_load_tree_sitter_language_degrades_when_runtime_metadata_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test load tree sitter language degrades when runtime metadata missing."""
    registry.load_tree_sitter_language.cache_clear()
    monkeypatch.setattr(registry, "_ts", SimpleNamespace())

    def _make_language(_capsule: object) -> SimpleNamespace:
        return SimpleNamespace()

    monkeypatch.setattr(registry, "_TreeSitterLanguage", _make_language)
    monkeypatch.setattr(
        registry,
        "_tree_sitter_python",
        SimpleNamespace(language=_python_capsule),
    )
    language = registry.load_tree_sitter_language("python")
    assert language is not None


def test_load_tree_sitter_language_applies_abi_gate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test load tree sitter language applies abi gate."""
    registry.load_tree_sitter_language.cache_clear()
    monkeypatch.setattr(
        registry,
        "_ts",
        SimpleNamespace(
            LANGUAGE_VERSION=15,
            MIN_COMPATIBLE_LANGUAGE_VERSION=13,
        ),
    )

    def _make_language(_capsule: object) -> SimpleNamespace:
        return SimpleNamespace(abi_version=11)

    monkeypatch.setattr(registry, "_TreeSitterLanguage", _make_language)
    monkeypatch.setattr(
        registry,
        "_tree_sitter_python",
        SimpleNamespace(language=_python_capsule),
    )
    with pytest.raises(RuntimeError, match="tree-sitter ABI mismatch"):
        registry.load_tree_sitter_language("python")

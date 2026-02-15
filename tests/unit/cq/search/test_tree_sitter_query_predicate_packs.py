"""Sanity checks for predicate-enabled query packs."""

from __future__ import annotations

from pathlib import Path


def _read(path: str) -> str:
    return Path(path).read_text(encoding="utf-8")


def test_python_predicate_filter_pack_contains_pushdown_predicates() -> None:
    source = _read("tools/cq/search/queries/python/70_predicate_filters.scm")
    assert "#match?" in source
    assert "cq.emit" in source


def test_rust_predicate_filter_pack_contains_pushdown_predicates() -> None:
    source = _read("tools/cq/search/queries/rust/70_predicate_filters.scm")
    assert "#match?" in source or "#any-of?" in source
    assert "cq.emit" in source

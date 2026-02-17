"""Tests for shared query-pack collection pipeline helpers."""

from __future__ import annotations

from tools.cq.search.tree_sitter.core.query_pack_pipeline import collect_query_pack_captures


def test_collect_query_pack_captures_skips_none_results() -> None:
    """Verify pack-collection pipeline skips `None` pack results."""
    seen: list[tuple[str, str]] = []

    def run_pack(pack_name: str, query_source: str) -> tuple[str, str] | None:
        seen.append((pack_name, query_source))
        if pack_name == "skip":
            return None
        return (pack_name, query_source)

    out = collect_query_pack_captures(
        pack_sources=(("a", "qa"), ("skip", "qskip"), ("b", "qb")),
        build_accumulator=list,
        run_pack=run_pack,
        merge_result=list.append,
        finalize=tuple,
    )

    assert seen == [("a", "qa"), ("skip", "qskip"), ("b", "qb")]
    assert out == (("a", "qa"), ("b", "qb"))


def test_collect_query_pack_captures_uses_finalize_hook() -> None:
    """Verify pack-collection pipeline applies caller-provided finalize hook."""
    out = collect_query_pack_captures(
        pack_sources=(("x", "qx"),),
        build_accumulator=dict,
        run_pack=lambda name, _src: (name, 1),
        merge_result=lambda acc, result: acc.__setitem__(result[0], result[1]),
        finalize=lambda acc: sorted(acc.items()),
    )

    assert out == [("x", 1)]

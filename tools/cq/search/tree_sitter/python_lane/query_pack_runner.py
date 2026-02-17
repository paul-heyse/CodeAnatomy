"""Python query-pack runner wrappers."""

from __future__ import annotations

from tools.cq.search.tree_sitter.python_lane.facts import build_python_tree_sitter_facts

__all__ = ["run_python_query_pack"]


def run_python_query_pack(
    source: str,
    *,
    byte_start: int,
    byte_end: int,
    cache_key: str | None = None,
) -> dict[str, object] | None:
    """Run Python query-pack fact collection for one byte-range.

    Returns:
        dict[str, object] | None: Python query-pack fact payload.
    """
    return build_python_tree_sitter_facts(
        source,
        byte_start=byte_start,
        byte_end=byte_end,
        cache_key=cache_key,
    )

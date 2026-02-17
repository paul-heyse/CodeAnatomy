"""Shared query-pack collection pipeline.

This module centralizes the loop that iterates query packs, executes each pack,
and merges successful results into an accumulator.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable


def collect_query_pack_captures[AccumulatorT, PackResultT, OutputT](
    *,
    pack_sources: Iterable[tuple[str, str]],
    build_accumulator: Callable[[], AccumulatorT],
    run_pack: Callable[[str, str], PackResultT | None],
    merge_result: Callable[[AccumulatorT, PackResultT], None],
    finalize: Callable[[AccumulatorT], OutputT],
) -> OutputT:
    """Collect captures across query packs using caller-provided hooks.

    Returns:
        The finalized accumulator payload.
    """
    accumulator = build_accumulator()
    for pack_name, query_source in pack_sources:
        result = run_pack(pack_name, query_source)
        if result is None:
            continue
        merge_result(accumulator, result)
    return finalize(accumulator)


__all__ = ["collect_query_pack_captures"]

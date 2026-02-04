"""Performance checks for interval indexing."""

from __future__ import annotations

from time import perf_counter

import pytest
from tools.cq.utils.interval_index import IntervalIndex


@pytest.mark.benchmark
def test_interval_index_lookup_perf() -> None:
    """Interval index lookups should remain fast at scale."""
    intervals = [(i * 3 + 1, i * 3 + 3, i) for i in range(20_000)]
    index = IntervalIndex.from_intervals(intervals)

    start = perf_counter()
    hits = 0
    for line in range(1, 60_000, 7):
        if index.find_containing(line) is not None:
            hits += 1
    elapsed = perf_counter() - start

    assert hits > 0
    assert elapsed < 1.0

"""Tests for shared timeout wrappers split from _shared.core."""

from __future__ import annotations

import asyncio

import pytest
from tools.cq.search._shared.timeouts import (
    search_async_with_timeout,
    search_sync_with_timeout,
)

EXPECTED_ASYNC_VALUE = 7


def test_search_sync_with_timeout_rejects_negative_timeout() -> None:
    """Negative sync timeout values should raise ValueError."""
    with pytest.raises(ValueError, match="Timeout must be positive"):
        search_sync_with_timeout(lambda: 1, timeout=-1.0)


@pytest.mark.asyncio
async def test_search_async_with_timeout_returns_result() -> None:
    """Async timeout wrapper should return completed coroutine values."""

    async def _value() -> int:
        await asyncio.sleep(0)
        return EXPECTED_ASYNC_VALUE

    assert await search_async_with_timeout(_value(), timeout_seconds=1.0) == EXPECTED_ASYNC_VALUE

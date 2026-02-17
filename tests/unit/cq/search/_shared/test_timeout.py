"""Tests for timeout helper exports."""

from __future__ import annotations

import asyncio

import pytest
from tools.cq.search._shared.timeouts import search_async_with_timeout, search_sync_with_timeout

_SYNC_RESULT = 7
_ASYNC_RESULT = 9


def test_search_sync_with_timeout_returns_value() -> None:
    """Sync timeout wrapper should return function value when under budget."""
    assert search_sync_with_timeout(lambda: _SYNC_RESULT, timeout=0.5) == _SYNC_RESULT


@pytest.mark.asyncio
async def test_search_async_with_timeout_returns_value() -> None:
    """Async timeout wrapper should return coroutine value when under budget."""

    async def _coro() -> int:
        await asyncio.sleep(0)
        return _ASYNC_RESULT

    assert await search_async_with_timeout(_coro(), 0.5) == _ASYNC_RESULT

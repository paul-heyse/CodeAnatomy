"""Tests for search timeout utilities."""

from __future__ import annotations

import asyncio
import time

import pytest
from tools.cq.search._shared.core import (
    search_async_with_timeout,
    search_sync_with_timeout,
)

SUM_RESULT = 5
PRODUCT_WITH_OFFSET = 22


class TestSyncTimeout:
    """Test synchronous timeout wrapper."""

    @staticmethod
    def test_completes_within_timeout() -> None:
        """Test function that completes in time returns result."""

        def fast_function() -> str:
            return "success"

        result = search_sync_with_timeout(fast_function, timeout=1.0)
        assert result == "success"

    @staticmethod
    def test_raises_timeout_error() -> None:
        """Test function that takes too long raises TimeoutError."""

        def slow_function() -> str:
            time.sleep(2.0)
            return "too late"

        with pytest.raises(TimeoutError, match="timed out after"):
            search_sync_with_timeout(slow_function, timeout=0.1)

    @staticmethod
    def test_with_args() -> None:
        """Test timeout wrapper passes args correctly."""

        def add(a: int, b: int) -> int:
            return a + b

        result = search_sync_with_timeout(add, timeout=1.0, args=(2, 3))
        assert result == SUM_RESULT

    @staticmethod
    def test_with_kwargs() -> None:
        """Test timeout wrapper passes kwargs correctly."""

        def greet(name: str, greeting: str = "Hello") -> str:
            return f"{greeting}, {name}"

        result = search_sync_with_timeout(
            greet,
            timeout=1.0,
            kwargs={"name": "World", "greeting": "Hi"},
        )
        assert result == "Hi, World"

    @staticmethod
    def test_with_args_and_kwargs() -> None:
        """Test timeout wrapper passes both args and kwargs."""

        def multiply_and_add(x: int, y: int, offset: int = 0) -> int:
            return x * y + offset

        result = search_sync_with_timeout(
            multiply_and_add,
            timeout=1.0,
            args=(3, 4),
            kwargs={"offset": 10},
        )
        assert result == PRODUCT_WITH_OFFSET

    @staticmethod
    def test_preserves_exceptions() -> None:
        """Test timeout wrapper preserves function exceptions."""

        def failing_function() -> None:
            msg = "test error"
            raise ValueError(msg)

        with pytest.raises(ValueError, match="test error"):
            search_sync_with_timeout(failing_function, timeout=1.0)


class TestAsyncTimeout:
    """Test asynchronous timeout wrapper."""

    @staticmethod
    @pytest.mark.asyncio
    async def test_completes_within_timeout() -> None:
        """Test coroutine that completes in time returns result."""

        async def fast_coroutine() -> str:
            await asyncio.sleep(0.01)
            return "success"

        result = await search_async_with_timeout(fast_coroutine(), timeout_seconds=1.0)
        assert result == "success"

    @staticmethod
    @pytest.mark.asyncio
    async def test_raises_timeout_error() -> None:
        """Test coroutine that takes too long raises TimeoutError."""

        async def slow_coroutine() -> str:
            await asyncio.sleep(2.0)
            return "too late"

        with pytest.raises(TimeoutError, match="timed out after"):
            await search_async_with_timeout(slow_coroutine(), timeout_seconds=0.1)

    @staticmethod
    @pytest.mark.asyncio
    async def test_preserves_exceptions() -> None:
        """Test timeout wrapper preserves coroutine exceptions."""

        async def failing_coroutine() -> None:
            await asyncio.sleep(0)
            msg = "async error"
            raise ValueError(msg)

        with pytest.raises(ValueError, match="async error"):
            await search_async_with_timeout(failing_coroutine(), timeout_seconds=1.0)

    @staticmethod
    @pytest.mark.asyncio
    async def test_cancellation_cleanup() -> None:
        """Test timeout properly cancels long-running coroutine."""
        cancelled = False

        async def cancellable_coroutine() -> str:
            nonlocal cancelled
            try:
                await asyncio.sleep(10.0)
            except asyncio.CancelledError:
                cancelled = True
                raise
            else:
                return "should not reach"

        with pytest.raises(TimeoutError):
            await search_async_with_timeout(cancellable_coroutine(), timeout_seconds=0.1)

        # Give cancellation time to propagate
        await asyncio.sleep(0.01)
        assert cancelled


class TestTimeoutEdgeCases:
    """Test edge cases for timeout handling."""

    @staticmethod
    def test_zero_timeout_sync() -> None:
        """Test zero timeout behavior (may complete or timeout depending on timing)."""

        def instant_function() -> str:
            return "result"

        # Zero timeout is valid but may time out on slow systems
        # Just verify it doesn't raise ValueError
        try:
            result = search_sync_with_timeout(instant_function, timeout=0.0)
            assert result == "result"
        except TimeoutError:
            pass  # Also acceptable

    @staticmethod
    @pytest.mark.asyncio
    async def test_zero_timeout_async() -> None:
        """Test zero timeout behavior for async."""

        async def instant_coroutine() -> str:
            await asyncio.sleep(0)
            return "result"

        # Zero timeout is valid
        try:
            result = await search_async_with_timeout(instant_coroutine(), timeout_seconds=0.0)
            assert result == "result"
        except TimeoutError:
            pass  # Also acceptable

    @staticmethod
    def test_negative_timeout_sync() -> None:
        """Test negative timeout raises ValueError."""

        def any_function() -> str:
            return "result"

        with pytest.raises(ValueError, match="Timeout must be positive"):
            search_sync_with_timeout(any_function, timeout=-1.0)

    @staticmethod
    @pytest.mark.asyncio
    async def test_negative_timeout_async() -> None:
        """Test negative timeout raises ValueError."""

        async def any_coroutine() -> str:
            await asyncio.sleep(0)
            return "result"

        coro = any_coroutine()
        try:
            with pytest.raises(ValueError, match="Timeout must be positive"):
                await search_async_with_timeout(coro, timeout_seconds=-1.0)
        finally:
            coro.close()

    @staticmethod
    @pytest.mark.parametrize("timeout", [0.1, 0.5, 1.0, 5.0])
    def test_various_timeouts_sync(timeout: float) -> None:
        """Test timeout wrapper with various timeout values."""

        def quick_function() -> float:
            return timeout

        result = search_sync_with_timeout(quick_function, timeout=timeout)
        assert result == timeout

    @staticmethod
    @pytest.mark.parametrize("timeout_seconds", [0.1, 0.5, 1.0, 5.0])
    @pytest.mark.asyncio
    async def test_various_timeouts_async(timeout_seconds: float) -> None:
        """Test timeout wrapper with various timeout values."""

        async def quick_coroutine() -> float:
            await asyncio.sleep(0.01)
            return timeout_seconds

        result = await search_async_with_timeout(quick_coroutine(), timeout_seconds=timeout_seconds)
        assert result == timeout_seconds

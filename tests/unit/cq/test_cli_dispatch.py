"""Tests for CQ CLI dispatch helpers."""

from __future__ import annotations

import asyncio
import inspect

import pytest
from tools.cq.cli_app.infrastructure import dispatch_bound_command

ASYNC_INPUT_VALUE = 7
ASYNC_OUTPUT_VALUE = 8


def test_dispatch_bound_command_sync() -> None:
    """Dispatch sync command should pass bound positional args unchanged."""

    def command(name: str) -> str:
        return f"hello {name}"

    bound = inspect.signature(command).bind("cq")
    assert dispatch_bound_command(command, bound) == "hello cq"


def test_dispatch_bound_command_async_without_running_loop() -> None:
    """Test dispatch bound command async without running loop."""

    async def command(value: int) -> int:
        await asyncio.sleep(0)
        return value + 1

    bound = inspect.signature(command).bind(ASYNC_INPUT_VALUE)
    assert dispatch_bound_command(command, bound) == ASYNC_OUTPUT_VALUE


def test_dispatch_bound_command_async_with_running_loop(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test dispatch bound command async with running loop."""

    async def command(value: int) -> int:
        await asyncio.sleep(0)
        return value + 1

    def _running_loop() -> object:
        return object()

    bound = inspect.signature(command).bind(7)
    monkeypatch.setattr("tools.cq.cli_app.infrastructure.asyncio.get_running_loop", _running_loop)
    with pytest.raises(RuntimeError, match="event loop is running"):
        _ = dispatch_bound_command(command, bound)

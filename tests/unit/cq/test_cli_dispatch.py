"""Tests for CQ CLI dispatch helpers."""

from __future__ import annotations

import asyncio
import inspect

import pytest
from tools.cq.cli_app.dispatch import dispatch_bound_command


def test_dispatch_bound_command_sync() -> None:
    def command(name: str) -> str:
        return f"hello {name}"

    bound = inspect.signature(command).bind("cq")
    assert dispatch_bound_command(command, bound) == "hello cq"


def test_dispatch_bound_command_async_without_running_loop() -> None:
    async def command(value: int) -> int:
        await asyncio.sleep(0)
        return value + 1

    bound = inspect.signature(command).bind(7)
    assert dispatch_bound_command(command, bound) == 8


def test_dispatch_bound_command_async_with_running_loop(monkeypatch: pytest.MonkeyPatch) -> None:
    async def command(value: int) -> int:
        await asyncio.sleep(0)
        return value + 1

    def _running_loop() -> object:
        return object()

    bound = inspect.signature(command).bind(7)
    monkeypatch.setattr("tools.cq.cli_app.dispatch.asyncio.get_running_loop", _running_loop)
    with pytest.raises(RuntimeError, match="event loop is running"):
        _ = dispatch_bound_command(command, bound)

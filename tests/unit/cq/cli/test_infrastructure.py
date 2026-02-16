"""Tests for CLI infrastructure consolidation module."""

from __future__ import annotations

import asyncio
import inspect
from pathlib import Path

import pytest
from tools.cq.cli_app.context import CliContext
from tools.cq.cli_app.infrastructure import (
    admin_group,
    analysis_group,
    build_config_chain,
    dispatch_bound_command,
    global_group,
    protocol_group,
    require_context,
    setup_group,
)

ASYNC_DISPATCH_RESULT = 10


class TestBuildConfigChain:
    """Tests for build_config_chain function."""

    @staticmethod
    def test_imports_correctly() -> None:
        """Test that build_config_chain is importable and callable."""
        assert callable(build_config_chain)
        providers = build_config_chain(use_config=False)
        assert len(providers) == 1
        assert providers[0].__class__.__name__ == "Env"


class TestCommandGroups:
    """Tests for command group definitions."""

    @staticmethod
    def test_all_groups_defined() -> None:
        """Test that all expected groups are defined and are Group instances."""
        assert global_group is not None
        assert analysis_group is not None
        assert admin_group is not None
        assert protocol_group is not None
        assert setup_group is not None

        # Verify they have expected attributes
        assert hasattr(global_group, "help")
        assert hasattr(analysis_group, "help")
        assert hasattr(admin_group, "help")
        assert hasattr(protocol_group, "help")
        assert hasattr(setup_group, "help")


class TestContextDecorators:
    """Tests for context decorator and validator functions."""

    @staticmethod
    def test_require_context_with_valid_context() -> None:
        """Test that require_context returns the context when valid."""
        ctx = CliContext.build(argv=["cq"], root=Path(), verbose=0)
        result = require_context(ctx)
        assert result is ctx

    @staticmethod
    def test_require_context_with_none_raises() -> None:
        """Test that require_context raises when context is None."""
        with pytest.raises(RuntimeError, match="Context not injected"):
            require_context(None)

    # Note: require_ctx was removed during S8 (dual context injection consolidation)
    # Use require_context() directly for validation instead


class TestDispatchBoundCommand:
    """Tests for dispatch_bound_command function."""

    @staticmethod
    def test_dispatch_sync_command() -> None:
        """Test dispatching a synchronous command."""

        def sync_command(name: str) -> str:
            return f"hello {name}"

        bound = inspect.signature(sync_command).bind("world")
        result = dispatch_bound_command(sync_command, bound)
        assert result == "hello world"

    @staticmethod
    def test_dispatch_async_command_without_running_loop() -> None:
        """Test dispatching an async command when no event loop is running."""

        async def async_command(value: int) -> int:
            await asyncio.sleep(0)
            return value * 2

        bound = inspect.signature(async_command).bind(5)
        result = dispatch_bound_command(async_command, bound)
        assert result == ASYNC_DISPATCH_RESULT

    @staticmethod
    def test_dispatch_async_command_with_running_loop_raises(
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that dispatching async command with running loop raises."""

        async def async_command(value: int) -> int:
            await asyncio.sleep(0)
            return value * 2

        def _running_loop() -> object:
            return object()

        bound = inspect.signature(async_command).bind(5)
        monkeypatch.setattr(
            "tools.cq.cli_app.infrastructure.asyncio.get_running_loop", _running_loop
        )

        with pytest.raises(RuntimeError, match="event loop is running"):
            dispatch_bound_command(async_command, bound)

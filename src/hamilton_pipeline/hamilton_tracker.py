"""Hamilton tracker wrappers with CodeAnatomy run tag support."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import Any

from hamilton_sdk import adapters as sdk_adapters


class CodeAnatomyHamiltonTracker(sdk_adapters.HamiltonTracker):
    """Hamilton tracker that injects run-scoped tags."""

    def __init__(
        self,
        *args: Any,
        run_tag_provider: Callable[[], dict[str, str]] | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)
        self._run_tag_provider = run_tag_provider

    def pre_graph_execute(
        self,
        run_id: str,
        graph: Any,
        final_vars: list[str],
        inputs: dict[str, Any],
        overrides: dict[str, Any],
    ) -> Any:
        """Attach run-scoped tags before graph execution.

        Returns
        -------
        Any
            Result from the base tracker pre-execute hook.
        """
        original_tags = self.base_tags
        self.base_tags = _merge_tags(original_tags, self._run_tag_provider)
        try:
            return super().pre_graph_execute(
                run_id=run_id,
                graph=graph,
                final_vars=final_vars,
                inputs=inputs,
                overrides=overrides,
            )
        finally:
            self.base_tags = original_tags

    def post_graph_execute(
        self,
        run_id: str,
        graph: Any,
        success: bool,
        error: Exception | None,
        results: dict[str, Any] | None,
    ) -> None:
        """Stop the tracker after graph execution completes."""
        try:
            super().post_graph_execute(
                run_id=run_id,
                graph=graph,
                success=success,
                error=error,
                results=results,
            )
        finally:
            self.stop()


class CodeAnatomyAsyncHamiltonTracker(sdk_adapters.AsyncHamiltonTracker):
    """Async Hamilton tracker that injects run-scoped tags."""

    def __init__(
        self,
        *args: Any,
        run_tag_provider: Callable[[], dict[str, str]] | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)
        self._run_tag_provider = run_tag_provider

    async def pre_graph_execute(
        self,
        run_id: str,
        graph: Any,
        final_vars: list[str],
        inputs: dict[str, Any],
        overrides: dict[str, Any],
    ) -> Any:
        """Attach run-scoped tags before async graph execution.

        Returns
        -------
        Any
            Result from the base tracker pre-execute hook.
        """
        original_tags = self.base_tags
        self.base_tags = _merge_tags(original_tags, self._run_tag_provider)
        try:
            return await super().pre_graph_execute(
                run_id=run_id,
                graph=graph,
                final_vars=final_vars,
                inputs=inputs,
                overrides=overrides,
            )
        finally:
            self.base_tags = original_tags

    async def post_graph_execute(
        self,
        run_id: str,
        graph: Any,
        success: bool,
        error: Exception | None,
        results: dict[str, Any] | None,
    ) -> None:
        """Flush tracker state after async graph execution completes."""
        try:
            await super().post_graph_execute(
                run_id=run_id,
                graph=graph,
                success=success,
                error=error,
                results=results,
            )
        finally:
            await _flush_async_tracker(self)


async def _flush_async_tracker(tracker: sdk_adapters.AsyncHamiltonTracker) -> None:
    client = getattr(tracker, "client", None)
    queue = getattr(client, "data_queue", None)
    if isinstance(queue, asyncio.Queue):
        await queue.put(None)
        interval = getattr(client, "flush_interval", 0.0)
        await asyncio.sleep(float(interval) if interval else 0.0)


def _merge_tags(
    base_tags: dict[str, str],
    provider: Callable[[], dict[str, str]] | None,
) -> dict[str, str]:
    merged = dict(base_tags)
    if provider is None:
        return merged
    merged.update(provider())
    return merged


__all__ = ["CodeAnatomyAsyncHamiltonTracker", "CodeAnatomyHamiltonTracker"]

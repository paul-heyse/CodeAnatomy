"""Hamilton tracker wrappers with CodeAnatomy run tag support."""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Sequence
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
        *args: object,
        **kwargs: object,
    ) -> None:
        """Stop the tracker after graph execution completes."""
        success, error, results = _coerce_post_args(list(args), kwargs)
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
        *args: object,
        **kwargs: object,
    ) -> None:
        """Flush tracker state after async graph execution completes."""
        success, error, results = _coerce_post_args(list(args), kwargs)
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


def _coerce_post_args(
    args: Sequence[object],
    kwargs: dict[str, object],
) -> tuple[bool, Exception | None, dict[str, Any] | None]:
    max_args = 3
    error_index = 1
    results_index = 2
    success = kwargs.pop("success", None)
    error = kwargs.pop("error", None)
    results = kwargs.pop("results", None)
    if kwargs:
        msg = f"Unexpected keyword arguments: {sorted(kwargs)}"
        raise TypeError(msg)
    if args:
        if len(args) > max_args:
            msg = "post_graph_execute received too many positional arguments"
            raise TypeError(msg)
        success = args[0] if len(args) > 0 else success
        error = args[error_index] if len(args) > error_index else error
        results = args[results_index] if len(args) > results_index else results
    if not isinstance(success, bool):
        msg = "post_graph_execute requires a boolean success flag"
        raise TypeError(msg)
    if error is not None and not isinstance(error, Exception):
        msg = "post_graph_execute error must be an Exception or None"
        raise TypeError(msg)
    if results is not None and not isinstance(results, dict):
        msg = "post_graph_execute results must be a dict or None"
        raise TypeError(msg)
    return success, error, results


__all__ = ["CodeAnatomyAsyncHamiltonTracker", "CodeAnatomyHamiltonTracker"]

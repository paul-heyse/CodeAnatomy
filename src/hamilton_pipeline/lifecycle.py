"""Hamilton lifecycle hooks for pipeline diagnostics."""

from __future__ import annotations

import time
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

from hamilton.lifecycle import api as lifecycle_api

from obs.diagnostics import DiagnosticsCollector

_DIAGNOSTICS_STATE: dict[str, DiagnosticsCollector | None] = {"collector": None}


def set_hamilton_diagnostics_collector(collector: DiagnosticsCollector | None) -> None:
    """Set the global diagnostics collector for Hamilton hooks."""
    _DIAGNOSTICS_STATE["collector"] = collector


def get_hamilton_diagnostics_collector() -> DiagnosticsCollector | None:
    """Return the configured diagnostics collector for Hamilton hooks.

    Returns
    -------
    DiagnosticsCollector | None
        Configured diagnostics collector when available.
    """
    return _DIAGNOSTICS_STATE["collector"]


@dataclass
class DiagnosticsNodeHook(lifecycle_api.NodeExecutionHook):
    """Record Hamilton node execution events in a DiagnosticsCollector."""

    collector: DiagnosticsCollector
    _starts: dict[tuple[str, str, str | None], float] = field(default_factory=dict)

    def run_before_node_execution(
        self,
        *,
        node_name: str,
        node_tags: Mapping[str, Any],
        run_id: str,
        task_id: str | None,
        **kwargs: Any,
    ) -> None:
        """Record diagnostics for a node starting execution."""
        key = (run_id, node_name, task_id)
        self._starts[key] = time.monotonic()
        node_input_types = kwargs.get("node_input_types", {})
        node_return_type = kwargs.get("node_return_type", object)
        self.collector.record_events(
            "hamilton_node_start_v1",
            [
                {
                    "run_id": run_id,
                    "task_id": task_id,
                    "node_name": node_name,
                    "node_tags": dict(node_tags),
                    "node_input_types": {k: str(v) for k, v in node_input_types.items()},
                    "node_return_type": str(node_return_type),
                }
            ],
        )

    def run_after_node_execution(
        self,
        *,
        node_name: str,
        node_tags: Mapping[str, Any],
        run_id: str,
        task_id: str | None,
        success: bool,
        **kwargs: Any,
    ) -> None:
        """Record diagnostics for a node after execution."""
        key = (run_id, node_name, task_id)
        start = self._starts.pop(key, None)
        duration_ms = None if start is None else int((time.monotonic() - start) * 1000)
        error = kwargs.get("error")
        payload = {
            "run_id": run_id,
            "task_id": task_id,
            "node_name": node_name,
            "node_tags": dict(node_tags),
            "success": bool(success),
            "duration_ms": duration_ms,
            "error": str(error) if error is not None else None,
        }
        self.collector.record_events("hamilton_node_finish_v1", [payload])


__all__ = [
    "DiagnosticsNodeHook",
    "get_hamilton_diagnostics_collector",
    "set_hamilton_diagnostics_collector",
]

"""Structured JSONL run logs for Hamilton executions."""

from __future__ import annotations

import time
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, cast

from hamilton.lifecycle import api as lifecycle_api

from core_types import JsonValue
from serde_artifact_specs import HAMILTON_RUN_LOG_SPEC
from serde_msgspec import JSON_ENCODER, StructBaseCompat

if TYPE_CHECKING:
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile


@dataclass
class StructuredLogHook(lifecycle_api.GraphExecutionHook):
    """Graph execution hook that emits structured JSONL logs."""

    profile: DataFusionRuntimeProfile
    config: Mapping[str, JsonValue]
    plan_signature: str
    _path: Path | None = None
    _event_count: int = 0
    _start_time: float | None = None

    def run_before_graph_execution(
        self,
        *,
        run_id: str,
        **kwargs: object,
    ) -> None:
        """Record a run-start event before graph execution."""
        _ = kwargs
        self._path = _resolve_log_path(self.config, run_id=run_id)
        self._event_count = 0
        self._start_time = time.monotonic()
        _append_event(
            self._path,
            GraphStartEvent(
                run_id=run_id,
                plan_signature=self.plan_signature,
                timestamp_ms=int(time.time() * 1000),
            ),
        )
        self._event_count += 1

    def run_after_graph_execution(
        self,
        *,
        run_id: str,
        success: bool,
        error: Exception | None,
        results: dict[str, object] | None,
        **kwargs: object,
    ) -> None:
        """Record a run-finish event after graph execution."""
        _ = results, kwargs
        path = self._path or _resolve_log_path(self.config, run_id=run_id)
        duration_ms = None
        if self._start_time is not None:
            duration_ms = int((time.monotonic() - self._start_time) * 1000)
        _append_event(
            path,
            GraphFinishEvent(
                run_id=run_id,
                plan_signature=self.plan_signature,
                timestamp_ms=int(time.time() * 1000),
                duration_ms=duration_ms,
                success=bool(success),
                error=str(error) if error is not None else None,
            ),
        )
        self._event_count += 1
        from datafusion_engine.lineage.diagnostics import record_artifact

        record_artifact(
            self.profile,
            HAMILTON_RUN_LOG_SPEC,
            {
                "run_id": run_id,
                "plan_signature": self.plan_signature,
                "path": str(path),
                "event_count": self._event_count,
                "success": bool(success),
                "duration_ms": duration_ms,
            },
        )


def _resolve_log_path(config: Mapping[str, JsonValue], *, run_id: str) -> Path:
    hamilton_config = config.get("hamilton")
    hamilton_payload = (
        cast("Mapping[str, JsonValue]", hamilton_config)
        if isinstance(hamilton_config, Mapping)
        else dict[str, JsonValue]()
    )
    explicit = hamilton_payload.get("structured_log_path") or hamilton_payload.get("run_log_path")
    if isinstance(explicit, str) and explicit:
        base = Path(explicit).expanduser()
        if base.suffix:
            return base
        return base / f"{run_id}.jsonl"
    cache_config = config.get("cache")
    cache_payload = (
        cast("Mapping[str, JsonValue]", cache_config)
        if isinstance(cache_config, Mapping)
        else dict[str, JsonValue]()
    )
    cache_path = cache_payload.get("path")
    if isinstance(cache_path, str) and cache_path:
        return Path(cache_path).expanduser() / "structured_logs" / f"{run_id}.jsonl"
    return Path("build") / "structured_logs" / f"{run_id}.jsonl"


class StructuredLogEvent(StructBaseCompat, tag=True, tag_field="event", frozen=True):
    """Base structured log event."""

    run_id: str
    plan_signature: str
    timestamp_ms: int


class GraphStartEvent(StructuredLogEvent, tag="graph_start", frozen=True):
    """Graph-start log event."""


class GraphFinishEvent(StructuredLogEvent, tag="graph_finish", frozen=True):
    """Graph-finish log event."""

    duration_ms: int | None = None
    success: bool = True
    error: str | None = None


def _append_event(path: Path, payload: StructuredLogEvent) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    data = JSON_ENCODER.encode_lines([payload])
    with path.open("ab") as handle:
        handle.write(data)


__all__ = ["StructuredLogHook"]

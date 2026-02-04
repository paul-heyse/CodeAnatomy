"""Graph snapshot export utilities for Hamilton runs."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Protocol, cast

from hamilton import driver as hamilton_driver
from hamilton.lifecycle import api as lifecycle_api

from core_types import JsonValue

_GRAPH_SNAPSHOT_DIRNAME = "graph_snapshots"

if TYPE_CHECKING:
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile


class _GraphSnapshotDriver(Protocol):
    def visualize_execution(
        self,
        final_vars: list[str],
        *,
        output_file_path: str | None = None,
        inputs: Mapping[str, object] | None = None,
        overrides: Mapping[str, object] | None = None,
    ) -> object | None:
        """Write a graph snapshot to the requested path."""


@dataclass
class GraphSnapshotHook(lifecycle_api.GraphExecutionHook):
    """Graph execution hook that captures a graph snapshot."""

    profile: DataFusionRuntimeProfile
    plan_signature: str
    config: Mapping[str, JsonValue]
    _driver: _GraphSnapshotDriver | None = None

    def bind_driver(self, driver: hamilton_driver.Driver) -> None:
        """Bind a Hamilton driver so snapshots can be emitted."""
        self._driver = cast("_GraphSnapshotDriver", driver)

    def run_before_graph_execution(self, **kwargs: object) -> None:
        """Emit a graph snapshot before graph execution."""
        hamilton_config = self.config.get("hamilton")
        hamilton_payload = (
            cast("Mapping[str, JsonValue]", hamilton_config)
            if isinstance(hamilton_config, Mapping)
            else dict[str, JsonValue]()
        )
        enable_snapshot = hamilton_payload.get("enable_graph_snapshot")
        if not isinstance(enable_snapshot, bool):
            enable_snapshot = True
        if not enable_snapshot:
            return
        driver = self._driver
        if driver is None:
            return
        final_vars = cast("list[str]", kwargs.get("final_vars", []))
        inputs = cast("Mapping[str, object]", kwargs.get("inputs", {}))
        overrides = cast("Mapping[str, object]", kwargs.get("overrides", {}))
        run_id_value = kwargs.get("run_id")
        run_id = run_id_value if isinstance(run_id_value, str) else "unknown"
        path = _graph_snapshot_path(self.config, plan_signature=self.plan_signature)
        path.parent.mkdir(parents=True, exist_ok=True)
        error: str | None = None
        success = False
        try:
            driver.visualize_execution(
                final_vars=final_vars,
                output_file_path=str(path),
                inputs=dict(inputs),
                overrides=dict(overrides),
            )
            success = True
        except (ImportError, OSError, RuntimeError, ValueError) as exc:
            error = f"{type(exc).__name__}: {exc}"
        from datafusion_engine.lineage.diagnostics import record_artifact

        record_artifact(
            self.profile,
            "hamilton_graph_snapshot_v1",
            {
                "run_id": run_id,
                "plan_signature": self.plan_signature,
                "path": str(path),
                "success": success,
                "error": error,
            },
        )

    def run_after_graph_execution(
        self,
        *,
        run_id: str,
        **kwargs: object,
    ) -> None:
        """No-op after graph execution to satisfy the hook contract."""
        _ = self, run_id, kwargs


def _graph_snapshot_path(
    config: Mapping[str, JsonValue],
    *,
    plan_signature: str,
) -> Path:
    hamilton_config = config.get("hamilton")
    hamilton_payload = (
        cast("Mapping[str, JsonValue]", hamilton_config)
        if isinstance(hamilton_config, Mapping)
        else dict[str, JsonValue]()
    )
    explicit = hamilton_payload.get("graph_snapshot_path") or hamilton_payload.get(
        "graph_snapshot_hamilton_path"
    )
    if isinstance(explicit, str) and explicit:
        base = Path(explicit).expanduser()
        if base.suffix:
            return base
        return base / f"{plan_signature}.png"
    cache_config = config.get("cache")
    cache_payload = (
        cast("Mapping[str, JsonValue]", cache_config)
        if isinstance(cache_config, Mapping)
        else dict[str, JsonValue]()
    )
    cache_path = cache_payload.get("path")
    if isinstance(cache_path, str) and cache_path:
        return Path(cache_path).expanduser() / _GRAPH_SNAPSHOT_DIRNAME / f"{plan_signature}.png"
    return Path("build") / "structured_logs" / _GRAPH_SNAPSHOT_DIRNAME / f"{plan_signature}.png"


__all__ = ["GraphSnapshotHook"]

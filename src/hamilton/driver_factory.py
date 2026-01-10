from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence

from hamilton import driver


def default_modules() -> List[Any]:
    """
    Default Hamilton module set for the CodeIntel CPG pipeline.
    """
    from .modules import inputs, extraction, normalization, cpg_build, outputs

    return [inputs, extraction, normalization, cpg_build, outputs]


def config_fingerprint(config: Mapping[str, Any]) -> str:
    """
    Stable config fingerprint used to cache built Driver instances.

    Hamilton build-time config is immutable after build; if config changes, rebuild a new driver.
    (The Hamilton docs recommend a small driver-factory that caches by config fingerprint.) :contentReference[oaicite:2]{index=2}
    """
    payload = json.dumps(config, sort_keys=True, default=str).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _maybe_build_tracker_adapter(config: Mapping[str, Any]) -> Optional[Any]:
    """
    Optional Hamilton UI tracker adapter (if hamilton_sdk is installed).

    Docs show:
      tracker = adapters.HamiltonTracker(project_id=..., username=..., dag_name=..., tags=...)
      Builder().with_modules(...).with_config(...).with_adapters(tracker).build() :contentReference[oaicite:3]{index=3}
    """
    enable = bool(config.get("enable_hamilton_tracker", False))
    if not enable:
        return None

    project_id = config.get("hamilton_project_id")
    username = config.get("hamilton_username")
    dag_name = config.get("hamilton_dag_name", "codeintel_cpg_v1")
    tags = dict(config.get("hamilton_tags", {}))

    if not project_id or not username:
        # Don’t raise — keep this optional.
        return None

    try:
        from hamilton_sdk import adapters  # type: ignore
    except Exception:
        return None

    return adapters.HamiltonTracker(
        project_id=project_id,
        username=username,
        dag_name=dag_name,
        tags=tags,
        # optional overrides:
        hamilton_api_url=config.get("hamilton_api_url"),
        hamilton_ui_url=config.get("hamilton_ui_url"),
    )


def build_driver(
    *,
    config: Mapping[str, Any],
    modules: Optional[Sequence[Any]] = None,
) -> Any:
    """
    Build a Hamilton Driver for the CodeIntel CPG pipeline.

    Key knobs supported via config:
      - enable_dynamic_execution: bool (optional)
      - cache_path: str | None
      - cache_opt_in: bool (if True, default_behavior="disable")
      - enable_hamilton_tracker + tracker config keys
    """
    modules = list(modules) if modules is not None else default_modules()

    b = driver.Builder().with_modules(*modules).with_config(dict(config))

    # Optional: dynamic execution (Parallelizable/Collect) — not required for this pipeline,
    # but supported for future “fan-out per-file” execution.
    if bool(config.get("enable_dynamic_execution", False)):
        from hamilton.execution import executors

        b = (
            b.enable_dynamic_execution(allow_experimental_mode=True)
             .with_local_executor(executors.SynchronousLocalTaskExecutor())
             .with_remote_executor(executors.MultiProcessingExecutor(max_tasks=int(config.get("max_tasks", 4))))
        )

    # Optional: caching
    cache_path = config.get("cache_path")
    if cache_path:
        cache_opt_in = bool(config.get("cache_opt_in", True))
        if cache_opt_in:
            # cache only nodes annotated for caching
            b = b.with_cache(path=str(cache_path), default_behavior="disable", log_to_file=True)
        else:
            # cache everything (aggressive)
            b = b.with_cache(path=str(cache_path), log_to_file=True)

    # Optional: UI tracker adapter
    tracker = _maybe_build_tracker_adapter(config)
    if tracker is not None:
        b = b.with_adapters(tracker)

    return b.build()


@dataclass
class DriverFactory:
    """
    Caches built Hamilton Drivers by config fingerprint.

    Use this if you're embedding the pipeline into a service where config changes
    are relatively infrequent but executions are frequent.
    """
    modules: Optional[Sequence[Any]] = None
    _cache: Dict[str, Any] = None  # fingerprint -> Driver

    def __post_init__(self) -> None:
        if self._cache is None:
            self._cache = {}

    def get(self, config: Mapping[str, Any]) -> Any:
        fp = config_fingerprint(config)
        if fp in self._cache:
            return self._cache[fp]
        dr = build_driver(config=config, modules=self.modules)
        self._cache[fp] = dr
        return dr

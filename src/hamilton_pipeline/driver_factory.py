"""Hamilton driver construction helpers for the pipeline."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from types import ModuleType
from typing import TypedDict, cast

from hamilton import driver
from hamilton.execution import executors
from hamilton.lifecycle import base as lifecycle_base

from core_types import JsonValue
from hamilton_pipeline.modules import ALL_MODULES

try:
    from hamilton_sdk import adapters as hamilton_adapters
except ModuleNotFoundError:  # pragma: no cover - optional dependency
    hamilton_adapters = None


def default_modules() -> list[ModuleType]:
    """Return the default Hamilton module set for the pipeline.

    Returns
    -------
    list[ModuleType]
        Default module list for the pipeline.
    """
    return list(ALL_MODULES)


def config_fingerprint(config: Mapping[str, JsonValue]) -> str:
    """Compute a stable config fingerprint for driver caching.

    Hamilton build-time config is immutable after build; if config changes, rebuild a new driver.
    (The Hamilton docs recommend a small driver-factory that caches by config fingerprint.) :contentReference[oaicite:2]{index=2}

    Returns
    -------
    str
        SHA-256 fingerprint for the config.
    """
    payload = json.dumps(config, sort_keys=True, default=str).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _maybe_build_tracker_adapter(
    config: Mapping[str, JsonValue],
) -> lifecycle_base.LifecycleAdapter | None:
    """Build an optional Hamilton UI tracker adapter.

    Docs show:
      tracker = adapters.HamiltonTracker(project_id=..., username=..., dag_name=..., tags=...)
      Builder().with_modules(...).with_config(...).with_adapters(tracker).build() :contentReference[oaicite:3]{index=3}

    Returns
    -------
    object | None
        Tracker adapter when enabled and available.
    """
    if hamilton_adapters is None:
        return None

    enable = bool(config.get("enable_hamilton_tracker", False))
    if not enable:
        return None

    project_id_value = config.get("hamilton_project_id")
    project_id: int | None = None
    if isinstance(project_id_value, int) and not isinstance(project_id_value, bool):
        project_id = project_id_value
    elif isinstance(project_id_value, str) and project_id_value.isdigit():
        project_id = int(project_id_value)

    username = config.get("hamilton_username")
    if project_id is None or not isinstance(username, str):
        return None

    dag_name_value = config.get("hamilton_dag_name")
    dag_name = dag_name_value if isinstance(dag_name_value, str) else "codeintel_cpg_v1"

    tags_value = config.get("hamilton_tags")
    tags: dict[str, str] = {}
    if isinstance(tags_value, Mapping):
        tags = {str(k): str(v) for k, v in tags_value.items()}

    api_url_value = config.get("hamilton_api_url")
    api_url = api_url_value if isinstance(api_url_value, str) else None
    ui_url_value = config.get("hamilton_ui_url")
    ui_url = ui_url_value if isinstance(ui_url_value, str) else None

    class _TrackerKwargs(TypedDict, total=False):
        project_id: int
        username: str
        dag_name: str
        tags: dict[str, str]
        hamilton_api_url: str
        hamilton_ui_url: str

    tracker_kwargs: _TrackerKwargs = {
        "project_id": project_id,
        "username": username,
        "dag_name": dag_name,
        "tags": tags,
    }
    if api_url is not None:
        tracker_kwargs["hamilton_api_url"] = api_url
    if ui_url is not None:
        tracker_kwargs["hamilton_ui_url"] = ui_url

    tracker = hamilton_adapters.HamiltonTracker(**tracker_kwargs)
    return cast("lifecycle_base.LifecycleAdapter", tracker)


def build_driver(
    *,
    config: Mapping[str, JsonValue],
    modules: Sequence[ModuleType] | None = None,
) -> driver.Driver:
    """Build a Hamilton Driver for the pipeline.

    Key knobs supported via config:
      - enable_dynamic_execution: bool (optional)
      - cache_path: str | None
      - cache_opt_in: bool (if True, default_behavior="disable")
      - enable_hamilton_tracker + tracker config keys

    Returns
    -------
    driver.Driver
        Built Hamilton driver instance.
    """
    modules = list(modules) if modules is not None else default_modules()

    b = driver.Builder().with_modules(*modules).with_config(dict(config))

    # Optional: dynamic execution (Parallelizable/Collect) — not required for this pipeline,
    # but supported for future “fan-out per-file” execution.
    if bool(config.get("enable_dynamic_execution", False)):
        max_tasks_value = config.get("max_tasks")
        max_tasks = 4
        if isinstance(max_tasks_value, int) and not isinstance(max_tasks_value, bool):
            max_tasks = max_tasks_value

        b = (
            b.enable_dynamic_execution(allow_experimental_mode=True)
            .with_local_executor(executors.SynchronousLocalTaskExecutor())
            .with_remote_executor(executors.MultiProcessingExecutor(max_tasks=max_tasks))
        )

    cache_path = config.get("cache_path")
    if isinstance(cache_path, str) and cache_path:
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

    modules: Sequence[ModuleType] | None = None
    _cache: dict[str, driver.Driver] = field(default_factory=dict)  # fingerprint -> Driver

    def get(self, config: Mapping[str, JsonValue]) -> driver.Driver:
        """Return a cached driver for the given config.

        Returns
        -------
        driver.Driver
            Cached or newly built Hamilton driver.
        """
        fp = config_fingerprint(config)
        if fp in self._cache:
            return self._cache[fp]
        dr = build_driver(config=config, modules=self.modules)
        self._cache[fp] = dr
        return dr

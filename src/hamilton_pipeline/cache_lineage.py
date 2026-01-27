"""Cache lineage export utilities for Hamilton runs."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from hamilton.caching.cache_key import decode_key
from hamilton.lifecycle import api as lifecycle_api

from core_types import JsonValue

_NODE_KEY_LEN = 2

if TYPE_CHECKING:
    from hamilton import driver as hamilton_driver

    from datafusion_engine.runtime import DataFusionRuntimeProfile


@dataclass(frozen=True)
class CacheLineageSummary:
    """Summary information for a cache lineage export."""

    path: Path
    run_id: str
    record_count: int
    error_count: int
    log_count: int
    metadata_count: int
    rows: tuple[dict[str, object], ...]


def export_cache_lineage_jsonl(
    *,
    driver: hamilton_driver.Driver,
    run_id: str,
    out_path: Path,
    plan_signature: str | None = None,
) -> CacheLineageSummary:
    """Export cache lineage details for a run into JSONL format.

    Returns
    -------
    CacheLineageSummary
        Summary of records written and errors encountered.
    """
    out_path.parent.mkdir(parents=True, exist_ok=True)
    cache = driver.cache
    logs_by_node = cache.logs(run_id=run_id, level="debug")
    log_rows, log_errors = _lineage_rows_from_logs(
        cache=cache,
        run_id=run_id,
        logs_by_node=logs_by_node,
        plan_signature=plan_signature,
    )
    metadata_rows, metadata_errors = _lineage_rows_from_metadata_store(
        cache=cache,
        run_id=run_id,
        plan_signature=plan_signature,
    )
    merged_rows = _merge_lineage_rows(log_rows=log_rows, metadata_rows=metadata_rows)
    record_count = len(merged_rows)
    error_count = log_errors + metadata_errors
    with out_path.open("w", encoding="utf-8") as handle:
        for record in merged_rows:
            handle.write(json.dumps(record, sort_keys=True))
            handle.write("\n")
    return CacheLineageSummary(
        path=out_path,
        run_id=run_id,
        record_count=record_count,
        error_count=error_count,
        log_count=len(log_rows),
        metadata_count=len(metadata_rows),
        rows=tuple(merged_rows),
    )


@dataclass
class CacheLineageHook(lifecycle_api.GraphExecutionHook):
    """Graph execution hook that exports cache lineage after a run."""

    profile: DataFusionRuntimeProfile
    config: Mapping[str, JsonValue]
    plan_signature: str
    _driver: hamilton_driver.Driver | None = None

    def bind_driver(self, driver: hamilton_driver.Driver) -> None:
        """Bind a Hamilton driver so cache lineage can be exported."""
        self._driver = driver

    def run_before_graph_execution(
        self,
        *,
        run_id: str,
        **kwargs: object,
    ) -> None:
        """No-op before graph execution to satisfy the hook contract."""
        _ = self, run_id, kwargs

    def run_after_graph_execution(
        self,
        *,
        graph: object,
        success: bool,
        error: Exception | None,
        results: dict[str, object] | None,
        run_id: str,
        **future_kwargs: object,
    ) -> None:
        """Export cache lineage after graph execution when enabled."""
        _ = graph, success, error, results, future_kwargs
        driver = self._driver
        if driver is None:
            return
        if not bool(self.config.get("enable_cache_lineage", True)):
            return
        out_path = _lineage_path(self.config, run_id=run_id)
        summary = export_cache_lineage_jsonl(
            driver=driver,
            run_id=run_id,
            out_path=out_path,
            plan_signature=self.plan_signature,
        )
        from datafusion_engine.diagnostics import record_cache_lineage

        record_cache_lineage(
            self.profile,
            summary={
                "run_id": run_id,
                "plan_signature": self.plan_signature,
                "path": str(summary.path),
                "record_count": summary.record_count,
                "error_count": summary.error_count,
                "log_count": summary.log_count,
                "metadata_count": summary.metadata_count,
            },
            rows=summary.rows,
        )


def _lineage_path(config: Mapping[str, JsonValue], *, run_id: str) -> Path:
    explicit = config.get("cache_lineage_path")
    if isinstance(explicit, str) and explicit:
        return Path(explicit).expanduser()
    cache_path = config.get("cache_path")
    if not isinstance(cache_path, str) or not cache_path:
        return Path("build") / "cache_lineage" / f"{run_id}.jsonl"
    base = Path(cache_path).expanduser()
    return base / "lineage" / f"{run_id}.jsonl"


def _node_and_task_id(node_key: object) -> tuple[str, str | None]:
    if isinstance(node_key, tuple) and len(node_key) == _NODE_KEY_LEN:
        name, task_id = node_key
        if isinstance(name, str) and (isinstance(task_id, str) or task_id is None):
            return name, task_id
    if isinstance(node_key, str):
        return node_key, None
    return str(node_key), None


def _cache_key_dependencies(cache_key: str | None) -> dict[str, str]:
    if cache_key is None:
        return {}
    decoded = decode_key(cache_key)
    dependencies_value = decoded.get("dependencies_data_versions")
    if not isinstance(dependencies_value, Mapping):
        return {}
    dependencies: dict[str, str] = {}
    for key, value in dependencies_value.items():
        if not isinstance(key, str):
            continue
        if isinstance(value, str):
            dependencies[key] = value
            continue
        dependencies[key] = str(value)
    return dependencies


def _lineage_rows_from_logs(
    *,
    cache: object,
    run_id: str,
    logs_by_node: Mapping[object, Sequence[object]],
    plan_signature: str | None,
) -> tuple[list[dict[str, object]], int]:
    rows: list[dict[str, object]] = []
    error_count = 0
    for node_key, events in sorted(logs_by_node.items(), key=lambda item: str(item[0])):
        node_name, task_id = _node_and_task_id(node_key)
        event_types = [
            getattr(getattr(event, "event_type", None), "value", "unknown") for event in events
        ]
        cache_key_value = None
        data_version_value: str | None = None
        code_version_value: str | None = None
        error_value: str | None = None
        try:
            get_cache_key = getattr(cache, "get_cache_key", None)
            cache_key_value = (
                get_cache_key(run_id=run_id, node_name=node_name, task_id=task_id)
                if callable(get_cache_key)
                else None
            )
            cache_key_str = cache_key_value if isinstance(cache_key_value, str) else None
            get_data_version = getattr(cache, "get_data_version", None)
            data_version = (
                get_data_version(
                    run_id=run_id,
                    node_name=node_name,
                    cache_key=cache_key_str,
                    task_id=task_id,
                )
                if callable(get_data_version)
                else None
            )
            if isinstance(data_version, str):
                data_version_value = data_version
            elif data_version is not None:
                data_version_value = str(data_version)
            get_code_version = getattr(cache, "get_code_version", None)
            code_version = (
                get_code_version(run_id=run_id, node_name=node_name, task_id=task_id)
                if callable(get_code_version)
                else None
            )
            if isinstance(code_version, str):
                code_version_value = code_version
            elif code_version is not None:
                code_version_value = str(code_version)
        except (KeyError, TypeError, ValueError) as exc:
            error_count += 1
            error_value = f"{type(exc).__name__}: {exc}"
        cache_key_str = cache_key_value if isinstance(cache_key_value, str) else None
        rows.append(
            {
                "run_id": run_id,
                "plan_signature": plan_signature,
                "node_name": node_name,
                "task_id": task_id,
                "source": "logs",
                "cache_key": cache_key_str,
                "data_version": data_version_value,
                "code_version": code_version_value,
                "dependencies_data_versions": _cache_key_dependencies(cache_key_str),
                "event_types": tuple(sorted(event_types)),
                "event_count": len(event_types),
                "error": error_value,
            }
        )
    return rows, error_count


def _lineage_rows_from_metadata_store(
    *,
    cache: object,
    run_id: str,
    plan_signature: str | None,
) -> tuple[list[dict[str, object]], int]:
    metadata_store = getattr(cache, "metadata_store", None)
    get_run = getattr(metadata_store, "get_run", None)
    if not callable(get_run):
        return [], 0
    try:
        run_meta = get_run(run_id)
    except (KeyError, TypeError, ValueError):
        return [], 0
    nodes = getattr(run_meta, "nodes", None)
    if not isinstance(nodes, Mapping):
        return [], 0
    rows: list[dict[str, object]] = []
    error_count = 0
    for node_name, meta in sorted(nodes.items(), key=lambda item: str(item[0])):
        if not isinstance(node_name, str):
            continue
        task_id_value = getattr(meta, "task_id", None)
        task_id = task_id_value if isinstance(task_id_value, str) else None
        cache_key_value = getattr(meta, "cache_key", None)
        cache_key_str = cache_key_value if isinstance(cache_key_value, str) else None
        data_version_value = _stringify(getattr(meta, "data_version", None))
        code_version_value = _stringify(getattr(meta, "code_version", None))
        error_value: str | None = None
        try:
            dependencies = _cache_key_dependencies(cache_key_str)
        except (TypeError, ValueError) as exc:
            error_count += 1
            dependencies: dict[str, str] = {}
            error_value = f"{type(exc).__name__}: {exc}"
        rows.append(
            {
                "run_id": run_id,
                "plan_signature": plan_signature,
                "node_name": node_name,
                "task_id": task_id,
                "source": "metadata_store",
                "cache_key": cache_key_str,
                "data_version": data_version_value,
                "code_version": code_version_value,
                "dependencies_data_versions": dependencies,
                "event_types": (),
                "event_count": 0,
                "error": error_value,
            }
        )
    return rows, error_count


def _stringify(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    return str(value)


def _merge_lineage_rows(
    *,
    log_rows: Sequence[Mapping[str, object]],
    metadata_rows: Sequence[Mapping[str, object]],
) -> list[dict[str, object]]:
    merged: dict[tuple[str, str | None], dict[str, object]] = {}
    for row in log_rows:
        node_name_value = row.get("node_name")
        if not isinstance(node_name_value, str):
            continue
        task_id_value = row.get("task_id")
        task_id = task_id_value if isinstance(task_id_value, str) else None
        key = (node_name_value, task_id)
        merged[key] = dict(row)
    for row in metadata_rows:
        node_name_value = row.get("node_name")
        if not isinstance(node_name_value, str):
            continue
        task_id_value = row.get("task_id")
        task_id = task_id_value if isinstance(task_id_value, str) else None
        key = (node_name_value, task_id)
        existing = merged.get(key)
        if existing is None:
            merged[key] = dict(row)
            continue
        # Prefer authoritative metadata-store facts but retain log event types.
        event_types_value = existing.get("event_types")
        event_types: set[str] = set()
        if isinstance(event_types_value, Sequence) and not isinstance(
            event_types_value,
            (str, bytes),
        ):
            event_types = {item for item in event_types_value if isinstance(item, str)}
        merged_row = dict(row)
        if event_types:
            merged_row["event_types"] = tuple(sorted(event_types))
            merged_row["event_count"] = len(event_types)
        merged[key] = merged_row
    ordered = list(merged.values())
    ordered.sort(
        key=lambda item: (
            str(item.get("node_name")),
            str(item.get("task_id")),
            str(item.get("source")),
        )
    )
    return ordered


__all__ = [
    "CacheLineageHook",
    "CacheLineageSummary",
    "export_cache_lineage_jsonl",
]

"""Cache lineage export utilities for Hamilton runs."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, cast

from hamilton.caching.cache_key import decode_key
from hamilton.lifecycle import api as lifecycle_api

from core_types import JsonValue

_NODE_KEY_LEN = 2
_CACHE_LINEAGE_DIRNAME = "cache_lineage"
_CACHE_LINEAGE_FILENAME = "cache_lineage.json"

if TYPE_CHECKING:
    from hamilton import driver as hamilton_driver

    from datafusion_engine.session.runtime import DataFusionRuntimeProfile


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


def export_cache_lineage_artifacts(
    *,
    driver: hamilton_driver.Driver,
    run_id: str,
    out_dir: Path,
    plan_signature: str | None = None,
) -> CacheLineageSummary:
    """Export cache lineage details for a run into artifacts.

    Returns:
    -------
    CacheLineageSummary
        Summary of records written and errors encountered.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    cache = driver.cache
    semantic_ids = _semantic_ids_by_node(driver)
    logs_by_node = cache.logs(run_id=run_id, level="debug")
    log_rows, log_errors = _lineage_rows_from_logs(
        cache=cache,
        run_id=run_id,
        logs_by_node=logs_by_node,
        plan_signature=plan_signature,
        semantic_ids=semantic_ids,
    )
    metadata_rows, metadata_errors = _lineage_rows_from_metadata_store(
        cache=cache,
        run_id=run_id,
        plan_signature=plan_signature,
        semantic_ids=semantic_ids,
    )
    merged_rows = _merge_lineage_rows(log_rows=log_rows, metadata_rows=metadata_rows)
    record_count = len(merged_rows)
    error_count = log_errors + metadata_errors
    artifact_path = out_dir / _CACHE_LINEAGE_FILENAME
    summary = CacheLineageSummary(
        path=artifact_path,
        run_id=run_id,
        record_count=record_count,
        error_count=error_count,
        log_count=len(log_rows),
        metadata_count=len(metadata_rows),
        rows=tuple(merged_rows),
    )
    _write_cache_lineage(artifact_path, summary)
    return summary


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
        hamilton_config = self.config.get("hamilton")
        hamilton_payload = (
            cast("Mapping[str, JsonValue]", hamilton_config)
            if isinstance(hamilton_config, Mapping)
            else dict[str, JsonValue]()
        )
        enable_cache_lineage = hamilton_payload.get("enable_cache_lineage")
        if not isinstance(enable_cache_lineage, bool):
            enable_cache_lineage = True
        if not enable_cache_lineage:
            return
        out_path = _lineage_path(self.config, run_id=run_id)
        summary = export_cache_lineage_artifacts(
            driver=driver,
            run_id=run_id,
            out_dir=out_path,
            plan_signature=self.plan_signature,
        )
        from datafusion_engine.lineage.diagnostics import record_cache_lineage

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
    hamilton_config = config.get("hamilton")
    hamilton_payload = (
        cast("Mapping[str, JsonValue]", hamilton_config)
        if isinstance(hamilton_config, Mapping)
        else dict[str, JsonValue]()
    )
    explicit = hamilton_payload.get("cache_lineage_path")
    if isinstance(explicit, str) and explicit:
        return Path(explicit).expanduser()
    cache_config = config.get("cache")
    cache_payload = (
        cast("Mapping[str, JsonValue]", cache_config)
        if isinstance(cache_config, Mapping)
        else dict[str, JsonValue]()
    )
    cache_path = cache_payload.get("path")
    if not isinstance(cache_path, str) or not cache_path:
        return Path("build") / "structured_logs" / _CACHE_LINEAGE_DIRNAME / run_id
    base = Path(cache_path).expanduser()
    return base / "lineage" / run_id


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
    semantic_ids: Mapping[str, str] | None = None,
) -> tuple[list[dict[str, object]], int]:
    rows: list[dict[str, object]] = []
    error_count = 0
    for node_key, events in sorted(logs_by_node.items(), key=lambda item: str(item[0])):
        node_name, task_id = _node_and_task_id(node_key)
        semantic_id = _semantic_id_for_node(node_name, semantic_ids)
        event_types = [
            getattr(getattr(event, "event_type", None), "value", "unknown") for event in events
        ]
        cache_key_str, data_version_value, code_version_value, error_value = (
            _cache_versions_for_node(
                cache,
                run_id=run_id,
                node_name=node_name,
                task_id=task_id,
            )
        )
        if error_value is not None:
            error_count += 1
        rows.append(
            {
                "run_id": run_id,
                "plan_signature": plan_signature,
                "node_name": node_name,
                "semantic_id": semantic_id,
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


def _cache_versions_for_node(
    cache: object,
    *,
    run_id: str,
    node_name: str,
    task_id: str | None,
) -> tuple[str | None, str | None, str | None, str | None]:
    cache_key_value = None
    data_version_value: str | None = None
    code_version_value: str | None = None
    error_value: str | None = None
    cache_key_str: str | None = None
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
        data_version_value = _stringify(data_version)
        get_code_version = getattr(cache, "get_code_version", None)
        code_version = (
            get_code_version(run_id=run_id, node_name=node_name, task_id=task_id)
            if callable(get_code_version)
            else None
        )
        code_version_value = _stringify(code_version)
    except (KeyError, TypeError, ValueError) as exc:
        error_value = f"{type(exc).__name__}: {exc}"
    return cache_key_str, data_version_value, code_version_value, error_value


def _nodes_from_run_meta(run_meta: object) -> Mapping[str, object] | None:
    nodes = getattr(run_meta, "nodes", None)
    if isinstance(nodes, Mapping):
        return nodes
    if isinstance(run_meta, Mapping):
        candidate = run_meta.get("nodes")
        if isinstance(candidate, Mapping):
            return candidate
    if isinstance(run_meta, Sequence) and not isinstance(run_meta, (str, bytes)):
        mapped: dict[str, object] = {}
        for entry in run_meta:
            if not isinstance(entry, Mapping):
                continue
            node_name = entry.get("node_name")
            if isinstance(node_name, str) and node_name:
                mapped[node_name] = entry
        if mapped:
            return mapped
    return None


def _meta_attr(meta: object, name: str) -> object | None:
    if isinstance(meta, Mapping):
        return meta.get(name)
    return getattr(meta, name, None)


def _lineage_rows_from_metadata_store(
    *,
    cache: object,
    run_id: str,
    plan_signature: str | None,
    semantic_ids: Mapping[str, str] | None = None,
) -> tuple[list[dict[str, object]], int]:
    metadata_store = getattr(cache, "metadata_store", None)
    get_run = getattr(metadata_store, "get_run", None)
    if not callable(get_run):
        return [], 0
    try:
        run_meta = get_run(run_id)
    except (KeyError, TypeError, ValueError):
        return [], 0
    nodes = _nodes_from_run_meta(run_meta)
    if nodes is None:
        return [], 0
    rows: list[dict[str, object]] = []
    error_count = 0
    for node_name, meta in sorted(nodes.items(), key=lambda item: str(item[0])):
        if not isinstance(node_name, str):
            continue
        semantic_id = _semantic_id_for_node(node_name, semantic_ids)
        task_id_value = _meta_attr(meta, "task_id")
        task_id = task_id_value if isinstance(task_id_value, str) else None
        cache_key_value = _meta_attr(meta, "cache_key")
        cache_key_str = cache_key_value if isinstance(cache_key_value, str) else None
        data_version_value = _stringify(_meta_attr(meta, "data_version"))
        code_version_value = _stringify(_meta_attr(meta, "code_version"))
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
                "semantic_id": semantic_id,
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


def _semantic_ids_by_node(
    driver: hamilton_driver.Driver,
) -> Mapping[str, str]:
    nodes = driver.list_available_variables(tag_filter={"layer": "semantic"})
    semantic_ids: dict[str, str] = {}
    for node in nodes:
        tags = node.tags
        if not isinstance(tags, Mapping):
            continue
        semantic_id_value = tags.get("semantic_id")
        if isinstance(semantic_id_value, str) and semantic_id_value:
            semantic_ids[node.name] = semantic_id_value
    return semantic_ids


def _semantic_id_for_node(
    node_name: str,
    semantic_ids: Mapping[str, str] | None,
) -> str | None:
    if semantic_ids is None:
        return None
    return semantic_ids.get(node_name)


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


def _write_cache_lineage(path: Path, summary: CacheLineageSummary) -> None:
    payload = {
        "run_id": summary.run_id,
        "path": str(summary.path),
        "record_count": summary.record_count,
        "error_count": summary.error_count,
        "log_count": summary.log_count,
        "metadata_count": summary.metadata_count,
        "rows": list(summary.rows),
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=True, sort_keys=True))
        handle.write("\n")


__all__ = [
    "CacheLineageHook",
    "CacheLineageSummary",
    "export_cache_lineage_artifacts",
]

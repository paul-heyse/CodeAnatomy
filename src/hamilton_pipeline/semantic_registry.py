"""Plan-native semantic registry compilation from Hamilton tags."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from hamilton.lifecycle import api as lifecycle_api

from core_types import JsonValue
from utils.registry_protocol import MutableRegistry

_SEMANTIC_LAYER = "semantic"
_REQUIRED_SEMANTIC_TAGS: frozenset[str] = frozenset(
    {
        "layer",
        "artifact",
        "semantic_id",
        "kind",
        "entity",
        "grain",
        "version",
        "stability",
    }
)
_REQUIRED_TABLE_TAGS: frozenset[str] = frozenset({"schema_ref", "entity_keys", "join_keys"})
_REQUIRED_NON_TABLE_TAGS: frozenset[str] = frozenset({"dtype"})
_ERROR_PREVIEW_LIMIT = 8

if TYPE_CHECKING:
    from hamilton import driver as hamilton_driver
    from hamilton import node as hamilton_node

    from datafusion_engine.session.runtime import DataFusionRuntimeProfile


@dataclass(frozen=True)
class SemanticNodeRecord:
    """Normalized semantic metadata for a single Hamilton node."""

    node_name: str
    semantic_id: str
    kind: str
    plan_signature: str
    version: str | None = None
    entity: str | None = None
    grain: str | None = None
    stability: str | None = None
    schema_ref: str | None = None
    materialization: str | None = None
    materialized_name: str | None = None
    entity_keys: tuple[str, ...] = ()
    join_keys: tuple[str, ...] = ()
    schedule_index: str | None = None
    generation_index: str | None = None
    generation_order: str | None = None
    generation_size: str | None = None

    def payload(self) -> dict[str, object]:
        """Return a deterministic JSON-ready payload.

        Returns
        -------
        dict[str, object]
            JSON-ready payload for the node record.
        """
        return {
            "node_name": self.node_name,
            "semantic_id": self.semantic_id,
            "kind": self.kind,
            "plan_signature": self.plan_signature,
            "version": self.version,
            "entity": self.entity,
            "grain": self.grain,
            "stability": self.stability,
            "schema_ref": self.schema_ref,
            "materialization": self.materialization,
            "materialized_name": self.materialized_name,
            "entity_keys": list(self.entity_keys),
            "join_keys": list(self.join_keys),
            "schedule_index": self.schedule_index,
            "generation_index": self.generation_index,
            "generation_order": self.generation_order,
            "generation_size": self.generation_size,
        }


@dataclass(frozen=True)
class SemanticRegistry:
    """Compiled semantic registry for a single execution plan."""

    plan_signature: str
    records: MutableRegistry[str, SemanticNodeRecord]
    errors: tuple[str, ...] = ()

    def payload(self) -> dict[str, object]:
        """Return the registry payload for diagnostics sinks.

        Returns
        -------
        dict[str, object]
            Registry payload for diagnostics sinks.
        """
        snapshot = self.records.snapshot()
        ordered = [record.payload() for record in sorted(snapshot.values(), key=_record_sort_key)]
        return {
            "plan_signature": self.plan_signature,
            "record_count": len(self.records),
            "error_count": len(self.errors),
            "errors": list(self.errors),
            "records": ordered,
        }


def compile_semantic_registry(
    nodes: Mapping[str, hamilton_node.Node],
    *,
    plan_signature: str,
) -> SemanticRegistry:
    """Compile a semantic registry from Hamilton graph nodes.

    Returns
    -------
    SemanticRegistry
        Compiled semantic registry.
    """
    tag_map = {name: node_.tags for name, node_ in nodes.items()}
    return compile_semantic_registry_from_tags(tag_map, plan_signature=plan_signature)


def compile_semantic_registry_from_tags(
    nodes: Mapping[str, Mapping[str, object]],
    *,
    plan_signature: str,
) -> SemanticRegistry:
    """Compile a semantic registry from node name -> tag mappings.

    Returns
    -------
    SemanticRegistry
        Compiled semantic registry.

    Raises
    ------
    ValueError
        Raised when semantic outputs are missing required semantic tags.
    """
    record_registry: MutableRegistry[str, SemanticNodeRecord] = MutableRegistry()
    errors: list[str] = []
    seen_semantic_ids: dict[str, str] = {}
    for node_name, tags in sorted(nodes.items()):
        record, node_errors = _semantic_record(
            node_name=node_name,
            tags=tags,
            plan_signature=plan_signature,
        )
        errors.extend(node_errors)
        if record is not None:
            existing = seen_semantic_ids.get(record.semantic_id)
            if existing is not None:
                errors.append(
                    "Duplicate semantic_id for nodes "
                    f"{existing!r} and {record.node_name!r}: {record.semantic_id!r}."
                )
                continue
            seen_semantic_ids[record.semantic_id] = record.node_name
            record_registry.register(record.node_name, record)
    registry = SemanticRegistry(
        plan_signature=plan_signature,
        records=record_registry,
        errors=tuple(errors),
    )
    if registry.errors:
        preview = "; ".join(registry.errors[:_ERROR_PREVIEW_LIMIT])
        msg = f"Semantic registry validation failed: {preview}."
        raise ValueError(msg)
    return registry


def semantic_registry_from_driver(
    driver: hamilton_driver.Driver,
    *,
    plan_signature: str,
) -> SemanticRegistry:
    """Compile the semantic registry directly from a Hamilton driver.

    Returns
    -------
    SemanticRegistry
        Compiled semantic registry.
    """
    nodes = driver.list_available_variables(tag_filter={"layer": "semantic"})
    tag_map = {node.name: node.tags for node in nodes}
    return compile_semantic_registry_from_tags(tag_map, plan_signature=plan_signature)


@dataclass
class SemanticRegistryHook(lifecycle_api.GraphExecutionHook):
    """Emit a semantic registry artifact for each run."""

    profile: DataFusionRuntimeProfile
    plan_signature: str
    config: Mapping[str, JsonValue]
    _driver: hamilton_driver.Driver | None = None

    def bind_driver(self, driver: hamilton_driver.Driver) -> None:
        """Bind the Hamilton driver used for registry compilation."""
        self._driver = driver

    def run_before_graph_execution(
        self,
        *,
        run_id: str,
        **kwargs: object,
    ) -> None:
        """Record the semantic registry before graph execution."""
        _ = run_id, kwargs
        if not bool(self.config.get("enable_semantic_registry", True)):
            return
        driver = self._driver
        if driver is None:
            return
        registry = semantic_registry_from_driver(
            driver,
            plan_signature=self.plan_signature,
        )
        path = _registry_path(
            self.config,
            plan_signature=self.plan_signature,
            run_id=run_id,
        )
        payload = registry.payload()
        _write_registry_payload(path, payload)
        from datafusion_engine.lineage.diagnostics import record_artifact

        payload_with_path = dict(payload)
        payload_with_path["path"] = str(path)
        record_artifact(
            self.profile,
            "semantic_registry_v1",
            payload_with_path,
        )

    def run_after_graph_execution(
        self,
        *,
        run_id: str,
        **kwargs: object,
    ) -> None:
        """No-op after execution to satisfy the hook contract."""
        _ = self, run_id, kwargs


def _semantic_record(
    *,
    node_name: str,
    tags: Mapping[str, object],
    plan_signature: str,
) -> tuple[SemanticNodeRecord | None, tuple[str, ...]]:
    semantic_id_value = tags.get("semantic_id")
    semantic_id = semantic_id_value if isinstance(semantic_id_value, str) else ""
    layer_value = tags.get("layer")
    layer = layer_value if isinstance(layer_value, str) else ""
    is_semantic_node = bool(semantic_id) or layer == _SEMANTIC_LAYER
    if not is_semantic_node:
        return None, ()
    errors: list[str] = []
    if layer != _SEMANTIC_LAYER:
        errors.append(f"{node_name}: semantic outputs must use layer=semantic")
    missing_tags = sorted(_missing_required_tags(tags))
    if missing_tags:
        errors.append(f"{node_name}: missing required semantic tags: {missing_tags}")
    if not semantic_id:
        semantic_id = node_name
    kind_value = tags.get("kind")
    kind = kind_value if isinstance(kind_value, str) and kind_value else "unknown"
    if kind == "table":
        missing_table_tags = sorted(_missing_required_table_tags(tags))
        if missing_table_tags:
            errors.append(
                f"{node_name}: missing required semantic table tags: {missing_table_tags}"
            )
    else:
        missing_non_table = sorted(_missing_required_non_table_tags(tags))
        if missing_non_table:
            errors.append(
                f"{node_name}: missing required semantic non-table tags: {missing_non_table}"
            )
    record = SemanticNodeRecord(
        node_name=node_name,
        semantic_id=semantic_id,
        kind=kind,
        plan_signature=plan_signature,
        version=_as_str(tags.get("version")),
        entity=_as_str(tags.get("entity")),
        grain=_as_str(tags.get("grain")),
        stability=_as_str(tags.get("stability")),
        schema_ref=_as_str(tags.get("schema_ref")),
        materialization=_as_str(tags.get("materialization")),
        materialized_name=_as_str(tags.get("materialized_name")),
        entity_keys=_parse_key_list(tags.get("entity_keys")),
        join_keys=_parse_key_list(tags.get("join_keys")),
        schedule_index=_as_str(tags.get("schedule_index")),
        generation_index=_as_str(tags.get("generation_index")),
        generation_order=_as_str(tags.get("generation_order")),
        generation_size=_as_str(tags.get("generation_size")),
    )
    return record, tuple(errors)


def _missing_required_tags(tags: Mapping[str, object]) -> set[str]:
    missing: set[str] = set()
    for key in _REQUIRED_SEMANTIC_TAGS:
        value = tags.get(key)
        if not isinstance(value, str) or not value.strip():
            missing.add(key)
    return missing


def _missing_required_table_tags(tags: Mapping[str, object]) -> set[str]:
    missing: set[str] = set()
    for key in _REQUIRED_TABLE_TAGS:
        value = tags.get(key)
        if not isinstance(value, (str, Sequence)) or not value:
            missing.add(key)
    return missing


def _missing_required_non_table_tags(tags: Mapping[str, object]) -> set[str]:
    missing: set[str] = set()
    for key in _REQUIRED_NON_TABLE_TAGS:
        value = tags.get(key)
        if not isinstance(value, str) or not value.strip():
            missing.add(key)
    return missing


def _parse_key_list(value: object) -> tuple[str, ...]:
    if isinstance(value, str):
        items = [item.strip() for item in value.split(",") if item.strip()]
        return tuple(items)
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        keys = [item.strip() for item in value if isinstance(item, str) and item.strip()]
        return tuple(keys)
    return ()


def _as_str(value: object) -> str | None:
    if isinstance(value, str) and value:
        return value
    return None


def _record_sort_key(record: SemanticNodeRecord) -> tuple[str, str]:
    return record.semantic_id, record.node_name


def _registry_path(
    config: Mapping[str, JsonValue],
    *,
    plan_signature: str,
    run_id: str | None,
) -> Path:
    filename = f"semantic_registry_{plan_signature}.json"
    explicit = config.get("semantic_registry_path") or config.get(
        "hamilton_semantic_registry_path"
    )
    if isinstance(explicit, str) and explicit:
        base = Path(explicit).expanduser()
        if base.suffix:
            return base
        if run_id:
            return base / run_id / filename
        return base / filename
    cache_path = config.get("cache_path")
    if isinstance(cache_path, str) and cache_path:
        base = Path(cache_path).expanduser() / "lineage"
        if run_id:
            base /= run_id
        return base / filename
    base = Path("build") / "structured_logs" / "cache_lineage"
    if run_id:
        base /= run_id
    return base / filename


def _write_registry_payload(path: Path, payload: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=True, sort_keys=True))
        handle.write("\n")


__all__ = [
    "SemanticNodeRecord",
    "SemanticRegistry",
    "SemanticRegistryHook",
    "compile_semantic_registry",
    "compile_semantic_registry_from_tags",
    "semantic_registry_from_driver",
]

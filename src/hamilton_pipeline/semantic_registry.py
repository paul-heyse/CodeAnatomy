"""Plan-native semantic registry compilation from Hamilton tags."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING

from hamilton.lifecycle import api as lifecycle_api

from core_types import JsonValue

_SEMANTIC_LAYER = "semantic"

if TYPE_CHECKING:
    from hamilton import driver as hamilton_driver
    from hamilton import node as hamilton_node

    from datafusion_engine.runtime import DataFusionRuntimeProfile


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
    records: tuple[SemanticNodeRecord, ...]
    errors: tuple[str, ...] = ()

    def payload(self) -> dict[str, object]:
        """Return the registry payload for diagnostics sinks.

        Returns
        -------
        dict[str, object]
            Registry payload for diagnostics sinks.
        """
        return {
            "plan_signature": self.plan_signature,
            "record_count": len(self.records),
            "error_count": len(self.errors),
            "errors": list(self.errors),
            "records": [record.payload() for record in self.records],
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
    records: list[SemanticNodeRecord] = []
    errors: list[str] = []
    for node_name, node_ in sorted(nodes.items()):
        record, node_errors = _semantic_record(
            node_name=node_name,
            tags=node_.tags,
            plan_signature=plan_signature,
        )
        errors.extend(node_errors)
        if record is not None:
            records.append(record)
    ordered_records = tuple(sorted(records, key=_record_sort_key))
    return SemanticRegistry(
        plan_signature=plan_signature,
        records=ordered_records,
        errors=tuple(errors),
    )


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
    return compile_semantic_registry(driver.graph.nodes, plan_signature=plan_signature)


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
        registry = semantic_registry_from_driver(driver, plan_signature=self.plan_signature)
        from datafusion_engine.diagnostics import record_artifact

        record_artifact(
            self.profile,
            "semantic_registry_v1",
            registry.payload(),
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
    if not semantic_id and layer != _SEMANTIC_LAYER:
        return None, ()
    errors: list[str] = []
    if not semantic_id:
        semantic_id = node_name
        errors.append(f"{node_name}: missing semantic_id tag; defaulting to node name")
    kind_value = tags.get("kind")
    kind = kind_value if isinstance(kind_value, str) and kind_value else "unknown"
    if kind == "unknown":
        errors.append(f"{node_name}: missing kind tag")
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


__all__ = [
    "SemanticNodeRecord",
    "SemanticRegistry",
    "SemanticRegistryHook",
    "compile_semantic_registry",
    "semantic_registry_from_driver",
]

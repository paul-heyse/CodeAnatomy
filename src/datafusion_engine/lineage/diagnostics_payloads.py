"""Diagnostics payload builders for lineage and UDF parity artifacts."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Protocol

from utils.hashing import hash_json_canonical

if TYPE_CHECKING:
    from datafusion import SessionContext


class PlanBundleLike(Protocol):
    """Minimal plan-bundle shape required for diagnostics payloads."""

    @property
    def plan_fingerprint(self) -> str: ...


class ViewNodeLike(Protocol):
    """Minimal view-node shape required for diagnostics payloads."""

    @property
    def name(self) -> str: ...

    @property
    def required_udfs(self) -> tuple[str, ...]: ...

    @property
    def plan_bundle(self) -> PlanBundleLike | None: ...


def view_udf_parity_payload(
    *,
    snapshot: Mapping[str, object],
    view_nodes: Sequence[ViewNodeLike],
    ctx: SessionContext | None = None,
) -> dict[str, object]:
    """Return a diagnostics payload describing view/UDF parity."""
    from datafusion_engine.udf.extension_core import udf_names_from_snapshot

    def _required_udfs(node: ViewNodeLike) -> tuple[str, ...]:
        return tuple(node.required_udfs)

    available = udf_names_from_snapshot(snapshot)
    info_available: set[str] | None = None
    if ctx is not None:
        from datafusion_engine.schema.introspection_core import SchemaIntrospector

        introspector = SchemaIntrospector(ctx)
        catalog = introspector.function_catalog_snapshot(include_parameters=False)
        info_available = {
            name.lower()
            for row in catalog
            for name in (
                row.get("function_name"),
                row.get("routine_name"),
                row.get("name"),
            )
            if isinstance(name, str)
        }
    rows: list[dict[str, object]] = []
    missing_snapshot_views = 0
    missing_info_views = 0
    for node in view_nodes:
        required = _required_udfs(node)
        missing_snapshot = [name for name in required if name not in available]
        missing_info: list[str] | None = None
        if info_available is not None:
            missing_info = [name for name in required if name.lower() not in info_available]
        if missing_snapshot:
            missing_snapshot_views += 1
        if missing_info:
            missing_info_views += 1
        rows.append(
            {
                "view": node.name,
                "required_udfs": list(required) or None,
                "missing_snapshot_udfs": missing_snapshot or None,
                "missing_information_schema_udfs": missing_info or None,
            }
        )
    return {
        "total_views": len(view_nodes),
        "views_with_requirements": sum(1 for node in view_nodes if _required_udfs(node)),
        "views_missing_snapshot_udfs": missing_snapshot_views,
        "views_missing_information_schema_udfs": missing_info_views,
        "rows": rows,
    }


def view_fingerprint_payload(
    *,
    view_nodes: Sequence[ViewNodeLike],
) -> dict[str, object]:
    """Return a diagnostics payload describing view fingerprints."""
    rows = [
        {
            "view": node.name,
            "plan_fingerprint": (
                node.plan_bundle.plan_fingerprint if node.plan_bundle is not None else None
            ),
        }
        for node in view_nodes
    ]
    return {
        "total_views": len(view_nodes),
        "rows": rows,
    }


def rust_udf_snapshot_payload(snapshot: Mapping[str, object]) -> dict[str, object]:
    """Return a diagnostics payload summarizing a Rust UDF snapshot."""
    from datafusion_engine.udf.extension_core import (
        rust_udf_snapshot_hash,
        udf_names_from_snapshot,
    )

    def _plugin_manifest() -> Mapping[str, object] | None:
        from datafusion_engine.extensions.plugin_manifest import resolve_plugin_manifest

        return resolve_plugin_manifest("datafusion_engine.extensions.datafusion_ext").manifest

    def _count_seq(key: str) -> int:
        value = snapshot.get(key, ())
        if isinstance(value, (str, bytes)) or not isinstance(value, Sequence):
            return 0
        return len(value)

    def _count_map(key: str) -> int:
        value = snapshot.get(key, {})
        if not isinstance(value, Mapping):
            return 0
        return len(value)

    manifest = _plugin_manifest()
    return {
        "snapshot_hash": rust_udf_snapshot_hash(snapshot),
        "manifest_hash": (
            hash_json_canonical(manifest, str_keys=True) if manifest is not None else None
        ),
        "manifest": manifest,
        "total_udfs": len(udf_names_from_snapshot(snapshot)),
        "scalar_udfs": _count_seq("scalar"),
        "aggregate_udfs": _count_seq("aggregate"),
        "window_udfs": _count_seq("window"),
        "table_udfs": _count_seq("table"),
        "custom_udfs": _count_seq("custom_udfs"),
        "aliases": _count_map("aliases"),
        "signature_inputs": _count_map("signature_inputs"),
        "return_types": _count_map("return_types"),
        "parameter_names": _count_map("parameter_names"),
        "volatility": _count_map("volatility"),
        "rewrite_tags": _count_map("rewrite_tags"),
    }


__all__ = [
    "rust_udf_snapshot_payload",
    "view_fingerprint_payload",
    "view_udf_parity_payload",
]

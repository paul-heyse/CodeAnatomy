"""Observation utilities for manifests, stats, and reproducibility."""

from __future__ import annotations

import importlib
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    column_stats_table: object
    datafusion_engine_runtime_metrics: object
    dataset_stats_table: object
    schema_identity_hash: object
    table_summary: object

_EXPORT_MAP: dict[str, tuple[str, str]] = {
    "column_stats_table": ("obs.metrics", "column_stats_table"),
    "dataset_stats_table": ("obs.metrics", "dataset_stats_table"),
    "schema_identity_hash": ("datafusion_engine.identity", "schema_identity_hash"),
    "table_summary": ("obs.metrics", "table_summary"),
}

_MODULE_EXPORTS: dict[str, str] = {
    "datafusion_engine_runtime_metrics": "obs.datafusion_engine_runtime_metrics",
}


def __getattr__(name: str) -> object:
    module_path = _MODULE_EXPORTS.get(name)
    if module_path is not None:
        module = importlib.import_module(module_path)
        globals()[name] = module
        return module
    target = _EXPORT_MAP.get(name)
    if target is None:
        msg = f"module {__name__!r} has no attribute {name!r}"
        raise AttributeError(msg)
    module_name, attr_name = target
    module = importlib.import_module(module_name)
    value = getattr(module, attr_name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(__all__)


__all__ = (
    "column_stats_table",
    "datafusion_engine_runtime_metrics",
    "dataset_stats_table",
    "schema_identity_hash",
    "table_summary",
)

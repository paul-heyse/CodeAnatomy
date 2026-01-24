"""Observation utilities for manifests, stats, and reproducibility."""

from __future__ import annotations

import importlib
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    column_stats_table: object
    dataset_stats_table: object
    table_summary: object
    schema_fingerprint: object

_EXPORT_MAP: dict[str, tuple[str, str]] = {
    "column_stats_table": ("arrowdsl.core.metrics", "column_stats_table"),
    "dataset_stats_table": ("arrowdsl.core.metrics", "dataset_stats_table"),
    "table_summary": ("arrowdsl.core.metrics", "table_summary"),
    "schema_fingerprint": ("arrowdsl.schema.serialization", "schema_fingerprint"),
}


def __getattr__(name: str) -> object:
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
    "dataset_stats_table",
    "schema_fingerprint",
    "table_summary",
)

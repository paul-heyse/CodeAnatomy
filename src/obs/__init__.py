"""Observation utilities for quality diagnostics and runtime metrics."""

from __future__ import annotations

import importlib
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from obs.quality_metrics import (
        QualityPlanSpec,
        concat_quality_tables,
        empty_quality_table,
        quality_from_ids,
        quality_issue_rows,
        record_quality_issue_counts,
    )

    datafusion_engine_runtime_metrics: object

_EXPORT_MAP: dict[str, tuple[str, str]] = {
    "QualityPlanSpec": ("obs.quality_metrics", "QualityPlanSpec"),
    "concat_quality_tables": ("obs.quality_metrics", "concat_quality_tables"),
    "empty_quality_table": ("obs.quality_metrics", "empty_quality_table"),
    "quality_from_ids": ("obs.quality_metrics", "quality_from_ids"),
    "quality_issue_rows": ("obs.quality_metrics", "quality_issue_rows"),
    "record_quality_issue_counts": ("obs.quality_metrics", "record_quality_issue_counts"),
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
    "QualityPlanSpec",
    "concat_quality_tables",
    "datafusion_engine_runtime_metrics",
    "empty_quality_table",
    "quality_from_ids",
    "quality_issue_rows",
    "record_quality_issue_counts",
)

"""Observation utilities for manifests, stats, and reproducibility."""

from __future__ import annotations

import importlib
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    DatasetRecord: type[object]
    Manifest: type[object]
    OutputRecord: type[object]
    RuleRecord: type[object]
    build_manifest: object
    write_manifest_json: object
    collect_repro_info: object
    make_run_bundle_name: object
    serialize_contract_catalog: object
    try_get_git_info: object
    write_run_bundle: object
    column_stats_table: object
    dataset_stats_table: object
    table_summary: object
    schema_fingerprint: object

_EXPORT_MAP: dict[str, tuple[str, str]] = {
    "DatasetRecord": ("obs.manifest", "DatasetRecord"),
    "Manifest": ("obs.manifest", "Manifest"),
    "OutputRecord": ("obs.manifest", "OutputRecord"),
    "RuleRecord": ("obs.manifest", "RuleRecord"),
    "build_manifest": ("obs.manifest", "build_manifest"),
    "write_manifest_json": ("obs.manifest", "write_manifest_json"),
    "collect_repro_info": ("obs.repro", "collect_repro_info"),
    "make_run_bundle_name": ("obs.repro", "make_run_bundle_name"),
    "serialize_contract_catalog": ("obs.repro", "serialize_contract_catalog"),
    "try_get_git_info": ("obs.repro", "try_get_git_info"),
    "write_run_bundle": ("obs.repro", "write_run_bundle"),
    "column_stats_table": ("arrowdsl.plan.metrics", "column_stats_table"),
    "dataset_stats_table": ("arrowdsl.plan.metrics", "dataset_stats_table"),
    "table_summary": ("arrowdsl.plan.metrics", "table_summary"),
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
    "DatasetRecord",
    "Manifest",
    "OutputRecord",
    "RuleRecord",
    "build_manifest",
    "collect_repro_info",
    "column_stats_table",
    "dataset_stats_table",
    "make_run_bundle_name",
    "schema_fingerprint",
    "serialize_contract_catalog",
    "table_summary",
    "try_get_git_info",
    "write_manifest_json",
    "write_run_bundle",
)

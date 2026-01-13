"""Observation utilities for manifests, stats, and reproducibility."""

from __future__ import annotations

from arrowdsl.plan.metrics import column_stats_table, dataset_stats_table, table_summary
from arrowdsl.schema.schema import schema_fingerprint
from obs.manifest import (
    DatasetRecord,
    Manifest,
    OutputRecord,
    RuleRecord,
    build_manifest,
    write_manifest_json,
)
from obs.repro import (
    collect_repro_info,
    make_run_bundle_name,
    serialize_contract_catalog,
    serialize_relationship_registry,
    try_get_git_info,
    write_run_bundle,
)

__all__ = [
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
    "serialize_relationship_registry",
    "table_summary",
    "try_get_git_info",
    "write_manifest_json",
    "write_run_bundle",
]

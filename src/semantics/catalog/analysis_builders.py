"""Facade re-exporting semantic catalog analysis builders by domain."""

from __future__ import annotations

from semantics.catalog.cfg_builders import cfg_blocks_df_builder, cfg_edges_df_builder
from semantics.catalog.def_use_builders import def_use_events_df_builder, reaching_defs_df_builder
from semantics.catalog.diagnostic_builders import (
    diagnostics_df_builder,
    exported_defs_df_builder,
    file_coverage_report_df_builder,
    file_quality_df_builder,
    relationship_ambiguity_report_df_builder,
    relationship_candidates_df_builder,
    relationship_decisions_df_builder,
    relationship_quality_metrics_df_builder,
    schema_anomalies_df_builder,
    span_errors_df_builder,
)
from semantics.catalog.type_builders import type_exprs_df_builder, type_nodes_df_builder

__all__ = [
    "cfg_blocks_df_builder",
    "cfg_edges_df_builder",
    "def_use_events_df_builder",
    "diagnostics_df_builder",
    "exported_defs_df_builder",
    "file_coverage_report_df_builder",
    "file_quality_df_builder",
    "reaching_defs_df_builder",
    "relationship_ambiguity_report_df_builder",
    "relationship_candidates_df_builder",
    "relationship_decisions_df_builder",
    "relationship_quality_metrics_df_builder",
    "schema_anomalies_df_builder",
    "span_errors_df_builder",
    "type_exprs_df_builder",
    "type_nodes_df_builder",
]

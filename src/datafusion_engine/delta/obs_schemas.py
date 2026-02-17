"""Delta observability schema builders."""

from __future__ import annotations

import pyarrow as pa

from datafusion_engine.arrow.field_builders import (
    binary_field,
    bool_field,
    int32_field,
    int64_field,
    list_field,
    string_field,
)


def delta_snapshot_schema() -> pa.Schema:
    """Return schema for Delta snapshot observability rows."""
    return pa.schema(
        [
            int64_field("event_time_unix_ms", nullable=False),
            string_field("run_id"),
            string_field("dataset_name"),
            string_field("table_uri", nullable=False),
            string_field("trace_id"),
            string_field("span_id"),
            int64_field("delta_version"),
            int64_field("snapshot_timestamp"),
            int32_field("min_reader_version"),
            int32_field("min_writer_version"),
            list_field("reader_features", pa.string()),
            list_field("writer_features", pa.string()),
            pa.field("table_properties", pa.map_(pa.string(), pa.string())),
            binary_field("schema_msgpack", nullable=False),
            string_field("schema_identity_hash"),
            string_field("ddl_fingerprint"),
            list_field("partition_columns", pa.string()),
        ]
    )


def delta_mutation_schema() -> pa.Schema:
    """Return schema for Delta mutation observability rows."""
    return pa.schema(
        [
            int64_field("event_time_unix_ms", nullable=False),
            string_field("run_id"),
            string_field("dataset_name"),
            string_field("table_uri", nullable=False),
            string_field("trace_id"),
            string_field("span_id"),
            string_field("operation", nullable=False),
            string_field("mode"),
            int64_field("delta_version"),
            int32_field("min_reader_version"),
            int32_field("min_writer_version"),
            list_field("reader_features", pa.string()),
            list_field("writer_features", pa.string()),
            pa.field("table_properties", pa.map_(pa.string(), pa.string())),
            string_field("constraint_status"),
            list_field("constraint_violations", pa.string()),
            string_field("commit_app_id"),
            int64_field("commit_version"),
            string_field("commit_run_id"),
            pa.field("commit_metadata", pa.map_(pa.string(), pa.string())),
            binary_field("metrics_msgpack", nullable=False),
        ]
    )


def delta_scan_plan_schema() -> pa.Schema:
    """Return schema for Delta scan-plan observability rows."""
    return pa.schema(
        [
            int64_field("event_time_unix_ms", nullable=False),
            string_field("run_id"),
            string_field("dataset_name", nullable=False),
            string_field("table_uri", nullable=False),
            string_field("trace_id"),
            string_field("span_id"),
            int64_field("delta_version"),
            int64_field("snapshot_timestamp"),
            int64_field("total_files", nullable=False),
            int64_field("candidate_files", nullable=False),
            int64_field("pruned_files", nullable=False),
            list_field("pushed_filters", pa.string()),
            list_field("projected_columns", pa.string()),
            binary_field("delta_protocol_msgpack"),
        ]
    )


def delta_maintenance_schema() -> pa.Schema:
    """Return schema for Delta maintenance observability rows."""
    return pa.schema(
        [
            int64_field("event_time_unix_ms", nullable=False),
            string_field("run_id"),
            string_field("dataset_name"),
            string_field("table_uri", nullable=False),
            string_field("trace_id"),
            string_field("span_id"),
            string_field("operation", nullable=False),
            int64_field("delta_version"),
            int32_field("min_reader_version"),
            int32_field("min_writer_version"),
            list_field("reader_features", pa.string()),
            list_field("writer_features", pa.string()),
            pa.field("table_properties", pa.map_(pa.string(), pa.string())),
            string_field("log_retention_duration"),
            string_field("checkpoint_interval"),
            string_field("checkpoint_retention_duration"),
            string_field("checkpoint_protection"),
            int64_field("retention_hours"),
            bool_field("dry_run"),
            binary_field("metrics_msgpack", nullable=False),
            pa.field("commit_metadata", pa.map_(pa.string(), pa.string())),
        ]
    )


__all__ = [
    "delta_maintenance_schema",
    "delta_mutation_schema",
    "delta_scan_plan_schema",
    "delta_snapshot_schema",
]

"""Diagnostics table builders."""

from __future__ import annotations

import hashlib
import json
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass

import pyarrow as pa

from arrowdsl.core.interop import RecordBatchReaderLike, TableLike
from arrowdsl.schema.build import table_from_rows
from datafusion_engine.schema_registry import schema_for

try:
    from sqlglot_tools.lineage import LineagePayload
except ImportError:
    LineagePayload = None  # type: ignore[misc, assignment]


def _now_ms() -> int:
    return int(time.time() * 1000)


def _coerce_event_time(value: object, *, default: int) -> int:
    if isinstance(value, bool):
        return default
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, str):
        stripped = value.strip()
        if stripped:
            try:
                return int(stripped)
            except ValueError:
                return default
    return default


def _coerce_int(value: object, *, default: int) -> int:
    if isinstance(value, bool):
        return default
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        stripped = value.strip()
        if stripped:
            try:
                return int(stripped)
            except ValueError:
                return default
    return default


def _coerce_float(value: object, *, default: float) -> float:
    if isinstance(value, bool):
        return default
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        stripped = value.strip()
        if stripped:
            try:
                return float(stripped)
            except ValueError:
                return default
    return default


def _coerce_str_list(value: object) -> list[str]:
    if value is None:
        return []
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [str(item) for item in value]
    return [str(value)]


def _stringify_payload(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=True, default=str)


def datafusion_explains_table(explains: Sequence[Mapping[str, object]]) -> pa.Table:
    """Build a DataFusion explain diagnostics table.

    Returns
    -------
    pyarrow.Table
        Diagnostics table aligned to DATAFUSION_EXPLAINS_V1.
    """
    now = _now_ms()
    rows: list[dict[str, object]] = []
    for explain in explains:
        raw_rows = explain.get("rows")
        artifact_path, artifact_format, schema_fp = _explain_rows_metadata(raw_rows)
        rows.append(
            {
                "event_time_unix_ms": _coerce_event_time(
                    explain.get("event_time_unix_ms"), default=now
                ),
                "sql": str(explain.get("sql") or ""),
                "explain_rows_artifact_path": artifact_path,
                "explain_rows_artifact_format": artifact_format,
                "explain_rows_schema_fingerprint": schema_fp,
                "explain_analyze": bool(explain.get("explain_analyze") or False),
            }
        )
    schema = schema_for("datafusion_explains_v1")
    return table_from_rows(schema, rows)


def datafusion_schema_registry_validation_table(
    records: Sequence[Mapping[str, object]],
) -> pa.Table:
    """Build a DataFusion schema registry validation diagnostics table.

    Returns
    -------
    pyarrow.Table
        Diagnostics table aligned to DATAFUSION_SCHEMA_REGISTRY_VALIDATION_V1.
    """
    now = _now_ms()
    rows: list[dict[str, object]] = []
    for record in records:
        event_time = _coerce_event_time(record.get("event_time_unix_ms"), default=now)
        missing = _coerce_str_list(record.get("missing"))
        rows.extend(
            {
                "event_time_unix_ms": event_time,
                "schema_name": name,
                "issue_type": "missing",
                "detail": None,
            }
            for name in missing
        )
        type_errors = record.get("type_errors")
        if isinstance(type_errors, Mapping):
            rows.extend(
                {
                    "event_time_unix_ms": event_time,
                    "schema_name": str(name),
                    "issue_type": "type_error",
                    "detail": str(detail),
                }
                for name, detail in type_errors.items()
            )
        view_errors = record.get("view_errors")
        if isinstance(view_errors, Mapping):
            rows.extend(
                {
                    "event_time_unix_ms": event_time,
                    "schema_name": str(name),
                    "issue_type": "view_error",
                    "detail": str(detail),
                }
                for name, detail in view_errors.items()
            )
        parse_errors = record.get("sql_parse_errors")
        if isinstance(parse_errors, Mapping):
            rows.extend(
                {
                    "event_time_unix_ms": event_time,
                    "schema_name": str(name),
                    "issue_type": "sql_parse_error",
                    "detail": json.dumps(detail, ensure_ascii=True, default=str),
                }
                for name, detail in parse_errors.items()
            )
        constraint_drift = record.get("constraint_drift")
        if isinstance(constraint_drift, Sequence):
            for entry in constraint_drift:
                if not isinstance(entry, Mapping):
                    continue
                name = entry.get("schema_name")
                if name is None:
                    continue
                rows.append(
                    {
                        "event_time_unix_ms": event_time,
                        "schema_name": str(name),
                        "issue_type": "constraint_drift",
                        "detail": json.dumps(entry, ensure_ascii=True, default=str),
                    }
                )
        relationship_errors = record.get("relationship_constraint_errors")
        if isinstance(relationship_errors, Mapping):
            rows.extend(
                {
                    "event_time_unix_ms": event_time,
                    "schema_name": str(name),
                    "issue_type": "relationship_constraint_error",
                    "detail": json.dumps(detail, ensure_ascii=True, default=str),
                }
                for name, detail in relationship_errors.items()
            )
    schema = schema_for("datafusion_schema_registry_validation_v1")
    return table_from_rows(schema, rows)


def datafusion_schema_map_fingerprints_table(
    records: Sequence[Mapping[str, object]],
) -> pa.Table:
    """Build a DataFusion schema map fingerprint diagnostics table.

    Returns
    -------
    pyarrow.Table
        Diagnostics table aligned to DATAFUSION_SCHEMA_MAP_FINGERPRINTS_V1.
    """
    now = _now_ms()
    rows = [
        {
            "event_time_unix_ms": _coerce_event_time(
                record.get("event_time_unix_ms"),
                default=now,
            ),
            "schema_map_hash": str(record.get("schema_map_hash") or ""),
            "schema_map_version": _coerce_int(
                record.get("schema_map_version"),
                default=1,
            ),
            "table_count": _coerce_int(record.get("table_count"), default=0),
            "column_count": _coerce_int(record.get("column_count"), default=0),
        }
        for record in records
    ]
    schema = schema_for("datafusion_schema_map_fingerprints_v1")
    return table_from_rows(schema, rows)


def datafusion_ddl_fingerprints_table(
    records: Sequence[Mapping[str, object]],
) -> pa.Table:
    """Build a DataFusion DDL fingerprint diagnostics table.

    Returns
    -------
    pyarrow.Table
        Diagnostics table aligned to DATAFUSION_DDL_FINGERPRINTS_V1.
    """
    now = _now_ms()
    rows = [
        {
            "event_time_unix_ms": _coerce_event_time(
                record.get("event_time_unix_ms"),
                default=now,
            ),
            "table_catalog": str(record.get("table_catalog") or ""),
            "table_schema": str(record.get("table_schema") or ""),
            "table_name": str(record.get("table_name") or ""),
            "table_type": (
                str(record.get("table_type")) if record.get("table_type") is not None else None
            ),
            "ddl_fingerprint": (
                str(record.get("ddl_fingerprint"))
                if record.get("ddl_fingerprint") is not None
                else None
            ),
        }
        for record in records
    ]
    schema = schema_for("datafusion_ddl_fingerprints_v1")
    return table_from_rows(schema, rows)


def _explain_rows_metadata(rows: object) -> tuple[str | None, str | None, str | None]:
    if isinstance(rows, (RecordBatchReaderLike, TableLike)):
        return None, "ipc_file", _schema_fingerprint(rows.schema)
    if isinstance(rows, Mapping):
        artifact_path = rows.get("artifact_path")
        artifact_format = rows.get("artifact_format")
        schema_fp = rows.get("schema_fingerprint")
        return (
            str(artifact_path) if artifact_path is not None else None,
            str(artifact_format) if artifact_format is not None else None,
            str(schema_fp) if schema_fp is not None else None,
        )
    return None, None, None


def _schema_fingerprint(schema: pa.Schema) -> str:
    payload = schema.serialize()
    return hashlib.sha256(payload.to_pybytes()).hexdigest()


def feature_state_table(events: Sequence[Mapping[str, object]]) -> pa.Table:
    """Build a feature state diagnostics table.

    Returns
    -------
    pyarrow.Table
        Diagnostics table aligned to FEATURE_STATE_V1.
    """
    rows = [
        {
            "profile_name": str(event.get("profile_name") or ""),
            "determinism_tier": str(event.get("determinism_tier") or ""),
            "dynamic_filters_enabled": bool(event.get("dynamic_filters_enabled") or False),
            "spill_enabled": bool(event.get("spill_enabled") or False),
            "named_args_supported": bool(event.get("named_args_supported") or False),
        }
        for event in events
    ]
    schema = schema_for("feature_state_v1")
    return table_from_rows(schema, rows)


def datafusion_runs_table(records: Sequence[Mapping[str, object]]) -> pa.Table:
    """Build a DataFusion runs diagnostics table.

    Returns
    -------
    pyarrow.Table
        Diagnostics table aligned to DATAFUSION_RUNS_V1.
    """
    now = _now_ms()
    rows: list[dict[str, object]] = []
    for record in records:
        metadata_raw = record.get("metadata")
        metadata_str = None
        if metadata_raw:
            if isinstance(metadata_raw, str):
                metadata_str = metadata_raw
            elif isinstance(metadata_raw, Mapping):
                metadata_str = json.dumps(metadata_raw, ensure_ascii=True, default=str)
        rows.append(
            {
                "run_id": str(record.get("run_id") or ""),
                "label": str(record.get("label") or ""),
                "start_time_unix_ms": _coerce_event_time(
                    record.get("start_time_unix_ms"),
                    default=now,
                ),
                "end_time_unix_ms": (
                    _coerce_event_time(record.get("end_time_unix_ms"), default=0)
                    if record.get("end_time_unix_ms") is not None
                    else None
                ),
                "status": str(record.get("status") or "unknown"),
                "duration_ms": (
                    _coerce_int(record.get("duration_ms"), default=0)
                    if record.get("duration_ms") is not None
                    else None
                ),
                "metadata": metadata_str,
            }
        )
    schema = schema_for("datafusion_runs_v1")
    return table_from_rows(schema, rows)


def datafusion_cache_state_table(
    records: Sequence[Mapping[str, object]],
) -> pa.Table:
    """Build a DataFusion cache state diagnostics table.

    Returns
    -------
    pyarrow.Table
        Diagnostics table aligned to DATAFUSION_CACHE_STATE_V1.
    """
    now = _now_ms()
    rows = [
        {
            "cache_name": str(record.get("cache_name") or ""),
            "event_time_unix_ms": _coerce_event_time(
                record.get("event_time_unix_ms"),
                default=now,
            ),
            "entry_count": (
                _coerce_int(record.get("entry_count"), default=0)
                if record.get("entry_count") is not None
                else None
            ),
            "hit_count": (
                _coerce_int(record.get("hit_count"), default=0)
                if record.get("hit_count") is not None
                else None
            ),
            "miss_count": (
                _coerce_int(record.get("miss_count"), default=0)
                if record.get("miss_count") is not None
                else None
            ),
            "eviction_count": (
                _coerce_int(record.get("eviction_count"), default=0)
                if record.get("eviction_count") is not None
                else None
            ),
            "config_ttl": (
                str(record.get("config_ttl")) if record.get("config_ttl") is not None else None
            ),
            "config_limit": (
                str(record.get("config_limit")) if record.get("config_limit") is not None else None
            ),
        }
        for record in records
    ]
    schema = schema_for("datafusion_cache_state_v1")
    return table_from_rows(schema, rows)


def datafusion_plan_artifacts_table(
    artifacts: Sequence[Mapping[str, object]],
) -> pa.Table:
    """Build a DataFusion plan artifacts diagnostics table.

    Returns
    -------
    pyarrow.Table
        Diagnostics table aligned to DATAFUSION_PLAN_ARTIFACTS_V1.
    """
    now = _now_ms()
    rows = [_plan_artifact_row(artifact, default_time=now) for artifact in artifacts]
    schema = schema_for("datafusion_plan_artifacts_v1")
    return table_from_rows(schema, rows)


def _string_list(value: object | None) -> list[str] | None:
    if value is None:
        return None
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        return [str(item) for item in value]
    return [str(value)]


def _plan_artifact_row(
    artifact: Mapping[str, object],
    *,
    default_time: int,
) -> dict[str, object]:
    explain_path, explain_format, explain_fp = _explain_rows_metadata(artifact.get("explain"))
    analyze_path, analyze_format, _ = _explain_rows_metadata(artifact.get("explain_analyze"))
    substrait_validation = artifact.get("substrait_validation")
    substrait_status = None
    if substrait_validation and isinstance(substrait_validation, Mapping):
        substrait_status = str(substrait_validation.get("status") or "")
    return {
        "event_time_unix_ms": _coerce_event_time(
            artifact.get("event_time_unix_ms"),
            default=default_time,
        ),
        "run_id": str(artifact.get("run_id")) if artifact.get("run_id") is not None else None,
        "plan_hash": (
            str(artifact.get("plan_hash"))
            if artifact.get("plan_hash") is not None
            else None
        ),
        "sql": str(artifact.get("sql") or ""),
        "normalized_sql": (
            str(artifact.get("normalized_sql"))
            if artifact.get("normalized_sql") is not None
            else None
        ),
        "explain_rows_artifact_path": explain_path,
        "explain_rows_artifact_format": explain_format,
        "explain_rows_schema_fingerprint": explain_fp,
        "explain_analyze_artifact_path": analyze_path,
        "explain_analyze_artifact_format": analyze_format,
        "substrait_b64": (
            str(artifact.get("substrait_b64"))
            if artifact.get("substrait_b64") is not None
            else None
        ),
        "substrait_validation_status": substrait_status,
        "sqlglot_ast": (
            str(artifact.get("sqlglot_ast"))
            if artifact.get("sqlglot_ast") is not None
            else None
        ),
        "read_dialect": (
            str(artifact.get("read_dialect"))
            if artifact.get("read_dialect") is not None
            else None
        ),
        "write_dialect": (
            str(artifact.get("write_dialect"))
            if artifact.get("write_dialect") is not None
            else None
        ),
        "canonical_fingerprint": (
            str(artifact.get("canonical_fingerprint"))
            if artifact.get("canonical_fingerprint") is not None
            else None
        ),
        "lineage_tables": _string_list(artifact.get("lineage_tables")),
        "lineage_columns": _string_list(artifact.get("lineage_columns")),
        "lineage_scopes": _string_list(artifact.get("lineage_scopes")),
        "param_signature": (
            str(artifact.get("param_signature"))
            if artifact.get("param_signature") is not None
            else None
        ),
        "projection_map": (
            str(artifact.get("projection_map"))
            if artifact.get("projection_map") is not None
            else None
        ),
        "unparsed_sql": (
            str(artifact.get("unparsed_sql"))
            if artifact.get("unparsed_sql") is not None
            else None
        ),
        "unparse_error": (
            str(artifact.get("unparse_error"))
            if artifact.get("unparse_error") is not None
            else None
        ),
        "logical_plan": (
            str(artifact.get("logical_plan"))
            if artifact.get("logical_plan") is not None
            else None
        ),
        "optimized_plan": (
            str(artifact.get("optimized_plan"))
            if artifact.get("optimized_plan") is not None
            else None
        ),
        "physical_plan": (
            str(artifact.get("physical_plan"))
            if artifact.get("physical_plan") is not None
            else None
        ),
        "graphviz": (
            str(artifact.get("graphviz")) if artifact.get("graphviz") is not None else None
        ),
        "partition_count": (
            _coerce_int(artifact.get("partition_count"), default=0)
            if artifact.get("partition_count") is not None
            else None
        ),
        "join_operators": _string_list(artifact.get("join_operators")),
    }


def engine_runtime_table(
    artifacts: Sequence[Mapping[str, object]],
) -> pa.Table:
    """Build an engine runtime diagnostics table.

    Returns
    -------
    pyarrow.Table
        Diagnostics table aligned to ENGINE_RUNTIME_V1.
    """
    now = _now_ms()
    rows = [
        {
            "event_time_unix_ms": _coerce_event_time(
                artifact.get("event_time_unix_ms"),
                default=now,
            ),
            "runtime_profile_name": str(artifact.get("runtime_profile_name") or ""),
            "determinism_tier": str(artifact.get("determinism_tier") or ""),
            "runtime_profile_hash": str(artifact.get("runtime_profile_hash") or ""),
            "runtime_profile_snapshot": _stringify_payload(
                artifact.get("runtime_profile_snapshot")
            )
            or "",
            "sqlglot_policy_hash": (
                str(artifact.get("sqlglot_policy_hash"))
                if artifact.get("sqlglot_policy_hash") is not None
                else None
            ),
            "sqlglot_policy_snapshot": _stringify_payload(
                artifact.get("sqlglot_policy_snapshot")
            ),
            "function_registry_hash": (
                str(artifact.get("function_registry_hash"))
                if artifact.get("function_registry_hash") is not None
                else None
            ),
            "function_registry_snapshot": _stringify_payload(
                artifact.get("function_registry_snapshot")
            ),
            "datafusion_settings_hash": (
                str(artifact.get("datafusion_settings_hash"))
                if artifact.get("datafusion_settings_hash") is not None
                else None
            ),
            "datafusion_settings": _stringify_payload(
                artifact.get("datafusion_settings")
            ),
        }
        for artifact in artifacts
    ]
    schema = schema_for("engine_runtime_v1")
    return table_from_rows(schema, rows)


def datafusion_udf_validation_table(
    records: Sequence[Mapping[str, object]],
) -> pa.Table:
    """Build a DataFusion UDF validation diagnostics table.

    Returns
    -------
    pyarrow.Table
        Diagnostics table aligned to DATAFUSION_UDF_VALIDATION_V1.
    """
    now = _now_ms()
    rows = [
        {
            "event_time_unix_ms": _coerce_event_time(
                record.get("event_time_unix_ms"),
                default=now,
            ),
            "udf_catalog_policy": str(record.get("udf_catalog_policy") or "default"),
            "missing_udfs": _coerce_str_list(record.get("missing_udfs")),
            "missing_count": (
                _coerce_int(record.get("missing_count"), default=0)
                if record.get("missing_count") is not None
                else None
            ),
        }
        for record in records
    ]
    schema = schema_for("datafusion_udf_validation_v1")
    return table_from_rows(schema, rows)


def datafusion_object_stores_table(
    records: Sequence[Mapping[str, object]],
) -> pa.Table:
    """Build a DataFusion object store diagnostics table.

    Returns
    -------
    pyarrow.Table
        Diagnostics table aligned to DATAFUSION_OBJECT_STORES_V1.
    """
    now = _now_ms()
    rows = [
        {
            "event_time_unix_ms": _coerce_event_time(
                record.get("event_time_unix_ms"),
                default=now,
            ),
            "scheme": str(record.get("scheme") or ""),
            "host": str(record.get("host")) if record.get("host") is not None else None,
            "store_type": (
                str(record.get("store_type"))
                if record.get("store_type") is not None
                else None
            ),
        }
        for record in records
    ]
    schema = schema_for("datafusion_object_stores_v1")
    return table_from_rows(schema, rows)


@dataclass(frozen=True)
class FilePruningDiagnostics:
    """Diagnostics for Delta file pruning operations.

    Attributes
    ----------
    candidate_count : int
        Number of files that passed pruning filters.
    total_files : int
        Total number of files before pruning.
    pruned_percentage : float
        Percentage of files pruned (0-100).
    filter_summary : str
        Summary of applied filters.
    """

    candidate_count: int
    total_files: int
    pruned_percentage: float
    filter_summary: str

    @property
    def pruned_count(self) -> int:
        """Calculate the number of files pruned.

        Returns
        -------
        int
            Number of files pruned.
        """
        return self.total_files - self.candidate_count


@dataclass(frozen=True)
class FilePruningDiagnosticsSpec:
    """Input specification for file pruning diagnostics rows."""

    candidate_count: int
    total_files: int
    filter_summary: str
    table_name: str | None = None
    table_path: str | None = None
    event_time_unix_ms: int | None = None


def build_file_pruning_diagnostics_row(spec: FilePruningDiagnosticsSpec) -> dict[str, object]:
    """Build a single file pruning diagnostics row.

    Parameters
    ----------
    spec : FilePruningDiagnosticsSpec
        Input specification describing pruning metrics and context.

    Returns
    -------
    dict[str, object]
        Dictionary row suitable for diagnostics table.
    """
    now = _now_ms()
    pruned_count = spec.total_files - spec.candidate_count
    pruned_percentage = (pruned_count / spec.total_files * 100.0) if spec.total_files > 0 else 0.0

    return {
        "event_time_unix_ms": spec.event_time_unix_ms or now,
        "table_name": spec.table_name or "",
        "table_path": spec.table_path or "",
        "total_files": spec.total_files,
        "candidate_count": spec.candidate_count,
        "pruned_count": pruned_count,
        "pruned_percentage": pruned_percentage,
        "filter_summary": spec.filter_summary,
    }


def file_pruning_diagnostics_table(
    records: Sequence[Mapping[str, object]],
) -> pa.Table:
    """Build a file pruning diagnostics table.

    Parameters
    ----------
    records : Sequence[Mapping[str, object]]
        Sequence of file pruning diagnostic records.

    Returns
    -------
    pa.Table
        Diagnostics table with file pruning metrics.
    """
    now = _now_ms()
    rows = [
        {
            "event_time_unix_ms": _coerce_event_time(
                record.get("event_time_unix_ms"),
                default=now,
            ),
            "table_name": str(record.get("table_name") or ""),
            "table_path": str(record.get("table_path") or ""),
            "total_files": _coerce_int(record.get("total_files"), default=0),
            "candidate_count": _coerce_int(record.get("candidate_count"), default=0),
            "pruned_count": _coerce_int(record.get("pruned_count"), default=0),
            "pruned_percentage": _coerce_float(record.get("pruned_percentage"), default=0.0),
            "filter_summary": str(record.get("filter_summary") or ""),
        }
        for record in records
    ]

    schema = pa.schema(
        [
            pa.field("event_time_unix_ms", pa.int64()),
            pa.field("table_name", pa.string()),
            pa.field("table_path", pa.string()),
            pa.field("total_files", pa.int64()),
            pa.field("candidate_count", pa.int64()),
            pa.field("pruned_count", pa.int64()),
            pa.field("pruned_percentage", pa.float64()),
            pa.field("filter_summary", pa.string()),
        ]
    )
    return table_from_rows(schema, rows)


def lineage_diagnostics_row(
    payload: object,
    query_id: str,
    timestamp_ms: int,
) -> dict[str, object]:
    """Build diagnostics row for lineage artifact.

    Parameters
    ----------
    payload : LineagePayload
        Lineage metadata from extraction.
    query_id : str
        Unique query identifier.
    timestamp_ms : int
        Unix timestamp in milliseconds.

    Returns
    -------
    dict[str, object]
        Row data for lineage diagnostics table.

    Raises
    ------
    ImportError
        Raised when LineagePayload type is unavailable.
    TypeError
        Raised when payload is not a LineagePayload instance.
    """
    if LineagePayload is None:
        msg = "LineagePayload not available; sqlglot_tools.lineage not imported"
        raise ImportError(msg)

    if not isinstance(payload, LineagePayload):
        msg = f"Expected LineagePayload, got {type(payload).__name__}"
        raise TypeError(msg)

    return {
        "query_id": query_id,
        "timestamp_ms": timestamp_ms,
        "tables": list(payload.tables),
        "columns": list(payload.columns),
        "scopes": list(payload.scopes),
        "canonical_fingerprint": payload.canonical_fingerprint,
        "qualified_sql": payload.qualified_sql,
    }


__all__ = [
    "FilePruningDiagnostics",
    "FilePruningDiagnosticsSpec",
    "build_file_pruning_diagnostics_row",
    "datafusion_cache_state_table",
    "datafusion_ddl_fingerprints_table",
    "datafusion_explains_table",
    "datafusion_object_stores_table",
    "datafusion_plan_artifacts_table",
    "datafusion_runs_table",
    "datafusion_schema_map_fingerprints_table",
    "datafusion_schema_registry_validation_table",
    "datafusion_udf_validation_table",
    "engine_runtime_table",
    "feature_state_table",
    "file_pruning_diagnostics_table",
    "lineage_diagnostics_row",
]

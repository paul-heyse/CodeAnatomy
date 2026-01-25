"""Diagnostics table builders."""

from __future__ import annotations

import hashlib
import json
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Annotated

import msgspec
import pyarrow as pa

from arrowdsl.core.interop import RecordBatchReaderLike, TableLike
from arrowdsl.schema.build import table_from_rows
from datafusion_engine.schema_registry import schema_for
from serde_msgspec import coalesce_unset_or_none, convert, unset_to_none

try:
    from sqlglot_tools.lineage import LineagePayload
except ImportError:
    LineagePayload = None  # type: ignore[misc, assignment]


class DiagnosticsStruct(
    msgspec.Struct,
    frozen=True,
    kw_only=True,
    omit_defaults=True,
    repr_omit_defaults=True,
    forbid_unknown_fields=False,
):
    """Base struct for diagnostics ingestion."""


EventTimeMs = Annotated[int, msgspec.Meta(ge=0)]


def _convert_event[T](obj: object, *, target_type: type[T]) -> T:
    try:
        return convert(obj, target_type=target_type, strict=False, from_attributes=True)
    except msgspec.ValidationError:
        return target_type()


def _now_ms() -> int:
    return int(time.time() * 1000)


def _coalesce_event_time(
    value: int | msgspec.UnsetType | None,
    *,
    default: int,
) -> int:
    if isinstance(value, bool):
        return default
    return coalesce_unset_or_none(value, default)


def _coalesce_int(
    value: int | msgspec.UnsetType | None,
    *,
    default: int,
) -> int:
    if isinstance(value, bool):
        return default
    return coalesce_unset_or_none(value, default)


def _coerce_int_value(value: object | msgspec.UnsetType) -> int | None:
    if value is msgspec.UNSET or value is None or isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, (float, str, bytes, bytearray)):
        try:
            return int(value)
        except (TypeError, ValueError):
            return None
    return None


def _optional_int(value: int | msgspec.UnsetType | None) -> int | None:
    normalized = unset_to_none(value)
    if normalized is None or isinstance(normalized, bool):
        return None
    return normalized


def _optional_float(value: float | msgspec.UnsetType | None) -> float | None:
    normalized = unset_to_none(value)
    if normalized is None or isinstance(normalized, bool):
        return None
    return float(normalized)


def _coalesce_float(
    value: float | msgspec.UnsetType | None,
    *,
    default: float,
) -> float:
    if isinstance(value, bool):
        return default
    return float(coalesce_unset_or_none(value, default))


def _string_list(value: object | msgspec.UnsetType) -> list[str] | None:
    if value is msgspec.UNSET or value is None:
        return None
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [str(item) for item in value]
    return [str(value)]


def _string_list_or_empty(value: object | msgspec.UnsetType) -> list[str]:
    items = _string_list(value)
    return items if items is not None else []


def _string_map(value: object | msgspec.UnsetType) -> dict[str, str] | None:
    if value is msgspec.UNSET or value is None:
        return None
    if isinstance(value, Mapping):
        return {str(key): str(item) for key, item in value.items()}
    return None


def _stringify_payload(value: object) -> str | None:
    if value is None or value is msgspec.UNSET:
        return None
    if isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=True, default=str)


def _binary_json_payload(value: object) -> bytes | None:
    if value is None or value is msgspec.UNSET:
        return None
    if isinstance(value, bytes):
        return value
    if isinstance(value, bytearray):
        return bytes(value)
    if isinstance(value, memoryview):
        return value.tobytes()
    payload = json.dumps(value, ensure_ascii=True, default=str)
    return payload.encode("utf-8")


def _binary_payload(value: object) -> bytes | None:
    if value is None or value is msgspec.UNSET:
        return None
    if isinstance(value, bytes):
        return value
    if isinstance(value, bytearray):
        return bytes(value)
    if isinstance(value, memoryview):
        return value.tobytes()
    return None


def _parse_error_details(value: object | msgspec.UnsetType) -> list[dict[str, object]] | None:
    if value is msgspec.UNSET or value is None:
        return None
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        return None
    details: list[dict[str, object]] = []
    for item in value:
        if not isinstance(item, Mapping):
            continue
        entry: dict[str, object] = {}
        for key in (
            "description",
            "message",
            "start_context",
            "highlight",
            "end_context",
            "into_expression",
        ):
            value_entry = item.get(key)
            if value_entry is not None:
                entry[key] = str(value_entry)
        line = _coerce_int_value(item.get("line"))
        if line is not None:
            entry["line"] = line
        col = _coerce_int_value(item.get("col"))
        if col is not None:
            entry["col"] = col
        if entry:
            details.append(entry)
    return details or None


class DatafusionExplainEvent(DiagnosticsStruct, frozen=True):
    event_time_unix_ms: EventTimeMs | msgspec.UnsetType | None = msgspec.UNSET
    sql: str | None = None
    rows: object | None = None
    explain_analyze: bool = False


class SchemaRegistryValidationRecord(DiagnosticsStruct, frozen=True):
    event_time_unix_ms: EventTimeMs | msgspec.UnsetType | None = msgspec.UNSET
    missing: Sequence[object] | None = None
    type_errors: Mapping[str, object] | None = None
    view_errors: Mapping[str, object] | None = None
    sql_parse_errors: Mapping[str, object] | None = None
    constraint_drift: Sequence[object] | None = None
    relationship_constraint_errors: Mapping[str, object] | None = None


class SchemaMapFingerprintRecord(DiagnosticsStruct, frozen=True):
    event_time_unix_ms: EventTimeMs | msgspec.UnsetType | None = msgspec.UNSET
    schema_map_hash: str | None = None
    schema_map_version: int | None = None
    table_count: int | None = None
    column_count: int | None = None


class FeatureStateEvent(DiagnosticsStruct, frozen=True):
    profile_name: str | None = None
    determinism_tier: str | None = None
    dynamic_filters_enabled: bool = False
    spill_enabled: bool = False
    named_args_supported: bool = False


class DatafusionRunRecord(DiagnosticsStruct, frozen=True):
    run_id: str | None = None
    label: str | None = None
    start_time_unix_ms: EventTimeMs | msgspec.UnsetType | None = msgspec.UNSET
    end_time_unix_ms: EventTimeMs | msgspec.UnsetType | None = msgspec.UNSET
    status: str | None = None
    duration_ms: int | None = None
    metadata: object | None = None


class DatafusionCacheStateRecord(DiagnosticsStruct, frozen=True):
    cache_name: str | None = None
    event_time_unix_ms: EventTimeMs | msgspec.UnsetType | None = msgspec.UNSET
    entry_count: int | None = None
    hit_count: int | None = None
    miss_count: int | None = None
    eviction_count: int | None = None
    config_ttl: str | None = None
    config_limit: str | None = None


class DatafusionPlanArtifactRecord(DiagnosticsStruct, frozen=True):
    event_time_unix_ms: EventTimeMs | msgspec.UnsetType | None = msgspec.UNSET
    run_id: str | None = None
    plan_hash: str | None = None
    sql: str | None = None
    normalized_sql: str | None = None
    explain: object | None = None
    explain_analyze: object | None = None
    substrait_b64: str | None = None
    substrait_validation: Mapping[str, object] | None = None
    sqlglot_ast: bytes | bytearray | memoryview | None = None
    ibis_decompile: str | None = None
    ibis_sql: str | None = None
    ibis_sql_pretty: str | None = None
    ibis_graphviz: str | None = None
    ibis_compiled_sql: str | None = None
    ibis_compiled_sql_hash: str | None = None
    ibis_compile_params: str | None = None
    ibis_compile_limit: int | None = None
    read_dialect: str | None = None
    write_dialect: str | None = None
    canonical_fingerprint: str | None = None
    lineage_tables: Sequence[object] | object | None = None
    lineage_columns: Sequence[object] | object | None = None
    lineage_scopes: Sequence[object] | object | None = None
    param_signature: str | None = None
    projection_map: bytes | bytearray | memoryview | None = None
    unparsed_sql: str | None = None
    unparse_error: str | None = None
    logical_plan: str | None = None
    optimized_plan: str | None = None
    physical_plan: str | None = None
    graphviz: str | None = None
    partition_count: int | None = None
    join_operators: Sequence[object] | object | None = None


class DatafusionSemanticDiffRecord(DiagnosticsStruct, frozen=True):
    event_time_unix_ms: EventTimeMs | msgspec.UnsetType | None = msgspec.UNSET
    run_id: str | None = None
    plan_hash: str | None = None
    base_plan_hash: str | None = None
    category: str | None = None
    changed: bool = False
    breaking: bool = False
    row_multiplying: bool = False
    change_count: int | None = None


class IbisSqlIngestRecord(DiagnosticsStruct, frozen=True):
    event_time_unix_ms: EventTimeMs | msgspec.UnsetType | None = msgspec.UNSET
    ingest_kind: str | None = None
    source_name: str | None = None
    sql: str | None = None
    decompiled_sql: str | None = None
    schema: Mapping[str, object] | None = None
    dialect: str | None = None
    parse_errors: Sequence[object] | object | None = None
    sqlglot_sql: str | None = None
    normalized_sql: str | None = None
    sqlglot_ast: bytes | bytearray | memoryview | None = None
    ibis_sqlglot_ast: bytes | bytearray | memoryview | None = None
    sqlglot_policy_hash: str | None = None
    sqlglot_policy_snapshot: object | None = None


class SqlglotParseErrorRecord(DiagnosticsStruct, frozen=True):
    event_time_unix_ms: EventTimeMs | msgspec.UnsetType | None = msgspec.UNSET
    source: str | None = None
    sql: str | None = None
    dialect: str | None = None
    error: str | None = None
    parse_errors: Sequence[object] | object | None = None


class EngineRuntimeRecord(DiagnosticsStruct, frozen=True):
    event_time_unix_ms: EventTimeMs | msgspec.UnsetType | None = msgspec.UNSET
    runtime_profile_name: str | None = None
    determinism_tier: str | None = None
    runtime_profile_hash: str | None = None
    runtime_profile_snapshot: bytes | bytearray | memoryview | None = None
    sqlglot_policy_hash: str | None = None
    sqlglot_policy_snapshot: bytes | bytearray | memoryview | None = None
    function_registry_hash: str | None = None
    function_registry_snapshot: bytes | bytearray | memoryview | None = None
    datafusion_settings_hash: str | None = None
    datafusion_settings: bytes | bytearray | memoryview | None = None


class DatafusionUdfValidationRecord(DiagnosticsStruct, frozen=True):
    event_time_unix_ms: EventTimeMs | msgspec.UnsetType | None = msgspec.UNSET
    udf_catalog_policy: str | None = None
    missing_udfs: Sequence[object] | object | None = None
    missing_count: int | None = None


class DatafusionObjectStoreRecord(DiagnosticsStruct, frozen=True):
    event_time_unix_ms: EventTimeMs | msgspec.UnsetType | None = msgspec.UNSET
    scheme: str | None = None
    host: str | None = None
    store_type: str | None = None


class FilePruningDiagnosticsRecord(DiagnosticsStruct, frozen=True):
    event_time_unix_ms: EventTimeMs | msgspec.UnsetType | None = msgspec.UNSET
    table_name: str | None = None
    table_path: str | None = None
    total_files: int | None = None
    candidate_count: int | None = None
    pruned_count: int | None = None
    pruned_percentage: float | int | None = None
    filter_summary: str | None = None


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
        event = _convert_event(explain, target_type=DatafusionExplainEvent)
        artifact_path, artifact_format, schema_fp = _explain_rows_metadata(event.rows)
        rows.append(
            {
                "event_time_unix_ms": _coalesce_event_time(
                    event.event_time_unix_ms,
                    default=now,
                ),
                "sql": str(event.sql or ""),
                "explain_rows_artifact_path": artifact_path,
                "explain_rows_artifact_format": artifact_format,
                "explain_rows_schema_fingerprint": schema_fp,
                "explain_analyze": bool(event.explain_analyze),
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
        event = _convert_event(record, target_type=SchemaRegistryValidationRecord)
        event_time = _coalesce_event_time(event.event_time_unix_ms, default=now)
        missing = _string_list_or_empty(event.missing)
        rows.extend(
            {
                "event_time_unix_ms": event_time,
                "schema_name": name,
                "issue_type": "missing",
                "detail": None,
            }
            for name in missing
        )
        type_errors = event.type_errors
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
        view_errors = event.view_errors
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
        parse_errors = event.sql_parse_errors
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
        constraint_drift = event.constraint_drift
        if isinstance(constraint_drift, Sequence) and not isinstance(
            constraint_drift,
            (str, bytes, bytearray),
        ):
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
        relationship_errors = event.relationship_constraint_errors
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
    rows = []
    for record in records:
        event = _convert_event(record, target_type=SchemaMapFingerprintRecord)
        rows.append(
            {
                "event_time_unix_ms": _coalesce_event_time(
                    event.event_time_unix_ms,
                    default=now,
                ),
                "schema_map_hash": str(event.schema_map_hash or ""),
                "schema_map_version": _coalesce_int(
                    event.schema_map_version,
                    default=1,
                ),
                "table_count": _coalesce_int(
                    event.table_count,
                    default=0,
                ),
                "column_count": _coalesce_int(
                    event.column_count,
                    default=0,
                ),
            }
        )
    schema = schema_for("datafusion_schema_map_fingerprints_v1")
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
    rows = []
    for event in events:
        entry = _convert_event(event, target_type=FeatureStateEvent)
        rows.append(
            {
                "profile_name": str(entry.profile_name or ""),
                "determinism_tier": str(entry.determinism_tier or ""),
                "dynamic_filters_enabled": bool(entry.dynamic_filters_enabled),
                "spill_enabled": bool(entry.spill_enabled),
                "named_args_supported": bool(entry.named_args_supported),
            }
        )
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
        entry = _convert_event(record, target_type=DatafusionRunRecord)
        metadata_str = _stringify_payload(entry.metadata)
        end_time = None
        if entry.end_time_unix_ms is not None and entry.end_time_unix_ms is not msgspec.UNSET:
            end_time = _coalesce_event_time(entry.end_time_unix_ms, default=0)
        duration_ms = None
        if entry.duration_ms is not None and not isinstance(entry.duration_ms, bool):
            duration_ms = entry.duration_ms
        rows.append(
            {
                "run_id": str(entry.run_id or ""),
                "label": str(entry.label or ""),
                "start_time_unix_ms": _coalesce_event_time(
                    entry.start_time_unix_ms,
                    default=now,
                ),
                "end_time_unix_ms": end_time,
                "status": str(entry.status or "unknown"),
                "duration_ms": duration_ms,
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
    rows = []
    for record in records:
        entry = _convert_event(record, target_type=DatafusionCacheStateRecord)
        rows.append(
            {
                "cache_name": str(entry.cache_name or ""),
                "event_time_unix_ms": _coalesce_event_time(
                    entry.event_time_unix_ms,
                    default=now,
                ),
                "entry_count": _optional_int(entry.entry_count),
                "hit_count": _optional_int(entry.hit_count),
                "miss_count": _optional_int(entry.miss_count),
                "eviction_count": _optional_int(entry.eviction_count),
                "config_ttl": str(entry.config_ttl) if entry.config_ttl is not None else None,
                "config_limit": str(entry.config_limit) if entry.config_limit is not None else None,
            }
        )
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


def datafusion_semantic_diff_table(
    records: Sequence[Mapping[str, object]],
) -> pa.Table:
    """Build a DataFusion semantic diff diagnostics table.

    Returns
    -------
    pyarrow.Table
        Diagnostics table aligned to DATAFUSION_SEMANTIC_DIFF_V1.
    """
    now = _now_ms()
    rows: list[dict[str, object]] = []
    for record in records:
        entry = _convert_event(record, target_type=DatafusionSemanticDiffRecord)
        rows.append(
            {
                "event_time_unix_ms": _coalesce_event_time(
                    entry.event_time_unix_ms,
                    default=now,
                ),
                "run_id": str(entry.run_id) if entry.run_id is not None else None,
                "plan_hash": str(entry.plan_hash) if entry.plan_hash is not None else None,
                "base_plan_hash": (
                    str(entry.base_plan_hash) if entry.base_plan_hash is not None else None
                ),
                "category": str(entry.category or ""),
                "changed": bool(entry.changed),
                "breaking": bool(entry.breaking),
                "row_multiplying": bool(entry.row_multiplying),
                "change_count": _optional_int(entry.change_count),
            }
        )
    schema = schema_for("datafusion_semantic_diff_v1")
    return table_from_rows(schema, rows)


def _plan_artifact_row(
    artifact: Mapping[str, object],
    *,
    default_time: int,
) -> dict[str, object]:
    entry = _convert_event(artifact, target_type=DatafusionPlanArtifactRecord)
    explain_path, explain_format, explain_fp = _explain_rows_metadata(entry.explain)
    analyze_path, analyze_format, _ = _explain_rows_metadata(entry.explain_analyze)
    substrait_validation = entry.substrait_validation
    substrait_status = None
    if substrait_validation and isinstance(substrait_validation, Mapping):
        substrait_status = str(substrait_validation.get("status") or "")
    return {
        "event_time_unix_ms": _coalesce_event_time(
            entry.event_time_unix_ms,
            default=default_time,
        ),
        "run_id": str(entry.run_id) if entry.run_id is not None else None,
        "plan_hash": str(entry.plan_hash) if entry.plan_hash is not None else None,
        "sql": str(entry.sql or ""),
        "normalized_sql": str(entry.normalized_sql) if entry.normalized_sql is not None else None,
        "explain_rows_artifact_path": explain_path,
        "explain_rows_artifact_format": explain_format,
        "explain_rows_schema_fingerprint": explain_fp,
        "explain_analyze_artifact_path": analyze_path,
        "explain_analyze_artifact_format": analyze_format,
        "substrait_b64": str(entry.substrait_b64) if entry.substrait_b64 is not None else None,
        "substrait_validation_status": substrait_status,
        "sqlglot_ast": _binary_payload(entry.sqlglot_ast),
        "ibis_decompile": str(entry.ibis_decompile) if entry.ibis_decompile is not None else None,
        "ibis_sql": str(entry.ibis_sql) if entry.ibis_sql is not None else None,
        "ibis_sql_pretty": str(entry.ibis_sql_pretty)
        if entry.ibis_sql_pretty is not None
        else None,
        "ibis_graphviz": str(entry.ibis_graphviz) if entry.ibis_graphviz is not None else None,
        "ibis_compiled_sql": (
            str(entry.ibis_compiled_sql) if entry.ibis_compiled_sql is not None else None
        ),
        "ibis_compiled_sql_hash": (
            str(entry.ibis_compiled_sql_hash) if entry.ibis_compiled_sql_hash is not None else None
        ),
        "ibis_compile_params": (
            str(entry.ibis_compile_params) if entry.ibis_compile_params is not None else None
        ),
        "ibis_compile_limit": _optional_int(entry.ibis_compile_limit),
        "read_dialect": str(entry.read_dialect) if entry.read_dialect is not None else None,
        "write_dialect": str(entry.write_dialect) if entry.write_dialect is not None else None,
        "canonical_fingerprint": (
            str(entry.canonical_fingerprint) if entry.canonical_fingerprint is not None else None
        ),
        "lineage_tables": _string_list(entry.lineage_tables),
        "lineage_columns": _string_list(entry.lineage_columns),
        "lineage_scopes": _string_list(entry.lineage_scopes),
        "param_signature": (
            str(entry.param_signature) if entry.param_signature is not None else None
        ),
        "projection_map": _binary_payload(entry.projection_map),
        "unparsed_sql": str(entry.unparsed_sql) if entry.unparsed_sql is not None else None,
        "unparse_error": str(entry.unparse_error) if entry.unparse_error is not None else None,
        "logical_plan": str(entry.logical_plan) if entry.logical_plan is not None else None,
        "optimized_plan": str(entry.optimized_plan) if entry.optimized_plan is not None else None,
        "physical_plan": str(entry.physical_plan) if entry.physical_plan is not None else None,
        "graphviz": str(entry.graphviz) if entry.graphviz is not None else None,
        "partition_count": _optional_int(entry.partition_count),
        "join_operators": _string_list(entry.join_operators),
    }


def ibis_sql_ingest_table(
    artifacts: Sequence[Mapping[str, object]],
) -> pa.Table:
    """Build an Ibis SQL ingest diagnostics table.

    Returns
    -------
    pyarrow.Table
        Diagnostics table aligned to IBIS_SQL_INGEST_V1.
    """
    now = _now_ms()
    rows = [_ibis_sql_ingest_row(artifact, default_time=now) for artifact in artifacts]
    schema = schema_for("ibis_sql_ingest_v1")
    return table_from_rows(schema, rows)


def _ibis_sql_ingest_row(
    artifact: Mapping[str, object],
    *,
    default_time: int,
) -> dict[str, object]:
    entry = _convert_event(artifact, target_type=IbisSqlIngestRecord)
    ingest_kind = str(entry.ingest_kind or "unknown")
    return {
        "event_time_unix_ms": _coalesce_event_time(
            entry.event_time_unix_ms,
            default=default_time,
        ),
        "ingest_kind": ingest_kind,
        "source_name": str(entry.source_name) if entry.source_name is not None else None,
        "sql": str(entry.sql or ""),
        "decompiled_sql": str(entry.decompiled_sql) if entry.decompiled_sql is not None else None,
        "schema": _string_map(entry.schema),
        "dialect": str(entry.dialect) if entry.dialect is not None else None,
        "parse_errors": _parse_error_details(entry.parse_errors),
        "sqlglot_sql": str(entry.sqlglot_sql) if entry.sqlglot_sql is not None else None,
        "normalized_sql": str(entry.normalized_sql) if entry.normalized_sql is not None else None,
        "sqlglot_ast": _binary_payload(entry.sqlglot_ast),
        "ibis_sqlglot_ast": _binary_payload(entry.ibis_sqlglot_ast),
        "sqlglot_policy_hash": (
            str(entry.sqlglot_policy_hash) if entry.sqlglot_policy_hash is not None else None
        ),
        "sqlglot_policy_snapshot": _binary_json_payload(entry.sqlglot_policy_snapshot),
    }


def sqlglot_parse_errors_table(
    records: Sequence[Mapping[str, object]],
) -> pa.Table:
    """Build a SQLGlot parse errors diagnostics table.

    Returns
    -------
    pyarrow.Table
        Diagnostics table aligned to SQLGLOT_PARSE_ERRORS_V1.
    """
    now = _now_ms()
    rows = [_sqlglot_parse_error_row(record, default_time=now) for record in records]
    schema = schema_for("sqlglot_parse_errors_v1")
    return table_from_rows(schema, rows)


def _sqlglot_parse_error_row(
    record: Mapping[str, object],
    *,
    default_time: int,
) -> dict[str, object]:
    entry = _convert_event(record, target_type=SqlglotParseErrorRecord)
    return {
        "event_time_unix_ms": _coalesce_event_time(
            entry.event_time_unix_ms,
            default=default_time,
        ),
        "source": str(entry.source) if entry.source is not None else None,
        "sql": str(entry.sql) if entry.sql is not None else None,
        "dialect": str(entry.dialect) if entry.dialect is not None else None,
        "error": str(entry.error) if entry.error is not None else None,
        "parse_errors": _parse_error_details(entry.parse_errors),
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
    rows = []
    for artifact in artifacts:
        entry = _convert_event(artifact, target_type=EngineRuntimeRecord)
        rows.append(
            {
                "event_time_unix_ms": _coalesce_event_time(
                    entry.event_time_unix_ms,
                    default=now,
                ),
                "runtime_profile_name": str(entry.runtime_profile_name or ""),
                "determinism_tier": str(entry.determinism_tier or ""),
                "runtime_profile_hash": str(entry.runtime_profile_hash or ""),
                "runtime_profile_snapshot": _binary_payload(entry.runtime_profile_snapshot) or b"",
                "sqlglot_policy_hash": (
                    str(entry.sqlglot_policy_hash)
                    if entry.sqlglot_policy_hash is not None
                    else None
                ),
                "sqlglot_policy_snapshot": _binary_payload(entry.sqlglot_policy_snapshot),
                "function_registry_hash": (
                    str(entry.function_registry_hash)
                    if entry.function_registry_hash is not None
                    else None
                ),
                "function_registry_snapshot": _binary_payload(entry.function_registry_snapshot),
                "datafusion_settings_hash": (
                    str(entry.datafusion_settings_hash)
                    if entry.datafusion_settings_hash is not None
                    else None
                ),
                "datafusion_settings": _binary_payload(entry.datafusion_settings),
            }
        )
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
    rows = []
    for record in records:
        entry = _convert_event(record, target_type=DatafusionUdfValidationRecord)
        rows.append(
            {
                "event_time_unix_ms": _coalesce_event_time(
                    entry.event_time_unix_ms,
                    default=now,
                ),
                "udf_catalog_policy": str(entry.udf_catalog_policy or "default"),
                "missing_udfs": _string_list_or_empty(entry.missing_udfs),
                "missing_count": _optional_int(entry.missing_count),
            }
        )
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
    rows = []
    for record in records:
        entry = _convert_event(record, target_type=DatafusionObjectStoreRecord)
        rows.append(
            {
                "event_time_unix_ms": _coalesce_event_time(
                    entry.event_time_unix_ms,
                    default=now,
                ),
                "scheme": str(entry.scheme or ""),
                "host": str(entry.host) if entry.host is not None else None,
                "store_type": str(entry.store_type) if entry.store_type is not None else None,
            }
        )
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
    rows = []
    for record in records:
        entry = _convert_event(record, target_type=FilePruningDiagnosticsRecord)
        rows.append(
            {
                "event_time_unix_ms": _coalesce_event_time(
                    entry.event_time_unix_ms,
                    default=now,
                ),
                "table_name": str(entry.table_name or ""),
                "table_path": str(entry.table_path or ""),
                "total_files": _coalesce_int(
                    entry.total_files,
                    default=0,
                ),
                "candidate_count": _coalesce_int(
                    entry.candidate_count,
                    default=0,
                ),
                "pruned_count": _coalesce_int(
                    entry.pruned_count,
                    default=0,
                ),
                "pruned_percentage": _coalesce_float(
                    entry.pruned_percentage,
                    default=0.0,
                ),
                "filter_summary": str(entry.filter_summary or ""),
            }
        )

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


_DIAGNOSTICS_MSGPACK_ENCODER = msgspec.msgpack.Encoder(order="deterministic")


def encode_diagnostics_rows(rows: Sequence[Mapping[str, object]]) -> bytes:
    """Encode diagnostics rows to MessagePack bytes.

    Returns
    -------
    bytes
        MessagePack payload for diagnostics rows.
    """
    buf = bytearray()
    _DIAGNOSTICS_MSGPACK_ENCODER.encode_into(list(rows), buf)
    return bytes(buf)


__all__ = [
    "FilePruningDiagnostics",
    "FilePruningDiagnosticsSpec",
    "build_file_pruning_diagnostics_row",
    "datafusion_cache_state_table",
    "datafusion_explains_table",
    "datafusion_object_stores_table",
    "datafusion_plan_artifacts_table",
    "datafusion_runs_table",
    "datafusion_schema_map_fingerprints_table",
    "datafusion_schema_registry_validation_table",
    "datafusion_semantic_diff_table",
    "datafusion_udf_validation_table",
    "encode_diagnostics_rows",
    "engine_runtime_table",
    "feature_state_table",
    "file_pruning_diagnostics_table",
    "ibis_sql_ingest_table",
    "lineage_diagnostics_row",
    "sqlglot_parse_errors_table",
]

"""Diagnostics table builders."""

from __future__ import annotations

import hashlib
import json
import time
from collections.abc import Mapping, Sequence

import pyarrow as pa

from arrowdsl.core.interop import RecordBatchReaderLike, TableLike
from arrowdsl.schema.build import table_from_rows
from datafusion_engine.schema_registry import schema_for


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


def _coerce_str_list(value: object) -> list[str]:
    if value is None:
        return []
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [str(item) for item in value]
    return [str(value)]


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
    schema = schema_for("datafusion_schema_registry_validation_v1")
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


__all__ = [
    "datafusion_explains_table",
    "datafusion_schema_registry_validation_table",
    "feature_state_table",
]

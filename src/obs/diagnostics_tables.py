"""Diagnostics table builders."""

from __future__ import annotations

import json
import time
from collections.abc import Mapping, Sequence
from typing import cast

import pyarrow as pa

from arrowdsl.core.interop import RecordBatchReaderLike, TableLike
from arrowdsl.schema.build import table_from_rows
from arrowdsl.schema.serialization import schema_fingerprint
from obs.diagnostics_schemas import (
    DATAFUSION_EXPLAINS_V1,
    DATAFUSION_FALLBACKS_V1,
    FEATURE_STATE_V1,
)


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


def datafusion_fallbacks_table(events: Sequence[Mapping[str, object]]) -> pa.Table:
    """Build a DataFusion fallback diagnostics table.

    Returns
    -------
    pyarrow.Table
        Diagnostics table aligned to DATAFUSION_FALLBACKS_V1.
    """
    now = _now_ms()
    rows = [
        {
            "event_time_unix_ms": _coerce_event_time(event.get("event_time_unix_ms"), default=now),
            "reason": str(event.get("reason") or ""),
            "error": str(event.get("error") or ""),
            "expression_type": str(event.get("expression_type") or ""),
            "sql": str(event.get("sql") or ""),
            "dialect": str(event.get("dialect") or ""),
            "policy_violations": _coerce_str_list(event.get("policy_violations")),
            "sql_policy_name": (
                str(event.get("sql_policy_name"))
                if event.get("sql_policy_name") is not None
                else None
            ),
            "param_mode": (
                str(event.get("param_mode")) if event.get("param_mode") is not None else None
            ),
        }
        for event in events
    ]
    return table_from_rows(DATAFUSION_FALLBACKS_V1, rows)


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
        rows_payload: object
        if isinstance(raw_rows, (RecordBatchReaderLike, TableLike)):
            rows_payload = {
                "artifact_path": None,
                "format": "ipc_file",
                "schema_fingerprint": schema_fingerprint(raw_rows.schema),
            }
        else:
            rows_payload = raw_rows if raw_rows is not None else cast("list[object]", [])
        rows.append(
            {
                "event_time_unix_ms": _coerce_event_time(
                    explain.get("event_time_unix_ms"), default=now
                ),
                "sql": str(explain.get("sql") or ""),
                "explain_rows_json": json.dumps(
                    rows_payload,
                    ensure_ascii=True,
                    separators=(",", ":"),
                ),
                "explain_analyze": bool(explain.get("explain_analyze") or False),
            }
        )
    return table_from_rows(DATAFUSION_EXPLAINS_V1, rows)


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
    return table_from_rows(FEATURE_STATE_V1, rows)


__all__ = ["datafusion_explains_table", "datafusion_fallbacks_table", "feature_state_table"]

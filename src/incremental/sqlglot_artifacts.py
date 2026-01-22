"""SQLGlot diagnostics helpers for incremental plans."""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, cast

import pyarrow as pa

from arrowdsl.io.ipc import payload_hash
from incremental.runtime import IncrementalRuntime
from sqlglot_tools.bridge import SqlGlotDiagnostics, SqlGlotDiagnosticsOptions, sqlglot_diagnostics
from sqlglot_tools.optimizer import sqlglot_policy_snapshot_for

if TYPE_CHECKING:
    from ibis.expr.types import Table as IbisTable

    from sqlglot_tools.bridge import IbisCompilerBackend


_SQLGLOT_ARTIFACT_SCHEMA = pa.schema(
    [
        pa.field("name", pa.string(), nullable=False),
        pa.field("sql_dialect", pa.string(), nullable=True),
        pa.field("sql_text_raw", pa.string(), nullable=True),
        pa.field("sql_text_optimized", pa.string(), nullable=True),
        pa.field("tables", pa.list_(pa.string()), nullable=True),
        pa.field("columns", pa.list_(pa.string()), nullable=True),
        pa.field("identifiers", pa.list_(pa.string()), nullable=True),
        pa.field("ast_repr", pa.string(), nullable=True),
        pa.field("normalization_distance", pa.int64(), nullable=True),
        pa.field("normalization_max_distance", pa.int64(), nullable=True),
        pa.field("normalization_applied", pa.bool_(), nullable=True),
        pa.field("policy_hash", pa.string(), nullable=True),
        pa.field("policy_rules_hash", pa.string(), nullable=True),
    ]
)


def sqlglot_artifact_payload(
    *,
    name: str,
    diagnostics: SqlGlotDiagnostics,
    policy_hash: str,
    policy_rules_hash: str,
) -> dict[str, object]:
    """Return a normalized payload for SQLGlot plan artifacts.

    Returns
    -------
    dict[str, object]
        Normalized SQLGlot artifact payload.
    """
    return {
        "name": name,
        "sql_dialect": diagnostics.sql_dialect,
        "sql_text_raw": diagnostics.sql_text_raw,
        "sql_text_optimized": diagnostics.sql_text_optimized,
        "tables": sorted(diagnostics.tables),
        "columns": sorted(diagnostics.columns),
        "identifiers": sorted(diagnostics.identifiers),
        "ast_repr": diagnostics.ast_repr,
        "normalization_distance": diagnostics.normalization_distance,
        "normalization_max_distance": diagnostics.normalization_max_distance,
        "normalization_applied": diagnostics.normalization_applied,
        "policy_hash": policy_hash,
        "policy_rules_hash": policy_rules_hash,
    }


def sqlglot_artifact_hash(payload: Mapping[str, object]) -> str:
    """Return a stable hash for a SQLGlot diagnostics payload.

    Returns
    -------
    str
        Stable hash for the normalized SQLGlot payload.
    """
    normalized: dict[str, object] = dict(payload)
    for key in ("tables", "columns", "identifiers"):
        value = normalized.get(key)
        if isinstance(value, list):
            normalized[key] = sorted(str(item) for item in value)
    return payload_hash(normalized, _SQLGLOT_ARTIFACT_SCHEMA)


def record_sqlglot_artifact(
    runtime: IncrementalRuntime,
    *,
    name: str,
    expr: IbisTable,
) -> None:
    """Record SQLGlot diagnostics for an Ibis expression."""
    sink = runtime.profile.diagnostics_sink
    if sink is None:
        return
    backend = cast("IbisCompilerBackend", runtime.ibis_backend())
    diagnostics = sqlglot_diagnostics(
        expr,
        backend=backend,
        options=SqlGlotDiagnosticsOptions(policy=runtime.sqlglot_policy),
    )
    policy_snapshot = sqlglot_policy_snapshot_for(runtime.sqlglot_policy)
    payload = sqlglot_artifact_payload(
        name=name,
        diagnostics=diagnostics,
        policy_hash=policy_snapshot.policy_hash,
        policy_rules_hash=policy_snapshot.rules_hash,
    )
    sink.record_artifact("incremental_sqlglot_plan_v1", payload)

__all__ = [
    "record_sqlglot_artifact",
    "sqlglot_artifact_hash",
    "sqlglot_artifact_payload",
]

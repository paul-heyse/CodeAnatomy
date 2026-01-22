"""SQLGlot diagnostics helpers for incremental plans."""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

from incremental.runtime import IncrementalRuntime
from sqlglot_tools.bridge import SqlGlotDiagnosticsOptions, sqlglot_diagnostics
from sqlglot_tools.optimizer import sqlglot_policy_snapshot_for

if TYPE_CHECKING:
    from ibis.expr.types import Table as IbisTable

    from sqlglot_tools.bridge import IbisCompilerBackend


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
    payload: dict[str, object] = {
        "name": name,
        "sql_dialect": diagnostics.sql_dialect,
        "sql_text_raw": diagnostics.sql_text_raw,
        "sql_text_optimized": diagnostics.sql_text_optimized,
        "tables": list(diagnostics.tables),
        "columns": list(diagnostics.columns),
        "identifiers": list(diagnostics.identifiers),
        "ast_repr": diagnostics.ast_repr,
        "normalization_distance": diagnostics.normalization_distance,
        "normalization_max_distance": diagnostics.normalization_max_distance,
        "normalization_applied": diagnostics.normalization_applied,
        "policy_hash": policy_snapshot.policy_hash,
        "policy_rules_hash": policy_snapshot.rules_hash,
    }
    sink.record_artifact("incremental_sqlglot_plan_v1", payload)


__all__ = ["record_sqlglot_artifact"]

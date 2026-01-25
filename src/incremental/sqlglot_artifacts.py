"""SQLGlot diagnostics helpers for incremental plans."""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, cast

from cache.diskcache_factory import cache_for_kind
from datafusion_engine.diagnostics import record_artifact
from datafusion_engine.schema_introspection import SchemaMapCacheOptions, schema_map_snapshot
from datafusion_engine.sql_options import sql_options_for_profile
from incremental.runtime import IncrementalRuntime
from sqlglot_tools.bridge import (
    SqlGlotPlanArtifacts,
    SqlGlotPlanOptions,
    collect_sqlglot_plan_artifacts,
)
from sqlglot_tools.lineage import LineagePayload

if TYPE_CHECKING:
    from ibis.expr.types import Table as IbisTable

    from sqlglot_tools.bridge import IbisCompilerBackend


def sqlglot_plan_artifact_payload(
    *,
    name: str,
    artifacts: SqlGlotPlanArtifacts,
) -> dict[str, object]:
    """Return a normalized payload for SQLGlot plan artifacts.

    Returns
    -------
    dict[str, object]
        Normalized SQLGlot artifact payload.
    """
    diagnostics = artifacts.diagnostics
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
        "plan_hash": artifacts.plan_hash,
        "policy_hash": artifacts.policy_hash,
        "policy_rules_hash": artifacts.policy_rules_hash,
        "schema_map_hash": artifacts.schema_map_hash,
        "canonical_fingerprint": artifacts.canonical_fingerprint,
        "sqlglot_ast": artifacts.sqlglot_ast,
        "lineage": _lineage_payload(artifacts.lineage),
    }


def record_sqlglot_plan_artifact(
    runtime: IncrementalRuntime,
    *,
    name: str,
    expr: IbisTable,
) -> None:
    """Record SQLGlot plan artifacts for an Ibis expression."""
    backend = cast("IbisCompilerBackend", runtime.ibis_backend())
    cache_profile = runtime.profile.diskcache_profile
    cache = cache_for_kind(cache_profile, "schema") if cache_profile is not None else None
    cache_key = (
        f"schema_map:{runtime.profile.context_cache_key()}" if cache_profile is not None else None
    )
    cache_ttl = cache_profile.ttl_for("schema") if cache_profile is not None else None
    cache_options = (
        SchemaMapCacheOptions(
            cache=cache,
            key=cache_key,
            ttl=cache_ttl,
            tag=runtime.profile.context_cache_key(),
        )
        if cache is not None and cache_key is not None
        else None
    )
    schema_map, schema_map_hash = schema_map_snapshot(
        runtime.session_context(),
        sql_options=sql_options_for_profile(runtime.profile),
        cache_options=cache_options,
    )
    artifacts = collect_sqlglot_plan_artifacts(
        expr,
        backend=backend,
        options=SqlGlotPlanOptions(
            policy=runtime.sqlglot_policy,
            schema_map=schema_map,
            schema_map_hash=schema_map_hash,
        ),
    )
    payload = sqlglot_plan_artifact_payload(
        name=name,
        artifacts=artifacts,
    )
    record_artifact(runtime.profile, "incremental_sqlglot_plan_v1", payload)


def _lineage_payload(lineage: LineagePayload | None) -> Mapping[str, object] | None:
    if lineage is None:
        return None
    return {
        "tables": list(lineage.tables),
        "columns": list(lineage.columns),
        "scopes": list(lineage.scopes),
        "canonical_fingerprint": lineage.canonical_fingerprint,
        "qualified_sql": lineage.qualified_sql,
    }


__all__ = ["record_sqlglot_plan_artifact", "sqlglot_plan_artifact_payload"]

"""Query-focused helpers for the plan artifact store."""

from __future__ import annotations

from collections.abc import Mapping, Sequence

import msgspec
import pyarrow as pa
from datafusion import SessionContext, SQLOptions
from msgspec import convert

from datafusion_engine.plan import artifact_store_core as _core
from datafusion_engine.plan.artifact_store_core import (
    _apply_plan_artifact_retention,
    _comparison_policy_for_profile,
    _DeterminismRow,
)
from datafusion_engine.session.runtime import DataFusionRuntimeProfile
from datafusion_engine.sql.options import sql_options_for_profile
from serde_msgspec import validation_error_payload

DeterminismValidationResult = _core.DeterminismValidationResult


def _latest_plan_snapshot_by_view(
    ctx: SessionContext,
    profile: DataFusionRuntimeProfile,
    *,
    table_path: str,
    view_names: Sequence[str],
) -> dict[str, tuple[str | None, str | None]]:
    sql_options = sql_options_for_profile(profile)
    snapshots: dict[str, tuple[str | None, str | None]] = {}
    for view_name in sorted(set(view_names)):
        escaped_view = view_name.replace("'", "''")
        query = (
            "SELECT plan_fingerprint, plan_identity_hash "
            f"FROM delta_scan('{table_path}') "
            f"WHERE view_name = '{escaped_view}' "
            "ORDER BY event_time_unix_ms DESC LIMIT 1"
        )
        try:
            batches = ctx.sql_with_options(query, sql_options).collect()
        except (RuntimeError, ValueError, TypeError):
            continue
        rows: list[dict[str, object]] = (
            [dict(row) for row in pa.Table.from_batches(batches).to_pylist()] if batches else []
        )
        if not rows:
            continue
        payload = rows[0]
        plan_fingerprint = payload.get("plan_fingerprint")
        plan_identity = payload.get("plan_identity_hash")
        snapshots[view_name] = (
            str(plan_fingerprint) if isinstance(plan_fingerprint, str) else None,
            str(plan_identity) if isinstance(plan_identity, str) else None,
        )
    return snapshots


def _plan_diff_gate_violations(
    rows: Sequence[_core.PlanArtifactRow],
    *,
    previous_by_view: Mapping[str, tuple[str | None, str | None]],
) -> tuple[dict[str, object], ...]:
    violations: list[dict[str, object]] = []
    for row in rows:
        previous = previous_by_view.get(row.view_name)
        if previous is None:
            continue
        previous_fingerprint, previous_identity = previous
        if previous_identity is not None and previous_identity != row.plan_identity_hash:
            violations.append(
                {
                    "view_name": row.view_name,
                    "previous_plan_fingerprint": previous_fingerprint,
                    "previous_plan_identity_hash": previous_identity,
                    "current_plan_fingerprint": row.plan_fingerprint,
                    "current_plan_identity_hash": row.plan_identity_hash,
                }
            )
    return tuple(violations)


def _collect_determinism_results(
    ctx: SessionContext,
    table_path: str,
    *,
    view_name: str | None,
    plan_fingerprint: str,
    sql_options: SQLOptions,
) -> tuple[list[pa.RecordBatch] | None, str | None]:
    identity_error: str | None = None
    identity_query = _determinism_validation_query(
        table_path,
        view_name=view_name,
        plan_fingerprint=plan_fingerprint,
        include_identity=True,
    )
    try:
        return ctx.sql_with_options(identity_query, sql_options).collect(), None
    except (RuntimeError, ValueError, TypeError) as exc:
        identity_error = str(exc)
    fallback_query = _determinism_validation_query(
        table_path,
        view_name=view_name,
        plan_fingerprint=plan_fingerprint,
        include_identity=False,
    )
    try:
        return ctx.sql_with_options(fallback_query, sql_options).collect(), identity_error
    except (RuntimeError, ValueError, TypeError) as fallback_exc:
        error = identity_error or str(fallback_exc)
        return None, error


def _determinism_sets(
    results: Sequence[pa.RecordBatch],
) -> tuple[int, set[str], set[str]]:
    fingerprints: set[str] = set()
    identities: set[str] = set()
    row_count = 0
    for batch in results:
        for row in batch.to_pylist():
            row_count += 1
            try:
                payload = convert(row, type=_DeterminismRow, strict=True)
            except msgspec.ValidationError as exc:
                details = validation_error_payload(exc)
                msg = f"Determinism row validation failed: {details}"
                raise ValueError(msg) from exc
            if payload.plan_fingerprint is not None:
                fingerprints.add(str(payload.plan_fingerprint))
            if payload.plan_identity_hash is not None:
                identities.add(str(payload.plan_identity_hash))
    return row_count, fingerprints, identities


def _determinism_outcome(
    *,
    plan_fingerprint: str,
    fingerprints: set[str],
    identities: set[str],
) -> tuple[bool, tuple[str, ...], tuple[str, ...]]:
    plan_identity_hashes = tuple(sorted(identities))
    if plan_identity_hashes:
        baseline_identity = plan_identity_hashes[0]
        conflicting_identities = tuple(
            value for value in plan_identity_hashes if value != baseline_identity
        )
        return (not conflicting_identities), plan_identity_hashes, conflicting_identities
    is_deterministic = len(fingerprints) <= 1 or plan_fingerprint in fingerprints
    return is_deterministic, plan_identity_hashes, ()


def _determinism_validation_query(
    table_path: str,
    *,
    view_name: str | None,
    plan_fingerprint: str,
    include_identity: bool,
) -> str:
    """Build SQL query for determinism validation.

    Returns:
    -------
    str
        SQL query string scoped to the requested fingerprint and optional view.
    """
    plan_literal = plan_fingerprint.replace("'", "''")
    select_cols = "plan_fingerprint"
    if include_identity:
        select_cols = "plan_fingerprint, plan_identity_hash"
    base = (
        f"SELECT DISTINCT {select_cols} FROM delta_scan('{table_path}') "
        f"WHERE plan_fingerprint = '{plan_literal}'"
    )
    if view_name is None:
        return base
    view_literal = view_name.replace("'", "''")
    return f"{base} AND view_name = '{view_literal}'"


def validate_plan_determinism(
    ctx: SessionContext,
    profile: DataFusionRuntimeProfile,
    *,
    plan_fingerprint: str,
    view_name: str | None = None,
) -> DeterminismValidationResult:
    """Validate deterministic plan identity across executions.

    Returns:
    -------
    DeterminismValidationResult
        Determinism status and conflict metadata.
    """
    from datafusion_engine.plan.artifact_store_core import plan_artifacts_location

    location = plan_artifacts_location(profile)
    if location is None:
        return DeterminismValidationResult(
            is_deterministic=True,
            plan_fingerprint=plan_fingerprint,
            view_name=view_name,
            matching_artifact_count=0,
            conflicting_fingerprints=(),
            validation_error="artifact_store_disabled",
        )
    table_path = str(location.path)
    results, identity_error = _collect_determinism_results(
        ctx,
        table_path,
        view_name=view_name,
        plan_fingerprint=plan_fingerprint,
        sql_options=sql_options_for_profile(profile),
    )
    if results is None:
        return DeterminismValidationResult(
            is_deterministic=True,
            plan_fingerprint=plan_fingerprint,
            view_name=view_name,
            matching_artifact_count=0,
            conflicting_fingerprints=(),
            validation_error=identity_error,
        )
    row_count, fingerprints, identities = _determinism_sets(results)
    is_deterministic, plan_identity_hashes, conflicting_identities = _determinism_outcome(
        plan_fingerprint=plan_fingerprint,
        fingerprints=fingerprints,
        identities=identities,
    )
    conflicting = tuple(sorted(fp for fp in fingerprints if fp != plan_fingerprint))
    return DeterminismValidationResult(
        is_deterministic=is_deterministic,
        plan_fingerprint=plan_fingerprint,
        view_name=view_name,
        matching_artifact_count=row_count,
        conflicting_fingerprints=conflicting,
        plan_identity_hashes=plan_identity_hashes,
        conflicting_plan_identity_hashes=conflicting_identities,
        validation_error=identity_error,
    )


def persist_plan_artifacts_for_views(
    ctx: SessionContext,
    profile: DataFusionRuntimeProfile,
    *,
    request: _core.PlanArtifactsForViewsRequest,
) -> tuple[_core.PlanArtifactRow, ...]:
    """Persist plan artifacts for view builders.

    Returns:
    -------
    tuple[_core.PlanArtifactRow, ...]
        Persisted plan-artifact rows.

    Raises:
        RuntimeError: If plan-diff gate detects identity changes.
    """
    location = _core.ensure_plan_artifacts_table(ctx, profile)
    if location is None:
        return ()
    comparison_policy = _comparison_policy_for_profile(profile)
    if not comparison_policy.retain_p0_artifacts:
        return ()
    previous_by_view: dict[str, tuple[str | None, str | None]] = {}
    if comparison_policy.enable_diff_gates:
        previous_by_view = _latest_plan_snapshot_by_view(
            ctx,
            profile,
            table_path=str(location.path),
            view_names=[node.name for node in request.view_nodes],
        )
    rows: list[_core.PlanArtifactRow] = []
    for node in request.view_nodes:
        bundle = node.plan_bundle
        if bundle is None:
            continue
        scan_keys = (
            tuple(request.scan_keys_by_view.get(node.name, ())) if request.scan_keys_by_view else ()
        )
        lineage = request.lineage_by_view.get(node.name) if request.lineage_by_view else None
        row = _core.build_plan_artifact_row(
            ctx,
            profile,
            request=_core.PlanArtifactBuildRequest(
                view_name=node.name,
                bundle=bundle,
                lineage=lineage,
                scan_units=request.scan_units,
                scan_keys=scan_keys,
            ),
        )
        rows.append(
            _apply_plan_artifact_retention(
                row,
                comparison_policy=comparison_policy,
            )
        )
    if not rows:
        return ()
    if comparison_policy.enable_diff_gates:
        violations = _plan_diff_gate_violations(
            rows,
            previous_by_view=previous_by_view,
        )
        if violations:
            msg = (
                "Plan artifact comparison gate failed: "
                f"{len(violations)} view(s) changed plan identity."
            )
            raise RuntimeError(msg)
    return _core.persist_plan_artifact_rows(ctx, profile, rows=rows, location=location)


__all__ = [
    "DeterminismValidationResult",
    "persist_plan_artifacts_for_views",
    "validate_plan_determinism",
]

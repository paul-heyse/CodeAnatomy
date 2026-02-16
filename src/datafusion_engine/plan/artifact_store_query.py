"""Query-focused helpers for the plan artifact store."""
# ruff: noqa: SLF001

from __future__ import annotations

from datafusion import SessionContext

from datafusion_engine.plan import artifact_store_core as _core
from datafusion_engine.session.runtime import DataFusionRuntimeProfile

DeterminismValidationResult = _core.DeterminismValidationResult


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
    location = _core._plan_artifacts_location(profile)
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
    results, identity_error = _core._collect_determinism_results(
        ctx,
        table_path,
        view_name=view_name,
        plan_fingerprint=plan_fingerprint,
        sql_options=_core.planning_sql_options(profile),
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
    row_count, fingerprints, identities = _core._determinism_sets(results)
    is_deterministic, plan_identity_hashes, conflicting_identities = _core._determinism_outcome(
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
    comparison_policy = _core._comparison_policy_for_profile(profile)
    if not comparison_policy.retain_p0_artifacts:
        return ()
    previous_by_view: dict[str, tuple[str | None, str | None]] = {}
    if comparison_policy.enable_diff_gates:
        previous_by_view = _core._latest_plan_snapshot_by_view(
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
            _core._apply_plan_artifact_retention(
                row,
                comparison_policy=comparison_policy,
            )
        )
    if not rows:
        return ()
    if comparison_policy.enable_diff_gates:
        violations = _core._plan_diff_gate_violations(
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

"""Two-pass planning pipeline that pins Delta inputs before scheduling."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING

from datafusion import SessionContext

from datafusion_engine.dataset.resolution import apply_scan_unit_overrides
from datafusion_engine.lineage.datafusion import LineageReport
from datafusion_engine.lineage.scan import ScanUnit, plan_scan_units
from datafusion_engine.plan.bundle import PlanBundleOptions, build_plan_bundle
from datafusion_engine.plan.diagnostics import record_plan_bundle_diagnostics
from datafusion_engine.session.facade import DataFusionExecutionFacade
from relspec.inferred_deps import InferredDeps, infer_deps_from_view_nodes
from utils.hashing import hash_msgpack_canonical, hash_sha256_hex

if TYPE_CHECKING:
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile, SessionRuntime
    from datafusion_engine.views.graph import ViewNode
    from semantics.program_manifest import ManifestDatasetResolver

_SCAN_TASK_PREFIX = "scan_unit_"
_HASH_SLICE = 16


@dataclass(frozen=True)
class PlanningPipelineResult:
    """Outputs of the two-pass planning pipeline."""

    view_nodes: tuple[ViewNode, ...]
    inferred: tuple[InferredDeps, ...]
    scan_units: tuple[ScanUnit, ...]
    scan_keys_by_task: Mapping[str, tuple[str, ...]]
    scan_task_name_by_key: Mapping[str, str]
    scan_task_units_by_name: Mapping[str, ScanUnit]
    scan_task_names_by_task: Mapping[str, tuple[str, ...]]
    scan_units_by_evidence_name: Mapping[str, ScanUnit]
    lineage_by_view: Mapping[str, LineageReport]
    session_runtime: SessionRuntime | None


@dataclass(frozen=True)
class _ScanPlanning:
    scan_units: tuple[ScanUnit, ...]
    scan_keys_by_task: Mapping[str, tuple[str, ...]]
    scan_task_name_by_key: Mapping[str, str]
    scan_task_units_by_name: Mapping[str, ScanUnit]
    scan_task_names_by_task: Mapping[str, tuple[str, ...]]


def plan_with_delta_pins(
    ctx: SessionContext,
    *,
    view_nodes: Sequence[ViewNode],
    runtime_profile: DataFusionRuntimeProfile | None,
    snapshot: Mapping[str, object] | None,
) -> PlanningPipelineResult:
    """Plan views, pin Delta inputs, and re-plan under pinned providers.

    Args:
        ctx: DataFusion session context.
        view_nodes: View nodes to plan.
        runtime_profile: Runtime profile for planning and pinning.
        snapshot: Optional semantic snapshot payload.

    Returns:
        PlanningPipelineResult: Result.

    Raises:
        ValueError: If runtime profile is not provided.
    """
    if runtime_profile is None:
        msg = "Runtime profile is required for planning with Delta pins."
        raise ValueError(msg)
    from semantics.compile_context import build_semantic_execution_context

    semantic_ctx = build_semantic_execution_context(
        runtime_profile=runtime_profile,
        ctx=ctx,
    )
    dataset_resolver = semantic_ctx.dataset_resolver
    session_runtime = runtime_profile.session_runtime()
    semantic_manifest = semantic_ctx.manifest
    facade = DataFusionExecutionFacade(ctx=ctx, runtime_profile=runtime_profile)
    # Baseline registration ensures UDF platform and registry views exist.
    facade.ensure_view_graph(semantic_manifest=semantic_manifest)
    baseline_nodes = _plan_view_nodes(
        ctx,
        view_nodes=view_nodes,
        session_runtime=session_runtime,
        scan_units=(),
    )
    snapshot = snapshot or (session_runtime.udf_snapshot if session_runtime is not None else None)
    baseline_inferred = infer_deps_from_view_nodes(
        baseline_nodes,
        ctx=ctx,
        snapshot=snapshot,
    )
    scan_planning = _scan_planning(
        ctx,
        runtime_profile=runtime_profile,
        inferred=baseline_inferred,
        dataset_resolver=dataset_resolver,
    )
    if scan_planning.scan_units:
        apply_scan_unit_overrides(
            ctx,
            scan_units=scan_planning.scan_units,
            runtime_profile=runtime_profile,
            dataset_resolver=dataset_resolver,
        )
        facade.ensure_view_graph(
            scan_units=scan_planning.scan_units,
            semantic_manifest=semantic_manifest,
            dataset_resolver=dataset_resolver,
        )
    pinned_nodes = _plan_view_nodes(
        ctx,
        view_nodes=view_nodes,
        session_runtime=session_runtime,
        scan_units=scan_planning.scan_units,
    )
    pinned_inferred = infer_deps_from_view_nodes(
        pinned_nodes,
        ctx=ctx,
        snapshot=snapshot,
    )
    lineage_by_view = _lineage_by_view(pinned_nodes)
    scan_inferred = _scan_inferred_deps(scan_planning.scan_task_units_by_name)
    inferred_all = (*pinned_inferred, *scan_inferred)
    return PlanningPipelineResult(
        view_nodes=pinned_nodes,
        inferred=tuple(inferred_all),
        scan_units=scan_planning.scan_units,
        scan_keys_by_task=scan_planning.scan_keys_by_task,
        scan_task_name_by_key=scan_planning.scan_task_name_by_key,
        scan_task_units_by_name=scan_planning.scan_task_units_by_name,
        scan_task_names_by_task=scan_planning.scan_task_names_by_task,
        scan_units_by_evidence_name=scan_planning.scan_task_units_by_name,
        lineage_by_view=lineage_by_view,
        session_runtime=session_runtime,
    )


def _scan_planning(
    ctx: SessionContext,
    *,
    runtime_profile: DataFusionRuntimeProfile,
    inferred: Sequence[InferredDeps],
    dataset_resolver: ManifestDatasetResolver | None = None,
) -> _ScanPlanning:
    scans_by_task = {dep.task_name: dep.scans for dep in inferred if dep.scans}
    if scans_by_task:
        if dataset_resolver is None:
            msg = "dataset_resolver is required for scan planning."
            raise ValueError(msg)
        dataset_locations = {
            name: loc
            for name in dataset_resolver.names()
            if (loc := dataset_resolver.location(name)) is not None
        }
        scan_units, scan_keys_by_task = plan_scan_units(
            ctx,
            dataset_locations=dataset_locations,
            scans_by_task=scans_by_task,
            runtime_profile=runtime_profile,
        )
    else:
        scan_units = ()
        scan_keys_by_task = dict[str, tuple[str, ...]]()
    scan_task_name_by_key = _scan_task_name_map(scan_units)
    scan_task_units_by_name = {
        scan_task_name_by_key[unit.key]: unit
        for unit in scan_units
        if unit.key in scan_task_name_by_key
    }
    scan_task_names_by_task = {
        task: tuple(scan_task_name_by_key[key] for key in keys if key in scan_task_name_by_key)
        for task, keys in scan_keys_by_task.items()
    }
    return _ScanPlanning(
        scan_units=scan_units,
        scan_keys_by_task=scan_keys_by_task,
        scan_task_name_by_key=scan_task_name_by_key,
        scan_task_units_by_name=scan_task_units_by_name,
        scan_task_names_by_task=scan_task_names_by_task,
    )


def _plan_view_nodes(
    ctx: SessionContext,
    *,
    view_nodes: Sequence[ViewNode],
    session_runtime: SessionRuntime | None,
    scan_units: Sequence[ScanUnit],
) -> tuple[ViewNode, ...]:
    planned: list[ViewNode] = []
    for node in view_nodes:
        df = node.builder(ctx)
        bundle = build_plan_bundle(
            ctx,
            df,
            options=PlanBundleOptions(
                compute_execution_plan=True,
                session_runtime=session_runtime,
                scan_units=scan_units,
            ),
        )
        runtime_profile = session_runtime.profile if session_runtime is not None else None
        record_plan_bundle_diagnostics(
            bundle=bundle,
            runtime_profile=runtime_profile,
            plan_kind="view",
            stage="planning",
            view_name=node.name,
        )
        planned.append(
            replace(
                node,
                plan_bundle=bundle,
                required_udfs=bundle.required_udfs,
            )
        )
    return tuple(planned)


def _scan_task_name_map(scan_units: Sequence[ScanUnit]) -> dict[str, str]:
    names: dict[str, str] = {}
    used: dict[str, str] = {}
    for unit in sorted(scan_units, key=lambda item: item.key):
        digest_len = _HASH_SLICE
        while True:
            digest = hash_sha256_hex(unit.key.encode("utf-8"), length=digest_len)
            name = f"{_SCAN_TASK_PREFIX}{digest}"
            existing_key = used.get(name)
            if existing_key is None or existing_key == unit.key:
                used[name] = unit.key
                names[unit.key] = name
                break
            digest_len += _HASH_SLICE
    return names


def _scan_inferred_deps(scan_units_by_name: Mapping[str, ScanUnit]) -> tuple[InferredDeps, ...]:
    inferred: list[InferredDeps] = []
    for task_name in sorted(scan_units_by_name):
        unit = scan_units_by_name[task_name]
        fingerprint = _scan_fingerprint(unit)
        inferred.append(
            InferredDeps(
                task_name=task_name,
                output=task_name,
                inputs=(),
                required_columns={},
                required_types={},
                required_metadata={},
                plan_fingerprint=fingerprint,
                required_udfs=(),
                required_rewrite_tags=(),
                scans=(),
            )
        )
    return tuple(inferred)


def _scan_fingerprint(unit: ScanUnit) -> str:
    payload = {
        "key": unit.key,
        "dataset_name": unit.dataset_name,
        "delta_version": unit.delta_version,
        "candidate_files": tuple(str(path) for path in unit.candidate_files),
        "projected_columns": unit.projected_columns,
        "pushed_filters": unit.pushed_filters,
    }
    return hash_msgpack_canonical(payload)


def _lineage_by_view(view_nodes: Sequence[ViewNode]) -> dict[str, LineageReport]:
    lineage: dict[str, LineageReport] = {}
    for node in view_nodes:
        bundle = node.plan_bundle
        if bundle is None or bundle.optimized_logical_plan is None:
            continue
        from datafusion_engine.lineage.datafusion import extract_lineage

        lineage[node.name] = extract_lineage(
            bundle.optimized_logical_plan,
            udf_snapshot=bundle.artifacts.udf_snapshot,
        )
    return lineage


__all__ = ["PlanningPipelineResult", "plan_with_delta_pins"]

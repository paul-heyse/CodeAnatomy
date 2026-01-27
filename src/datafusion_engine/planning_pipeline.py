"""Two-pass planning pipeline that pins Delta inputs before scheduling."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING

from datafusion import SessionContext

from datafusion_engine.dataset_registry import DatasetLocation
from datafusion_engine.lineage_datafusion import LineageReport
from datafusion_engine.plan_bundle import build_plan_bundle
from datafusion_engine.scan_overrides import apply_scan_unit_overrides
from datafusion_engine.scan_planner import ScanUnit, plan_scan_units
from datafusion_engine.view_registry import ensure_view_graph
from relspec.inferred_deps import InferredDeps, infer_deps_from_view_nodes
from serde_msgspec import dumps_msgpack

if TYPE_CHECKING:
    from datafusion_engine.runtime import DataFusionRuntimeProfile, SessionRuntime
    from datafusion_engine.view_graph_registry import ViewNode

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

    Returns
    -------
    PlanningPipelineResult
        Planning outputs with scan units promoted to schedulable tasks.

    Raises
    ------
    ValueError
        Raised when the runtime profile is unavailable.
    """
    if runtime_profile is None:
        msg = "Runtime profile is required for planning with Delta pins."
        raise ValueError(msg)
    session_runtime = runtime_profile.session_runtime()
    # Baseline registration ensures UDF platform and registry views exist.
    ensure_view_graph(ctx, runtime_profile=runtime_profile, include_registry_views=True)
    baseline_nodes = _plan_view_nodes(
        ctx,
        view_nodes=view_nodes,
        session_runtime=session_runtime,
        scan_units=(),
    )
    snapshot = snapshot or (session_runtime.udf_snapshot if session_runtime is not None else None)
    baseline_inferred = infer_deps_from_view_nodes(baseline_nodes, snapshot=snapshot)
    scan_planning = _scan_planning(
        ctx,
        runtime_profile=runtime_profile,
        inferred=baseline_inferred,
    )
    if scan_planning.scan_units:
        apply_scan_unit_overrides(
            ctx,
            scan_units=scan_planning.scan_units,
            runtime_profile=runtime_profile,
        )
        ensure_view_graph(
            ctx,
            runtime_profile=runtime_profile,
            include_registry_views=True,
            scan_units=scan_planning.scan_units,
        )
    pinned_nodes = _plan_view_nodes(
        ctx,
        view_nodes=view_nodes,
        session_runtime=session_runtime,
        scan_units=scan_planning.scan_units,
    )
    pinned_inferred = infer_deps_from_view_nodes(pinned_nodes, snapshot=snapshot)
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
) -> _ScanPlanning:
    scans_by_task = {dep.task_name: dep.scans for dep in inferred if dep.scans}
    if scans_by_task:
        scan_units, scan_keys_by_task = plan_scan_units(
            ctx,
            dataset_locations=_dataset_location_map(runtime_profile),
            scans_by_task=scans_by_task,
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
        task: tuple(
            scan_task_name_by_key[key]
            for key in keys
            if key in scan_task_name_by_key
        )
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
            compute_execution_plan=False,
            session_runtime=session_runtime,
            scan_units=scan_units,
        )
        planned.append(
            replace(
                node,
                plan_bundle=bundle,
                required_udfs=bundle.required_udfs,
            )
        )
    return tuple(planned)


def _dataset_location_map(profile: DataFusionRuntimeProfile) -> dict[str, DatasetLocation]:
    locations: dict[str, DatasetLocation] = {}
    for name, location in profile.extract_dataset_locations.items():
        locations.setdefault(name, location)
    for name, location in profile.scip_dataset_locations.items():
        locations.setdefault(name, location)
    for catalog in profile.registry_catalogs.values():
        for name in catalog.names():
            if name in locations:
                continue
            try:
                locations[name] = catalog.get(name)
            except KeyError:
                continue
    return locations


def _scan_task_name_map(scan_units: Sequence[ScanUnit]) -> dict[str, str]:
    names: dict[str, str] = {}
    used: dict[str, str] = {}
    for unit in sorted(scan_units, key=lambda item: item.key):
        digest_len = _HASH_SLICE
        while True:
            digest = hashlib.sha256(unit.key.encode("utf-8")).hexdigest()[:digest_len]
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
    return hashlib.sha256(dumps_msgpack(payload)).hexdigest()


def _lineage_by_view(view_nodes: Sequence[ViewNode]) -> dict[str, LineageReport]:
    lineage: dict[str, LineageReport] = {}
    for node in view_nodes:
        bundle = node.plan_bundle
        if bundle is None or bundle.optimized_logical_plan is None:
            continue
        from datafusion_engine.lineage_datafusion import extract_lineage

        lineage[node.name] = extract_lineage(
            bundle.optimized_logical_plan,
            udf_snapshot=bundle.artifacts.udf_snapshot,
        )
    return lineage


__all__ = ["PlanningPipelineResult", "plan_with_delta_pins"]

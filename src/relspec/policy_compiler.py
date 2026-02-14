"""Compile execution policy from plan artifacts and graph topology.

The compiler inspects the ``TaskGraph`` (rustworkx), output locations,
and runtime profile to produce a ``CompiledExecutionPolicy`` that
captures every policy decision made during plan compilation.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol

from relspec.compiled_policy import CompiledExecutionPolicy
from relspec.contracts import CompileExecutionPolicyRequestV1
from serde_msgspec import to_builtins
from utils.hashing import hash_json_canonical

if TYPE_CHECKING:
    from datafusion_engine.dataset.registry import DatasetLocation
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile
    from datafusion_engine.views.graph import ViewNode
    from relspec.pipeline_policy import DiagnosticsPolicy
    from semantics.ir import SemanticIR


class _OutDegreeGraph(Protocol):
    def out_degree(self, node_idx: object) -> int: ...


class _TaskGraphLike(Protocol):
    task_idx: Mapping[str, object]
    graph: _OutDegreeGraph


class _ScanOverrideLike(Protocol):
    dataset_name: str
    policy: object
    reasons: object
    inference_confidence: object | None


_LOGGER = logging.getLogger(__name__)

# Fan-out threshold above which an intermediate node is promoted to
# ``delta_staging`` regardless of other signals.
_HIGH_FANOUT_THRESHOLD = 2


@dataclass(frozen=True)
class _CompiledPolicyComponents:
    cache_policies: dict[str, str]
    scan_policy_map: dict[str, object]
    maintenance_policy_map: dict[str, object]
    udf_requirements: dict[str, tuple[str, ...]]
    join_strategies: dict[str, str]
    inference_confidence: dict[str, object]
    materialization_strategy: str | None
    diagnostics_flags: dict[str, bool]
    workload_class: str | None


def compile_execution_policy(request: CompileExecutionPolicyRequestV1) -> CompiledExecutionPolicy:
    """Compile all execution policy from plan artifacts.

    Parameters
    ----------
    task_graph
        Rustworkx task graph with topology information.
    output_locations
        Mapping of dataset names to their resolved output locations.
    runtime_profile
        DataFusion runtime profile for the current session.
    view_nodes
        Planned view nodes including bundle-derived UDF requirements.
    semantic_ir
        Semantic IR carrying inferred join strategies and confidence.
    scan_overrides
        Per-dataset scan policy overrides derived from plan signals.
    diagnostics_policy
        Pipeline diagnostics capture policy, if available.
    workload_class
        Optional workload class used to specialize policy compilation.

    Returns:
    -------
    CompiledExecutionPolicy
        Frozen policy artifact ready for downstream consumption.
    """
    components = _derive_policy_components(request)
    preliminary = CompiledExecutionPolicy(
        cache_policy_by_view=components.cache_policies,
        scan_policy_overrides=components.scan_policy_map,
        maintenance_policy_by_dataset=components.maintenance_policy_map,
        udf_requirements_by_view=components.udf_requirements,
        join_strategy_by_view=components.join_strategies,
        inference_confidence_by_view=components.inference_confidence,
        materialization_strategy=components.materialization_strategy,
        diagnostics_flags=components.diagnostics_flags,
        workload_class=components.workload_class,
    )
    fingerprint = _compute_policy_fingerprint(preliminary)

    return CompiledExecutionPolicy(
        cache_policy_by_view=components.cache_policies,
        scan_policy_overrides=components.scan_policy_map,
        maintenance_policy_by_dataset=components.maintenance_policy_map,
        udf_requirements_by_view=components.udf_requirements,
        join_strategy_by_view=components.join_strategies,
        inference_confidence_by_view=components.inference_confidence,
        materialization_strategy=components.materialization_strategy,
        diagnostics_flags=components.diagnostics_flags,
        workload_class=components.workload_class,
        policy_fingerprint=fingerprint,
    )


def _derive_policy_components(
    request: CompileExecutionPolicyRequestV1,
) -> _CompiledPolicyComponents:
    cache_overrides = _merge_cache_overrides(
        _cache_overrides_from_semantic_ir(request.semantic_ir),
        _cache_overrides_from_view_nodes(request.view_nodes),
    )
    return _CompiledPolicyComponents(
        cache_policies=_derive_cache_policies(
            request.task_graph,
            request.output_locations,
            cache_overrides=cache_overrides,
            workload_class=request.workload_class,
        ),
        scan_policy_map=_scan_overrides_to_mapping(request.scan_overrides),
        maintenance_policy_map=_derive_maintenance_policies(request.output_locations),
        udf_requirements=_derive_udf_requirements(
            task_graph=request.task_graph,
            view_nodes=request.view_nodes,
        ),
        join_strategies=_derive_join_strategies_from_semantic_ir(request.semantic_ir),
        inference_confidence=_derive_inference_confidence_from_semantic_ir(request.semantic_ir),
        materialization_strategy=_derive_materialization_strategy(
            output_locations=request.output_locations,
            runtime_profile=request.runtime_profile,
            workload_class=request.workload_class,
        ),
        diagnostics_flags=_diagnostics_flags_from_policy(request.diagnostics_policy),
        workload_class=request.workload_class,
    )


def _derive_cache_policies(
    task_graph: _TaskGraphLike,
    output_locations: Mapping[str, object],
    *,
    cache_overrides: Mapping[str, str] | None = None,
    workload_class: str | None = None,
) -> dict[str, str]:
    """Derive cache policies from TaskGraph topology.

    Parameters
    ----------
    task_graph
        Rustworkx task graph with node indices and connectivity.
    output_locations
        Known output dataset locations (terminal sinks).

    Returns:
    -------
    dict[str, str]
        Mapping of task names to cache policy literals.
    """
    policies: dict[str, str] = {}
    normalized_workload = _normalize_workload_class(workload_class)
    for task_name, node_idx in task_graph.task_idx.items():
        out_degree = task_graph.graph.out_degree(node_idx)
        is_output = task_name in output_locations

        # Terminal node that maps to a declared output location.
        if is_output:
            policy_value = "delta_output"
        elif out_degree > _HIGH_FANOUT_THRESHOLD:
            # High fan-out intermediate: staging avoids recomputation.
            policy_value = "delta_staging"
        elif out_degree == 0:
            # Leaf node with no declared output: no caching needed.
            policy_value = "none"
        else:
            # Default for intermediate nodes with low fan-out.
            policy_value = "delta_staging" if out_degree > 0 else "none"
        policy_value = _workload_adjusted_cache_policy(
            base_policy=policy_value,
            out_degree=out_degree,
            is_output=is_output,
            workload_class=normalized_workload,
        )
        if cache_overrides is not None:
            override = _normalize_cache_policy_value(cache_overrides.get(task_name))
            if override is not None:
                policy_value = override
        policies[task_name] = policy_value

    return policies


def _cache_overrides_from_view_nodes(
    view_nodes: Sequence[ViewNode] | None,
) -> dict[str, str]:
    """Extract cache-policy overrides from view nodes.

    Returns:
    -------
    dict[str, str]
        Per-view cache policy overrides keyed by view name.
    """
    if view_nodes is None:
        return {}
    overrides: dict[str, str] = {}
    for node in view_nodes:
        policy_value = _normalize_cache_policy_value(getattr(node, "cache_policy", None))
        if policy_value is None:
            continue
        overrides[node.name] = policy_value
    return overrides


def _cache_overrides_from_semantic_ir(
    semantic_ir: SemanticIR | None,
) -> dict[str, str]:
    """Extract cache-policy overrides from inferred Semantic IR hints.

    Returns:
    -------
    dict[str, str]
        Per-view cache policy overrides inferred from IR cache hints.
    """
    if semantic_ir is None:
        return {}
    from semantics.ir import ir_cache_hint_to_execution_policy

    overrides: dict[str, str] = {}
    for view in semantic_ir.views:
        inferred = view.inferred_properties
        if inferred is None:
            continue
        policy_value = _normalize_cache_policy_value(
            ir_cache_hint_to_execution_policy(inferred.inferred_cache_policy)
        )
        if policy_value is None:
            continue
        overrides[view.name] = policy_value
    return overrides


def _merge_cache_overrides(*sources: Mapping[str, str]) -> dict[str, str]:
    """Merge cache override mappings with later mappings taking precedence.

    Returns:
    -------
    dict[str, str]
        Merged cache override mapping.
    """
    merged: dict[str, str] = {}
    for source in sources:
        merged.update(source)
    return merged


def _normalize_workload_class(workload_class: str | None) -> str | None:
    if workload_class is None:
        return None
    normalized = workload_class.strip().lower()
    return normalized or None


def _normalize_cache_policy_value(value: object) -> str | None:
    if value not in {"none", "delta_staging", "delta_output"}:
        return None
    return str(value)


def _workload_adjusted_cache_policy(
    *,
    base_policy: str,
    out_degree: int,
    is_output: bool,
    workload_class: str | None,
) -> str:
    if is_output or workload_class is None:
        return base_policy
    # Interactive and replay workloads avoid staging for low fan-out
    # intermediates to reduce write amplification.
    if workload_class == "interactive_query" and base_policy == "delta_staging" and out_degree <= 1:
        return "none"
    if workload_class == "compile_replay" and base_policy == "delta_staging":
        return "none"
    return base_policy


def _scan_overrides_to_mapping(
    overrides: tuple[_ScanOverrideLike, ...],
) -> dict[str, object]:
    """Convert scan policy overrides to a serializable mapping.

    Parameters
    ----------
    overrides
        Scan policy overrides from plan signal derivation.

    Returns:
    -------
    dict[str, object]
        Mapping of dataset names to override configuration dicts.
    """
    result: dict[str, object] = {}
    for override in overrides:
        entry: dict[str, object] = {
            "policy": to_builtins(override.policy),
            "reasons": override.reasons,
        }
        if override.inference_confidence is not None:
            entry["inference_confidence"] = to_builtins(override.inference_confidence)
        result[override.dataset_name] = entry
    return result


def _derive_udf_requirements(
    *,
    task_graph: _TaskGraphLike,
    view_nodes: Sequence[ViewNode] | None = None,
) -> dict[str, tuple[str, ...]]:
    """Extract per-task UDF requirements from task graph node payloads.

    Parameters
    ----------
    task_graph
        Task graph with node payloads.

    Returns:
    -------
    dict[str, tuple[str, ...]]
        Mapping of task names to sorted tuples of required UDF names.
    """
    _ = task_graph
    if view_nodes is None:
        return {}

    requirements: dict[str, tuple[str, ...]] = {}
    for node in sorted(view_nodes, key=lambda item: item.name):
        required_udfs = tuple(
            sorted(
                {
                    udf_name
                    for udf_name in (node.required_udfs or ())
                    if isinstance(udf_name, str) and udf_name
                }
            )
        )
        if required_udfs:
            requirements[node.name] = required_udfs
    return requirements


def _derive_join_strategies_from_semantic_ir(
    semantic_ir: SemanticIR | None,
) -> dict[str, str]:
    """Extract inferred join-strategy decisions from semantic IR.

    Returns:
    -------
    dict[str, str]
        Join strategy by view name for inferred join views.
    """
    if semantic_ir is None:
        return {}
    strategies: dict[str, str] = {}
    for view in semantic_ir.views:
        inferred = view.inferred_properties
        if inferred is None or inferred.inferred_join_strategy is None:
            continue
        strategies[view.name] = inferred.inferred_join_strategy
    return strategies


def _derive_inference_confidence_from_semantic_ir(
    semantic_ir: SemanticIR | None,
) -> dict[str, object]:
    """Extract structured inference confidence from semantic IR views.

    Returns:
    -------
    dict[str, object]
        Serialized inference confidence payload by view name.
    """
    if semantic_ir is None:
        return {}
    confidence_by_view: dict[str, object] = {}
    for view in semantic_ir.views:
        inferred = view.inferred_properties
        if inferred is None or inferred.inference_confidence is None:
            continue
        confidence_by_view[view.name] = to_builtins(
            inferred.inference_confidence,
            str_keys=True,
        )
    return confidence_by_view


def _derive_maintenance_policies(
    output_locations: Mapping[str, DatasetLocation],
) -> dict[str, object]:
    """Extract per-dataset maintenance policy from resolved output locations.

    Parameters
    ----------
    output_locations
        Mapping of dataset names to output locations.

    Returns:
    -------
    dict[str, object]
        Mapping of dataset names to serialized maintenance policy payloads.
    """
    policies: dict[str, object] = {}
    for dataset_name, location in sorted(output_locations.items()):
        resolved = getattr(location, "resolved", None)
        if resolved is None:
            continue
        maintenance_policy = getattr(resolved, "delta_maintenance_policy", None)
        if maintenance_policy is None:
            continue
        policies[dataset_name] = to_builtins(maintenance_policy, str_keys=True)
    return policies


def _derive_materialization_strategy(
    *,
    output_locations: Mapping[str, DatasetLocation],
    runtime_profile: DataFusionRuntimeProfile,
    workload_class: str | None = None,
) -> str | None:
    """Derive a high-level materialization strategy identifier.

    Parameters
    ----------
    output_locations
        Mapping of configured semantic outputs.
    runtime_profile
        Active runtime profile.

    Returns:
    -------
    str | None
        Strategy identifier, or ``None`` when no materialized outputs exist.
    """
    if not output_locations:
        return None
    normalized_workload = _normalize_workload_class(workload_class)
    base_strategy: str
    if getattr(getattr(runtime_profile, "policies", None), "write_policy", None) is not None:
        base_strategy = "delta_write_policy"
    elif bool(getattr(getattr(runtime_profile, "features", None), "enable_delta_cdf", False)):
        base_strategy = "delta_cdf_outputs"
    else:
        base_strategy = "delta_outputs"
    if normalized_workload is None:
        return base_strategy
    return f"{base_strategy}:{normalized_workload}"


def _diagnostics_flags_from_policy(
    policy: DiagnosticsPolicy | None,
) -> dict[str, bool]:
    """Extract boolean flags from the diagnostics policy.

    Parameters
    ----------
    policy
        Pipeline diagnostics policy, or ``None`` for defaults.

    Returns:
    -------
    dict[str, bool]
        Boolean diagnostic capture flags.
    """
    if policy is None:
        return {}
    return {
        "capture_datafusion_metrics": policy.capture_datafusion_metrics,
        "capture_datafusion_traces": policy.capture_datafusion_traces,
        "capture_datafusion_explains": policy.capture_datafusion_explains,
        "explain_analyze": policy.explain_analyze,
        "emit_kernel_lane_diagnostics": policy.emit_kernel_lane_diagnostics,
        "emit_semantic_quality_diagnostics": policy.emit_semantic_quality_diagnostics,
    }


def _compute_policy_fingerprint(policy: CompiledExecutionPolicy) -> str:
    """Compute a deterministic fingerprint of the compiled policy.

    Parameters
    ----------
    policy
        Compiled policy (without fingerprint populated).

    Returns:
    -------
    str
        SHA-256 hex digest of the canonical JSON encoding.
    """
    payload = to_builtins(policy)
    return hash_json_canonical(payload, str_keys=True)


__all__ = [
    "compile_execution_policy",
]

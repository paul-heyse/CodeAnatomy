"""Compile execution policy from plan artifacts and graph topology.

The compiler inspects the ``TaskGraph`` (rustworkx), output locations,
and runtime profile to produce a ``CompiledExecutionPolicy`` that
captures every policy decision made during plan compilation.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING

from relspec.compiled_policy import (
    CompiledExecutionPolicy,
    CompiledInferenceConfidence,
    CompiledMaintenancePolicy,
    CompiledScanPolicyOverride,
    JsonValue,
)
from relspec.contracts import CompileExecutionPolicyRequestV1, ScanOverrideLike, TaskGraphLike
from relspec.ports import RuntimeProfilePort
from serde_msgspec import to_builtins
from utils.hashing import hash_json_canonical

if TYPE_CHECKING:
    from datafusion_engine.dataset.registry import DatasetLocation
    from datafusion_engine.views.graph import ViewNode
    from relspec.pipeline_policy import DiagnosticsPolicy
    from semantics.ir import SemanticIR

_LOGGER = logging.getLogger(__name__)


def _to_json_scalar(value: object) -> str | int | float | bool | None:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


def _to_json_value(value: object) -> JsonValue:
    """Normalize arbitrary payloads to JSON-compatible typed values.

    Returns:
    -------
    JsonValue
        JSON-compatible value representation.
    """
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Mapping):
        return {str(key): _to_json_scalar(item) for key, item in value.items()}
    if isinstance(value, Sequence) and not isinstance(value, str):
        return tuple(_to_json_scalar(item) for item in value)
    return str(value)


def _to_json_mapping(value: object) -> dict[str, JsonValue]:
    if not isinstance(value, Mapping):
        return {}
    return {str(key): _to_json_value(item) for key, item in value.items()}


def _normalize_reason_labels(value: object) -> tuple[str, ...]:
    if isinstance(value, str):
        normalized = value.strip()
        return (normalized,) if normalized else ()
    if isinstance(value, Sequence):
        labels = tuple(str(item).strip() for item in value if str(item).strip())
        return tuple(dict.fromkeys(labels))
    return ()


@dataclass(frozen=True)
class _CompiledPolicyComponents:
    cache_policies: dict[str, str]
    scan_policy_map: dict[str, CompiledScanPolicyOverride]
    maintenance_policy_map: dict[str, CompiledMaintenancePolicy]
    udf_requirements: dict[str, tuple[str, ...]]
    join_strategies: dict[str, str]
    inference_confidence: dict[str, CompiledInferenceConfidence]
    materialization_strategy: str | None
    diagnostics_flags: dict[str, bool]
    workload_class: str | None


def _component_counts(components: _CompiledPolicyComponents) -> dict[str, int | str | None]:
    return {
        "cache_policies": len(components.cache_policies),
        "scan_policy_overrides": len(components.scan_policy_map),
        "maintenance_policy_overrides": len(components.maintenance_policy_map),
        "udf_requirement_sets": len(components.udf_requirements),
        "join_strategies": len(components.join_strategies),
        "inference_confidence_entries": len(components.inference_confidence),
        "diagnostics_flags": len(components.diagnostics_flags),
        "materialization_strategy": components.materialization_strategy,
        "workload_class": components.workload_class,
    }


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
    _LOGGER.debug(
        "Derived execution-policy component sets.",
        extra={
            "codeanatomy.policy_components": _component_counts(components),
        },
    )
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
    _LOGGER.debug(
        "Compiled execution policy fingerprint.",
        extra={
            "codeanatomy.policy_fingerprint": fingerprint,
            "codeanatomy.policy_components": _component_counts(components),
        },
    )

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
    task_graph: TaskGraphLike,
    output_locations: Mapping[str, DatasetLocation],
    *,
    cache_overrides: Mapping[str, str] | None = None,
    workload_class: str | None = None,
) -> dict[str, str]:
    """Derive cache policies from the Rust scheduling bridge.

    Design-phase hard cutover: Python graph traversal fallback is removed.

    Returns:
    -------
    dict[str, str]
        Cache-policy mapping keyed by task/view name.

    Raises:
    ------
    RuntimeError:
        If the bridge payload is unavailable or invalid.
    """
    bridged = _derive_cache_policies_rust(
        task_graph=task_graph,
        output_locations=output_locations,
        cache_overrides=cache_overrides,
        workload_class=workload_class,
    )
    if bridged is None:
        msg = (
            "derive_cache_policies bridge returned no policy payload. "
            "Rust scheduling bridge entrypoint is required."
        )
        raise RuntimeError(msg)
    return bridged


def derive_cache_policies_from_graph(
    task_graph: TaskGraphLike,
    output_locations: Mapping[str, DatasetLocation],
    *,
    cache_overrides: Mapping[str, str] | None = None,
    workload_class: str | None = None,
) -> dict[str, str]:
    """Public bridge facade for cache-policy derivation.

    Returns:
    -------
    dict[str, str]
        Cache-policy mapping keyed by task/view name.
    """
    return _derive_cache_policies(
        task_graph,
        output_locations,
        cache_overrides=cache_overrides,
        workload_class=workload_class,
    )


def _derive_cache_policies_rust(
    *,
    task_graph: TaskGraphLike,
    output_locations: Mapping[str, DatasetLocation],
    cache_overrides: Mapping[str, str] | None,
    workload_class: str | None,
) -> dict[str, str] | None:
    from datafusion_engine.extensions import datafusion_ext

    derive = getattr(datafusion_ext, "derive_cache_policies", None)
    if not callable(derive):
        return None
    graph_payload = {
        "task_names": sorted(str(name) for name in task_graph.task_idx),
        "out_degree": {
            str(task_name): int(task_graph.graph.out_degree(node_idx))
            for task_name, node_idx in task_graph.task_idx.items()
        },
    }
    request_payload = {
        "graph": graph_payload,
        "outputs": sorted(str(name) for name in output_locations),
        "cache_overrides": {str(key): str(value) for key, value in (cache_overrides or {}).items()},
        "workload_class": workload_class,
    }
    try:
        payload = derive(request_payload)
    except (RuntimeError, TypeError, ValueError):
        return None
    if isinstance(payload, Mapping):
        return {str(key): str(value) for key, value in payload.items()}
    if not isinstance(payload, Sequence) or isinstance(payload, (str, bytes, bytearray)):
        return None
    policies: dict[str, str] = {}
    for item in payload:
        if not isinstance(item, Mapping):
            continue
        view_name = str(item.get("view_name") or "").strip()
        policy = _normalize_cache_policy_value(item.get("policy"))
        if view_name and policy is not None:
            policies[view_name] = policy
    return policies or None


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
        policy_value = _normalize_cache_policy_value(node.cache_policy)
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


def _scan_overrides_to_mapping(
    overrides: tuple[ScanOverrideLike, ...],
) -> dict[str, CompiledScanPolicyOverride]:
    """Convert scan policy overrides to a serializable mapping.

    Parameters
    ----------
    overrides
        Scan policy overrides from plan signal derivation.

    Returns:
    -------
    dict[str, CompiledScanPolicyOverride]
        Mapping of dataset names to override configuration dicts.
    """
    result: dict[str, CompiledScanPolicyOverride] = {}
    for override in overrides:
        raw_policy = to_builtins(override.policy, str_keys=True)
        policy_payload = _to_json_mapping(raw_policy)
        raw_conf = (
            to_builtins(override.inference_confidence, str_keys=True)
            if override.inference_confidence is not None
            else None
        )
        conf_payload = _to_json_mapping(raw_conf) if raw_conf is not None else None
        result[override.dataset_name] = CompiledScanPolicyOverride(
            policy=policy_payload,
            reasons=_normalize_reason_labels(override.reasons),
            inference_confidence=conf_payload,
        )
    return result


def _derive_udf_requirements(
    *,
    task_graph: TaskGraphLike,
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
) -> dict[str, CompiledInferenceConfidence]:
    """Extract structured inference confidence from semantic IR views.

    Returns:
    -------
    dict[str, CompiledInferenceConfidence]
        Serialized inference confidence payload by view name.
    """
    if semantic_ir is None:
        return {}
    confidence_by_view: dict[str, CompiledInferenceConfidence] = {}
    for view in semantic_ir.views:
        inferred = view.inferred_properties
        if inferred is None or inferred.inference_confidence is None:
            continue
        payload = to_builtins(
            inferred.inference_confidence,
            str_keys=True,
        )
        normalized = _to_json_mapping(payload)
        if normalized:
            confidence_by_view[view.name] = CompiledInferenceConfidence(payload=normalized)
    return confidence_by_view


def _derive_maintenance_policies(
    output_locations: Mapping[str, DatasetLocation],
) -> dict[str, CompiledMaintenancePolicy]:
    """Extract per-dataset maintenance policy from resolved output locations.

    Parameters
    ----------
    output_locations
        Mapping of dataset names to output locations.

    Returns:
    -------
    dict[str, CompiledMaintenancePolicy]
        Mapping of dataset names to serialized maintenance policy payloads.
    """
    policies: dict[str, CompiledMaintenancePolicy] = {}
    for dataset_name, location in sorted(output_locations.items()):
        maintenance_policy = location.delta_maintenance_policy
        if maintenance_policy is None:
            continue
        payload = to_builtins(maintenance_policy, str_keys=True)
        normalized = _to_json_mapping(payload)
        if normalized:
            policies[dataset_name] = CompiledMaintenancePolicy(payload=normalized)
    return policies


def _derive_materialization_strategy(
    *,
    output_locations: Mapping[str, DatasetLocation],
    runtime_profile: RuntimeProfilePort,
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
    if runtime_profile.policies.write_policy is not None:
        base_strategy = "delta_write_policy"
    elif runtime_profile.features.enable_delta_cdf:
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
    "derive_cache_policies_from_graph",
]

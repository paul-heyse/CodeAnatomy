"""Compile execution policy from plan artifacts and graph topology.

The compiler inspects the ``TaskGraph`` (rustworkx), output locations,
and runtime profile to produce a ``CompiledExecutionPolicy`` that
captures every policy decision made during plan compilation.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import TYPE_CHECKING

from relspec.compiled_policy import CompiledExecutionPolicy
from serde_msgspec import to_builtins
from utils.hashing import hash_json_canonical

if TYPE_CHECKING:
    from datafusion_engine.dataset.registry import DatasetLocation
    from datafusion_engine.delta.scan_policy_inference import ScanPolicyOverride
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile
    from relspec.pipeline_policy import DiagnosticsPolicy
    from relspec.rustworkx_graph import TaskGraph

_LOGGER = logging.getLogger(__name__)

# Fan-out threshold above which an intermediate node is promoted to
# ``delta_staging`` regardless of other signals.
_HIGH_FANOUT_THRESHOLD = 2


def compile_execution_policy(
    *,
    task_graph: TaskGraph,
    output_locations: Mapping[str, DatasetLocation],
    runtime_profile: DataFusionRuntimeProfile,
    scan_overrides: tuple[ScanPolicyOverride, ...] = (),
    diagnostics_policy: DiagnosticsPolicy | None = None,
) -> CompiledExecutionPolicy:
    """Compile all execution policy from plan artifacts.

    Parameters
    ----------
    task_graph
        Rustworkx task graph with topology information.
    output_locations
        Mapping of dataset names to their resolved output locations.
    runtime_profile
        DataFusion runtime profile for the current session.
    scan_overrides
        Per-dataset scan policy overrides derived from plan signals.
    diagnostics_policy
        Pipeline diagnostics capture policy, if available.

    Returns:
    -------
    CompiledExecutionPolicy
        Frozen policy artifact ready for downstream consumption.
    """
    _ = runtime_profile
    cache_policies = _derive_cache_policies(task_graph, output_locations)
    scan_policy_map = _scan_overrides_to_mapping(scan_overrides)
    udf_reqs = _derive_udf_requirements(task_graph)
    diag_flags = _diagnostics_flags_from_policy(diagnostics_policy)

    # Build the policy without fingerprint first, then compute it.
    preliminary = CompiledExecutionPolicy(
        cache_policy_by_view=cache_policies,
        scan_policy_overrides=scan_policy_map,
        udf_requirements_by_view=udf_reqs,
        diagnostics_flags=diag_flags,
    )
    fingerprint = _compute_policy_fingerprint(preliminary)

    return CompiledExecutionPolicy(
        cache_policy_by_view=cache_policies,
        scan_policy_overrides=scan_policy_map,
        udf_requirements_by_view=udf_reqs,
        diagnostics_flags=diag_flags,
        policy_fingerprint=fingerprint,
    )


def _derive_cache_policies(
    task_graph: TaskGraph,
    output_locations: Mapping[str, object],
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
    for task_name, node_idx in task_graph.task_idx.items():
        out_degree = task_graph.graph.out_degree(node_idx)

        # Terminal node that maps to a declared output location.
        if task_name in output_locations:
            policies[task_name] = "delta_output"
        elif out_degree > _HIGH_FANOUT_THRESHOLD:
            # High fan-out intermediate: staging avoids recomputation.
            policies[task_name] = "delta_staging"
        elif out_degree == 0:
            # Leaf node with no declared output: no caching needed.
            policies[task_name] = "none"
        else:
            # Default for intermediate nodes with low fan-out.
            policies[task_name] = "delta_staging" if out_degree > 0 else "none"

    return policies


def _scan_overrides_to_mapping(
    overrides: tuple[ScanPolicyOverride, ...],
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


def _derive_udf_requirements(task_graph: TaskGraph) -> dict[str, tuple[str, ...]]:
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
    # UDF requirements are not directly on TaskNode today; this is a
    # placeholder that returns an empty map.  When UDF metadata is added
    # to TaskNode or extracted from plan bundles, this function will be
    # expanded.
    return {}


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

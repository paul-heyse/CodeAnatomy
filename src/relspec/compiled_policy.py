"""Compile-time-resolved execution policy artifact.

Runtime consumes this artifact unchanged; it does not re-derive
policy heuristically.  The ``CompiledExecutionPolicy`` captures all
policy decisions made during plan compilation so that downstream
execution is deterministic and audit-friendly.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Literal

from serde_msgspec import StructBaseStrict

# CachePolicy is Literal["none", "delta_staging", "delta_output"] defined at
# datafusion_engine/views/artifacts.py:25.  We use plain ``str`` in the
# serialized struct to avoid pulling heavy DataFusion imports into this
# lightweight contract module.
CachePolicyValue = Literal["none", "delta_staging", "delta_output"]
ValidationMode = Literal["off", "warn", "error"]


class CompiledExecutionPolicy(StructBaseStrict, frozen=True):
    """Compile-time-resolved execution policy artifact.

    Each section captures a policy decision that was previously derived
    at runtime via naming conventions or other heuristics.  The compiled
    policy is produced once during plan compilation and consumed
    unchanged by the execution layer.

    Attributes:
    ----------
    cache_policy_by_view
        Mapping of view names to cache policy literals.  Derived from
        task-graph topology (fan-out, terminal status, output locations).
    scan_policy_overrides
        Per-dataset scan policy override configurations derived from
        plan signals.
    maintenance_policy_by_dataset
        Per-dataset maintenance policy configuration.
    udf_requirements_by_view
        Per-view UDF requirement names extracted from plan expressions.
    join_strategy_by_view
        Per-view inferred join strategy names from semantic inference.
    inference_confidence_by_view
        Structured inference-confidence payloads keyed by view name.
    materialization_strategy
        High-level materialization strategy identifier, if resolved.
    diagnostics_flags
        Boolean diagnostic capture flags propagated from pipeline policy.
    workload_class
        Workload class used when compiling this policy (for example
        ``interactive_query`` or ``batch_ingest``).
    validation_mode
        How policy violations are handled at runtime.
    policy_fingerprint
        Deterministic fingerprint of the entire compiled policy for
        reproducibility and cache-key computation.
    """

    cache_policy_by_view: Mapping[str, str] = {}
    scan_policy_overrides: Mapping[str, object] = {}
    maintenance_policy_by_dataset: Mapping[str, object] = {}
    udf_requirements_by_view: Mapping[str, tuple[str, ...]] = {}
    join_strategy_by_view: Mapping[str, str] = {}
    inference_confidence_by_view: Mapping[str, object] = {}
    materialization_strategy: str | None = None
    diagnostics_flags: Mapping[str, bool] = {}
    workload_class: str | None = None
    validation_mode: str = "warn"
    policy_fingerprint: str | None = None


__all__ = [
    "CachePolicyValue",
    "CompiledExecutionPolicy",
    "ValidationMode",
]

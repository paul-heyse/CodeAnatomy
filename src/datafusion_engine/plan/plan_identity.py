"""Plan-identity payload helpers extracted from bundle_artifact."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING

from serde_msgspec import to_builtins

if TYPE_CHECKING:
    from datafusion_engine.lineage.scheduling import ScanUnit
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile
    from serde_artifacts import DeltaInputPin, PlanArtifacts

PLAN_IDENTITY_PAYLOAD_VERSION = 4


def _delta_inputs_payload(
    delta_inputs: Sequence[DeltaInputPin],
) -> tuple[dict[str, object], ...]:
    payloads: list[dict[str, object]] = [
        {
            "dataset_name": pin.dataset_name,
            "version": pin.version,
            "timestamp": pin.timestamp,
            "protocol": (
                to_builtins(pin.protocol, str_keys=True) if pin.protocol is not None else None
            ),
            "delta_scan_config": (
                to_builtins(pin.delta_scan_config) if pin.delta_scan_config is not None else None
            ),
            "delta_scan_config_hash": pin.delta_scan_config_hash,
            "datafusion_provider": pin.datafusion_provider,
            "protocol_compatible": pin.protocol_compatible,
            "protocol_compatibility": (
                to_builtins(pin.protocol_compatibility)
                if pin.protocol_compatibility is not None
                else None
            ),
        }
        for pin in delta_inputs
    ]
    payloads.sort(key=lambda item: str(item["dataset_name"]))
    return tuple(payloads)


def _scan_units_payload(
    scan_units: Sequence[ScanUnit],
) -> tuple[dict[str, object], ...]:
    payloads: list[dict[str, object]] = [
        {
            "key": unit.key,
            "dataset_name": unit.dataset_name,
            "delta_version": unit.delta_version,
            "delta_timestamp": unit.delta_timestamp,
            "snapshot_timestamp": unit.snapshot_timestamp,
            "delta_protocol": (
                to_builtins(unit.delta_protocol, str_keys=True)
                if unit.delta_protocol is not None
                else None
            ),
            "delta_scan_config": (
                to_builtins(unit.delta_scan_config) if unit.delta_scan_config is not None else None
            ),
            "delta_scan_config_hash": unit.delta_scan_config_hash,
            "datafusion_provider": unit.datafusion_provider,
            "protocol_compatible": unit.protocol_compatible,
            "protocol_compatibility": (
                to_builtins(unit.protocol_compatibility)
                if unit.protocol_compatibility is not None
                else None
            ),
            "total_files": unit.total_files,
            "candidate_file_count": unit.candidate_file_count,
            "pruned_file_count": unit.pruned_file_count,
            "candidate_files": [str(path) for path in unit.candidate_files],
            "pushed_filters": list(unit.pushed_filters),
            "projected_columns": list(unit.projected_columns),
        }
        for unit in scan_units
    ]
    payloads.sort(key=lambda item: str(item["key"]))
    return tuple(payloads)


@dataclass(frozen=True)
class PlanIdentityInputs:
    """Inputs used to assemble a stable plan-identity payload."""

    plan_fingerprint: str
    artifacts: PlanArtifacts
    required_udfs: tuple[str, ...]
    required_rewrite_tags: tuple[str, ...]
    delta_inputs: Sequence[DeltaInputPin]
    scan_units: Sequence[ScanUnit]
    profile: DataFusionRuntimeProfile


def plan_identity_payload(inputs: PlanIdentityInputs) -> Mapping[str, object]:
    """Build the deterministic payload used for plan-identity hashing.

    Returns:
        Mapping[str, object]: Canonical identity payload for hashing.
    """
    df_settings_entries = tuple(
        sorted((str(key), str(value)) for key, value in inputs.artifacts.df_settings.items())
    )
    return {
        "version": PLAN_IDENTITY_PAYLOAD_VERSION,
        "plan_fingerprint": inputs.plan_fingerprint,
        "udf_snapshot_hash": inputs.artifacts.udf_snapshot_hash,
        "function_registry_hash": inputs.artifacts.function_registry_hash,
        "required_udfs": tuple(sorted(inputs.required_udfs)),
        "required_rewrite_tags": tuple(sorted(inputs.required_rewrite_tags)),
        "domain_planner_names": tuple(sorted(inputs.artifacts.domain_planner_names)),
        "df_settings_entries": df_settings_entries,
        "planning_env_hash": inputs.artifacts.planning_env_hash,
        "rulepack_hash": inputs.artifacts.rulepack_hash,
        "information_schema_hash": inputs.artifacts.information_schema_hash,
        "delta_inputs": tuple(_delta_inputs_payload(inputs.delta_inputs)),
        "scan_units": tuple(_scan_units_payload(inputs.scan_units)),
        "scan_keys": (),
        "profile_settings_hash": inputs.profile.settings_hash(),
        "profile_context_key": inputs.profile.context_cache_key(),
    }


__all__ = ["PLAN_IDENTITY_PAYLOAD_VERSION", "PlanIdentityInputs", "plan_identity_payload"]

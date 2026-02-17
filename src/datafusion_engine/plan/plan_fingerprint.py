"""Plan fingerprinting helpers for plan bundles."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass

import msgspec

from serde_artifacts import DeltaInputPin
from serde_msgspec import to_builtins
from utils.hashing import hash_msgpack_canonical, hash_sha256_hex


@dataclass(frozen=True)
class PlanFingerprintInputs:
    """Inputs required to fingerprint a plan bundle."""

    substrait_bytes: bytes
    df_settings: Mapping[str, str]
    planning_env_hash: str | None
    rulepack_hash: str | None
    information_schema_hash: str | None
    udf_snapshot_hash: str
    required_udfs: Sequence[str]
    required_rewrite_tags: Sequence[str]
    delta_inputs: Sequence[DeltaInputPin]
    delta_store_policy_hash: str | None


def _delta_protocol_payload(
    protocol: object | None,
) -> tuple[tuple[str, object], ...] | None:
    if protocol is None:
        return None
    resolved: Mapping[str, object] | None
    if isinstance(protocol, Mapping):
        resolved = protocol
    elif isinstance(protocol, msgspec.Struct):
        payload = to_builtins(protocol, str_keys=True)
        resolved = payload if isinstance(payload, Mapping) else None
    else:
        resolved = None
    if resolved is None:
        return None
    items: list[tuple[str, object]] = []
    for key, value in resolved.items():
        if isinstance(value, (str, int, float)) or value is None:
            items.append((str(key), value))
            continue
        if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
            items.append((str(key), tuple(str(item) for item in value)))
            continue
        items.append((str(key), str(value)))
    return tuple(sorted(items, key=lambda item: item[0]))


def compute_plan_fingerprint(inputs: PlanFingerprintInputs) -> str:
    """Compute stable fingerprint for a plan bundle.

    Returns:
        str: Deterministic hash for plan identity and caching.
    """
    settings_items = tuple(sorted(inputs.df_settings.items()))
    settings_hash = hash_msgpack_canonical(settings_items)
    planning_env_hash = inputs.planning_env_hash or ""
    rulepack_hash = inputs.rulepack_hash or ""
    substrait_hash = hash_sha256_hex(inputs.substrait_bytes)
    delta_payload = tuple(
        sorted(
            (
                (
                    pin.dataset_name,
                    pin.version,
                    pin.timestamp,
                    _delta_protocol_payload(pin.protocol),
                    pin.delta_scan_config_hash,
                    pin.datafusion_provider,
                    pin.protocol_compatible,
                )
                for pin in inputs.delta_inputs
            ),
            key=lambda item: item[0],
        )
    )
    payload = (
        ("substrait_hash", substrait_hash),
        ("settings_hash", settings_hash),
        ("planning_env_hash", planning_env_hash),
        ("rulepack_hash", rulepack_hash),
        ("information_schema_hash", inputs.information_schema_hash or ""),
        ("udf_snapshot_hash", inputs.udf_snapshot_hash),
        ("required_udfs", tuple(sorted(inputs.required_udfs))),
        ("required_rewrite_tags", tuple(sorted(inputs.required_rewrite_tags))),
        ("delta_inputs", delta_payload),
        ("delta_store_policy_hash", inputs.delta_store_policy_hash),
    )
    return hash_msgpack_canonical(payload)


__all__ = ["PlanFingerprintInputs", "compute_plan_fingerprint"]

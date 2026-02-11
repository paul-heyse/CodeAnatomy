"""Typed helpers for Rust run-result planning artifacts."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

import msgspec


def _mapping(value: object) -> Mapping[str, object]:
    if isinstance(value, Mapping):
        return value
    return {}


def _string(value: object) -> str | None:
    if isinstance(value, str) and value:
        return value
    return None


def _digest_to_hex(value: object) -> str | None:
    if isinstance(value, str) and value:
        return value
    if isinstance(value, bytes):
        return value.hex()
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        try:
            payload = bytes(int(item) & 0xFF for item in value)
        except (TypeError, ValueError):
            return None
        if payload:
            return payload.hex()
    return None


@dataclass(frozen=True)
class ProviderIdentityContract:
    """Provider identity tuple decoded from Rust planning artifacts."""

    table_name: str
    identity_hash_hex: str | None


@dataclass(frozen=True)
class ProviderLineageEntryContract:
    """Lineage details for a provider scan node."""

    scan_node_id: str
    provider_name: str
    provider_identity_hash_hex: str | None
    delta_version: int | None
    file_count: int | None


@dataclass(frozen=True)
class PlanBundleArtifactContract:
    """Typed projection of a Rust plan-bundle artifact payload."""

    p0_digest_hex: str | None
    p1_digest_hex: str | None
    p2_digest_hex: str | None
    planning_surface_hash_hex: str | None
    provider_identities: tuple[ProviderIdentityContract, ...]
    provider_lineage: tuple[ProviderLineageEntryContract, ...]
    referenced_tables: tuple[str, ...]
    required_udfs: tuple[str, ...]
    portable_format: str | None


@dataclass(frozen=True)
class RunResultContract:
    """Typed top-level run-result payload consumed by Python callers."""

    spec_hash_hex: str | None
    envelope_hash_hex: str | None
    warnings: tuple[dict[str, object], ...]
    plan_bundles: tuple[PlanBundleArtifactContract, ...]


def _provider_identity_from_payload(payload: Mapping[str, object]) -> ProviderIdentityContract:
    return ProviderIdentityContract(
        table_name=str(payload.get("table_name", "")),
        identity_hash_hex=_digest_to_hex(payload.get("identity_hash")),
    )


def _provider_lineage_from_payload(payload: Mapping[str, object]) -> ProviderLineageEntryContract:
    delta_version = payload.get("delta_version")
    file_count = payload.get("file_count")
    return ProviderLineageEntryContract(
        scan_node_id=str(payload.get("scan_node_id", "")),
        provider_name=str(payload.get("provider_name", "")),
        provider_identity_hash_hex=_digest_to_hex(payload.get("provider_identity_hash")),
        delta_version=int(delta_version) if isinstance(delta_version, int) else None,
        file_count=int(file_count) if isinstance(file_count, int) else None,
    )


def plan_bundle_from_payload(payload: Mapping[str, object]) -> PlanBundleArtifactContract:
    """Convert a raw plan-bundle payload into a typed contract object.

    Returns:
    -------
    PlanBundleArtifactContract
        Typed plan bundle contract decoded from payload content.
    """
    provider_identities_raw = payload.get("provider_identities")
    provider_lineage_raw = payload.get("provider_lineage")
    referenced_tables_raw = payload.get("referenced_tables")
    required_udfs_raw = payload.get("required_udfs")
    portability = _mapping(payload.get("portability"))
    return PlanBundleArtifactContract(
        p0_digest_hex=_digest_to_hex(payload.get("p0_digest")),
        p1_digest_hex=_digest_to_hex(payload.get("p1_digest")),
        p2_digest_hex=_digest_to_hex(payload.get("p2_digest")),
        planning_surface_hash_hex=_digest_to_hex(payload.get("planning_surface_hash")),
        provider_identities=tuple(
            _provider_identity_from_payload(_mapping(item)) for item in provider_identities_raw
        )
        if isinstance(provider_identities_raw, Sequence)
        else (),
        provider_lineage=tuple(
            _provider_lineage_from_payload(_mapping(item)) for item in provider_lineage_raw
        )
        if isinstance(provider_lineage_raw, Sequence)
        else (),
        referenced_tables=tuple(item for item in referenced_tables_raw if isinstance(item, str))
        if isinstance(referenced_tables_raw, Sequence)
        else (),
        required_udfs=tuple(item for item in required_udfs_raw if isinstance(item, str))
        if isinstance(required_udfs_raw, Sequence)
        else (),
        portable_format=_string(portability.get("portable_format")),
    )


def run_result_from_payload(payload: Mapping[str, object]) -> RunResultContract:
    """Convert a raw run-result mapping into a typed contract object.

    Returns:
    -------
    RunResultContract
        Typed run-result contract with decoded plan bundles and warnings.
    """
    plan_bundles_raw = payload.get("plan_bundles")
    warnings_raw = payload.get("warnings")
    return RunResultContract(
        spec_hash_hex=_digest_to_hex(payload.get("spec_hash")),
        envelope_hash_hex=_digest_to_hex(payload.get("envelope_hash")),
        warnings=tuple(dict(item) for item in warnings_raw if isinstance(item, Mapping))
        if isinstance(warnings_raw, Sequence)
        else (),
        plan_bundles=tuple(plan_bundle_from_payload(_mapping(item)) for item in plan_bundles_raw)
        if isinstance(plan_bundles_raw, Sequence)
        else (),
    )


def decode_run_result_payload(payload: dict[str, object] | str | bytes) -> RunResultContract:
    """Decode JSON-compatible run-result payload into typed contract data.

    Returns:
    -------
    RunResultContract
        Typed run-result contract decoded from dict, bytes, or JSON string input.
    """
    data: dict[str, object]
    if isinstance(payload, dict):
        data = payload
    elif isinstance(payload, bytes):
        data = msgspec.json.decode(payload, type=dict[str, Any])
    else:
        data = msgspec.json.decode(payload.encode(), type=dict[str, Any])
    return run_result_from_payload(data)


__all__ = [
    "PlanBundleArtifactContract",
    "ProviderIdentityContract",
    "ProviderLineageEntryContract",
    "RunResultContract",
    "decode_run_result_payload",
    "plan_bundle_from_payload",
    "run_result_from_payload",
]

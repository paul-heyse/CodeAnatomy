"""Reproducible execution package for deterministic replay and bisecting.

An execution package captures the composite fingerprint of all inputs that
determine a pipeline execution outcome.  By recording this package as an
artifact, callers can:

- **Replay**: Reconstruct the exact execution environment from hashes.
- **Bisect**: Compare packages to identify which component changed between
  two runs.
- **Attribute**: Link performance regressions to specific policy or manifest
  changes.
"""

from __future__ import annotations

import time
from collections.abc import Mapping
from typing import TYPE_CHECKING, Protocol

from serde_msgspec import StructBaseCompat
from utils.hashing import hash_msgpack_canonical

if TYPE_CHECKING:
    from semantics.program_manifest import SemanticProgramManifest


class SemanticIrHashLike(Protocol):
    """Protocol for semantic IR hash access."""

    ir_hash: str


class ManifestHashLike(Protocol):
    """Protocol for manifest hash access."""

    model_hash: str


class ManifestWithSemanticIr(Protocol):
    """Protocol for manifest objects carrying semantic IR payloads."""

    semantic_ir: SemanticIrHashLike | None


class PolicyFingerprintLike(Protocol):
    """Protocol for compiled policy fingerprint access."""

    policy_fingerprint: str | None


class SettingsHashValueLike(Protocol):
    """Protocol for settings hash attribute access."""

    settings_hash: str


class SettingsHashCallableLike(Protocol):
    """Protocol for settings hash method access."""

    def settings_hash(self) -> str: ...


class ExecutionPackageArtifact(StructBaseCompat, frozen=True):
    """Replayable execution package keyed by composite fingerprint.

    All fields are deterministic hash strings (or a timestamp) enabling
    downstream consumers to detect exactly which inputs changed between
    two pipeline runs.
    """

    package_fingerprint: str
    manifest_hash: str
    policy_artifact_hash: str
    capability_snapshot_hash: str
    plan_bundle_fingerprints: dict[str, str]
    session_config_hash: str
    created_at_unix_ms: int


def _hash_manifest(
    manifest: SemanticProgramManifest | ManifestHashLike | ManifestWithSemanticIr | object | None,
) -> str:
    if manifest is None:
        return ""
    model_hash = getattr(manifest, "model_hash", None)
    if isinstance(model_hash, str) and model_hash:
        return model_hash
    ir = getattr(manifest, "semantic_ir", None)
    ir_hash = getattr(ir, "ir_hash", None)
    if isinstance(ir_hash, str) and ir_hash:
        return ir_hash
    return hash_msgpack_canonical(repr(manifest))


def _hash_policy(compiled_policy: PolicyFingerprintLike | object | None) -> str:
    if compiled_policy is None:
        return ""
    fingerprint = getattr(compiled_policy, "policy_fingerprint", None)
    if isinstance(fingerprint, str) and fingerprint:
        return fingerprint
    return hash_msgpack_canonical(repr(compiled_policy))


def _hash_capability_snapshot(
    capability_snapshot: SettingsHashValueLike | object | None,
) -> str:
    if capability_snapshot is None:
        return ""
    settings_hash = getattr(capability_snapshot, "settings_hash", None)
    if isinstance(settings_hash, str) and settings_hash:
        return settings_hash
    return hash_msgpack_canonical(repr(capability_snapshot))


def _hash_session_config(
    session_config: SettingsHashValueLike | SettingsHashCallableLike | str | object | None,
) -> str:
    if session_config is None:
        return ""
    if isinstance(session_config, str):
        return session_config
    settings_hash_method = getattr(session_config, "settings_hash", None)
    if callable(settings_hash_method):
        result = settings_hash_method()
        if isinstance(result, str):
            return result
    if isinstance(settings_hash_method, str) and settings_hash_method:
        return settings_hash_method
    return hash_msgpack_canonical(repr(session_config))


def _normalize_plan_bundle_fingerprints(
    plan_bundles: Mapping[str, str] | None,
) -> dict[str, str]:
    if plan_bundles is None:
        return {}
    return dict(sorted(plan_bundles.items()))


def _composite_fingerprint(
    *,
    manifest_hash: str,
    policy_artifact_hash: str,
    capability_snapshot_hash: str,
    plan_bundle_fingerprints: dict[str, str],
    session_config_hash: str,
) -> str:
    payload = (
        ("manifest_hash", manifest_hash),
        ("policy_artifact_hash", policy_artifact_hash),
        ("capability_snapshot_hash", capability_snapshot_hash),
        ("plan_bundle_fingerprints", tuple(sorted(plan_bundle_fingerprints.items()))),
        ("session_config_hash", session_config_hash),
    )
    return hash_msgpack_canonical(payload)


def build_execution_package(
    *,
    manifest: SemanticProgramManifest | object | None = None,
    compiled_policy: object | None = None,
    capability_snapshot: object | None = None,
    plan_bundle_fingerprints: Mapping[str, str] | None = None,
    session_config: object | None = None,
) -> ExecutionPackageArtifact:
    """Build a reproducible execution package from pipeline components.

    Each component is hashed independently.  The composite
    ``package_fingerprint`` is derived from all component hashes so that
    any single change produces a different package fingerprint.

    Parameters
    ----------
    manifest
        Semantic program manifest (or None if unavailable).
    compiled_policy
        Compiled execution policy (or None if unavailable).
    capability_snapshot
        Runtime capabilities snapshot (or None if unavailable).
    plan_bundle_fingerprints
        Per-view plan fingerprints mapping.
    session_config
        Session configuration object or settings hash string.

    Returns:
    -------
    ExecutionPackageArtifact
        Fully populated execution package with composite fingerprint.
    """
    manifest_hash = _hash_manifest(manifest)
    policy_hash = _hash_policy(compiled_policy)
    capability_hash = _hash_capability_snapshot(capability_snapshot)
    normalized_bundles = _normalize_plan_bundle_fingerprints(plan_bundle_fingerprints)
    config_hash = _hash_session_config(session_config)

    fingerprint = _composite_fingerprint(
        manifest_hash=manifest_hash,
        policy_artifact_hash=policy_hash,
        capability_snapshot_hash=capability_hash,
        plan_bundle_fingerprints=normalized_bundles,
        session_config_hash=config_hash,
    )

    return ExecutionPackageArtifact(
        package_fingerprint=fingerprint,
        manifest_hash=manifest_hash,
        policy_artifact_hash=policy_hash,
        capability_snapshot_hash=capability_hash,
        plan_bundle_fingerprints=normalized_bundles,
        session_config_hash=config_hash,
        created_at_unix_ms=int(time.time() * 1000),
    )


__all__ = [
    "ExecutionPackageArtifact",
    "build_execution_package",
]

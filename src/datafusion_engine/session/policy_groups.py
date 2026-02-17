"""Focused policy sub-groups for runtime profile policy bundles."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


@dataclass(frozen=True)
class CachePolicyGroup:
    """Cache-related policy settings."""

    cache_profile_name: (
        Literal[
            "snapshot_pinned",
            "always_latest_ttl30s",
            "multi_tenant_strict",
        ]
        | None
    ) = None
    cache_max_columns: int | None = 64
    snapshot_pinned_mode: Literal["off", "delta_version"] = "off"


@dataclass(frozen=True)
class SqlPolicyGroup:
    """SQL-policy related settings."""

    sql_policy_name: str | None = "write"
    param_identifier_allowlist: tuple[str, ...] = ()


@dataclass(frozen=True)
class DeltaPolicyGroup:
    """Delta protocol and mutation policy settings."""

    delta_plan_codec_physical: str = "delta_physical"
    delta_plan_codec_logical: str = "delta_logical"
    delta_protocol_mode: Literal["error", "warn", "ignore"] = "error"


@dataclass(frozen=True)
class RuntimeArtifactPolicyGroup:
    """Runtime artifact caching and root settings."""

    runtime_artifact_cache_enabled: bool = False
    runtime_artifact_cache_root: str | None = None
    metadata_cache_snapshot_enabled: bool = False
    plan_artifacts_root: str | None = None


__all__ = [
    "CachePolicyGroup",
    "DeltaPolicyGroup",
    "RuntimeArtifactPolicyGroup",
    "SqlPolicyGroup",
]

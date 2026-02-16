"""Single-authority shared session constants and helpers."""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING

from datafusion import SessionContext

from cache.diskcache_factory import cache_for_kind
from datafusion_engine.schema.introspection_core import SchemaIntrospector
from datafusion_engine.sql.options import sql_options_for_profile

if TYPE_CHECKING:
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile

KIB: int = 1024
MIB: int = 1024 * KIB
GIB: int = 1024 * MIB

CACHE_PROFILES: Mapping[str, Mapping[str, str]] = {
    "snapshot_pinned": {
        "datafusion.runtime.list_files_cache_limit": str(64 * MIB),
        "datafusion.runtime.metadata_cache_limit": str(128 * MIB),
    },
    "always_latest_ttl30s": {
        "datafusion.runtime.list_files_cache_limit": str(64 * MIB),
        "datafusion.runtime.list_files_cache_ttl": "30s",
        "datafusion.runtime.metadata_cache_limit": str(128 * MIB),
    },
    "multi_tenant_strict": {
        "datafusion.runtime.list_files_cache_limit": "0",
        "datafusion.runtime.metadata_cache_limit": "0",
    },
}

DATAFUSION_SQL_ERROR = Exception
EXTENSION_MODULE_NAMES: tuple[str, ...] = ("datafusion_engine.extensions.datafusion_ext",)


def parse_major_version(version: str) -> int | None:
    """Return parsed major version from a dotted version string."""
    head = version.split(".", maxsplit=1)[0]
    if head.isdigit():
        return int(head)
    return None


def create_schema_introspector(
    profile: DataFusionRuntimeProfile,
    ctx: SessionContext,
) -> SchemaIntrospector:
    """Create a schema introspector for a profile/context pair.

    Returns:
    -------
    SchemaIntrospector
        Introspector configured from runtime profile cache policies.
    """
    diskcache_profile = profile.policies.diskcache_profile
    cache = cache_for_kind(diskcache_profile, "schema") if diskcache_profile is not None else None
    cache_ttl = diskcache_profile.ttl_for("schema") if diskcache_profile is not None else None
    return SchemaIntrospector(
        ctx,
        sql_options=sql_options_for_profile(profile),
        cache=cache,
        cache_prefix=profile.context_cache_key(),
        cache_ttl=cache_ttl,
    )


__all__ = [
    "CACHE_PROFILES",
    "DATAFUSION_SQL_ERROR",
    "EXTENSION_MODULE_NAMES",
    "GIB",
    "KIB",
    "MIB",
    "create_schema_introspector",
    "parse_major_version",
]

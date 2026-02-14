"""Dataset, schema, and UDF introspection helpers.

Extracted from session/runtime.py to isolate standalone introspection
functions from session construction logic.

These helpers query registered tables, schemas, and UDFs from DataFusion
sessions for extraction-phase workflows. Functions that deeply depend on
internal runtime.py state remain in runtime.py; this module contains
the self-contained helpers.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING

from cache.diskcache_factory import (
    DiskCacheKind,
    cache_for_kind,
    evict_cache_tag,
    run_profile_maintenance,
)
from datafusion_engine.lineage.diagnostics import record_events
from datafusion_engine.schema.introspection import SchemaIntrospector

if TYPE_CHECKING:
    from datafusion import SessionContext

    from datafusion_engine.session.runtime import DataFusionRuntimeProfile
    from semantics.program_manifest import ManifestDatasetResolver


def schema_introspector_for_profile(
    profile: DataFusionRuntimeProfile,
    ctx: SessionContext,
    *,
    cache_prefix: str | None = None,
) -> SchemaIntrospector:
    """Return a schema introspector for a runtime profile.

    Parameters
    ----------
    profile
        Runtime profile providing cache and SQL options.
    ctx
        DataFusion session context to introspect.
    cache_prefix
        Optional cache prefix override.

    Returns:
    -------
    SchemaIntrospector
        Introspector configured from the profile.
    """
    cache_profile = profile.policies.diskcache_profile
    cache = cache_for_kind(cache_profile, "schema") if cache_profile is not None else None
    cache_ttl = cache_profile.ttl_for("schema") if cache_profile is not None else None
    resolved_prefix = cache_prefix or profile.context_cache_key()
    return SchemaIntrospector(
        ctx,
        sql_options=profile.sql_options(),
        cache=cache,
        cache_prefix=resolved_prefix,
        cache_ttl=cache_ttl,
    )


def collect_datafusion_metrics(
    profile: DataFusionRuntimeProfile,
) -> Mapping[str, object] | None:
    """Return optional DataFusion metrics payload.

    Parameters
    ----------
    profile
        Runtime profile to check for metrics configuration.

    Returns:
    -------
    Mapping[str, object] | None
        Metrics payload when enabled and available.
    """
    if not profile.features.enable_metrics or profile.diagnostics.metrics_collector is None:
        return None
    return profile.diagnostics.metrics_collector()


def collect_datafusion_traces(
    profile: DataFusionRuntimeProfile,
) -> Mapping[str, object] | None:
    """Return optional DataFusion tracing payload.

    Parameters
    ----------
    profile
        Runtime profile to check for tracing configuration.

    Returns:
    -------
    Mapping[str, object] | None
        Tracing payload when enabled and available.
    """
    if not profile.features.enable_tracing or profile.diagnostics.tracing_collector is None:
        return None
    return profile.diagnostics.tracing_collector()


def run_diskcache_maintenance(
    profile: DataFusionRuntimeProfile,
    *,
    kinds: tuple[DiskCacheKind, ...] | None = None,
    include_check: bool = False,
    record: bool = True,
) -> list[dict[str, object]]:
    """Run DiskCache maintenance for a runtime profile.

    Parameters
    ----------
    profile
        Runtime profile providing DiskCache configuration.
    kinds
        Optional subset of cache kinds to maintain.
    include_check
        Include integrity checks during maintenance.
    record
        Record maintenance events to diagnostics.

    Returns:
    -------
    list[dict[str, object]]
        Maintenance payloads for each cache kind.
    """
    cache_profile = profile.policies.diskcache_profile
    if cache_profile is None:
        return []
    results = run_profile_maintenance(
        cache_profile,
        kinds=kinds,
        include_check=include_check,
    )
    payloads: list[dict[str, object]] = [
        {
            "kind": result.kind,
            "expired": result.expired,
            "culled": result.culled,
            "check_errors": result.check_errors,
        }
        for result in results
    ]
    if record and payloads:
        record_events(profile, "diskcache_maintenance_v1", payloads)
    return payloads


def evict_diskcache_entries(
    profile: DataFusionRuntimeProfile,
    *,
    kind: DiskCacheKind,
    tag: str,
) -> int:
    """Evict DiskCache entries for a runtime profile.

    Parameters
    ----------
    profile
        Runtime profile providing DiskCache configuration.
    kind
        Cache kind to evict from.
    tag
        Tag to evict.

    Returns:
    -------
    int
        Count of evicted entries.
    """
    cache_profile = profile.policies.diskcache_profile
    if cache_profile is None:
        return 0
    return evict_cache_tag(cache_profile, kind=kind, tag=tag)


def register_cdf_inputs_for_profile(
    profile: DataFusionRuntimeProfile,
    ctx: SessionContext,
    *,
    table_names: Sequence[str],
    dataset_resolver: ManifestDatasetResolver | None = None,
) -> Mapping[str, str]:
    """Register Delta CDF inputs for the requested tables.

    Parameters
    ----------
    profile
        Runtime profile for CDF registration.
    ctx
        DataFusion session context.
    table_names
        Table names to register CDF inputs for.
    dataset_resolver
        Optional manifest dataset resolver.

    Returns:
    -------
    Mapping[str, str]
        Mapping of base table names to registered CDF view names.
    """
    from datafusion_engine.delta.cdf import register_cdf_inputs

    return register_cdf_inputs(
        ctx, profile, table_names=table_names, dataset_resolver=dataset_resolver
    )

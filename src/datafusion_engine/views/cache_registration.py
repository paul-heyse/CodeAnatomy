"""View cache registration helpers for dependency-aware graph assembly."""

from __future__ import annotations

import time
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Protocol, cast

import pyarrow as pa
from datafusion import SessionContext
from datafusion.dataframe import DataFrame

from datafusion_engine.identity import schema_identity_hash
from datafusion_engine.io.adapter import DataFusionIOAdapter
from datafusion_engine.views.bundle_extraction import arrow_schema_from_df
from serde_artifacts import ViewCacheArtifact, ViewCacheArtifactEnvelope
from serde_msgspec import convert, to_builtins
from utils.uuid_factory import uuid7_hex

if TYPE_CHECKING:
    from datafusion_engine.dataset.registry import DatasetLocation
    from datafusion_engine.plan.bundle_artifact import DataFusionPlanArtifact
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile
    from datafusion_engine.views.artifacts import CachePolicy
    from semantics.program_manifest import ManifestDatasetResolver


class ViewNodeLike(Protocol):
    """Protocol required by cache registration helpers."""

    @property
    def name(self) -> str: ...

    @property
    def cache_policy(self) -> CachePolicy: ...

    @property
    def plan_bundle(self) -> DataFusionPlanArtifact | None: ...


class ViewCacheRuntimeLike(Protocol):
    """Runtime projection required by cache registration helpers."""

    @property
    def runtime_profile(self) -> DataFusionRuntimeProfile | None: ...

    @property
    def dataset_resolver(self) -> ManifestDatasetResolver | None: ...


class ViewCacheOptionsLike(Protocol):
    """Cache options projection required by cache registration helpers."""

    @property
    def overwrite(self) -> bool: ...

    @property
    def temporary(self) -> bool: ...


class ViewCacheContextLike(Protocol):
    """Cache context projection required by cache registration helpers."""

    @property
    def runtime(self) -> ViewCacheRuntimeLike: ...

    @property
    def options(self) -> ViewCacheOptionsLike: ...


@dataclass(frozen=True)
class CacheRegistrationContext:
    """Inputs required to register a cached view."""

    ctx: SessionContext
    adapter: DataFusionIOAdapter
    node: ViewNodeLike
    df: DataFrame
    cache: ViewCacheContextLike
    schema: pa.Schema
    schema_hash: str | None


def register_view_with_cache(
    ctx: SessionContext,
    *,
    adapter: DataFusionIOAdapter,
    node: ViewNodeLike,
    df: DataFrame,
    cache: ViewCacheContextLike,
) -> DataFrame:
    """Register a DataFrame according to the view cache policy.

    Returns:
        Registered DataFrame handle for the configured cache policy.

    Raises:
        ValueError: If cache policy configuration is invalid.
    """
    schema = arrow_schema_from_df(df)
    schema_hash = schema_identity_hash(schema)
    registration = CacheRegistrationContext(
        ctx=ctx,
        adapter=adapter,
        node=node,
        df=df,
        cache=cache,
        schema=schema,
        schema_hash=schema_hash,
    )
    if node.cache_policy == "delta_staging":
        return _register_delta_staging_cache(registration)
    if node.cache_policy == "delta_output":
        return _register_delta_output_cache(
            registration,
            dataset_resolver=cache.runtime.dataset_resolver,
        )
    if node.cache_policy != "none":
        msg = f"Unsupported cache policy: {node.cache_policy!r}."
        raise ValueError(msg)
    return _register_uncached_view(registration)


def register_delta_staging_cache(registration: CacheRegistrationContext) -> DataFrame:
    """Register a view using the Delta staging cache policy.

    Returns:
        DataFrame resolved from the staging cache table.
    """
    return _register_delta_staging_cache(registration)


def register_delta_output_cache(
    registration: CacheRegistrationContext,
    *,
    dataset_resolver: ManifestDatasetResolver | None = None,
) -> DataFrame:
    """Register a view using the Delta output cache policy.

    Returns:
        DataFrame resolved from the output cache table.
    """
    return _register_delta_output_cache(registration, dataset_resolver=dataset_resolver)


def register_uncached_view(registration: CacheRegistrationContext) -> DataFrame:
    """Register a view directly without cache materialization.

    Returns:
        Original DataFrame after registration.
    """
    return _register_uncached_view(registration)


def _record_cache_artifact(
    cache: ViewCacheContextLike,
    *,
    node: ViewNodeLike,
    cache_path: str | None,
    status: str,
    hit: bool | None = None,
) -> None:
    profile = cache.runtime.runtime_profile
    if profile is None:
        return
    from datafusion_engine.lineage.diagnostics import record_artifact
    from serde_artifact_specs import VIEW_CACHE_ARTIFACTS_SPEC

    artifact = ViewCacheArtifact(
        view_name=node.name,
        cache_policy=node.cache_policy,
        cache_path=cache_path,
        plan_fingerprint=node.plan_bundle.plan_fingerprint if node.plan_bundle else None,
        status=status,
        hit=hit,
    )
    envelope = ViewCacheArtifactEnvelope(payload=artifact)
    validated = convert(
        to_builtins(envelope, str_keys=True),
        target_type=ViewCacheArtifactEnvelope,
        strict=True,
    )
    payload = to_builtins(validated, str_keys=True)
    record_artifact(
        profile,
        VIEW_CACHE_ARTIFACTS_SPEC,
        cast("Mapping[str, object]", payload),
    )


def _record_cache_error(
    cache: ViewCacheContextLike,
    *,
    node: ViewNodeLike,
    cache_path: str | None,
    error: str,
    expected_schema_hash: str | None,
) -> None:
    profile = cache.runtime.runtime_profile
    if profile is None:
        return
    from datafusion_engine.lineage.diagnostics import record_artifact
    from serde_artifact_specs import VIEW_CACHE_ERRORS_SPEC

    payload = {
        "event_time_unix_ms": int(time.time() * 1000),
        "view_name": node.name,
        "cache_policy": node.cache_policy,
        "cache_path": cache_path,
        "expected_schema_hash": expected_schema_hash,
        "error": error,
    }
    record_artifact(profile, VIEW_CACHE_ERRORS_SPEC, payload)


def _register_delta_staging_cache(registration: CacheRegistrationContext) -> DataFrame:
    runtime_profile = _require_runtime_profile(
        registration.cache,
        policy_label="Delta staging cache",
    )
    staging_path = _delta_staging_path(runtime_profile, registration.node)
    plan_fingerprint, plan_identity_hash = _plan_identifiers(registration.node)

    from datafusion_engine.cache.commit_metadata import (
        CacheCommitMetadataRequest,
        cache_commit_metadata,
    )
    from datafusion_engine.cache.inventory import CacheInventoryEntry, delta_report_file_count
    from datafusion_engine.cache.registry import (
        CacheHitRequest,
        record_cache_inventory,
        register_cached_delta_table,
        resolve_cache_hit,
    )
    from datafusion_engine.dataset.registry import DatasetLocation
    from datafusion_engine.delta import enforce_schema_evolution
    from datafusion_engine.delta.contracts import DeltaSchemaMismatchError
    from datafusion_engine.io.write_core import (
        WriteFormat,
        WriteMode,
        WritePipeline,
        WriteRequest,
    )
    from obs.otel import cache_span
    from storage.deltalake import DeltaSchemaRequest

    try:
        with cache_span(
            "cache.view.delta_staging.read",
            cache_policy=registration.node.cache_policy,
            cache_scope="view",
            operation="read",
            attributes={
                "view_name": registration.node.name,
                "plan_identity_hash": plan_identity_hash,
            },
        ) as (_span, set_result):
            cache_hit = resolve_cache_hit(
                registration.ctx,
                runtime_profile,
                request=CacheHitRequest(
                    view_name=registration.node.name,
                    cache_path=staging_path,
                    plan_identity_hash=plan_identity_hash,
                    expected_schema_hash=registration.schema_hash,
                    allow_evolution=False,
                    storage_options=None,
                    log_storage_options=None,
                ),
            )
            set_result("hit" if cache_hit is not None else "miss")
    except DeltaSchemaMismatchError as exc:
        _record_cache_error(
            registration.cache,
            node=registration.node,
            cache_path=staging_path,
            error=str(exc),
            expected_schema_hash=registration.schema_hash,
        )
        raise

    if cache_hit is not None:
        register_cached_delta_table(
            registration.ctx,
            runtime_profile,
            name=registration.node.name,
            location=DatasetLocation(path=staging_path, format="delta"),
            snapshot_version=cache_hit.snapshot_version,
        )
        _record_cache_artifact(
            registration.cache,
            node=registration.node,
            cache_path=staging_path,
            status="cached",
            hit=True,
        )
        return registration.ctx.table(registration.node.name)

    try:
        enforce_schema_evolution(
            request=DeltaSchemaRequest(path=staging_path),
            expected_schema_hash=registration.schema_hash,
            allow_evolution=False,
        )
    except DeltaSchemaMismatchError as exc:
        _record_cache_error(
            registration.cache,
            node=registration.node,
            cache_path=staging_path,
            error=str(exc),
            expected_schema_hash=registration.schema_hash,
        )
        raise

    partition_by = _cache_partition_by(registration.schema, location=None)
    commit_metadata = cache_commit_metadata(
        CacheCommitMetadataRequest(
            operation="cache_write",
            cache_policy=registration.node.cache_policy,
            cache_scope="view",
            schema_hash=registration.schema_hash,
            plan_hash=plan_identity_hash,
        )
    )
    pipeline = WritePipeline(registration.ctx, runtime_profile=runtime_profile)

    with cache_span(
        "cache.view.delta_staging.write",
        cache_policy=registration.node.cache_policy,
        cache_scope="view",
        operation="write",
        attributes={
            "view_name": registration.node.name,
            "plan_identity_hash": plan_identity_hash,
        },
    ) as (_span, set_result):
        result = pipeline.write(
            WriteRequest(
                source=registration.df,
                destination=staging_path,
                format=WriteFormat.DELTA,
                mode=WriteMode.OVERWRITE,
                partition_by=partition_by,
                plan_fingerprint=plan_fingerprint,
                plan_identity_hash=plan_identity_hash,
                format_options={"commit_metadata": commit_metadata},
            )
        )
        set_result("write")

    if result.delta_result is None or result.delta_result.version is None:
        _record_cache_error(
            registration.cache,
            node=registration.node,
            cache_path=staging_path,
            error="Delta write did not resolve a snapshot version.",
            expected_schema_hash=registration.schema_hash,
        )
        return _register_uncached_view(registration)

    try:
        register_cached_delta_table(
            registration.ctx,
            runtime_profile,
            name=registration.node.name,
            location=DatasetLocation(path=staging_path, format="delta"),
            snapshot_version=result.delta_result.version if result.delta_result else None,
        )
    except (RuntimeError, ValueError, TypeError, OSError) as exc:
        _record_cache_error(
            registration.cache,
            node=registration.node,
            cache_path=staging_path,
            error=str(exc),
            expected_schema_hash=registration.schema_hash,
        )
        return _register_uncached_view(registration)

    file_count = delta_report_file_count(
        result.delta_result.report if result.delta_result is not None else None
    )
    record_cache_inventory(
        runtime_profile,
        entry=CacheInventoryEntry(
            view_name=registration.node.name,
            cache_policy=registration.node.cache_policy,
            cache_path=staging_path,
            result="write",
            plan_fingerprint=plan_fingerprint,
            plan_identity_hash=plan_identity_hash,
            schema_identity_hash=registration.schema_hash,
            snapshot_version=result.delta_result.version if result.delta_result else None,
            snapshot_timestamp=None,
            row_count=result.rows_written,
            file_count=file_count,
            partition_by=partition_by,
        ),
        ctx=registration.ctx,
    )
    _record_cache_artifact(
        registration.cache,
        node=registration.node,
        cache_path=staging_path,
        status="cached",
        hit=False,
    )
    return registration.ctx.table(registration.node.name)


def _register_delta_output_cache(
    registration: CacheRegistrationContext,
    *,
    dataset_resolver: ManifestDatasetResolver | None = None,
) -> DataFrame:
    runtime_profile = _require_runtime_profile(
        registration.cache,
        policy_label="Delta output cache",
    )
    if dataset_resolver is None:
        msg = f"dataset_resolver is required for Delta output cache of {registration.node.name!r}."
        raise ValueError(msg)
    location = dataset_resolver.location(registration.node.name)
    if location is None:
        msg = f"Delta output cache missing dataset location for {registration.node.name!r}."
        raise ValueError(msg)
    target_path = str(location.path)

    from datafusion_engine.cache.commit_metadata import (
        CacheCommitMetadataRequest,
        cache_commit_metadata,
    )
    from datafusion_engine.cache.inventory import CacheInventoryEntry, delta_report_file_count
    from datafusion_engine.cache.registry import (
        CacheHitRequest,
        record_cache_inventory,
        register_cached_delta_table,
        resolve_cache_hit,
    )
    from datafusion_engine.delta import enforce_schema_evolution
    from datafusion_engine.delta.contracts import DeltaSchemaMismatchError
    from datafusion_engine.io.write_core import (
        WriteFormat,
        WriteMode,
        WritePipeline,
        WriteRequest,
    )
    from obs.otel import cache_span
    from storage.deltalake import DeltaSchemaRequest

    allow_evolution = _allow_schema_evolution(location)
    plan_fingerprint, plan_identity_hash = _plan_identifiers(registration.node)
    try:
        with cache_span(
            "cache.view.delta_output.read",
            cache_policy=registration.node.cache_policy,
            cache_scope="view",
            operation="read",
            attributes={
                "view_name": registration.node.name,
                "plan_identity_hash": plan_identity_hash,
            },
        ) as (_span, set_result):
            cache_hit = resolve_cache_hit(
                registration.ctx,
                runtime_profile,
                request=CacheHitRequest(
                    view_name=registration.node.name,
                    cache_path=target_path,
                    plan_identity_hash=plan_identity_hash,
                    expected_schema_hash=registration.schema_hash,
                    allow_evolution=allow_evolution,
                    storage_options=location.storage_options,
                    log_storage_options=location.delta_log_storage_options,
                ),
            )
            set_result("hit" if cache_hit is not None else "miss")
    except DeltaSchemaMismatchError as exc:
        _record_cache_error(
            registration.cache,
            node=registration.node,
            cache_path=target_path,
            error=str(exc),
            expected_schema_hash=registration.schema_hash,
        )
        raise

    if cache_hit is not None:
        register_cached_delta_table(
            registration.ctx,
            runtime_profile,
            name=registration.node.name,
            location=location,
            snapshot_version=cache_hit.snapshot_version,
        )
        _record_cache_artifact(
            registration.cache,
            node=registration.node,
            cache_path=target_path,
            status="cached",
            hit=True,
        )
        return registration.ctx.table(registration.node.name)

    try:
        enforce_schema_evolution(
            request=DeltaSchemaRequest(
                path=target_path,
                storage_options=location.storage_options,
                log_storage_options=location.delta_log_storage_options,
                version=location.delta_version,
                timestamp=location.delta_timestamp,
            ),
            expected_schema_hash=registration.schema_hash,
            allow_evolution=allow_evolution,
        )
    except DeltaSchemaMismatchError as exc:
        _record_cache_error(
            registration.cache,
            node=registration.node,
            cache_path=target_path,
            error=str(exc),
            expected_schema_hash=registration.schema_hash,
        )
        raise

    partition_by = _cache_partition_by(registration.schema, location=location)
    commit_metadata = cache_commit_metadata(
        CacheCommitMetadataRequest(
            operation="cache_write",
            cache_policy=registration.node.cache_policy,
            cache_scope="view",
            schema_hash=registration.schema_hash,
            plan_hash=plan_identity_hash,
        )
    )
    pipeline = WritePipeline(
        registration.ctx,
        runtime_profile=runtime_profile,
        dataset_resolver=dataset_resolver,
    )

    with cache_span(
        "cache.view.delta_output.write",
        cache_policy=registration.node.cache_policy,
        cache_scope="view",
        operation="write",
        attributes={
            "view_name": registration.node.name,
            "plan_identity_hash": plan_identity_hash,
        },
    ) as (_span, set_result):
        result = pipeline.write(
            WriteRequest(
                source=registration.df,
                destination=target_path,
                format=WriteFormat.DELTA,
                mode=WriteMode.OVERWRITE,
                partition_by=partition_by,
                plan_fingerprint=plan_fingerprint,
                plan_identity_hash=plan_identity_hash,
                format_options={"commit_metadata": commit_metadata},
            )
        )
        set_result("write")

    register_cached_delta_table(
        registration.ctx,
        runtime_profile,
        name=registration.node.name,
        location=location,
        snapshot_version=result.delta_result.version if result.delta_result else None,
    )
    file_count = delta_report_file_count(
        result.delta_result.report if result.delta_result is not None else None
    )
    record_cache_inventory(
        runtime_profile,
        entry=CacheInventoryEntry(
            view_name=registration.node.name,
            cache_policy=registration.node.cache_policy,
            cache_path=target_path,
            result="write",
            plan_fingerprint=plan_fingerprint,
            plan_identity_hash=plan_identity_hash,
            schema_identity_hash=registration.schema_hash,
            snapshot_version=result.delta_result.version if result.delta_result else None,
            snapshot_timestamp=None,
            row_count=result.rows_written,
            file_count=file_count,
            partition_by=partition_by,
        ),
        ctx=registration.ctx,
    )
    _record_cache_artifact(
        registration.cache,
        node=registration.node,
        cache_path=target_path,
        status="cached",
        hit=False,
    )
    return registration.ctx.table(registration.node.name)


def _register_uncached_view(registration: CacheRegistrationContext) -> DataFrame:
    registration.adapter.register_view(
        registration.node.name,
        registration.df,
        overwrite=registration.cache.options.overwrite,
        temporary=registration.cache.options.temporary,
    )
    return registration.df


def _require_runtime_profile(
    cache: ViewCacheContextLike,
    *,
    policy_label: str,
) -> DataFusionRuntimeProfile:
    profile = cache.runtime.runtime_profile
    if profile is None:
        msg = f"{policy_label} requires a runtime profile."
        raise ValueError(msg)
    return profile


def _plan_identifiers(node: ViewNodeLike) -> tuple[str | None, str | None]:
    plan_bundle = node.plan_bundle
    if plan_bundle is None:
        return None, None
    return plan_bundle.plan_fingerprint, plan_bundle.plan_identity_hash


def _delta_staging_path(
    runtime_profile: DataFusionRuntimeProfile,
    node: ViewNodeLike,
) -> str:
    cache_root = Path(runtime_profile.io_ops.cache_root()) / "view_cache"
    cache_root.mkdir(parents=True, exist_ok=True)
    if node.plan_bundle is not None and node.plan_bundle.plan_identity_hash is not None:
        fingerprint = node.plan_bundle.plan_identity_hash
    elif node.plan_bundle is not None and node.plan_bundle.plan_fingerprint is not None:
        fingerprint = node.plan_bundle.plan_fingerprint
    else:
        fingerprint = uuid7_hex()
    safe_name = node.name.replace("/", "_").replace(":", "_")
    return str(cache_root / f"{safe_name}__{fingerprint}")


def _cache_partition_by(
    schema: pa.Schema,
    *,
    location: DatasetLocation | None,
) -> tuple[str, ...]:
    policy_partition_by: tuple[str, ...] = ()
    if location is not None:
        policy = location.delta_write_policy
        if policy is not None:
            policy_partition_by = tuple(str(name) for name in policy.partition_by)
    available = set(schema.names)
    if policy_partition_by:
        missing = [name for name in policy_partition_by if name not in available]
        if missing:
            msg = f"Delta partition_by columns missing from schema: {sorted(missing)}."
            raise ValueError(msg)
    candidates = policy_partition_by or _default_partition_candidates(schema)
    return tuple(name for name in candidates if name in available)


def _default_partition_candidates(schema: pa.Schema) -> tuple[str, ...]:
    preferred = ("repo", "path_prefix", "path")
    names = set(schema.names)
    return tuple(name for name in preferred if name in names)


def _allow_schema_evolution(location: DatasetLocation) -> bool:
    policy = location.delta_schema_policy
    schema_mode = getattr(policy, "schema_mode", None) if policy is not None else None
    if isinstance(schema_mode, str):
        normalized = schema_mode.strip().lower()
        return normalized in {"merge", "overwrite"}
    return False


__all__ = [
    "CacheRegistrationContext",
    "register_delta_output_cache",
    "register_delta_staging_cache",
    "register_uncached_view",
    "register_view_with_cache",
]

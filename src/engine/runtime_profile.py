"""Runtime profile presets and helpers."""

from __future__ import annotations

import os
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, cast

import msgspec
import pyarrow as pa

from arrowdsl.core.determinism import DeterminismTier
from arrowdsl.core.runtime_profiles import RuntimeProfile, ScanProfile, runtime_profile_factory
from arrowdsl.schema.serialization import schema_to_msgpack
from engine.function_registry import FunctionRegistryOptions, build_function_registry
from serde_msgspec import dumps_msgpack, to_builtins
from sqlglot_tools.optimizer import sqlglot_policy_snapshot
from storage.ipc import payload_hash

if TYPE_CHECKING:
    from datafusion_engine.runtime import DataFusionRuntimeProfile
    from sqlglot_tools.optimizer import SqlGlotPolicySnapshot


def _cpu_count() -> int:
    count = os.cpu_count()
    return count if count is not None and count > 0 else 1


def _settings_int(value: str | None) -> int | None:
    if not value:
        return None
    try:
        return int(value)
    except ValueError:
        return None


def _env_value(name: str) -> str | None:
    value = os.environ.get(name, "").strip()
    return value if value else None


@dataclass(frozen=True)
class RuntimeProfileSpec:
    """Runtime profile plus compiler/engine preferences."""

    name: str
    runtime: RuntimeProfile
    ibis_fuse_selects: bool
    ibis_default_limit: int | None
    ibis_default_dialect: str | None
    ibis_interactive: bool | None

    @property
    def datafusion_settings_hash(self) -> str | None:
        """Return DataFusion settings hash when configured."""
        if self.runtime.datafusion is None:
            return None
        return self.runtime.datafusion.settings_hash()

    def runtime_profile_snapshot(self) -> RuntimeProfileSnapshot:
        """Return a unified runtime profile snapshot.

        Returns
        -------
        RuntimeProfileSnapshot
            Snapshot combining runtime, compiler, and engine policies.
        """
        return runtime_profile_snapshot(self.runtime)

    @property
    def runtime_profile_hash(self) -> str:
        """Return a stable hash of the unified runtime profile.

        Returns
        -------
        str
            Hash for the runtime profile snapshot.
        """
        return self.runtime_profile_snapshot().profile_hash


@dataclass(frozen=True)
class RuntimeProfileSnapshot:
    """Unified runtime profile snapshot for reproducibility."""

    version: int
    name: str
    determinism_tier: str
    scan_profile: dict[str, object]
    plan_use_threads: bool
    ibis_options: dict[str, object]
    arrow_resources: dict[str, object]
    sqlglot_policy: dict[str, object] | None
    datafusion: dict[str, object] | None
    function_registry_hash: str
    profile_hash: str
    scan_profile_schema_msgpack: bytes | None = None

    def payload(self) -> dict[str, object]:
        """Return the snapshot payload for serialization.

        Returns
        -------
        dict[str, object]
            Serialized payload for the runtime profile snapshot.
        """
        return {
            "version": self.version,
            "name": self.name,
            "determinism_tier": self.determinism_tier,
            "scan_profile": self.scan_profile,
            "plan_use_threads": self.plan_use_threads,
            "ibis_options": self.ibis_options,
            "arrow_resources": self.arrow_resources,
            "sqlglot_policy": self.sqlglot_policy,
            "datafusion": self.datafusion,
            "function_registry_hash": self.function_registry_hash,
            "profile_hash": self.profile_hash,
            "scan_profile_schema_msgpack": self.scan_profile_schema_msgpack,
        }


PROFILE_HASH_VERSION: int = 1
_PARQUET_READ_SCHEMA = pa.struct(
    [
        pa.field("dictionary_columns", pa.list_(pa.string())),
        pa.field("coerce_int96_timestamp_unit", pa.string()),
        pa.field("binary_type", pa.string()),
        pa.field("list_type", pa.string()),
    ]
)
_PARQUET_FRAGMENT_SCAN_SCHEMA = pa.struct(
    [
        pa.field("buffer_size", pa.int64()),
        pa.field("pre_buffer", pa.bool_()),
        pa.field("use_buffered_stream", pa.bool_()),
        pa.field("page_checksum_verification", pa.bool_()),
        pa.field("thrift_string_size_limit", pa.int64()),
        pa.field("thrift_container_size_limit", pa.int64()),
        pa.field("arrow_extensions_enabled", pa.bool_()),
    ]
)
_SCAN_PROFILE_SCHEMA = pa.struct(
    [
        pa.field("name", pa.string()),
        pa.field("batch_size", pa.int64()),
        pa.field("batch_readahead", pa.int64()),
        pa.field("fragment_readahead", pa.int64()),
        pa.field("fragment_scan_options", pa.map_(pa.string(), pa.string())),
        pa.field("parquet_read_options", _PARQUET_READ_SCHEMA),
        pa.field("parquet_fragment_scan_options", _PARQUET_FRAGMENT_SCAN_SCHEMA),
        pa.field("cache_metadata", pa.bool_()),
        pa.field("use_threads", pa.bool_()),
        pa.field("require_sequenced_output", pa.bool_()),
        pa.field("implicit_ordering", pa.bool_()),
        pa.field("scan_provenance_columns", pa.list_(pa.string())),
    ]
)
_IBIS_OPTIONS_SCHEMA = pa.struct(
    [
        pa.field("fuse_selects", pa.bool_()),
        pa.field("default_limit", pa.int64()),
        pa.field("default_dialect", pa.string()),
        pa.field("interactive", pa.bool_()),
    ]
)
_ARROW_RESOURCES_SCHEMA = pa.struct(
    [
        pa.field("pyarrow_version", pa.string()),
        pa.field("cpu_threads", pa.int64()),
        pa.field("io_threads", pa.int64()),
        pa.field("memory_pool", pa.string()),
        pa.field("bytes_allocated", pa.int64()),
        pa.field("max_memory", pa.int64()),
    ]
)
_PROFILE_HASH_SCHEMA = pa.schema(
    [
        pa.field("version", pa.int32()),
        pa.field("name", pa.string()),
        pa.field("determinism_tier", pa.string()),
        pa.field("scan_profile", _SCAN_PROFILE_SCHEMA),
        pa.field("plan_use_threads", pa.bool_()),
        pa.field("ibis_options", _IBIS_OPTIONS_SCHEMA),
        pa.field("arrow_resources", _ARROW_RESOURCES_SCHEMA),
        pa.field("sqlglot_policy_hash", pa.string()),
        pa.field("datafusion_hash", pa.string()),
        pa.field("function_registry_hash", pa.string()),
    ]
)


@dataclass(frozen=True)
class _RegistryContext:
    session: object | None
    function_catalog: Sequence[Mapping[str, object]] | None
    registry_snapshot: Mapping[str, object] | None


@dataclass(frozen=True)
class _RuntimePayloads:
    scan_payload: dict[str, object]
    ibis_payload: dict[str, object]
    arrow_payload: dict[str, object]
    sqlglot_snapshot: SqlGlotPolicySnapshot | None
    function_registry_hash: str


def runtime_profile_snapshot(runtime: RuntimeProfile) -> RuntimeProfileSnapshot:
    """Return a unified runtime profile snapshot.

    Returns
    -------
    RuntimeProfileSnapshot
        Snapshot combining runtime, compiler, and engine policies.
    """
    scan_payload = dict(_scan_profile_payload(runtime.scan))
    arrow_payload = dict(runtime.arrow_resource_snapshot().to_payload())
    ibis_payload = dict(runtime.ibis_options_payload())
    sqlglot_snapshot = sqlglot_policy_snapshot()
    registry_context = _build_registry_context(runtime)
    payloads = _RuntimePayloads(
        scan_payload=scan_payload,
        ibis_payload=ibis_payload,
        arrow_payload=arrow_payload,
        sqlglot_snapshot=sqlglot_snapshot,
        function_registry_hash=_function_registry_hash(registry_context),
    )
    snapshot_payload = _runtime_snapshot_payload(runtime, payloads)
    hash_payload = _runtime_hash_payload(runtime, payloads)
    profile_hash = payload_hash(hash_payload, _PROFILE_HASH_SCHEMA)
    sqlglot_policy = snapshot_payload["sqlglot_policy"]
    datafusion_payload = snapshot_payload["datafusion"]
    return RuntimeProfileSnapshot(
        version=1,
        name=runtime.name,
        determinism_tier=runtime.determinism.value,
        scan_profile=payloads.scan_payload,
        plan_use_threads=runtime.plan_use_threads,
        ibis_options=payloads.ibis_payload,
        arrow_resources=payloads.arrow_payload,
        sqlglot_policy=cast("dict[str, object] | None", sqlglot_policy),
        datafusion=cast("dict[str, object] | None", datafusion_payload),
        function_registry_hash=payloads.function_registry_hash,
        profile_hash=profile_hash,
        scan_profile_schema_msgpack=schema_to_msgpack(
            pa.schema([pa.field("scan_profile", _SCAN_PROFILE_SCHEMA)])
        ),
    )


def _build_registry_context(runtime: RuntimeProfile) -> _RegistryContext:
    function_catalog = None
    session = None
    if runtime.datafusion is not None and runtime.datafusion.enable_information_schema:
        try:
            from datafusion_engine.runtime import function_catalog_snapshot_for_profile

            session = runtime.datafusion.session_context()
            function_catalog = function_catalog_snapshot_for_profile(
                runtime.datafusion,
                session,
                include_routines=True,
            )
        except (RuntimeError, TypeError, ValueError):
            function_catalog = None
    registry_snapshot = None
    if session is not None:
        from datafusion_engine.udf_runtime import register_rust_udfs

        enable_async = False
        async_timeout_ms = None
        async_batch_size = None
        if runtime.datafusion is not None:
            enable_async = runtime.datafusion.enable_async_udfs
            if enable_async:
                async_timeout_ms = runtime.datafusion.async_udf_timeout_ms
                async_batch_size = runtime.datafusion.async_udf_batch_size
        registry_snapshot = register_rust_udfs(
            session,
            enable_async=enable_async,
            async_udf_timeout_ms=async_timeout_ms,
            async_udf_batch_size=async_batch_size,
        )
    return _RegistryContext(
        session=session,
        function_catalog=function_catalog,
        registry_snapshot=registry_snapshot,
    )


def _function_registry_hash(context: _RegistryContext) -> str:
    options = FunctionRegistryOptions(
        datafusion_function_catalog=context.function_catalog,
        registry_snapshot=context.registry_snapshot,
    )
    function_registry = build_function_registry(options=options)
    return function_registry.fingerprint()


def _runtime_snapshot_payload(
    runtime: RuntimeProfile,
    payloads: _RuntimePayloads,
) -> dict[str, object]:
    return {
        "name": runtime.name,
        "determinism_tier": runtime.determinism.value,
        "scan_profile": payloads.scan_payload,
        "plan_use_threads": runtime.plan_use_threads,
        "ibis_options": payloads.ibis_payload,
        "arrow_resources": payloads.arrow_payload,
        "sqlglot_policy": payloads.sqlglot_snapshot.payload()
        if payloads.sqlglot_snapshot is not None
        else None,
        "datafusion": runtime.datafusion.telemetry_payload_v1()
        if runtime.datafusion is not None
        else None,
        "function_registry_hash": payloads.function_registry_hash,
    }


def _runtime_hash_payload(
    runtime: RuntimeProfile,
    payloads: _RuntimePayloads,
) -> dict[str, object]:
    return {
        "version": PROFILE_HASH_VERSION,
        "name": runtime.name,
        "determinism_tier": runtime.determinism.value,
        "scan_profile": payloads.scan_payload,
        "plan_use_threads": runtime.plan_use_threads,
        "ibis_options": payloads.ibis_payload,
        "arrow_resources": payloads.arrow_payload,
        "sqlglot_policy_hash": payloads.sqlglot_snapshot.policy_hash
        if payloads.sqlglot_snapshot is not None
        else None,
        "datafusion_hash": runtime.datafusion.telemetry_payload_hash()
        if runtime.datafusion is not None
        else None,
        "function_registry_hash": payloads.function_registry_hash,
    }


def _function_catalog_snapshot(
    runtime: RuntimeProfile,
) -> Sequence[Mapping[str, object]] | None:
    profile = runtime.datafusion
    if profile is None or not profile.enable_information_schema:
        return None
    try:
        from datafusion_engine.runtime import function_catalog_snapshot_for_profile

        session = profile.session_context()
        return function_catalog_snapshot_for_profile(
            profile,
            session,
            include_routines=True,
        )
    except (RuntimeError, TypeError, ValueError):
        return None


def engine_runtime_artifact(runtime: RuntimeProfile) -> dict[str, object]:
    """Return an engine runtime artifact payload for diagnostics.

    Returns
    -------
    dict[str, object]
        Diagnostics payload for engine runtime settings.
    """
    snapshot = runtime_profile_snapshot(runtime)
    policy_snapshot = sqlglot_policy_snapshot()
    function_catalog = _function_catalog_snapshot(runtime)
    session = None
    if runtime.datafusion is not None and runtime.datafusion.enable_information_schema:
        try:
            session = runtime.datafusion.session_context()
        except (RuntimeError, TypeError, ValueError):
            session = None
    registry_snapshot = None
    if session is not None:
        from datafusion_engine.udf_runtime import register_rust_udfs

        enable_async = False
        async_timeout_ms = None
        async_batch_size = None
        if runtime.datafusion is not None:
            enable_async = runtime.datafusion.enable_async_udfs
            if enable_async:
                async_timeout_ms = runtime.datafusion.async_udf_timeout_ms
                async_batch_size = runtime.datafusion.async_udf_batch_size
        registry_snapshot = register_rust_udfs(
            session,
            enable_async=enable_async,
            async_udf_timeout_ms=async_timeout_ms,
            async_udf_batch_size=async_batch_size,
        )
    function_registry = build_function_registry(
        options=FunctionRegistryOptions(
            datafusion_function_catalog=function_catalog or [],
            registry_snapshot=registry_snapshot,
        )
    )
    datafusion_settings = (
        runtime.datafusion.settings_payload() if runtime.datafusion is not None else None
    )
    return {
        "event_time_unix_ms": int(time.time() * 1000),
        "runtime_profile_name": runtime.name,
        "determinism_tier": runtime.determinism.value,
        "runtime_profile_hash": snapshot.profile_hash,
        "runtime_profile_snapshot": dumps_msgpack(snapshot.payload()),
        "sqlglot_policy_hash": (
            policy_snapshot.policy_hash if policy_snapshot is not None else None
        ),
        "sqlglot_policy_snapshot": (
            dumps_msgpack(policy_snapshot.payload()) if policy_snapshot is not None else None
        ),
        "function_registry_hash": function_registry.fingerprint(),
        "function_registry_snapshot": dumps_msgpack(function_registry.payload()),
        "datafusion_settings_hash": (
            runtime.datafusion.settings_hash() if runtime.datafusion is not None else None
        ),
        "datafusion_settings": (
            dumps_msgpack(datafusion_settings) if datafusion_settings is not None else None
        ),
    }


def _scan_profile_payload(scan: ScanProfile) -> dict[str, object]:
    """Return a scan profile payload for runtime snapshots.

    Returns
    -------
    dict[str, object]
        Serialized scan profile payload.
    """
    return {
        "name": scan.name,
        "batch_size": scan.batch_size,
        "batch_readahead": scan.batch_readahead,
        "fragment_readahead": scan.fragment_readahead,
        "fragment_scan_options": _fragment_scan_options(scan.fragment_scan_options),
        "parquet_read_options": scan.parquet_read_payload(),
        "parquet_fragment_scan_options": scan.parquet_fragment_scan_payload(),
        "cache_metadata": scan.cache_metadata,
        "use_threads": scan.use_threads,
        "require_sequenced_output": scan.require_sequenced_output,
        "implicit_ordering": scan.implicit_ordering,
        "scan_provenance_columns": list(scan.scan_provenance_columns),
    }


def _fragment_scan_options(options: object | None) -> dict[str, str] | None:
    if options is None:
        return None
    if isinstance(options, Mapping):
        return {str(key): str(value) for key, value in options.items()}
    try:
        payload = to_builtins(options)
    except (msgspec.EncodeError, TypeError):
        payload = None
    if isinstance(payload, Mapping):
        return {str(key): str(value) for key, value in payload.items()}
    return {"value": str(options)}


def _apply_named_profile_overrides(
    name: str,
    runtime: RuntimeProfile,
    df_profile: DataFusionRuntimeProfile,
) -> tuple[RuntimeProfile, DataFusionRuntimeProfile]:
    cpu_count = _cpu_count()
    if name == "dev_debug":
        runtime = replace(
            runtime,
            cpu_threads=min(cpu_count, 4),
            io_threads=min(cpu_count * 2, 8),
        )
        df_profile = replace(
            df_profile,
            config_policy_name="dev",
            target_partitions=min(cpu_count, 8),
            batch_size=4096,
            capture_explain=True,
            explain_analyze=True,
            explain_analyze_level="dev",
        )
    elif name == "prod_fast":
        runtime = replace(
            runtime,
            cpu_threads=cpu_count,
            io_threads=cpu_count * 2,
        )
        df_profile = replace(
            df_profile,
            config_policy_name="prod",
            capture_explain=False,
            explain_analyze=False,
            explain_analyze_level=None,
        )
    elif name == "memory_tight":
        runtime = replace(
            runtime,
            cpu_threads=min(cpu_count, 2),
            io_threads=min(cpu_count, 4),
        )
        df_profile = replace(
            df_profile,
            config_policy_name="symtable",
            target_partitions=min(cpu_count, 4),
            batch_size=4096,
            capture_explain=False,
            explain_analyze=False,
            explain_analyze_level="summary",
        )
    return runtime, df_profile


def _apply_memory_overrides(
    name: str,
    df_profile: DataFusionRuntimeProfile,
    settings: Mapping[str, str],
) -> DataFusionRuntimeProfile:
    if name == "dev_debug":
        return df_profile
    spill_dir = df_profile.spill_dir or settings.get("datafusion.runtime.temp_directory")
    memory_limit = df_profile.memory_limit_bytes or _settings_int(
        settings.get("datafusion.runtime.memory_limit")
    )
    memory_pool = df_profile.memory_pool
    if memory_limit is not None and memory_pool == "greedy":
        memory_pool = "fair"
    return replace(
        df_profile,
        spill_dir=spill_dir,
        memory_limit_bytes=memory_limit,
        memory_pool=memory_pool,
    )


def _apply_env_overrides(df_profile: DataFusionRuntimeProfile) -> DataFusionRuntimeProfile:
    policy_override = _env_value("CODEANATOMY_DATAFUSION_POLICY")
    if policy_override is not None:
        df_profile = replace(df_profile, config_policy_name=policy_override)
    catalog_location = _env_value("CODEANATOMY_DATAFUSION_CATALOG_LOCATION")
    if catalog_location is not None:
        df_profile = replace(df_profile, catalog_auto_load_location=catalog_location)
    catalog_format = _env_value("CODEANATOMY_DATAFUSION_CATALOG_FORMAT")
    if catalog_format is not None:
        df_profile = replace(df_profile, catalog_auto_load_format=catalog_format)
    return df_profile


def _apply_profile_overrides(name: str, runtime: RuntimeProfile) -> RuntimeProfile:
    df_profile = runtime.datafusion
    if df_profile is None:
        return runtime
    settings = df_profile.settings_payload()
    runtime, df_profile = _apply_named_profile_overrides(name, runtime, df_profile)
    df_profile = _apply_memory_overrides(name, df_profile, settings)
    df_profile = _apply_env_overrides(df_profile)
    return runtime.with_datafusion(df_profile)


def resolve_runtime_profile(
    profile: str,
    *,
    determinism: DeterminismTier | None = None,
) -> RuntimeProfileSpec:
    """Return a runtime profile spec for the requested profile name.

    Returns
    -------
    RuntimeProfileSpec
        Resolved runtime profile spec.
    """
    runtime = runtime_profile_factory(profile)
    if determinism is not None:
        runtime = runtime.with_determinism(determinism)
    runtime = _apply_profile_overrides(profile, runtime)
    return RuntimeProfileSpec(
        name=profile,
        runtime=runtime,
        ibis_fuse_selects=runtime.ibis_fuse_selects,
        ibis_default_limit=runtime.ibis_default_limit,
        ibis_default_dialect=runtime.ibis_default_dialect,
        ibis_interactive=runtime.ibis_interactive,
    )


__all__ = [
    "RuntimeProfileSnapshot",
    "RuntimeProfileSpec",
    "engine_runtime_artifact",
    "resolve_runtime_profile",
    "runtime_profile_snapshot",
]

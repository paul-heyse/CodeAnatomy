"""Session runtime build and management for DataFusion execution."""

from __future__ import annotations

import logging
import time
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, cast

import pyarrow as pa
from datafusion import DataFrame, SessionContext, SQLOptions

from datafusion_engine.schema.introspection_core import SchemaIntrospector
from datafusion_engine.session._session_caches import (
    RUNTIME_SETTINGS_OVERLAY,
    SESSION_RUNTIME_CACHE,
)
from datafusion_engine.sql.options import planning_sql_options, sql_options_for_profile
from datafusion_engine.views.artifacts import DataFusionViewArtifact
from storage.ipc_utils import payload_hash
from utils.registry_protocol import Registry

if TYPE_CHECKING:
    from datafusion_engine.catalog.introspection import IntrospectionCache
    from datafusion_engine.dataset.registry import DatasetLocation
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile

from datafusion_engine.session.runtime_telemetry import (
    _SESSION_RUNTIME_HASH_SCHEMA,
)

_logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SessionRuntime:
    """Authoritative runtime surface for planning and execution."""

    ctx: SessionContext
    profile: DataFusionRuntimeProfile
    udf_snapshot_hash: str
    udf_rewrite_tags: tuple[str, ...]
    domain_planner_names: tuple[str, ...]
    udf_snapshot: Mapping[str, object]
    df_settings: Mapping[str, str]


_SESSION_RUNTIME_HASH_VERSION = 1


def _settings_rows_to_mapping(rows: Sequence[Mapping[str, object]]) -> dict[str, str]:
    """Build a name/value settings mapping from introspection rows.

    Parameters
    ----------
    rows
        Settings rows from information_schema snapshots.

    Returns:
    -------
    dict[str, str]
        Mapping of setting names to stringified values.
    """
    mapping: dict[str, str] = {}
    for row in rows:
        name = row.get("name") or row.get("setting_name") or row.get("key")
        if name is None:
            continue
        value = row.get("value")
        mapping[str(name)] = "" if value is None else str(value)
    return mapping


def _empty_udf_snapshot_payload() -> dict[str, object]:
    empty_names: tuple[str, ...] = ()
    empty_mapping: dict[str, object] = {}
    return {
        "scalar": empty_names,
        "aggregate": empty_names,
        "window": empty_names,
        "table": empty_names,
        "aliases": dict(empty_mapping),
        "parameter_names": dict(empty_mapping),
        "volatility": dict(empty_mapping),
        "simplify": dict(empty_mapping),
        "coerce_types": dict(empty_mapping),
        "short_circuits": dict(empty_mapping),
        "signature_inputs": dict(empty_mapping),
        "return_types": dict(empty_mapping),
        "config_defaults": dict(empty_mapping),
    }


def _build_session_runtime_from_context(
    ctx: SessionContext,
    *,
    profile: DataFusionRuntimeProfile,
) -> SessionRuntime:
    from datafusion_engine.expr.domain_planner import domain_planner_names_from_snapshot
    from datafusion_engine.udf.extension_core import rust_udf_snapshot, rust_udf_snapshot_hash
    from datafusion_engine.udf.metadata import rewrite_tag_index

    try:
        snapshot = rust_udf_snapshot(ctx, registries=profile.udf_extension_registries)
    except (RuntimeError, TypeError, ValueError):
        snapshot = _empty_udf_snapshot_payload()
    snapshot_hash = rust_udf_snapshot_hash(snapshot)
    tag_index = rewrite_tag_index(snapshot)
    rewrite_tags = tuple(sorted(tag_index))
    planner_names = domain_planner_names_from_snapshot(snapshot)
    df_settings: Mapping[str, str]
    try:
        settings_table = settings_snapshot_for_profile(profile, ctx)
        df_settings = _settings_rows_to_mapping(settings_table.to_pylist())
    except (RuntimeError, TypeError, ValueError):
        df_settings = {}
    return SessionRuntime(
        ctx=ctx,
        profile=profile,
        udf_snapshot_hash=snapshot_hash,
        udf_rewrite_tags=rewrite_tags,
        domain_planner_names=planner_names,
        udf_snapshot=snapshot,
        df_settings=df_settings,
    )


def _settings_row_name(row: Mapping[str, object]) -> str | None:
    value = row.get("name") or row.get("setting_name") or row.get("key")
    if value is None:
        return None
    return str(value)


def _settings_name_key(schema: pa.Schema) -> str:
    for name in ("name", "setting_name", "key"):
        if name in schema.names:
            return name
    return "name"


def _settings_value_key(schema: pa.Schema) -> str:
    for name in ("value", "setting_value"):
        if name in schema.names:
            return name
    return "value"


def _settings_row_template(schema: pa.Schema) -> dict[str, object]:
    return {field.name: None for field in schema}


def _runtime_settings_from_profile(
    profile: DataFusionRuntimeProfile | None,
) -> dict[str, str]:
    if profile is None:
        return {}
    return {
        key: value
        for key, value in profile.settings_payload().items()
        if key.startswith("datafusion.runtime.")
    }


def record_runtime_setting_override(
    ctx: SessionContext,
    *,
    key: str,
    value: str,
) -> None:
    """Record runtime settings that DataFusion does not surface via SQL."""
    if not key.startswith("datafusion.runtime."):
        return
    overrides = RUNTIME_SETTINGS_OVERLAY.setdefault(ctx, {})
    overrides[key] = value


def runtime_setting_overrides(ctx: SessionContext) -> Mapping[str, str]:
    """Return recorded runtime setting overrides for a SessionContext.

    Returns:
    -------
    Mapping[str, str]
        Runtime setting overrides keyed by setting name.
    """
    overrides = RUNTIME_SETTINGS_OVERLAY.get(ctx)
    return dict(overrides) if overrides else {}


def _merge_runtime_settings_rows(
    rows: list[dict[str, object]],
    *,
    schema: pa.Schema,
    profile: DataFusionRuntimeProfile | None,
    ctx: SessionContext,
) -> list[dict[str, object]]:
    name_key = _settings_name_key(schema)
    value_key = _settings_value_key(schema)
    name_to_row = {name: row for row in rows if (name := _settings_row_name(row)) is not None}
    defaults = _runtime_settings_from_profile(profile)
    for key, value in defaults.items():
        if key in name_to_row:
            continue
        row = _settings_row_template(schema)
        row[name_key] = key
        row[value_key] = value
        rows.append(row)
        name_to_row[key] = row
    overrides = runtime_setting_overrides(ctx)
    for key, value in overrides.items():
        row = name_to_row.get(key)
        if row is None:
            row = _settings_row_template(schema)
            rows.append(row)
        row[name_key] = key
        row[value_key] = value
        name_to_row[key] = row
    return rows


def build_session_runtime(
    profile: DataFusionRuntimeProfile, *, use_cache: bool = True
) -> SessionRuntime:
    """Build and cache a planning-ready SessionRuntime for a profile.

    Parameters
    ----------
    profile
        DataFusion runtime profile to materialize.
    use_cache
        When ``True``, cache the runtime by the profile cache key.

    Returns:
    -------
    SessionRuntime
        Planning-ready runtime with UDF identity and settings snapshots.
    """
    cache_key = profile.context_cache_key()
    cached_obj = SESSION_RUNTIME_CACHE.get(cache_key)
    cached = cached_obj if isinstance(cached_obj, SessionRuntime) else None
    ctx = profile.session_context()
    # Guard against cache-key collisions across profile variants.
    if cached is not None and use_cache and cached.profile == profile and cached.ctx is ctx:
        return cached
    from datafusion_engine.expr.domain_planner import domain_planner_names_from_snapshot
    from datafusion_engine.udf.extension_core import rust_udf_snapshot, rust_udf_snapshot_hash
    from datafusion_engine.udf.metadata import rewrite_tag_index

    snapshot = rust_udf_snapshot(ctx, registries=profile.udf_extension_registries)
    snapshot_hash = rust_udf_snapshot_hash(snapshot)
    tag_index = rewrite_tag_index(snapshot)
    rewrite_tags = tuple(sorted(tag_index))
    planner_names = domain_planner_names_from_snapshot(snapshot)
    df_settings: Mapping[str, str]
    try:
        settings_table = settings_snapshot_for_profile(profile, ctx)
        df_settings = _settings_rows_to_mapping(settings_table.to_pylist())
    except (RuntimeError, TypeError, ValueError):
        df_settings = {}
    runtime = SessionRuntime(
        ctx=ctx,
        profile=profile,
        udf_snapshot_hash=snapshot_hash,
        udf_rewrite_tags=rewrite_tags,
        domain_planner_names=planner_names,
        udf_snapshot=snapshot,
        df_settings=df_settings,
    )
    if use_cache:
        SESSION_RUNTIME_CACHE[cache_key] = runtime
    return runtime


def refresh_session_runtime(
    profile: DataFusionRuntimeProfile,
    *,
    ctx: SessionContext | None = None,
) -> SessionRuntime:
    """Rebuild and cache a SessionRuntime for the current SessionContext.

    Parameters
    ----------
    profile : DataFusionRuntimeProfile
        Runtime profile to refresh.
    ctx : SessionContext | None
        Optional context to rebuild for.

    Returns:
    -------
    SessionRuntime
        Refreshed session runtime.

    Raises:
        TypeError: If the profile does not expose a session runtime builder.
    """
    resolved_ctx = ctx or profile.session_context()
    runtime_builder = cast(
        "Callable[[SessionContext], SessionRuntime] | None",
        getattr(profile, "_session_runtime_from_context", None),
    )
    if not callable(runtime_builder):
        msg = "DataFusionRuntimeProfile does not expose a session runtime builder."
        raise TypeError(msg)
    runtime = runtime_builder(resolved_ctx)
    SESSION_RUNTIME_CACHE[profile.context_cache_key()] = runtime
    return runtime


def _session_runtime_entries(mapping: Mapping[str, str]) -> list[dict[str, str]]:
    entries: list[dict[str, str]] = []
    for key, value in sorted(mapping.items(), key=lambda item: item[0]):
        entries.append(
            {
                "key": str(key),
                "value_kind": type(value).__name__,
                "value": str(value),
            }
        )
    return entries


def session_runtime_hash(runtime: SessionRuntime) -> str:
    """Return a stable hash for session runtime identity.

    Parameters
    ----------
    runtime
        Planning-ready session runtime snapshot.

    Returns:
    -------
    str
        Stable identity hash for runtime-sensitive plan signatures.
    """
    profile_context_key = runtime.profile.context_cache_key()
    profile_settings_hash = runtime.profile.settings_hash()
    df_entries = _session_runtime_entries(runtime.df_settings)
    payload = {
        "version": _SESSION_RUNTIME_HASH_VERSION,
        "profile_context_key": profile_context_key,
        "profile_settings_hash": profile_settings_hash,
        "udf_snapshot_hash": runtime.udf_snapshot_hash,
        "udf_rewrite_tags": list(runtime.udf_rewrite_tags),
        "domain_planner_names": list(runtime.domain_planner_names),
        "df_settings_entries": df_entries,
    }
    return payload_hash(payload, _SESSION_RUNTIME_HASH_SCHEMA)


def session_runtime_for_context(
    profile: DataFusionRuntimeProfile,
    ctx: SessionContext,
) -> SessionRuntime | None:
    """Return a SessionRuntime for the provided context when compatible.

    Returns:
    -------
    SessionRuntime | None
        Session runtime for the context when compatible, otherwise ``None``.
    """
    try:
        return _build_session_runtime_from_context(ctx, profile=profile)
    except (RuntimeError, TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# SQL helpers
# ---------------------------------------------------------------------------


def _read_only_sql_options() -> SQLOptions:
    return planning_sql_options(None)


def _is_sql_options_type_mismatch(exc: TypeError) -> bool:
    message = str(exc)
    return "cannot be converted to 'SQLOptions'" in message


def _sql_with_options(
    ctx: SessionContext,
    sql: str,
    *,
    sql_options: SQLOptions | None = None,
    allow_statements: bool | None = None,
) -> DataFrame:

    resolved_sql_options = sql_options or _read_only_sql_options()
    if allow_statements:
        allow_statements_flag = True
        resolved_sql_options = resolved_sql_options.with_allow_statements(allow_statements_flag)
    try:
        df = ctx.sql_with_options(sql, resolved_sql_options)
    except TypeError as exc:
        if not _is_sql_options_type_mismatch(exc):
            msg = "Runtime SQL execution did not return a DataFusion DataFrame."
            raise ValueError(msg) from exc
        try:
            df = ctx.sql(sql)
        except (RuntimeError, TypeError, ValueError) as fallback_exc:
            msg = "Runtime SQL execution did not return a DataFusion DataFrame."
            raise ValueError(msg) from fallback_exc
    except (RuntimeError, ValueError) as exc:
        msg = "Runtime SQL execution did not return a DataFusion DataFrame."
        raise ValueError(msg) from exc
    if df is None:
        msg = "Runtime SQL execution did not return a DataFusion DataFrame."
        raise ValueError(msg)
    return df


# ---------------------------------------------------------------------------
# Snapshot functions
# ---------------------------------------------------------------------------


def _introspection_cache_for_ctx(
    ctx: SessionContext,
    *,
    sql_options: SQLOptions | None,
) -> IntrospectionCache:
    from datafusion_engine.catalog.introspection import introspection_cache_for_ctx

    return introspection_cache_for_ctx(ctx, sql_options=sql_options)


def settings_snapshot_for_profile(
    profile: DataFusionRuntimeProfile, ctx: SessionContext
) -> pa.Table:
    """Return a DataFusion settings snapshot for a runtime profile.

    Returns:
    -------
    pyarrow.Table
        Table of settings from information_schema.df_settings.
    """
    cache = _introspection_cache_for_ctx(ctx, sql_options=profile.sql_options())
    base_table = cache.snapshot.settings
    rows = [dict(row) for row in base_table.to_pylist()]
    rows = _merge_runtime_settings_rows(
        rows,
        schema=base_table.schema,
        profile=profile,
        ctx=ctx,
    )
    rows = sorted(rows, key=lambda row: _settings_row_name(row) or "")
    return pa.Table.from_pylist(rows, schema=base_table.schema)


def catalog_snapshot_for_profile(
    profile: DataFusionRuntimeProfile, ctx: SessionContext
) -> pa.Table:
    """Return a DataFusion catalog snapshot for a runtime profile.

    Returns:
    -------
    pyarrow.Table
        Table inventory from information_schema.tables.
    """
    cache = _introspection_cache_for_ctx(ctx, sql_options=profile.sql_options())
    return cache.snapshot.tables


def function_catalog_snapshot_for_profile(
    profile: DataFusionRuntimeProfile,
    ctx: SessionContext,
    *,
    include_routines: bool = False,
) -> list[dict[str, object]]:
    """Return a function catalog snapshot for a runtime profile.

    Returns:
    -------
    list[dict[str, object]]
        Sorted function catalog entries from information_schema.
    """
    from datafusion_engine.session.introspection import schema_introspector_for_profile

    return schema_introspector_for_profile(profile, ctx).function_catalog_snapshot(
        include_parameters=include_routines,
    )


def record_view_definition(
    profile: DataFusionRuntimeProfile,
    *,
    artifact: DataFusionViewArtifact,
) -> None:
    """Record a view artifact for diagnostics snapshots.

    Parameters
    ----------
    profile
        Runtime profile for recording diagnostics.
    artifact
        View artifact payload for diagnostics.
    """
    from datafusion_engine.session.runtime_compile import record_artifact
    from serde_artifact_specs import DATAFUSION_VIEW_ARTIFACTS_SPEC

    if profile.view_registry is None:
        return
    profile.view_registry.record(name=artifact.name, artifact=artifact)
    payload = artifact.diagnostics_payload(event_time_unix_ms=int(time.time() * 1000))
    record_artifact(profile, DATAFUSION_VIEW_ARTIFACTS_SPEC, payload)


# ---------------------------------------------------------------------------
# DataFusion version helpers
# ---------------------------------------------------------------------------


def _datafusion_version(ctx: SessionContext) -> str | None:
    try:
        table = _sql_with_options(ctx, "SELECT version() AS version").to_arrow_table()
    except (RuntimeError, TypeError, ValueError):
        return None
    if "version" not in table.column_names or table.num_rows < 1:
        return None
    values = table["version"].to_pylist()
    value = values[0] if values else None
    return str(value) if value is not None else None


def _datafusion_function_names(ctx: SessionContext) -> set[str]:
    try:
        names = SchemaIntrospector(ctx, sql_options=sql_options_for_profile(None)).function_names()
    except (RuntimeError, TypeError, ValueError):
        return set()
    return {name.lower() for name in names}


# ---------------------------------------------------------------------------
# SCIP registration snapshot
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _ScipRegistrationSnapshot:
    name: str
    location: DatasetLocation
    expected_fingerprint: str | None
    actual_fingerprint: str | None
    schema_match: bool | None


# ---------------------------------------------------------------------------
# DataFusion view registry
# ---------------------------------------------------------------------------


@dataclass
class DataFusionViewRegistry(Registry[str, DataFusionViewArtifact]):
    """Record DataFusion view artifacts for reproducibility."""

    entries: dict[str, DataFusionViewArtifact] = field(default_factory=dict)

    def register(self, key: str, value: DataFusionViewArtifact) -> None:
        """Register a view artifact by name.

        Parameters
        ----------
        key
            View name.
        value
            View artifact payload for the registry.
        """
        self.entries[key] = value

    def record(self, *, name: str, artifact: DataFusionViewArtifact) -> None:
        """Record a view artifact by name.

        Parameters
        ----------
        name
            View name.
        artifact
            View artifact payload for the registry.
        """
        self.register(name, artifact)

    def get(self, key: str) -> DataFusionViewArtifact | None:
        """Return a view artifact when present.

        Parameters
        ----------
        key
            View name.

        Returns:
        -------
        DataFusionViewArtifact | None
            View artifact when registered, otherwise ``None``.
        """
        return self.entries.get(key)

    def __contains__(self, key: str) -> bool:
        """Return True when a view artifact is registered.

        Parameters
        ----------
        key
            View name.

        Returns:
        -------
        bool
            ``True`` when the view is registered.
        """
        return key in self.entries

    def __iter__(self) -> Iterator[str]:
        """Iterate over registered view names.

        Returns:
        -------
        Iterator[str]
            Iterator of registered view names.
        """
        return iter(self.entries)

    def __len__(self) -> int:
        """Return the number of registered views.

        Returns:
        -------
        int
            Count of registered views.
        """
        return len(self.entries)

    def snapshot(self) -> list[dict[str, object]]:
        """Return a stable snapshot of registered view artifacts.

        Returns:
        -------
        list[dict[str, object]]
            Snapshot payloads for registered view artifacts.
        """
        return [
            artifact.payload()
            for _, artifact in sorted(self.entries.items(), key=lambda item: item[0])
        ]

    def diagnostics_snapshot(self, *, event_time_unix_ms: int) -> list[dict[str, object]]:
        """Return diagnostics payloads for registered view artifacts.

        Parameters
        ----------
        event_time_unix_ms
            Event timestamp to attach to each payload.

        Returns:
        -------
        list[dict[str, object]]
            Diagnostics-ready payloads for registered view artifacts.
        """
        return [
            artifact.diagnostics_payload(event_time_unix_ms=event_time_unix_ms)
            for _, artifact in sorted(self.entries.items(), key=lambda item: item[0])
        ]


__all__ = [
    "DataFusionViewRegistry",
    "SessionRuntime",
    "build_session_runtime",
    "catalog_snapshot_for_profile",
    "function_catalog_snapshot_for_profile",
    "record_runtime_setting_override",
    "record_view_definition",
    "refresh_session_runtime",
    "runtime_setting_overrides",
    "session_runtime_for_context",
    "session_runtime_hash",
    "settings_snapshot_for_profile",
]

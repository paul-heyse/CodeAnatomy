"""Dataset I/O helpers for DataFusion runtime."""

from __future__ import annotations

import logging
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import TYPE_CHECKING, cast
from urllib.parse import urlparse

import msgspec
import pyarrow as pa
from datafusion import DataFrame, SessionContext, col, lit
from datafusion.expr import Expr

from datafusion_engine.arrow.coercion import to_arrow_table
from datafusion_engine.arrow.interop import (
    RecordBatchReaderLike,
    SchemaLike,
    TableLike,
)
from datafusion_engine.dataset.registry import DatasetLocation
from datafusion_engine.expr.cast import safe_cast
from datafusion_engine.schema.introspection_routines import _introspection_cache_for_ctx
from datafusion_engine.session.helpers import deregister_table, register_temp_table
from datafusion_engine.sql.options import planning_sql_options
from datafusion_engine.tables.metadata import table_provider_metadata
from schema_spec.dataset_spec import (
    DatasetSpec,
    DeltaScanOptions,
    dataset_spec_from_schema,
)
from storage.cdf_cursor_protocol import CdfCursorStoreLike
from utils.value_coercion import coerce_int

if TYPE_CHECKING:
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile
    from semantics.program_manifest import ManifestDatasetResolver, SemanticProgramManifest

logger = logging.getLogger(__name__)

__all__ = [
    "_align_projection_exprs",
    "_align_table_with_arrow",
    "_apply_readiness_status",
    "_apply_table_schema_metadata",
    "_base_readiness_payload",
    "_cache_config_payload",
    "_cache_snapshot_rows",
    "_capture_cache_diagnostics",
    "_count_dir_entries",
    "_datafusion_type_name",
    "_dataset_readiness_payload",
    "_delta_readiness_payload",
    "_extract_output_config_for_profile",
    "_file_readiness_payload",
    "_introspection_cache_for_ctx",
    "_is_remote_scheme",
    "_local_readiness_payload",
    "_normalize_dataset_locations_for_root",
    "_register_cache_introspection_functions",
    "_remote_readiness_payload",
    "_resolve_local_path",
    "_schema_has_extension",
    "_schema_with_table_metadata",
    "_semantic_output_config_for_profile",
    "_type_has_extension",
    "align_table_to_schema",
    "assert_schema_metadata",
    "cache_prefix_for_delta_snapshot",
    "dataset_schema_from_context",
    "dataset_spec_from_context",
    "datasource_config_from_manifest",
    "datasource_config_from_profile",
    "extract_output_locations_for_profile",
    "normalize_dataset_locations_for_profile",
    "read_delta_as_reader",
    "record_dataset_readiness",
    "semantic_output_locations_for_profile",
]


# ---------------------------------------------------------------------------
# Config structures (imported from runtime_profile_config)
# ---------------------------------------------------------------------------
from datafusion_engine.session.runtime_profile_config import (
    DataSourceConfig,
    ExtractOutputConfig,
    SemanticOutputConfig,
)

# ---------------------------------------------------------------------------
# Schema alignment functions
# ---------------------------------------------------------------------------


def _apply_table_schema_metadata(
    table: pa.Table,
    *,
    schema: pa.Schema,
    keep_extra_columns: bool,
) -> pa.Table:
    if not keep_extra_columns:
        return table.cast(schema)
    metadata = dict(table.schema.metadata or {})
    metadata.update(schema.metadata or {})
    fields: list[pa.Field] = []
    for table_field in table.schema:
        try:
            expected = schema.field(table_field.name)
        except KeyError:
            fields.append(table_field)
            continue
        fields.append(
            pa.field(
                table_field.name,
                table_field.type,
                table_field.nullable,
                metadata=expected.metadata,
            )
        )
    return table.cast(pa.schema(fields, metadata=metadata))


def _align_projection_exprs(
    *,
    schema: pa.Schema,
    input_columns: Sequence[str],
    keep_extra_columns: bool,
    safe_cast_mode: bool = True,
) -> list[Expr]:
    caster = safe_cast if safe_cast_mode else lambda expr, dtype: expr.cast(dtype)
    selections: list[Expr] = []
    for schema_field in schema:
        col_name = schema_field.name
        if schema_field.name in input_columns:
            selections.append(caster(col(col_name), schema_field.type).alias(col_name))
        else:
            selections.append(caster(lit(None), schema_field.type).alias(col_name))
    if keep_extra_columns:
        for name in input_columns:
            if name in schema.names:
                continue
            selections.append(col(name))
    return selections


def align_table_to_schema(
    table: TableLike | RecordBatchReaderLike,
    *,
    schema: SchemaLike,
    keep_extra_columns: bool = False,
    safe_cast_mode: bool = True,
    ctx: SessionContext | None = None,
) -> pa.Table:
    """Align a table to a target schema using DataFusion casts.

    Returns:
    -------
    pyarrow.Table
        Table aligned to the provided schema.
    """
    resolved_schema = pa.schema(schema)
    resolved_table = to_arrow_table(table)
    if _schema_has_extension(resolved_schema):
        return _align_table_with_arrow(
            resolved_table,
            schema=resolved_schema,
            keep_extra_columns=keep_extra_columns,
        )
    session = ctx or SessionContext()
    temp_name = register_temp_table(session, resolved_table, prefix="__schema_align_")
    try:
        selections = _align_projection_exprs(
            schema=resolved_schema,
            input_columns=resolved_table.column_names,
            keep_extra_columns=keep_extra_columns,
            safe_cast_mode=safe_cast_mode,
        )
        aligned = session.table(temp_name).select(*selections).to_arrow_table()
    finally:
        deregister_table(session, temp_name)
    return _apply_table_schema_metadata(
        aligned,
        schema=resolved_schema,
        keep_extra_columns=keep_extra_columns,
    )


def _schema_has_extension(schema: pa.Schema) -> bool:
    return any(_type_has_extension(field.type) for field in schema)


def _type_has_extension(data_type: pa.DataType) -> bool:
    if isinstance(data_type, pa.ExtensionType):
        return True
    if pa.types.is_struct(data_type):
        return any(_type_has_extension(field.type) for field in data_type)
    if pa.types.is_list(data_type) or pa.types.is_large_list(data_type):
        return _type_has_extension(data_type.value_field.type)
    if pa.types.is_map(data_type):
        return _type_has_extension(data_type.key_field.type) or _type_has_extension(
            data_type.item_field.type
        )
    if pa.types.is_union(data_type):
        return any(_type_has_extension(field.type) for field in data_type)
    return False


def _align_table_with_arrow(
    table: pa.Table,
    *,
    schema: pa.Schema,
    keep_extra_columns: bool,
) -> pa.Table:
    arrays: list[pa.Array | pa.ChunkedArray] = []
    fields: list[pa.Field] = []
    num_rows = int(table.num_rows)
    table_fields = {field.name: field for field in table.schema}
    for schema_field in schema:
        if schema_field.name in table.column_names:
            column = table[schema_field.name]
            if column.type != schema_field.type:
                column = column.cast(schema_field.type)
            arrays.append(column)
        else:
            arrays.append(pa.nulls(num_rows, type=schema_field.type))
        fields.append(schema_field)
    if keep_extra_columns:
        for name in table.column_names:
            if name in schema.names:
                continue
            arrays.append(table[name])
            fields.append(table_fields[name])
    resolved_schema = pa.schema(fields, metadata=schema.metadata)
    return pa.Table.from_arrays(arrays, schema=resolved_schema)


def assert_schema_metadata(
    table: TableLike | RecordBatchReaderLike,
    *,
    schema: SchemaLike,
) -> None:
    """Raise when schema metadata does not match the target schema.

    Args:
        table: Description.
        schema: Description.

    Raises:
        ValueError: If the operation cannot be completed.
    """
    table_schema = pa.schema(table.schema)
    expected_schema = pa.schema(schema)
    if not table_schema.equals(expected_schema, check_metadata=True):
        msg = "Schema metadata mismatch after finalize."
        raise ValueError(msg)


# ---------------------------------------------------------------------------
# Dataset schema functions
# ---------------------------------------------------------------------------


def dataset_schema_from_context(
    name: str,
    *,
    ctx: SessionContext | None = None,
) -> SchemaLike:
    """Return the dataset schema from the DataFusion SessionContext.

    Args:
        name: Description.
        ctx: Description.

    Raises:
        KeyError: If the operation cannot be completed.
    """
    session_ctx = ctx or SessionContext()
    try:
        schema = session_ctx.table(name).schema()
    except (KeyError, RuntimeError, TypeError, ValueError) as exc:
        msg = f"Dataset schema not registered in DataFusion: {name!r}."
        raise KeyError(msg) from exc
    metadata = table_provider_metadata(session_ctx, table_name=name)
    if metadata is None or not metadata.metadata:
        return schema
    return _schema_with_table_metadata(schema, metadata=metadata.metadata)


def _schema_with_table_metadata(
    schema: SchemaLike,
    *,
    metadata: Mapping[str, str],
) -> SchemaLike:
    if not metadata:
        return schema
    if isinstance(schema, pa.Schema):
        merged = dict(schema.metadata or {})
        for key, value in metadata.items():
            merged.setdefault(key.encode("utf-8"), str(value).encode("utf-8"))
        return schema.with_metadata(merged)
    to_pyarrow = getattr(schema, "to_pyarrow", None)
    if callable(to_pyarrow):
        resolved = to_pyarrow()
        if isinstance(resolved, pa.Schema):
            resolved_schema = cast("pa.Schema", resolved)
            merged = dict(resolved_schema.metadata or {})
            for key, value in metadata.items():
                merged.setdefault(key.encode("utf-8"), str(value).encode("utf-8"))
            return resolved_schema.with_metadata(merged)
    return schema


def read_delta_as_reader(
    path: str,
    *,
    storage_options: Mapping[str, str] | None = None,
    log_storage_options: Mapping[str, str] | None = None,
    delta_scan: DeltaScanOptions | None = None,
) -> pa.RecordBatchReader:
    """Return a streaming Delta table snapshot using the Delta TableProvider.

    Returns:
    -------
    pyarrow.RecordBatchReader
        Streaming reader for the Delta table via DataFusion's Delta table provider.
    """
    if delta_scan is None:
        delta_scan = DeltaScanOptions(schema_force_view_types=False)
    elif delta_scan.schema_force_view_types is None:
        delta_scan = msgspec.structs.replace(delta_scan, schema_force_view_types=False)
    ctx = SessionContext()
    from datafusion_engine.dataset.registry import DatasetLocation, DatasetLocationOverrides
    from datafusion_engine.dataset.resolution import (
        DatasetResolutionRequest,
        resolve_dataset_provider,
    )

    overrides = None
    if delta_scan is not None:
        from schema_spec.dataset_spec import DeltaPolicyBundle

        overrides = DatasetLocationOverrides(delta=DeltaPolicyBundle(scan=delta_scan))
    location = DatasetLocation(
        path=path,
        format="delta",
        storage_options=dict(storage_options or {}),
        delta_log_storage_options=dict(log_storage_options or {}),
        overrides=overrides,
    )
    resolution = resolve_dataset_provider(
        DatasetResolutionRequest(
            ctx=ctx,
            location=location,
            runtime_profile=None,
        )
    )
    from datafusion_engine.tables.metadata import TableProviderCapsule

    df = ctx.read_table(TableProviderCapsule(resolution.provider))
    batches = df.collect()
    if batches:
        table = pa.Table.from_batches(batches)
    else:
        schema = df.schema()
        table = pa.Table.from_batches([], schema=schema)
    if "__delta_rs_path" in table.column_names:
        table = table.drop(["__delta_rs_path"])
    return pa.RecordBatchReader.from_batches(table.schema, table.to_batches())


def dataset_spec_from_context(
    name: str,
    *,
    ctx: SessionContext | None = None,
) -> DatasetSpec:
    """Return a DatasetSpec derived from the DataFusion schema.

    Parameters
    ----------
    name : str
        Dataset name registered in the SessionContext.
    ctx : SessionContext | None
        Optional SessionContext override for schema resolution.

    Returns:
    -------
    DatasetSpec
        DatasetSpec derived from the DataFusion schema.
    """
    schema = dataset_schema_from_context(name, ctx=ctx)
    return dataset_spec_from_schema(name, schema)


# ---------------------------------------------------------------------------
# DataFusion type name helper
# ---------------------------------------------------------------------------


def _datafusion_type_name(dtype: pa.DataType) -> str:
    ctx = SessionContext()
    table = pa.Table.from_arrays([pa.array([None], type=dtype)], names=["value"])
    from datafusion_engine.io.adapter import DataFusionIOAdapter

    adapter = DataFusionIOAdapter(ctx=ctx, profile=None)
    adapter.register_table("t", ctx.from_arrow(table))
    result = _sql_with_options(
        ctx,
        "SELECT arrow_typeof(value) AS dtype FROM t LIMIT 1",
    ).to_arrow_table()
    value = result["dtype"][0].as_py()
    if not isinstance(value, str):
        msg = "Failed to resolve DataFusion type name."
        raise TypeError(msg)
    return value


def _sql_with_options(ctx: SessionContext, sql: str) -> DataFrame:
    """Execute SQL with read-only options.

    Returns:
    -------
    object
        DataFusion DataFrame result.

    Raises:
        ValueError: When SQL execution does not return a DataFrame.
    """
    sql_options = planning_sql_options(None)
    try:
        df = ctx.sql_with_options(sql, sql_options)
    except TypeError:
        df = ctx.sql(sql)
    if df is None:
        msg = "Runtime SQL execution did not return a DataFusion DataFrame."
        raise ValueError(msg)
    return df


# ---------------------------------------------------------------------------
# Dataset location functions
# ---------------------------------------------------------------------------


def normalize_dataset_locations_for_profile(
    profile: DataFusionRuntimeProfile,
) -> Mapping[str, DatasetLocation]:
    """Return normalize dataset locations derived from the output root.

    Returns:
    -------
    Mapping[str, DatasetLocation]
        Mapping of normalize dataset names to locations, or empty mapping
        when normalize output root is not configured.
    """
    normalize_root = profile.data_sources.semantic_output.normalize_output_root
    return _normalize_dataset_locations_for_root(normalize_root)


def _normalize_dataset_locations_for_root(
    normalize_root: str | None,
) -> Mapping[str, DatasetLocation]:
    """Return normalize dataset locations for an explicit normalize root."""
    if normalize_root is None:
        return {}
    root = Path(normalize_root)
    from schema_spec.dataset_spec import dataset_spec_name
    from semantics.catalog.dataset_specs import dataset_specs

    locations: dict[str, DatasetLocation] = {}
    for spec in dataset_specs():
        name = dataset_spec_name(spec)
        locations[name] = DatasetLocation(
            path=str(root / name),
            format="delta",
            dataset_spec=spec,
        )
    return locations


def extract_output_locations_for_profile(
    profile: DataFusionRuntimeProfile,
) -> Mapping[str, DatasetLocation]:
    """Return extract output dataset locations derived from the output root.

    Returns:
    -------
    Mapping[str, DatasetLocation]
        Mapping of extract dataset names to locations, or empty mapping
        when extract output root/catalog is not configured.
    """
    extract_output = profile.data_sources.extract_output
    if extract_output.dataset_locations:
        return extract_output.dataset_locations
    if extract_output.output_catalog_name is not None:
        catalog = profile.catalog.registry_catalogs.get(extract_output.output_catalog_name)
        if catalog is None:
            return {}
        return {name: catalog.get(name) for name in catalog.names()}
    if extract_output.output_root is None:
        return {}
    from datafusion_engine.extract.output_catalog import build_extract_output_catalog

    catalog = build_extract_output_catalog(output_root=extract_output.output_root)
    return {name: catalog.get(name) for name in catalog.names()}


def semantic_output_locations_for_profile(
    profile: DataFusionRuntimeProfile,
) -> Mapping[str, DatasetLocation]:
    """Return semantic output dataset locations derived from the output root.

    Returns:
    -------
    Mapping[str, DatasetLocation]
        Mapping of semantic output names to locations, or empty mapping
        when semantic output root is not configured and no explicit
        semantic output locations are provided.
    """
    from semantics.registry import SEMANTIC_MODEL

    view_names = [spec.name for spec in SEMANTIC_MODEL.outputs]
    semantic_output = profile.data_sources.semantic_output
    if semantic_output.locations:
        return semantic_output.locations
    if semantic_output.output_catalog_name is not None:
        catalog = profile.catalog.registry_catalogs.get(semantic_output.output_catalog_name)
        if catalog is None:
            return {}
        locations: dict[str, DatasetLocation] = {}
        for name in view_names:
            if catalog.has(name):
                locations[name] = catalog.get(name)
        return locations
    if semantic_output.output_root is None:
        return {}
    root = Path(semantic_output.output_root)
    locations: dict[str, DatasetLocation] = {}
    for name in view_names:
        locations[name] = DatasetLocation(
            path=str(root / name),
            format="delta",
        )
    return locations


def _extract_output_config_for_profile(
    runtime_profile: DataFusionRuntimeProfile,
) -> ExtractOutputConfig:
    """Return the extract output config from the profile's data_sources."""
    return runtime_profile.data_sources.extract_output


def _semantic_output_config_for_profile(
    runtime_profile: DataFusionRuntimeProfile,
) -> SemanticOutputConfig:
    """Return the semantic output config from the profile's data_sources."""
    return runtime_profile.data_sources.semantic_output


def datasource_config_from_manifest(
    manifest: SemanticProgramManifest,
    *,
    runtime_profile: DataFusionRuntimeProfile,
    extract_output: ExtractOutputConfig | None = None,
    semantic_output: SemanticOutputConfig | None = None,
    cdf_cursor_store: CdfCursorStoreLike | None = None,
) -> DataSourceConfig:
    """Build DataSourceConfig in post-compile semantic-authority mode.

    Fail closed when semantic compile bindings are empty.

    Returns:
        DataSourceConfig: Manifest-authoritative data source configuration.

    Raises:
        ValueError: If manifest dataset bindings are empty.
    """
    locations = dict(manifest.dataset_bindings.locations)
    if not locations:
        msg = (
            "Manifest dataset bindings are empty; cannot build DataSourceConfig "
            "in semantic-authority mode."
        )
        raise ValueError(msg)
    resolved_extract_output = (
        extract_output
        if extract_output is not None
        else _extract_output_config_for_profile(runtime_profile)
    )
    resolved_semantic_output = (
        semantic_output
        if semantic_output is not None
        else _semantic_output_config_for_profile(runtime_profile)
    )
    resolved_cdf_cursor_store = (
        cdf_cursor_store if cdf_cursor_store is not None else runtime_profile.cdf_cursor_store()
    )
    return DataSourceConfig(
        dataset_templates=locations,
        extract_output=resolved_extract_output,
        semantic_output=resolved_semantic_output,
        cdf_cursor_store=resolved_cdf_cursor_store,
    )


def datasource_config_from_profile(
    runtime_profile: DataFusionRuntimeProfile,
    *,
    extract_output: ExtractOutputConfig | None = None,
    semantic_output: SemanticOutputConfig | None = None,
    dataset_templates: Mapping[str, DatasetLocation] | None = None,
    cdf_cursor_store: CdfCursorStoreLike | None = None,
) -> DataSourceConfig:
    """Build DataSourceConfig in pre-compile runtime-bootstrap mode.

    Returns:
    -------
    DataSourceConfig
        Bootstrap data source configuration derived from runtime profile.
    """
    resolved_extract_output = (
        extract_output
        if extract_output is not None
        else _extract_output_config_for_profile(runtime_profile)
    )
    resolved_semantic_output = (
        semantic_output
        if semantic_output is not None
        else _semantic_output_config_for_profile(runtime_profile)
    )
    if dataset_templates is None:
        derived_templates = _normalize_dataset_locations_for_root(
            resolved_semantic_output.normalize_output_root
        )
        merged_templates = dict(runtime_profile.data_sources.dataset_templates)
        for name, location in derived_templates.items():
            merged_templates.setdefault(name, location)
    else:
        merged_templates = dict(dataset_templates)
    resolved_cdf_cursor_store = (
        cdf_cursor_store if cdf_cursor_store is not None else runtime_profile.cdf_cursor_store()
    )
    return DataSourceConfig(
        dataset_templates=merged_templates,
        extract_output=resolved_extract_output,
        semantic_output=resolved_semantic_output,
        cdf_cursor_store=resolved_cdf_cursor_store,
    )


# ---------------------------------------------------------------------------
# Readiness functions
# ---------------------------------------------------------------------------


def record_dataset_readiness(
    profile: DataFusionRuntimeProfile,
    *,
    dataset_names: Sequence[str],
    dataset_resolver: ManifestDatasetResolver,
) -> None:
    """Record readiness diagnostics for configured dataset locations."""
    from semantics.resolver_identity import record_resolver_if_tracking

    record_resolver_if_tracking(dataset_resolver, label="dataset_readiness")
    if profile.diagnostics_sink() is None:
        return
    from datafusion_engine.lineage.diagnostics import record_artifact
    from obs.otel import set_heartbeat_blockers
    from serde_artifact_specs import DATASET_READINESS_SPEC

    blockers: list[str] = []
    for name in dataset_names:
        location = dataset_resolver.location(name)
        if location is None:
            record_artifact(
                profile,
                DATASET_READINESS_SPEC,
                {
                    "dataset": name,
                    "status": "missing_location",
                    "reason": "no_dataset_location_configured",
                },
            )
            blockers.append(name)
            continue
        readiness = _dataset_readiness_payload(name, location)
        record_artifact(
            profile,
            DATASET_READINESS_SPEC,
            readiness,
        )
        status = readiness.get("status")
        if isinstance(status, str) and status not in {"ok", "remote_path"}:
            blockers.append(name)
    if blockers:
        set_heartbeat_blockers(sorted(set(blockers)))


def _dataset_readiness_payload(name: str, location: DatasetLocation) -> dict[str, object]:
    path_value = str(location.path)
    payload = _base_readiness_payload(name, location, path_value)
    parsed = urlparse(path_value)
    if _is_remote_scheme(parsed.scheme):
        return _remote_readiness_payload(payload)
    path = _resolve_local_path(parsed, path_value)
    return _local_readiness_payload(payload, path, location.format)


def _base_readiness_payload(
    name: str,
    location: DatasetLocation,
    path_value: str,
) -> dict[str, object]:
    return {
        "dataset": name,
        "path": path_value,
        "format": location.format,
        "status": "ok",
    }


def _is_remote_scheme(scheme: str) -> bool:
    return scheme not in {"", "file"}


def _remote_readiness_payload(payload: dict[str, object]) -> dict[str, object]:
    payload.update(
        {
            "status": "remote_path",
            "reason": "remote_scheme",
            "path_exists": None,
            "delta_log_present": None,
        }
    )
    return payload


def _resolve_local_path(parsed: object, path_value: str) -> Path:
    scheme = getattr(parsed, "scheme", "")
    parsed_path = getattr(parsed, "path", "")
    if scheme == "file" and parsed_path:
        return Path(parsed_path)
    return Path(path_value)


def _local_readiness_payload(
    payload: dict[str, object],
    path: Path,
    format_name: str,
) -> dict[str, object]:
    exists = path.exists()
    payload["path_exists"] = exists
    if not exists:
        return _apply_readiness_status(payload, "missing_path", "path_not_found")
    if format_name == "delta":
        return _delta_readiness_payload(payload, path)
    return _file_readiness_payload(payload, path)


def _delta_readiness_payload(payload: dict[str, object], path: Path) -> dict[str, object]:
    delta_log = path / "_delta_log"
    delta_exists = delta_log.exists()
    payload["delta_log_present"] = delta_exists
    if not delta_exists:
        return _apply_readiness_status(payload, "missing_delta_log", "delta_log_missing")
    delta_count = _count_dir_entries(delta_log)
    payload["delta_log_file_count"] = delta_count
    if delta_count == 0:
        return _apply_readiness_status(payload, "empty_delta_log", "delta_log_empty")
    return payload


def _file_readiness_payload(payload: dict[str, object], path: Path) -> dict[str, object]:
    payload["delta_log_present"] = None
    if path.is_dir():
        file_count = _count_dir_entries(path)
        payload["file_count"] = file_count
        if file_count == 0:
            return _apply_readiness_status(payload, "empty_path", "no_files_found")
        return payload
    payload["file_count"] = 1
    return payload


def _count_dir_entries(path: Path) -> int:
    try:
        return sum(1 for _ in path.iterdir())
    except OSError:
        return 0


def _apply_readiness_status(
    payload: dict[str, object],
    status: str,
    reason: str,
) -> dict[str, object]:
    payload["status"] = status
    payload["reason"] = reason
    return payload


# ---------------------------------------------------------------------------
# Cache diagnostics helpers
# ---------------------------------------------------------------------------


def _capture_cache_diagnostics(ctx: SessionContext) -> Mapping[str, object]:
    from datafusion_engine.catalog.introspection import capture_cache_diagnostics

    return capture_cache_diagnostics(ctx)


def _register_cache_introspection_functions(ctx: SessionContext) -> None:
    from datafusion_engine.catalog.introspection import register_cache_introspection_functions

    register_cache_introspection_functions(ctx)


def _cache_config_payload(cache_diag: Mapping[str, object]) -> Mapping[str, object]:
    payload = cache_diag.get("config")
    if isinstance(payload, Mapping):
        return payload
    return {}


def _cache_snapshot_rows(cache_diag: Mapping[str, object]) -> list[Mapping[str, object]]:
    payload = cache_diag.get("cache_snapshots")
    if not isinstance(payload, Sequence):
        return []
    rows: list[Mapping[str, object]] = []
    rows.extend(snapshot for snapshot in payload if isinstance(snapshot, Mapping))
    return rows


# ---------------------------------------------------------------------------
# Cache prefix for Delta snapshot
# ---------------------------------------------------------------------------


def cache_prefix_for_delta_snapshot(
    profile: DataFusionRuntimeProfile,
    *,
    dataset_name: str,
    snapshot: Mapping[str, object] | None = None,
    delta_version: int | None = None,
    delta_timestamp: str | int | None = None,
) -> str | None:
    """Return a cache prefix that pins caches to a Delta snapshot.

    Returns:
    -------
    str | None
        Cache prefix when snapshot identifiers are available; otherwise ``None``.
    """
    version = coerce_int(snapshot.get("version")) if snapshot is not None else None
    if version is None:
        version = delta_version
    timestamp_value = None
    if snapshot is not None:
        timestamp_value = coerce_int(snapshot.get("snapshot_timestamp"))
        if timestamp_value is None:
            timestamp_value = snapshot.get("timestamp")
    if timestamp_value is None and delta_timestamp is not None:
        timestamp_value = delta_timestamp
    if version is None and timestamp_value is None:
        return None
    suffix = version if version is not None else timestamp_value
    return f"{profile.context_cache_key()}::{dataset_name}::{suffix}"

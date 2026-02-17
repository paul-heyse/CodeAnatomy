"""Dataset registration surfaces for DataFusion SessionContext.

This module provides dataset registration utilities that bind datasets to
DataFusion's catalog via native registration APIs and DDL surfaces. All IO
contracts are specified through DataFusion registration parameters and options.

Registration surfaces:
- register_object_store(): Object store routing (S3, GCS, Azure, etc.)
- CREATE EXTERNAL TABLE: DDL-based registration with schema and options
- register_table(): TableProvider registration with custom capsules

Schema discovery and validation query DataFusion's catalog and information_schema
views after registration, ensuring DataFusion is the source of truth for all
table metadata.

Schema drift and evolution:
Schema adapters are attached at registration time via the runtime profile's
physical expression adapter factory. When `enable_schema_evolution_adapter=True`
in the runtime profile, scan-time adapters handle schema drift resolution at
the TableProvider boundary, eliminating the need for downstream cast/projection
transforms. This ensures schema normalization happens during physical plan
execution rather than in post-processing.

Write routing:
All write operations should use `WritePipeline` from `datafusion_engine.write_pipeline`,
which provides DataFusion-native write surfaces (write_parquet, write_csv, write_json,
write_table, and streaming Delta writes via provider inserts).
"""
# NOTE(size-exception): This module is temporarily >800 LOC during hard-cutover
# decomposition. Remaining extraction and contraction work is tracked in
# docs/plans/src_design_improvements_implementation_plan_v1_2026-02-16.md.

from __future__ import annotations

import importlib
import json
import logging
import re
import time
from collections.abc import Callable, Mapping, Sequence
from contextlib import suppress
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import TYPE_CHECKING, Literal
from urllib.parse import urlparse
from weakref import WeakKeyDictionary

import msgspec
import pyarrow as pa
import pyarrow.fs as pafs
from datafusion import SessionContext, SQLOptions, col
from datafusion.dataframe import DataFrame
from datafusion.expr import Expr

from arrow_utils.core.ordering import OrderingLevel
from arrow_utils.core.schema_constants import DEFAULT_VALUE_META
from core.config_base import FingerprintableConfig, config_fingerprint
from core_types import ensure_path
from datafusion_engine.arrow.abi import schema_to_dict
from datafusion_engine.arrow.interop import SchemaLike, arrow_schema_from_df
from datafusion_engine.arrow.metadata import (
    ordering_from_schema,
    schema_constraints_from_metadata,
)
from datafusion_engine.catalog.introspection import (
    invalidate_introspection_cache,
)
from datafusion_engine.dataset.registry import (
    DatasetCatalog,
    DatasetLocation,
    DatasetLocationOverrides,
    resolve_dataset_schema,
)
from datafusion_engine.dataset.resolution import (
    DatasetResolution,
)
from datafusion_engine.delta.capabilities import is_delta_extension_compatible
from datafusion_engine.delta.protocol import (
    combined_table_features,
    delta_protocol_compatibility,
)
from datafusion_engine.delta.provider_artifacts import (
    RegistrationProviderArtifactInput,
    build_delta_provider_build_result,
    delta_scan_snapshot_payload,
    provider_build_request_from_registration_context,
)
from datafusion_engine.errors import DataFusionEngineError, ErrorKind
from datafusion_engine.identity import schema_identity_hash
from datafusion_engine.io.adapter import DataFusionIOAdapter
from datafusion_engine.lineage.diagnostics import record_artifact
from datafusion_engine.lineage.reporting import referenced_tables_from_plan
from datafusion_engine.plan.bundle_artifact import PlanBundleOptions, build_plan_artifact
from datafusion_engine.schema.contracts import (
    table_constraint_definitions,
    table_constraints_from_location,
)
from datafusion_engine.schema.introspection_core import (
    SchemaIntrospector,
    table_constraint_rows,
)
from datafusion_engine.schema.validation import _datafusion_type_name
from datafusion_engine.session.introspection import schema_introspector_for_profile
from datafusion_engine.session.runtime_config_policies import (
    DATAFUSION_MAJOR_VERSION,
    DATAFUSION_RUNTIME_SETTINGS_SKIP_VERSION,
)
from datafusion_engine.session.runtime_dataset_io import cache_prefix_for_delta_snapshot
from datafusion_engine.session.runtime_session import record_runtime_setting_override
from datafusion_engine.sql import options as _sql_options
from datafusion_engine.tables.metadata import (
    TableProviderCapsule,
    TableProviderMetadata,
    record_table_provider_metadata,
    table_provider_metadata,
)
from obs.otel import get_run_id
from schema_spec.dataset_spec import (
    DataFusionScanOptions,
    DatasetSpec,
    DeltaScanOptions,
    ParquetColumnOptions,
    TableSchemaContract,
    dataset_spec_from_schema,
    ddl_fingerprint_from_definition,
    make_dataset_spec,
)
from storage.deltalake import DeltaCdfOptions
from utils.validation import validate_required_items
from utils.value_coercion import coerce_int

if TYPE_CHECKING:
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile
    from semantics.program_manifest import ManifestDatasetResolver

DEFAULT_CACHE_MAX_COLUMNS = 64
_INPUT_PLUGIN_PREFIXES = ("artifact://", "dataset://", "repo://")
_DDL_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_CST_EXTERNAL_TABLE_NAME = "libcst_files_v1"
_CST_PARTITION_FIELDS: tuple[str, ...] = ("repo",)
_CST_FILE_SORT_ORDER: tuple[tuple[str, str], ...] = (
    ("path", "ascending"),
    ("file_id", "ascending"),
)
_CST_PROJECTION_EXPRS: tuple[str, ...] = (
    "repo",
    "path",
    "file_id",
    "nodes",
    "edges",
    "parse_manifest",
    "parse_errors",
    "refs",
    "imports",
    "callsites",
    "defs",
    "type_exprs",
    "docstrings",
    "decorators",
    "call_args",
    "attrs",
)
_CST_PARQUET_COLUMN_OPTIONS = ParquetColumnOptions(statistics_enabled=("file_id", "path", "repo"))
_AST_EXTERNAL_TABLE_NAME = "ast_files_v1"
_AST_PARTITION_FIELDS: tuple[str, ...] = ("repo", "path")
_AST_FILE_SORT_ORDER: tuple[tuple[str, str], ...] = (
    ("repo", "ascending"),
    ("path", "ascending"),
)
_SYMTABLE_EXTERNAL_TABLE_NAME = "symtable_files_v1"
_SYMTABLE_PARTITION_FIELDS: tuple[str, ...] = ("repo",)
_SYMTABLE_FILE_SORT_ORDER: tuple[tuple[str, str], ...] = (
    ("path", "ascending"),
    ("file_id", "ascending"),
)
_BYTECODE_EXTERNAL_TABLE_NAME = "bytecode_files_v1"
_BYTECODE_PARTITION_FIELDS: tuple[str, ...] = ("repo",)
_BYTECODE_FILE_SORT_ORDER: tuple[tuple[str, str], ...] = (
    ("path", "ascending"),
    ("file_id", "ascending"),
)
_TREE_SITTER_EXTERNAL_TABLE_NAME = "tree_sitter_files_v1"
_TREE_SITTER_PARTITION_FIELDS: tuple[str, ...] = ("repo",)
_TREE_SITTER_FILE_SORT_ORDER: tuple[tuple[str, str], ...] = (
    ("path", "ascending"),
    ("file_id", "ascending"),
)

_DDL_TYPE_ALIASES: dict[str, str] = {
    "Int8": "TINYINT",
    "Int16": "SMALLINT",
    "Int32": "INT",
    "Int64": "BIGINT",
    "UInt8": "TINYINT",
    "UInt16": "SMALLINT",
    "UInt32": "INT",
    "UInt64": "BIGINT",
    "Float32": "FLOAT",
    "Float64": "DOUBLE",
    "Utf8": "VARCHAR",
    "LargeUtf8": "VARCHAR",
    "Boolean": "BOOLEAN",
}

logger = logging.getLogger(__name__)


def _sql_type_name(dtype: pa.DataType) -> str:
    dtype_name = _datafusion_type_name(dtype)
    return _DDL_TYPE_ALIASES.get(dtype_name, dtype_name)


try:
    from datafusion.input.base import BaseInputSource as _BaseInputSource
except ImportError:  # pragma: no cover - optional dependency
    _BaseInputSource = None
    _INPUT_PLUGIN_AVAILABLE = False
else:
    _INPUT_PLUGIN_AVAILABLE = True


class _DatasetInputSource:
    """Resolve dataset handles into registered tables."""

    def __init__(
        self,
        ctx: SessionContext,
        *,
        catalog: DatasetCatalog,
        runtime_profile: DataFusionRuntimeProfile | None,
    ) -> None:
        """Initialize the instance.

        Args:
            ctx: Description.
            catalog: Description.
            runtime_profile: Description.
        """
        self._ctx = ctx
        self._catalog = catalog
        self._runtime_profile = runtime_profile

    def is_correct_input(
        self,
        input_item: object,
        table_name: str,
        **kwargs: object,
    ) -> bool:
        """Return True when the input matches a dataset registry handle.

        Returns:
        -------
        bool
            ``True`` when the dataset handle is recognized.
        """
        _ = table_name, kwargs
        name = _dataset_name_from_input(input_item)
        return name is not None and self._catalog.has(name)

    def build_table(
        self,
        input_item: object,
        table_name: str,
        **kwargs: object,
    ) -> DataFrame:
        """Build a DataFusion DataFrame for a dataset registry handle.

        Args:
            input_item: Input source handle.
            table_name: Target table name to register.
            **kwargs: Additional plugin arguments (unused).

        Returns:
            DataFrame: Result.

        Raises:
            ValueError: If the input handle cannot be resolved.
        """
        _ = kwargs
        name = _dataset_name_from_input(input_item)
        if name is None:
            msg = f"Unsupported dataset handle: {input_item!r}."
            raise ValueError(msg)
        location = self._catalog.get(name)
        return register_dataset_df(
            self._ctx,
            name=table_name,
            location=location,
            runtime_profile=self._runtime_profile,
        )


def _dataset_name_from_input(value: object) -> str | None:
    handle = str(value)
    for prefix in _INPUT_PLUGIN_PREFIXES:
        if handle.startswith(prefix):
            name = handle.removeprefix(prefix)
            return name if name else None
    return None


def dataset_input_plugin(
    catalog: DatasetCatalog,
    *,
    runtime_profile: DataFusionRuntimeProfile | None = None,
) -> Callable[[SessionContext], None]:
    """Return a SessionContext installer for dataset input sources.

    Returns:
    -------
    Callable[[SessionContext], None]
        Installer that registers the dataset input source.
    """

    def _install(ctx: SessionContext) -> None:
        if not _INPUT_PLUGIN_AVAILABLE:
            return
        register = getattr(ctx, "register_input_source", None)
        if not callable(register):
            return
        register(
            _DatasetInputSource(
                ctx,
                catalog=catalog,
                runtime_profile=runtime_profile,
            )
        )

    return _install


def input_plugin_prefixes() -> tuple[str, ...]:
    """Return the registered input plugin prefixes.

    Returns:
    -------
    tuple[str, ...]
        Supported input plugin prefixes.
    """
    return _INPUT_PLUGIN_PREFIXES


@dataclass(frozen=True)
class DataFusionRegistryOptions:
    """Resolved DataFusion registration options for a dataset."""

    scan: DataFusionScanOptions | None
    schema: SchemaLike | None
    read_options: Mapping[str, object]
    cache: bool
    provider: Literal["listing", "delta_cdf"] | None


@dataclass(frozen=True)
class DeltaCdfArtifact:
    """Diagnostics payload for Delta CDF registration."""

    name: str
    path: str
    provider: str
    options: DeltaCdfOptions | None
    log_storage_options: Mapping[str, str] | None
    snapshot: Mapping[str, object] | None = None


@dataclass(frozen=True)
class DataFusionCachePolicy(FingerprintableConfig):
    """Cache policy overrides for DataFusion dataset registration."""

    enabled: bool | None = None
    max_columns: int | None = None
    storage: Literal["memory", "delta_staging"] = "memory"

    def fingerprint_payload(self) -> Mapping[str, object]:
        """Return fingerprint payload for cache policy overrides.

        Returns:
        -------
        Mapping[str, object]
            Payload describing cache policy overrides.
        """
        return {
            "enabled": self.enabled,
            "max_columns": self.max_columns,
            "storage": self.storage,
        }

    def fingerprint(self) -> str:
        """Return fingerprint for cache policy overrides.

        Returns:
        -------
        str
            Deterministic fingerprint for the cache policy.
        """
        return config_fingerprint(self.fingerprint_payload())


@dataclass(frozen=True)
class _DataFusionCacheSettings:
    """Resolved cache settings for DataFusion registration."""

    enabled: bool
    max_columns: int | None
    storage: Literal["memory", "delta_staging"]


@dataclass(frozen=True)
class DataFusionRegistrationContext:
    """Inputs needed to register a dataset with DataFusion."""

    ctx: SessionContext
    name: str
    location: DatasetLocation
    options: DataFusionRegistryOptions
    cache: _DataFusionCacheSettings
    caches: DatasetCaches
    runtime_profile: DataFusionRuntimeProfile | None = None


@dataclass
class DatasetCaches:
    """Injectable mutable cache container for dataset registration state."""

    cached_datasets: WeakKeyDictionary[SessionContext, set[str]] = field(
        default_factory=WeakKeyDictionary
    )
    registered_catalogs: WeakKeyDictionary[SessionContext, set[str]] = field(
        default_factory=WeakKeyDictionary
    )
    registered_schemas: WeakKeyDictionary[SessionContext, set[tuple[str, str]]] = field(
        default_factory=WeakKeyDictionary
    )


def _resolve_dataset_caches(caches: DatasetCaches | None) -> DatasetCaches:
    return caches or DatasetCaches()


@dataclass(frozen=True)
class _ScanDefaults:
    partition_fields: tuple[str, ...]
    file_sort_order: tuple[tuple[str, str], ...]
    infer_partitions: bool
    cache_ttl: str
    listing_mutable: bool
    projection_exprs: tuple[str, ...]
    parquet_column_options: ParquetColumnOptions | None
    collect_statistics: bool = False


_DEFAULT_SCAN_CONFIGS: dict[str, _ScanDefaults] = {
    _CST_EXTERNAL_TABLE_NAME: _ScanDefaults(
        partition_fields=_CST_PARTITION_FIELDS,
        file_sort_order=_CST_FILE_SORT_ORDER,
        infer_partitions=True,
        cache_ttl="2m",
        listing_mutable=True,
        projection_exprs=_CST_PROJECTION_EXPRS,
        parquet_column_options=_CST_PARQUET_COLUMN_OPTIONS,
    ),
    _AST_EXTERNAL_TABLE_NAME: _ScanDefaults(
        partition_fields=_AST_PARTITION_FIELDS,
        file_sort_order=_AST_FILE_SORT_ORDER,
        infer_partitions=True,
        cache_ttl="2m",
        listing_mutable=False,
        projection_exprs=(),
        parquet_column_options=None,
    ),
    _BYTECODE_EXTERNAL_TABLE_NAME: _ScanDefaults(
        partition_fields=_BYTECODE_PARTITION_FIELDS,
        file_sort_order=_BYTECODE_FILE_SORT_ORDER,
        infer_partitions=True,
        cache_ttl="5m",
        listing_mutable=False,
        projection_exprs=(),
        parquet_column_options=None,
    ),
    _TREE_SITTER_EXTERNAL_TABLE_NAME: _ScanDefaults(
        partition_fields=_TREE_SITTER_PARTITION_FIELDS,
        file_sort_order=_TREE_SITTER_FILE_SORT_ORDER,
        infer_partitions=False,
        cache_ttl="1m",
        listing_mutable=False,
        projection_exprs=(),
        parquet_column_options=None,
        collect_statistics=True,
    ),
    _SYMTABLE_EXTERNAL_TABLE_NAME: _ScanDefaults(
        partition_fields=_SYMTABLE_PARTITION_FIELDS,
        file_sort_order=_SYMTABLE_FILE_SORT_ORDER,
        infer_partitions=False,
        cache_ttl="1m",
        listing_mutable=False,
        projection_exprs=(),
        parquet_column_options=None,
    ),
}


def _schema_field_type(schema: SchemaLike, field: str) -> pa.DataType | None:
    """Return the Arrow type for a schema field, when available.

    Returns:
    -------
    pyarrow.DataType | None
        Field type when available.
    """
    if field not in schema.names:
        return None
    return schema.field(field).type


def _default_scan_options_for_dataset(
    name: str,
    *,
    schema: SchemaLike | None,
) -> DataFusionScanOptions | None:
    """Return default DataFusion scan options for supported datasets.

    Returns:
    -------
    DataFusionScanOptions | None
        Default scan options when the dataset is supported.
    """
    defaults = _DEFAULT_SCAN_CONFIGS.get(name)
    if defaults is None:
        return None
    if schema is None:
        return None
    partition_cols: list[tuple[str, pa.DataType]] = []
    for field_name in defaults.partition_fields:
        dtype = _schema_field_type(schema, field_name)
        if dtype is not None:
            partition_cols.append((field_name, dtype))
    file_sort_order = tuple(
        (field, order) for field, order in defaults.file_sort_order if field in schema.names
    )
    return DataFusionScanOptions(
        partition_cols=tuple(partition_cols),
        file_sort_order=file_sort_order,
        file_extension=None,
        parquet_pruning=True,
        skip_metadata=True,
        collect_statistics=defaults.collect_statistics,
        listing_table_factory_infer_partitions=defaults.infer_partitions,
        list_files_cache_limit=str(64 * 1024 * 1024),
        list_files_cache_ttl=defaults.cache_ttl,
        projection_exprs=defaults.projection_exprs,
        parquet_column_options=defaults.parquet_column_options,
        listing_mutable=defaults.listing_mutable,
    )


def _apply_scan_defaults(name: str, location: DatasetLocation) -> DatasetLocation:
    """Attach default scan options to a dataset location.

    Returns:
    -------
    DatasetLocation
        Dataset location with default scan options applied.
    """
    updated = location
    if location.resolved.datafusion_scan is not None:
        return updated
    schema: SchemaLike | None = None
    try:
        schema = resolve_dataset_schema(location)
    except (RuntimeError, TypeError, ValueError):
        schema = None
    defaults = _default_scan_options_for_dataset(name, schema=schema)
    if defaults is None:
        return updated
    overrides = updated.overrides
    if overrides is None:
        overrides = DatasetLocationOverrides(datafusion_scan=defaults)
    else:
        overrides = msgspec.structs.replace(overrides, datafusion_scan=defaults)
    return msgspec.structs.replace(updated, overrides=overrides)


def _resolve_registry_options_for_location(
    location: DatasetLocation,
) -> DataFusionRegistryOptions:
    """Resolve DataFusion registration hints for a dataset location.

    Args:
        location: Description.

    Returns:
        DataFusionRegistryOptions: Result.

    Raises:
        ValueError: If the operation cannot be completed.
    """
    resolved = location.resolved
    scan = resolved.datafusion_scan
    schema = resolved.schema
    provider = resolved.datafusion_provider
    format_name = location.format or "delta"
    cdf_policy = resolved.delta_cdf_policy
    if provider == "dataset":
        msg = "DataFusion dataset providers are not supported; use listing or native formats."
        raise ValueError(msg)
    if cdf_policy is not None and cdf_policy.required:
        if format_name != "delta":
            msg = "Delta CDF policy requires delta-format datasets."
            raise ValueError(msg)
        provider = "delta_cdf"
    if provider == "delta_cdf" and format_name != "delta":
        msg = "Delta CDF provider requires delta-format datasets."
        raise ValueError(msg)
    if provider is None and format_name == "delta" and location.delta_cdf_options is not None:
        provider = "delta_cdf"
    if provider is None and format_name != "delta":
        provider = "listing"
    if provider is None and _prefers_listing_table(location, scan=scan):
        provider = "listing"
    return DataFusionRegistryOptions(
        scan=scan,
        schema=schema,
        read_options=dict(location.read_options),
        cache=bool(scan.cache) if scan is not None else False,
        provider=provider,
    )


def _apply_runtime_scan_hardening(
    options: DataFusionRegistryOptions,
    *,
    runtime_profile: DataFusionRuntimeProfile | None,
) -> DataFusionRegistryOptions:
    if runtime_profile is None:
        return options
    hardened_scan = _scan_hardening_defaults(options.scan, runtime_profile=runtime_profile)
    updated = options
    if hardened_scan is not options.scan:
        updated = replace(updated, scan=hardened_scan)
    return updated


def _scan_hardening_defaults(
    scan: DataFusionScanOptions | None,
    *,
    runtime_profile: DataFusionRuntimeProfile,
) -> DataFusionScanOptions | None:
    if scan is None or scan.schema_force_view_types is not None:
        return scan
    enable_view_types = _schema_hardening_view_types(runtime_profile)
    if not enable_view_types:
        return scan
    return replace(scan, schema_force_view_types=True)


def _schema_hardening_view_types(runtime_profile: DataFusionRuntimeProfile) -> bool:
    hardening = runtime_profile.policies.schema_hardening
    if hardening is not None:
        return hardening.enable_view_types
    return runtime_profile.policies.schema_hardening_name == "arrow_performance"


def _prefers_listing_table(
    location: DatasetLocation,
    *,
    scan: DataFusionScanOptions | None,
) -> bool:
    if location.format is None or location.format == "delta":
        return False
    if location.files:
        return False
    if scan is not None and (scan.partition_cols or scan.file_sort_order):
        return True
    return _path_is_directory(location.path)


def _path_is_directory(path: str | Path) -> bool:
    if isinstance(path, str) and "://" in path:
        parsed = urlparse(path)
        if parsed.path.endswith("/"):
            return True
        return not Path(parsed.path).suffix
    resolved = ensure_path(path)
    if resolved.exists():
        return resolved.is_dir()
    return not resolved.suffix


@dataclass(frozen=True)
class _ExternalTableDdlRequest:
    name: str
    location: DatasetLocation
    format_name: str
    column_defs: tuple[str, ...]
    partition_cols: tuple[str, ...]
    ordering: tuple[tuple[str, str], ...]
    options: Mapping[str, str]
    unbounded: bool




def _apply_scan_settings(
    ctx: SessionContext,
    *,
    scan: DataFusionScanOptions | None,
    sql_options: SQLOptions,
) -> None:
    if scan is None:
        return
    skip_runtime_settings = (
        DATAFUSION_MAJOR_VERSION is not None
        and DATAFUSION_MAJOR_VERSION >= DATAFUSION_RUNTIME_SETTINGS_SKIP_VERSION
    )
    settings: list[tuple[str, object | None, bool]] = [
        ("datafusion.execution.collect_statistics", scan.collect_statistics, True),
        ("datafusion.execution.meta_fetch_concurrency", scan.meta_fetch_concurrency, False),
        ("datafusion.runtime.list_files_cache_limit", scan.list_files_cache_limit, False),
        ("datafusion.runtime.list_files_cache_ttl", scan.list_files_cache_ttl, False),
        (
            "datafusion.execution.listing_table_factory_infer_partitions",
            scan.listing_table_factory_infer_partitions,
            True,
        ),
        (
            "datafusion.execution.listing_table_ignore_subdirectory",
            scan.listing_table_ignore_subdirectory,
            True,
        ),
    ]
    for key, value, lower in settings:
        if value is None:
            continue
        text = str(value).lower() if lower else str(value)
        if skip_runtime_settings and key.startswith("datafusion.runtime."):
            record_runtime_setting_override(ctx, key=key, value=text)
            continue
        _set_runtime_setting(ctx, key=key, value=text, sql_options=sql_options)


def _set_runtime_setting(
    ctx: SessionContext,
    *,
    key: str,
    value: str,
    sql_options: SQLOptions,
) -> None:
    sql = f"SET {key} = '{value}'"
    allow_statements = True
    resolved = sql_options.with_allow_statements(allow_statements)
    try:
        df = ctx.sql_with_options(sql, resolved)
    except (RuntimeError, TypeError, ValueError) as exc:
        msg = "SET execution failed."
        raise ValueError(msg) from exc
    if df is None:
        msg = "SET execution did not return a DataFusion DataFrame."
        raise ValueError(msg)
    df.collect()





def _scan_details(scan: DataFusionScanOptions) -> dict[str, object]:
    return {
        "partition_cols": [(col, str(dtype)) for col, dtype in scan.partition_cols_pyarrow()],
        "file_sort_order": [list(value) for value in scan.file_sort_order],
        "file_extension": scan.file_extension,
        "parquet_pruning": scan.parquet_pruning,
        "skip_metadata": scan.skip_metadata,
        "skip_arrow_metadata": scan.skip_arrow_metadata,
        "binary_as_string": scan.binary_as_string,
        "schema_force_view_types": scan.schema_force_view_types,
        "listing_table_factory_infer_partitions": scan.listing_table_factory_infer_partitions,
        "listing_table_ignore_subdirectory": scan.listing_table_ignore_subdirectory,
        "cache": scan.cache,
        "collect_statistics": scan.collect_statistics,
        "meta_fetch_concurrency": scan.meta_fetch_concurrency,
        "list_files_cache_ttl": scan.list_files_cache_ttl,
        "list_files_cache_limit": scan.list_files_cache_limit,
        "projection_exprs": list(scan.projection_exprs),
        "listing_mutable": scan.listing_mutable,
        "unbounded": scan.unbounded,
    }


def _table_key_fields(context: DataFusionRegistrationContext) -> tuple[str, ...]:
    schema = context.options.schema
    if schema is None:
        return ()
    _required, key_fields = schema_constraints_from_metadata(schema.metadata)
    return key_fields


def _expected_column_defaults(schema: pa.Schema) -> dict[str, str]:
    defaults: dict[str, str] = {}
    for schema_field in schema:
        meta = schema_field.metadata or {}
        default_value = meta.get(DEFAULT_VALUE_META)
        if default_value is None:
            continue
        defaults[schema_field.name] = default_value.decode("utf-8", errors="replace")
    return defaults


def _validate_constraints_and_defaults(
    context: DataFusionRegistrationContext,
    *,
    enable_information_schema: bool,
    cache_prefix: str | None = None,
) -> None:
    if not enable_information_schema:
        return
    schema = context.options.schema
    if schema is None:
        return
    _required, key_fields = schema_constraints_from_metadata(schema.metadata)
    expected_defaults = _expected_column_defaults(schema)
    if not key_fields and not expected_defaults:
        return
    sql_options = _sql_options.sql_options_for_profile(context.runtime_profile)
    if context.runtime_profile is not None:
        introspector = schema_introspector_for_profile(
            context.runtime_profile,
            context.ctx,
            cache_prefix=cache_prefix,
        )
    else:
        introspector = SchemaIntrospector(context.ctx, sql_options=sql_options)
    if key_fields:
        constraint_rows = table_constraint_rows(
            context.ctx,
            table_name=context.name,
            sql_options=sql_options,
        )
        key_columns = {
            str(row["column_name"])
            for row in constraint_rows
            if row.get("constraint_type") in {"PRIMARY KEY", "UNIQUE"}
            and row.get("column_name") is not None
        }
        validate_required_items(
            key_fields,
            key_columns,
            item_label=f"{context.name} DataFusion constraints for key fields",
            error_type=ValueError,
        )
    if expected_defaults:
        column_defaults = introspector.table_column_defaults(context.name)
        validate_required_items(
            expected_defaults,
            column_defaults,
            item_label=f"{context.name} column defaults",
            error_type=ValueError,
        )


def _install_schema_evolution_adapter_factory(ctx: SessionContext) -> None:
    from datafusion_engine.extensions.schema_evolution import (
        install_schema_evolution_adapter_factory,
    )

    install_schema_evolution_adapter_factory(ctx)


def _resolve_dataset_spec(name: str, location: DatasetLocation) -> DatasetSpec | None:
    resolved = location.resolved
    if resolved.dataset_spec is not None:
        return resolved.dataset_spec
    if resolved.table_spec is not None:
        return make_dataset_spec(table_spec=resolved.table_spec)
    schema = resolved.schema
    if schema is None:
        return None
    return dataset_spec_from_schema(name, schema)


def register_dataset_df(
    ctx: SessionContext,
    *,
    name: str,
    location: DatasetLocation,
    cache_policy: DataFusionCachePolicy | None = None,
    runtime_profile: DataFusionRuntimeProfile | None = None,
    overwrite: bool = True,
) -> DataFrame:
    """Register a dataset location with DataFusion and return a DataFrame.

    .. deprecated::
        For new code, prefer ``RegistryFacade.register_dataset_df()`` for
        unified registration with rollback semantics. Import via
        ``from datafusion_engine.registry_facade import registry_facade_for_context``.

    Parameters
    ----------
    ctx
        DataFusion session context.
    name
        Dataset name to register.
    location
        Dataset location descriptor.
    cache_policy
        Cache policy overrides for registration.
    runtime_profile
        Optional runtime profile for session-scoped registration behaviors.
    overwrite
        Whether to replace an existing table binding.

    Returns:
    -------
    datafusion.dataframe.DataFrame
        DataFusion DataFrame for the registered dataset.
    """
    from datafusion_engine.session.helpers import deregister_table
    from datafusion_engine.tables.registration import (
        TableRegistrationRequest,
        register_table,
    )

    if runtime_profile is not None:
        from datafusion_engine.registry_facade import registry_facade_for_context
        from semantics.program_manifest import ManifestDatasetBindings

        facade = registry_facade_for_context(
            ctx,
            runtime_profile=runtime_profile,
            dataset_resolver=ManifestDatasetBindings(locations={}),
        )
        return facade.register_dataset_df(
            name=name,
            location=location,
            cache_policy=cache_policy,
            overwrite=overwrite,
        )
    existing = False
    if overwrite:
        from datafusion_engine.schema.introspection_core import table_names_snapshot

        existing = name in table_names_snapshot(ctx)
        deregister_table(ctx, name)
    df = register_table(
        ctx,
        TableRegistrationRequest(
            name=name,
            location=location,
            cache_policy=cache_policy,
            runtime_profile=runtime_profile,
        ),
    )
    scan = location.resolved.datafusion_scan
    if existing and scan is not None and scan.listing_mutable and runtime_profile is not None:
        from datafusion_engine.lineage.diagnostics import record_artifact
        from serde_artifact_specs import DATAFUSION_LISTING_REFRESH_SPEC

        record_artifact(
            runtime_profile,
            DATAFUSION_LISTING_REFRESH_SPEC,
            {
                "name": name,
                "path": str(location.path),
                "format": location.format,
                "event_time_unix_ms": int(time.time() * 1000),
            },
        )
    return df


def _build_registration_context(
    ctx: SessionContext,
    *,
    name: str,
    location: DatasetLocation,
    cache_policy: DataFusionCachePolicy | None,
    runtime_profile: DataFusionRuntimeProfile | None,
    caches: DatasetCaches | None = None,
) -> DataFusionRegistrationContext:
    resolved_caches = _resolve_dataset_caches(caches)
    location = _apply_scan_defaults(name, location)
    runtime_profile = _prepare_runtime_profile(runtime_profile, ctx=ctx, caches=resolved_caches)
    options = _resolve_registry_options(name, location, runtime_profile=runtime_profile)
    if options.provider == "listing":
        _register_object_store_for_location(ctx, location, runtime_profile=runtime_profile)
    cache = _resolve_cache_policy(
        options,
        cache_policy=cache_policy,
        runtime_profile=runtime_profile,
    )
    context = DataFusionRegistrationContext(
        ctx=ctx,
        name=name,
        location=location,
        options=options,
        cache=cache,
        caches=resolved_caches,
        runtime_profile=runtime_profile,
    )
    provider_metadata: dict[str, str] = {}
    if location.delta_version is not None:
        provider_metadata["delta_version"] = str(location.delta_version)
    if location.delta_timestamp is not None:
        provider_metadata["delta_timestamp"] = str(location.delta_timestamp)
    if location.delta_cdf_options is not None:
        provider_metadata["delta_cdf_enabled"] = "true"
    if location.datafusion_provider is not None:
        provider_metadata["datafusion_provider"] = str(location.datafusion_provider)
    default_values: dict[str, object] = {}
    resolved_table_spec = location.resolved.table_spec
    if resolved_table_spec is not None:
        default_values = dict(_expected_column_defaults(resolved_table_spec.to_arrow_schema()))
    # Schema adapters are attached at SessionContext creation via the runtime
    # profile's physical expression adapter factory. When enabled, scan-time
    # adapters handle schema drift resolution at the TableProvider boundary.
    schema_adapter_enabled = (
        runtime_profile is not None and runtime_profile.features.enable_schema_evolution_adapter
    )
    metadata = TableProviderMetadata(
        table_name=name,
        ddl=None,
        constraints=table_constraint_definitions(table_constraints_from_location(location)),
        default_values=default_values,
        storage_location=str(location.path),
        file_format=location.format or "unknown",
        partition_columns=tuple(col for col, _ in options.scan.partition_cols)
        if options.scan and options.scan.partition_cols
        else (),
        unbounded=options.scan.unbounded if options.scan else False,
        ddl_fingerprint=None,
        metadata=provider_metadata,
        schema_adapter_enabled=schema_adapter_enabled,
    )
    record_table_provider_metadata(ctx, metadata=metadata)
    return context


def _register_object_store_for_location(
    ctx: SessionContext,
    location: DatasetLocation,
    *,
    runtime_profile: DataFusionRuntimeProfile | None,
) -> None:
    if location.filesystem is None:
        return
    scheme = _scheme_prefix(location.path)
    if scheme is None:
        return
    adapter = DataFusionIOAdapter(ctx=ctx, profile=runtime_profile)
    adapter.register_object_store(
        scheme=scheme,
        store=location.filesystem,
        host=None,
    )


def _prepare_runtime_profile(
    runtime_profile: DataFusionRuntimeProfile | None,
    *,
    ctx: SessionContext,
    caches: DatasetCaches | None = None,
) -> DataFusionRuntimeProfile | None:
    if runtime_profile is None:
        return None
    runtime_profile = _populate_schema_adapter_factories(runtime_profile)
    resolved_caches = _resolve_dataset_caches(caches)
    _ensure_catalog_schema(
        ctx,
        catalog=runtime_profile.catalog.default_catalog,
        schema=runtime_profile.catalog.default_schema,
        caches=resolved_caches,
    )
    return runtime_profile


def _resolve_registry_options(
    _name: str,
    location: DatasetLocation,
    *,
    runtime_profile: DataFusionRuntimeProfile | None,
) -> DataFusionRegistryOptions:
    options = _resolve_registry_options_for_location(location)
    return _apply_runtime_scan_hardening(options, runtime_profile=runtime_profile)


def _invalidate_information_schema_cache(
    runtime_profile: DataFusionRuntimeProfile | None,
    ctx: SessionContext,
    *,
    cache_prefix: str | None = None,
) -> None:
    if runtime_profile is None or not runtime_profile.catalog.enable_information_schema:
        return
    invalidate_introspection_cache(ctx)
    introspector = schema_introspector_for_profile(
        runtime_profile,
        ctx,
        cache_prefix=cache_prefix,
    )
    introspector.invalidate_cache()




@dataclass(frozen=True)
class _DeltaProviderArtifactContext:
    dataset_name: str | None
    provider_mode: str
    ffi_table_provider: bool
    strict_native_provider_enabled: bool | None
    strict_native_provider_violation: bool | None
    delta_scan: DeltaScanOptions | None
    delta_scan_effective: Mapping[str, object] | None
    delta_scan_snapshot: object | None
    delta_scan_identity_hash: str | None
    snapshot: Mapping[str, object] | None
    registration_path: str
    predicate: str | None
    predicate_error: str | None
    add_actions: Sequence[Mapping[str, object]] | None


@dataclass(frozen=True)
class _DeltaProviderRegistration:
    resolution: DatasetResolution
    predicate_sql: str | None
    predicate_error: str | None


@dataclass(frozen=True)
class _DeltaRegistrationState:
    registration: _DeltaProviderRegistration
    resolution: DatasetResolution
    adapter: DataFusionIOAdapter
    provider: object
    provider_to_register: object
    provider_is_native: bool
    format_name: str
    cache_prefix: str | None


@dataclass(frozen=True)
class _DeltaRegistrationResult:
    df: DataFrame
    cache_prefix: str | None


def _delta_pruning_predicate(
    context: DataFusionRegistrationContext,
) -> tuple[str | None, str | None]:
    dataset_spec = _resolve_dataset_spec(context.name, context.location)
    if dataset_spec is None:
        return None, None
    from schema_spec.dataset_spec import dataset_spec_query

    query_spec = dataset_spec_query(dataset_spec)
    predicate = query_spec.pushdown_predicate or query_spec.predicate
    if predicate is None:
        return None, None
    try:
        return predicate.to_sql(), None
    except (TypeError, ValueError) as exc:
        return None, str(exc)


def _scan_files_from_add_actions(
    add_actions: Sequence[Mapping[str, object]] | None,
) -> tuple[str, ...]:
    if not add_actions:
        return ()
    return tuple(
        str(entry["path"])
        for entry in add_actions
        if isinstance(entry, Mapping) and entry.get("path") is not None
    )


def _provider_for_registration(provider: object) -> object:
    if hasattr(provider, "__datafusion_table_provider__"):
        return provider
    try:
        import pyarrow.dataset as ds
        from datafusion.catalog import Table
        from datafusion.dataframe import DataFrame
    except ImportError:
        return TableProviderCapsule(provider)
    if isinstance(provider, (ds.Dataset, DataFrame, Table)):
        return provider
    return TableProviderCapsule(provider)


def _cache_prefix_for_registration(
    context: DataFusionRegistrationContext,
    *,
    resolution: DatasetResolution,
) -> str | None:
    if context.runtime_profile is None:
        return None
    if context.runtime_profile.policies.snapshot_pinned_mode != "delta_version":
        return None
    return cache_prefix_for_delta_snapshot(
        context.runtime_profile,
        dataset_name=context.name,
        snapshot=resolution.delta_snapshot,
    )


def _enforce_delta_native_provider_policy(
    state: _DeltaRegistrationState,
    context: DataFusionRegistrationContext,
) -> None:
    if state.format_name != "delta" or state.provider_is_native:
        return
    msg = (
        "Delta provider registration fell back to a PyArrow dataset; "
        "FFI TableProvider is required for pushdown and correctness."
    )
    if (
        context.runtime_profile is not None
        and context.runtime_profile.features.enforce_delta_ffi_provider
    ):
        raise DataFusionEngineError(msg, kind=ErrorKind.PLUGIN)
    logger.warning(msg)


def _record_delta_snapshot_if_applicable(
    context: DataFusionRegistrationContext,
    *,
    resolution: DatasetResolution,
    schema_identity_hash_value: str | None,
    ddl_fingerprint: str | None,
) -> None:
    if resolution.delta_snapshot is None:
        return
    from datafusion_engine.delta.observability import (
        DELTA_MAINTENANCE_TABLE_NAME,
        DELTA_MUTATION_TABLE_NAME,
        DELTA_SCAN_PLAN_TABLE_NAME,
        DELTA_SNAPSHOT_TABLE_NAME,
        DeltaSnapshotArtifact,
        record_delta_snapshot,
    )

    observability_tables = {
        DELTA_SNAPSHOT_TABLE_NAME,
        DELTA_MUTATION_TABLE_NAME,
        DELTA_SCAN_PLAN_TABLE_NAME,
        DELTA_MAINTENANCE_TABLE_NAME,
    }
    if context.name in observability_tables:
        return
    record_delta_snapshot(
        context.runtime_profile,
        artifact=DeltaSnapshotArtifact(
            table_uri=str(context.location.path),
            snapshot=resolution.delta_snapshot,
            dataset_name=context.name,
            schema_identity_hash=schema_identity_hash_value,
            ddl_fingerprint=ddl_fingerprint,
        ),
        ctx=context.ctx,
    )


def _delta_provider_artifact_payload(
    ctx: SessionContext,
    location: DatasetLocation,
    *,
    context: _DeltaProviderArtifactContext,
) -> dict[str, object]:
    compatibility = is_delta_extension_compatible(
        ctx,
        entrypoint="delta_table_provider_from_session",
        require_non_fallback=bool(context.strict_native_provider_enabled),
    )
    request = provider_build_request_from_registration_context(
        RegistrationProviderArtifactInput(
            table_uri=str(location.path),
            dataset_format=location.format,
            provider_kind="delta",
            compatibility=compatibility,
            context=context,
        )
    )
    request = replace(
        request,
        delta_version=location.delta_version,
        delta_timestamp=location.delta_timestamp,
        delta_log_storage_options=(
            dict(location.delta_log_storage_options) if location.delta_log_storage_options else None
        ),
        delta_storage_options=(
            dict(location.storage_options) if location.storage_options else None
        ),
    )
    return build_delta_provider_build_result(request).as_payload()


def _record_delta_log_health(
    runtime_profile: DataFusionRuntimeProfile | None,
    *,
    name: str,
    location: DatasetLocation,
    resolution: DatasetResolution,
) -> None:
    if runtime_profile is None:
        return
    if location.format != "delta":
        return
    payload = _delta_log_health_payload(
        name=name,
        location=location,
        resolution=resolution,
        runtime_profile=runtime_profile,
    )
    from serde_artifact_specs import DELTA_LOG_HEALTH_SPEC

    record_artifact(runtime_profile, DELTA_LOG_HEALTH_SPEC, payload)


def _delta_log_health_payload(
    *,
    name: str,
    location: DatasetLocation,
    resolution: DatasetResolution,
    runtime_profile: DataFusionRuntimeProfile,
) -> dict[str, object]:
    delta_log_present, checkpoint_version, log_reason, log_count = _delta_log_status(location)
    snapshot = resolution.delta_snapshot
    min_reader = _snapshot_int(snapshot, "min_reader_version")
    min_writer = _snapshot_int(snapshot, "min_writer_version")
    reader_features = _snapshot_features(snapshot, "reader_features")
    writer_features = _snapshot_features(snapshot, "writer_features")
    compatibility = delta_protocol_compatibility(
        snapshot,
        runtime_profile.policies.delta_protocol_support,
    )
    table_features = list(combined_table_features(compatibility))
    protocol_compatible = compatibility.compatible
    severity = _delta_health_severity(
        delta_log_present=delta_log_present,
        protocol_compatible=protocol_compatible,
    )
    return {
        "dataset": name,
        "path": str(location.path),
        "delta_log_present": delta_log_present,
        "delta_log_reason": log_reason,
        "delta_log_file_count": log_count,
        "last_checkpoint_version": checkpoint_version,
        "min_reader_version": min_reader,
        "min_writer_version": min_writer,
        "reader_features": reader_features,
        "writer_features": writer_features,
        "table_features": table_features,
        "protocol_compatible": protocol_compatible,
        "protocol_reason": compatibility.reason,
        "missing_reader_features": list(compatibility.missing_reader_features),
        "missing_writer_features": list(compatibility.missing_writer_features),
        "run_id": get_run_id(),
        "diagnostic.severity": severity,
        "diagnostic.category": "delta_protocol",
    }


def _delta_log_status(
    location: DatasetLocation,
) -> tuple[bool | None, int | None, str | None, int | None]:
    path_text = str(location.path)
    parsed = urlparse(path_text)
    if parsed.scheme and parsed.scheme != "file":
        return None, None, "remote_path", None
    path = Path(parsed.path if parsed.scheme == "file" and parsed.path else path_text)
    delta_log = path / "_delta_log"
    if not delta_log.exists():
        return False, None, "delta_log_missing", None
    try:
        json_files = list(delta_log.glob("*.json"))
    except OSError:
        return False, None, "delta_log_unreadable", None
    if not json_files:
        return False, None, "delta_log_empty", 0
    checkpoint_version = _read_last_checkpoint_version(delta_log)
    return True, checkpoint_version, None, len(json_files)


def _read_last_checkpoint_version(delta_log: Path) -> int | None:
    checkpoint = delta_log / "_last_checkpoint"
    if not checkpoint.exists():
        return None
    try:
        with checkpoint.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except (OSError, json.JSONDecodeError, ValueError):
        return None
    if isinstance(payload, Mapping):
        return coerce_int(payload.get("version"))
    return None


def _delta_health_severity(
    *,
    delta_log_present: bool | None,
    protocol_compatible: bool | None,
) -> str:
    if protocol_compatible is False:
        return "error"
    if delta_log_present is False:
        return "warn"
    return "info"


def _snapshot_int(snapshot: Mapping[str, object] | None, key: str) -> int | None:
    if snapshot is None:
        return None
    return coerce_int(snapshot.get(key))


def _snapshot_features(snapshot: Mapping[str, object] | None, key: str) -> list[str]:
    if snapshot is None:
        return []
    values = snapshot.get(key)
    if isinstance(values, Sequence) and not isinstance(values, (str, bytes, bytearray)):
        return [str(item) for item in values if str(item)]
    return []


def _delta_cdf_artifact_payload(
    location: DatasetLocation,
    *,
    resolution: DatasetResolution,
) -> dict[str, object]:
    return {
        "path": str(location.path),
        "format": location.format,
        "delta_log_storage_options": (
            dict(location.delta_log_storage_options) if location.delta_log_storage_options else None
        ),
        "delta_storage_options": (
            dict(location.storage_options) if location.storage_options else None
        ),
        "delta_snapshot": resolution.delta_snapshot,
        "cdf_options": _cdf_options_payload(location.delta_cdf_options),
    }


def provider_capsule_id(provider: object) -> str:
    """Return a stable identifier for a DataFusion table provider capsule.

    Args:
        provider: Description.

    Raises:
        AttributeError: If the operation cannot be completed.
    """
    capsule = _table_provider_capsule(provider)
    if capsule is None:
        msg = "Provider does not expose a DataFusion table provider capsule."
        raise AttributeError(msg)
    return repr(capsule)


def _table_provider_capsule(provider: object) -> object | None:
    attr = getattr(provider, "__datafusion_table_provider__", None)
    if callable(attr):
        return attr()
    attr = getattr(provider, "datafusion_table_provider", None)
    if callable(attr):
        return attr()
    return None


def _provider_capsule_id(
    provider: object | None,
    *,
    source: object | None,
) -> str | None:
    if source is not None:
        try:
            return provider_capsule_id(source)
        except (AttributeError, RuntimeError, TypeError, ValueError):
            return None
    if provider is None:
        return None
    return repr(provider)


def _provider_pushdown_value(provider: object, *, names: Sequence[str]) -> str | bool | None:
    for name in names:
        attr = getattr(provider, name, None)
        if isinstance(attr, bool):
            return attr
        if callable(attr):
            try:
                value = attr()
            except TypeError:
                continue
            if isinstance(value, bool):
                return value
            if value is None:
                return None
            return str(value)
    return None


def _provider_pushdown_hints(provider: object) -> dict[str, object]:
    return {
        "projection_pushdown": _provider_pushdown_value(
            provider,
            names=("supports_projection_pushdown",),
        ),
        "predicate_pushdown": _provider_pushdown_value(
            provider,
            names=(
                "supports_filter_pushdown",
                "supports_filters_pushdown",
                "supports_predicate_pushdown",
            ),
        ),
        "limit_pushdown": _provider_pushdown_value(
            provider,
            names=("supports_limit_pushdown",),
        ),
    }


@dataclass(frozen=True)
class _TableProviderArtifact:
    name: str
    provider: object | None
    provider_kind: str
    source: object | None = None
    details: Mapping[str, object] | None = None


@dataclass(frozen=True)
class _ProviderModeContext:
    name: str
    provider_kind: str
    provider: object | None
    capsule_id: str | None
    details: Mapping[str, object] | None


def _record_table_provider_artifact(
    runtime_profile: DataFusionRuntimeProfile | None,
    *,
    artifact: _TableProviderArtifact,
) -> None:
    capsule_id = _provider_capsule_id(artifact.provider, source=artifact.source)
    _record_provider_mode_diagnostics(
        runtime_profile=runtime_profile,
        context=_ProviderModeContext(
            name=artifact.name,
            provider_kind=artifact.provider_kind,
            provider=artifact.provider,
            capsule_id=capsule_id,
            details=artifact.details,
        ),
    )
    payload: dict[str, object] = {
        "name": artifact.name,
        "provider": artifact.provider_kind,
        "provider_type": type(artifact.provider).__name__
        if artifact.provider is not None
        else None,
        "capsule_id": capsule_id,
    }
    if artifact.provider is not None:
        payload.update(_provider_pushdown_hints(artifact.provider))
    if artifact.details:
        payload.update(artifact.details)
    from serde_artifact_specs import DATAFUSION_TABLE_PROVIDERS_SPEC

    record_artifact(runtime_profile, DATAFUSION_TABLE_PROVIDERS_SPEC, payload)


def _record_provider_mode_diagnostics(
    runtime_profile: DataFusionRuntimeProfile | None,
    *,
    context: _ProviderModeContext,
) -> None:
    if runtime_profile is None:
        return
    format_name = None
    if context.details is not None:
        format_value = context.details.get("format")
        if isinstance(format_value, str):
            format_name = format_value
    provider_class = type(context.provider).__name__ if context.provider is not None else None
    ffi_table_provider = context.capsule_id is not None
    severity = _provider_mode_severity(
        format_name,
        provider_is_native=ffi_table_provider,
    )
    strict_native_provider_enabled = runtime_profile.features.enforce_delta_ffi_provider
    expected_native_provider = format_name == "delta"
    provider_is_native = ffi_table_provider
    strict_native_provider_violation = (
        strict_native_provider_enabled and expected_native_provider and not provider_is_native
    )
    payload = {
        "dataset": context.name,
        "provider_mode": context.provider_kind,
        "provider_class": provider_class,
        "ffi_table_provider": ffi_table_provider,
        "capsule_id": context.capsule_id,
        "strict_native_provider_enabled": strict_native_provider_enabled,
        "expected_native_provider": expected_native_provider,
        "provider_is_native": provider_is_native,
        "strict_native_provider_violation": strict_native_provider_violation,
        "run_id": get_run_id(),
        "diagnostic.severity": severity,
        "diagnostic.category": "datafusion_provider",
    }
    from serde_artifact_specs import DATASET_PROVIDER_MODE_SPEC

    record_artifact(runtime_profile, DATASET_PROVIDER_MODE_SPEC, payload)


def _provider_mode_severity(
    format_name: str | None,
    *,
    provider_is_native: bool,
) -> str:
    if format_name == "delta" and not provider_is_native:
        return "warn"
    return "info"


def _update_table_provider_capabilities(
    ctx: SessionContext,
    *,
    name: str,
    supports_insert: bool | None = None,
    supports_cdf: bool | None = None,
) -> None:
    metadata = table_provider_metadata(ctx, table_name=name)
    if metadata is None:
        return
    updated = replace(
        metadata,
        supports_insert=supports_insert,
        supports_cdf=supports_cdf,
    )
    record_table_provider_metadata(ctx, metadata=updated)


def _update_table_provider_scan_config(
    ctx: SessionContext,
    *,
    name: str,
    delta_scan_snapshot: object | None,
    delta_scan_identity_hash: str | None,
    delta_scan_effective: Mapping[str, object] | None,
) -> None:
    metadata = table_provider_metadata(ctx, table_name=name)
    if metadata is None:
        return
    updated = replace(
        metadata,
        delta_scan_config=delta_scan_snapshot_payload(delta_scan_snapshot),
        delta_scan_identity_hash=delta_scan_identity_hash,
        delta_scan_effective=dict(delta_scan_effective) if delta_scan_effective else None,
    )
    record_table_provider_metadata(ctx, metadata=updated)


def _update_table_provider_fingerprints(
    ctx: SessionContext,
    *,
    name: str,
    schema: pa.Schema | None,
) -> tuple[str | None, str | None, dict[str, object]]:
    metadata = table_provider_metadata(ctx, table_name=name)
    if metadata is None:
        return None, None, {}
    schema_identity_hash_value = _schema_identity_hash(schema)
    ddl_fingerprint = metadata.ddl_fingerprint
    if ddl_fingerprint is None and metadata.ddl is not None:
        ddl_fingerprint = ddl_fingerprint_from_definition(metadata.ddl)
    updated = replace(
        metadata,
        schema_identity_hash=schema_identity_hash_value,
        ddl_fingerprint=ddl_fingerprint,
    )
    record_table_provider_metadata(ctx, metadata=updated)
    details: dict[str, object] = {}
    if schema_identity_hash_value is not None:
        details["schema_identity_hash"] = schema_identity_hash_value
    if ddl_fingerprint is not None:
        details["ddl_fingerprint"] = ddl_fingerprint
    return schema_identity_hash_value, ddl_fingerprint, details


def _apply_projection_exprs(
    ctx: SessionContext,
    *,
    table_name: str,
    projection_exprs: Sequence[str],
    sql_options: SQLOptions,
    runtime_profile: DataFusionRuntimeProfile | None,
) -> DataFrame:
    if not projection_exprs:
        return ctx.table(table_name)
    _ = sql_options
    df = ctx.table(table_name)
    resolved_exprs = _resolve_projection_exprs(df, projection_exprs)
    if not resolved_exprs:
        msg = "Projection expressions are required for dynamic projection."
        raise ValueError(msg)
    projected = df.select(*resolved_exprs)
    adapter = DataFusionIOAdapter(ctx=ctx, profile=runtime_profile)
    adapter.register_view(table_name, projected, overwrite=True, temporary=False)
    if runtime_profile is not None:
        from serde_artifact_specs import PROJECTION_VIEW_ARTIFACT_SKIPPED_SPEC

        runtime_profile.record_artifact(
            PROJECTION_VIEW_ARTIFACT_SKIPPED_SPEC,
            {
                "event_time_unix_ms": int(time.time() * 1000),
                "table_name": table_name,
                "reason": "disabled_due_to_datafusion_panics",
                "projection_expr_count": len(projection_exprs),
            },
        )
    return ctx.table(table_name)


def _resolve_projection_exprs(
    df: DataFrame,
    projection_exprs: Sequence[str],
) -> list[Expr | str]:
    schema_fields = list(df.schema().names)
    schema_names = set(schema_fields)
    resolved_exprs: list[Expr | str] = []
    expanded_star = False
    for expr_text in projection_exprs:
        if expr_text.strip() == "*":
            if not expanded_star:
                resolved_exprs.extend(col(name) for name in schema_fields)
                expanded_star = True
            continue
        if expr_text in schema_names:
            resolved_exprs.append(col(expr_text))
            continue
        try:
            resolved_exprs.append(df.parse_sql_expr(expr_text))
        except (RuntimeError, TypeError, ValueError) as exc:
            msg = f"Projection expression parse failed: {expr_text!r} ({exc})"
            raise ValueError(msg) from exc
    return resolved_exprs


def _record_projection_view_artifact(
    ctx: SessionContext,
    *,
    table_name: str,
    projected: DataFrame,
    runtime_profile: DataFusionRuntimeProfile,
) -> None:
    from datafusion_engine.session.runtime_session import (
        record_view_definition,
        session_runtime_hash,
    )
    from datafusion_engine.views.artifacts import (
        ViewArtifactLineage,
        ViewArtifactRequest,
        build_view_artifact_from_bundle,
    )

    schema = arrow_schema_from_df(projected)
    session_runtime = runtime_profile.session_runtime()
    bundle = build_plan_artifact(
        ctx,
        projected,
        options=PlanBundleOptions(
            session_runtime=session_runtime,
        ),
    )
    runtime_hash = session_runtime_hash(session_runtime)
    artifact = build_view_artifact_from_bundle(
        bundle,
        request=ViewArtifactRequest(
            name=table_name,
            schema=schema,
            lineage=ViewArtifactLineage(
                required_udfs=bundle.required_udfs,
                referenced_tables=referenced_tables_from_plan(bundle.optimized_logical_plan),
            ),
            runtime_hash=runtime_hash,
        ),
    )
    record_view_definition(runtime_profile, artifact=artifact)
    _invalidate_information_schema_cache(runtime_profile, ctx)


def _projection_exprs_for_schema(
    *,
    actual_columns: Sequence[str],
    expected_schema: pa.Schema,
) -> tuple[str, ...]:
    actual = set(actual_columns)
    defaults = _expected_column_defaults(expected_schema)
    projection_exprs: list[str] = []
    for schema_field in expected_schema:
        if schema_field.name in actual:
            if _supports_projection_cast(schema_field.type):
                dtype_name = _sql_type_name(schema_field.type)
                cast_expr = f"cast({schema_field.name} as {dtype_name})"
                default_value = defaults.get(schema_field.name)
                if default_value is not None:
                    literal = _sql_literal_for_field(default_value, dtype=schema_field.type)
                    if literal is not None:
                        cast_expr = f"coalesce({cast_expr}, {literal})"
                projection_exprs.append(f"{cast_expr} as {schema_field.name}")
            else:
                projection_exprs.append(schema_field.name)
        elif _supports_projection_cast(schema_field.type):
            dtype_name = _sql_type_name(schema_field.type)
            projection_exprs.append(f"cast(NULL as {dtype_name}) as {schema_field.name}")
        else:
            projection_exprs.append(f"NULL as {schema_field.name}")
    return tuple(projection_exprs)


def _supports_projection_cast(dtype: pa.DataType) -> bool:
    return not (
        pa.types.is_list(dtype)
        or pa.types.is_large_list(dtype)
        or pa.types.is_fixed_size_list(dtype)
        or pa.types.is_struct(dtype)
        or pa.types.is_map(dtype)
        or pa.types.is_union(dtype)
        or pa.types.is_dictionary(dtype)
        or pa.types.is_binary(dtype)
        or pa.types.is_large_binary(dtype)
        or pa.types.is_fixed_size_binary(dtype)
    )


def _sql_literal_for_field(value: str, *, dtype: pa.DataType) -> str | None:
    normalized = value.strip()
    if not normalized:
        return None
    if pa.types.is_boolean(dtype):
        return _sql_bool_literal(normalized)
    if pa.types.is_integer(dtype):
        return _sql_int_literal(normalized)
    if pa.types.is_floating(dtype):
        return _sql_float_literal(normalized)
    escaped = normalized.replace("'", "''")
    return f"'{escaped}'"


def _sql_bool_literal(value: str) -> str | None:
    """Return SQL boolean literal or None for invalid inputs.

    Returns:
    -------
    str | None
        SQL boolean literal when valid.
    """
    lowered = value.lower()
    if lowered in {"true", "false"}:
        return lowered
    return None


def _sql_int_literal(value: str) -> str | None:
    """Return SQL integer literal or None for invalid inputs.

    Returns:
    -------
    str | None
        SQL integer literal when valid.
    """
    try:
        return str(int(value))
    except ValueError:
        return None


def _sql_float_literal(value: str) -> str | None:
    """Return SQL float literal or None for invalid inputs.

    Returns:
    -------
    str | None
        SQL float literal when valid.
    """
    try:
        return str(float(value))
    except ValueError:
        return None


def apply_projection_overrides(
    ctx: SessionContext,
    *,
    projection_map: Mapping[str, Sequence[str]],
    sql_options: SQLOptions,
    runtime_profile: DataFusionRuntimeProfile | None = None,
) -> None:
    """Apply projection overrides for table names when available."""
    if not projection_map:
        return
    for table_name, columns in projection_map.items():
        if not columns:
            continue
        projection_exprs = [str(name) for name in columns if str(name)]
        try:
            _apply_projection_exprs(
                ctx,
                table_name=table_name,
                projection_exprs=projection_exprs,
                sql_options=sql_options,
                runtime_profile=runtime_profile,
            )
        except (KeyError, RuntimeError, TypeError, ValueError):
            continue


def apply_projection_scan_overrides(
    ctx: SessionContext,
    *,
    projection_map: Mapping[str, Sequence[str]],
    runtime_profile: DataFusionRuntimeProfile | None,
    dataset_resolver: ManifestDatasetResolver | None = None,
) -> None:
    """Re-register datasets with projection scan overrides when possible.

    Raises:
    ------
    ValueError
        If ``dataset_resolver`` is None.
    """
    if runtime_profile is None:
        return
    if dataset_resolver is None:
        return
    for table_name, columns in projection_map.items():
        if not columns:
            continue
        location = dataset_resolver.location(table_name)
        if location is None:
            continue
        scan = location.resolved.datafusion_scan
        if scan is None:
            continue
        projection_exprs = tuple(str(name) for name in columns if str(name))
        if scan.projection_exprs == projection_exprs:
            continue
        updated_scan = replace(scan, projection_exprs=projection_exprs)
        overrides = location.overrides
        if overrides is None:
            overrides = DatasetLocationOverrides(datafusion_scan=updated_scan)
        else:
            overrides = msgspec.structs.replace(overrides, datafusion_scan=updated_scan)
        updated_location = msgspec.structs.replace(location, overrides=overrides)
        adapter = DataFusionIOAdapter(ctx=ctx, profile=runtime_profile)
        with suppress(KeyError, RuntimeError, TypeError, ValueError):
            adapter.deregister_table(table_name)
        try:
            from datafusion_engine.session.facade import DataFusionExecutionFacade

            facade = DataFusionExecutionFacade(ctx=ctx, runtime_profile=runtime_profile)
            facade.register_dataset(name=table_name, location=updated_location)
        except (RuntimeError, TypeError, ValueError):
            continue


def _schema_identity_hash(schema: pa.Schema | None) -> str | None:
    if schema is None:
        return None
    return schema_identity_hash(schema)


from datafusion_engine.dataset.registration_schema import (
    _partition_schema_validation,
)


def _validate_ordering_contract(
    context: DataFusionRegistrationContext,
    *,
    scan: DataFusionScanOptions | None,
) -> None:
    if context.name != _BYTECODE_EXTERNAL_TABLE_NAME:
        return
    schema = context.options.schema
    if schema is None:
        return
    ordering = ordering_from_schema(schema)
    if ordering.level != OrderingLevel.EXPLICIT or not ordering.keys:
        return
    expected = [list(key) for key in ordering.keys]
    if scan is None or not scan.file_sort_order:
        msg = f"{context.name} ordering requires file_sort_order {expected}."
        raise ValueError(msg)
    if [list(key) for key in scan.file_sort_order] != expected:
        msg = (
            f"{context.name} file_sort_order {[list(key) for key in scan.file_sort_order]} does not match "
            f"schema ordering {expected}."
        )
        raise ValueError(msg)


def _populate_schema_adapter_factories(
    runtime_profile: DataFusionRuntimeProfile,
) -> DataFusionRuntimeProfile:
    if not runtime_profile.features.enable_schema_evolution_adapter:
        return runtime_profile
    if (
        runtime_profile.policies.schema_adapter_factories
        or not runtime_profile.catalog.registry_catalogs
    ):
        return runtime_profile
    factories: dict[str, object] = {}
    adapter_factory: object | None = None
    for catalog in runtime_profile.catalog.registry_catalogs.values():
        for name in catalog.names():
            location = catalog.get(name)
            spec = _resolve_dataset_spec(name, location)
            if spec is None:
                continue
            if not _requires_schema_evolution_adapter(spec.evolution_spec):
                continue
            if adapter_factory is None:
                adapter_factory = _schema_evolution_adapter_factory()
            factories.setdefault(name, adapter_factory)
    if not factories:
        return runtime_profile
    merged = dict(runtime_profile.policies.schema_adapter_factories)
    for name, factory in factories.items():
        merged.setdefault(name, factory)
    return replace(
        runtime_profile,
        policies=replace(
            runtime_profile.policies,
            schema_adapter_factories=merged,
        ),
    )


def _resolve_expr_adapter_factory(
    scan: DataFusionScanOptions | None,
    *,
    runtime_profile: DataFusionRuntimeProfile | None,
    dataset_name: str,
    location: DatasetLocation,
) -> object | None:
    if scan is not None and scan.expr_adapter_factory is not None:
        return scan.expr_adapter_factory
    if runtime_profile is not None:
        factory = runtime_profile.policies.schema_adapter_factories.get(dataset_name)
        if factory is not None:
            return factory
    spec = _resolve_dataset_spec(dataset_name, location)
    if spec is not None and _requires_schema_evolution_adapter(spec.evolution_spec):
        return _schema_evolution_adapter_factory()
    if (
        runtime_profile is not None
        and runtime_profile.policies.physical_expr_adapter_factory is not None
    ):
        return runtime_profile.policies.physical_expr_adapter_factory
    return None


def _ensure_expr_adapter_factory(
    ctx: SessionContext,
    *,
    factory: object | None,
    evolution_required: bool,
) -> None:
    if factory is None:
        if evolution_required:
            msg = "Schema evolution adapter is required but not configured."
            raise ValueError(msg)
        return
    register = getattr(ctx, "register_physical_expr_adapter_factory", None)
    if callable(register):
        try:
            register(factory)
        except (RuntimeError, TypeError, ValueError) as exc:
            msg = f"Failed to register physical expr adapter factory: {exc}"
            raise ValueError(msg) from exc
        return
    if evolution_required:
        _install_schema_evolution_adapter_factory(ctx)
        return
    msg = "SessionContext does not support physical expr adapter registration."
    raise TypeError(msg)


def _requires_schema_evolution_adapter(evolution: object) -> bool:
    rename_map = getattr(evolution, "rename_map", None)
    allow_missing = bool(getattr(evolution, "allow_missing", False))
    allow_extra = bool(getattr(evolution, "allow_extra", False))
    allow_casts = bool(getattr(evolution, "allow_casts", False))
    return bool(rename_map) or allow_missing or allow_extra or allow_casts


def _schema_evolution_adapter_factory() -> object:
    module = None
    for module_name in ("datafusion_engine.extensions.datafusion_ext",):
        try:
            candidate = importlib.import_module(module_name)
        except ImportError:
            continue
        if hasattr(candidate, "schema_evolution_adapter_factory"):
            module = candidate
            break
    if module is None:
        msg = "Schema evolution adapter requires datafusion_ext."
        raise RuntimeError(msg)
    factory = getattr(module, "schema_evolution_adapter_factory", None)
    if not callable(factory):
        msg = "schema_evolution_adapter_factory is unavailable in the extension module."
        raise TypeError(msg)
    return factory()


def _fingerprints_match(left: str | None, right: str | None) -> bool | None:
    if left is None or right is None:
        return None
    return left == right


def _table_schema_contract_payload(
    contract: TableSchemaContract | None,
) -> dict[str, object] | None:
    if contract is None:
        return None
    partition_schema = contract.partition_schema()
    return {
        "file_schema": schema_to_dict(contract.file_schema),
        "partition_cols": [
            {"name": name, "dtype": str(dtype)} for name, dtype in contract.partition_cols_pyarrow()
        ],
        "partition_schema": schema_to_dict(partition_schema)
        if partition_schema is not None
        else None,
    }


def _optional_bool(value: object | None) -> bool | None:
    if value is None:
        return None
    return bool(value)


def _schema_evolution_payload(evolution: object | None) -> dict[str, object] | None:
    if evolution is None:
        return None
    rename_map = getattr(evolution, "rename_map", None)
    rename_payload = (
        {str(key): str(value) for key, value in rename_map.items()}
        if isinstance(rename_map, Mapping)
        else None
    )
    return {
        "promote_options": getattr(evolution, "promote_options", None),
        "rename_map": rename_payload,
        "allow_missing": _optional_bool(getattr(evolution, "allow_missing", None)),
        "allow_extra": _optional_bool(getattr(evolution, "allow_extra", None)),
        "allow_casts": _optional_bool(getattr(evolution, "allow_casts", None)),
    }


def _schema_evolution_details(
    context: DataFusionRegistrationContext,
) -> tuple[dict[str, object] | None, bool | None]:
    spec = _resolve_dataset_spec(context.name, context.location)
    if spec is None:
        return None, None
    evolution_spec = spec.evolution_spec
    return (
        _schema_evolution_payload(evolution_spec),
        _requires_schema_evolution_adapter(evolution_spec),
    )


def _adapter_factory_payload(factory: object | None) -> str | None:
    if factory is None:
        return None
    class_name = getattr(factory, "__class__", None)
    if class_name is not None:
        return class_name.__name__
    return repr(factory)


@dataclass(frozen=True)
class _TableProvenanceRequest:
    ctx: SessionContext
    name: str
    enable_information_schema: bool
    sql_options: SQLOptions
    runtime_profile: DataFusionRuntimeProfile | None = None
    cache_prefix: str | None = None


def _table_provenance_snapshot(
    request: _TableProvenanceRequest,
) -> dict[str, object]:
    if request.runtime_profile is not None:
        introspector = schema_introspector_for_profile(
            request.runtime_profile,
            request.ctx,
            cache_prefix=request.cache_prefix,
        )
    else:
        introspector = SchemaIntrospector(request.ctx, sql_options=request.sql_options)
    table_definition = introspector.table_definition(request.name)
    constraints: tuple[str, ...] = ()
    column_defaults: dict[str, object] | None = None
    logical_plan: str | None = None
    if request.enable_information_schema:
        constraints = introspector.table_constraints(request.name)
        column_defaults = introspector.table_column_defaults(request.name) or None
        logical_plan = introspector.table_logical_plan(request.name)
    return {
        "table_definition": table_definition,
        "table_constraints": list(constraints) if constraints else None,
        "column_defaults": column_defaults,
        "logical_plan": logical_plan,
    }


@dataclass(frozen=True)
class _PartitionSchemaContext:
    ctx: SessionContext
    table_name: str
    enable_information_schema: bool
    runtime_profile: DataFusionRuntimeProfile | None = None


def _require_partition_schema_validation(
    context: _PartitionSchemaContext,
    *,
    expected_partition_cols: Sequence[tuple[str, str]] | None,
) -> None:
    validation = _partition_schema_validation(
        context,
        expected_partition_cols=expected_partition_cols,
    )
    if validation is None:
        return
    missing = _validation_missing_partition_cols(validation)
    order_matches = validation.get("partition_order_matches")
    type_mismatches = _validation_type_mismatches(validation)
    if missing or order_matches is False or type_mismatches:
        msg = f"Partition schema validation failed for {context.table_name}: {validation}."
        raise ValueError(msg)


def _validation_missing_partition_cols(
    validation: Mapping[str, object],
) -> list[str]:
    missing_value = validation.get("missing_partition_cols")
    if isinstance(missing_value, Sequence) and not isinstance(
        missing_value,
        (str, bytes, bytearray),
    ):
        return [str(item) for item in missing_value]
    return []


def _validation_type_mismatches(
    validation: Mapping[str, object],
) -> list[dict[str, str]]:
    type_value = validation.get("partition_type_mismatches")
    if isinstance(type_value, Sequence) and not isinstance(
        type_value,
        (str, bytes, bytearray),
    ):
        return [
            {str(key): str(val) for key, val in item.items()}
            for item in type_value
            if isinstance(item, Mapping)
        ]
    return []


def _normalize_filesystem(filesystem: object) -> pafs.FileSystem | None:
    if isinstance(filesystem, pafs.FileSystem):
        return filesystem
    handler = getattr(pafs, "FSSpecHandler", None)
    if handler is not None:
        try:
            return pafs.PyFileSystem(handler(filesystem))
        except (TypeError, ValueError):
            return None
    if isinstance(filesystem, pafs.FileSystemHandler):
        return pafs.PyFileSystem(filesystem)
    return None


def _record_delta_cdf_artifact(
    runtime_profile: DataFusionRuntimeProfile | None,
    *,
    artifact: DeltaCdfArtifact,
) -> None:
    payload: dict[str, object] = {
        "name": artifact.name,
        "path": artifact.path,
        "provider": artifact.provider,
        "options": _cdf_options_payload(artifact.options),
        "delta_log_storage_options": (
            dict(artifact.log_storage_options) if artifact.log_storage_options else None
        ),
        "delta_snapshot": artifact.snapshot,
    }
    from serde_artifact_specs import DATAFUSION_DELTA_CDF_SPEC

    record_artifact(runtime_profile, DATAFUSION_DELTA_CDF_SPEC, payload)


def _cdf_options_payload(options: DeltaCdfOptions | None) -> dict[str, object] | None:
    if options is None:
        return None
    return {
        "starting_version": options.starting_version,
        "ending_version": options.ending_version,
        "starting_timestamp": options.starting_timestamp,
        "ending_timestamp": options.ending_timestamp,
        "columns": list(options.columns) if options.columns is not None else None,
        "predicate": options.predicate,
        "allow_out_of_range": options.allow_out_of_range,
    }


def _scheme_prefix(path: str | Path) -> str | None:
    if not isinstance(path, str):
        return None
    parsed = urlparse(path)
    if not parsed.scheme:
        return None
    return f"{parsed.scheme}://"


def _merge_kwargs(base: Mapping[str, object], extra: Mapping[str, object]) -> dict[str, object]:
    merged = dict(base)
    merged.update(extra)
    return merged


def _effective_skip_metadata(location: DatasetLocation, scan: DataFusionScanOptions) -> bool:
    return scan.skip_metadata and not _has_metadata_sidecars(location.path)


def _has_metadata_sidecars(path: str | Path) -> bool:
    if isinstance(path, str) and "://" in path:
        return False
    base = ensure_path(path)
    if not base.exists():
        return False
    if base.is_dir():
        return (base / "_common_metadata").exists() or (base / "_metadata").exists()
    return False

def _resolve_cache_policy(
    options: DataFusionRegistryOptions,
    *,
    cache_policy: DataFusionCachePolicy | None,
    runtime_profile: DataFusionRuntimeProfile | None,
) -> _DataFusionCacheSettings:
    from datafusion_engine.dataset.registration_cache import _resolve_cache_policy as _resolve_cache

    return _resolve_cache(
        options,
        cache_policy=cache_policy,
        runtime_profile=runtime_profile,
    )


def _ensure_catalog_schema(
    ctx: SessionContext,
    *,
    catalog: str,
    schema: str,
    caches: DatasetCaches | None = None,
) -> None:
    from datafusion_engine.dataset.registration_listing import (
        _ensure_catalog_schema as _ensure_schema,
    )

    _ensure_schema(
        ctx,
        catalog=catalog,
        schema=schema,
        caches=caches,
    )


def _register_dataset_with_context(
    context: DataFusionRegistrationContext,
) -> DataFrame:
    from datafusion_engine.dataset.registration_listing import (
        _register_dataset_with_context as _register_dataset,
    )

    return _register_dataset(context)


def _maybe_cache(
    context: DataFusionRegistrationContext,
    df: DataFrame,
) -> DataFrame:
    from datafusion_engine.dataset.registration_cache import _maybe_cache as _cache_df

    return _cache_df(context, df)


def cached_dataset_names(
    ctx: SessionContext,
    *,
    caches: DatasetCaches | None = None,
) -> tuple[str, ...]:
    """Return cached dataset names for a session context."""
    from datafusion_engine.dataset.registration_cache import (
        cached_dataset_names as _cached_dataset_names,
    )

    return _cached_dataset_names(ctx, caches=caches)

__all__ = [
    "DataFusionCachePolicy",
    "DataFusionRegistryOptions",
    "cached_dataset_names",
    "dataset_input_plugin",
    "input_plugin_prefixes",
    "register_dataset_df",
]

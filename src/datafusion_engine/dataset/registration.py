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

from __future__ import annotations

import importlib
import inspect
import logging
import re
import time
from collections.abc import Callable, Mapping, Sequence
from contextlib import suppress
from dataclasses import dataclass, replace
from pathlib import Path
from typing import TYPE_CHECKING, Literal, cast
from urllib.parse import urlparse

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.fs as pafs
from datafusion import SessionContext, SQLOptions, col
from datafusion.catalog import Catalog, Schema
from datafusion.dataframe import DataFrame
from datafusion.expr import Expr

from arrow_utils.core.ordering import OrderingLevel
from arrow_utils.core.schema_constants import DEFAULT_VALUE_META
from core.config_base import FingerprintableConfig, config_fingerprint
from core_types import ensure_path
from datafusion_engine.arrow.abi import schema_to_dict
from datafusion_engine.arrow.interop import SchemaLike
from datafusion_engine.arrow.metadata import (
    ordering_from_schema,
    schema_constraints_from_metadata,
)
from datafusion_engine.catalog.introspection import (
    introspection_cache_for_ctx,
    invalidate_introspection_cache,
)
from datafusion_engine.dataset.registry import (
    DatasetCatalog,
    DatasetLocation,
    resolve_datafusion_scan_options,
    resolve_dataset_schema,
    resolve_delta_cdf_policy,
)
from datafusion_engine.dataset.resolution import (
    DatasetResolution,
    DatasetResolutionRequest,
    resolve_dataset_provider,
)
from datafusion_engine.delta.scan_config import (
    delta_scan_config_snapshot_from_options,
)
from datafusion_engine.identity import schema_identity_hash
from datafusion_engine.io.adapter import DataFusionIOAdapter
from datafusion_engine.lineage.datafusion import referenced_tables_from_plan
from datafusion_engine.lineage.diagnostics import record_artifact
from datafusion_engine.plan.bundle import PlanBundleOptions, build_plan_bundle
from datafusion_engine.schema.contracts import (
    EvolutionPolicy,
    schema_contract_from_table_schema_contract,
    table_constraint_definitions,
    table_constraints_from_location,
)
from datafusion_engine.schema.introspection import (
    SchemaIntrospector,
    table_constraint_rows,
)
from datafusion_engine.schema.validation import _datafusion_type_name
from datafusion_engine.session.runtime import (
    DATAFUSION_MAJOR_VERSION,
    DATAFUSION_RUNTIME_SETTINGS_SKIP_VERSION,
    record_runtime_setting_override,
    schema_introspector_for_profile,
)
from datafusion_engine.sql import options as _sql_options
from datafusion_engine.tables.metadata import (
    TableProviderCapsule,
    TableProviderMetadata,
    record_table_provider_metadata,
    table_provider_metadata,
)
from datafusion_engine.views.graph import arrow_schema_from_df
from schema_spec.system import (
    DataFusionScanOptions,
    DatasetSpec,
    DeltaScanOptions,
    ParquetColumnOptions,
    TableSchemaContract,
    dataset_spec_from_schema,
    ddl_fingerprint_from_definition,
    make_dataset_spec,
)
from serde_msgspec import to_builtins
from storage.deltalake import DeltaCdfOptions
from utils.validation import find_missing, validate_required_items

if TYPE_CHECKING:
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile

DEFAULT_CACHE_MAX_COLUMNS = 64
_CACHED_DATASETS: dict[int, set[str]] = {}
_REGISTERED_CATALOGS: dict[int, set[str]] = {}
_REGISTERED_SCHEMAS: dict[int, set[tuple[str, str]]] = {}
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


class DatasetInputSource:
    """Resolve dataset handles into registered tables."""

    def __init__(
        self,
        ctx: SessionContext,
        *,
        catalog: DatasetCatalog,
        runtime_profile: DataFusionRuntimeProfile | None,
    ) -> None:
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

        Returns
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

        Returns
        -------
        datafusion.dataframe.DataFrame
            DataFrame registered for the dataset handle.

        Raises
        ------
        ValueError
            Raised when the dataset handle is not recognized.
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
            options=DatasetRegistrationOptions(runtime_profile=self._runtime_profile),
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

    Returns
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
            DatasetInputSource(
                ctx,
                catalog=catalog,
                runtime_profile=runtime_profile,
            )
        )

    return _install


def input_plugin_prefixes() -> tuple[str, ...]:
    """Return the registered input plugin prefixes.

    Returns
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

        Returns
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

        Returns
        -------
        str
            Deterministic fingerprint for the cache policy.
        """
        return config_fingerprint(self.fingerprint_payload())


@dataclass(frozen=True)
class DataFusionCacheSettings:
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
    cache: DataFusionCacheSettings
    runtime_profile: DataFusionRuntimeProfile | None = None


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

    Returns
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

    Returns
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

    Returns
    -------
    DatasetLocation
        Dataset location with default scan options applied.
    """
    updated = location
    if location.datafusion_scan is not None:
        return updated
    schema: SchemaLike | None = None
    try:
        schema = resolve_dataset_schema(location)
    except (RuntimeError, TypeError, ValueError):
        schema = None
    defaults = _default_scan_options_for_dataset(name, schema=schema)
    if defaults is None:
        return updated
    return replace(updated, datafusion_scan=defaults)


def resolve_registry_options(location: DatasetLocation) -> DataFusionRegistryOptions:
    """Resolve DataFusion registration hints for a dataset location.

    Raises
    ------
    ValueError
        Raised when an unsupported DataFusion provider is configured.

    Returns
    -------
    DataFusionRegistryOptions
        Registration options derived from the dataset location.
    """
    scan = resolve_datafusion_scan_options(location)
    schema = resolve_dataset_schema(location)
    provider = location.datafusion_provider
    format_name = location.format or "delta"
    cdf_policy = resolve_delta_cdf_policy(location)
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
    if (
        provider is None
        and location.dataset_spec is not None
        and location.dataset_spec.dataset_kind == "delta_cdf"
    ):
        provider = "delta_cdf"
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
    hardening = runtime_profile.schema_hardening
    if hardening is not None:
        return hardening.enable_view_types
    return runtime_profile.schema_hardening_name == "arrow_performance"


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


def _ddl_identifier_part(name: str) -> str:
    if _DDL_IDENTIFIER_RE.match(name):
        return name
    escaped = name.replace('"', '""')
    return f'"{escaped}"'


def _ddl_identifier(name: str) -> str:
    parts = [part for part in name.split(".") if part]
    if not parts:
        return _ddl_identifier_part(name)
    return ".".join(_ddl_identifier_part(part) for part in parts)


def _ddl_string_literal(value: str) -> str:
    escaped = value.replace("'", "''")
    return f"'{escaped}'"


def _ddl_order_clause(order: Sequence[tuple[str, str]]) -> str | None:
    if not order:
        return None
    fragments: list[str] = []
    for name, direction in order:
        direction_token: str | None = None
        if direction:
            normalized = direction.strip().lower()
            if normalized.startswith("asc"):
                direction_token = "ASC"
            elif normalized.startswith("desc"):
                direction_token = "DESC"
        if direction_token is None:
            fragments.append(_ddl_identifier(name))
        else:
            fragments.append(f"{_ddl_identifier(name)} {direction_token}")
    return f"WITH ORDER ({', '.join(fragments)})"


def _ddl_options_clause(options: Mapping[str, str]) -> str | None:
    if not options:
        return None
    items = ", ".join(
        f"{_ddl_string_literal(key)} {_ddl_string_literal(value)}"
        for key, value in sorted(options.items(), key=lambda item: item[0])
    )
    return f"OPTIONS ({items})"


def _ddl_schema_components(
    *,
    schema: pa.Schema | None,
    scan: DataFusionScanOptions | None,
) -> tuple[
    pa.Schema | None,
    tuple[str, ...],
    tuple[str, ...],
    tuple[str, ...],
    dict[str, str],
]:
    contract = _resolve_table_schema_contract(
        schema=schema,
        scan=scan,
        partition_cols=scan.partition_cols if scan is not None else None,
    )
    if contract is not None:
        schema = contract.file_schema
        partition_cols = contract.partition_cols
    else:
        partition_cols = scan.partition_cols if scan is not None else ()
    if schema is None:
        return None, (), (), (), {}
    field_names = set(schema.names)
    fields = list(schema)
    for name, dtype in partition_cols:
        if name in field_names:
            continue
        fields.append(pa.field(name, dtype, nullable=False))
        field_names.add(name)
    merged_schema = pa.schema(fields, metadata=schema.metadata)
    required_non_null, key_fields = schema_constraints_from_metadata(schema.metadata)
    if partition_cols:
        required_non_null = tuple(
            dict.fromkeys([*required_non_null, *(name for name, _ in partition_cols)])
        )
    defaults = _expected_column_defaults(merged_schema)
    return (
        merged_schema,
        tuple(name for name, _dtype in partition_cols),
        tuple(required_non_null),
        tuple(key_fields),
        defaults,
    )


def _ddl_column_definitions(
    *,
    schema: pa.Schema,
    required_non_null: Sequence[str],
    key_fields: Sequence[str],
    defaults: Mapping[str, str],
) -> tuple[str, ...]:
    required_set = set(required_non_null)
    available_names = set(schema.names)
    filtered_keys = tuple(name for name in key_fields if name in available_names)
    lines: list[str] = []
    for field in schema:
        dtype_name = _sql_type_name(field.type)
        fragments = [_ddl_identifier(field.name), dtype_name]
        if not field.nullable or field.name in required_set:
            fragments.append("NOT NULL")
        default_value = defaults.get(field.name)
        if default_value is not None:
            literal = _sql_literal_for_field(default_value, dtype=field.type)
            if literal is not None:
                fragments.append(f"DEFAULT {literal}")
        lines.append(" ".join(fragments))
    if filtered_keys:
        keys = ", ".join(_ddl_identifier(name) for name in filtered_keys)
        lines.append(f"PRIMARY KEY ({keys})")
    return tuple(lines)


def _ddl_options_payload(scan: DataFusionScanOptions | None) -> dict[str, str]:
    if scan is None:
        return {}
    options: dict[str, str] = {}
    if scan.parquet_column_options is not None:
        options.update(scan.parquet_column_options.external_table_options())
    if scan.collect_statistics is not None and "statistics_enabled" not in options:
        options["statistics_enabled"] = str(scan.collect_statistics).lower()
    if scan.schema_force_view_types is not None:
        options["schema_force_view_types"] = str(scan.schema_force_view_types).lower()
    if scan.skip_metadata is not None:
        options["skip_metadata"] = str(scan.skip_metadata).lower()
    if scan.skip_arrow_metadata is not None:
        options["skip_arrow_metadata"] = str(scan.skip_arrow_metadata).lower()
    if scan.binary_as_string is not None:
        options["binary_as_string"] = str(scan.binary_as_string).lower()
    return options


def _ddl_prefix(*, unbounded: bool) -> str:
    return "CREATE UNBOUNDED EXTERNAL TABLE" if unbounded else "CREATE EXTERNAL TABLE"


def _ddl_ordering_keys(
    scan: DataFusionScanOptions | None,
    merged_schema: pa.Schema | None,
) -> tuple[tuple[str, str], ...]:
    if scan is not None and scan.file_sort_order:
        return scan.file_sort_order
    if merged_schema is None:
        return ()
    metadata_ordering = ordering_from_schema(merged_schema)
    if metadata_ordering.level == OrderingLevel.EXPLICIT and metadata_ordering.keys:
        return metadata_ordering.keys
    return ()


def _ddl_constraints(
    key_fields: Sequence[str],
    merged_schema: pa.Schema | None,
) -> tuple[str, ...]:
    if not key_fields or merged_schema is None:
        return ()
    filtered = tuple(name for name in key_fields if name in merged_schema.names)
    if not filtered:
        return ()
    return (f"PRIMARY KEY ({', '.join(filtered)})",)


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


def _ddl_statement(request: _ExternalTableDdlRequest) -> str:
    ddl = f"{_ddl_prefix(unbounded=request.unbounded)} {_ddl_identifier(request.name)}"
    if request.column_defs:
        ddl = f"{ddl} ({', '.join(request.column_defs)})"
    ddl = (
        f"{ddl} STORED AS {request.format_name} LOCATION "
        f"{_ddl_string_literal(str(request.location.path))}"
    )
    if request.partition_cols:
        ddl = (
            f"{ddl} PARTITIONED BY "
            f"({', '.join(_ddl_identifier(name) for name in request.partition_cols)})"
        )
    order_clause = _ddl_order_clause(request.ordering)
    if order_clause:
        ddl = f"{ddl} {order_clause}"
    options_clause = _ddl_options_clause(dict(request.options))
    if options_clause:
        ddl = f"{ddl} {options_clause}"
    return ddl


def _external_table_ddl(
    *,
    name: str,
    location: DatasetLocation,
    schema: pa.Schema | None,
    scan: DataFusionScanOptions | None,
) -> tuple[str, tuple[str, ...], tuple[str, ...], tuple[str, ...], dict[str, str]]:
    merged_schema, partition_cols, required_non_null, key_fields, defaults = _ddl_schema_components(
        schema=schema,
        scan=scan,
    )
    column_defs = (
        _ddl_column_definitions(
            schema=merged_schema,
            required_non_null=required_non_null,
            key_fields=key_fields,
            defaults=defaults,
        )
        if merged_schema is not None
        else ()
    )
    format_name = (location.format or "parquet").upper()
    ddl = _ddl_statement(
        _ExternalTableDdlRequest(
            name=name,
            location=location,
            format_name=format_name,
            column_defs=column_defs,
            partition_cols=partition_cols,
            ordering=_ddl_ordering_keys(scan, merged_schema),
            options=_ddl_options_payload(scan),
            unbounded=bool(scan is not None and scan.unbounded),
        )
    )
    constraints = _ddl_constraints(key_fields, merged_schema)
    return ddl, partition_cols, constraints, required_non_null, defaults


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


def _build_pyarrow_dataset(
    location: DatasetLocation,
    *,
    schema: object | None,
) -> object:
    arrow_schema = cast("pa.Schema | None", schema)
    if location.files:
        return ds.dataset(
            list(location.files),
            format=location.format,
            filesystem=location.filesystem,
            schema=arrow_schema,
        )
    return ds.dataset(
        str(Path(location.path)),
        format=location.format,
        filesystem=location.filesystem,
        partitioning=location.partitioning,
        schema=arrow_schema,
    )


def _scan_details(scan: DataFusionScanOptions) -> dict[str, object]:
    return {
        "partition_cols": [(col, str(dtype)) for col, dtype in scan.partition_cols],
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
    for field in schema:
        meta = field.metadata or {}
        default_value = meta.get(DEFAULT_VALUE_META)
        if default_value is None:
            continue
        defaults[field.name] = default_value.decode("utf-8", errors="replace")
    return defaults


def _validate_constraints_and_defaults(
    context: DataFusionRegistrationContext,
    *,
    enable_information_schema: bool,
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
        introspector = schema_introspector_for_profile(context.runtime_profile, context.ctx)
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
    module = None
    for module_name in ("datafusion._internal", "datafusion_ext"):
        try:
            candidate = importlib.import_module(module_name)
        except ImportError:
            continue
        if hasattr(candidate, "install_schema_evolution_adapter_factory"):
            module = candidate
            break
    if module is None:  # pragma: no cover - optional dependency
        msg = "Schema evolution adapter requires datafusion._internal or datafusion_ext."
        raise RuntimeError(msg)
    installer = getattr(module, "install_schema_evolution_adapter_factory", None)
    if not callable(installer):
        msg = "Schema evolution adapter installer is unavailable in the extension module."
        raise TypeError(msg)
    installer(ctx)


def _resolve_dataset_spec(name: str, location: DatasetLocation) -> DatasetSpec | None:
    if location.dataset_spec is not None:
        return location.dataset_spec
    if location.table_spec is not None:
        return make_dataset_spec(table_spec=location.table_spec)
    schema = resolve_dataset_schema(location)
    if schema is None:
        return None
    return dataset_spec_from_schema(name, schema)


@dataclass(frozen=True)
class DatasetRegistrationOptions:
    """Configure DataFusion dataset registration."""

    cache_policy: DataFusionCachePolicy | None = None
    runtime_profile: DataFusionRuntimeProfile | None = None
    overwrite: bool = True


def register_dataset_df(
    ctx: SessionContext,
    *,
    name: str,
    location: DatasetLocation,
    options: DatasetRegistrationOptions | None = None,
) -> DataFrame:
    """Register a dataset location with DataFusion and return a DataFrame.

    Returns
    -------
    datafusion.dataframe.DataFrame
        DataFusion DataFrame for the registered dataset.

    """
    from datafusion_engine.session.helpers import deregister_table
    from datafusion_engine.tables.registration import (
        TableRegistrationRequest,
        register_table,
    )

    resolved = options or DatasetRegistrationOptions()
    existing = False
    if resolved.overwrite:
        from datafusion_engine.schema.introspection import table_names_snapshot

        existing = name in table_names_snapshot(ctx)
        deregister_table(ctx, name)
    df = register_table(
        ctx,
        TableRegistrationRequest(
            name=name,
            location=location,
            cache_policy=resolved.cache_policy,
            runtime_profile=resolved.runtime_profile,
        ),
    )
    scan = location.datafusion_scan
    if (
        existing
        and scan is not None
        and scan.listing_mutable
        and resolved.runtime_profile is not None
    ):
        from datafusion_engine.lineage.diagnostics import record_artifact

        record_artifact(
            resolved.runtime_profile,
            "datafusion_listing_refresh_v1",
            {
                "name": name,
                "path": str(location.path),
                "format": location.format,
                "event_time_unix_ms": int(time.time() * 1000),
            },
        )
    return df


@dataclass(frozen=True)
class DatasetRegistration:
    """Define inputs for dataset registration."""

    name: str
    spec: DatasetSpec
    location: str
    file_format: str = "delta"


def register_dataset_spec(
    ctx: SessionContext,
    registration: DatasetRegistration,
    *,
    runtime_profile: DataFusionRuntimeProfile | None = None,
) -> None:
    """Register a dataset using non-DDL DataFusion APIs.

    Parameters
    ----------
    ctx : SessionContext
        DataFusion session context to register the table in.
    registration : DatasetRegistration
        Registration inputs including name, spec, location, and file format.
    runtime_profile : DataFusionRuntimeProfile | None
        Optional runtime profile used for registration defaults.

    Notes
    -----
    This function preserves the DatasetSpec's scan configuration without
    executing SQL DDL statements.

    Examples
    --------
    Register a Delta dataset spec:

    >>> from datafusion_engine.session.runtime import DataFusionRuntimeProfile
    >>> from schema_spec.system import DatasetSpec
    >>> ctx = DataFusionRuntimeProfile().ephemeral_context()
    >>> spec = DatasetSpec(table_spec=table_spec)
    >>> register_dataset_spec(
    ...     ctx,
    ...     DatasetRegistration(
    ...         name="events",
    ...         spec=spec,
    ...         location="s3://bucket/events/",
    ...     ),
    ... )
    """
    location = DatasetLocation(
        path=registration.location,
        format=registration.file_format,
        dataset_spec=registration.spec,
    )
    register_dataset_df(
        ctx,
        name=registration.name,
        location=location,
        options=DatasetRegistrationOptions(runtime_profile=runtime_profile),
    )


def _build_registration_context(
    ctx: SessionContext,
    *,
    name: str,
    location: DatasetLocation,
    cache_policy: DataFusionCachePolicy | None,
    runtime_profile: DataFusionRuntimeProfile | None,
) -> DataFusionRegistrationContext:
    location = _apply_scan_defaults(name, location)
    runtime_profile = _prepare_runtime_profile(runtime_profile, ctx=ctx)
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
    # Schema adapters are attached at SessionContext creation via the runtime
    # profile's physical expression adapter factory. When enabled, scan-time
    # adapters handle schema drift resolution at the TableProvider boundary.
    schema_adapter_enabled = (
        runtime_profile is not None and runtime_profile.enable_schema_evolution_adapter
    )
    metadata = TableProviderMetadata(
        table_name=name,
        ddl=None,
        constraints=table_constraint_definitions(table_constraints_from_location(location)),
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
    record_table_provider_metadata(id(ctx), metadata=metadata)
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
) -> DataFusionRuntimeProfile | None:
    if runtime_profile is None:
        return None
    runtime_profile = _populate_schema_adapter_factories(runtime_profile)
    _ensure_catalog_schema(
        ctx,
        catalog=runtime_profile.default_catalog,
        schema=runtime_profile.default_schema,
    )
    return runtime_profile


def _resolve_registry_options(
    _name: str,
    location: DatasetLocation,
    *,
    runtime_profile: DataFusionRuntimeProfile | None,
) -> DataFusionRegistryOptions:
    options = resolve_registry_options(location)
    return _apply_runtime_scan_hardening(options, runtime_profile=runtime_profile)


def _invalidate_information_schema_cache(
    runtime_profile: DataFusionRuntimeProfile | None,
    ctx: SessionContext,
) -> None:
    if runtime_profile is None or not runtime_profile.enable_information_schema:
        return
    invalidate_introspection_cache(ctx)
    introspector = schema_introspector_for_profile(runtime_profile, ctx)
    introspector.invalidate_cache()


def _register_dataset_with_context(context: DataFusionRegistrationContext) -> DataFrame:
    if context.options.provider == "listing":
        df = _register_listing_table(context)
    else:
        df = _register_delta_provider(context)
    scan = context.options.scan
    projection_exprs = scan.projection_exprs if scan is not None else ()
    if not projection_exprs and context.options.schema is not None:
        projection_exprs = _projection_exprs_for_schema(
            actual_columns=df.schema().names,
            expected_schema=pa.schema(context.options.schema),
        )
    if projection_exprs:
        df = _apply_projection_exprs(
            context.ctx,
            table_name=context.name,
            projection_exprs=projection_exprs,
            sql_options=_sql_options.sql_options_for_profile(context.runtime_profile),
            runtime_profile=context.runtime_profile,
        )
    _invalidate_information_schema_cache(context.runtime_profile, context.ctx)
    _validate_schema_contracts(context)
    return df


@dataclass(frozen=True)
class _ListingRegistrationState:
    provider: object | None
    registration_mode: str
    ddl: str | None


@dataclass(frozen=True)
class _ListingMetadataUpdate:
    partition_cols: tuple[str, ...]
    constraints: tuple[str, ...]
    defaults: dict[str, str]
    ddl: str | None
    scan: DataFusionScanOptions | None


def _record_listing_metadata(
    *,
    context: DataFusionRegistrationContext,
    update: _ListingMetadataUpdate,
) -> None:
    metadata = table_provider_metadata(id(context.ctx), table_name=context.name)
    if metadata is None:
        return
    updated = replace(
        metadata,
        ddl=update.ddl if update.ddl is not None else metadata.ddl,
        constraints=update.constraints,
        default_values=dict(update.defaults),
        partition_columns=update.partition_cols,
        unbounded=update.scan.unbounded if update.scan is not None else metadata.unbounded,
    )
    record_table_provider_metadata(id(context.ctx), metadata=updated)


def _register_listing_from_files(
    context: DataFusionRegistrationContext,
    *,
    scan: DataFusionScanOptions | None,
) -> _ListingRegistrationState:
    provider = _build_pyarrow_dataset(context.location, schema=context.options.schema)
    adapter = DataFusionIOAdapter(ctx=context.ctx, profile=context.runtime_profile)
    adapter.register_table_provider(context.name, provider)
    merged_schema, partition_cols, _required_non_null, key_fields, defaults = (
        _ddl_schema_components(
            schema=cast("pa.Schema | None", context.options.schema),
            scan=scan,
        )
    )
    constraints = _ddl_constraints(key_fields, merged_schema)
    _record_listing_metadata(
        context=context,
        update=_ListingMetadataUpdate(
            partition_cols=partition_cols,
            constraints=constraints,
            defaults=defaults,
            ddl=None,
            scan=scan,
        ),
    )
    return _ListingRegistrationState(
        provider=provider,
        registration_mode="pyarrow_dataset",
        ddl=None,
    )


def _execute_external_ddl(
    ctx: SessionContext,
    *,
    ddl: str,
    sql_options: SQLOptions,
) -> None:
    allow_statements = True
    resolved_options = sql_options.with_allow_statements(allow_statements)
    try:
        ddl_df = ctx.sql_with_options(ddl, resolved_options)
    except (RuntimeError, TypeError, ValueError) as exc:
        msg = f"External table DDL failed: {exc}"
        raise ValueError(msg) from exc
    if ddl_df is None:
        msg = "External table DDL did not return a DataFusion DataFrame."
        raise ValueError(msg)
    ddl_df.collect()


def _register_listing_from_ddl(
    context: DataFusionRegistrationContext,
    *,
    sql_options: SQLOptions,
) -> _ListingRegistrationState:
    scan = context.options.scan
    ddl, partition_cols, constraints, _required_non_null, defaults = _external_table_ddl(
        name=context.name,
        location=context.location,
        schema=cast("pa.Schema | None", context.options.schema),
        scan=scan,
    )
    _record_listing_metadata(
        context=context,
        update=_ListingMetadataUpdate(
            partition_cols=partition_cols,
            constraints=constraints,
            defaults=defaults,
            ddl=ddl,
            scan=scan,
        ),
    )
    _execute_external_ddl(context.ctx, ddl=ddl, sql_options=sql_options)
    return _ListingRegistrationState(
        provider=None,
        registration_mode="external_table_ddl",
        ddl=ddl,
    )


def _register_listing_table(context: DataFusionRegistrationContext) -> DataFrame:
    runtime_profile = context.runtime_profile
    scan = context.options.scan
    sql_options = _sql_options.sql_options_for_profile(runtime_profile)
    _apply_scan_settings(context.ctx, scan=scan, sql_options=sql_options)
    state = (
        _register_listing_from_files(context, scan=scan)
        if context.location.files
        else _register_listing_from_ddl(context, sql_options=sql_options)
    )
    df = context.ctx.table(context.name)
    _, _, fingerprint_details = _update_table_provider_fingerprints(
        context.ctx,
        name=context.name,
        schema=df.schema(),
    )
    details: dict[str, object] = {
        "path": str(context.location.path),
        "format": context.location.format,
        "partitioning": context.location.partitioning,
        "read_options": dict(context.options.read_options),
        "registration_mode": state.registration_mode,
    }
    if state.ddl is not None:
        details["ddl"] = state.ddl
    if scan is not None:
        details.update(_scan_details(scan))
    if fingerprint_details:
        details.update(fingerprint_details)
    _record_table_provider_artifact(
        runtime_profile,
        artifact=_TableProviderArtifact(
            name=context.name,
            provider=state.provider,
            provider_kind=state.registration_mode,
            source=None,
            details=details,
        ),
    )
    return _maybe_cache(context, df)


@dataclass(frozen=True)
class _DeltaProviderArtifactContext:
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


def _delta_pruning_predicate(
    context: DataFusionRegistrationContext,
) -> tuple[str | None, str | None]:
    dataset_spec = _resolve_dataset_spec(context.name, context.location)
    if dataset_spec is None:
        return None, None
    query_spec = dataset_spec.query()
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


def _build_delta_provider_registration(
    context: DataFusionRegistrationContext,
) -> _DeltaProviderRegistration:
    location = context.location
    predicate_sql, predicate_error = _delta_pruning_predicate(context)
    resolution = resolve_dataset_provider(
        DatasetResolutionRequest(
            ctx=context.ctx,
            location=location,
            runtime_profile=context.runtime_profile,
            name=context.name,
            predicate=predicate_sql,
        )
    )
    combined_error = predicate_error or resolution.predicate_error
    return _DeltaProviderRegistration(
        resolution=resolution,
        predicate_sql=predicate_sql,
        predicate_error=combined_error,
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


def _register_delta_provider(context: DataFusionRegistrationContext) -> DataFrame:
    location = context.location
    registration = _build_delta_provider_registration(context)
    resolution = registration.resolution
    provider = resolution.provider
    adapter = DataFusionIOAdapter(ctx=context.ctx, profile=context.runtime_profile)
    provider_to_register = _provider_for_registration(provider)
    if resolution.provider_kind == "delta_cdf":
        adapter.register_delta_cdf_provider(context.name, provider_to_register)
    else:
        adapter.register_delta_table_provider(context.name, provider_to_register)
    df = context.ctx.table(context.name)
    schema_identity_hash_value, ddl_fingerprint, fingerprint_details = (
        _update_table_provider_fingerprints(
            context.ctx,
            name=context.name,
            schema=df.schema(),
        )
    )
    if resolution.provider_kind == "delta_cdf":
        artifact_details = _delta_cdf_artifact_payload(location, resolution=resolution)
        if fingerprint_details:
            artifact_details.update(fingerprint_details)
        _record_table_provider_artifact(
            context.runtime_profile,
            artifact=_TableProviderArtifact(
                name=context.name,
                provider=provider,
                provider_kind="cdf_table_provider",
                source=None,
                details=artifact_details,
            ),
        )
        _update_table_provider_capabilities(
            context.ctx,
            name=context.name,
            supports_cdf=True,
        )
        _record_delta_cdf_artifact(
            context.runtime_profile,
            artifact=DeltaCdfArtifact(
                name=context.name,
                path=str(location.path),
                provider="table_provider",
                options=location.delta_cdf_options,
                log_storage_options=location.delta_log_storage_options,
                snapshot=resolution.delta_snapshot,
            ),
        )
        _invalidate_information_schema_cache(context.runtime_profile, context.ctx)
        return _maybe_cache(context, df)
    artifact_details = _delta_provider_artifact_payload(
        location,
        context=_DeltaProviderArtifactContext(
            delta_scan=resolution.delta_scan_options,
            delta_scan_effective=resolution.delta_scan_effective,
            delta_scan_snapshot=resolution.delta_scan_snapshot,
            delta_scan_identity_hash=resolution.delta_scan_identity_hash,
            snapshot=resolution.delta_snapshot,
            registration_path="provider",
            predicate=registration.predicate_sql,
            predicate_error=registration.predicate_error,
            add_actions=resolution.add_actions,
        ),
    )
    if fingerprint_details:
        artifact_details.update(fingerprint_details)
    _record_table_provider_artifact(
        context.runtime_profile,
        artifact=_TableProviderArtifact(
            name=context.name,
            provider=provider,
            provider_kind="delta_table_provider",
            source=None,
            details=artifact_details,
        ),
    )
    _update_table_provider_scan_config(
        context.ctx,
        name=context.name,
        delta_scan_snapshot=resolution.delta_scan_snapshot,
        delta_scan_identity_hash=resolution.delta_scan_identity_hash,
        delta_scan_effective=resolution.delta_scan_effective,
    )
    if resolution.delta_snapshot is not None:
        from datafusion_engine.delta.observability import (
            DeltaSnapshotArtifact,
            record_delta_snapshot,
        )

        record_delta_snapshot(
            context.runtime_profile,
            artifact=DeltaSnapshotArtifact(
                table_uri=str(location.path),
                snapshot=resolution.delta_snapshot,
                dataset_name=context.name,
                schema_identity_hash=schema_identity_hash_value,
                ddl_fingerprint=ddl_fingerprint,
            ),
        )
    _update_table_provider_capabilities(
        context.ctx,
        name=context.name,
        supports_insert=True,
    )
    return _maybe_cache(context, df)


def _delta_provider_artifact_payload(
    location: DatasetLocation,
    *,
    context: _DeltaProviderArtifactContext,
) -> dict[str, object]:
    pruned_files_count = len(context.add_actions) if context.add_actions is not None else None
    pruning_applied = context.add_actions is not None
    delta_scan_ignored = context.registration_path == "ddl" and context.delta_scan is not None
    return {
        "path": str(location.path),
        "registration_path": context.registration_path,
        "delta_version": location.delta_version,
        "delta_timestamp": location.delta_timestamp,
        "delta_log_storage_options": (
            dict(location.delta_log_storage_options) if location.delta_log_storage_options else None
        ),
        "delta_storage_options": (
            dict(location.storage_options) if location.storage_options else None
        ),
        "delta_scan": _delta_scan_payload(context.delta_scan),
        "delta_scan_effective": context.delta_scan_effective,
        "delta_scan_snapshot": _delta_scan_snapshot_payload(context.delta_scan_snapshot),
        "delta_scan_identity_hash": context.delta_scan_identity_hash,
        "delta_snapshot": context.snapshot,
        "delta_scan_ignored": delta_scan_ignored,
        "delta_pruning_predicate": context.predicate,
        "delta_pruning_error": context.predicate_error,
        "delta_pruning_applied": pruning_applied,
        "delta_pruned_files": pruned_files_count,
    }


def _delta_cdf_artifact_payload(
    location: DatasetLocation,
    *,
    resolution: DatasetResolution,
) -> dict[str, object]:
    return {
        "path": str(location.path),
        "delta_log_storage_options": (
            dict(location.delta_log_storage_options) if location.delta_log_storage_options else None
        ),
        "delta_storage_options": (
            dict(location.storage_options) if location.storage_options else None
        ),
        "delta_snapshot": resolution.delta_snapshot,
        "cdf_options": _cdf_options_payload(location.delta_cdf_options),
    }


def _delta_scan_payload(options: DeltaScanOptions | None) -> dict[str, object] | None:
    if options is None:
        return None
    snapshot = delta_scan_config_snapshot_from_options(options)
    if snapshot is None:
        return None
    return cast("dict[str, object]", to_builtins(snapshot, str_keys=True))


def _delta_scan_snapshot_payload(snapshot: object | None) -> dict[str, object] | None:
    if snapshot is None:
        return None
    if isinstance(snapshot, Mapping):
        return {str(key): value for key, value in snapshot.items()}
    payload = to_builtins(snapshot, str_keys=True)
    if isinstance(payload, Mapping):
        return {str(key): value for key, value in payload.items()}
    return None


def provider_capsule_id(provider: object) -> str:
    """Return a stable identifier for a DataFusion table provider capsule.

    Returns
    -------
    str
        Capsule identifier string.

    Raises
    ------
    AttributeError
        Raised when the provider does not expose a capsule.
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


def _record_table_provider_artifact(
    runtime_profile: DataFusionRuntimeProfile | None,
    *,
    artifact: _TableProviderArtifact,
) -> None:
    capsule_id = _provider_capsule_id(artifact.provider, source=artifact.source)
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
    record_artifact(runtime_profile, "datafusion_table_providers_v1", payload)


def _update_table_provider_capabilities(
    ctx: SessionContext,
    *,
    name: str,
    supports_insert: bool | None = None,
    supports_cdf: bool | None = None,
) -> None:
    metadata = table_provider_metadata(id(ctx), table_name=name)
    if metadata is None:
        return
    updated = replace(
        metadata,
        supports_insert=supports_insert,
        supports_cdf=supports_cdf,
    )
    record_table_provider_metadata(id(ctx), metadata=updated)


def _update_table_provider_scan_config(
    ctx: SessionContext,
    *,
    name: str,
    delta_scan_snapshot: object | None,
    delta_scan_identity_hash: str | None,
    delta_scan_effective: Mapping[str, object] | None,
) -> None:
    metadata = table_provider_metadata(id(ctx), table_name=name)
    if metadata is None:
        return
    updated = replace(
        metadata,
        delta_scan_config=_delta_scan_snapshot_payload(delta_scan_snapshot),
        delta_scan_identity_hash=delta_scan_identity_hash,
        delta_scan_effective=dict(delta_scan_effective) if delta_scan_effective else None,
    )
    record_table_provider_metadata(id(ctx), metadata=updated)


def _update_table_provider_fingerprints(
    ctx: SessionContext,
    *,
    name: str,
    schema: pa.Schema | None,
) -> tuple[str | None, str | None, dict[str, object]]:
    metadata = table_provider_metadata(id(ctx), table_name=name)
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
    record_table_provider_metadata(id(ctx), metadata=updated)
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
        runtime_profile.record_artifact(
            "projection_view_artifact_skipped_v1",
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
    from datafusion_engine.session.runtime import record_view_definition, session_runtime_hash
    from datafusion_engine.views.artifacts import (
        ViewArtifactLineage,
        ViewArtifactRequest,
        build_view_artifact_from_bundle,
    )

    schema = arrow_schema_from_df(projected)
    session_runtime = runtime_profile.session_runtime()
    bundle = build_plan_bundle(
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
    for field in expected_schema:
        if field.name in actual:
            if _supports_projection_cast(field.type):
                dtype_name = _sql_type_name(field.type)
                cast_expr = f"cast({field.name} as {dtype_name})"
                default_value = defaults.get(field.name)
                if default_value is not None:
                    literal = _sql_literal_for_field(default_value, dtype=field.type)
                    if literal is not None:
                        cast_expr = f"coalesce({cast_expr}, {literal})"
                projection_exprs.append(f"{cast_expr} as {field.name}")
            else:
                projection_exprs.append(field.name)
        elif _supports_projection_cast(field.type):
            dtype_name = _sql_type_name(field.type)
            projection_exprs.append(f"cast(NULL as {dtype_name}) as {field.name}")
        else:
            projection_exprs.append(f"NULL as {field.name}")
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

    Returns
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

    Returns
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

    Returns
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
) -> None:
    """Re-register datasets with projection scan overrides when possible."""
    if runtime_profile is None:
        return
    for table_name, columns in projection_map.items():
        if not columns:
            continue
        location = runtime_profile.dataset_location(table_name)
        if location is None:
            continue
        scan = resolve_datafusion_scan_options(location)
        if scan is None:
            continue
        projection_exprs = tuple(str(name) for name in columns if str(name))
        if scan.projection_exprs == projection_exprs:
            continue
        updated_scan = replace(scan, projection_exprs=projection_exprs)
        updated_location = replace(location, datafusion_scan=updated_scan)
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


def _resolve_table_schema_contract(
    *,
    schema: pa.Schema | None,
    scan: DataFusionScanOptions | None,
    partition_cols: Sequence[tuple[str, pa.DataType]] | None,
) -> TableSchemaContract | None:
    if scan is not None and scan.table_schema_contract is not None:
        return scan.table_schema_contract
    if schema is None:
        return None
    resolved_partitions = tuple(partition_cols or ())
    if scan is not None and scan.partition_cols:
        resolved_partitions = scan.partition_cols
    return TableSchemaContract(file_schema=schema, partition_cols=resolved_partitions)


def _validate_table_schema_contract(contract: TableSchemaContract | None) -> None:
    if contract is None:
        return
    file_names = set(contract.file_schema.names)
    seen: set[str] = set()
    for name, _dtype in contract.partition_cols:
        if name in seen:
            msg = f"TableSchema contract has duplicate partition column {name!r}."
            raise ValueError(msg)
        if name in file_names:
            msg = f"Partition column {name!r} duplicates file schema column."
            raise ValueError(msg)
        seen.add(name)


def _validate_schema_contracts(context: DataFusionRegistrationContext) -> None:
    if context.runtime_profile is None or not context.runtime_profile.enable_information_schema:
        return
    scan = context.options.scan
    contract = _resolve_table_schema_contract(
        schema=context.options.schema,
        scan=scan,
        partition_cols=scan.partition_cols if scan is not None else None,
    )
    if contract is None:
        return
    _validate_table_schema_contract(contract)
    cache = introspection_cache_for_ctx(
        context.ctx,
        sql_options=_sql_options.sql_options_for_profile(context.runtime_profile),
    )
    cache.invalidate()
    snapshot = cache.snapshot
    evolution_policy = EvolutionPolicy.STRICT
    if scan is not None and scan.projection_exprs and scan.table_schema_contract is None:
        evolution_policy = EvolutionPolicy.ADDITIVE
    schema_contract = schema_contract_from_table_schema_contract(
        table_name=context.name,
        contract=contract,
        evolution_policy=evolution_policy,
    )
    violations = schema_contract.validate_against_introspection(snapshot)
    if not violations:
        return
    details = "; ".join(str(violation) for violation in violations)
    msg = f"Schema contract validation failed for {context.name}: {details}"
    raise ValueError(msg)


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
    if not runtime_profile.enable_schema_evolution_adapter:
        return runtime_profile
    if runtime_profile.schema_adapter_factories or not runtime_profile.registry_catalogs:
        return runtime_profile
    factories: dict[str, object] = {}
    adapter_factory: object | None = None
    for catalog in runtime_profile.registry_catalogs.values():
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
    merged = dict(runtime_profile.schema_adapter_factories)
    for name, factory in factories.items():
        merged.setdefault(name, factory)
    return replace(runtime_profile, schema_adapter_factories=merged)


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
        factory = runtime_profile.schema_adapter_factories.get(dataset_name)
        if factory is not None:
            return factory
    spec = _resolve_dataset_spec(dataset_name, location)
    if spec is not None and _requires_schema_evolution_adapter(spec.evolution_spec):
        return _schema_evolution_adapter_factory()
    if runtime_profile is not None and runtime_profile.physical_expr_adapter_factory is not None:
        return runtime_profile.physical_expr_adapter_factory
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
    for module_name in ("datafusion._internal", "datafusion_ext"):
        try:
            candidate = importlib.import_module(module_name)
        except ImportError:
            continue
        if hasattr(candidate, "schema_evolution_adapter_factory"):
            module = candidate
            break
    if module is None:
        msg = "Schema evolution adapter requires datafusion._internal or datafusion_ext."
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
            {"name": name, "dtype": str(dtype)} for name, dtype in contract.partition_cols
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


def _table_provenance_snapshot(
    ctx: SessionContext,
    *,
    name: str,
    enable_information_schema: bool,
    sql_options: SQLOptions,
    runtime_profile: DataFusionRuntimeProfile | None = None,
) -> dict[str, object]:
    if runtime_profile is not None:
        introspector = schema_introspector_for_profile(runtime_profile, ctx)
    else:
        introspector = SchemaIntrospector(ctx, sql_options=sql_options)
    table_definition = introspector.table_definition(name)
    constraints: tuple[str, ...] = ()
    column_defaults: dict[str, object] | None = None
    logical_plan: str | None = None
    if enable_information_schema:
        constraints = introspector.table_constraints(name)
        column_defaults = introspector.table_column_defaults(name) or None
        logical_plan = introspector.table_logical_plan(name)
    return {
        "table_definition": table_definition,
        "table_constraints": list(constraints) if constraints else None,
        "column_defaults": column_defaults,
        "logical_plan": logical_plan,
    }


def _table_schema_snapshot(
    *,
    schema: SchemaLike | None,
    partition_cols: Sequence[tuple[str, str]] | None,
) -> dict[str, object] | None:
    if schema is None and not partition_cols:
        return None
    return {
        "file_schema": schema_to_dict(schema) if schema is not None else None,
        "partition_cols": [{"name": name, "dtype": dtype} for name, dtype in (partition_cols or ())]
        or None,
    }


def _arrow_schema_from_dataframe(
    ctx: SessionContext,
    *,
    table_name: str,
) -> pa.Schema | None:
    try:
        df = ctx.table(table_name)
    except (KeyError, RuntimeError, TypeError, ValueError):
        return None
    schema = df.schema()
    if isinstance(schema, pa.Schema):
        return schema
    to_arrow = getattr(schema, "to_arrow", None)
    if callable(to_arrow):
        resolved = to_arrow()
        if isinstance(resolved, pa.Schema):
            return resolved
    return None


def _partition_column_rows(
    ctx: SessionContext,
    *,
    table_name: str,
    runtime_profile: DataFusionRuntimeProfile | None = None,
) -> tuple[list[dict[str, object]] | None, str | None]:
    try:
        if runtime_profile is not None:
            table = schema_introspector_for_profile(
                runtime_profile,
                ctx,
            ).table_columns_with_ordinal(table_name)
        else:
            sql_options = _sql_options.sql_options_for_profile(runtime_profile)
            table = SchemaIntrospector(ctx, sql_options=sql_options).table_columns_with_ordinal(
                table_name
            )
    except (RuntimeError, TypeError, ValueError) as exc:
        return None, str(exc)
    return table, None


def _partition_columns_from_rows(
    rows: Sequence[Mapping[str, object]],
) -> tuple[list[str], dict[str, str]]:
    actual_order: list[str] = []
    actual_types: dict[str, str] = {}
    for row in rows:
        name = row.get("column_name")
        if name is None:
            continue
        name_text = str(name)
        actual_order.append(name_text)
        data_type = row.get("data_type")
        if data_type is not None:
            actual_types[name_text] = str(data_type)
    return actual_order, actual_types


def _partition_type_mismatches(
    expected_types: Mapping[str, str],
    actual_types: Mapping[str, str],
    expected_names: Sequence[str],
) -> list[dict[str, str]]:
    mismatches: list[dict[str, str]] = []
    for name in expected_names:
        expected_type = expected_types.get(name)
        actual_type = actual_types.get(name)
        if expected_type is None or actual_type is None:
            continue
        if expected_type.lower() != actual_type.lower():
            mismatches.append(
                {
                    "name": name,
                    "expected": expected_type,
                    "actual": actual_type,
                }
            )
    return mismatches


def _table_schema_partition_snapshot(
    ctx: SessionContext,
    *,
    table_name: str,
    expected_types: Mapping[str, str],
    expected_names: Sequence[str],
) -> tuple[dict[str, str], list[str], list[dict[str, str]]]:
    table_schema = _arrow_schema_from_dataframe(ctx, table_name=table_name)
    if table_schema is None:
        return {}, [], []
    table_schema_types = {field.name: str(field.type) for field in table_schema}
    missing = find_missing(expected_names, table_schema_types)
    mismatches = _partition_type_mismatches(
        expected_types,
        table_schema_types,
        expected_names,
    )
    return table_schema_types, missing, mismatches


@dataclass(frozen=True)
class _PartitionSchemaContext:
    ctx: SessionContext
    table_name: str
    enable_information_schema: bool
    runtime_profile: DataFusionRuntimeProfile | None = None


def _partition_schema_validation(
    context: _PartitionSchemaContext,
    *,
    expected_partition_cols: Sequence[tuple[str, str]] | None,
) -> dict[str, object] | None:
    if not context.enable_information_schema:
        return None
    if not expected_partition_cols:
        return None
    expected_names = [name for name, _ in expected_partition_cols]
    expected_types = {name: str(dtype) for name, dtype in expected_partition_cols}
    rows, error = _partition_column_rows(
        context.ctx,
        table_name=context.table_name,
        runtime_profile=context.runtime_profile,
    )
    if error is not None:
        return {
            "expected_partition_cols": expected_names,
            "error": error,
        }
    if rows is None:
        return {
            "expected_partition_cols": expected_names,
            "error": "Partition schema query returned no rows.",
        }
    actual_order, actual_types = _partition_columns_from_rows(rows)
    actual_partition_cols = [name for name in actual_order if name in expected_types]
    missing = find_missing(expected_names, actual_types)
    order_matches = actual_partition_cols == expected_names if actual_partition_cols else None
    type_mismatches = _partition_type_mismatches(
        expected_types,
        actual_types,
        expected_names,
    )
    table_schema_types, table_missing, table_type_mismatches = _table_schema_partition_snapshot(
        context.ctx,
        table_name=context.table_name,
        expected_types=expected_types,
        expected_names=expected_names,
    )
    return {
        "expected_partition_cols": expected_names,
        "actual_partition_cols": actual_partition_cols,
        "missing_partition_cols": missing or None,
        "partition_order_matches": order_matches,
        "expected_partition_types": expected_types,
        "actual_partition_types": actual_types or None,
        "partition_type_mismatches": type_mismatches or None,
        "table_schema_partition_types": table_schema_types or None,
        "table_schema_missing_cols": table_missing or None,
        "table_schema_type_mismatches": table_type_mismatches or None,
    }


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
    record_artifact(runtime_profile, "datafusion_delta_cdf_v1", payload)


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


def _maybe_cache(context: DataFusionRegistrationContext, df: DataFrame) -> DataFrame:
    if not context.cache.enabled or not _should_cache_df(
        df,
        cache_max_columns=context.cache.max_columns,
    ):
        return df
    if context.cache.storage == "delta_staging":
        return _register_delta_cache_for_dataset(context, df)
    return _register_memory_cache(context, df)


def _register_memory_cache(
    context: DataFusionRegistrationContext,
    df: DataFrame,
) -> DataFrame:
    cached = df.cache()
    adapter = DataFusionIOAdapter(ctx=context.ctx, profile=context.runtime_profile)
    adapter.register_view(
        context.name,
        cached,
        overwrite=True,
        temporary=False,
    )
    cached_set = _CACHED_DATASETS.setdefault(id(context.ctx), set())
    cached_set.add(context.name)
    return cached


def _register_delta_cache_for_dataset(
    context: DataFusionRegistrationContext,
    df: DataFrame,
) -> DataFrame:
    runtime_profile = context.runtime_profile
    if runtime_profile is None:
        return _register_memory_cache(context, df)
    cache_root = Path(runtime_profile.cache_root()) / "dataset_cache"
    cache_root.mkdir(parents=True, exist_ok=True)
    from datafusion_engine.tables.spec import table_spec_from_location

    cache_key = table_spec_from_location(
        context.name,
        context.location,
    ).cache_key()
    safe_name = context.name.replace("/", "_").replace(":", "_")
    cache_path = str(cache_root / f"{safe_name}__{cache_key}")
    schema = arrow_schema_from_df(df)
    schema_hash = schema_identity_hash(schema)
    partition_by = _dataset_cache_partition_by(schema, location=context.location)
    from datafusion_engine.cache.commit_metadata import (
        CacheCommitMetadataRequest,
        cache_commit_metadata,
    )
    from datafusion_engine.io.write import (
        WriteFormat,
        WriteMode,
        WritePipeline,
        WriteRequest,
    )
    from obs.otel.cache import cache_span

    commit_metadata = cache_commit_metadata(
        CacheCommitMetadataRequest(
            operation="cache_write",
            cache_policy="dataset_delta_staging",
            cache_scope="dataset",
            schema_hash=schema_hash,
            cache_key=cache_key,
        )
    )
    pipeline = WritePipeline(context.ctx, runtime_profile=runtime_profile)
    with cache_span(
        "cache.dataset.delta_staging.write",
        cache_policy="dataset_delta_staging",
        cache_scope="dataset",
        operation="write",
        attributes={
            "dataset_name": context.name,
            "cache_key": cache_key,
        },
    ) as (_span, set_result):
        result = pipeline.write(
            WriteRequest(
                source=df,
                destination=cache_path,
                format=WriteFormat.DELTA,
                mode=WriteMode.OVERWRITE,
                partition_by=partition_by,
                format_options={"commit_metadata": commit_metadata},
            )
        )
        set_result("write")
    from datafusion_engine.cache.inventory import CacheInventoryEntry, delta_report_file_count
    from datafusion_engine.cache.registry import (
        record_cache_inventory,
        register_cached_delta_table,
    )
    from datafusion_engine.dataset.registry import DatasetLocation

    location = DatasetLocation(path=cache_path, format="delta")
    register_cached_delta_table(
        context.ctx,
        runtime_profile,
        name=context.name,
        location=location,
        snapshot_version=result.delta_result.version if result.delta_result else None,
    )
    file_count = delta_report_file_count(
        result.delta_result.report if result.delta_result is not None else None
    )
    record_cache_inventory(
        runtime_profile,
        entry=CacheInventoryEntry(
            view_name=context.name,
            cache_policy="dataset_delta_staging",
            cache_path=cache_path,
            result="write",
            plan_fingerprint=None,
            plan_identity_hash=cache_key,
            schema_identity_hash=schema_hash,
            snapshot_version=result.delta_result.version if result.delta_result else None,
            snapshot_timestamp=None,
            row_count=result.rows_written,
            file_count=file_count,
            partition_by=partition_by,
        ),
        ctx=context.ctx,
    )
    return context.ctx.table(context.name)


def _dataset_cache_partition_by(
    schema: pa.Schema,
    *,
    location: DatasetLocation,
) -> tuple[str, ...]:
    from datafusion_engine.dataset.registry import resolve_delta_write_policy

    policy_partition_by: tuple[str, ...] = ()
    policy = resolve_delta_write_policy(location)
    if policy is not None:
        policy_partition_by = tuple(str(name) for name in policy.partition_by)
    available = set(schema.names)
    if policy_partition_by:
        missing = [name for name in policy_partition_by if name not in available]
        if missing:
            msg = f"Delta partition_by columns missing from schema: {sorted(missing)}."
            raise ValueError(msg)
    return tuple(name for name in policy_partition_by if name in available)


def cached_dataset_names(ctx: SessionContext) -> tuple[str, ...]:
    """Return cached dataset names for a SessionContext.

    Returns
    -------
    tuple[str, ...]
        Cached dataset names sorted in ascending order.
    """
    cached = _CACHED_DATASETS.get(id(ctx), set())
    return tuple(sorted(cached))


def _ensure_catalog_schema(ctx: SessionContext, *, catalog: str, schema: str) -> None:
    ctx_id = id(ctx)
    registered_catalogs = _REGISTERED_CATALOGS.setdefault(ctx_id, set())
    registered_schemas = _REGISTERED_SCHEMAS.setdefault(ctx_id, set())
    cat: Catalog
    if catalog in registered_catalogs:
        cat = ctx.catalog(catalog)
    else:
        try:
            cat = ctx.catalog(catalog)
        except KeyError:
            from datafusion_engine.io.adapter import DataFusionIOAdapter

            cat = Catalog.memory_catalog()
            adapter = DataFusionIOAdapter(ctx=ctx, profile=None)
            adapter.register_catalog_provider(catalog, cat)
        registered_catalogs.add(catalog)
    if (catalog, schema) in registered_schemas:
        return
    if schema not in cat.schema_names():
        cat.register_schema(schema, Schema.memory_schema())
    registered_schemas.add((catalog, schema))


def _should_cache_df(df: DataFrame, *, cache_max_columns: int | None) -> bool:
    if cache_max_columns is None:
        return True
    column_count = len(df.schema().names)
    return column_count <= cache_max_columns


def _resolve_cache_policy(
    options: DataFusionRegistryOptions,
    *,
    cache_policy: DataFusionCachePolicy | None,
    runtime_profile: DataFusionRuntimeProfile | None,
) -> DataFusionCacheSettings:
    enabled = cache_policy.enabled if cache_policy is not None else None
    max_columns = cache_policy.max_columns if cache_policy is not None else None
    storage = cache_policy.storage if cache_policy is not None else "memory"
    if runtime_profile is not None:
        if enabled is None:
            enabled = runtime_profile.cache_enabled
        if max_columns is None:
            max_columns = runtime_profile.cache_max_columns
    if enabled is None:
        enabled = True
    if max_columns is None:
        max_columns = DEFAULT_CACHE_MAX_COLUMNS
    return DataFusionCacheSettings(
        enabled=options.cache and enabled,
        max_columns=max_columns,
        storage=storage,
    )


def _call_register(
    fn: Callable[..., object],
    name: str,
    path: str | Path,
    kwargs: Mapping[str, object],
) -> None:
    if not callable(fn):
        msg = "DataFusion registration target is not callable."
        raise TypeError(msg)
    filtered = _filter_kwargs(fn, kwargs)
    fn(name, path, **filtered)


def _filter_kwargs(fn: Callable[..., object], kwargs: Mapping[str, object]) -> dict[str, object]:
    try:
        signature = inspect.signature(fn)
    except (TypeError, ValueError):
        return dict(kwargs)
    if any(param.kind == param.VAR_KEYWORD for param in signature.parameters.values()):
        return dict(kwargs)
    return {key: value for key, value in kwargs.items() if key in signature.parameters}


__all__ = [
    "DataFusionCachePolicy",
    "DataFusionCacheSettings",
    "DataFusionRegistryOptions",
    "DatasetInputSource",
    "DatasetRegistration",
    "DatasetRegistrationOptions",
    "dataset_input_plugin",
    "input_plugin_prefixes",
    "register_dataset_df",
    "register_dataset_spec",
    "resolve_registry_options",
]

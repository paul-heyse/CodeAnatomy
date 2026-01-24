"""Dataset registry bridge for DataFusion SessionContext."""

from __future__ import annotations

import importlib
import inspect
import logging
from collections.abc import Callable, Mapping, Sequence
from contextlib import suppress
from dataclasses import dataclass, replace
from pathlib import Path
from typing import TYPE_CHECKING, Literal
from urllib.parse import urlparse

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.fs as pafs
from datafusion import SessionContext, SQLOptions
from datafusion.catalog import Catalog, Schema
from datafusion.dataframe import DataFrame
from sqlglot.errors import ParseError

from arrowdsl.core.interop import SchemaLike
from arrowdsl.core.ordering import OrderingLevel
from arrowdsl.core.schema_constants import DEFAULT_VALUE_META
from arrowdsl.schema.metadata import ordering_from_schema, schema_constraints_from_metadata
from arrowdsl.schema.serialization import schema_fingerprint, schema_to_dict
from core_types import ensure_path
from datafusion_engine.runtime import schema_introspector_for_profile
from datafusion_engine.schema_introspection import (
    SchemaIntrospector,
    table_constraint_rows,
)
from datafusion_engine.schema_registry import SCHEMA_REGISTRY
from datafusion_engine.sql_options import (
    sql_options_for_profile as _sql_options_for_profile,
)
from datafusion_engine.sql_options import (
    statement_sql_options_for_profile as _statement_sql_options_for_profile,
)
from datafusion_engine.table_provider_capsule import TableProviderCapsule
from datafusion_engine.table_provider_metadata import (
    TableProviderMetadata,
    record_table_provider_metadata,
)
from ibis_engine.registry import (
    DatasetLocation,
    IbisDatasetRegistry,
    resolve_datafusion_scan_options,
    resolve_dataset_schema,
    resolve_delta_log_storage_options,
    resolve_delta_scan_options,
)
from schema_spec.specs import ExternalTableConfigOverrides
from sqlglot_tools.compat import exp, parse_one
from sqlglot_tools.optimizer import (
    ExternalTableOptionsProperty,
    build_select,
    parse_sql_strict,
    resolve_sqlglot_policy,
    sqlglot_emit,
)

if TYPE_CHECKING:
    from datafusion_engine.runtime import DataFusionRuntimeProfile
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
from storage.deltalake import DeltaCdfOptions

DEFAULT_CACHE_MAX_COLUMNS = 64
_REGISTERED_OBJECT_STORES: dict[int, set[str]] = {}
_CACHED_DATASETS: dict[int, set[str]] = {}
_REGISTERED_CATALOGS: dict[int, set[str]] = {}
_REGISTERED_SCHEMAS: dict[int, set[tuple[str, str]]] = {}
_INPUT_PLUGIN_PREFIXES = ("artifact://", "dataset://", "repo://")
_OPTIONS_TUPLE_ARITY: int = 2
_CST_EXTERNAL_TABLE_NAME = "libcst_files_v1"
_CST_PARTITION_FIELDS: tuple[str, ...] = ("repo",)
_CST_FILE_SORT_ORDER: tuple[str, ...] = ("path", "file_id")
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
_AST_FILE_SORT_ORDER: tuple[str, ...] = ("repo", "path")
_SYMTABLE_EXTERNAL_TABLE_NAME = "symtable_files_v1"
_SYMTABLE_PARTITION_FIELDS: tuple[str, ...] = ("repo",)
_SYMTABLE_FILE_SORT_ORDER: tuple[str, ...] = ("path", "file_id")
_BYTECODE_EXTERNAL_TABLE_NAME = "bytecode_files_v1"
_BYTECODE_PARTITION_FIELDS: tuple[str, ...] = ("repo",)
_BYTECODE_FILE_SORT_ORDER: tuple[str, ...] = ("path", "file_id")
_TREE_SITTER_EXTERNAL_TABLE_NAME = "tree_sitter_files_v1"
_TREE_SITTER_PARTITION_FIELDS: tuple[str, ...] = ("repo",)
_TREE_SITTER_FILE_SORT_ORDER: tuple[str, ...] = ("path", "file_id")

logger = logging.getLogger(__name__)

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
        registry: IbisDatasetRegistry,
        runtime_profile: DataFusionRuntimeProfile | None,
    ) -> None:
        self._ctx = ctx
        self._registry = registry
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
        return name is not None and self._registry.catalog.has(name)

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
        location = self._registry.catalog.get(name)
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
    registry: IbisDatasetRegistry,
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
                registry=registry,
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
    provider: Literal["listing", "parquet", "delta_cdf"] | None


@dataclass(frozen=True)
class DeltaCdfRegistrationOptions:
    """Options for registering Delta CDF tables."""

    cdf_options: DeltaCdfOptions | None = None
    storage_options: Mapping[str, str] | None = None
    log_storage_options: Mapping[str, str] | None = None
    runtime_profile: DataFusionRuntimeProfile | None = None


@dataclass(frozen=True)
class DeltaCdfArtifact:
    """Diagnostics payload for Delta CDF registration."""

    name: str
    path: str
    provider: str
    options: DeltaCdfOptions | None
    log_storage_options: Mapping[str, str] | None


@dataclass(frozen=True)
class DataFusionCachePolicy:
    """Cache policy overrides for DataFusion dataset registration."""

    enabled: bool | None = None
    max_columns: int | None = None


@dataclass(frozen=True)
class DataFusionCacheSettings:
    """Resolved cache settings for DataFusion registration."""

    enabled: bool
    max_columns: int | None


@dataclass(frozen=True)
class DataFusionRegistrationContext:
    """Inputs needed to register a dataset with DataFusion."""

    ctx: SessionContext
    name: str
    location: DatasetLocation
    options: DataFusionRegistryOptions
    cache: DataFusionCacheSettings
    external_table_sql: str | None = None
    runtime_profile: DataFusionRuntimeProfile | None = None


@dataclass(frozen=True)
class _ScanDefaults:
    partition_fields: tuple[str, ...]
    file_sort_order: tuple[str, ...]
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


@dataclass(frozen=True)
class _ListingTableArtifactDetails:
    scan: DataFusionScanOptions | None
    file_extension: str
    table_partition_cols: Sequence[tuple[str, str]] | None
    skip_metadata: bool | None
    table_schema_contract: TableSchemaContract | None
    expr_adapter_factory: object | None
    actual_schema: pa.Schema | None


def _schema_field_type(dataset: str, field: str) -> pa.DataType | None:
    """Return the Arrow type for a schema field, when available.

    Returns
    -------
    pyarrow.DataType | None
        Field type when available.
    """
    schema = SCHEMA_REGISTRY.get(dataset)
    if schema is None:
        return None
    if field not in schema.names:
        return None
    return schema.field(field).type


def _default_scan_options_for_dataset(name: str) -> DataFusionScanOptions | None:
    """Return default DataFusion scan options for supported datasets.

    Returns
    -------
    DataFusionScanOptions | None
        Default scan options when the dataset is supported.
    """
    defaults = _DEFAULT_SCAN_CONFIGS.get(name)
    if defaults is None:
        return None
    schema = SCHEMA_REGISTRY.get(name)
    if schema is None:
        return None
    partition_cols: list[tuple[str, pa.DataType]] = []
    for field_name in defaults.partition_fields:
        dtype = _schema_field_type(name, field_name)
        if dtype is not None:
            partition_cols.append((field_name, dtype))
    file_sort_order = tuple(field for field in defaults.file_sort_order if field in schema.names)
    return DataFusionScanOptions(
        partition_cols=tuple(partition_cols),
        file_sort_order=file_sort_order,
        file_extension=".parquet",
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
    defaults = _default_scan_options_for_dataset(name)
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
    if provider == "dataset":
        msg = "DataFusion dataset providers are not supported; use listing or native formats."
        raise ValueError(msg)
    if provider == "delta_cdf" and location.format != "delta":
        msg = "Delta CDF provider requires delta-format datasets."
        raise ValueError(msg)
    if provider is None and location.format == "delta" and location.delta_cdf_options is not None:
        provider = "delta_cdf"
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
    if location.format != "parquet":
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
    sql_options = _sql_options_for_profile(context.runtime_profile)
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
        missing = [name for name in key_fields if name not in key_columns]
        if missing:
            msg = f"{context.name} missing DataFusion constraints for key fields {missing}."
            raise ValueError(msg)
    if expected_defaults:
        column_defaults = introspector.table_column_defaults(context.name)
        missing = [name for name in expected_defaults if name not in column_defaults]
        if missing:
            msg = f"{context.name} missing column defaults for {missing}."
            raise ValueError(msg)


def datafusion_external_table_sql(
    *,
    name: str,
    location: DatasetLocation,
    dialect: str | None = None,
    runtime_profile: DataFusionRuntimeProfile | None = None,
    options_override: Mapping[str, object] | None = None,
) -> str | None:
    """Return a CREATE EXTERNAL TABLE statement for a dataset location.

    Returns
    -------
    str | None
        External table DDL when available, otherwise ``None``.

    Raises
    ------
    RuntimeError
        Raised when Delta DDL registration is requested without the factory.
    """
    if location.format == "delta" and not _delta_table_factory_available():
        msg = "Delta DDL registration requires datafusion_ext.install_delta_table_factory."
        raise RuntimeError(msg)
    spec = _resolve_dataset_spec(name, location)
    if spec is None:
        return None
    scan = resolve_datafusion_scan_options(location)
    partitioned_by = None
    file_sort_order = None
    unbounded = None
    if scan is not None:
        partitioned_by = tuple(col for col, _ in scan.partition_cols) or None
        file_sort_order = scan.file_sort_order or None
        unbounded = scan.unbounded
    ddl_dialect = dialect
    if (
        ddl_dialect is None
        and runtime_profile is not None
        and runtime_profile.enable_delta_session_defaults
        and location.format == "delta"
    ):
        ddl_dialect = "datafusion_ext"
    if ddl_dialect == "datafusion_ext":
        from sqlglot_tools.optimizer import register_datafusion_dialect

        register_datafusion_dialect()
    overrides = ExternalTableConfigOverrides(
        table_name=name,
        dialect=ddl_dialect,
        options=_external_table_options(
            location=location,
            scan=scan,
            runtime_profile=runtime_profile,
            options_override=options_override,
        ),
        partitioned_by=partitioned_by,
        file_sort_order=file_sort_order,
        unbounded=unbounded,
    )
    config = spec.table_spec.external_table_config(
        location=str(location.path),
        file_format=location.format,
        overrides=overrides,
    )
    ddl = spec.external_table_sql(config)
    expected = _expected_scan_option_literals(location, scan)
    _validate_scan_options_in_ddl(ddl, expected=expected)
    return ddl


def _delta_table_factory_available() -> bool:
    try:
        module = importlib.import_module("datafusion_ext")
    except ImportError:  # pragma: no cover - optional dependency
        return False
    installer = getattr(module, "install_delta_table_factory", None)
    return callable(installer)


def _install_schema_evolution_adapter_factory(ctx: SessionContext) -> None:
    try:
        module = importlib.import_module("datafusion_ext")
    except ImportError as exc:  # pragma: no cover - optional dependency
        msg = "Schema evolution adapter requires datafusion_ext."
        raise RuntimeError(msg) from exc
    installer = getattr(module, "install_schema_evolution_adapter_factory", None)
    if not callable(installer):
        msg = "Schema evolution adapter installer is unavailable in datafusion_ext."
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


def _external_table_options(
    *,
    location: DatasetLocation,
    scan: DataFusionScanOptions | None,
    runtime_profile: DataFusionRuntimeProfile | None,
    options_override: Mapping[str, object] | None,
) -> dict[str, object] | None:
    options: dict[str, object] = {}
    if runtime_profile is not None and runtime_profile.external_table_options:
        options.update(runtime_profile.external_table_options)
    if location.format == "delta":
        log_storage = resolve_delta_log_storage_options(location)
        if log_storage:
            options.update(log_storage)
    elif location.storage_options:
        options.update(location.storage_options)
    if location.read_options:
        options.update(location.read_options)
    options.update(_scan_external_table_options(location, scan))
    if options_override:
        options.update(options_override)
    return options or None


def _scan_external_table_options(
    location: DatasetLocation,
    scan: DataFusionScanOptions | None,
) -> dict[str, object]:
    if scan is None:
        return {}
    options: dict[str, object] = {}
    if scan.file_extension and location.format != "delta":
        options["file_extension"] = scan.file_extension
    if location.format == "parquet":
        if scan.skip_metadata is not None:
            options["skip_metadata"] = scan.skip_metadata
        if scan.schema_force_view_types is not None:
            options["schema_force_view_types"] = scan.schema_force_view_types
        if scan.binary_as_string is not None:
            options["binary_as_string"] = scan.binary_as_string
        if scan.skip_arrow_metadata is not None:
            options["skip_arrow_metadata"] = scan.skip_arrow_metadata
        if scan.parquet_column_options is not None:
            options.update(scan.parquet_column_options.external_table_options())
    return options


def _option_literal_text(value: object) -> str | None:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, str):
        return value
    return None


def _expected_scan_option_literals(
    location: DatasetLocation,
    scan: DataFusionScanOptions | None,
) -> dict[str, str]:
    raw = _scan_external_table_options(location, scan)
    return {
        str(key).lower(): rendered
        for key, value in raw.items()
        if (rendered := _option_literal_text(value)) is not None
    }


def _literal_token(node: exp.Expression) -> str | None:
    if isinstance(node, exp.Literal):
        return str(node.this)
    if isinstance(node, exp.Identifier):
        return node.name
    return None


def _ddl_external_table_options(
    ddl: str,
    *,
    dialect: str,
) -> dict[str, str]:
    expr = parse_sql_strict(ddl, dialect=dialect)
    create_expr = expr if isinstance(expr, exp.Create) else expr.find(exp.Create)
    if create_expr is None:
        return {}
    properties = create_expr.args.get("properties")
    if not isinstance(properties, exp.Properties):
        return {}
    options_property = None
    for prop in properties.expressions:
        if isinstance(prop, ExternalTableOptionsProperty):
            options_property = prop
            break
    if options_property is None:
        return {}
    options: dict[str, str] = {}
    for entry in options_property.expressions:
        if not isinstance(entry, exp.Tuple) or len(entry.expressions) != _OPTIONS_TUPLE_ARITY:
            continue
        key_expr, value_expr = entry.expressions
        key = _literal_token(key_expr)
        value = _literal_token(value_expr)
        if key is None or value is None:
            continue
        options[key.lower()] = value
    return options


def _validate_scan_options_in_ddl(
    ddl: str,
    *,
    expected: Mapping[str, str],
) -> None:
    if not expected:
        return
    policy = resolve_sqlglot_policy(name="datafusion_ddl")
    try:
        actual = _ddl_external_table_options(ddl, dialect=policy.read_dialect)
    except ParseError as exc:
        msg = f"Failed to parse external table DDL for option validation: {exc}"
        raise ValueError(msg) from exc
    missing = {key: value for key, value in expected.items() if actual.get(key) != value}
    if missing:
        msg = f"External table DDL is missing expected scan options: {sorted(missing.items())}."
        raise ValueError(msg)


def register_dataset_df(
    ctx: SessionContext,
    *,
    name: str,
    location: DatasetLocation,
    cache_policy: DataFusionCachePolicy | None = None,
    runtime_profile: DataFusionRuntimeProfile | None = None,
) -> DataFrame:
    """Register a dataset location with DataFusion and return a DataFrame.

    Returns
    -------
    datafusion.dataframe.DataFrame
        DataFusion DataFrame for the registered dataset.

    """
    context = _build_registration_context(
        ctx,
        name=name,
        location=location,
        cache_policy=cache_policy,
        runtime_profile=runtime_profile,
    )
    return _register_dataset_with_context(context)


@dataclass(frozen=True)
class DatasetDdlRegistration:
    """Define inputs for DDL-based dataset registration."""

    name: str
    spec: DatasetSpec
    location: str
    file_format: str = "PARQUET"


def register_dataset_ddl(
    ctx: SessionContext,
    registration: DatasetDdlRegistration,
    *,
    sql_options: SQLOptions | None = None,
) -> None:
    """Register dataset using DDL (preferred path for streaming sources).

    This uses CREATE [UNBOUNDED] EXTERNAL TABLE DDL which properly sets
    streaming semantics in DataFusion. The unbounded flag is derived from
    the DatasetSpec's DataFusionScanOptions.

    Parameters
    ----------
    ctx : SessionContext
        DataFusion session context to register the table in.
    registration : DatasetDdlRegistration
        Registration inputs including name, spec, location, and file format.
    sql_options : SQLOptions | None
        Optional SQL execution options for the DDL statement.

    Notes
    -----
    This function is particularly useful for streaming sources where the
    unbounded flag needs to be properly propagated through DDL generation
    to DataFusion's internal table provider setup.

    Examples
    --------
    Register a streaming Parquet source:

    >>> from datafusion_engine.runtime import DataFusionRuntimeProfile
    >>> from schema_spec.system import DatasetSpec, DataFusionScanOptions
    >>> ctx = DataFusionRuntimeProfile().ephemeral_context()
    >>> spec = DatasetSpec(
    ...     table_spec=table_spec,
    ...     datafusion_scan=DataFusionScanOptions(unbounded=True),
    ... )
    >>> register_dataset_ddl(
    ...     ctx,
    ...     DatasetDdlRegistration(
    ...         name="streaming_events",
    ...         spec=spec,
    ...         location="s3://bucket/events/",
    ...     ),
    ... )
    """
    config = registration.spec.external_table_config_with_streaming(
        location=registration.location,
        file_format=registration.file_format,
    )
    ddl = registration.spec.external_table_sql(config)

    options = sql_options or _statement_sql_options_for_profile(None)
    ctx.sql_with_options(ddl, options).collect()

    # Record metadata for diagnostics
    record_table_provider_metadata(
        ctx_id=id(ctx),
        metadata=TableProviderMetadata(
            table_name=registration.name,
            ddl=ddl,
            ddl_fingerprint=ddl_fingerprint_from_definition(ddl),
            storage_location=registration.location,
            file_format=registration.file_format,
            unbounded=config.unbounded,
            partition_columns=tuple(config.partitioned_by) if config.partitioned_by else (),
        ),
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
    _register_object_store(ctx, location)
    if runtime_profile is not None:
        runtime_profile = _populate_schema_adapter_factories(runtime_profile)
    if runtime_profile is not None:
        _ensure_catalog_schema(
            ctx,
            catalog=runtime_profile.default_catalog,
            schema=runtime_profile.default_schema,
        )
    options = resolve_registry_options(location)
    if options.schema is None:
        canonical = SCHEMA_REGISTRY.get(name)
        if canonical is not None:
            options = replace(options, schema=canonical)
    options = _apply_runtime_scan_hardening(options, runtime_profile=runtime_profile)
    cache = _resolve_cache_policy(
        options,
        cache_policy=cache_policy,
        runtime_profile=runtime_profile,
    )
    external_table_sql = datafusion_external_table_sql(
        name=name,
        location=location,
        runtime_profile=runtime_profile,
    )
    if location.format == "delta" and location.files:
        msg = "Delta DDL registration does not support file-restricted inputs."
        raise ValueError(msg)
    if location.format == "delta" and external_table_sql is None:
        msg = "Delta registration requires DDL-backed schema metadata."
        raise ValueError(msg)
    context = DataFusionRegistrationContext(
        ctx=ctx,
        name=name,
        location=location,
        options=options,
        cache=cache,
        external_table_sql=external_table_sql,
        runtime_profile=runtime_profile,
    )
    if external_table_sql is not None:
        metadata = TableProviderMetadata(
            table_name=name,
            ddl=external_table_sql,
            storage_location=str(location.path),
            file_format=location.format or "unknown",
            partition_columns=tuple(col for col, _ in options.scan.partition_cols)
            if options.scan and options.scan.partition_cols
            else (),
            unbounded=options.scan.unbounded if options.scan else False,
            ddl_fingerprint=ddl_fingerprint_from_definition(external_table_sql),
        )
        record_table_provider_metadata(id(ctx), metadata=metadata)
    return context


def _register_dataset_with_context(context: DataFusionRegistrationContext) -> DataFrame:
    if context.location.format == "delta":
        if context.options.provider == "delta_cdf":
            return _register_delta_cdf(context)
        if _should_register_delta_provider(context):
            return _register_delta_provider(context)
        if context.external_table_sql is None:
            msg = "Delta registration requires a schema-backed DDL statement."
            raise ValueError(msg)
        return _register_external_table(context)
    if context.external_table_sql is None:
        msg = "External table registration requires a schema-backed DDL statement."
        raise ValueError(msg)
    return _register_external_table(context)


def _should_register_delta_provider(context: DataFusionRegistrationContext) -> bool:
    location = context.location
    if location.format != "delta":
        return False
    if location.delta_version is not None or location.delta_timestamp is not None:
        return True
    return resolve_delta_scan_options(location) is not None


@dataclass(frozen=True)
class _DeltaProviderRequest:
    ctx: SessionContext
    path: str
    log_storage_options: Mapping[str, str] | None
    version: int | None
    timestamp: str | None
    delta_scan: DeltaScanOptions | None


@dataclass(frozen=True)
class _DeltaProviderResponse:
    provider: object
    delta_scan_effective: Mapping[str, object] | None


def _register_delta_provider(context: DataFusionRegistrationContext) -> DataFrame:
    location = context.location
    delta_scan = resolve_delta_scan_options(location)
    if (
        delta_scan is not None
        and delta_scan.schema_force_view_types is None
        and context.runtime_profile is not None
    ):
        enable_view_types = _schema_hardening_view_types(context.runtime_profile)
        delta_scan = replace(delta_scan, schema_force_view_types=enable_view_types)
    request = _DeltaProviderRequest(
        ctx=context.ctx,
        path=str(location.path),
        log_storage_options=resolve_delta_log_storage_options(location),
        version=location.delta_version,
        timestamp=location.delta_timestamp,
        delta_scan=delta_scan,
    )
    response = _delta_table_provider_from_session(request)
    provider = response.provider
    register = getattr(context.ctx, "register_table", None)
    if not callable(register):
        msg = "DataFusion SessionContext missing register_table."
        raise TypeError(msg)
    register(context.name, TableProviderCapsule(provider))
    _record_table_provider_artifact(
        context.runtime_profile,
        artifact=_TableProviderArtifact(
            name=context.name,
            provider=provider,
            provider_kind="delta_table_provider",
            source=None,
            details=_delta_provider_artifact_payload(
                location,
                delta_scan=delta_scan,
                delta_scan_effective=response.delta_scan_effective,
            ),
        ),
    )
    df = context.ctx.table(context.name)
    return _maybe_cache(context, df)


def _register_delta_cdf(context: DataFusionRegistrationContext) -> DataFrame:
    options = DeltaCdfRegistrationOptions(
        cdf_options=context.location.delta_cdf_options,
        storage_options=context.location.storage_options,
        log_storage_options=resolve_delta_log_storage_options(context.location),
        runtime_profile=context.runtime_profile,
    )
    return register_delta_cdf_df(
        context.ctx,
        name=context.name,
        path=str(context.location.path),
        options=options,
    )


def _delta_table_provider_from_session(
    request: _DeltaProviderRequest,
) -> _DeltaProviderResponse:
    try:
        module = importlib.import_module("datafusion_ext")
    except ImportError as exc:
        msg = "Delta table providers require datafusion_ext."
        raise RuntimeError(msg) from exc
    provider_factory = getattr(module, "delta_table_provider_from_session", None)
    if not callable(provider_factory):
        msg = "datafusion_ext.delta_table_provider_from_session is unavailable."
        raise TypeError(msg)
    schema_ipc = _schema_ipc_payload(request.delta_scan.schema) if request.delta_scan else None
    storage_payload = (
        list(request.log_storage_options.items()) if request.log_storage_options else None
    )
    provider = provider_factory(
        request.ctx,
        request.path,
        storage_payload,
        request.version,
        request.timestamp,
        request.delta_scan.file_column_name if request.delta_scan else None,
        request.delta_scan.enable_parquet_pushdown if request.delta_scan else None,
        request.delta_scan.schema_force_view_types if request.delta_scan else None,
        request.delta_scan.wrap_partition_values if request.delta_scan else None,
        schema_ipc,
    )
    effective_scan = _delta_scan_effective_from_session(
        module,
        ctx=request.ctx,
        delta_scan=request.delta_scan,
        schema_ipc=schema_ipc,
    )
    return _DeltaProviderResponse(provider=provider, delta_scan_effective=effective_scan)


def _schema_ipc_payload(schema: pa.Schema | None) -> bytes | None:
    if schema is None:
        return None
    buffer = schema.serialize()
    return buffer.to_pybytes()


def _delta_scan_effective_from_session(
    module: object,
    *,
    ctx: SessionContext,
    delta_scan: DeltaScanOptions | None,
    schema_ipc: bytes | None,
) -> Mapping[str, object] | None:
    config_factory = getattr(module, "delta_scan_config_from_session", None)
    if not callable(config_factory):
        msg = "datafusion_ext.delta_scan_config_from_session is unavailable."
        raise TypeError(msg)
    payload = config_factory(
        ctx,
        delta_scan.file_column_name if delta_scan else None,
        delta_scan.enable_parquet_pushdown if delta_scan else None,
        delta_scan.schema_force_view_types if delta_scan else None,
        delta_scan.wrap_partition_values if delta_scan else None,
        schema_ipc,
    )
    if not isinstance(payload, Mapping):
        msg = "delta_scan_config_from_session returned invalid payload."
        raise TypeError(msg)
    return _delta_scan_effective_payload(payload)


def _delta_scan_effective_payload(
    payload: Mapping[str, object],
) -> dict[str, object]:
    schema_payload = None
    schema_ipc = payload.get("schema_ipc")
    if isinstance(schema_ipc, (bytes, bytearray)):
        schema_payload = schema_to_dict(_decode_schema_ipc(bytes(schema_ipc)))
    return {
        "file_column_name": payload.get("file_column_name"),
        "enable_parquet_pushdown": payload.get("enable_parquet_pushdown"),
        "schema_force_view_types": payload.get("schema_force_view_types"),
        "wrap_partition_values": payload.get("wrap_partition_values"),
        "schema": schema_payload,
    }


def _decode_schema_ipc(payload: bytes) -> pa.Schema:
    try:
        return pa.ipc.read_schema(pa.BufferReader(payload))
    except (pa.ArrowInvalid, TypeError, ValueError) as exc:
        msg = "Invalid Delta scan schema IPC payload."
        raise ValueError(msg) from exc


def _delta_provider_artifact_payload(
    location: DatasetLocation,
    *,
    delta_scan: DeltaScanOptions | None,
    delta_scan_effective: Mapping[str, object] | None,
) -> dict[str, object]:
    log_storage = resolve_delta_log_storage_options(location)
    return {
        "path": str(location.path),
        "delta_version": location.delta_version,
        "delta_timestamp": location.delta_timestamp,
        "delta_log_storage_options": dict(log_storage) if log_storage else None,
        "delta_scan": _delta_scan_payload(delta_scan),
        "delta_scan_effective": delta_scan_effective,
    }


def _delta_scan_payload(options: DeltaScanOptions | None) -> dict[str, object] | None:
    if options is None:
        return None
    schema_payload = schema_to_dict(options.schema) if options.schema is not None else None
    return {
        "file_column_name": options.file_column_name,
        "enable_parquet_pushdown": options.enable_parquet_pushdown,
        "schema_force_view_types": options.schema_force_view_types,
        "wrap_partition_values": options.wrap_partition_values,
        "schema": schema_payload,
    }


def _register_external_table(
    context: DataFusionRegistrationContext,
) -> DataFrame:
    """Register external table via DDL-based path (preferred).

    This is the preferred registration path that uses DataFusion's native
    DDL parsing and execution instead of bespoke registry logic. It leverages
    CREATE EXTERNAL TABLE statements and information_schema for schema contracts.

    Returns
    -------
    datafusion.dataframe.DataFrame
        Registered DataFusion DataFrame.

    Raises
    ------
    ValueError
        If the external table SQL statement is missing.
    """
    scan = context.options.scan
    if context.external_table_sql is None:
        msg = "External table registration requires a schema-backed DDL statement."
        raise ValueError(msg)
    file_extension = scan.file_extension if scan and scan.file_extension else ".parquet"
    table_schema_contract = _resolve_table_schema_contract(
        schema=context.options.schema,
        scan=scan,
        partition_cols=scan.partition_cols if scan is not None else None,
    )
    _validate_table_schema_contract(table_schema_contract)
    _validate_ordering_contract(context, scan=scan)
    table_partition_cols = (
        [(col, str(dtype)) for col, dtype in scan.partition_cols]
        if scan and scan.partition_cols
        else None
    )
    skip_metadata = None
    if scan is not None:
        skip_metadata = _effective_skip_metadata(context.location, scan)
    _apply_scan_settings(
        context.ctx,
        scan=scan,
        sql_options=_statement_sql_options_for_profile(context.runtime_profile),
    )
    try:
        context.ctx.sql_with_options(
            context.external_table_sql,
            _statement_sql_options_for_profile(context.runtime_profile),
        ).collect()
    except (RuntimeError, TypeError, ValueError) as exc:
        msg = f"Failed to register unbounded external table: {exc}"
        raise ValueError(msg) from exc
    df = context.ctx.table(context.name)
    registered_schema = df.schema()
    expr_adapter_factory = _resolve_expr_adapter_factory(
        scan,
        runtime_profile=context.runtime_profile,
        dataset_name=context.name,
        location=context.location,
    )
    dataset_spec = _resolve_dataset_spec(context.name, context.location)
    evolution_required = (
        _requires_schema_evolution_adapter(dataset_spec.evolution_spec)
        if dataset_spec is not None
        else False
    )
    _ensure_expr_adapter_factory(
        context.ctx,
        factory=expr_adapter_factory,
        evolution_required=evolution_required,
    )
    _validate_constraints_and_defaults(
        context,
        enable_information_schema=(
            context.runtime_profile.enable_information_schema
            if context.runtime_profile is not None
            else False
        ),
    )
    _record_listing_table_artifact(
        context,
        details=_ListingTableArtifactDetails(
            scan=scan,
            file_extension=file_extension,
            table_partition_cols=table_partition_cols,
            skip_metadata=skip_metadata,
            table_schema_contract=table_schema_contract,
            expr_adapter_factory=expr_adapter_factory,
            actual_schema=registered_schema,
        ),
    )
    return _maybe_cache(context, df)


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
    if runtime_profile is None or runtime_profile.diagnostics_sink is None:
        return
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
    runtime_profile.diagnostics_sink.record_artifact("datafusion_table_providers_v1", payload)


def _record_listing_table_artifact(
    context: DataFusionRegistrationContext,
    *,
    details: _ListingTableArtifactDetails,
) -> None:
    profile = context.runtime_profile
    if profile is None or profile.diagnostics_sink is None:
        return
    provenance = _table_provenance_snapshot(
        context.ctx,
        name=context.name,
        enable_information_schema=profile.enable_information_schema,
        sql_options=_sql_options_for_profile(profile),
        runtime_profile=profile,
    )
    expected_schema = context.options.schema
    expected_schema_fingerprint = _schema_fingerprint(expected_schema)
    actual_schema_fingerprint = _schema_fingerprint(details.actual_schema)
    expected_ddl_fingerprint = (
        ddl_fingerprint_from_definition(context.external_table_sql)
        if context.external_table_sql is not None
        else None
    )
    observed_ddl_fingerprint = _ddl_fingerprint(
        _DdlFingerprintContext(
            ctx=context.ctx,
            enable_information_schema=profile.enable_information_schema,
            runtime_profile=profile,
        ),
        name=context.name,
        schema=expected_schema,
    )
    table_schema_snapshot = _table_schema_snapshot(
        schema=context.options.schema,
        partition_cols=details.table_partition_cols,
    )
    evolution_payload, evolution_required = _schema_evolution_details(context)
    ordering_keys: list[list[str]] | None = None
    ordering_matches_scan: bool | None = None
    if expected_schema is not None:
        ordering = ordering_from_schema(expected_schema)
        if ordering.keys:
            ordering_keys = [list(key) for key in ordering.keys]
            if details.scan is not None and details.scan.file_sort_order:
                expected = [key[0] for key in ordering.keys]
                ordering_matches_scan = list(details.scan.file_sort_order) == expected
    payload: dict[str, object] = {
        "name": context.name,
        "path": str(context.location.path),
        "format": context.location.format,
        "provider": "listing",
        "file_extension": details.file_extension,
        "partition_cols": [
            {"name": col, "dtype": dtype} for col, dtype in (details.table_partition_cols or ())
        ],
        "file_sort_order": (
            list(details.scan.file_sort_order) if details.scan is not None else None
        ),
        "ordering_keys": ordering_keys,
        "ordering_matches_scan": ordering_matches_scan,
        "parquet_pruning": details.scan.parquet_pruning if details.scan is not None else None,
        "skip_metadata": details.skip_metadata,
        "skip_arrow_metadata": (
            details.scan.skip_arrow_metadata if details.scan is not None else None
        ),
        "binary_as_string": (details.scan.binary_as_string if details.scan is not None else None),
        "schema_force_view_types": (
            details.scan.schema_force_view_types if details.scan is not None else None
        ),
        "collect_statistics": (
            details.scan.collect_statistics if details.scan is not None else None
        ),
        "meta_fetch_concurrency": (
            details.scan.meta_fetch_concurrency if details.scan is not None else None
        ),
        "parquet_column_options": (
            details.scan.parquet_column_options.external_table_options()
            if details.scan is not None and details.scan.parquet_column_options is not None
            else None
        ),
        "list_files_cache_limit": (
            details.scan.list_files_cache_limit if details.scan is not None else None
        ),
        "list_files_cache_ttl": (
            details.scan.list_files_cache_ttl if details.scan is not None else None
        ),
        "listing_table_factory_infer_partitions": (
            details.scan.listing_table_factory_infer_partitions
            if details.scan is not None
            else None
        ),
        "listing_table_ignore_subdirectory": (
            details.scan.listing_table_ignore_subdirectory if details.scan is not None else None
        ),
        "projection_exprs": (
            list(details.scan.projection_exprs) if details.scan is not None else None
        ),
        "listing_mutable": details.scan.listing_mutable if details.scan is not None else None,
        "unbounded": details.scan.unbounded if details.scan is not None else None,
        "schema": schema_to_dict(expected_schema) if expected_schema is not None else None,
        "expected_schema_fingerprint": expected_schema_fingerprint,
        "actual_schema_fingerprint": actual_schema_fingerprint,
        "schema_match": _fingerprints_match(expected_schema_fingerprint, actual_schema_fingerprint),
        "expected_ddl_fingerprint": expected_ddl_fingerprint,
        "observed_ddl_fingerprint": observed_ddl_fingerprint,
        "ddl_match": _fingerprints_match(expected_ddl_fingerprint, observed_ddl_fingerprint),
        "table_schema_snapshot": table_schema_snapshot,
        "table_schema_contract": _table_schema_contract_payload(details.table_schema_contract),
        "schema_evolution": evolution_payload,
        "schema_evolution_required": evolution_required,
        "expr_adapter_factory": _adapter_factory_payload(details.expr_adapter_factory),
        "read_options": dict(context.options.read_options),
    }
    payload["partition_schema_validation"] = _partition_schema_validation(
        _PartitionSchemaContext(
            ctx=context.ctx,
            table_name=context.name,
            enable_information_schema=profile.enable_information_schema,
            runtime_profile=profile,
        ),
        expected_partition_cols=details.table_partition_cols,
    )
    payload.update(provenance)
    profile.diagnostics_sink.record_artifact("datafusion_listing_tables_v1", payload)


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
    base_name = f"{table_name}__raw"
    with suppress(KeyError, ValueError):
        ctx.deregister_table(base_name)
    base_view = ctx.table(table_name).into_view(temporary=True)
    ctx.register_table(base_name, base_view)
    parsed_exprs: list[exp.Expression] = []
    for projection in projection_exprs:
        try:
            parsed_exprs.append(parse_one(projection))
        except (ParseError, TypeError, ValueError):
            parsed_exprs = []
            break
    if parsed_exprs:
        policy = resolve_sqlglot_policy(name="datafusion_compile")
        view_expr = build_select(parsed_exprs, from_=base_name)
        view_sql = sqlglot_emit(view_expr, policy=policy)
    else:
        selection = ", ".join(projection_exprs)
        view_sql = f"SELECT {selection} FROM {base_name}"
    projected = ctx.sql_with_options(view_sql, sql_options)
    ctx.deregister_table(table_name)
    projected_view = projected.into_view(temporary=False)
    ctx.register_table(table_name, projected_view)
    if runtime_profile is not None:
        from datafusion_engine.runtime import record_view_definition

        record_view_definition(runtime_profile, name=table_name, sql=view_sql)
    return ctx.table(table_name)


def _sql_identifier(name: str) -> str:
    escaped = name.replace('"', '""')
    return f'"{escaped}"'


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
        projection_exprs = [_sql_identifier(name) for name in columns]
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
        projection_exprs = tuple(_sql_identifier(name) for name in columns)
        if scan.projection_exprs == projection_exprs:
            continue
        updated_scan = replace(scan, projection_exprs=projection_exprs)
        updated_location = replace(location, datafusion_scan=updated_scan)
        deregister = getattr(ctx, "deregister_table", None)
        if callable(deregister):
            with suppress(KeyError, RuntimeError, TypeError, ValueError):
                deregister(table_name)
        try:
            register_dataset_df(
                ctx,
                name=table_name,
                location=updated_location,
                runtime_profile=runtime_profile,
            )
        except (RuntimeError, TypeError, ValueError):
            continue


def _apply_scan_settings(
    ctx: SessionContext,
    *,
    scan: DataFusionScanOptions | None,
    sql_options: SQLOptions,
) -> None:
    """Apply per-table DataFusion scan settings via SET statements."""
    if scan is None:
        return
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
        _set_runtime_setting(ctx, key=key, value=text, sql_options=sql_options)


def _set_runtime_setting(
    ctx: SessionContext,
    *,
    key: str,
    value: str,
    sql_options: SQLOptions,
) -> None:
    """Apply a DataFusion session setting."""
    ctx.sql_with_options(f"SET {key} = '{value}'", sql_options).collect()


def _refresh_listing_table(
    ctx: SessionContext,
    *,
    name: str,
    register: Callable[[], None],
    runtime_profile: DataFusionRuntimeProfile | None,
) -> None:
    """Refresh a listing table registration by deregistering and re-registering."""
    with suppress(KeyError, ValueError):
        ctx.deregister_table(name)
    register()
    if runtime_profile is None or runtime_profile.diagnostics_sink is None:
        return
    runtime_profile.diagnostics_sink.record_artifact(
        "datafusion_listing_refresh_v1",
        {"name": name},
    )


def _schema_fingerprint(schema: pa.Schema | None) -> str | None:
    if schema is None:
        return None
    return schema_fingerprint(schema)


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
    expected = [key[0] for key in ordering.keys]
    if scan is None or not scan.file_sort_order:
        msg = f"{context.name} ordering requires file_sort_order {expected}."
        raise ValueError(msg)
    if list(scan.file_sort_order) != expected:
        msg = (
            f"{context.name} file_sort_order {list(scan.file_sort_order)} does not match "
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
    try:
        module = importlib.import_module("datafusion_ext")
    except ImportError as exc:
        msg = "Schema evolution adapter requires datafusion_ext."
        raise RuntimeError(msg) from exc
    factory = getattr(module, "schema_evolution_adapter_factory", None)
    if not callable(factory):
        msg = "schema_evolution_adapter_factory is not available in datafusion_ext."
        raise TypeError(msg)
    return factory()


@dataclass(frozen=True)
class _DdlFingerprintContext:
    ctx: SessionContext
    enable_information_schema: bool
    runtime_profile: DataFusionRuntimeProfile | None = None


def _ddl_fingerprint(
    context: _DdlFingerprintContext,
    *,
    name: str,
    schema: pa.Schema | None,
) -> str | None:
    if context.enable_information_schema:
        if context.runtime_profile is not None:
            ddl = schema_introspector_for_profile(
                context.runtime_profile,
                context.ctx,
            ).table_definition(name)
        else:
            ddl = SchemaIntrospector(
                context.ctx,
                sql_options=_sql_options_for_profile(None),
            ).table_definition(name)
        if ddl is not None:
            return ddl_fingerprint_from_definition(ddl)
    _ = schema
    return None


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
            sql_options = _sql_options_for_profile(runtime_profile)
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
    missing = [name for name in expected_names if name not in table_schema_types]
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
    missing = [name for name in expected_names if name not in actual_types]
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
    missing: list[str] = []
    missing_value = validation.get("missing_partition_cols")
    if isinstance(missing_value, Sequence) and not isinstance(
        missing_value,
        (str, bytes, bytearray),
    ):
        missing = [str(item) for item in missing_value]
    order_matches = validation.get("partition_order_matches")
    type_value = validation.get("partition_type_mismatches")
    type_mismatches: list[dict[str, str]]
    if isinstance(type_value, Sequence) and not isinstance(type_value, (str, bytes, bytearray)):
        type_mismatches = [
            {str(key): str(val) for key, val in item.items()}
            for item in type_value
            if isinstance(item, Mapping)
        ]
    else:
        type_mismatches = []
    if missing or order_matches is False or type_mismatches:
        msg = f"Partition schema validation failed for {context.table_name}: {validation}."
        raise ValueError(msg)


def _parquet_read_options(read_options: Mapping[str, object]) -> ds.ParquetReadOptions | None:
    option = read_options.get("parquet_read_options")
    if isinstance(option, ds.ParquetReadOptions):
        return option
    return None


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


def register_delta_cdf_df(
    ctx: SessionContext,
    *,
    name: str,
    path: str,
    options: DeltaCdfRegistrationOptions | None = None,
) -> DataFrame:
    """Register a Delta CDF snapshot as a DataFusion table.

    Raises
    ------
    ValueError
        Raised when the Delta CDF provider cannot be constructed.

    Returns
    -------
    datafusion.dataframe.DataFrame
        DataFusion DataFrame for the registered CDF dataset.
    """
    resolved = options or DeltaCdfRegistrationOptions()
    cdf_options = resolved.cdf_options
    storage_options = resolved.log_storage_options or resolved.storage_options
    runtime_profile = resolved.runtime_profile
    provider = _delta_cdf_table_provider(
        path=path,
        storage_options=storage_options,
        options=cdf_options,
    )
    if provider is None:
        msg = "Delta CDF provider requires datafusion_ext.delta_cdf_table_provider."
        raise ValueError(msg)
    ctx.register_table(name, provider)
    _record_table_provider_artifact(
        runtime_profile,
        artifact=_TableProviderArtifact(
            name=name,
            provider=provider,
            provider_kind="cdf_table_provider",
            source=None,
        ),
    )
    _record_delta_cdf_artifact(
        runtime_profile,
        artifact=DeltaCdfArtifact(
            name=name,
            path=path,
            provider="table_provider",
            options=cdf_options,
            log_storage_options=storage_options,
        ),
    )
    return ctx.table(name)


def _delta_cdf_table_provider(
    *,
    path: str,
    storage_options: Mapping[str, str] | None,
    options: DeltaCdfOptions | None,
) -> object | None:
    try:
        module = importlib.import_module("datafusion_ext")
    except ImportError:
        return None
    provider_factory = getattr(module, "delta_cdf_table_provider", None)
    options_type = getattr(module, "DeltaCdfOptions", None)
    if not callable(provider_factory) or options_type is None:
        return None
    resolved = options or DeltaCdfOptions()
    ext_options = options_type()
    if resolved.starting_version is not None:
        ext_options.starting_version = resolved.starting_version
    if resolved.ending_version is not None:
        ext_options.ending_version = resolved.ending_version
    if resolved.starting_timestamp is not None:
        ext_options.starting_timestamp = resolved.starting_timestamp
    if resolved.ending_timestamp is not None:
        ext_options.ending_timestamp = resolved.ending_timestamp
    ext_options.allow_out_of_range = resolved.allow_out_of_range
    storage = list(storage_options.items()) if storage_options else None
    return provider_factory(path, storage, ext_options)


def _record_delta_cdf_artifact(
    runtime_profile: DataFusionRuntimeProfile | None,
    *,
    artifact: DeltaCdfArtifact,
) -> None:
    if runtime_profile is None or runtime_profile.diagnostics_sink is None:
        return
    payload: dict[str, object] = {
        "name": artifact.name,
        "path": artifact.path,
        "provider": artifact.provider,
        "options": _cdf_options_payload(artifact.options),
        "delta_log_storage_options": (
            dict(artifact.log_storage_options) if artifact.log_storage_options else None
        ),
    }
    runtime_profile.diagnostics_sink.record_artifact("datafusion_delta_cdf_v1", payload)


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


def _register_object_store(ctx: SessionContext, location: DatasetLocation) -> None:
    register = getattr(ctx, "register_object_store", None)
    if not callable(register):
        return
    if location.filesystem is None:
        return
    scheme = _scheme_prefix(location.path)
    if scheme is None:
        return
    ctx_key = id(ctx)
    registered = _REGISTERED_OBJECT_STORES.setdefault(ctx_key, set())
    if scheme in registered:
        return
    register(scheme, location.filesystem, None)
    registered.add(scheme)


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
    cached = df.cache()
    context.ctx.deregister_table(context.name)
    cached_view = cached.into_view(temporary=False)
    context.ctx.register_table(context.name, cached_view)
    cached_set = _CACHED_DATASETS.setdefault(id(context.ctx), set())
    cached_set.add(context.name)
    return cached


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
            cat = Catalog.memory_catalog()
            ctx.register_catalog_provider(catalog, cat)
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
    if runtime_profile is not None:
        if enabled is None:
            enabled = runtime_profile.cache_enabled
        if max_columns is None:
            max_columns = runtime_profile.cache_max_columns
    if enabled is None:
        enabled = True
    if max_columns is None:
        max_columns = DEFAULT_CACHE_MAX_COLUMNS
    return DataFusionCacheSettings(enabled=options.cache and enabled, max_columns=max_columns)


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
    "DatasetDdlRegistration",
    "DatasetInputSource",
    "datafusion_external_table_sql",
    "dataset_input_plugin",
    "input_plugin_prefixes",
    "register_dataset_ddl",
    "register_dataset_df",
    "register_delta_cdf_df",
    "resolve_registry_options",
]

"""Listing-table focused helpers for dataset registration."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import TYPE_CHECKING, cast

import pyarrow as pa
import pyarrow.dataset as ds
from datafusion.catalog import Catalog, Schema
from datafusion.dataframe import DataFrame

from arrow_utils.core.ordering import OrderingLevel
from datafusion_engine.arrow.metadata import ordering_from_schema, schema_constraints_from_metadata
from datafusion_engine.dataset.registration_core import (
    DDL_IDENTIFIER_RE as _DDL_IDENTIFIER_RE,
)
from datafusion_engine.dataset.registration_core import (
    ExternalTableDdlRequest as _ExternalTableDdlRequest,
)
from datafusion_engine.dataset.registration_core import (
    invalidate_information_schema_cache as _invalidate_information_schema_cache,
)
from datafusion_engine.dataset.registration_core import (
    resolve_dataset_caches as _resolve_dataset_caches,
)
from datafusion_engine.dataset.registration_core import (
    sql_type_name as _sql_type_name,
)
from datafusion_engine.dataset.registration_projection import (
    _apply_projection_exprs,
    _projection_exprs_for_schema,
    _sql_literal_for_field,
)
from datafusion_engine.dataset.registration_provider import (
    TableProviderArtifact as _TableProviderArtifact,
)
from datafusion_engine.dataset.registration_provider import (
    record_table_provider_artifact as _record_table_provider_artifact,
)
from datafusion_engine.dataset.registration_provider import (
    update_table_provider_fingerprints as _update_table_provider_fingerprints,
)
from datafusion_engine.dataset.registration_validation import _expected_column_defaults
from datafusion_engine.dataset.registry import DatasetLocation
from datafusion_engine.sql.options import sql_options_for_profile
from datafusion_engine.tables.registration import (
    register_listing_table as _register_listing_authority,
)

if TYPE_CHECKING:
    from datafusion import SessionContext

    from datafusion_engine.dataset.registration_core import (
        DataFusionRegistrationContext,
        DatasetCaches,
    )
    from schema_spec.scan_options import DataFusionScanOptions


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


def _ddl_order_clause(order: tuple[tuple[str, str], ...]) -> str | None:
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
    from datafusion_engine.dataset.registration_schema import _resolve_table_schema_contract

    contract = _resolve_table_schema_contract(
        schema=schema,
        scan=scan,
        partition_cols=scan.partition_cols_pyarrow() if scan is not None else None,
    )
    if contract is not None:
        schema = contract.file_schema
        partition_cols = contract.partition_cols_pyarrow()
    else:
        partition_cols = scan.partition_cols_pyarrow() if scan is not None else ()
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
    for schema_field in schema:
        dtype_name = _sql_type_name(schema_field.type)
        fragments = [_ddl_identifier(schema_field.name), dtype_name]
        if not schema_field.nullable or schema_field.name in required_set:
            fragments.append("NOT NULL")
        default_value = defaults.get(schema_field.name)
        if default_value is not None:
            literal = _sql_literal_for_field(default_value, dtype=schema_field.type)
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


def _ddl_statement(request: _ExternalTableDdlRequest) -> str:
    """Build CREATE EXTERNAL TABLE statements for listing registration.

    Returns:
        str: Fully rendered CREATE EXTERNAL TABLE statement.
    """
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
    """Resolve full DDL payload tuple for listing-table registration.

    Returns:
        tuple[str, tuple[str, ...], tuple[str, ...], tuple[str, ...], dict[str, str]]: DDL and
            schema-derived registration metadata.
    """
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


def _build_pyarrow_dataset(
    location: DatasetLocation,
    *,
    schema: object | None,
) -> object:
    """Build a PyArrow dataset from resolved listing inputs.

    Returns:
        object: PyArrow dataset instance suitable for DataFusion registration.
    """
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


def _register_dataset_with_context(context: DataFusionRegistrationContext) -> DataFrame:
    """Register dataset in-session using listing authority rules.

    Returns:
        DataFrame: Registered DataFusion dataframe.
    """
    from datafusion_engine.dataset.registration_cache import _maybe_cache
    from datafusion_engine.dataset.registration_delta import _register_delta_provider
    from datafusion_engine.dataset.registration_schema import _validate_schema_contracts

    if context.options.provider == "listing":
        result = _register_listing_authority(context)

        _, _, fingerprint_details = _update_table_provider_fingerprints(
            context.ctx,
            name=context.name,
            schema=result.df.schema(),
        )
        details = dict(result.details)
        if fingerprint_details:
            details.update(fingerprint_details)
        _record_table_provider_artifact(
            context.runtime_profile,
            artifact=_TableProviderArtifact(
                name=context.name,
                provider=result.provider,
                provider_kind=str(details.get("registration_mode", "listing_table")),
                source=None,
                details=details,
            ),
        )
        df = _maybe_cache(context, result.df)
        cache_prefix = None
    else:
        df, cache_prefix = _register_delta_provider(context)
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
            sql_options=sql_options_for_profile(context.runtime_profile),
            runtime_profile=context.runtime_profile,
        )
    _invalidate_information_schema_cache(
        context.runtime_profile,
        context.ctx,
        cache_prefix=cache_prefix,
    )
    _validate_schema_contracts(context)
    return df


def _ensure_catalog_schema(
    ctx: SessionContext,
    *,
    catalog: str,
    schema: str,
    caches: DatasetCaches | None = None,
) -> None:
    """Ensure catalog/schema containers exist before registration."""
    resolved_caches = _resolve_dataset_caches(caches)
    registered_catalogs = resolved_caches.registered_catalogs.setdefault(ctx, set())
    registered_schemas = resolved_caches.registered_schemas.setdefault(ctx, set())
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


__all__: list[str] = []

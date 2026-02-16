"""Listing-table focused helpers for dataset registration."""

# ruff: noqa: SLF001

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, cast

import pyarrow as pa
import pyarrow.dataset as ds
from datafusion.catalog import Catalog, Schema
from datafusion.dataframe import DataFrame

from datafusion_engine.dataset import registration_core as _core
from datafusion_engine.dataset.registry import DatasetLocation
from datafusion_engine.tables.registration import (
    register_listing_table as _register_listing_authority,
)

if TYPE_CHECKING:
    from datafusion import SessionContext

    from datafusion_engine.dataset.registration_core import (
        DataFusionRegistrationContext,
        DatasetCaches,
    )
    from schema_spec.dataset_spec import DataFusionScanOptions


_ExternalTableDdlRequest = _core._ExternalTableDdlRequest


def _ddl_statement(request: _ExternalTableDdlRequest) -> str:
    """Build CREATE EXTERNAL TABLE statements for listing registration.

    Returns:
        str: Fully rendered CREATE EXTERNAL TABLE statement.
    """
    ddl = f"{_core._ddl_prefix(unbounded=request.unbounded)} {_core._ddl_identifier(request.name)}"
    if request.column_defs:
        ddl = f"{ddl} ({', '.join(request.column_defs)})"
    ddl = (
        f"{ddl} STORED AS {request.format_name} LOCATION "
        f"{_core._ddl_string_literal(str(request.location.path))}"
    )
    if request.partition_cols:
        ddl = (
            f"{ddl} PARTITIONED BY "
            f"({', '.join(_core._ddl_identifier(name) for name in request.partition_cols)})"
        )
    order_clause = _core._ddl_order_clause(request.ordering)
    if order_clause:
        ddl = f"{ddl} {order_clause}"
    options_clause = _core._ddl_options_clause(dict(request.options))
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
    merged_schema, partition_cols, required_non_null, key_fields, defaults = (
        _core._ddl_schema_components(
            schema=schema,
            scan=scan,
        )
    )
    column_defs = (
        _core._ddl_column_definitions(
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
            ordering=_core._ddl_ordering_keys(scan, merged_schema),
            options=_core._ddl_options_payload(scan),
            unbounded=bool(scan is not None and scan.unbounded),
        )
    )
    constraints = _core._ddl_constraints(key_fields, merged_schema)
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
    if context.options.provider == "listing":
        result = _register_listing_authority(context)
        _, _, fingerprint_details = _core._update_table_provider_fingerprints(
            context.ctx,
            name=context.name,
            schema=result.df.schema(),
        )
        details = dict(result.details)
        if fingerprint_details:
            details.update(fingerprint_details)
        _core._record_table_provider_artifact(
            context.runtime_profile,
            artifact=_core._TableProviderArtifact(
                name=context.name,
                provider=result.provider,
                provider_kind=str(details.get("registration_mode", "listing_table")),
                source=None,
                details=details,
            ),
        )
        df = _core._maybe_cache(context, result.df)
        cache_prefix = None
    else:
        df, cache_prefix = _core._register_delta_provider(context)
    scan = context.options.scan
    projection_exprs = scan.projection_exprs if scan is not None else ()
    if not projection_exprs and context.options.schema is not None:
        projection_exprs = _core._projection_exprs_for_schema(
            actual_columns=df.schema().names,
            expected_schema=pa.schema(context.options.schema),
        )
    if projection_exprs:
        df = _core._apply_projection_exprs(
            context.ctx,
            table_name=context.name,
            projection_exprs=projection_exprs,
            sql_options=_core._sql_options.sql_options_for_profile(context.runtime_profile),
            runtime_profile=context.runtime_profile,
        )
    _core._invalidate_information_schema_cache(
        context.runtime_profile,
        context.ctx,
        cache_prefix=cache_prefix,
    )
    _core._validate_schema_contracts(context)
    return df


def _ensure_catalog_schema(
    ctx: SessionContext,
    *,
    catalog: str,
    schema: str,
    caches: DatasetCaches | None = None,
) -> None:
    """Ensure catalog/schema containers exist before registration."""
    resolved_caches = _core._resolve_dataset_caches(caches)
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


__all__ = [
    "_build_pyarrow_dataset",
    "_ddl_statement",
    "_ensure_catalog_schema",
    "_external_table_ddl",
    "_register_dataset_with_context",
]

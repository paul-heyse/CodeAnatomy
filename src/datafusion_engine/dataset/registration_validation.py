"""Runtime-setting and contract validation helpers for registration."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pyarrow as pa
from datafusion import SessionContext, SQLOptions

from arrow_utils.core.schema_constants import DEFAULT_VALUE_META
from datafusion_engine.arrow.metadata import schema_constraints_from_metadata
from datafusion_engine.schema.introspection_core import (
    SchemaIntrospector,
    table_constraint_rows,
)
from datafusion_engine.session.introspection import schema_introspector_for_profile
from datafusion_engine.session.runtime_config_policies import (
    DATAFUSION_MAJOR_VERSION,
    DATAFUSION_RUNTIME_SETTINGS_SKIP_VERSION,
)
from datafusion_engine.session.runtime_session import record_runtime_setting_override
from datafusion_engine.sql import options as _sql_options
from utils.validation import validate_required_items

if TYPE_CHECKING:
    from datafusion_engine.dataset.registration_core import DataFusionRegistrationContext
    from schema_spec.dataset_spec import DataFusionScanOptions


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
    resolved = sql_options.with_allow_statements(allow=True)
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


__all__: list[str] = []

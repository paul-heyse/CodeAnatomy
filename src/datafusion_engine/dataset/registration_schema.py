"""Schema-focused helpers for dataset registration."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING

import pyarrow as pa

from datafusion_engine.arrow.abi import schema_to_dict
from datafusion_engine.arrow.interop import SchemaLike, arrow_schema_from_df
from datafusion_engine.catalog.introspection import introspection_cache_for_ctx
from datafusion_engine.schema.contracts import (
    EvolutionPolicy,
    schema_contract_from_table_schema_contract,
)
from datafusion_engine.schema.introspection_core import SchemaIntrospector
from datafusion_engine.session.introspection import schema_introspector_for_profile
from datafusion_engine.sql.options import sql_options_for_profile
from schema_spec.dataset_spec import DataFusionScanOptions, TableSchemaContract
from utils.validation import find_missing

if TYPE_CHECKING:
    from datafusion import SessionContext

    from datafusion_engine.dataset.registration_core import DataFusionRegistrationContext
    from datafusion_engine.dataset.registration_delta_helpers import (
        _PartitionSchemaContext,
    )
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile


def _resolve_table_schema_contract(
    *,
    schema: pa.Schema | None,
    scan: DataFusionScanOptions | None,
    partition_cols: Sequence[tuple[str, pa.DataType]] | None,
) -> TableSchemaContract | None:
    """Resolve table-schema contract from file schema + scan options.

    Returns:
        TableSchemaContract | None: Effective contract for schema validation.
    """
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
    """Validate schema contracts against introspection snapshots.

    Raises:
        ValueError: If introspection snapshot violates the schema contract.
    """
    if (
        context.runtime_profile is None
        or not context.runtime_profile.catalog.enable_information_schema
    ):
        return
    scan = context.options.scan
    contract = _resolve_table_schema_contract(
        schema=context.options.schema,
        scan=scan,
        partition_cols=scan.partition_cols_pyarrow() if scan is not None else None,
    )
    if contract is None:
        return
    _validate_table_schema_contract(contract)
    cache = introspection_cache_for_ctx(
        context.ctx,
        sql_options=sql_options_for_profile(context.runtime_profile),
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


def _table_schema_snapshot(
    *,
    schema: SchemaLike | None,
    partition_cols: Sequence[tuple[str, str]] | None,
) -> dict[str, object] | None:
    """Build a schema snapshot payload for diagnostics artifacts.

    Returns:
        dict[str, object] | None: Normalized schema snapshot payload.
    """
    if schema is None and not partition_cols:
        return None
    return {
        "file_schema": schema_to_dict(schema) if schema is not None else None,
        "partition_cols": [{"name": name, "dtype": dtype} for name, dtype in (partition_cols or ())]
        or None,
    }


def _table_schema_partition_snapshot(
    ctx: SessionContext,
    *,
    table_name: str,
    expected_types: Mapping[str, str],
    expected_names: Sequence[str],
) -> tuple[dict[str, str], list[str], list[dict[str, str]]]:
    """Resolve partition-schema snapshot details for diagnostics.

    Returns:
        tuple[dict[str, str], list[str], list[dict[str, str]]]: Table schema types, missing
            columns, and type mismatches.
    """
    table_schema: pa.Schema | None = None
    try:
        table_schema = arrow_schema_from_df(ctx.table(table_name))
    except (KeyError, RuntimeError, TypeError, ValueError):
        table_schema = None
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


def _partition_column_rows(
    ctx: SessionContext,
    *,
    table_name: str,
    runtime_profile: DataFusionRuntimeProfile | None = None,
    cache_prefix: str | None = None,
) -> tuple[list[dict[str, object]] | None, str | None]:
    try:
        if runtime_profile is not None:
            table = schema_introspector_for_profile(
                runtime_profile,
                ctx,
                cache_prefix=cache_prefix,
            ).table_columns_with_ordinal(table_name)
        else:
            sql_options = sql_options_for_profile(runtime_profile)
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


def _partition_schema_validation(
    context: _PartitionSchemaContext,
    *,
    expected_partition_cols: Sequence[tuple[str, str]] | None,
) -> dict[str, object] | None:
    """Validate partition schema/order/type details.

    Returns:
        dict[str, object] | None: Partition validation payload for diagnostics.
    """
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


__all__ = [
    "_partition_column_rows",
    "_partition_columns_from_rows",
    "_partition_schema_validation",
    "_partition_type_mismatches",
    "_resolve_table_schema_contract",
    "_table_schema_partition_snapshot",
    "_table_schema_snapshot",
    "_validate_schema_contracts",
    "_validate_table_schema_contract",
]

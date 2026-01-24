"""Shared extract schema normalization helpers."""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from typing import cast

import pyarrow as pa

from arrowdsl.core.execution_context import ExecutionContext
from arrowdsl.core.interop import RecordBatchReaderLike, TableLike
from arrowdsl.schema.policy import SchemaPolicy
from arrowdsl.schema.schema import SchemaMetadataSpec, align_table, encode_table
from datafusion_engine.extract_registry import (
    dataset_metadata_with_options,
    dataset_schema_policy,
    dataset_spec,
)
from datafusion_engine.finalize import FinalizeContext, FinalizeResult
from datafusion_engine.runtime import sql_options_for_profile
from datafusion_engine.schema_introspection import SchemaIntrospector


def schema_policy_for_dataset(
    name: str,
    *,
    ctx: ExecutionContext,
    options: object | None = None,
    repo_id: str | None = None,
    enable_encoding: bool = True,
) -> SchemaPolicy:
    """Return the schema policy for an extract dataset.

    Returns
    -------
    SchemaPolicy
        Schema policy aligned to the dataset spec and runtime options.
    """
    return dataset_schema_policy(
        name,
        ctx=ctx,
        options=options,
        repo_id=repo_id,
        enable_encoding=enable_encoding,
    )


@dataclass(frozen=True)
class ExtractNormalizeOptions:
    """Normalization options for extract outputs."""

    options: object | None = None
    repo_id: str | None = None
    enable_encoding: bool = True


def _information_schema_column_order(
    name: str,
    *,
    ctx: ExecutionContext,
) -> tuple[str, ...] | None:
    runtime = ctx.runtime.datafusion
    if runtime is None or not runtime.enable_information_schema:
        return None
    sql_options = sql_options_for_profile(runtime)
    introspector = SchemaIntrospector(runtime.session_context(), sql_options=sql_options)
    try:
        rows = introspector.table_columns_with_ordinal(name)
    except (RuntimeError, TypeError, ValueError):
        return None
    columns: list[str] = []
    for row in rows:
        value = row.get("column_name")
        if not isinstance(value, str) or not value:
            continue
        if value in columns:
            continue
        columns.append(value)
    return tuple(columns) if columns else None


def _ordered_schema(schema: pa.Schema, ordered: Sequence[str]) -> pa.Schema:
    ordered_fields = [schema.field(name) for name in ordered if name in schema.names]
    if not ordered_fields:
        return schema
    ordered_names = {field.name for field in ordered_fields}
    remaining = [field for field in schema if field.name not in ordered_names]
    return pa.schema(ordered_fields + remaining, metadata=schema.metadata)


def metadata_spec_for_dataset(
    name: str,
    *,
    options: object | None = None,
    repo_id: str | None = None,
) -> SchemaMetadataSpec:
    """Return the metadata spec for an extract dataset.

    Returns
    -------
    SchemaMetadataSpec
        Metadata spec merged with runtime options.
    """
    return dataset_metadata_with_options(name, options=options, repo_id=repo_id)


def metadata_specs_for_datasets(
    names: Iterable[str],
    *,
    options: object | None = None,
    repo_id: str | None = None,
) -> Mapping[str, SchemaMetadataSpec]:
    """Return metadata specs for multiple datasets.

    Returns
    -------
    Mapping[str, SchemaMetadataSpec]
        Metadata spec map keyed by dataset name.
    """
    return {
        name: metadata_spec_for_dataset(name, options=options, repo_id=repo_id) for name in names
    }


def normalize_extract_output(
    name: str,
    table: TableLike,
    *,
    ctx: ExecutionContext,
    normalize: ExtractNormalizeOptions | None = None,
    apply_post_kernels: bool = True,
) -> TableLike:
    """Align, encode, and postprocess a table using registry policy.

    Returns
    -------
    TableLike
        Normalized table aligned to the dataset schema policy.
    """
    normalize = normalize or ExtractNormalizeOptions()
    processed = apply_pipeline_kernels(name, table) if apply_post_kernels else table
    policy = schema_policy_for_dataset(
        name,
        ctx=ctx,
        options=normalize.options,
        repo_id=normalize.repo_id,
        enable_encoding=normalize.enable_encoding,
    )
    schema = policy.resolved_schema()
    ordered = _information_schema_column_order(name, ctx=ctx)
    if ordered is not None:
        schema = _ordered_schema(schema, ordered)
    aligned = align_table(
        processed,
        schema=schema,
        safe_cast=policy.safe_cast,
        keep_extra_columns=policy.keep_extra_columns,
        on_error=policy.on_error,
    )
    if policy.encoding is None or not policy.encoding.dictionary_cols:
        return aligned
    columns = [field.name for field in schema if field.name in policy.encoding.dictionary_cols]
    return encode_table(aligned, columns=columns)


def normalize_extract_reader(
    name: str,
    reader: RecordBatchReaderLike,
    *,
    ctx: ExecutionContext,
    normalize: ExtractNormalizeOptions | None = None,
    apply_post_kernels: bool = True,
) -> RecordBatchReaderLike:
    """Normalize a RecordBatchReader for extract output streaming.

    Returns
    -------
    RecordBatchReaderLike
        Reader yielding normalized record batches.

    Raises
    ------
    ValueError
        Raised when streaming normalization is incompatible with the dataset policy.
    """
    normalize = normalize or ExtractNormalizeOptions()
    policy = schema_policy_for_dataset(
        name,
        ctx=ctx,
        options=normalize.options,
        repo_id=normalize.repo_id,
        enable_encoding=normalize.enable_encoding,
    )
    schema = policy.resolved_schema()
    ordered = _information_schema_column_order(name, ctx=ctx)
    if ordered is not None:
        schema = _ordered_schema(schema, ordered)
    if policy.keep_extra_columns:
        msg = f"Streaming normalization does not support keep_extra_columns for {name!r}."
        raise ValueError(msg)
    encode_columns = None
    if policy.encoding is not None and policy.encoding.dictionary_cols:
        encode_columns = tuple(
            field.name for field in schema if field.name in policy.encoding.dictionary_cols
        )

    def _iter_batches() -> Iterator[pa.RecordBatch]:
        for batch in reader:
            table = pa.Table.from_batches([batch], schema=batch.schema)
            processed = apply_pipeline_kernels(name, table) if apply_post_kernels else table
            aligned = align_table(
                processed,
                schema=schema,
                safe_cast=policy.safe_cast,
                keep_extra_columns=False,
                on_error=policy.on_error,
            )
            if encode_columns:
                aligned = encode_table(aligned, columns=encode_columns)
            yield from cast("pa.Table", aligned).to_batches()

    return pa.RecordBatchReader.from_batches(schema, _iter_batches())


def apply_pipeline_kernels(_name: str, table: TableLike) -> TableLike:
    """Apply postprocess kernels for a dataset name.

    Returns
    -------
    TableLike
        Table with pipeline kernels applied.
    """
    return table


def finalize_context_for_dataset(
    name: str,
    *,
    ctx: ExecutionContext,
    normalize: ExtractNormalizeOptions | None = None,
) -> FinalizeContext:
    """Return a finalize context for the dataset name.

    Returns
    -------
    FinalizeContext
        Finalize context configured with schema policy and contract.
    """
    normalize = normalize or ExtractNormalizeOptions()
    policy = schema_policy_for_dataset(
        name,
        ctx=ctx,
        options=normalize.options,
        repo_id=normalize.repo_id,
        enable_encoding=normalize.enable_encoding,
    )
    contract = dataset_spec(name).contract()
    return FinalizeContext(contract=contract, schema_policy=policy)


def validate_extract_output(
    name: str,
    table: TableLike,
    *,
    ctx: ExecutionContext,
    normalize: ExtractNormalizeOptions | None = None,
    apply_post_kernels: bool = True,
) -> FinalizeResult:
    """Validate an extract output, returning good/errors/stats tables.

    Returns
    -------
    FinalizeResult
        Finalize result with good, errors, stats, and alignment outputs.
    """
    normalize = normalize or ExtractNormalizeOptions()
    processed = apply_pipeline_kernels(name, table) if apply_post_kernels else table
    finalize_ctx = finalize_context_for_dataset(name, ctx=ctx, normalize=normalize)
    return finalize_ctx.run(processed, ctx=ctx)


__all__ = [
    "ExtractNormalizeOptions",
    "apply_pipeline_kernels",
    "finalize_context_for_dataset",
    "metadata_spec_for_dataset",
    "metadata_specs_for_datasets",
    "normalize_extract_output",
    "schema_policy_for_dataset",
    "validate_extract_output",
]

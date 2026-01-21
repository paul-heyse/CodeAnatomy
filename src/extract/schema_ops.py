"""Shared extract schema normalization helpers."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass

from arrowdsl.core.execution_context import ExecutionContext
from arrowdsl.core.interop import TableLike
from arrowdsl.finalize.finalize import FinalizeContext, FinalizeResult
from arrowdsl.schema.policy import SchemaPolicy
from arrowdsl.schema.schema import SchemaMetadataSpec, align_table, encode_table
from datafusion_engine.extract_registry import (
    dataset_metadata_with_options,
    dataset_schema_policy,
    dataset_spec,
)


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

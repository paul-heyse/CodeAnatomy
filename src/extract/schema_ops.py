"""Shared extract schema normalization helpers."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, replace

import pyarrow as pa

from arrow_utils.core.interop import TableLike
from arrow_utils.schema.metadata import SchemaMetadataSpec
from arrowdsl.schema.policy import SchemaPolicy
from core_types import DeterminismTier
from datafusion_engine.extract_registry import (
    dataset_metadata_with_options,
    dataset_schema,
    dataset_schema_policy,
    dataset_spec,
)
from datafusion_engine.finalize import FinalizeContext, FinalizeResult, FinalizeRunRequest
from datafusion_engine.runtime import DataFusionRuntimeProfile, sql_options_for_profile
from datafusion_engine.schema_introspection import SchemaIntrospector


def schema_policy_for_dataset(
    name: str,
    *,
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


@dataclass(frozen=True)
class _ValidateExtractRequest:
    """Inputs required to validate extract outputs."""

    runtime_profile: DataFusionRuntimeProfile
    determinism_tier: DeterminismTier
    normalize: ExtractNormalizeOptions | None = None
    apply_post_kernels: bool = True


def _information_schema_column_order(
    name: str,
    *,
    runtime_profile: DataFusionRuntimeProfile,
) -> tuple[str, ...] | None:
    if not runtime_profile.enable_information_schema:
        msg = "information_schema must be enabled for schema introspection."
        raise ValueError(msg)
    sql_options = sql_options_for_profile(runtime_profile)
    session_runtime = runtime_profile.session_runtime()
    introspector = SchemaIntrospector(session_runtime.ctx, sql_options=sql_options)
    try:
        rows = introspector.table_columns_with_ordinal(name)
    except (RuntimeError, TypeError, ValueError):
        return _fallback_column_order(name)
    columns: list[str] = []
    for row in rows:
        value = row.get("column_name")
        if not isinstance(value, str) or not value:
            continue
        if value in columns:
            continue
        columns.append(value)
    return tuple(columns) if columns else _fallback_column_order(name)


def _ordered_schema(schema: pa.Schema, ordered: Sequence[str]) -> pa.Schema:
    ordered_fields = [schema.field(name) for name in ordered if name in schema.names]
    if not ordered_fields:
        return schema
    ordered_names = {field.name for field in ordered_fields}
    remaining = [field for field in schema if field.name not in ordered_names]
    return pa.schema(ordered_fields + remaining, metadata=schema.metadata)


def _fallback_column_order(name: str) -> tuple[str, ...] | None:
    schema = dataset_schema(name)
    names = getattr(schema, "names", None)
    if names:
        return tuple(names)
    try:
        return tuple(field.name for field in schema)
    except TypeError:
        return None


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
    runtime_profile: DataFusionRuntimeProfile,
    normalize: ExtractNormalizeOptions | None = None,
) -> FinalizeContext:
    """Return a finalize context for the dataset name.

    Returns
    -------
    FinalizeContext
        Finalize context configured with schema policy and contract.
    """
    policy = normalized_schema_policy_for_dataset(
        name,
        runtime_profile=runtime_profile,
        normalize=normalize,
    )
    contract = dataset_spec(name).contract()
    return FinalizeContext(contract=contract, schema_policy=policy)


def normalized_schema_policy_for_dataset(
    name: str,
    *,
    runtime_profile: DataFusionRuntimeProfile,
    normalize: ExtractNormalizeOptions | None = None,
) -> SchemaPolicy:
    """Return schema policy with information_schema column ordering applied.

    Returns
    -------
    SchemaPolicy
        Schema policy with ordered schema when available.
    """
    normalize = normalize or ExtractNormalizeOptions()
    policy = schema_policy_for_dataset(
        name,
        runtime_profile=runtime_profile,
        options=normalize.options,
        repo_id=normalize.repo_id,
        enable_encoding=normalize.enable_encoding,
    )
    ordered = _information_schema_column_order(name, runtime_profile=runtime_profile)
    if ordered is None:
        return policy
    schema = _ordered_schema(policy.resolved_schema(), ordered)
    return replace(policy, schema=schema, metadata=None)


def validate_extract_output(
    name: str,
    table: TableLike,
    *,
    request: _ValidateExtractRequest,
) -> FinalizeResult:
    """Validate an extract output, returning good/errors/stats tables.

    Returns
    -------
    FinalizeResult
        Finalize result with good, errors, stats, and alignment outputs.
    """
    normalize = request.normalize or ExtractNormalizeOptions()
    processed = apply_pipeline_kernels(name, table) if request.apply_post_kernels else table
    finalize_ctx = finalize_context_for_dataset(
        name,
        runtime_profile=request.runtime_profile,
        normalize=normalize,
    )
    return finalize_ctx.run(
        processed,
        request=FinalizeRunRequest(
            runtime_profile=request.runtime_profile,
            determinism_tier=request.determinism_tier,
        ),
    )


__all__ = [
    "ExtractNormalizeOptions",
    "apply_pipeline_kernels",
    "finalize_context_for_dataset",
    "metadata_spec_for_dataset",
    "metadata_specs_for_datasets",
    "normalized_schema_policy_for_dataset",
    "schema_policy_for_dataset",
    "validate_extract_output",
]

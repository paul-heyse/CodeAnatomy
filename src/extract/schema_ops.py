"""Shared extract schema normalization helpers."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass

from arrowdsl.core.context import ExecutionContext
from arrowdsl.core.interop import TableLike
from arrowdsl.plan.plan import Plan
from arrowdsl.schema.ops import align_plan as align_plan_to_schema
from arrowdsl.schema.ops import encode_plan
from arrowdsl.schema.policy import SchemaPolicy
from arrowdsl.schema.schema import SchemaMetadataSpec
from extract.registry_specs import (
    dataset_metadata_with_options,
    dataset_schema_policy,
    postprocess_table,
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
        name: metadata_spec_for_dataset(name, options=options, repo_id=repo_id)
        for name in names
    }


def normalize_extract_plan(
    name: str,
    plan: Plan,
    *,
    ctx: ExecutionContext,
    normalize: ExtractNormalizeOptions | None = None,
) -> Plan:
    """Align and encode a plan using the dataset schema policy.

    Returns
    -------
    Plan
        Normalized plan aligned to the dataset schema policy.
    """
    normalize = normalize or ExtractNormalizeOptions()
    policy = schema_policy_for_dataset(
        name,
        ctx=ctx,
        options=normalize.options,
        repo_id=normalize.repo_id,
        enable_encoding=normalize.enable_encoding,
    )
    plan = align_plan_to_schema(
        plan,
        schema=policy.resolved_schema(),
        ctx=ctx,
        keep_extra_columns=policy.keep_extra_columns,
    )
    if policy.encoding is None:
        return plan
    columns = tuple(spec.column for spec in policy.encoding.specs)
    if not columns:
        return plan
    return encode_plan(plan, columns=columns, ctx=ctx)


def normalize_plan_with_policy(
    plan: Plan,
    *,
    policy: SchemaPolicy,
    ctx: ExecutionContext,
) -> Plan:
    """Align and encode a plan using a precomputed schema policy.

    Returns
    -------
    Plan
        Normalized plan aligned to the provided schema policy.
    """
    plan = align_plan_to_schema(
        plan,
        schema=policy.resolved_schema(),
        ctx=ctx,
        keep_extra_columns=policy.keep_extra_columns,
    )
    if policy.encoding is None:
        return plan
    columns = tuple(spec.column for spec in policy.encoding.specs)
    if not columns:
        return plan
    return encode_plan(plan, columns=columns, ctx=ctx)


def normalize_extract_output(
    name: str,
    table: TableLike,
    *,
    ctx: ExecutionContext,
    normalize: ExtractNormalizeOptions | None = None,
) -> TableLike:
    """Align, encode, and postprocess a table using registry policy.

    Returns
    -------
    TableLike
        Normalized table aligned to the dataset schema policy.
    """
    normalize = normalize or ExtractNormalizeOptions()
    processed = postprocess_table(name, table)
    policy = schema_policy_for_dataset(
        name,
        ctx=ctx,
        options=normalize.options,
        repo_id=normalize.repo_id,
        enable_encoding=normalize.enable_encoding,
    )
    return policy.apply(processed)


__all__ = [
    "ExtractNormalizeOptions",
    "metadata_spec_for_dataset",
    "metadata_specs_for_datasets",
    "normalize_extract_output",
    "normalize_extract_plan",
    "normalize_plan_with_policy",
    "schema_policy_for_dataset",
]

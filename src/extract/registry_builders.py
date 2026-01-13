"""Programmatic builders for extract dataset specs and schemas."""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING

from arrowdsl.compute.expr_core import HashExprSpec, MaskedHashExprSpec
from arrowdsl.compute.macros import CoalesceExpr, ConstExpr, FieldExpr
from arrowdsl.core.context import OrderingLevel
from arrowdsl.core.interop import SchemaLike
from arrowdsl.plan.query import ProjectionSpec, QuerySpec
from arrowdsl.schema.metadata import (
    extractor_metadata_spec,
    infer_ordering_keys,
    merge_metadata_specs,
    ordering_metadata_spec,
)
from arrowdsl.schema.schema import SchemaMetadataSpec
from arrowdsl.spec.infra import DatasetRegistration, register_dataset
from extract.registry_bundles import bundle
from extract.registry_fields import field, field_name, fields
from extract.registry_ids import hash_spec
from extract.registry_rows import DatasetRow, DerivedIdSpec
from extract.registry_templates import template
from schema_spec.system import DatasetSpec, make_table_spec

if TYPE_CHECKING:
    from arrowdsl.compute.expr_core import ExprSpec


@dataclass(frozen=True)
class QueryContext:
    """Context options for building dataset queries."""

    repo_id: str | None = None


def _dedupe(seq: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in seq:
        if value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out


def _row_field_keys(
    row: DatasetRow,
    *,
    extra_fields: Sequence[str] = (),
) -> tuple[str, ...]:
    base = row.row_fields if row.row_fields else row.fields
    if not row.row_extras and not extra_fields:
        return base
    return tuple(_dedupe((*base, *row.row_extras, *extra_fields)))


def _template_extra_fields(row: DatasetRow) -> tuple[str, ...]:
    if row.template is None:
        return ()
    templ = template(row.template)
    extras: list[str] = []
    if templ.evidence_rank is not None:
        extras.append("evidence_rank")
    if templ.confidence is not None:
        extras.append("confidence")
    return tuple(extras)


def _default_exprs(row: DatasetRow) -> dict[str, CoalesceExpr]:
    if row.template is None:
        return {}
    templ = template(row.template)
    defaults: dict[str, CoalesceExpr] = {}
    if templ.evidence_rank is not None:
        defaults["evidence_rank"] = CoalesceExpr(
            (
                FieldExpr("evidence_rank"),
                ConstExpr(templ.evidence_rank, dtype=field("evidence_rank").dtype),
            )
        )
    if templ.confidence is not None:
        defaults["confidence"] = CoalesceExpr(
            (
                FieldExpr("confidence"),
                ConstExpr(templ.confidence, dtype=field("confidence").dtype),
            )
        )
    return defaults


def _bundle_field_keys(bundle_names: Sequence[str]) -> list[str]:
    keys: list[str] = []
    for name in bundle_names:
        keys.extend(field.name for field in bundle(name).fields)
    return keys


def _derived_expr(spec: DerivedIdSpec, *, ctx: QueryContext) -> HashExprSpec | MaskedHashExprSpec:
    hash_spec_obj = hash_spec(spec.spec, repo_id=ctx.repo_id)
    if spec.kind == "hash":
        return HashExprSpec(spec=hash_spec_obj)
    required = spec.required or hash_spec_obj.cols
    return MaskedHashExprSpec(spec=hash_spec_obj, required=required)


def build_query_spec(row: DatasetRow, *, ctx: QueryContext) -> QuerySpec:
    """Build the QuerySpec for a dataset row.

    Returns
    -------
    QuerySpec
        Query specification for the dataset.
    """
    base_cols = _dedupe(_bundle_field_keys(row.bundles) + [field_name(key) for key in row.fields])
    derived_map: dict[str, ExprSpec] = {
        spec.name: _derived_expr(spec, ctx=ctx) for spec in row.derived
    }
    derived_map.update(_default_exprs(row))
    if not derived_map:
        return QuerySpec.simple(*base_cols)
    return QuerySpec(projection=ProjectionSpec(base=tuple(base_cols), derived=derived_map))


def build_metadata_spec(row: DatasetRow, *, base_columns: Sequence[str]) -> SchemaMetadataSpec:
    """Build the base metadata spec for a dataset row.

    Returns
    -------
    SchemaMetadataSpec
        Metadata spec for the dataset schema.
    """
    templ = template(row.template) if row.template is not None else None
    level = templ.ordering_level if templ is not None else OrderingLevel.IMPLICIT
    extractor_name = templ.extractor_name if templ is not None else "extract"
    extractor_extra = templ.metadata_extra if templ is not None else None
    if row.metadata_extra:
        merged_extra = dict(extractor_extra or {})
        merged_extra.update(row.metadata_extra)
        extractor_extra = merged_extra
    if row.ordering_keys:
        ordering_keys = row.ordering_keys
    elif row.join_keys:
        ordering_keys = tuple((name, "ascending") for name in row.join_keys)
    else:
        ordering_keys = infer_ordering_keys(base_columns)
    ordering = ordering_metadata_spec(level, keys=ordering_keys)
    extractor_meta = extractor_metadata_spec(
        extractor_name,
        row.version,
        extra=extractor_extra,
    )
    return merge_metadata_specs(ordering, extractor_meta)


def build_row_schema(row: DatasetRow) -> SchemaLike:
    """Build the row schema used for plan_from_rows ingestion.

    Returns
    -------
    SchemaLike
        Arrow schema for row-based ingestion.
    """
    bundle_fields = [bundle_field for name in row.bundles for bundle_field in bundle(name).fields]
    row_fields = fields(_row_field_keys(row, extra_fields=_template_extra_fields(row)))
    return make_table_spec(
        name=f"{row.name}_rows",
        version=row.version,
        bundles=(),
        fields=(*bundle_fields, *row_fields),
    ).to_arrow_schema()


def build_dataset_spec(row: DatasetRow, *, ctx: QueryContext) -> DatasetSpec:
    """Build the DatasetSpec for a dataset row.

    Returns
    -------
    DatasetSpec
        Dataset spec including the query and metadata registration.
    """
    bundles = tuple(bundle(name) for name in row.bundles)
    field_keys = _dedupe((*row.fields, *_template_extra_fields(row)))
    table_spec = make_table_spec(
        name=row.name,
        version=row.version,
        bundles=bundles,
        fields=fields(field_keys),
    )
    base_columns = _dedupe(
        _bundle_field_keys(row.bundles) + [field_name(key) for key in row.fields]
    )
    metadata_spec = build_metadata_spec(row, base_columns=base_columns)
    registration = DatasetRegistration(
        query_spec=build_query_spec(row, ctx=ctx),
        metadata_spec=metadata_spec,
    )
    return register_dataset(table_spec=table_spec, registration=registration)


__all__ = [
    "QueryContext",
    "build_dataset_spec",
    "build_metadata_spec",
    "build_query_spec",
    "build_row_schema",
]

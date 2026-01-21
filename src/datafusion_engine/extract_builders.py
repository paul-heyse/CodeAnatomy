"""Programmatic builders for extract dataset specs and schemas."""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass

from arrowdsl.core.ordering import OrderingLevel
from arrowdsl.schema.metadata import (
    extractor_metadata_spec,
    extractor_option_defaults_spec,
    infer_ordering_keys,
    merge_metadata_specs,
    ordering_metadata_spec,
)
from arrowdsl.schema.schema import SchemaMetadataSpec
from arrowdsl.spec.expr_ir import ExprIR
from datafusion_engine.extract_bundles import bundle
from datafusion_engine.extract_ids import hash_spec
from datafusion_engine.extract_metadata import ExtractMetadata
from datafusion_engine.extract_templates import config as extractor_config
from datafusion_engine.extract_templates import template
from ibis_engine.hashing import hash_expr_ir, masked_hash_expr_ir
from ibis_engine.query_compiler import IbisProjectionSpec, IbisQuerySpec


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


def _template_extra_fields(row: ExtractMetadata) -> tuple[str, ...]:
    if row.template is None:
        return ()
    templ = template(row.template)
    extras: list[str] = []
    if templ.evidence_rank is not None:
        extras.append("evidence_rank")
    if templ.confidence is not None:
        extras.append("confidence")
    return tuple(extras)


def _default_exprs(row: ExtractMetadata) -> dict[str, ExprIR]:
    if row.template is None:
        return {}
    templ = template(row.template)
    defaults: dict[str, ExprIR] = {}
    if templ.evidence_rank is not None:
        defaults["evidence_rank"] = ExprIR(
            op="call",
            name="coalesce",
            args=(
                ExprIR(op="field", name="evidence_rank"),
                ExprIR(op="literal", value=templ.evidence_rank),
            ),
        )
    if templ.confidence is not None:
        defaults["confidence"] = ExprIR(
            op="call",
            name="coalesce",
            args=(
                ExprIR(op="field", name="confidence"),
                ExprIR(op="literal", value=templ.confidence),
            ),
        )
    return defaults


def _bundle_field_keys(bundle_names: Sequence[str]) -> list[str]:
    keys: list[str] = []
    for name in bundle_names:
        keys.extend(field.name for field in bundle(name).fields)
    return keys


def _derived_expr(spec: ExtractMetadata, *, ctx: QueryContext, name: str) -> ExprIR:
    derived = next(item for item in spec.derived if item.name == name)
    hash_spec_obj = hash_spec(derived.spec, repo_id=ctx.repo_id)
    if derived.kind == "hash":
        return hash_expr_ir(spec=hash_spec_obj)
    required = derived.required or hash_spec_obj.cols
    return masked_hash_expr_ir(spec=hash_spec_obj, required=required)


def _normalize_ordering_key(key: str) -> str:
    return key


def build_query_spec(row: ExtractMetadata, *, ctx: QueryContext) -> IbisQuerySpec:
    """Build the IbisQuerySpec for a dataset row.

    Returns
    -------
    IbisQuerySpec
        Ibis query specification for the dataset.
    """
    base_cols = _dedupe(_bundle_field_keys(row.bundles) + list(row.fields))
    derived_map: dict[str, ExprIR] = {
        spec.name: _derived_expr(row, ctx=ctx, name=spec.name) for spec in row.derived
    }
    derived_map.update(_default_exprs(row))
    if not derived_map:
        return IbisQuerySpec.simple(*base_cols)
    return IbisQuerySpec(projection=IbisProjectionSpec(base=tuple(base_cols), derived=derived_map))


def build_metadata_spec(row: ExtractMetadata, *, base_columns: Sequence[str]) -> SchemaMetadataSpec:
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
    if row.template is not None:
        defaults_meta = extractor_option_defaults_spec(
            extractor_config(row.template).defaults
        ).schema_metadata
        if defaults_meta:
            merged_extra = dict(extractor_extra or {})
            merged_extra.update(defaults_meta)
            extractor_extra = merged_extra
    if row.ordering_keys:
        ordering_keys = tuple(
            (_normalize_ordering_key(item.column), item.order) for item in row.ordering_keys
        )
    elif row.join_keys:
        ordering_keys = tuple(
            (_normalize_ordering_key(name), "ascending") for name in row.join_keys
        )
    else:
        ordering_keys = infer_ordering_keys(base_columns)
    ordering = ordering_metadata_spec(level, keys=ordering_keys)
    extractor_meta = extractor_metadata_spec(
        extractor_name,
        row.version,
        extra=extractor_extra,
    )
    return merge_metadata_specs(ordering, extractor_meta)


__all__ = [
    "QueryContext",
    "build_metadata_spec",
    "build_query_spec",
]

"""Field catalog for normalize dataset schemas."""

from __future__ import annotations

from collections.abc import Mapping, Sequence

import pyarrow as pa

from arrowdsl.schema.build import list_view_type, struct_type
from registry_common.field_catalog import FieldCatalog
from schema_spec.specs import ArrowFieldSpec, dict_field

DIAG_TAGS_TYPE = list_view_type(pa.string(), large=True)
DIAG_DETAIL_STRUCT = struct_type(
    {
        "detail_kind": pa.string(),
        "error_type": pa.string(),
        "source": pa.string(),
        "tags": DIAG_TAGS_TYPE,
    }
)
DIAG_DETAILS_TYPE = list_view_type(DIAG_DETAIL_STRUCT, large=True)


def _spec(
    name: str,
    dtype: pa.DataType,
    *,
    nullable: bool = True,
    metadata: Mapping[str, str] | None = None,
) -> ArrowFieldSpec:
    return ArrowFieldSpec(
        name=name,
        dtype=dtype,
        nullable=nullable,
        metadata=dict(metadata or {}),
    )


def _dict(name: str) -> ArrowFieldSpec:
    return dict_field(name)


_FIELD_CATALOG = FieldCatalog()


def _register(key: str, spec: ArrowFieldSpec) -> None:
    _FIELD_CATALOG.register(key, spec)


def _register_many(entries: Mapping[str, ArrowFieldSpec]) -> None:
    _FIELD_CATALOG.register_many(entries)


_register_many(
    {
        "file_id": _spec("file_id", pa.string()),
        "path": _spec("path", pa.string()),
        "line_base": _spec("line_base", pa.int32()),
        "col_unit": _spec("col_unit", pa.string()),
        "end_exclusive": _spec("end_exclusive", pa.bool_()),
        "span_id": _spec("span_id", pa.string()),
        "bstart": _spec("bstart", pa.int64()),
        "bend": _spec("bend", pa.int64()),
        "evidence_family": _spec("evidence_family", pa.string()),
        "source": _spec("source", pa.string()),
        "role": _spec("role", pa.string()),
        "confidence": _spec("confidence", pa.float32()),
        "ambiguity_group_id": _spec("ambiguity_group_id", pa.string()),
        "rule_name": _spec("rule_name", pa.string()),
        "type_expr_id": _spec("type_expr_id", pa.string()),
        "owner_def_id": _spec("owner_def_id", pa.string()),
        "param_name": _spec("param_name", pa.string()),
        "expr_kind": _dict("expr_kind"),
        "expr_role": _dict("expr_role"),
        "expr_text": _spec("expr_text", pa.string()),
        "type_repr": _spec("type_repr", pa.string()),
        "type_id": _spec("type_id", pa.string()),
        "type_form": _dict("type_form"),
        "origin": _dict("origin"),
        "block_id": _spec("block_id", pa.string()),
        "code_unit_id": _spec("code_unit_id", pa.string()),
        "start_offset": _spec("start_offset", pa.int32()),
        "end_offset": _spec("end_offset", pa.int32()),
        "kind": _dict("kind"),
        "edge_id": _spec("edge_id", pa.string()),
        "src_block_id": _spec("src_block_id", pa.string()),
        "dst_block_id": _spec("dst_block_id", pa.string()),
        "cond_instr_id": _spec("cond_instr_id", pa.string()),
        "exc_index": _spec("exc_index", pa.int32()),
        "event_id": _spec("event_id", pa.string()),
        "instr_id": _spec("instr_id", pa.string()),
        "symbol": _spec("symbol", pa.string()),
        "opname": _dict("opname"),
        "offset": _spec("offset", pa.int32()),
        "argval_str": _spec("argval_str", pa.string()),
        "argrepr": _spec("argrepr", pa.string()),
        "def_event_id": _spec("def_event_id", pa.string()),
        "use_event_id": _spec("use_event_id", pa.string()),
        "document_id": _spec("document_id", pa.string()),
        "reason": _dict("reason"),
        "diag_id": _spec("diag_id", pa.string()),
        "severity": _dict("severity"),
        "message": _spec("message", pa.string()),
        "diag_source": _dict("diag_source"),
        "code": _spec("code", pa.string()),
        "details": _spec("details", DIAG_DETAILS_TYPE),
    }
)


def field(key: str) -> ArrowFieldSpec:
    """Return the ArrowFieldSpec for a field key.

    Returns
    -------
    ArrowFieldSpec
        Field specification for the key.
    """
    return _FIELD_CATALOG.field(key)


def fields(keys: Sequence[str]) -> list[ArrowFieldSpec]:
    """Return ArrowFieldSpec instances for field keys.

    Returns
    -------
    list[ArrowFieldSpec]
        Field specifications for the keys.
    """
    return _FIELD_CATALOG.fields_for(keys)


def field_name(key: str) -> str:
    """Return the column name for a field key.

    Returns
    -------
    str
        Column name for the key.
    """
    return field(key).name


__all__ = [
    "DIAG_DETAILS_TYPE",
    "DIAG_DETAIL_STRUCT",
    "DIAG_TAGS_TYPE",
    "field",
    "field_name",
    "fields",
]

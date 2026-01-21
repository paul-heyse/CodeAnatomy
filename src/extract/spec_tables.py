"""Arrow spec tables for extract dataset registries."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass

import pyarrow as pa

from arrowdsl.schema.build import list_view_type
from arrowdsl.schema.schema import EncodingPolicy, EncodingSpec
from arrowdsl.spec.codec import parse_string_tuple
from arrowdsl.spec.io import rows_from_table, table_from_rows
from datafusion_engine.extract_metadata import ExtractDerivedIdSpec, ExtractOrderingKeySpec

DERIVED_ID_STRUCT = pa.struct(
    [
        pa.field("name", pa.string(), nullable=False),
        pa.field("spec", pa.string(), nullable=False),
        pa.field("kind", pa.string(), nullable=False),
        pa.field("required", list_view_type(pa.string()), nullable=True),
    ]
)

ORDERING_KEY_STRUCT = pa.struct(
    [
        pa.field("column", pa.string(), nullable=False),
        pa.field("order", pa.string(), nullable=False),
    ]
)

EXTRACT_DATASET_SCHEMA = pa.schema(
    [
        pa.field("name", pa.string(), nullable=False),
        pa.field("version", pa.int32(), nullable=False),
        pa.field("bundles", list_view_type(pa.string()), nullable=True),
        pa.field("fields", list_view_type(pa.string()), nullable=True),
        pa.field("derived", list_view_type(DERIVED_ID_STRUCT), nullable=True),
        pa.field("row_fields", list_view_type(pa.string()), nullable=True),
        pa.field("row_extras", list_view_type(pa.string()), nullable=True),
        pa.field("template", pa.string(), nullable=True),
        pa.field("ordering_keys", list_view_type(ORDERING_KEY_STRUCT), nullable=True),
        pa.field("join_keys", list_view_type(pa.string()), nullable=True),
        pa.field("enabled_when", pa.string(), nullable=True),
        pa.field("feature_flag", pa.string(), nullable=True),
        pa.field("postprocess", pa.string(), nullable=True),
        pa.field("metadata_extra", pa.map_(pa.string(), pa.string()), nullable=True),
        pa.field("evidence_required_columns", list_view_type(pa.string()), nullable=True),
        pa.field("pipeline_name", pa.string(), nullable=True),
    ],
    metadata={b"spec_kind": b"extract_datasets"},
)


EXTRACT_DATASET_ENCODING = EncodingPolicy(
    specs=(
        EncodingSpec(column="name"),
        EncodingSpec(column="template"),
        EncodingSpec(column="pipeline_name"),
    )
)


@dataclass(frozen=True)
class ExtractDatasetRowSpec:
    """Spec row describing an extract dataset."""

    name: str
    version: int
    bundles: tuple[str, ...]
    fields: tuple[str, ...]
    derived: tuple[ExtractDerivedIdSpec, ...] = ()
    row_fields: tuple[str, ...] = ()
    row_extras: tuple[str, ...] = ()
    template: str | None = None
    ordering_keys: tuple[ExtractOrderingKeySpec, ...] = ()
    join_keys: tuple[str, ...] = ()
    enabled_when: str | None = None
    feature_flag: str | None = None
    postprocess: str | None = None
    metadata_extra: dict[str, str] | None = None
    evidence_required_columns: tuple[str, ...] = ()
    pipeline_name: str | None = None


def extract_dataset_table_from_rows(
    rows: Sequence[Mapping[str, object]],
) -> pa.Table:
    """Return an extract dataset table from row records.

    Returns
    -------
    pyarrow.Table
        Table of extract dataset rows.
    """
    table = table_from_rows(EXTRACT_DATASET_SCHEMA, rows)
    return EXTRACT_DATASET_ENCODING.apply(table)


def _string_tuple(value: object | None, *, label: str) -> tuple[str, ...]:
    return parse_string_tuple(value, label=label)


def _metadata_extra(value: object | None) -> dict[str, str]:
    if value is None:
        return {}
    if isinstance(value, Mapping):
        return {str(key): str(val) for key, val in value.items()}
    msg = "metadata_extra must be a mapping of strings."
    raise TypeError(msg)


def _derived_specs(value: object | None) -> tuple[ExtractDerivedIdSpec, ...]:
    if not value:
        return ()
    items: list[ExtractDerivedIdSpec] = []
    if not isinstance(value, Sequence):
        msg = "derived specs must be a sequence of mappings."
        raise TypeError(msg)
    for item in value:
        if not isinstance(item, Mapping):
            msg = "derived spec entries must be mappings."
            raise TypeError(msg)
        items.append(
            ExtractDerivedIdSpec(
                name=str(item.get("name")),
                spec=str(item.get("spec")),
                kind=str(item.get("kind", "masked_hash")),
                required=_string_tuple(item.get("required"), label="derived.required"),
            )
        )
    return tuple(items)


def _ordering_specs(value: object | None) -> tuple[ExtractOrderingKeySpec, ...]:
    if not value:
        return ()
    if not isinstance(value, Sequence):
        msg = "ordering_keys must be a sequence of mappings."
        raise TypeError(msg)
    items: list[ExtractOrderingKeySpec] = []
    for item in value:
        if not isinstance(item, Mapping):
            msg = "ordering key entries must be mappings."
            raise TypeError(msg)
        items.append(
            ExtractOrderingKeySpec(
                column=str(item.get("column")),
                order=str(item.get("order", "ascending")),
            )
        )
    return tuple(items)


def _coerce_int(value: object | None, *, label: str) -> int:
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    msg = f"{label} must be an int."
    raise TypeError(msg)


def dataset_rows_from_table(table: pa.Table) -> tuple[ExtractDatasetRowSpec, ...]:
    """Decode extract dataset rows from a spec table.

    Returns
    -------
    tuple[ExtractDatasetRowSpec, ...]
        Extract dataset row specs.
    """
    return tuple(
        ExtractDatasetRowSpec(
            name=str(record.get("name")),
            version=_coerce_int(record.get("version"), label="version"),
            bundles=_string_tuple(record.get("bundles"), label="bundles"),
            fields=_string_tuple(record.get("fields"), label="fields"),
            derived=_derived_specs(record.get("derived")),
            row_fields=_string_tuple(record.get("row_fields"), label="row_fields"),
            row_extras=_string_tuple(record.get("row_extras"), label="row_extras"),
            template=str(record.get("template")) if record.get("template") else None,
            ordering_keys=_ordering_specs(record.get("ordering_keys")),
            join_keys=_string_tuple(record.get("join_keys"), label="join_keys"),
            enabled_when=str(record.get("enabled_when")) if record.get("enabled_when") else None,
            feature_flag=str(record.get("feature_flag")) if record.get("feature_flag") else None,
            postprocess=str(record.get("postprocess")) if record.get("postprocess") else None,
            metadata_extra=_metadata_extra(record.get("metadata_extra")),
            evidence_required_columns=_string_tuple(
                record.get("evidence_required_columns"),
                label="evidence_required_columns",
            ),
            pipeline_name=str(record.get("pipeline_name")) if record.get("pipeline_name") else None,
        )
        for record in rows_from_table(table)
    )


__all__ = [
    "DERIVED_ID_STRUCT",
    "EXTRACT_DATASET_SCHEMA",
    "ORDERING_KEY_STRUCT",
    "ExtractDatasetRowSpec",
    "ExtractDerivedIdSpec",
    "ExtractOrderingKeySpec",
    "dataset_rows_from_table",
    "extract_dataset_table_from_rows",
]

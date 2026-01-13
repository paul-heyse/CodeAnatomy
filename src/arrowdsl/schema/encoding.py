"""Encoding policy helpers for schema specifications."""

from __future__ import annotations

from collections.abc import Sequence

import pyarrow.types as patypes

from arrowdsl.schema.schema import EncodingPolicy, EncodingSpec
from schema_spec.specs import (
    ENCODING_DICTIONARY,
    ENCODING_META,
    ArrowFieldSpec,
    TableSchemaSpec,
)


def _encoding_hint(field: ArrowFieldSpec) -> str | None:
    if field.encoding is not None:
        return field.encoding
    return field.metadata.get(ENCODING_META)


def _encoding_spec_from_field(field: ArrowFieldSpec) -> EncodingSpec | None:
    hint = _encoding_hint(field)
    if hint != ENCODING_DICTIONARY:
        if patypes.is_dictionary(field.dtype):
            return EncodingSpec(column=field.name, dtype=field.dtype)
        return None
    if patypes.is_dictionary(field.dtype):
        return EncodingSpec(column=field.name, dtype=field.dtype)
    return EncodingSpec(column=field.name)


def encoding_policy_from_spec(table_spec: TableSchemaSpec) -> EncodingPolicy:
    """Return an encoding policy derived from a TableSchemaSpec.

    Returns
    -------
    EncodingPolicy
        Encoding policy derived from the table spec.
    """
    specs = tuple(
        spec
        for field in table_spec.fields
        if (spec := _encoding_spec_from_field(field)) is not None
    )
    return EncodingPolicy(specs=specs)


def encoding_policy_from_fields(fields: Sequence[ArrowFieldSpec]) -> EncodingPolicy:
    """Return an encoding policy derived from ArrowFieldSpec values.

    Returns
    -------
    EncodingPolicy
        Encoding policy derived from field specs.
    """
    specs = tuple(
        spec for field in fields if (spec := _encoding_spec_from_field(field)) is not None
    )
    return EncodingPolicy(specs=specs)


__all__ = [
    "encoding_policy_from_fields",
    "encoding_policy_from_spec",
]

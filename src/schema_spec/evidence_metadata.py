"""Evidence metadata field definitions and bundle helpers.

Provide canonical field definitions for evidence metadata columns used across
extraction, normalization, and relationship resolution pipelines. These fields
track provenance information such as source task, priority, confidence, and
resolution method.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pyarrow as pa

from datafusion_engine.arrow import interop
from schema_spec.field_spec import FieldSpec
from schema_spec.specs import FieldBundle

if TYPE_CHECKING:
    from datafusion_engine.arrow.interop import DataTypeLike


# Canonical evidence metadata fields for provenance tracking.
# These columns appear in relationship outputs and evidence tables.
EVIDENCE_METADATA_FIELDS: tuple[pa.Field, ...] = (
    pa.field("task_name", pa.string(), nullable=True),
    pa.field("task_priority", pa.int32(), nullable=True),
    pa.field("confidence", pa.float64(), nullable=True),
    pa.field("score", pa.float64(), nullable=True),
    pa.field("resolution_method", pa.string(), nullable=True),
)


def evidence_metadata_bundle() -> FieldBundle:
    """Return a bundle containing canonical evidence metadata fields.

    The bundle includes provenance columns for tracking extraction source,
    priority ordering, confidence scores, and resolution methodology.

    Returns
    -------
    FieldBundle
        Bundle containing evidence metadata field specs.
    """
    return FieldBundle(
        name="evidence_metadata",
        fields=(
            FieldSpec(name="task_name", dtype=interop.string(), nullable=True),
            FieldSpec(name="task_priority", dtype=interop.int32(), nullable=True),
            FieldSpec(name="confidence", dtype=interop.float64(), nullable=True),
            FieldSpec(name="score", dtype=interop.float64(), nullable=True),
            FieldSpec(name="resolution_method", dtype=interop.string(), nullable=True),
        ),
    )


def evidence_struct_type() -> DataTypeLike:
    """Return a struct type for nested evidence metadata.

    Use this when embedding evidence metadata as a nested struct column
    rather than flat top-level columns.

    Returns
    -------
    DataTypeLike
        PyArrow struct type for evidence metadata.
    """
    return pa.struct(list(EVIDENCE_METADATA_FIELDS))


def evidence_metadata_field_names() -> tuple[str, ...]:
    """Return the canonical evidence metadata field names.

    Returns
    -------
    tuple[str, ...]
        Field names in canonical order.
    """
    return tuple(field.name for field in EVIDENCE_METADATA_FIELDS)


def evidence_metadata_defaults() -> dict[str, object]:
    """Return default values for evidence metadata fields.

    Use these defaults when constructing evidence rows without explicit
    provenance information.

    Returns
    -------
    dict[str, object]
        Mapping of field name to default value.
    """
    return {
        "task_name": None,
        "task_priority": None,
        "confidence": 0.5,
        "score": 0.5,
        "resolution_method": None,
    }


__all__ = [
    "EVIDENCE_METADATA_FIELDS",
    "evidence_metadata_bundle",
    "evidence_metadata_defaults",
    "evidence_metadata_field_names",
    "evidence_struct_type",
]

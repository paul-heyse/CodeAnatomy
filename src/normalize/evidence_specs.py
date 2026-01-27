"""Evidence defaults derived from normalize schema metadata."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from arrow_utils.core.interop import SchemaLike
from arrow_utils.schema.metadata import (
    decode_metadata_list,
    decode_metadata_map,
    decode_metadata_scalar_map,
)
from schema_spec.literals import parse_scalar_value

if TYPE_CHECKING:
    from arrow_utils.core.expr_types import ScalarValue

EVIDENCE_REQUIRED_COLUMNS_META = b"evidence_required_columns"
EVIDENCE_REQUIRED_TYPES_META = b"evidence_required_types"
EVIDENCE_REQUIRED_METADATA_META = b"evidence_required_metadata"
EVIDENCE_OUTPUT_MAP_META = b"evidence_output_map"
EVIDENCE_OUTPUT_LITERALS_META = b"evidence_output_literals"
EVIDENCE_OUTPUT_PROVENANCE_META = b"evidence_output_provenance"


@dataclass(frozen=True)
class EvidenceSpec:
    """Evidence requirements for normalize outputs."""

    required_columns: tuple[str, ...] = ()
    required_types: Mapping[str, str] = field(default_factory=dict)
    required_metadata: Mapping[bytes, bytes] = field(default_factory=dict)


@dataclass(frozen=True)
class EvidenceOutput:
    """Evidence output hints derived from schema metadata."""

    column_map: Mapping[str, str] = field(default_factory=dict)
    literals: Mapping[str, ScalarValue] = field(default_factory=dict)
    provenance_columns: tuple[str, ...] = ()


def evidence_spec_from_schema(schema: SchemaLike) -> EvidenceSpec | None:
    """Derive evidence requirements from schema metadata.

    Returns
    -------
    EvidenceSpec | None
        Evidence spec defaults or ``None`` when absent.
    """
    meta = schema.metadata or {}
    required_columns = tuple(_meta_list(meta, EVIDENCE_REQUIRED_COLUMNS_META))
    required_types = _meta_map(meta, EVIDENCE_REQUIRED_TYPES_META)
    required_metadata = _meta_metadata_map(meta, EVIDENCE_REQUIRED_METADATA_META)
    if not required_columns and not required_types and not required_metadata:
        return None
    return EvidenceSpec(
        required_columns=required_columns,
        required_types=required_types,
        required_metadata=required_metadata,
    )


def evidence_output_from_schema(schema: SchemaLike) -> EvidenceOutput | None:
    """Derive evidence output defaults from schema metadata.

    Returns
    -------
    EvidenceOutput | None
        Evidence output defaults or ``None`` when absent.
    """
    meta = schema.metadata or {}
    column_map = _meta_map(meta, EVIDENCE_OUTPUT_MAP_META)
    literals = _meta_scalar_map(meta, EVIDENCE_OUTPUT_LITERALS_META)
    provenance = tuple(_meta_list(meta, EVIDENCE_OUTPUT_PROVENANCE_META))
    if not column_map and not literals and not provenance:
        return None
    return EvidenceOutput(
        column_map=column_map,
        literals=literals,
        provenance_columns=provenance,
    )


def _meta_list(meta: Mapping[bytes, bytes], key: bytes) -> list[str]:
    raw = meta.get(key)
    if raw is None:
        return []
    return [str(item) for item in decode_metadata_list(raw) if str(item)]


def _meta_map(meta: Mapping[bytes, bytes], key: bytes) -> dict[str, str]:
    raw = meta.get(key)
    if raw is None:
        return {}
    payload = decode_metadata_map(raw)
    return {str(k): str(v) for k, v in payload.items()}


def _meta_scalar_map(meta: Mapping[bytes, bytes], key: bytes) -> dict[str, ScalarValue]:
    raw = meta.get(key)
    if raw is None:
        return {}
    payload = decode_metadata_scalar_map(raw)
    parsed: dict[str, ScalarValue] = {}
    for name, value in payload.items():
        parsed[str(name)] = parse_scalar_value(value)
    return parsed


def _meta_metadata_map(meta: Mapping[bytes, bytes], key: bytes) -> dict[bytes, bytes]:
    raw = meta.get(key)
    if raw is None:
        return {}
    payload = decode_metadata_map(raw)
    return {str(k).encode("utf-8"): str(v).encode("utf-8") for k, v in payload.items()}


__all__ = [
    "EVIDENCE_OUTPUT_LITERALS_META",
    "EVIDENCE_OUTPUT_MAP_META",
    "EVIDENCE_OUTPUT_PROVENANCE_META",
    "EVIDENCE_REQUIRED_COLUMNS_META",
    "EVIDENCE_REQUIRED_METADATA_META",
    "EVIDENCE_REQUIRED_TYPES_META",
    "EvidenceOutput",
    "EvidenceSpec",
    "evidence_output_from_schema",
    "evidence_spec_from_schema",
]

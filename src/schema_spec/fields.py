"""Shared field bundles and constants for schema specs."""

from __future__ import annotations

from dataclasses import dataclass

import arrowdsl.pyarrow_core as pa
from schema_spec.core import ArrowFieldSpec

DICT_STRING = pa.dictionary(pa.int32(), pa.string())

PROVENANCE_COLS: tuple[str, ...] = (
    "prov_filename",
    "prov_fragment_index",
    "prov_batch_index",
    "prov_last_in_fragment",
)

PROVENANCE_SOURCE_FIELDS: dict[str, str] = {
    "prov_filename": "__filename",
    "prov_fragment_index": "__fragment_index",
    "prov_batch_index": "__batch_index",
    "prov_last_in_fragment": "__last_in_fragment",
}


@dataclass(frozen=True)
class FieldBundle:
    """Bundle of fields plus required/key constraints."""

    name: str
    fields: tuple[ArrowFieldSpec, ...]
    required_non_null: tuple[str, ...] = ()
    key_fields: tuple[str, ...] = ()


def file_identity_bundle(*, include_sha256: bool = True) -> FieldBundle:
    """Return a bundle for file identity columns.

    Returns
    -------
    FieldBundle
        Bundle containing file identity fields.
    """
    fields = [
        ArrowFieldSpec(name="file_id", dtype=pa.string()),
        ArrowFieldSpec(name="path", dtype=pa.string()),
    ]
    if include_sha256:
        fields.append(ArrowFieldSpec(name="file_sha256", dtype=pa.string()))
    return FieldBundle(name="file_identity", fields=tuple(fields))


def span_bundle() -> FieldBundle:
    """Return a bundle for byte-span columns.

    Returns
    -------
    FieldBundle
        Bundle containing byte-span fields.
    """
    return FieldBundle(
        name="span",
        fields=(
            ArrowFieldSpec(name="bstart", dtype=pa.int64()),
            ArrowFieldSpec(name="bend", dtype=pa.int64()),
        ),
    )


def call_span_bundle() -> FieldBundle:
    """Return a bundle for callsite byte-span columns.

    Returns
    -------
    FieldBundle
        Bundle containing callsite byte-span fields.
    """
    return FieldBundle(
        name="call_span",
        fields=(
            ArrowFieldSpec(name="call_bstart", dtype=pa.int64()),
            ArrowFieldSpec(name="call_bend", dtype=pa.int64()),
        ),
    )


def scip_range_bundle(*, prefix: str = "", include_len: bool = False) -> FieldBundle:
    """Return a bundle for SCIP line/character range columns.

    Parameters
    ----------
    prefix:
        Optional column prefix (e.g., "enc_" for encoded ranges).
    include_len:
        When ``True``, include the range_len column for the range.

    Returns
    -------
    FieldBundle
        Bundle containing range fields.
    """
    normalized = f"{prefix}_" if prefix and not prefix.endswith("_") else prefix
    fields = [
        ArrowFieldSpec(name=f"{normalized}start_line", dtype=pa.int32()),
        ArrowFieldSpec(name=f"{normalized}start_char", dtype=pa.int32()),
        ArrowFieldSpec(name=f"{normalized}end_line", dtype=pa.int32()),
        ArrowFieldSpec(name=f"{normalized}end_char", dtype=pa.int32()),
    ]
    if include_len:
        fields.append(ArrowFieldSpec(name=f"{normalized}range_len", dtype=pa.int32()))
    name = f"{normalized}scip_range" if normalized else "scip_range"
    return FieldBundle(name=name, fields=tuple(fields))


def provenance_bundle() -> FieldBundle:
    """Return a bundle for dataset scan provenance columns.

    Returns
    -------
    FieldBundle
        Bundle containing provenance fields.
    """
    return FieldBundle(
        name="provenance",
        fields=(
            ArrowFieldSpec(name="prov_filename", dtype=pa.string()),
            ArrowFieldSpec(name="prov_fragment_index", dtype=pa.int32()),
            ArrowFieldSpec(name="prov_batch_index", dtype=pa.int32()),
            ArrowFieldSpec(name="prov_last_in_fragment", dtype=pa.bool_()),
        ),
    )

"""Shared bundle catalog helpers."""

from __future__ import annotations

from typing import TYPE_CHECKING

from schema_spec.evidence_metadata import evidence_metadata_bundle
from schema_spec.specs import file_identity_bundle, span_bundle

if TYPE_CHECKING:
    from schema_spec.specs import FieldBundle


def base_bundle_catalog(*, include_sha256: bool) -> dict[str, FieldBundle]:
    """Return a base bundle catalog with file identity and span bundles.

    Returns:
    -------
    dict[str, FieldBundle]
        Bundle catalog mapping.
    """
    return {
        "file_identity": file_identity_bundle(include_sha256=include_sha256),
        "span": span_bundle(),
    }


def extended_bundle_catalog(*, include_sha256: bool) -> dict[str, FieldBundle]:
    """Return an extended bundle catalog with evidence metadata included.

    The extended catalog includes file identity, span, and evidence metadata
    bundles for use in relationship and CPG pipelines.

    Parameters
    ----------
    include_sha256
        Include file_sha256 in file identity bundle.

    Returns:
    -------
    dict[str, FieldBundle]
        Extended bundle catalog mapping.
    """
    catalog = base_bundle_catalog(include_sha256=include_sha256)
    catalog["evidence_metadata"] = evidence_metadata_bundle()
    return catalog


__all__ = ["base_bundle_catalog", "extended_bundle_catalog"]

"""Shared bundle catalog helpers."""

from __future__ import annotations

from typing import TYPE_CHECKING

from schema_spec.specs import file_identity_bundle, span_bundle

if TYPE_CHECKING:
    from schema_spec.specs import FieldBundle


def base_bundle_catalog(*, include_sha256: bool) -> dict[str, FieldBundle]:
    """Return a base bundle catalog with file identity and span bundles.

    Returns
    -------
    dict[str, FieldBundle]
        Bundle catalog mapping.
    """
    return {
        "file_identity": file_identity_bundle(include_sha256=include_sha256),
        "span": span_bundle(),
    }


__all__ = ["base_bundle_catalog"]

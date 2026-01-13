"""Bundle catalog for programmatic CPG schema definitions."""

from __future__ import annotations

from schema_spec.specs import FieldBundle, file_identity_bundle, span_bundle

_BUNDLE_CATALOG: dict[str, FieldBundle] = {
    "file_identity": file_identity_bundle(include_sha256=False),
    "span": span_bundle(),
}


def bundle(name: str) -> FieldBundle:
    """Return the FieldBundle for a catalog name.

    Returns
    -------
    FieldBundle
        Bundle specification for the requested name.
    """
    return _BUNDLE_CATALOG[name]


__all__ = ["bundle"]

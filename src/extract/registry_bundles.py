"""Bundle catalog for extract dataset schemas."""

from __future__ import annotations

from collections.abc import Mapping

from schema_spec.specs import FieldBundle, call_span_bundle, file_identity_bundle, scip_range_bundle

_BUNDLE_CATALOG: Mapping[str, FieldBundle] = {
    "file_identity": file_identity_bundle(),
    "file_identity_no_sha": file_identity_bundle(include_sha256=False),
    "call_span": call_span_bundle(),
    "scip_range": scip_range_bundle(),
    "scip_range_len": scip_range_bundle(include_len=True),
    "scip_range_enc_len": scip_range_bundle(prefix="enc_", include_len=True),
}


def bundle(name: str) -> FieldBundle:
    """Return a bundle by name.

    Returns
    -------
    FieldBundle
        Bundle definition for the name.
    """
    return _BUNDLE_CATALOG[name]


__all__ = ["bundle"]

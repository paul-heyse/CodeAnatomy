"""Field bundle catalog for normalize dataset schemas."""

from __future__ import annotations

from collections.abc import Sequence

from datafusion_engine.arrow_schema.semantic_types import SPAN_STORAGE
from schema_spec.bundles import base_bundle_catalog
from schema_spec.specs import ArrowFieldSpec, FieldBundle

_BUNDLE_CATALOG: dict[str, FieldBundle] = base_bundle_catalog(include_sha256=False)
_BUNDLE_CATALOG["span"] = FieldBundle(
    name="span",
    fields=(ArrowFieldSpec(name="span", dtype=SPAN_STORAGE),),
)


def bundle(name: str) -> FieldBundle:
    """Return a field bundle by name.

    Returns
    -------
    FieldBundle
        Bundle specification.
    """
    return _BUNDLE_CATALOG[name]


def bundles(names: Sequence[str]) -> tuple[FieldBundle, ...]:
    """Return field bundles for the given bundle names.

    Returns
    -------
    tuple[FieldBundle, ...]
        Bundle specifications.
    """
    return tuple(bundle(name) for name in names)


def bundle_names() -> tuple[str, ...]:
    """Return the bundle catalog keys.

    Returns
    -------
    tuple[str, ...]
        Bundle catalog keys.
    """
    return tuple(_BUNDLE_CATALOG.keys())


__all__ = ["bundle", "bundle_names", "bundles"]

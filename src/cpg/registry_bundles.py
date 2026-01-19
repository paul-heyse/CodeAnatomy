"""Bundle catalog for programmatic CPG schema definitions."""

from __future__ import annotations

from typing import TYPE_CHECKING

from registry_common.bundles import base_bundle_catalog

if TYPE_CHECKING:
    from schema_spec.specs import FieldBundle

_BUNDLE_CATALOG: dict[str, FieldBundle] = base_bundle_catalog(include_sha256=False)


def bundle(name: str) -> FieldBundle:
    """Return the FieldBundle for a catalog name.

    Returns
    -------
    FieldBundle
        Bundle specification for the requested name.
    """
    return _BUNDLE_CATALOG[name]


__all__ = ["bundle"]

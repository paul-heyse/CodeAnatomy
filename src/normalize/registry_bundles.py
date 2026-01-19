"""Field bundle catalog for normalize dataset schemas."""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING

from registry_common.bundles import base_bundle_catalog

if TYPE_CHECKING:
    from schema_spec.specs import FieldBundle

_BUNDLE_CATALOG: dict[str, FieldBundle] = base_bundle_catalog(include_sha256=False)


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

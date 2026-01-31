"""Registration utilities for semantic view builders.

Provides helper functions for registering builders with the semantic catalog.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from semantics.builders.protocol import SemanticViewBuilder
    from semantics.catalog.catalog import SemanticCatalog


def register_builder(
    catalog: SemanticCatalog,
    builder: SemanticViewBuilder,
    *,
    overwrite: bool = False,
) -> None:
    """Register a single builder with the catalog.

    Parameters
    ----------
    catalog
        Semantic catalog to register with.
    builder
        Builder instance to register.
    overwrite
        Whether to overwrite existing registration.
    """
    catalog.register(builder, overwrite=overwrite)


def register_builders(
    catalog: SemanticCatalog,
    builders: Iterable[SemanticViewBuilder],
    *,
    overwrite: bool = False,
) -> int:
    """Register multiple builders with the catalog.

    Parameters
    ----------
    catalog
        Semantic catalog to register with.
    builders
        Iterable of builder instances to register.
    overwrite
        Whether to overwrite existing registrations.

    Returns
    -------
    int
        Number of builders registered.
    """
    count = 0
    for builder in builders:
        catalog.register(builder, overwrite=overwrite)
        count += 1
    return count


__all__ = [
    "register_builder",
    "register_builders",
]

"""Semantic view catalog for programmatic view registration.

This module provides a registry of semantic views with metadata including
evidence tier, upstream dependencies, and plan fingerprints. The catalog
enables dependency-ordered view construction and incremental processing.

Example
-------
>>> from semantics.catalog import SEMANTIC_CATALOG, CatalogEntry
>>> from semantics.builders import SemanticViewBuilder
>>>
>>> # Register a builder
>>> SEMANTIC_CATALOG.register(my_builder)
>>>
>>> # Get dependency order for building views
>>> build_order = SEMANTIC_CATALOG.topological_order()
>>> for name in build_order:
...     entry = SEMANTIC_CATALOG.get(name)
...     df = entry.builder.build(ctx)
...     ctx.register_view(name, df)
"""

from __future__ import annotations

from semantics.catalog.catalog import (
    SEMANTIC_CATALOG,
    CatalogEntry,
    SemanticCatalog,
)

__all__ = [
    "SEMANTIC_CATALOG",
    "CatalogEntry",
    "SemanticCatalog",
]

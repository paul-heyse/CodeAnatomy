"""Semantic view catalog.

Provides a registry of semantic views with metadata for dependency tracking,
evidence tier classification, and plan fingerprinting.
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from typing import TYPE_CHECKING, Final

from semantics.builders.protocol import SemanticViewBuilder

if TYPE_CHECKING:
    from semantics.plans.fingerprints import PlanFingerprint
    from semantics.stats.collector import ViewStats
    from semantics.types.annotated_schema import AnnotatedSchema


@dataclass
class CatalogEntry:
    """Entry in the semantic catalog.

    Attributes
    ----------
    name
        Unique view name.
    evidence_tier
        Evidence tier (1=highest confidence, 4=lowest).
    upstream_deps
        Names of upstream tables/views this entry depends on.
    builder
        The view builder instance.
    plan_fingerprint
        Optional fingerprint of the DataFusion plan for incremental processing.
    annotated_schema
        Optional annotated schema with semantic type information.
    view_stats
        Optional statistics for the view (row counts, column stats).
    """

    name: str
    evidence_tier: int
    upstream_deps: tuple[str, ...]
    builder: SemanticViewBuilder
    plan_fingerprint: PlanFingerprint | None = None
    annotated_schema: AnnotatedSchema | None = None
    view_stats: ViewStats | None = None


class SemanticCatalog:
    """Registry of semantic views.

    Manages registration and lookup of semantic view builders with support
    for dependency tracking and topological ordering. Does not inherit from
    MutableRegistry to allow richer validation and metadata handling.

    Attributes
    ----------
    _entries : dict[str, CatalogEntry]
        Internal storage of catalog entries.

    Example
    -------
    >>> catalog = SemanticCatalog()
    >>> catalog.register(my_builder)
    >>> entry = catalog.get("my_view")
    >>> entry.evidence_tier
    2
    """

    def __init__(self) -> None:
        """Initialize an empty catalog."""
        self._entries: dict[str, CatalogEntry] = {}

    def register(
        self,
        builder: SemanticViewBuilder,
        *,
        overwrite: bool = False,
        plan_fingerprint: PlanFingerprint | None = None,
        annotated_schema: AnnotatedSchema | None = None,
    ) -> CatalogEntry:
        """Register a semantic view builder.

        Parameters
        ----------
        builder
            View builder to register.
        overwrite
            Whether to overwrite existing registration.
        plan_fingerprint
            Optional plan fingerprint for incremental processing.
        annotated_schema
            Optional annotated schema with semantic types.

        Returns
        -------
        CatalogEntry
            The created catalog entry.

        Raises
        ------
        TypeError
            Raised when builder does not implement SemanticViewBuilder protocol.
        ValueError
            Raised when name is already registered and overwrite is False.
        """
        if not isinstance(builder, SemanticViewBuilder):
            msg = f"Builder must implement SemanticViewBuilder protocol, got {type(builder)}"
            raise TypeError(msg)

        name = builder.name
        if name in self._entries and not overwrite:
            msg = f"View '{name}' already registered. Use overwrite=True."
            raise ValueError(msg)

        entry = CatalogEntry(
            name=name,
            evidence_tier=builder.evidence_tier,
            upstream_deps=builder.upstream_deps,
            builder=builder,
            plan_fingerprint=plan_fingerprint,
            annotated_schema=annotated_schema,
        )
        self._entries[name] = entry
        return entry

    def get(self, name: str) -> CatalogEntry | None:
        """Get a catalog entry by name.

        Parameters
        ----------
        name
            View name to look up.

        Returns
        -------
        CatalogEntry | None
            Catalog entry or None if not found.
        """
        return self._entries.get(name)

    def __getitem__(self, name: str) -> CatalogEntry:
        """Get a catalog entry by name.

        Parameters
        ----------
        name
            View name to look up.

        Returns
        -------
        CatalogEntry
            Catalog entry.
        """
        return self._entries[name]

    def __contains__(self, name: str) -> bool:
        """Check if a view is registered.

        Parameters
        ----------
        name
            View name to check.

        Returns
        -------
        bool
            True if the view is registered.
        """
        return name in self._entries

    def __iter__(self) -> Iterator[str]:
        """Iterate over registered view names.

        Returns
        -------
        Iterator[str]
            Iterator over view names.
        """
        return iter(self._entries)

    def __len__(self) -> int:
        """Return number of registered views.

        Returns
        -------
        int
            Number of registered views.
        """
        return len(self._entries)

    def all_entries(self) -> tuple[CatalogEntry, ...]:
        """Get all catalog entries.

        Returns
        -------
        tuple[CatalogEntry, ...]
            All registered entries.
        """
        return tuple(self._entries.values())

    def entries_by_tier(self, tier: int) -> tuple[CatalogEntry, ...]:
        """Get entries for a specific evidence tier.

        Parameters
        ----------
        tier
            Evidence tier to filter by.

        Returns
        -------
        tuple[CatalogEntry, ...]
            Entries matching the tier.
        """
        return tuple(e for e in self._entries.values() if e.evidence_tier == tier)

    def update_fingerprint(self, name: str, fingerprint: PlanFingerprint) -> None:
        """Update the plan fingerprint for an entry.

        Parameters
        ----------
        name
            View name to update.
        fingerprint
            New plan fingerprint payload.
        """
        entry = self._entries[name]
        self._entries[name] = CatalogEntry(
            name=entry.name,
            evidence_tier=entry.evidence_tier,
            upstream_deps=entry.upstream_deps,
            builder=entry.builder,
            plan_fingerprint=fingerprint,
            annotated_schema=entry.annotated_schema,
            view_stats=entry.view_stats,
        )

    def update_schema(self, name: str, schema: AnnotatedSchema) -> None:
        """Update the annotated schema for an entry.

        Parameters
        ----------
        name
            View name to update.
        schema
            Annotated schema with semantic types.
        """
        entry = self._entries[name]
        self._entries[name] = CatalogEntry(
            name=entry.name,
            evidence_tier=entry.evidence_tier,
            upstream_deps=entry.upstream_deps,
            builder=entry.builder,
            plan_fingerprint=entry.plan_fingerprint,
            annotated_schema=schema,
            view_stats=entry.view_stats,
        )

    def update_stats(self, name: str, stats: ViewStats) -> None:
        """Update the view statistics for an entry.

        Parameters
        ----------
        name
            View name to update.
        stats
            View statistics with row counts and column stats.
        """
        entry = self._entries[name]
        self._entries[name] = CatalogEntry(
            name=entry.name,
            evidence_tier=entry.evidence_tier,
            upstream_deps=entry.upstream_deps,
            builder=entry.builder,
            plan_fingerprint=entry.plan_fingerprint,
            annotated_schema=entry.annotated_schema,
            view_stats=stats,
        )

    def topological_order(self) -> list[str]:
        """Return view names in dependency order.

        Performs a topological sort based on upstream dependencies.
        Views with no dependencies come first, followed by views that
        depend on them.

        Returns
        -------
        list[str]
            View names in dependency order.

        Notes
        -----
        External dependencies (tables not in the catalog) are ignored
        for ordering purposes. Circular dependencies are not detected
        and may cause infinite recursion.
        """
        visited: set[str] = set()
        order: list[str] = []

        def visit(name: str) -> None:
            if name in visited:
                return
            visited.add(name)
            entry = self._entries.get(name)
            if entry:
                for dep in entry.upstream_deps:
                    if dep in self._entries:
                        visit(dep)
                order.append(name)

        for name in self._entries:
            visit(name)

        return order

    def dependency_graph(self) -> dict[str, tuple[str, ...]]:
        """Return the dependency graph as an adjacency list.

        Returns
        -------
        dict[str, tuple[str, ...]]
            Mapping from view names to their upstream dependencies
            that are also in the catalog.
        """
        return {
            name: tuple(dep for dep in entry.upstream_deps if dep in self._entries)
            for name, entry in self._entries.items()
        }

    def clear(self) -> None:
        """Remove all entries from the catalog."""
        self._entries.clear()


# Module-level catalog instance
SEMANTIC_CATALOG: Final[SemanticCatalog] = SemanticCatalog()


__all__ = [
    "SEMANTIC_CATALOG",
    "CatalogEntry",
    "SemanticCatalog",
]

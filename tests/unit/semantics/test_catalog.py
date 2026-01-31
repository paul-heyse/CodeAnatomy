"""Tests for semantic catalog and builder protocol."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from semantics.builders import SemanticViewBuilder
from semantics.catalog import SemanticCatalog
from semantics.plans.fingerprints import PlanFingerprint

if TYPE_CHECKING:
    from datafusion import DataFrame, SessionContext


class MockViewBuilder:
    """Mock builder for testing."""

    def __init__(
        self,
        name: str,
        evidence_tier: int = 1,
        upstream_deps: tuple[str, ...] = (),
    ) -> None:
        self._name = name
        self._evidence_tier = evidence_tier
        self._upstream_deps = upstream_deps

    @property
    def name(self) -> str:
        """Return the builder name."""
        return self._name

    @property
    def evidence_tier(self) -> int:
        """Return the evidence tier."""
        return self._evidence_tier

    @property
    def upstream_deps(self) -> tuple[str, ...]:
        """Return upstream dependencies."""
        return self._upstream_deps

    def build(self, ctx: SessionContext) -> DataFrame:
        """Mock build hook for protocol compliance."""
        # Return a mock DataFrame - not called in these tests
        raise NotImplementedError


class TestSemanticViewBuilderProtocol:
    """Test SemanticViewBuilder protocol."""

    def test_mock_builder_is_protocol_compliant(self) -> None:
        """Verify mock builder implements the protocol."""
        builder = MockViewBuilder("test_view")
        assert isinstance(builder, SemanticViewBuilder)

    def test_protocol_requires_name(self) -> None:
        """Verify protocol checks name property."""

        class MissingName:
            @property
            def evidence_tier(self) -> int:
                return 1

            @property
            def upstream_deps(self) -> tuple[str, ...]:
                return ()

            def build(self, ctx: SessionContext) -> DataFrame:
                raise NotImplementedError

        assert not isinstance(MissingName(), SemanticViewBuilder)

    def test_protocol_requires_build_method(self) -> None:
        """Verify protocol checks build method."""

        class MissingBuild:
            @property
            def name(self) -> str:
                return "test"

            @property
            def evidence_tier(self) -> int:
                return 1

            @property
            def upstream_deps(self) -> tuple[str, ...]:
                return ()

        assert not isinstance(MissingBuild(), SemanticViewBuilder)


class TestSemanticCatalog:
    """Test SemanticCatalog functionality."""

    def test_register_builder(self) -> None:
        """Verify builder registration works."""
        catalog = SemanticCatalog()
        builder = MockViewBuilder("test_view", evidence_tier=2)
        entry = catalog.register(builder)

        assert entry.name == "test_view"
        assert entry.evidence_tier == 2
        assert entry.builder is builder

    def test_get_entry(self) -> None:
        """Verify entry retrieval works."""
        catalog = SemanticCatalog()
        builder = MockViewBuilder("test_view")
        catalog.register(builder)

        entry = catalog.get("test_view")
        assert entry is not None
        assert entry.name == "test_view"

    def test_get_missing_entry(self) -> None:
        """Verify get returns None for missing entries."""
        catalog = SemanticCatalog()
        assert catalog.get("nonexistent") is None

    def test_getitem(self) -> None:
        """Verify bracket notation works."""
        catalog = SemanticCatalog()
        builder = MockViewBuilder("test_view")
        catalog.register(builder)

        entry = catalog["test_view"]
        assert entry.name == "test_view"

    def test_getitem_raises_keyerror(self) -> None:
        """Verify bracket notation raises KeyError for missing entries."""
        catalog = SemanticCatalog()
        with pytest.raises(KeyError):
            _ = catalog["nonexistent"]

    def test_contains(self) -> None:
        """Verify contains check works."""
        catalog = SemanticCatalog()
        builder = MockViewBuilder("test_view")
        catalog.register(builder)

        assert "test_view" in catalog
        assert "nonexistent" not in catalog

    def test_len(self) -> None:
        """Verify len works."""
        catalog = SemanticCatalog()
        assert len(catalog) == 0

        catalog.register(MockViewBuilder("view1"))
        assert len(catalog) == 1

        catalog.register(MockViewBuilder("view2"))
        assert len(catalog) == 2

    def test_iter(self) -> None:
        """Verify iteration works."""
        catalog = SemanticCatalog()
        catalog.register(MockViewBuilder("view1"))
        catalog.register(MockViewBuilder("view2"))

        names = list(catalog)
        assert set(names) == {"view1", "view2"}

    def test_duplicate_registration_raises(self) -> None:
        """Verify duplicate registration raises ValueError."""
        catalog = SemanticCatalog()
        catalog.register(MockViewBuilder("test_view"))

        with pytest.raises(ValueError, match="already registered"):
            catalog.register(MockViewBuilder("test_view"))

    def test_overwrite_registration(self) -> None:
        """Verify overwrite allows re-registration."""
        catalog = SemanticCatalog()
        catalog.register(MockViewBuilder("test_view", evidence_tier=1))
        catalog.register(MockViewBuilder("test_view", evidence_tier=2), overwrite=True)

        entry = catalog.get("test_view")
        assert entry is not None
        assert entry.evidence_tier == 2

    def test_invalid_builder_raises_typeerror(self) -> None:
        """Verify non-protocol objects raise TypeError."""
        catalog = SemanticCatalog()

        class NotABuilder:
            pass

        with pytest.raises(TypeError, match="SemanticViewBuilder protocol"):
            catalog.register(NotABuilder())  # type: ignore[arg-type]


class TestCatalogTopologicalOrder:
    """Test topological ordering of catalog entries."""

    def test_empty_catalog(self) -> None:
        """Verify empty catalog returns empty order."""
        catalog = SemanticCatalog()
        assert catalog.topological_order() == []

    def test_no_dependencies(self) -> None:
        """Verify views without dependencies are returned."""
        catalog = SemanticCatalog()
        catalog.register(MockViewBuilder("view1"))
        catalog.register(MockViewBuilder("view2"))

        order = catalog.topological_order()
        assert set(order) == {"view1", "view2"}

    def test_linear_dependency_chain(self) -> None:
        """Verify linear dependencies are correctly ordered."""
        catalog = SemanticCatalog()
        catalog.register(MockViewBuilder("view1"))
        catalog.register(MockViewBuilder("view2", upstream_deps=("view1",)))
        catalog.register(MockViewBuilder("view3", upstream_deps=("view2",)))

        order = catalog.topological_order()
        assert order.index("view1") < order.index("view2")
        assert order.index("view2") < order.index("view3")

    def test_diamond_dependency(self) -> None:
        """Verify diamond dependencies are correctly ordered."""
        catalog = SemanticCatalog()
        catalog.register(MockViewBuilder("base"))
        catalog.register(MockViewBuilder("left", upstream_deps=("base",)))
        catalog.register(MockViewBuilder("right", upstream_deps=("base",)))
        catalog.register(MockViewBuilder("top", upstream_deps=("left", "right")))

        order = catalog.topological_order()
        assert order.index("base") < order.index("left")
        assert order.index("base") < order.index("right")
        assert order.index("left") < order.index("top")
        assert order.index("right") < order.index("top")

    def test_external_dependencies_ignored(self) -> None:
        """Verify external dependencies don't affect ordering."""
        catalog = SemanticCatalog()
        catalog.register(MockViewBuilder("view1", upstream_deps=("external_table",)))
        catalog.register(MockViewBuilder("view2", upstream_deps=("view1",)))

        order = catalog.topological_order()
        assert order.index("view1") < order.index("view2")


class TestCatalogEntries:
    """Test catalog entry utilities."""

    def test_all_entries(self) -> None:
        """Verify all_entries returns all entries."""
        catalog = SemanticCatalog()
        catalog.register(MockViewBuilder("view1", evidence_tier=1))
        catalog.register(MockViewBuilder("view2", evidence_tier=2))

        entries = catalog.all_entries()
        assert len(entries) == 2
        names = {e.name for e in entries}
        assert names == {"view1", "view2"}

    def test_entries_by_tier(self) -> None:
        """Verify entries_by_tier filters correctly."""
        catalog = SemanticCatalog()
        catalog.register(MockViewBuilder("tier1_a", evidence_tier=1))
        catalog.register(MockViewBuilder("tier1_b", evidence_tier=1))
        catalog.register(MockViewBuilder("tier2_a", evidence_tier=2))

        tier1 = catalog.entries_by_tier(1)
        assert len(tier1) == 2
        names = {e.name for e in tier1}
        assert names == {"tier1_a", "tier1_b"}

        tier2 = catalog.entries_by_tier(2)
        assert len(tier2) == 1
        assert tier2[0].name == "tier2_a"

    def test_update_fingerprint(self) -> None:
        """Verify fingerprint update works."""
        catalog = SemanticCatalog()
        catalog.register(MockViewBuilder("view1"))

        fingerprint = PlanFingerprint(view_name="view1", logical_plan_hash="abc123")
        catalog.update_fingerprint("view1", fingerprint)
        entry = catalog.get("view1")
        assert entry is not None
        assert entry.plan_fingerprint == fingerprint

    def test_dependency_graph(self) -> None:
        """Verify dependency graph is correctly built."""
        catalog = SemanticCatalog()
        catalog.register(MockViewBuilder("base"))
        catalog.register(MockViewBuilder("derived", upstream_deps=("base", "external")))

        graph = catalog.dependency_graph()
        assert graph["base"] == ()
        assert graph["derived"] == ("base",)  # external filtered out

    def test_clear(self) -> None:
        """Verify clear removes all entries."""
        catalog = SemanticCatalog()
        catalog.register(MockViewBuilder("view1"))
        catalog.register(MockViewBuilder("view2"))

        catalog.clear()
        assert len(catalog) == 0
        assert catalog.get("view1") is None

"""Tests for decorator-based queries.

Verifies:
1. Decorator entity query parsing
2. Decorator filter construction
3. Decorator extraction from source
"""

from __future__ import annotations

from typing import cast

from tools.cq.query.ir import DecoratorFilter, Query
from tools.cq.query.parser import parse_query


class TestDecoratorFilter:
    """Tests for DecoratorFilter dataclass."""

    def test_basic_decorator_filter(self) -> None:
        """Create basic decorator filter."""
        filt = DecoratorFilter(decorated_by="property")
        assert filt.decorated_by == "property"
        assert filt.decorator_count_min is None
        assert filt.decorator_count_max is None

    def test_decorator_filter_count_range(self) -> None:
        """Create decorator filter with count constraints."""
        filt = DecoratorFilter(
            decorator_count_min=1,
            decorator_count_max=3,
        )
        assert filt.decorator_count_min == 1
        assert filt.decorator_count_max == 3


class TestDecoratorQueryParsing:
    """Tests for parsing decorator queries."""

    def test_decorator_entity(self) -> None:
        """Parse decorator entity query."""
        query = parse_query("entity=decorator")
        assert query.entity == "decorator"

    def test_decorated_by_filter(self) -> None:
        """Parse query with decorated_by filter."""
        query = parse_query("entity=function decorated_by=property")
        assert query.decorator_filter is not None
        assert query.decorator_filter.decorated_by == "property"

    def test_decorator_count_filter(self) -> None:
        """Parse query with decorator count filter."""
        query = parse_query("entity=function decorator_count_min=2")
        assert query.decorator_filter is not None
        assert query.decorator_filter.decorator_count_min == 2

    def test_decorator_filter_with_scope(self) -> None:
        """Parse decorator filter with scope constraint."""
        query = parse_query("entity=function decorated_by=staticmethod in=src/")
        assert query.decorator_filter is not None
        assert query.scope.in_dir == "src/"


class TestDecoratorQueryIR:
    """Tests for decorator query IR construction."""

    def test_query_with_decorator_filter(self) -> None:
        """Create query with decorator filter."""
        query = Query(
            entity="function",
            decorator_filter=DecoratorFilter(decorated_by="dataclass"),
        )
        assert query.decorator_filter is not None
        assert query.decorator_filter.decorated_by == "dataclass"


class TestDecoratorExtraction:
    """Tests for decorator extraction from source."""

    def test_extract_simple_decorator(self) -> None:
        """Extract simple decorator from function."""
        from tools.cq.query.enrichment import extract_decorators_from_function

        source = """
@property
def foo(self):
    return self._foo
"""
        decorators = extract_decorators_from_function(source, 3)
        assert "property" in decorators

    def test_extract_dotted_decorator(self) -> None:
        """Extract dotted decorator from function."""
        from tools.cq.query.enrichment import extract_decorators_from_function

        source = """
@functools.cache
def expensive(x):
    return x * x
"""
        decorators = extract_decorators_from_function(source, 3)
        assert "functools.cache" in decorators

    def test_extract_decorator_with_args(self) -> None:
        """Extract decorator with arguments from function."""
        from tools.cq.query.enrichment import extract_decorators_from_function

        source = """
@dataclass(frozen=True)
class Point:
    x: int
    y: int
"""
        decorators = extract_decorators_from_function(source, 3)
        assert "dataclass" in decorators

    def test_extract_multiple_decorators(self) -> None:
        """Extract multiple decorators from function."""
        from tools.cq.query.enrichment import extract_decorators_from_function

        source = """
@staticmethod
@deprecated
def helper():
    pass
"""
        decorators = extract_decorators_from_function(source, 4)
        assert len(decorators) == 2
        assert "staticmethod" in decorators
        assert "deprecated" in decorators

    def test_extract_no_decorators(self) -> None:
        """Extract decorators from undecorated function."""
        from tools.cq.query.enrichment import extract_decorators_from_function

        source = """
def plain_function():
    pass
"""
        decorators = extract_decorators_from_function(source, 2)
        assert len(decorators) == 0

    def test_extract_class_decorators(self) -> None:
        """Extract decorators from class definition."""
        from tools.cq.query.enrichment import extract_decorators_from_function

        source = """
@dataclass
class Point:
    x: int
"""
        decorators = extract_decorators_from_function(source, 3)
        assert "dataclass" in decorators


class TestDecoratorEnrichment:
    """Tests for decorator enrichment of findings."""

    def test_enrich_with_decorators(self) -> None:
        """Enrich finding with decorator information."""
        from tools.cq.core.schema import Anchor, Finding
        from tools.cq.query.enrichment import enrich_with_decorators

        source = """
@property
def foo(self):
    return self._foo
"""
        finding = Finding(
            category="definition",
            message="function: foo",
            anchor=Anchor(file="test.py", line=3),
        )

        enrichment = enrich_with_decorators(finding, source)
        assert "decorators" in enrichment
        decorators = cast("list[str]", enrichment["decorators"])
        assert "property" in decorators
        assert enrichment["decorator_count"] == 1

    def test_enrich_undecorated(self) -> None:
        """Enrich undecorated finding returns empty."""
        from tools.cq.core.schema import Anchor, Finding
        from tools.cq.query.enrichment import enrich_with_decorators

        source = """
def plain():
    pass
"""
        finding = Finding(
            category="definition",
            message="function: plain",
            anchor=Anchor(file="test.py", line=2),
        )

        enrichment = enrich_with_decorators(finding, source)
        assert enrichment == {}

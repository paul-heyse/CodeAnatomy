"""Tests for decorator-based queries.

Verifies:
1. Decorator entity query parsing
2. Decorator filter construction
3. Decorator extraction from source
"""

from __future__ import annotations

from typing import cast

from tools.cq.query.ir import DecoratorFilter, Query

MAX_DECORATOR_COUNT = 3
MIN_DECORATOR_COUNT = 2
from tools.cq.query.parser import parse_query


class TestDecoratorFilter:
    """Tests for DecoratorFilter dataclass."""

    @staticmethod
    def test_basic_decorator_filter() -> None:
        """Create basic decorator filter."""
        filt = DecoratorFilter(decorated_by="property")
        assert filt.decorated_by == "property"
        assert filt.decorator_count_min is None
        assert filt.decorator_count_max is None

    @staticmethod
    def test_decorator_filter_count_range() -> None:
        """Create decorator filter with count constraints."""
        filt = DecoratorFilter(
            decorator_count_min=1,
            decorator_count_max=MAX_DECORATOR_COUNT,
        )
        assert filt.decorator_count_min == 1
        assert filt.decorator_count_max == MAX_DECORATOR_COUNT


class TestDecoratorQueryParsing:
    """Tests for parsing decorator queries."""

    @staticmethod
    def test_decorator_entity() -> None:
        """Parse decorator entity query."""
        query = parse_query("entity=decorator")
        assert query.entity == "decorator"

    @staticmethod
    def test_decorated_by_filter() -> None:
        """Parse query with decorated_by filter."""
        query = parse_query("entity=function decorated_by=property")
        assert query.decorator_filter is not None
        assert query.decorator_filter.decorated_by == "property"

    @staticmethod
    def test_decorator_count_filter() -> None:
        """Parse query with decorator count filter."""
        query = parse_query(f"entity=function decorator_count_min={MIN_DECORATOR_COUNT}")
        assert query.decorator_filter is not None
        assert query.decorator_filter.decorator_count_min == MIN_DECORATOR_COUNT

    @staticmethod
    def test_decorator_filter_with_scope() -> None:
        """Parse decorator filter with scope constraint."""
        query = parse_query("entity=function decorated_by=staticmethod in=src/")
        assert query.decorator_filter is not None
        assert query.scope.in_dir == "src/"


class TestDecoratorQueryIR:
    """Tests for decorator query IR construction."""

    @staticmethod
    def test_query_with_decorator_filter() -> None:
        """Create query with decorator filter."""
        query = Query(
            entity="function",
            decorator_filter=DecoratorFilter(decorated_by="dataclass"),
        )
        assert query.decorator_filter is not None
        assert query.decorator_filter.decorated_by == "dataclass"


class TestDecoratorExtraction:
    """Tests for decorator extraction from source."""

    @staticmethod
    def test_extract_simple_decorator() -> None:
        """Extract simple decorator from function."""
        from tools.cq.query.enrichment import extract_decorators_from_function

        source = """
@property
def foo(self):
    return self._foo
"""
        decorators = extract_decorators_from_function(source, 3)
        assert "property" in decorators

    @staticmethod
    def test_extract_dotted_decorator() -> None:
        """Extract dotted decorator from function."""
        from tools.cq.query.enrichment import extract_decorators_from_function

        source = """
@functools.cache
def expensive(x):
    return x * x
"""
        decorators = extract_decorators_from_function(source, 3)
        assert "functools.cache" in decorators

    @staticmethod
    def test_extract_decorator_with_args() -> None:
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

    @staticmethod
    def test_extract_multiple_decorators() -> None:
        """Extract multiple decorators from function."""
        from tools.cq.query.enrichment import extract_decorators_from_function

        source = """
@staticmethod
@deprecated
def helper():
    pass
"""
        decorators = extract_decorators_from_function(source, 4)
        assert len(decorators) == MIN_DECORATOR_COUNT
        assert "staticmethod" in decorators
        assert "deprecated" in decorators

    @staticmethod
    def test_extract_no_decorators() -> None:
        """Extract decorators from undecorated function."""
        from tools.cq.query.enrichment import extract_decorators_from_function

        source = """
def plain_function():
    pass
"""
        decorators = extract_decorators_from_function(source, 2)
        assert len(decorators) == 0

    @staticmethod
    def test_extract_class_decorators() -> None:
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

    @staticmethod
    def test_enrich_with_decorators() -> None:
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

    @staticmethod
    def test_enrich_undecorated() -> None:
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

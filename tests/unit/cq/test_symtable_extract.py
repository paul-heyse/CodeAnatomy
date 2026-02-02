"""Tests for symtable extraction module.

Verifies:
1. Scope extraction from Python source
2. Symbol classification (free, cell, local)
3. Closure detection
"""

from __future__ import annotations

from tools.cq.introspection.symtable_extract import (
    ScopeFact,
    ScopeType,
    SymbolFact,
    extract_scope_graph,
    get_cell_vars,
    get_free_vars,
    is_closure,
)


class TestExtractScopeGraph:
    """Tests for extract_scope_graph function."""

    def test_simple_function(self) -> None:
        """Extract scope from simple function."""
        source = """
def foo():
    x = 1
    return x
"""
        graph = extract_scope_graph(source, "test.py")
        assert len(graph.scopes) >= 2  # module + foo

        # Find the foo scope
        foo_scope = graph.scope_by_name.get("foo")
        assert foo_scope is not None
        assert foo_scope.scope_type == ScopeType.FUNCTION

    def test_nested_function_closure(self) -> None:
        """Extract scope from nested function with closure."""
        source = """
def outer():
    x = 1
    def inner():
        return x
    return inner
"""
        graph = extract_scope_graph(source, "test.py")

        inner_scope = graph.scope_by_name.get("inner")
        assert inner_scope is not None
        assert inner_scope.is_nested
        assert inner_scope.has_free_vars
        assert "x" in inner_scope.free_vars

    def test_class_scope(self) -> None:
        """Extract scope from class definition."""
        source = """
class Foo:
    x = 1
    def method(self):
        return self.x
"""
        graph = extract_scope_graph(source, "test.py")

        foo_scope = graph.scope_by_name.get("Foo")
        assert foo_scope is not None
        assert foo_scope.scope_type == ScopeType.CLASS

    def test_multiple_functions(self) -> None:
        """Extract scopes from multiple functions."""
        source = """
def foo():
    return 1

def bar():
    return 2
"""
        graph = extract_scope_graph(source, "test.py")

        assert "foo" in graph.scope_by_name
        assert "bar" in graph.scope_by_name

    def test_syntax_error_returns_empty(self) -> None:
        """Syntax error returns empty graph."""
        source = "def foo( invalid syntax"
        graph = extract_scope_graph(source, "test.py")
        assert len(graph.scopes) == 0

    def test_root_scope_is_module(self) -> None:
        """Root scope is the module."""
        source = """
x = 1
def foo():
    pass
"""
        graph = extract_scope_graph(source, "test.py")

        assert graph.root_scope is not None
        assert graph.root_scope.scope_type == ScopeType.MODULE


class TestScopeFact:
    """Tests for ScopeFact dataclass."""

    def test_scope_fact_creation(self) -> None:
        """Create ScopeFact instance."""
        scope = ScopeFact(
            name="foo",
            scope_type=ScopeType.FUNCTION,
            lineno=5,
        )
        assert scope.name == "foo"
        assert scope.scope_type == ScopeType.FUNCTION
        assert scope.lineno == 5

    def test_scope_fact_defaults(self) -> None:
        """ScopeFact has correct defaults."""
        scope = ScopeFact(
            name="foo",
            scope_type=ScopeType.FUNCTION,
        )
        assert scope.symbols == ()
        assert scope.children == ()
        assert scope.is_nested is False
        assert scope.has_free_vars is False
        assert scope.has_cell_vars is False


class TestSymbolFact:
    """Tests for SymbolFact dataclass."""

    def test_symbol_fact_creation(self) -> None:
        """Create SymbolFact instance."""
        symbol = SymbolFact(
            name="x",
            is_local=True,
            is_assigned=True,
        )
        assert symbol.name == "x"
        assert symbol.is_local
        assert symbol.is_assigned

    def test_symbol_fact_defaults(self) -> None:
        """SymbolFact has correct defaults."""
        symbol = SymbolFact(name="x")
        assert symbol.is_local is False
        assert symbol.is_global is False
        assert symbol.is_free is False
        assert symbol.is_cell is False


class TestClosureHelpers:
    """Tests for closure helper functions."""

    def test_is_closure_true(self) -> None:
        """is_closure returns True for closure scope."""
        scope = ScopeFact(
            name="inner",
            scope_type=ScopeType.FUNCTION,
            has_free_vars=True,
            free_vars=("x",),
        )
        assert is_closure(scope)

    def test_is_closure_false(self) -> None:
        """is_closure returns False for non-closure scope."""
        scope = ScopeFact(
            name="foo",
            scope_type=ScopeType.FUNCTION,
            has_free_vars=False,
        )
        assert not is_closure(scope)

    def test_get_free_vars(self) -> None:
        """get_free_vars returns free variables."""
        scope = ScopeFact(
            name="inner",
            scope_type=ScopeType.FUNCTION,
            has_free_vars=True,
            free_vars=("x", "y"),
        )
        assert get_free_vars(scope) == ("x", "y")

    def test_get_cell_vars(self) -> None:
        """get_cell_vars returns cell variables."""
        scope = ScopeFact(
            name="outer",
            scope_type=ScopeType.FUNCTION,
            has_cell_vars=True,
            cell_vars=("x",),
        )
        assert get_cell_vars(scope) == ("x",)


class TestScopeType:
    """Tests for ScopeType enum."""

    def test_scope_types(self) -> None:
        """All expected scope types exist."""
        assert ScopeType.MODULE.value == "module"
        assert ScopeType.FUNCTION.value == "function"
        assert ScopeType.CLASS.value == "class"
        assert ScopeType.ANNOTATION.value == "annotation"


class TestIntegration:
    """Integration tests for scope extraction."""

    def test_closure_chain(self) -> None:
        """Extract closure chain with multiple levels."""
        source = """
def level1():
    a = 1
    def level2():
        b = a
        def level3():
            return a + b
        return level3
    return level2
"""
        graph = extract_scope_graph(source, "test.py")

        level2 = graph.scope_by_name.get("level2")
        level3 = graph.scope_by_name.get("level3")

        assert level2 is not None
        assert level3 is not None

        # level2 captures 'a' from level1
        assert "a" in level2.free_vars

        # level3 captures 'a' and 'b'
        assert "a" in level3.free_vars
        assert "b" in level3.free_vars

    def test_generator_function(self) -> None:
        """Extract scope from generator function."""
        source = """
def gen():
    for i in range(10):
        yield i
"""
        graph = extract_scope_graph(source, "test.py")

        gen_scope = graph.scope_by_name.get("gen")
        assert gen_scope is not None
        assert gen_scope.scope_type == ScopeType.FUNCTION

    def test_async_function(self) -> None:
        """Extract scope from async function."""
        source = """
async def async_foo():
    x = 1
    return x
"""
        graph = extract_scope_graph(source, "test.py")

        # async functions have FUNCTION scope type
        foo_scope = graph.scope_by_name.get("async_foo")
        assert foo_scope is not None
        assert foo_scope.scope_type == ScopeType.FUNCTION

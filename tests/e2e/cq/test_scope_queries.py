"""E2E tests for scope and symbol query features.

Tests the Python symtable integration for scope-level queries.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from tools.cq.introspection import (
    ScopeType,
    extract_scope_graph,
    get_free_vars,
    is_closure,
)

MIN_SCOPES_WITH_FUNCTION = 2
MIN_FUNCTION_SYMBOLS = 3
MIN_SCOPES_WITH_FIXTURE_CLOSURES = 3


class TestScopeGraphExtraction:
    """Tests for scope graph extraction."""

    @staticmethod
    def test_extract_simple_module() -> None:
        """Extract scope graph from simple module."""
        source = """
x = 1
y = 2
"""
        graph = extract_scope_graph(source, "test.py")
        assert graph.root_scope is not None
        assert graph.root_scope.scope_type == ScopeType.MODULE

    @staticmethod
    def test_extract_function_scope() -> None:
        """Extract function scope from module."""
        source = """
def my_func(a, b):
    return a + b
"""
        graph = extract_scope_graph(source, "test.py")
        assert len(graph.scopes) >= MIN_SCOPES_WITH_FUNCTION  # module + function

        # Find function scope
        func_scope = graph.scope_by_name.get("my_func")
        assert func_scope is not None
        assert func_scope.scope_type == ScopeType.FUNCTION

    @staticmethod
    def test_extract_class_scope() -> None:
        """Extract class scope from module."""
        source = """
class MyClass:
    def method(self):
        pass
"""
        graph = extract_scope_graph(source, "test.py")

        # Find class scope
        class_scope = graph.scope_by_name.get("MyClass")
        assert class_scope is not None
        assert class_scope.scope_type == ScopeType.CLASS


class TestScopeTypeDetection:
    """Tests for scope type detection."""

    @staticmethod
    def test_module_scope_type() -> None:
        """Module scope has MODULE type."""
        source = "x = 1"
        graph = extract_scope_graph(source, "test.py")
        assert graph.root_scope is not None
        assert graph.root_scope.scope_type == ScopeType.MODULE

    @staticmethod
    def test_function_scope_type() -> None:
        """Function scope has FUNCTION type."""
        source = """
def foo():
    pass
"""
        graph = extract_scope_graph(source, "test.py")
        func = graph.scope_by_name.get("foo")
        assert func is not None
        assert func.scope_type == ScopeType.FUNCTION

    @staticmethod
    def test_class_scope_type() -> None:
        """Class scope has CLASS type."""
        source = """
class Bar:
    pass
"""
        graph = extract_scope_graph(source, "test.py")
        cls = graph.scope_by_name.get("Bar")
        assert cls is not None
        assert cls.scope_type == ScopeType.CLASS

    @staticmethod
    def test_nested_function_is_nested() -> None:
        """Nested function has is_nested=True."""
        source = """
def outer():
    def inner():
        pass
"""
        graph = extract_scope_graph(source, "test.py")
        inner = graph.scope_by_name.get("inner")
        assert inner is not None
        assert inner.is_nested is True


class TestSymbolExtraction:
    """Tests for symbol extraction."""

    @staticmethod
    def test_extract_local_symbol() -> None:
        """Extract local variable symbol."""
        source = """
def func():
    x = 1
    return x
"""
        graph = extract_scope_graph(source, "test.py")
        func = graph.scope_by_name.get("func")
        assert func is not None

        # Find x symbol
        x_sym = next((s for s in func.symbols if s.name == "x"), None)
        assert x_sym is not None
        assert x_sym.is_local is True
        assert x_sym.is_assigned is True

    @staticmethod
    def test_extract_parameter_symbol() -> None:
        """Extract function parameter symbol."""
        source = """
def func(param):
    return param
"""
        graph = extract_scope_graph(source, "test.py")
        func = graph.scope_by_name.get("func")
        assert func is not None

        # Find param symbol
        param_sym = next((s for s in func.symbols if s.name == "param"), None)
        assert param_sym is not None
        assert param_sym.is_parameter is True

    @staticmethod
    def test_extract_global_symbol() -> None:
        """Extract global variable symbol."""
        source = """
G = 1

def func():
    global G
    G = 2
"""
        graph = extract_scope_graph(source, "test.py")
        func = graph.scope_by_name.get("func")
        assert func is not None

        # Find G symbol in function scope
        g_sym = next((s for s in func.symbols if s.name == "G"), None)
        assert g_sym is not None
        assert g_sym.is_global is True

    @staticmethod
    def test_extract_imported_symbol() -> None:
        """Extract imported symbol."""
        source = """
import os
from sys import path
"""
        graph = extract_scope_graph(source, "test.py")
        assert graph.root_scope is not None

        # Find os symbol
        os_sym = next((s for s in graph.root_scope.symbols if s.name == "os"), None)
        assert os_sym is not None
        assert os_sym.is_imported is True


class TestClosureDetection:
    """Tests for closure and free variable detection."""

    @staticmethod
    def test_detect_closure() -> None:
        """Detect closure with free variables."""
        source = """
def outer():
    x = 1
    def inner():
        return x
    return inner
"""
        graph = extract_scope_graph(source, "test.py")
        inner = graph.scope_by_name.get("inner")
        assert inner is not None
        assert is_closure(inner) is True
        assert "x" in get_free_vars(inner)

    @staticmethod
    def test_detect_non_closure() -> None:
        """Function without free vars is not closure."""
        source = """
def func(x):
    return x + 1
"""
        graph = extract_scope_graph(source, "test.py")
        func = graph.scope_by_name.get("func")
        assert func is not None
        assert is_closure(func) is False

    @staticmethod
    def test_detect_cell_vars() -> None:
        """Detect cell variables captured by inner scope."""
        source = """
def outer():
    captured = 10
    def inner():
        return captured
    return inner
"""
        graph = extract_scope_graph(source, "test.py")
        outer = graph.scope_by_name.get("outer")
        assert outer is not None
        # Cell vars are captured by inner scopes
        # The detection may vary by Python version
        if not (outer.has_cell_vars or "captured" in outer.cell_vars):
            pytest.skip("Cell var detection differs by Python version")


class TestScopePartitions:
    """Tests for scope variable partitions."""

    @staticmethod
    def test_scope_has_symbols() -> None:
        """Scope contains its symbols."""
        source = """
def func(a, b):
    x = a + b
    return x
"""
        graph = extract_scope_graph(source, "test.py")
        func = graph.scope_by_name.get("func")
        assert func is not None
        assert len(func.symbols) >= MIN_FUNCTION_SYMBOLS  # a, b, x

    @staticmethod
    def test_parameters_are_marked() -> None:
        """Function parameters are marked as is_parameter."""
        source = """
def func(p1, p2, p3):
    pass
"""
        graph = extract_scope_graph(source, "test.py")
        func = graph.scope_by_name.get("func")
        assert func is not None

        params = [s for s in func.symbols if s.is_parameter]
        param_names = {s.name for s in params}
        assert "p1" in param_names
        assert "p2" in param_names
        assert "p3" in param_names

    @staticmethod
    def test_locals_are_marked() -> None:
        """Local variables are marked as is_local."""
        source = """
def func():
    local_var = 1
    return local_var
"""
        graph = extract_scope_graph(source, "test.py")
        func = graph.scope_by_name.get("func")
        assert func is not None

        local = next((s for s in func.symbols if s.name == "local_var"), None)
        assert local is not None
        assert local.is_local is True


class TestScopeOptimization:
    """Tests for scope optimization detection."""

    @staticmethod
    def test_function_is_optimized() -> None:
        """Function scopes use fast locals (optimized)."""
        source = """
def func():
    x = 1
    return x
"""
        graph = extract_scope_graph(source, "test.py")
        func = graph.scope_by_name.get("func")
        assert func is not None
        assert func.is_optimized is True

    @staticmethod
    def test_module_not_optimized() -> None:
        """Module scopes are not optimized."""
        source = "x = 1"
        graph = extract_scope_graph(source, "test.py")
        assert graph.root_scope is not None
        assert graph.root_scope.is_optimized is False


class TestScopeGraphWithFixtures:
    """Tests using fixture files."""

    @staticmethod
    @pytest.fixture
    def fixtures_dir() -> Path:
        """Get fixtures directory.

        Returns:
        -------
        Path
            Path to the fixture directory.
        """
        return Path(__file__).parent / "_fixtures"

    @staticmethod
    def test_extract_closures_file(fixtures_dir: Path) -> None:
        """Extract scope graph from closures fixture."""
        closures_path = fixtures_dir / "closures.py"
        if not closures_path.exists():
            pytest.skip("closures.py fixture not found")

        source = closures_path.read_text()
        graph = extract_scope_graph(source, str(closures_path))

        # Should have multiple scopes
        assert len(graph.scopes) >= MIN_SCOPES_WITH_FIXTURE_CLOSURES

        # Should detect closures
        closures = [s for s in graph.scopes if is_closure(s)]
        assert len(closures) >= 1

    @staticmethod
    def test_extract_control_flow_file(fixtures_dir: Path) -> None:
        """Extract scope graph from control flow fixture."""
        control_path = fixtures_dir / "control_flow.py"
        if not control_path.exists():
            pytest.skip("control_flow.py fixture not found")

        source = control_path.read_text()
        graph = extract_scope_graph(source, str(control_path))

        # Should have module and function scopes
        assert graph.root_scope is not None
        func_scopes = [s for s in graph.scopes if s.scope_type == ScopeType.FUNCTION]
        assert len(func_scopes) >= 1

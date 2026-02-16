"""Tests for the builder dispatch factory in semantics.pipeline_build.

Verify that ``_dispatch_from_registry`` creates handlers that resolve
builders from name-keyed registries, and that the master dispatcher
``_builder_for_semantic_spec`` routes all consolidated kind classes.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING
from unittest.mock import MagicMock

import pytest

from semantics.pipeline_build import (
    _CONSOLIDATED_BUILDER_HANDLERS,
    _dispatch_from_registry,
)
from semantics.registry import SemanticSpecIndex
from semantics.view_kinds import ViewKindStr

CONSOLIDATED_HANDLER_COUNT = 6

if TYPE_CHECKING:
    from datafusion import DataFrame, SessionContext

    from datafusion_engine.plan.bundle_artifact import DataFrameBuilder
    from semantics.pipeline_build import _SemanticSpecContext


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_spec(name: str, kind: ViewKindStr = "normalize") -> SemanticSpecIndex:
    """Build a minimal ``SemanticSpecIndex`` for testing.

    Parameters
    ----------
    name
        Spec output name.
    kind
        Spec kind string (must match a ``SpecKind`` literal).

    Returns:
    -------
    SemanticSpecIndex
        A frozen spec index instance.
    """
    return SemanticSpecIndex(
        name=name,
        kind=kind,
        inputs=(),
        outputs=(name,),
    )


def _stub_builder(_ctx: SessionContext) -> DataFrame:
    """Trivial builder stub for testing dispatch.

    Returns:
    -------
    DataFrame
        A mock DataFrame.
    """
    return MagicMock()


def _stub_context() -> _SemanticSpecContext:
    """Build a minimal mock ``_SemanticSpecContext``.

    Returns:
    -------
    _SemanticSpecContext
        A mock object with the expected attribute interface.
    """
    ctx = MagicMock()
    ctx.runtime_profile = MagicMock()
    ctx.manifest = MagicMock()
    return ctx


# ---------------------------------------------------------------------------
# _dispatch_from_registry unit tests
# ---------------------------------------------------------------------------


class TestDispatchFromRegistry:
    """Test the ``_dispatch_from_registry`` factory."""

    @staticmethod
    def test_returns_builder_for_known_name() -> None:
        """Resolve a builder for a name present in the registry."""
        registry: dict[str, DataFrameBuilder] = {"alpha": _stub_builder}

        def factory(_ctx: _SemanticSpecContext) -> Mapping[str, DataFrameBuilder]:
            return registry

        handler = _dispatch_from_registry(factory, "test")
        spec = _make_spec("alpha", "normalize")
        ctx = _stub_context()

        result = handler(spec, ctx)
        assert result is _stub_builder

    @staticmethod
    def test_raises_key_error_for_unknown_name() -> None:
        """Raise ``KeyError`` when the spec name is not in the registry."""
        registry: dict[str, DataFrameBuilder] = {"alpha": _stub_builder}

        def factory(_ctx: _SemanticSpecContext) -> Mapping[str, DataFrameBuilder]:
            return registry

        handler = _dispatch_from_registry(factory, "test")
        spec = _make_spec("missing", "normalize")
        ctx = _stub_context()

        with pytest.raises(KeyError, match="Missing test builder for output 'missing'"):
            handler(spec, ctx)

    @staticmethod
    def test_error_message_includes_context_label() -> None:
        """Verify the error message uses the provided context label."""

        def factory(_ctx: _SemanticSpecContext) -> Mapping[str, DataFrameBuilder]:
            return {}

        handler = _dispatch_from_registry(factory, "span-unnest")
        spec = _make_spec("bad_name", "span_unnest")
        ctx = _stub_context()

        with pytest.raises(KeyError, match="Missing span-unnest builder"):
            handler(spec, ctx)

    @staticmethod
    def test_finalize_wraps_builder() -> None:
        """When finalize=True, wrap the resolved builder."""
        registry: dict[str, DataFrameBuilder] = {"alpha": _stub_builder}

        def factory(_ctx: _SemanticSpecContext) -> Mapping[str, DataFrameBuilder]:
            return registry

        handler = _dispatch_from_registry(factory, "test", finalize=True)
        spec = _make_spec("alpha", "normalize")
        ctx = _stub_context()

        result = handler(spec, ctx)
        # The finalize wrapper is a closure, not the original builder.
        assert result is not _stub_builder
        # It should be callable (a DataFrameBuilder).
        assert callable(result)

    @staticmethod
    def test_finalize_false_returns_raw_builder() -> None:
        """When finalize=False (default), return the builder as-is."""
        registry: dict[str, DataFrameBuilder] = {"alpha": _stub_builder}

        def factory(_ctx: _SemanticSpecContext) -> Mapping[str, DataFrameBuilder]:
            return registry

        handler = _dispatch_from_registry(factory, "test", finalize=False)
        spec = _make_spec("alpha", "normalize")
        ctx = _stub_context()

        result = handler(spec, ctx)
        assert result is _stub_builder

    @staticmethod
    def test_factory_receives_context() -> None:
        """Verify the registry factory is called with the spec context."""
        received_contexts: list[object] = []

        def factory(ctx: _SemanticSpecContext) -> Mapping[str, DataFrameBuilder]:
            received_contexts.append(ctx)
            return {"alpha": _stub_builder}

        handler = _dispatch_from_registry(factory, "test")
        spec = _make_spec("alpha", "normalize")
        ctx = _stub_context()

        handler(spec, ctx)
        assert len(received_contexts) == 1
        assert received_contexts[0] is ctx


# ---------------------------------------------------------------------------
# Master dispatcher coverage
# ---------------------------------------------------------------------------


class TestBuilderHandlersTable:
    """Verify consolidated dispatcher coverage."""

    @staticmethod
    def test_all_spec_kinds_have_handlers() -> None:
        """Every consolidated kind has a dedicated dispatch handler."""
        expected_kinds = {
            "normalize",
            "derive",
            "relate",
            "union",
            "project",
            "diagnostic",
        }
        assert set(_CONSOLIDATED_BUILDER_HANDLERS.keys()) == expected_kinds

    @staticmethod
    def test_handler_count_matches_spec_kinds() -> None:
        """Handler table has exactly 6 entries (one per consolidated kind)."""
        assert len(_CONSOLIDATED_BUILDER_HANDLERS) == CONSOLIDATED_HANDLER_COUNT

    @staticmethod
    def test_all_handlers_are_callable() -> None:
        """Every handler in the table must be callable."""
        for kind, handler in _CONSOLIDATED_BUILDER_HANDLERS.items():
            assert callable(handler), f"Handler for {kind!r} is not callable"


# ---------------------------------------------------------------------------
# Registry function smoke tests
# ---------------------------------------------------------------------------


class TestRegistryFunctions:
    """Verify the registry helper functions return expected keys."""

    @staticmethod
    def test_span_unnest_registry_keys() -> None:
        """The span-unnest registry returns all 4 expected builder names."""
        from semantics.pipeline_build import _span_unnest_registry

        ctx = _stub_context()
        registry = _span_unnest_registry(ctx)
        expected = {
            "ast_span_unnest",
            "ts_span_unnest",
            "symtable_span_unnest",
            "py_bc_instruction_span_unnest",
        }
        assert set(registry.keys()) == expected

    @staticmethod
    def test_symtable_registry_keys() -> None:
        """The symtable registry returns all 5 expected builder names."""
        from semantics.pipeline_build import _symtable_registry

        ctx = _stub_context()
        registry = _symtable_registry(ctx)
        expected = {
            "symtable_bindings",
            "symtable_def_sites",
            "symtable_use_sites",
            "symtable_type_params",
            "symtable_type_param_edges",
        }
        assert set(registry.keys()) == expected

    @staticmethod
    def test_diagnostic_registry_keys() -> None:
        """The diagnostic registry returns at least one builder."""
        from semantics.pipeline_build import _diagnostic_registry

        ctx = _stub_context()
        registry = _diagnostic_registry(ctx)
        assert len(registry) > 0
        for name, builder in registry.items():
            assert callable(builder), f"Builder for {name!r} is not callable"

    @staticmethod
    def test_finalize_kind_is_rust_only() -> None:
        """FINALIZE views should be rejected in Python dispatch."""
        from semantics.pipeline_build import _builder_for_project_kind

        spec = _make_spec("cpg_nodes", "finalize")
        with pytest.raises(ValueError, match="Rust CpgEmit transforms"):
            _builder_for_project_kind(spec, _stub_context())

    @staticmethod
    def test_registry_builders_are_callable() -> None:
        """All registry values must be callable DataFrameBuilder instances."""
        from semantics.pipeline_build import _span_unnest_registry, _symtable_registry

        ctx = _stub_context()
        for registry in (_span_unnest_registry(ctx), _symtable_registry(ctx)):
            for name, builder in registry.items():
                assert callable(builder), f"Builder {name!r} is not callable"

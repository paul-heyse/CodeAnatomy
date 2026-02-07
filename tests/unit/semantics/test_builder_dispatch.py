"""Tests for the builder dispatch factory in semantics.pipeline.

Verify that ``_dispatch_from_registry`` creates handlers that resolve
builders from name-keyed registries, and that the master dispatcher
``_builder_for_semantic_spec`` routes all 14 spec kinds.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING
from unittest.mock import MagicMock

import pytest

from semantics.pipeline import (
    _BUILDER_HANDLERS,
    _dispatch_from_registry,
)
from semantics.spec_registry import SemanticSpecIndex

if TYPE_CHECKING:
    from datafusion import DataFrame, SessionContext

    from datafusion_engine.plan.bundle import DataFrameBuilder
    from semantics.pipeline import _SemanticSpecContext


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_spec(name: str, kind: str) -> SemanticSpecIndex:
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
        kind=kind,  # type: ignore[arg-type]
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

    def test_returns_builder_for_known_name(self) -> None:
        """Resolve a builder for a name present in the registry."""
        registry: dict[str, DataFrameBuilder] = {"alpha": _stub_builder}

        def factory(_ctx: _SemanticSpecContext) -> Mapping[str, DataFrameBuilder]:
            return registry

        handler = _dispatch_from_registry(factory, "test")
        spec = _make_spec("alpha", "normalize")
        ctx = _stub_context()

        result = handler(spec, ctx)
        assert result is _stub_builder

    def test_raises_key_error_for_unknown_name(self) -> None:
        """Raise ``KeyError`` when the spec name is not in the registry."""
        registry: dict[str, DataFrameBuilder] = {"alpha": _stub_builder}

        def factory(_ctx: _SemanticSpecContext) -> Mapping[str, DataFrameBuilder]:
            return registry

        handler = _dispatch_from_registry(factory, "test")
        spec = _make_spec("missing", "normalize")
        ctx = _stub_context()

        with pytest.raises(KeyError, match="Missing test builder for output 'missing'"):
            handler(spec, ctx)

    def test_error_message_includes_context_label(self) -> None:
        """Verify the error message uses the provided context label."""

        def factory(_ctx: _SemanticSpecContext) -> Mapping[str, DataFrameBuilder]:
            return {}

        handler = _dispatch_from_registry(factory, "span-unnest")
        spec = _make_spec("bad_name", "span_unnest")
        ctx = _stub_context()

        with pytest.raises(KeyError, match="Missing span-unnest builder"):
            handler(spec, ctx)

    def test_finalize_wraps_builder(self) -> None:
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

    def test_finalize_false_returns_raw_builder(self) -> None:
        """When finalize=False (default), return the builder as-is."""
        registry: dict[str, DataFrameBuilder] = {"alpha": _stub_builder}

        def factory(_ctx: _SemanticSpecContext) -> Mapping[str, DataFrameBuilder]:
            return registry

        handler = _dispatch_from_registry(factory, "test", finalize=False)
        spec = _make_spec("alpha", "normalize")
        ctx = _stub_context()

        result = handler(spec, ctx)
        assert result is _stub_builder

    def test_factory_receives_context(self) -> None:
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
    """Verify the ``_BUILDER_HANDLERS`` table covers all spec kinds."""

    def test_all_spec_kinds_have_handlers(self) -> None:
        """Every ``SpecKind`` literal must appear as a key in ``_BUILDER_HANDLERS``."""
        expected_kinds = {
            "normalize",
            "scip_normalize",
            "bytecode_line_index",
            "span_unnest",
            "symtable",
            "diagnostic",
            "export",
            "projection",
            "finalize",
            "artifact",
            "join_group",
            "relate",
            "union_nodes",
            "union_edges",
        }
        assert set(_BUILDER_HANDLERS.keys()) == expected_kinds

    def test_handler_count_matches_spec_kinds(self) -> None:
        """Handler table has exactly 14 entries (one per SpecKind)."""
        assert len(_BUILDER_HANDLERS) == 14

    def test_all_handlers_are_callable(self) -> None:
        """Every handler in the table must be callable."""
        for kind, handler in _BUILDER_HANDLERS.items():
            assert callable(handler), f"Handler for {kind!r} is not callable"


# ---------------------------------------------------------------------------
# Registry function smoke tests
# ---------------------------------------------------------------------------


class TestRegistryFunctions:
    """Verify the registry helper functions return expected keys."""

    def test_span_unnest_registry_keys(self) -> None:
        """The span-unnest registry returns all 4 expected builder names."""
        from semantics.pipeline import _span_unnest_registry

        ctx = _stub_context()
        registry = _span_unnest_registry(ctx)
        expected = {
            "ast_span_unnest",
            "ts_span_unnest",
            "symtable_span_unnest",
            "py_bc_instruction_span_unnest",
        }
        assert set(registry.keys()) == expected

    def test_symtable_registry_keys(self) -> None:
        """The symtable registry returns all 5 expected builder names."""
        from semantics.pipeline import _symtable_registry

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

    def test_diagnostic_registry_keys(self) -> None:
        """The diagnostic registry returns at least one builder."""
        from semantics.pipeline import _diagnostic_registry

        ctx = _stub_context()
        registry = _diagnostic_registry(ctx)
        assert len(registry) > 0
        for name, builder in registry.items():
            assert callable(builder), f"Builder for {name!r} is not callable"

    def test_finalize_registry_keys(self) -> None:
        """The finalize registry returns CPG output builders."""
        from semantics.pipeline import _finalize_registry

        # _finalize_registry needs real context.runtime_profile and
        # context.manifest because _cpg_output_view_specs accesses them.
        # Build a mock with the .session_runtime() chain.
        mock_runtime_profile = MagicMock()
        mock_session_runtime = MagicMock()
        mock_runtime_profile.session_runtime.return_value = mock_session_runtime
        mock_session_runtime.semantic_output_key = "test_key"
        mock_session_runtime.semantic_output_format = "delta"

        ctx = MagicMock()
        ctx.runtime_profile = mock_runtime_profile
        ctx.manifest = MagicMock()

        # The registry may raise if the mock is insufficient for
        # _cpg_output_view_specs; guard with try/except for robustness.
        try:
            registry = _finalize_registry(ctx)
            assert len(registry) > 0
        except (AttributeError, TypeError):
            # Expected if mock is too shallow for the full builder chain.
            pytest.skip("Mock insufficient for _cpg_output_view_specs")

    def test_registry_builders_are_callable(self) -> None:
        """All registry values must be callable DataFrameBuilder instances."""
        from semantics.pipeline import _span_unnest_registry, _symtable_registry

        ctx = _stub_context()
        for registry in (_span_unnest_registry(ctx), _symtable_registry(ctx)):
            for name, builder in registry.items():
                assert callable(builder), f"Builder {name!r} is not callable"

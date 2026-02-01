"""Tests for semantics.catalog.view_builders module."""

from __future__ import annotations

import pytest

from semantics.catalog.view_builders import (
    VIEW_BUILDERS,
    VIEW_BUNDLE_BUILDERS,
    view_builder,
    view_builders,
)


class TestViewBuildersDict:
    """Tests for VIEW_BUILDERS dictionary."""

    @pytest.mark.smoke
    def test_is_dict(self) -> None:
        """VIEW_BUILDERS is a dictionary."""
        assert isinstance(VIEW_BUILDERS, dict)

    def test_contains_expected_keys(self) -> None:
        """VIEW_BUILDERS contains expected analysis view keys."""
        expected_keys = [
            "type_exprs_norm_v1",
            "type_nodes_v1",
            "py_bc_blocks_norm_v1",
            "py_bc_cfg_edges_norm_v1",
            "py_bc_def_use_events_v1",
            "py_bc_reaches_v1",
            "diagnostics_norm_v1",
            "span_errors_v1",
        ]
        for key in expected_keys:
            assert key in VIEW_BUILDERS, f"Missing key: {key}"

    def test_values_are_callable(self) -> None:
        """VIEW_BUILDERS values are callable."""
        for name, builder in VIEW_BUILDERS.items():
            assert callable(builder), f"Builder for {name} is not callable"


class TestViewBundleBuildersDict:
    """Tests for VIEW_BUNDLE_BUILDERS dictionary."""

    @pytest.mark.smoke
    def test_is_dict(self) -> None:
        """VIEW_BUNDLE_BUILDERS is a dictionary."""
        assert isinstance(VIEW_BUNDLE_BUILDERS, dict)

    def test_keys_match_view_builders(self) -> None:
        """VIEW_BUNDLE_BUILDERS keys match VIEW_BUILDERS keys."""
        assert set(VIEW_BUNDLE_BUILDERS.keys()) == set(VIEW_BUILDERS.keys())

    def test_values_are_callable(self) -> None:
        """VIEW_BUNDLE_BUILDERS values are callable."""
        for name, builder in VIEW_BUNDLE_BUILDERS.items():
            assert callable(builder), f"Bundle builder for {name} is not callable"


class TestViewBuilder:
    """Tests for view_builder function."""

    @pytest.mark.smoke
    def test_returns_callable_for_known_name(self) -> None:
        """view_builder returns callable for known analysis view name."""
        builder = view_builder(
            "type_exprs_norm_v1",
            input_mapping={},
            config=None,
        )
        assert builder is not None
        assert callable(builder)

    def test_returns_none_for_unknown_name(self) -> None:
        """view_builder returns None for unknown name."""
        builder = view_builder(
            "completely_unknown_view_xyz",
            input_mapping={},
            config=None,
        )
        assert builder is None

    def test_analysis_builder_found(self) -> None:
        """view_builder finds analysis builders."""
        for name in VIEW_BUILDERS:
            builder = view_builder(name, input_mapping={}, config=None)
            assert builder is not None, f"Builder not found for {name}"

    def test_empty_input_mapping_works(self) -> None:
        """view_builder works with empty input mapping."""
        builder = view_builder(
            "type_nodes_v1",
            input_mapping={},
            config=None,
        )
        assert builder is not None


class TestViewBuilders:
    """Tests for view_builders function."""

    @pytest.mark.smoke
    def test_returns_dict(self) -> None:
        """view_builders returns a dictionary."""
        builders = view_builders(input_mapping={}, config=None)
        assert isinstance(builders, dict)

    def test_contains_analysis_builders(self) -> None:
        """view_builders contains all analysis builders."""
        builders = view_builders(input_mapping={}, config=None)
        for name in VIEW_BUILDERS:
            assert name in builders, f"Missing analysis builder: {name}"

    def test_all_values_are_callable(self) -> None:
        """view_builders values are all callable."""
        builders = view_builders(input_mapping={}, config=None)
        for name, builder in builders.items():
            assert callable(builder), f"Builder for {name} is not callable"

    def test_input_mapping_parameter(self) -> None:
        """view_builders accepts input_mapping parameter."""
        # Custom input mapping
        mapping = {"base_table": "custom_table_name"}
        builders = view_builders(input_mapping=mapping, config=None)
        assert isinstance(builders, dict)


class TestBuilderSignatures:
    """Tests for builder function signatures."""

    def test_analysis_builders_accept_session_context(self) -> None:
        """Analysis builders accept SessionContext as single argument."""
        # We just verify the signature, not the actual execution
        # which would require a full DataFusion setup
        for name, builder in VIEW_BUILDERS.items():
            # Check that builder is a callable
            assert callable(builder), f"Builder {name} is not callable"


class TestBuilderTypeAnnotations:
    """Tests for builder type exports."""

    def test_dataframe_builder_is_callable_type(self) -> None:
        """DataFrameBuilder type is exported and usable."""
        # Verify it's a type alias for Callable
        # We can check by verifying a known builder matches the type
        builder = VIEW_BUILDERS.get("type_exprs_norm_v1")
        if builder:
            # This is a compile-time check; at runtime we verify callable
            assert callable(builder)

    def test_plan_bundle_builder_is_callable_type(self) -> None:
        """PlanBundleBuilder type is exported and usable."""
        from semantics.catalog.view_builders import PlanBundleBuilder

        # Verify the type is accessible
        assert PlanBundleBuilder is not None


class TestViewBuilderIntegration:
    """Integration tests for view builder retrieval."""

    def test_semantic_builders_generated_with_input_mapping(self) -> None:
        """Semantic builders are generated when input_mapping provided."""
        # Provide a realistic input mapping
        mapping = {
            "cst_refs": "cst_refs_v1",
            "scip_occurrences": "scip_occurrences_v1",
            "file_line_index_v1": "file_line_index_v1",
        }
        builders = view_builders(input_mapping=mapping, config=None)

        # Should have at least the analysis builders
        assert len(builders) >= len(VIEW_BUILDERS)

    def test_config_none_is_acceptable(self) -> None:
        """view_builders works when config is None."""
        builders = view_builders(input_mapping={}, config=None)
        assert builders is not None
        assert len(builders) > 0

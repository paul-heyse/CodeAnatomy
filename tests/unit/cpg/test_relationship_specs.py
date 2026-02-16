"""Unit tests for semantic relationship specs."""

from __future__ import annotations

from semantics.naming import canonical_output_name
from semantics.quality import QualityRelationshipSpec
from semantics.spec_registry import RELATIONSHIP_SPECS


class TestSemanticRelationshipSpecs:
    """Tests for semantic relationship spec registry."""

    @staticmethod
    def test_specs_are_quality_relationship_spec_instances() -> None:
        """All specs should be QualityRelationshipSpec instances."""
        assert RELATIONSHIP_SPECS
        assert all(isinstance(spec, QualityRelationshipSpec) for spec in RELATIONSHIP_SPECS)

    @staticmethod
    def test_spec_names_are_unique() -> None:
        """Relationship spec names must be unique."""
        names = [spec.name for spec in RELATIONSHIP_SPECS]
        assert len(names) == len(set(names))

    @staticmethod
    def test_spec_names_are_canonical() -> None:
        """Spec names should use canonical output naming."""
        for spec in RELATIONSHIP_SPECS:
            assert spec.name == canonical_output_name(spec.name)

    @staticmethod
    def test_spec_tables_are_canonical() -> None:
        """Spec table names should be canonical outputs."""
        scip_norm = canonical_output_name("scip_occurrences_norm")
        for spec in RELATIONSHIP_SPECS:
            assert spec.left_view == canonical_output_name(spec.left_view)
            assert spec.right_view == canonical_output_name(spec.right_view)
            assert spec.right_view == scip_norm

    @staticmethod
    def test_join_types_valid() -> None:
        """Join types should use supported values."""
        for spec in RELATIONSHIP_SPECS:
            assert spec.how in {"inner", "left", "right", "full"}

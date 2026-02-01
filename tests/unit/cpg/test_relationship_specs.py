"""Unit tests for semantic relationship specs."""

from __future__ import annotations

from semantics.naming import canonical_output_name
from semantics.spec_registry import RELATIONSHIP_SPECS
from semantics.specs import RelationshipSpec


class TestSemanticRelationshipSpecs:
    """Tests for semantic relationship spec registry."""

    def test_specs_are_relationship_spec_instances(self) -> None:
        """All specs should be RelationshipSpec instances."""
        assert RELATIONSHIP_SPECS
        assert all(isinstance(spec, RelationshipSpec) for spec in RELATIONSHIP_SPECS)

    def test_spec_names_are_unique(self) -> None:
        """Relationship spec names must be unique."""
        names = [spec.name for spec in RELATIONSHIP_SPECS]
        assert len(names) == len(set(names))

    def test_spec_names_are_canonical(self) -> None:
        """Spec names should use canonical output naming."""
        for spec in RELATIONSHIP_SPECS:
            assert spec.name == canonical_output_name(spec.name)

    def test_spec_tables_are_canonical(self) -> None:
        """Spec table names should be canonical outputs."""
        scip_norm = canonical_output_name("scip_occurrences_norm")
        for spec in RELATIONSHIP_SPECS:
            assert spec.left_table.endswith("_v1")
            assert spec.right_table.endswith("_v1")
            assert spec.right_table == scip_norm

    def test_join_hints_valid(self) -> None:
        """Join hints should use supported values."""
        for spec in RELATIONSHIP_SPECS:
            assert spec.join_hint in {"overlap", "contains", "ownership"}

"""Unit tests for the relationship specs declarative DSL."""

from __future__ import annotations

import pytest

from cpg.kind_catalog import (
    EDGE_KIND_PY_CALLS_QNAME,
    EDGE_KIND_PY_IMPORTS_SYMBOL,
    EDGE_KIND_PY_REFERENCES_SYMBOL,
)
from cpg.relationship_specs import (
    ALL_RELATIONSHIP_SPECS,
    REL_CALLSITE_QNAME_SPEC,
    REL_CALLSITE_SYMBOL_SPEC,
    REL_DEF_SYMBOL_SPEC,
    REL_IMPORT_SYMBOL_SPEC,
    REL_NAME_SYMBOL_SPEC,
    CoalesceRef,
    ColumnRef,
    QNameRelationshipSpec,
    RelationshipOrigin,
    RelationshipSpec,
    coalesce_ref,
    col_ref,
    get_qname_specs,
    get_relationship_spec,
    get_symbol_specs,
)


class TestColumnRef:
    """Tests for ColumnRef."""

    def test_col_ref_creation(self) -> None:
        """Create a simple column reference."""
        ref = col_ref("my_column")
        assert ref.name == "my_column"
        assert str(ref) == "my_column"

    def test_column_ref_frozen(self) -> None:
        """Verify ColumnRef is immutable."""
        ref = ColumnRef("test")
        with pytest.raises(AttributeError):
            ref.name = "changed"  # type: ignore[misc]


class TestCoalesceRef:
    """Tests for CoalesceRef."""

    def test_coalesce_ref_creation(self) -> None:
        """Create a coalesced column reference."""
        ref = coalesce_ref("col1", "col2")
        assert ref.columns == ("col1", "col2")
        assert str(ref) == "coalesce(col1, col2)"

    def test_coalesce_ref_three_columns(self) -> None:
        """Create coalesced reference with three columns."""
        ref = coalesce_ref("a", "b", "c")
        assert ref.columns == ("a", "b", "c")
        assert str(ref) == "coalesce(a, b, c)"

    def test_coalesce_ref_requires_two_columns(self) -> None:
        """Coalesce requires at least two columns."""
        with pytest.raises(ValueError, match="at least two columns"):
            coalesce_ref("only_one")

    def test_coalesce_ref_frozen(self) -> None:
        """Verify CoalesceRef is immutable."""
        ref = CoalesceRef(("a", "b"))
        with pytest.raises(AttributeError):
            ref.columns = ("x", "y")  # type: ignore[misc]


class TestRelationshipSpec:
    """Tests for RelationshipSpec."""

    def test_name_symbol_spec_properties(self) -> None:
        """Verify name_symbol spec has correct values."""
        spec = REL_NAME_SYMBOL_SPEC
        assert spec.name == "name_symbol"
        assert spec.edge_kind == EDGE_KIND_PY_REFERENCES_SYMBOL
        assert spec.origin == RelationshipOrigin.CST
        assert spec.src_table == "cst_refs"
        assert spec.output_view_name == "rel_name_symbol_v1"
        assert spec.entity_id_alias == "ref_id"
        assert spec.confidence == 0.5
        assert spec.score == 0.5

    def test_import_symbol_spec_coalesce(self) -> None:
        """Verify import_symbol spec uses coalesce for entity_id."""
        spec = REL_IMPORT_SYMBOL_SPEC
        assert spec.name == "import_symbol"
        assert spec.edge_kind == EDGE_KIND_PY_IMPORTS_SYMBOL
        assert isinstance(spec.entity_id_col, CoalesceRef)
        assert spec.entity_id_col.columns == ("import_alias_id", "import_id")
        assert spec.entity_id_alias == "import_alias_id"

    def test_def_symbol_spec_confidence(self) -> None:
        """Verify def_symbol spec has higher confidence."""
        spec = REL_DEF_SYMBOL_SPEC
        assert spec.confidence == 0.6
        assert spec.score == 0.6

    def test_callsite_symbol_spec_span_cols(self) -> None:
        """Verify callsite_symbol uses call_bstart/call_bend."""
        spec = REL_CALLSITE_SYMBOL_SPEC
        assert isinstance(spec.bstart_col, ColumnRef)
        assert spec.bstart_col.name == "call_bstart"
        assert isinstance(spec.bend_col, ColumnRef)
        assert spec.bend_col.name == "call_bend"


class TestQNameRelationshipSpec:
    """Tests for QNameRelationshipSpec."""

    def test_callsite_qname_spec_properties(self) -> None:
        """Verify callsite_qname spec has correct values."""
        spec = REL_CALLSITE_QNAME_SPEC
        assert spec.name == "callsite_qname"
        assert spec.edge_kind == EDGE_KIND_PY_CALLS_QNAME
        assert spec.origin == RelationshipOrigin.CST
        assert spec.src_table == "callsite_qname_candidates_v1"
        assert spec.qname_col == "qname"
        assert spec.qname_source_col == "qname_source"
        assert spec.ambiguity_group_col == "ambiguity_group_id"


class TestSpecRegistry:
    """Tests for spec registry functions."""

    def test_symbol_specs_count(self) -> None:
        """Verify correct number of symbol specs."""
        specs = get_symbol_specs()
        assert len(specs) == 4
        assert all(isinstance(s, RelationshipSpec) for s in specs)

    def test_qname_specs_count(self) -> None:
        """Verify correct number of qname specs."""
        specs = get_qname_specs()
        assert len(specs) == 1
        assert all(isinstance(s, QNameRelationshipSpec) for s in specs)

    def test_all_specs_contains_both_types(self) -> None:
        """Verify ALL_RELATIONSHIP_SPECS contains both types."""
        assert len(ALL_RELATIONSHIP_SPECS) == 5
        symbol_count = sum(1 for s in ALL_RELATIONSHIP_SPECS if isinstance(s, RelationshipSpec))
        qname_count = sum(1 for s in ALL_RELATIONSHIP_SPECS if isinstance(s, QNameRelationshipSpec))
        assert symbol_count == 4
        assert qname_count == 1

    def test_get_relationship_spec_by_name(self) -> None:
        """Retrieve spec by name."""
        spec = get_relationship_spec("name_symbol")
        assert spec is REL_NAME_SYMBOL_SPEC

        spec = get_relationship_spec("callsite_qname")
        assert spec is REL_CALLSITE_QNAME_SPEC

    def test_get_relationship_spec_unknown(self) -> None:
        """Unknown spec name raises KeyError."""
        with pytest.raises(KeyError, match="Unknown relationship spec"):
            get_relationship_spec("nonexistent")

    def test_spec_names_are_unique(self) -> None:
        """All spec names are unique."""
        names = [spec.name for spec in ALL_RELATIONSHIP_SPECS]
        assert len(names) == len(set(names))

    def test_output_view_names_are_unique(self) -> None:
        """All output view names are unique."""
        names = [spec.output_view_name for spec in ALL_RELATIONSHIP_SPECS]
        assert len(names) == len(set(names))


class TestRelationshipOrigin:
    """Tests for RelationshipOrigin enum."""

    def test_cst_origin_value(self) -> None:
        """CST origin has correct string value."""
        assert RelationshipOrigin.CST == "cst"

    def test_scip_origin_value(self) -> None:
        """SCIP origin has correct string value."""
        assert RelationshipOrigin.SCIP == "scip"

    def test_symtable_origin_value(self) -> None:
        """SYMTABLE origin has correct string value."""
        assert RelationshipOrigin.SYMTABLE == "symtable"

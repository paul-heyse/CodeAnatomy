"""Unit tests for relationship contract spec generator."""

from __future__ import annotations

import pytest

from cpg.relationship_contracts import (
    RELATIONSHIP_CONTRACT_DATA,
    RELATIONSHIP_SCHEMA_VERSION,
    STANDARD_CANONICAL_SORT_PREFIX,
    STANDARD_DEDUPE_STRATEGY,
    STANDARD_RELATIONSHIP_TIE_BREAKERS,
    STANDARD_SYMBOL_DEDUPE_COLUMNS,
    STANDARD_VIRTUAL_FIELDS,
    RelationshipContractData,
    derive_relationship_contract_data,
    relationship_contract_data_by_name,
)
from schema_spec.system import SortKeySpec


class TestRelationshipContractData:
    """Test suite for RelationshipContractData dataclass."""

    def test_dedupe_keys_symbol_relationship(self) -> None:
        """Test dedupe keys for symbol relationships."""
        data = RelationshipContractData(
            table_name="rel_name_symbol",
            entity_id_cols=("entity_id",),
            dedupe_columns=STANDARD_SYMBOL_DEDUPE_COLUMNS,
        )
        expected = ("entity_id", "symbol", "path", "bstart", "bend")
        assert data.dedupe_keys == expected

    def test_dedupe_keys_with_extra(self) -> None:
        """Test dedupe keys with extra columns."""
        data = RelationshipContractData(
            table_name="test",
            entity_id_cols=("id",),
            dedupe_columns=("a", "b"),
            extra_dedupe_keys=("extra",),
        )
        assert data.dedupe_keys == ("id", "a", "b", "extra")

    def test_tie_breakers_default(self) -> None:
        """Test that default tie breakers are standard."""
        data = RelationshipContractData(
            table_name="test",
            entity_id_cols=("id",),
        )
        assert data.tie_breakers == STANDARD_RELATIONSHIP_TIE_BREAKERS

    def test_tie_breakers_custom(self) -> None:
        """Test custom tie breakers override."""
        custom = (SortKeySpec(column="custom", order="ascending"),)
        data = RelationshipContractData(
            table_name="test",
            entity_id_cols=("id",),
            custom_tie_breakers=custom,
        )
        assert data.tie_breakers == custom

    def test_canonical_sort_default(self) -> None:
        """Test canonical sort derivation from entity ID cols."""
        data = RelationshipContractData(
            table_name="test",
            entity_id_cols=("col_a", "col_b"),
        )
        expected = (
            SortKeySpec(column="path", order="ascending"),
            SortKeySpec(column="bstart", order="ascending"),
            SortKeySpec(column="col_a", order="ascending"),
            SortKeySpec(column="col_b", order="ascending"),
        )
        assert data.canonical_sort == expected

    def test_canonical_sort_custom_suffix(self) -> None:
        """Test custom canonical sort suffix override."""
        custom_suffix = (SortKeySpec(column="custom", order="descending"),)
        data = RelationshipContractData(
            table_name="test",
            entity_id_cols=("id",),
            custom_canonical_sort_suffix=custom_suffix,
        )
        expected = STANDARD_CANONICAL_SORT_PREFIX + custom_suffix
        assert data.canonical_sort == expected

    def test_resolved_version_default(self) -> None:
        """Test default version is RELATIONSHIP_SCHEMA_VERSION."""
        data = RelationshipContractData(
            table_name="test",
            entity_id_cols=("id",),
        )
        assert data.resolved_version == RELATIONSHIP_SCHEMA_VERSION

    def test_resolved_version_custom(self) -> None:
        """Test custom version override."""
        data = RelationshipContractData(
            table_name="test",
            entity_id_cols=("id",),
            version=42,
        )
        assert data.resolved_version == 42


class TestDeriveRelationshipContractData:
    """Test suite for derive_relationship_contract_data."""

    def test_derives_all_specs(self) -> None:
        """Test that derive_relationship_contract_data covers all specs."""
        derived = derive_relationship_contract_data()
        assert len(derived) == len(RELATIONSHIP_CONTRACT_DATA)

    def test_derived_matches_static_data(self) -> None:
        """Test that derived data matches RELATIONSHIP_CONTRACT_DATA structure."""
        derived = derive_relationship_contract_data()
        derived_names = {d.table_name for d in derived}
        static_names = {d.table_name for d in RELATIONSHIP_CONTRACT_DATA}
        assert derived_names == static_names


class TestRelationshipContractDataByName:
    """Test suite for relationship_contract_data_by_name."""

    def test_returns_mapping(self) -> None:
        """Test that function returns a mapping."""
        mapping = relationship_contract_data_by_name()
        assert isinstance(mapping, dict)

    def test_contains_all_contracts(self) -> None:
        """Test that mapping contains all contract data entries."""
        mapping = relationship_contract_data_by_name()
        assert len(mapping) == len(RELATIONSHIP_CONTRACT_DATA)

    def test_lookup_by_name(self) -> None:
        """Test that contracts can be looked up by name."""
        mapping = relationship_contract_data_by_name()
        data = mapping["rel_name_symbol"]
        assert data.entity_id_cols == ("entity_id",)


class TestStaticContractDataMatches:
    """Test that RELATIONSHIP_CONTRACT_DATA matches expected patterns."""

    @pytest.mark.parametrize(
        ("table_name", "entity_id_cols"),
        [
            ("rel_name_symbol", ("entity_id",)),
            ("rel_import_symbol", ("entity_id",)),
            ("rel_def_symbol", ("entity_id",)),
            ("rel_callsite_symbol", ("entity_id",)),
        ],
    )
    def test_contract_data_entity_cols(
        self,
        table_name: str,
        entity_id_cols: tuple[str, ...],
    ) -> None:
        """Test that contract data has correct entity ID columns."""
        mapping = relationship_contract_data_by_name()
        data = mapping[table_name]
        assert data.entity_id_cols == entity_id_cols

    def test_all_symbol_relationships_use_symbol_dedupe(self) -> None:
        """Test that symbol relationships use STANDARD_SYMBOL_DEDUPE_COLUMNS."""
        symbol_names = {
            "rel_name_symbol",
            "rel_import_symbol",
            "rel_def_symbol",
            "rel_callsite_symbol",
        }
        mapping = relationship_contract_data_by_name()
        for name in symbol_names:
            assert mapping[name].dedupe_columns == STANDARD_SYMBOL_DEDUPE_COLUMNS


class TestStandardConstants:
    """Test suite for module-level standard constants."""

    def test_tie_breakers_structure(self) -> None:
        """Test standard tie breakers have expected structure."""
        assert len(STANDARD_RELATIONSHIP_TIE_BREAKERS) == 3
        assert STANDARD_RELATIONSHIP_TIE_BREAKERS[0].column == "score"
        assert STANDARD_RELATIONSHIP_TIE_BREAKERS[0].order == "descending"
        assert STANDARD_RELATIONSHIP_TIE_BREAKERS[1].column == "confidence"
        assert STANDARD_RELATIONSHIP_TIE_BREAKERS[2].column == "task_priority"
        assert STANDARD_RELATIONSHIP_TIE_BREAKERS[2].order == "ascending"

    def test_virtual_fields(self) -> None:
        """Test standard virtual fields."""
        assert STANDARD_VIRTUAL_FIELDS == ("origin",)

    def test_dedupe_strategy(self) -> None:
        """Test standard dedupe strategy."""
        assert STANDARD_DEDUPE_STRATEGY == "KEEP_FIRST_AFTER_SORT"

    def test_canonical_sort_prefix(self) -> None:
        """Test canonical sort prefix."""
        assert len(STANDARD_CANONICAL_SORT_PREFIX) == 2
        assert STANDARD_CANONICAL_SORT_PREFIX[0].column == "path"
        assert STANDARD_CANONICAL_SORT_PREFIX[1].column == "bstart"

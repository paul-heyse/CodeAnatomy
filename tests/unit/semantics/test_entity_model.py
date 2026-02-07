"""Verify entity declarations produce structurally identical table specs.

Every ``EntityDeclaration`` in ``ENTITY_DECLARATIONS`` must generate the
exact same ``SemanticTableSpec`` and ``SemanticNormalizationSpec`` as the
hand-written entries in ``semantics.registry``.
"""

from __future__ import annotations

import pytest

from semantics.entity_model import (
    EntityDeclaration,
    ForeignKeySpec,
    IdentitySpec,
    LocationSpec,
)
from semantics.entity_registry import (
    ENTITY_DECLARATIONS,
    entity_to_normalization_spec,
    entity_to_table_spec,
    generate_normalization_specs,
    generate_table_specs,
)
from semantics.registry import (
    SEMANTIC_NORMALIZATION_SPECS,
    SEMANTIC_TABLE_SPECS,
)


class TestEntityDeclarationCount:
    """Verify all expected entities are declared."""

    def test_declaration_count_matches_table_specs(self) -> None:
        """Ensure every table spec has a corresponding entity declaration."""
        assert len(ENTITY_DECLARATIONS) == len(SEMANTIC_TABLE_SPECS)

    def test_all_source_tables_covered(self) -> None:
        """Ensure every source table in the registry has a declaration."""
        declared_tables = {decl.source_table for decl in ENTITY_DECLARATIONS}
        assert declared_tables == set(SEMANTIC_TABLE_SPECS.keys())


class TestTableSpecParity:
    """Verify generated table specs match the static registry exactly."""

    @pytest.mark.parametrize(
        "source_table",
        sorted(SEMANTIC_TABLE_SPECS.keys()),
        ids=sorted(SEMANTIC_TABLE_SPECS.keys()),
    )
    def test_table_spec_structural_identity(self, source_table: str) -> None:
        """Verify generated SemanticTableSpec equals the hand-written one."""
        expected = SEMANTIC_TABLE_SPECS[source_table]
        decl = _declaration_for_table(source_table)
        actual = entity_to_table_spec(decl)
        assert actual == expected, f"Mismatch for {source_table}: {actual!r} != {expected!r}"

    def test_generate_table_specs_round_trip(self) -> None:
        """Verify generate_table_specs produces the full registry dict."""
        generated = generate_table_specs(ENTITY_DECLARATIONS)
        assert generated == SEMANTIC_TABLE_SPECS


class TestNormalizationSpecParity:
    """Verify generated normalization specs match the static registry."""

    @pytest.mark.parametrize(
        "index",
        range(len(SEMANTIC_NORMALIZATION_SPECS)),
        ids=[s.source_table for s in SEMANTIC_NORMALIZATION_SPECS],
    )
    def test_normalization_spec_structural_identity(self, index: int) -> None:
        """Verify generated SemanticNormalizationSpec equals the static one."""
        expected = SEMANTIC_NORMALIZATION_SPECS[index]
        decl = _declaration_for_table(expected.source_table)
        actual = entity_to_normalization_spec(decl)
        assert actual == expected, (
            f"Mismatch for {expected.source_table}: {actual!r} != {expected!r}"
        )

    def test_generate_normalization_specs_round_trip(self) -> None:
        """Verify generate_normalization_specs produces the full tuple."""
        generated = generate_normalization_specs(ENTITY_DECLARATIONS)
        assert generated == SEMANTIC_NORMALIZATION_SPECS

    def test_normalization_spec_order_matches(self) -> None:
        """Verify declaration order matches normalization spec order."""
        generated = generate_normalization_specs(ENTITY_DECLARATIONS)
        for gen, expected in zip(generated, SEMANTIC_NORMALIZATION_SPECS, strict=True):
            assert gen.source_table == expected.source_table


class TestForeignKeyEntities:
    """Verify entities with foreign keys produce correct derivations."""

    def test_cst_defs_has_container_fk(self) -> None:
        """Verify cst_defs foreign key to container definition."""
        decl = _declaration_for_table("cst_defs")
        spec = entity_to_table_spec(decl)
        assert len(spec.foreign_keys) == 1
        fk = spec.foreign_keys[0]
        assert fk.out_col == "container_def_id"
        assert fk.target_namespace == "cst_def"
        assert fk.start_col == "container_def_bstart"
        assert fk.end_col == "container_def_bend"
        assert fk.guard_null_if == ("container_def_kind",)

    def test_cst_call_args_has_call_fk(self) -> None:
        """Verify cst_call_args foreign key to call entity."""
        decl = _declaration_for_table("cst_call_args")
        spec = entity_to_table_spec(decl)
        assert len(spec.foreign_keys) == 1
        fk = spec.foreign_keys[0]
        assert fk.out_col == "call_id"
        assert fk.target_namespace == "cst_call"

    def test_cst_docstrings_has_owner_fk(self) -> None:
        """Verify cst_docstrings foreign key to owner definition."""
        decl = _declaration_for_table("cst_docstrings")
        spec = entity_to_table_spec(decl)
        assert len(spec.foreign_keys) == 1
        fk = spec.foreign_keys[0]
        assert fk.out_col == "owner_def_id"
        assert fk.target_namespace == "cst_def"

    def test_cst_decorators_has_owner_fk(self) -> None:
        """Verify cst_decorators foreign key to owner definition."""
        decl = _declaration_for_table("cst_decorators")
        spec = entity_to_table_spec(decl)
        assert len(spec.foreign_keys) == 1
        fk = spec.foreign_keys[0]
        assert fk.out_col == "owner_def_id"
        assert fk.target_namespace == "cst_def"

    def test_entities_without_fks(self) -> None:
        """Verify entities without foreign keys produce empty tuples."""
        for table in ("cst_refs", "cst_imports", "cst_callsites"):
            decl = _declaration_for_table(table)
            spec = entity_to_table_spec(decl)
            assert spec.foreign_keys == (), f"{table} should have no FKs"


class TestContentColumns:
    """Verify content column declarations."""

    @pytest.mark.parametrize(
        ("source_table", "expected_content"),
        [
            ("cst_refs", ("ref_text",)),
            ("cst_call_args", ("arg_text",)),
            ("cst_docstrings", ("docstring",)),
            ("cst_decorators", ("decorator_text",)),
        ],
    )
    def test_entity_with_content(
        self, source_table: str, expected_content: tuple[str, ...]
    ) -> None:
        """Verify entities with content columns produce matching text_cols."""
        decl = _declaration_for_table(source_table)
        spec = entity_to_table_spec(decl)
        assert spec.text_cols == expected_content

    def test_entities_without_content(self) -> None:
        """Verify entities without content produce empty text_cols."""
        for table in ("cst_defs", "cst_imports", "cst_callsites"):
            decl = _declaration_for_table(table)
            spec = entity_to_table_spec(decl)
            assert spec.text_cols == (), f"{table} should have no text_cols"


class TestNonDefaultSpanColumns:
    """Verify entities with non-default span columns."""

    def test_cst_defs_uses_def_span(self) -> None:
        """Verify cst_defs uses def_bstart/def_bend span columns."""
        decl = _declaration_for_table("cst_defs")
        spec = entity_to_table_spec(decl)
        assert spec.primary_span.start_col == "def_bstart"
        assert spec.primary_span.end_col == "def_bend"
        assert spec.entity_id.start_col == "def_bstart"
        assert spec.entity_id.end_col == "def_bend"

    def test_cst_imports_uses_alias_span(self) -> None:
        """Verify cst_imports uses alias_bstart/alias_bend span columns."""
        decl = _declaration_for_table("cst_imports")
        spec = entity_to_table_spec(decl)
        assert spec.primary_span.start_col == "alias_bstart"
        assert spec.primary_span.end_col == "alias_bend"
        assert spec.entity_id.start_col == "alias_bstart"
        assert spec.entity_id.end_col == "alias_bend"

    def test_cst_callsites_uses_call_span(self) -> None:
        """Verify cst_callsites uses call_bstart/call_bend span columns."""
        decl = _declaration_for_table("cst_callsites")
        spec = entity_to_table_spec(decl)
        assert spec.primary_span.start_col == "call_bstart"
        assert spec.primary_span.end_col == "call_bend"
        assert spec.entity_id.start_col == "call_bstart"
        assert spec.entity_id.end_col == "call_bend"


class TestNormalizedNameOverride:
    """Verify the normalized name override mechanism."""

    def test_cst_callsites_uses_override(self) -> None:
        """Verify cst_callsites uses cst_calls_norm (not cst_callsites_norm)."""
        decl = _declaration_for_table("cst_callsites")
        assert decl.normalized_name == "cst_calls_norm"
        norm_spec = entity_to_normalization_spec(decl)
        assert norm_spec.normalized_name == "cst_calls_norm"

    def test_default_convention(self) -> None:
        """Verify entities without override use source_table + _norm."""
        decl = _declaration_for_table("cst_refs")
        assert decl.normalized_name is None
        assert decl.effective_normalized_name() == "cst_refs_norm"


class TestEntityDeclarationDefaults:
    """Verify default field values on entity declarations."""

    def test_span_unit_defaults_to_byte(self) -> None:
        """Verify all declarations default to byte span unit."""
        for decl in ENTITY_DECLARATIONS:
            assert decl.span_unit == "byte", f"{decl.name} should use byte spans"

    def test_include_in_cpg_nodes_defaults_to_true(self) -> None:
        """Verify all declarations default to include_in_cpg_nodes=True."""
        for decl in ENTITY_DECLARATIONS:
            assert decl.include_in_cpg_nodes, f"{decl.name} should include in CPG nodes"


class TestEntityModelStructs:
    """Verify entity model struct construction."""

    def test_identity_spec_fields(self) -> None:
        """Verify IdentitySpec stores namespace and id_column."""
        spec = IdentitySpec(namespace="test_ns", id_column="test_id")
        assert spec.namespace == "test_ns"
        assert spec.id_column == "test_id"

    def test_location_spec_fields(self) -> None:
        """Verify LocationSpec stores start and end columns."""
        spec = LocationSpec(start_col="start", end_col="end")
        assert spec.start_col == "start"
        assert spec.end_col == "end"

    def test_foreign_key_spec_defaults(self) -> None:
        """Verify ForeignKeySpec guard_null_if defaults to empty."""
        spec = ForeignKeySpec(
            column="fk_id",
            target_entity="target",
            ref_start_col="s",
            ref_end_col="e",
        )
        assert spec.guard_null_if == ()

    def test_entity_declaration_minimal(self) -> None:
        """Verify minimal EntityDeclaration construction."""
        decl = EntityDeclaration(
            name="test",
            source_table="test_table",
            identity=IdentitySpec(namespace="test", id_column="test_id"),
            location=LocationSpec(start_col="bstart", end_col="bend"),
        )
        assert decl.content == ()
        assert decl.foreign_keys == ()
        assert decl.span_unit == "byte"
        assert decl.include_in_cpg_nodes is True
        assert decl.normalized_name is None
        assert decl.effective_normalized_name() == "test_table_norm"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _declaration_for_table(source_table: str) -> EntityDeclaration:
    """Return the entity declaration for a given source table.

    Parameters
    ----------
    source_table
        Source table name to look up.

    Returns:
    -------
    EntityDeclaration
        Matching entity declaration.

    Raises:
        ValueError: When no declaration matches ``source_table``.
    """
    for decl in ENTITY_DECLARATIONS:
        if decl.source_table == source_table:
            return decl
    msg = f"No entity declaration for source table {source_table!r}"
    raise ValueError(msg)

# ruff: noqa: PLR2004, PLR6301
"""Tests for cpg.node_families module."""

from __future__ import annotations

from cpg.node_families import (
    NODE_FAMILY_DEFAULTS,
    NodeFamily,
    NodeSpecOptions,
    merge_field_bundles,
    node_family_bundle,
    node_family_defaults,
    node_spec_with_family,
)
from schema_spec.field_spec import FieldSpec
from schema_spec.specs import FieldBundle


class TestNodeFamily:
    """Tests for NodeFamily enum."""

    def test_all_families_defined(self) -> None:
        """Verify all expected families are present."""
        expected = {
            "file",
            "cst",
            "symtable",
            "scip",
            "treesitter",
            "type",
            "diagnostic",
            "runtime",
            "bytecode",
        }
        actual = {f.value for f in NodeFamily}
        assert actual == expected

    def test_strenum_behavior(self) -> None:
        """Verify StrEnum string representation."""
        assert str(NodeFamily.FILE) == "file"
        assert str(NodeFamily.CST) == "cst"


class TestNodeFamilyDefaults:
    """Tests for NODE_FAMILY_DEFAULTS mapping."""

    def test_all_families_have_defaults(self) -> None:
        """Verify every family has a defaults entry."""
        for family in NodeFamily:
            assert family in NODE_FAMILY_DEFAULTS

    def test_file_family_defaults(self) -> None:
        """Verify FILE family defaults."""
        defaults = NODE_FAMILY_DEFAULTS[NodeFamily.FILE]
        assert defaults.family == NodeFamily.FILE
        assert defaults.path_cols == ("path",)
        assert defaults.bstart_cols == ("bstart",)
        assert defaults.bend_cols == ("bend",)
        assert defaults.file_id_cols == ("file_id",)
        assert defaults.has_span is True
        assert defaults.has_file_id is True

    def test_scip_family_defaults(self) -> None:
        """Verify SCIP family defaults have no anchoring."""
        defaults = NODE_FAMILY_DEFAULTS[NodeFamily.SCIP]
        assert defaults.path_cols == ()
        assert defaults.bstart_cols == ()
        assert defaults.bend_cols == ()
        assert defaults.file_id_cols == ()
        assert defaults.has_span is False
        assert defaults.has_file_id is False

    def test_treesitter_family_span_cols(self) -> None:
        """Verify TREESITTER family uses start_byte/end_byte."""
        defaults = NODE_FAMILY_DEFAULTS[NodeFamily.TREESITTER]
        assert defaults.bstart_cols == ("start_byte",)
        assert defaults.bend_cols == ("end_byte",)

    def test_runtime_family_defaults(self) -> None:
        """Verify RUNTIME family has no file anchoring."""
        defaults = NODE_FAMILY_DEFAULTS[NodeFamily.RUNTIME]
        assert defaults.has_span is False
        assert defaults.has_file_id is False

    def test_family_fields_include_node_id(self) -> None:
        """Verify all family fields include node_id."""
        for family in NodeFamily:
            defaults = NODE_FAMILY_DEFAULTS[family]
            field_names = [f.name for f in defaults.fields]
            assert "node_id" in field_names

    def test_family_fields_include_node_kind(self) -> None:
        """Verify all family fields include node_kind."""
        for family in NodeFamily:
            defaults = NODE_FAMILY_DEFAULTS[family]
            field_names = [f.name for f in defaults.fields]
            assert "node_kind" in field_names


class TestNodeFamilyBundle:
    """Tests for node_family_bundle function."""

    def test_returns_field_bundle(self) -> None:
        """Verify return type is FieldBundle."""
        bundle = node_family_bundle(NodeFamily.CST)
        assert isinstance(bundle, FieldBundle)

    def test_bundle_name_format(self) -> None:
        """Verify bundle name follows pattern."""
        bundle = node_family_bundle(NodeFamily.CST)
        assert bundle.name == "cst_node"

    def test_bundle_required_non_null(self) -> None:
        """Verify bundle includes required non-null constraints."""
        bundle = node_family_bundle(NodeFamily.FILE)
        assert "node_id" in bundle.required_non_null
        assert "node_kind" in bundle.required_non_null

    def test_bundle_fields_match_defaults(self) -> None:
        """Verify bundle fields match family defaults."""
        for family in NodeFamily:
            bundle = node_family_bundle(family)
            defaults = NODE_FAMILY_DEFAULTS[family]
            assert bundle.fields == defaults.fields


class TestNodeFamilyDefaultsFunction:
    """Tests for node_family_defaults function."""

    def test_returns_correct_defaults(self) -> None:
        """Verify function returns correct defaults object."""
        defaults = node_family_defaults(NodeFamily.SYMTABLE)
        expected = NODE_FAMILY_DEFAULTS[NodeFamily.SYMTABLE]
        assert defaults is expected


class TestNodeSpecWithFamily:
    """Tests for node_spec_with_family function."""

    def test_creates_table_schema_spec(self) -> None:
        """Verify function creates a TableSchemaSpec."""
        spec = node_spec_with_family("test_spec", NodeFamily.CST)
        assert spec.name == "test_spec"

    def test_includes_family_fields(self) -> None:
        """Verify spec includes family default fields."""
        spec = node_spec_with_family("test_spec", NodeFamily.FILE)
        field_names = [f.name for f in spec.fields]
        assert "node_id" in field_names
        assert "node_kind" in field_names
        assert "path" in field_names

    def test_adds_extra_fields(self) -> None:
        """Verify extra fields are appended."""
        from datafusion_engine.arrow import interop

        extra = [FieldSpec(name="custom_field", dtype=interop.string())]
        spec = node_spec_with_family("test_spec", NodeFamily.CST, extra_fields=extra)
        field_names = [f.name for f in spec.fields]
        assert "custom_field" in field_names

    def test_no_duplicate_fields(self) -> None:
        """Verify duplicate field names are not added."""
        from datafusion_engine.arrow import interop

        extra = [FieldSpec(name="node_id", dtype=interop.string())]
        spec = node_spec_with_family("test_spec", NodeFamily.CST, extra_fields=extra)
        node_id_count = sum(1 for f in spec.fields if f.name == "node_id")
        assert node_id_count == 1

    def test_options_version(self) -> None:
        """Verify options version is applied."""
        options = NodeSpecOptions(version=2)
        spec = node_spec_with_family("test_spec", NodeFamily.CST, options=options)
        assert spec.version == 2

    def test_options_key_fields(self) -> None:
        """Verify options key_fields is applied."""
        options = NodeSpecOptions(key_fields=("node_id",))
        spec = node_spec_with_family("test_spec", NodeFamily.CST, options=options)
        assert spec.key_fields == ("node_id",)

    def test_combined_required_non_null(self) -> None:
        """Verify required_non_null combines family and options."""
        options = NodeSpecOptions(required_non_null=("path",))
        spec = node_spec_with_family("test_spec", NodeFamily.CST, options=options)
        assert "node_id" in spec.required_non_null
        assert "node_kind" in spec.required_non_null
        assert "path" in spec.required_non_null


class TestMergeFieldBundles:
    """Tests for merge_field_bundles function."""

    def test_merges_bundles(self) -> None:
        """Verify bundles are merged correctly."""
        from datafusion_engine.arrow import interop

        bundle1 = FieldBundle(
            name="b1",
            fields=(FieldSpec(name="f1", dtype=interop.string()),),
        )
        bundle2 = FieldBundle(
            name="b2",
            fields=(FieldSpec(name="f2", dtype=interop.int32()),),
        )
        result = merge_field_bundles(bundle1, bundle2)
        assert len(result) == 2
        names = [f.name for f in result]
        assert "f1" in names
        assert "f2" in names

    def test_removes_duplicates(self) -> None:
        """Verify duplicate field names are removed."""
        from datafusion_engine.arrow import interop

        bundle1 = FieldBundle(
            name="b1",
            fields=(FieldSpec(name="shared", dtype=interop.string()),),
        )
        bundle2 = FieldBundle(
            name="b2",
            fields=(FieldSpec(name="shared", dtype=interop.int32()),),
        )
        result = merge_field_bundles(bundle1, bundle2)
        assert len(result) == 1
        assert result[0].name == "shared"
        # First occurrence wins
        assert result[0].dtype == interop.string()

    def test_empty_bundles(self) -> None:
        """Verify empty bundles work."""
        result = merge_field_bundles()
        assert result == ()


class TestEntityFamilySpecIntegration:
    """Tests for EntityFamilySpec integration with NodeFamily."""

    def test_entity_family_spec_uses_family_defaults(self) -> None:
        """Verify EntityFamilySpec resolves columns from family."""
        from cpg.kind_catalog import NODE_KIND_PY_FILE
        from cpg.spec_registry import EntityFamilySpec

        spec = EntityFamilySpec(
            name="test",
            node_kind=NODE_KIND_PY_FILE,
            id_cols=("file_id",),
            node_table="test_table",
            node_family=NodeFamily.FILE,
        )
        assert spec.resolved_path_cols == ("path",)
        assert spec.resolved_bstart_cols == ("bstart",)
        assert spec.resolved_bend_cols == ("bend",)
        assert spec.resolved_file_id_cols == ("file_id",)

    def test_entity_family_spec_explicit_overrides_family(self) -> None:
        """Verify explicit column values override family defaults."""
        from cpg.kind_catalog import NODE_KIND_CST_CALLSITE
        from cpg.spec_registry import EntityFamilySpec

        spec = EntityFamilySpec(
            name="test",
            node_kind=NODE_KIND_CST_CALLSITE,
            id_cols=("call_id",),
            node_table="test_table",
            node_family=NodeFamily.CST,
            bstart_cols=("call_bstart", "bstart"),
            bend_cols=("call_bend", "bend"),
        )
        # Explicit overrides family defaults
        assert spec.resolved_bstart_cols == ("call_bstart", "bstart")
        assert spec.resolved_bend_cols == ("call_bend", "bend")
        # Family defaults used when not overridden
        assert spec.resolved_path_cols == ("path",)
        assert spec.resolved_file_id_cols == ("file_id",)

    def test_entity_family_spec_no_family_uses_empty(self) -> None:
        """Verify no family falls back to empty tuples."""
        from cpg.kind_catalog import NODE_KIND_PY_FILE
        from cpg.spec_registry import EntityFamilySpec

        spec = EntityFamilySpec(
            name="test",
            node_kind=NODE_KIND_PY_FILE,
            id_cols=("file_id",),
            node_table="test_table",
            # No node_family set
        )
        assert spec.resolved_path_cols == ()
        assert spec.resolved_bstart_cols == ()
        assert spec.resolved_bend_cols == ()
        assert spec.resolved_file_id_cols == ()

    def test_entity_family_spec_scip_family(self) -> None:
        """Verify SCIP family has no anchoring columns."""
        from cpg.kind_catalog import NODE_KIND_SCIP_SYMBOL
        from cpg.spec_registry import EntityFamilySpec

        spec = EntityFamilySpec(
            name="test",
            node_kind=NODE_KIND_SCIP_SYMBOL,
            id_cols=("symbol",),
            node_table="test_table",
            node_family=NodeFamily.SCIP,
        )
        assert spec.resolved_path_cols == ()
        assert spec.resolved_bstart_cols == ()
        assert spec.resolved_bend_cols == ()
        assert spec.resolved_file_id_cols == ()

    def test_entity_family_spec_treesitter_family(self) -> None:
        """Verify TREESITTER family uses start_byte/end_byte."""
        from cpg.kind_catalog import NODE_KIND_TS_NODE
        from cpg.spec_registry import EntityFamilySpec

        spec = EntityFamilySpec(
            name="test",
            node_kind=NODE_KIND_TS_NODE,
            id_cols=("ts_node_id",),
            node_table="test_table",
            node_family=NodeFamily.TREESITTER,
        )
        assert spec.resolved_bstart_cols == ("start_byte",)
        assert spec.resolved_bend_cols == ("end_byte",)

    def test_all_entity_family_specs_have_families(self) -> None:
        """Verify all ENTITY_FAMILY_SPECS have node_family set."""
        from cpg.spec_registry import ENTITY_FAMILY_SPECS

        for spec in ENTITY_FAMILY_SPECS:
            assert spec.node_family is not None, f"Spec {spec.name!r} missing node_family"

    def test_node_plan_uses_resolved_cols(self) -> None:
        """Verify to_node_plan uses resolved column values."""
        from cpg.kind_catalog import NODE_KIND_CST_CALLSITE
        from cpg.spec_registry import EntityFamilySpec

        spec = EntityFamilySpec(
            name="test",
            node_kind=NODE_KIND_CST_CALLSITE,
            id_cols=("call_id",),
            node_table="test_table",
            node_family=NodeFamily.CST,
            bstart_cols=("call_bstart", "bstart"),
        )
        plan = spec.to_node_plan()
        assert plan is not None
        assert plan.emit.bstart_cols == ("call_bstart", "bstart")
        # Family defaults for other cols
        assert plan.emit.path_cols == ("path",)

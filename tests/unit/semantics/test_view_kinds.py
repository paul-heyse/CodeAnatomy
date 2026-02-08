"""Tests for the ``semantics.view_kinds`` single-authority module.

Verify that ``ViewKind``, ``VIEW_KIND_ORDER``, ``CONSOLIDATED_KIND``,
and ``ViewKindParams`` are correct and that downstream consumers
use ``ViewKindStr`` from the single-authority module.
"""

from __future__ import annotations

import pytest

from semantics.view_kinds import (
    CONSOLIDATED_KIND,
    VIEW_KIND_ORDER,
    ViewKind,
    ViewKindParams,
    ViewKindStr,
)

# ---------------------------------------------------------------------------
# Expected raw string values (the canonical set from the old Literal types)
# ---------------------------------------------------------------------------

_EXPECTED_KINDS: frozenset[str] = frozenset(
    {
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
)


# ---------------------------------------------------------------------------
# ViewKind enum coverage
# ---------------------------------------------------------------------------


class TestViewKind:
    """Verify ``ViewKind`` enum completeness and StrEnum behavior."""

    def test_has_all_14_values(self) -> None:
        """Enum must contain exactly the 14 canonical kind values."""
        assert {str(k) for k in ViewKind} == _EXPECTED_KINDS

    def test_count(self) -> None:
        """Enum member count must be 14."""
        assert len(ViewKind) == 14

    @pytest.mark.parametrize("value", sorted(_EXPECTED_KINDS))
    def test_str_equality(self, value: str) -> None:
        """Each enum member must compare equal to its plain string value."""
        member = ViewKind(value)
        assert member == value
        assert str(member) == value

    def test_is_str_subclass(self) -> None:
        """``ViewKind`` members must be ``str`` instances (StrEnum contract)."""
        for member in ViewKind:
            assert isinstance(member, str)


# ---------------------------------------------------------------------------
# VIEW_KIND_ORDER coverage
# ---------------------------------------------------------------------------


class TestViewKindOrder:
    """Verify ``VIEW_KIND_ORDER`` completeness and structure."""

    def test_covers_all_kinds(self) -> None:
        """Order dict must have an entry for every ``ViewKind`` member."""
        assert set(VIEW_KIND_ORDER.keys()) == set(ViewKind)

    def test_count(self) -> None:
        """Order dict must have exactly 14 entries."""
        assert len(VIEW_KIND_ORDER) == 14

    def test_values_are_non_negative_ints(self) -> None:
        """All order values must be non-negative integers."""
        for kind, order in VIEW_KIND_ORDER.items():
            assert isinstance(order, int), f"{kind}: expected int, got {type(order)}"
            assert order >= 0, f"{kind}: negative order {order}"

    def test_normalize_is_first(self) -> None:
        """The ``normalize`` kind must have the lowest order value (0)."""
        assert VIEW_KIND_ORDER[ViewKind.NORMALIZE] == 0

    def test_artifact_is_last(self) -> None:
        """The ``artifact`` kind must have the highest order value (11)."""
        assert VIEW_KIND_ORDER[ViewKind.ARTIFACT] == 11

    def test_matches_legacy_kind_order(self) -> None:
        """Order dict must match the values from the former ``_KIND_ORDER`` dict."""
        legacy: dict[str, int] = {
            "normalize": 0,
            "scip_normalize": 1,
            "bytecode_line_index": 2,
            "span_unnest": 2,
            "symtable": 2,
            "join_group": 3,
            "relate": 4,
            "union_edges": 5,
            "union_nodes": 6,
            "projection": 7,
            "diagnostic": 8,
            "export": 9,
            "finalize": 10,
            "artifact": 11,
        }
        for kind_str, order in legacy.items():
            member = ViewKind(kind_str)
            assert VIEW_KIND_ORDER[member] == order, (
                f"{kind_str}: expected {order}, got {VIEW_KIND_ORDER[member]}"
            )


# ---------------------------------------------------------------------------
# CONSOLIDATED_KIND mapping
# ---------------------------------------------------------------------------


class TestConsolidatedKind:
    """Verify the future 14 -> 6 consolidation mapping."""

    def test_covers_all_kinds(self) -> None:
        """Mapping must have an entry for every ``ViewKind`` member."""
        assert set(CONSOLIDATED_KIND.keys()) == set(ViewKind)

    def test_target_values_are_valid(self) -> None:
        """All target values must be one of the six consolidated kinds."""
        valid_targets = {"normalize", "derive", "relate", "union", "project", "diagnostic"}
        for kind, target in CONSOLIDATED_KIND.items():
            assert target in valid_targets, f"{kind} -> {target!r} is not a valid target"

    def test_exactly_six_unique_targets(self) -> None:
        """Consolidation must produce exactly 6 unique target kinds."""
        assert len(set(CONSOLIDATED_KIND.values())) == 6

    def test_normalize_group(self) -> None:
        """Normalize-category kinds must all map to 'normalize'."""
        normalize_kinds = {
            ViewKind.NORMALIZE,
            ViewKind.SCIP_NORMALIZE,
            ViewKind.BYTECODE_LINE_INDEX,
            ViewKind.SPAN_UNNEST,
        }
        for kind in normalize_kinds:
            assert CONSOLIDATED_KIND[kind] == "normalize"


# ---------------------------------------------------------------------------
# Single-authority consumer wiring
# ---------------------------------------------------------------------------


class TestSingleAuthorityConsumers:
    """Verify downstream modules consume ``ViewKindStr`` directly."""

    def test_semantic_ir_view_kind_annotation_uses_view_kind_str(self) -> None:
        """``SemanticIRView.kind`` should resolve to ``ViewKindStr``."""
        from typing import get_type_hints

        from semantics.ir import SemanticIRView

        assert get_type_hints(SemanticIRView)["kind"] == ViewKindStr

    def test_semantic_spec_index_kind_annotation_uses_view_kind_str(self) -> None:
        """``SemanticSpecIndex.kind`` should resolve to ``ViewKindStr``."""
        from typing import get_type_hints

        from semantics.spec_registry import SemanticSpecIndex

        assert get_type_hints(SemanticSpecIndex)["kind"] == ViewKindStr

    def test_semantic_ir_view_accepts_string_kind(self) -> None:
        """``SemanticIRView`` must accept string literals for ``kind``."""
        from semantics.ir import SemanticIRView

        view = SemanticIRView(
            name="test_view",
            kind="normalize",
            inputs=("input_a",),
            outputs=("output_a",),
        )
        assert view.kind == "normalize"
        assert view.kind == ViewKind.NORMALIZE

    def test_view_kind_enum_equals_string(self) -> None:
        """``ViewKind`` enum members must equal their plain string values."""
        from semantics.ir import SemanticIRView

        view = SemanticIRView(
            name="test_view",
            kind="normalize",
            inputs=("input_a",),
            outputs=("output_a",),
        )
        # StrEnum values compare equal to plain strings.
        assert view.kind == ViewKind.NORMALIZE

    def test_semantic_spec_index_accepts_string_kind(self) -> None:
        """``SemanticSpecIndex`` must accept string literals for ``kind``."""
        from semantics.spec_registry import SemanticSpecIndex

        spec = SemanticSpecIndex(
            name="test_spec",
            kind="relate",
            inputs=("in_a",),
            outputs=("out_a",),
        )
        assert spec.kind == "relate"
        assert spec.kind == ViewKind.RELATE


# ---------------------------------------------------------------------------
# ViewKindParams defaults
# ---------------------------------------------------------------------------


class TestViewKindParams:
    """Verify ``ViewKindParams`` default construction and field values."""

    def test_default_construction(self) -> None:
        """Default construction must succeed with expected field values."""
        params = ViewKindParams()
        assert params.span_unit == "byte"
        assert params.structure == "flat"
        assert params.derivation_spec is None
        assert params.union_target is None
        assert params.output_contract is None

    def test_custom_construction(self) -> None:
        """Custom construction must accept all field overrides."""
        params = ViewKindParams(
            span_unit="byte",
            structure="nested",
            derivation_spec="symtable_bindings",
            union_target="nodes",
            output_contract="cpg_v1",
        )
        assert params.structure == "nested"
        assert params.derivation_spec == "symtable_bindings"
        assert params.union_target == "nodes"
        assert params.output_contract == "cpg_v1"

    def test_frozen(self) -> None:
        """Instances must be immutable (frozen)."""
        params = ViewKindParams()
        with pytest.raises(AttributeError):
            params.span_unit = "byte"  # type: ignore[misc]

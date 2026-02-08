"""Tests for Rust-aligned semantic execution spec builder."""

from __future__ import annotations

import msgspec

from engine.spec_builder import build_spec_from_ir
from semantics.ir import SemanticIR, SemanticIRJoinGroup, SemanticIRView


def test_build_spec_from_ir_produces_rust_compatible_payload() -> None:
    """IR compilation returns a payload with Rust enum/tag shapes."""
    ir = SemanticIR(
        views=(
            SemanticIRView(
                name="nodes_norm",
                kind="normalize",
                inputs=("raw_nodes",),
                outputs=("nodes_norm",),
            ),
            SemanticIRView(
                name="rel_nodes",
                kind="relate",
                inputs=("nodes_norm", "nodes_norm"),
                outputs=("rel_nodes",),
            ),
        ),
        join_groups=(
            SemanticIRJoinGroup(
                name="join_nodes",
                left_view="nodes_norm",
                right_view="nodes_norm",
                left_on=("entity_id",),
                right_on=("entity_id",),
                how="inner",
                relationship_names=("rel_nodes",),
            ),
        ),
    )

    spec = build_spec_from_ir(
        ir=ir,
        input_locations={"raw_nodes": "/tmp/delta/raw_nodes"},
        output_targets=["rel_nodes"],
        rulepack_profile="strict",
    )
    payload = msgspec.to_builtins(spec)

    assert payload["rulepack_profile"] == "Strict"
    assert payload["view_definitions"][1]["transform"]["kind"] == "Relate"
    assert payload["join_graph"]["edges"][0]["join_type"] == "Inner"
    assert any(rule["name"] == "strict_safety" for rule in payload["rule_intents"])
    assert payload["runtime"]["compliance_capture"] is False
    assert payload["runtime"]["tuner_mode"] == "Off"

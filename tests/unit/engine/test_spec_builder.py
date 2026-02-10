"""Tests for Rust-aligned semantic execution spec builder."""

from __future__ import annotations

import msgspec

from engine.spec_builder import SemanticExecutionSpec, build_spec_from_ir
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


def test_runtime_tracing_contract_allows_new_preset_and_redaction_fields() -> None:
    runtime = {
        "tracing_preset": "MaximalNoData",
        "capture_optimizer_lab": True,
        "capture_delta_codec": True,
        "lineage_tags": {"team": "graph"},
        "tracing": {
            "enabled": True,
            "rule_mode": "PhaseOnly",
            "plan_diff": True,
            "preview_limit": 0,
            "preview_redaction_mode": "DenyList",
            "preview_redacted_columns": ["token"],
            "preview_redaction_token": "[MASKED]",
            "otlp_protocol": "http/protobuf",
        },
    }

    payload = {
        "version": 4,
        "input_relations": (),
        "view_definitions": (),
        "join_graph": {"edges": (), "constraints": ()},
        "output_targets": (),
        "rule_intents": (),
        "rulepack_profile": "Default",
        "typed_parameters": (),
        "runtime": runtime,
    }

    spec = msgspec.convert(payload, type=SemanticExecutionSpec)
    built = msgspec.to_builtins(spec)

    assert built["runtime"]["tracing_preset"] == "MaximalNoData"
    assert built["runtime"]["capture_optimizer_lab"] is True
    assert built["runtime"]["capture_delta_codec"] is True
    assert built["runtime"]["lineage_tags"]["team"] == "graph"
    assert built["runtime"]["tracing"]["preview_redaction_mode"] == "DenyList"
    assert built["runtime"]["tracing"]["preview_redaction_token"] == "[MASKED]"
    assert built["runtime"]["tracing"]["otlp_protocol"] == "http/protobuf"

"""Integration smoke tests for Rust codeanatomy_engine bindings."""

from __future__ import annotations

import json

import pytest


@pytest.mark.integration
def test_rust_engine_compile_and_materialize_boundary() -> None:
    """Rust API can compile spec JSON and returns runtime errors with typed boundaries."""
    codeanatomy_engine = pytest.importorskip("codeanatomy_engine")
    input_relations: list[dict[str, object]] = []
    view_dependencies: list[str] = []
    join_edges: list[dict[str, object]] = []
    join_constraints: list[dict[str, object]] = []
    rule_intents: list[dict[str, object]] = []
    parameter_templates: list[dict[str, object]] = []

    spec_payload = {
        "version": 1,
        "input_relations": input_relations,
        "view_definitions": [
            {
                "name": "v1",
                "view_kind": "project",
                "view_dependencies": view_dependencies,
                "transform": {"kind": "Project", "source": "missing_source", "columns": ["id"]},
                "output_schema": {"columns": {"id": "Int64"}},
            }
        ],
        "join_graph": {"edges": join_edges, "constraints": join_constraints},
        "output_targets": [
            {
                "table_name": "out",
                "source_view": "v1",
                "columns": ["id"],
                "materialization_mode": "Overwrite",
            }
        ],
        "rule_intents": rule_intents,
        "rulepack_profile": "Default",
        "parameter_templates": parameter_templates,
    }
    spec_json = json.dumps(spec_payload)

    factory = codeanatomy_engine.SessionFactory.from_class("small")
    compiler = codeanatomy_engine.SemanticPlanCompiler()
    compiled = compiler.compile(spec_json)
    materializer = codeanatomy_engine.CpgMaterializer()

    with pytest.raises(RuntimeError):
        materializer.execute(factory, compiled)

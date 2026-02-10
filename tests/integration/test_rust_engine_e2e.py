"""Integration smoke tests for Rust codeanatomy_engine bindings."""

from __future__ import annotations

import json
from pathlib import Path

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
    typed_parameters: list[dict[str, object]] = []

    spec_payload = {
        "version": 3,
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
        "typed_parameters": typed_parameters,
    }
    spec_json = json.dumps(spec_payload)

    factory = codeanatomy_engine.SessionFactory.from_class("small")
    compiler = codeanatomy_engine.SemanticPlanCompiler()
    compiled = compiler.compile(spec_json)
    materializer = codeanatomy_engine.CpgMaterializer()

    with pytest.raises(RuntimeError):
        materializer.execute(factory, compiled)


@pytest.mark.integration
def test_rust_engine_exports_all_classes() -> None:
    """All expected Rust engine classes are importable."""
    codeanatomy_engine = pytest.importorskip("codeanatomy_engine")
    assert hasattr(codeanatomy_engine, "SessionFactory")
    assert hasattr(codeanatomy_engine, "SemanticPlanCompiler")
    assert hasattr(codeanatomy_engine, "CpgMaterializer")


def _successful_spec(input_location: str, output_location: str) -> dict[str, object]:
    return {
        "version": 3,
        "input_relations": [
            {
                "logical_name": "input",
                "delta_location": input_location,
                "requires_lineage": False,
                "version_pin": None,
            }
        ],
        "view_definitions": [
            {
                "name": "v1",
                "view_kind": "project",
                "view_dependencies": [],
                "transform": {"kind": "Project", "source": "input", "columns": ["id"]},
                "output_schema": {"columns": {"id": "Int64"}},
            }
        ],
        "join_graph": {"edges": [], "constraints": []},
        "output_targets": [
            {
                "table_name": "out_delta",
                "delta_location": output_location,
                "source_view": "v1",
                "columns": ["id"],
                "materialization_mode": "Overwrite",
            }
        ],
        "rule_intents": [],
        "rulepack_profile": "Default",
        "typed_parameters": [],
        "runtime": {"compliance_capture": True},
    }


@pytest.mark.integration
def test_rust_engine_identity_surfaces_stable_across_runs(tmp_path: Path) -> None:
    """Repeated Python materialization preserves envelope/planning/provider identity surfaces."""
    engine = pytest.importorskip("codeanatomy_engine")
    pyarrow = pytest.importorskip("pyarrow")
    write_deltalake = getattr(pytest.importorskip("deltalake"), "write_deltalake", None)
    if write_deltalake is None:
        pytest.skip("deltalake.write_deltalake is unavailable in this environment")

    input_location = tmp_path / "input_delta"
    output_location = tmp_path / "output_delta"
    write_deltalake(
        str(input_location),
        pyarrow.table({"id": pyarrow.array([1, 2, 3], type=pyarrow.int64())}),
        mode="overwrite",
    )

    factory = engine.SessionFactory.from_class("small")
    compiled = engine.SemanticPlanCompiler().compile(
        json.dumps(_successful_spec(str(input_location), str(output_location)))
    )
    materializer = engine.CpgMaterializer()

    runs = [materializer.execute(factory, compiled).to_dict() for _ in range(2)]
    assert runs[0]["envelope_hash"] == runs[1]["envelope_hash"]

    bundles = [run.get("plan_bundles", []) for run in runs]
    assert bundles[0], "compliance capture should emit at least one plan bundle"
    assert bundles[1], "compliance capture should emit at least one plan bundle"
    assert len(bundles[0]) == len(bundles[1])

    assert bundles[0][0]["planning_surface_hash"] == bundles[1][0]["planning_surface_hash"]
    assert bundles[0][0]["provider_identities"] == bundles[1][0]["provider_identities"]
    assert bundles[0][0]["provider_identities"] == sorted(
        bundles[0][0]["provider_identities"],
        key=lambda identity: identity["table_name"],
    )

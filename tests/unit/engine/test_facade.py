"""Tests for engine Rust facade compatibility behavior."""

from __future__ import annotations

import json
import types
from dataclasses import dataclass

import pytest

from engine.facade import execute_cpg_build
from engine.spec_builder import (
    FilterTransform,
    InputRelation,
    JoinGraph,
    OutputTarget,
    SchemaContract,
    SemanticExecutionSpec,
    ViewDefinition,
)


@dataclass(slots=True)
class _FakeCompiledPlan:
    spec_json: str


class _FakeRunResult:
    def to_json(self) -> str:
        return json.dumps({"status": "ok", "engine": "rust"})


class _FakeSessionFactory:
    @staticmethod
    def from_class(environment_class: str) -> _FakeSessionFactory:
        assert environment_class == "medium"
        return _FakeSessionFactory()


class _FakeSemanticPlanCompiler:
    last_spec_json: str | None = None

    def compile(self, spec_json: str) -> _FakeCompiledPlan:
        _FakeSemanticPlanCompiler.last_spec_json = spec_json
        return _FakeCompiledPlan(spec_json)


class _FakeMaterializer:
    def execute(
        self,
        session_factory: _FakeSessionFactory,
        compiled_plan: _FakeCompiledPlan,
    ) -> _FakeRunResult:
        assert isinstance(session_factory, _FakeSessionFactory)
        assert isinstance(compiled_plan, _FakeCompiledPlan)
        return _FakeRunResult()


def _minimal_spec() -> SemanticExecutionSpec:
    return SemanticExecutionSpec(
        version=3,
        input_relations=(
            InputRelation(
                logical_name="raw_nodes", delta_location="/tmp/old", requires_lineage=False
            ),
        ),
        view_definitions=(
            ViewDefinition(
                name="nodes_view",
                view_kind="filter",
                view_dependencies=(),
                transform=FilterTransform(source="raw_nodes", predicate="TRUE"),
                output_schema=SchemaContract(),
            ),
        ),
        join_graph=JoinGraph(),
        output_targets=(
            OutputTarget(
                table_name="nodes_view",
                delta_location="/tmp/out_nodes",
                source_view="nodes_view",
                columns=(),
                materialization_mode="Overwrite",
            ),
        ),
        rule_intents=(),
        rulepack_profile="Default",
    )


def test_execute_cpg_build_updates_input_locations(monkeypatch: pytest.MonkeyPatch) -> None:
    """Facade applies extraction input overrides before Rust compilation."""
    fake_module = types.SimpleNamespace(
        SessionFactory=_FakeSessionFactory,
        SemanticPlanCompiler=_FakeSemanticPlanCompiler,
        CpgMaterializer=_FakeMaterializer,
    )
    monkeypatch.setitem(__import__("sys").modules, "codeanatomy_engine", fake_module)

    result = execute_cpg_build(
        extraction_inputs={"raw_nodes": "/tmp/new", "extra_input": "/tmp/extra"},
        semantic_spec=_minimal_spec(),
        environment_class="medium",
    )

    assert result["status"] == "ok"
    assert _FakeSemanticPlanCompiler.last_spec_json is not None
    payload = json.loads(_FakeSemanticPlanCompiler.last_spec_json)
    relation_map = {
        row["logical_name"]: row["delta_location"] for row in payload["input_relations"]
    }
    assert relation_map["raw_nodes"] == "/tmp/new"
    assert relation_map["extra_input"] == "/tmp/extra"


def test_execute_cpg_build_raises_with_missing_extension(monkeypatch: pytest.MonkeyPatch) -> None:
    """Facade raises a clear ImportError when the extension is unavailable."""
    monkeypatch.delitem(__import__("sys").modules, "codeanatomy_engine", raising=False)
    with pytest.raises(ImportError, match="codeanatomy_engine Rust extension not built"):
        execute_cpg_build(
            extraction_inputs={},
            semantic_spec=_minimal_spec(),
            environment_class="medium",
        )

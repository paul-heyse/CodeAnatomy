"""E2E-style tests for CLI plan command using stubbed engine bindings."""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from cli.commands.plan import PlanOptions, plan_command
from planning_engine.output_contracts import ENGINE_CPG_OUTPUTS
from planning_engine.spec_builder import (
    FilterTransform,
    JoinGraph,
    OutputTarget,
    RulepackProfile,
    RuntimeConfig,
    SchemaContract,
    SemanticExecutionSpec,
    ViewDefinition,
)


def _spec_fixture(repo_root: Path, rulepack_profile: RulepackProfile) -> SemanticExecutionSpec:
    output_locations = {name: str(repo_root / "build" / name) for name in ENGINE_CPG_OUTPUTS}
    return SemanticExecutionSpec(
        version=1,
        input_relations=(),
        view_definitions=(
            ViewDefinition(
                name="cpg_nodes_view",
                view_kind="filter",
                view_dependencies=(),
                transform=FilterTransform(source="repo_files_v1", predicate="TRUE"),
                output_schema=SchemaContract(),
            ),
            ViewDefinition(
                name="cpg_edges_view",
                view_kind="filter",
                view_dependencies=("cpg_nodes_view",),
                transform=FilterTransform(source="repo_files_v1", predicate="TRUE"),
                output_schema=SchemaContract(),
            ),
        ),
        join_graph=JoinGraph(edges=(), constraints=()),
        output_targets=tuple(
            OutputTarget(
                table_name=name,
                source_view="cpg_nodes_view",
                columns=(),
                delta_location=output_locations[name],
            )
            for name in ENGINE_CPG_OUTPUTS
        ),
        rule_intents=(),
        rulepack_profile=rulepack_profile,
        typed_parameters=(),
        runtime=RuntimeConfig(),
        spec_hash=b"",
    )


def _wire_plan_command_stubs(
    monkeypatch: pytest.MonkeyPatch,
    *,
    repo_root: Path,
) -> None:
    def _resolve_runtime_profile_stub(_profile: str) -> SimpleNamespace:
        return SimpleNamespace(name="stub_profile", runtime_profile_hash="stub_hash")

    monkeypatch.setattr(
        "planning_engine.runtime_profile.resolve_runtime_profile",
        _resolve_runtime_profile_stub,
    )

    def _build_semantic_ir_stub() -> object:
        return object()

    monkeypatch.setattr("semantics.ir_pipeline.build_semantic_ir", _build_semantic_ir_stub)

    def _build_execution_spec_stub(
        *,
        ir: object,
        input_locations: dict[str, str],
        output_targets: list[str],
        rulepack_profile: RulepackProfile,
        output_locations: dict[str, str],
        runtime_config: RuntimeConfig | None = None,
    ) -> SemanticExecutionSpec:
        _ = (ir, input_locations, output_targets, output_locations, runtime_config)
        return _spec_fixture(repo_root, rulepack_profile)

    monkeypatch.setattr(
        "planning_engine.spec_builder.build_execution_spec", _build_execution_spec_stub
    )

    class _FakeSessionFactory:
        @staticmethod
        def from_class(_profile: str) -> object:
            return object()

    class _FakeCompiledPlan:
        @staticmethod
        def spec_hash_hex() -> str:
            return "feedbeef"

    class _FakeSemanticPlanCompiler:
        def compile(self, _spec_json: str) -> object:
            return _FakeCompiledPlan()

        def compile_metadata_json(self, _session_factory: object, _spec_json: str) -> str:
            return json.dumps(
                {
                    "spec_hash": "feedbeef",
                    "dependency_map": {"cpg_edges_view": ["cpg_nodes_view"]},
                    "task_schedule": {
                        "execution_order": ["cpg_nodes_view", "cpg_edges_view"],
                        "critical_path": ["cpg_nodes_view", "cpg_edges_view"],
                    },
                }
            )

    engine_module = SimpleNamespace(
        SessionFactory=_FakeSessionFactory,
        SemanticPlanCompiler=_FakeSemanticPlanCompiler,
    )

    def _import_module_stub(_name: str) -> object:
        return engine_module

    monkeypatch.setattr("cli.commands.plan.importlib.import_module", _import_module_stub)


@pytest.mark.e2e
def test_plan_text_output(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    _wire_plan_command_stubs(monkeypatch, repo_root=tmp_path)

    exit_code = plan_command(
        repo_root=tmp_path,
        options=PlanOptions(output_format="text", engine_profile="small"),
    )

    assert exit_code == 0
    output = capsys.readouterr().out
    assert "plan_signature: feedbeef" in output
    assert "engine_profile: small" in output
    assert "runtime_profile_name: small" in output
    assert "runtime_profile_hash: rust_session_factory" in output


@pytest.mark.e2e
def test_plan_json_output(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    _wire_plan_command_stubs(monkeypatch, repo_root=tmp_path)

    exit_code = plan_command(
        repo_root=tmp_path,
        options=PlanOptions(output_format="json", engine_profile="medium"),
    )

    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["plan_signature"] == "feedbeef"
    assert payload["engine_profile"] == "medium"
    assert payload["runtime_profile_name"] == "medium"
    assert payload["runtime_profile_hash"] == "rust_session_factory"
    assert payload["output_targets"] == list(ENGINE_CPG_OUTPUTS)


@pytest.mark.e2e
def test_plan_dot_output(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    _wire_plan_command_stubs(monkeypatch, repo_root=tmp_path)

    exit_code = plan_command(
        repo_root=tmp_path,
        options=PlanOptions(output_format="dot"),
    )

    assert exit_code == 0
    output = capsys.readouterr().out
    assert "digraph execution_plan {" in output
    assert '"cpg_nodes_view"' in output


@pytest.mark.e2e
def test_plan_tier1_fields_present(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _wire_plan_command_stubs(monkeypatch, repo_root=tmp_path)

    exit_code = plan_command(
        repo_root=tmp_path,
        options=PlanOptions(output_format="json", engine_profile="large"),
    )

    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out)
    assert {"plan_signature", "view_count", "output_targets"} <= set(payload)
    assert payload["engine_profile"] == "large"

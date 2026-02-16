"""E2E-style tests for CLI plan command using stubbed engine bindings."""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import msgspec
import pytest

from cli.commands.plan import PlanOptions, plan_command
from planning_engine.output_contracts import ENGINE_CPG_OUTPUTS
from planning_engine.spec_contracts import (
    JoinGraph,
    OutputTarget,
    RulepackProfile,
    RuntimeConfig,
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
                transform={"kind": "Filter", "source": "repo_files_v1", "predicate": "TRUE"},
                output_schema={"columns": {}},
            ),
            ViewDefinition(
                name="cpg_edges_view",
                view_kind="filter",
                view_dependencies=("cpg_nodes_view",),
                transform={"kind": "Filter", "source": "repo_files_v1", "predicate": "TRUE"},
                output_schema={"columns": {}},
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
    def _build_semantic_ir_stub() -> object:
        join_groups: list[dict[str, object]] = []
        return {
            "views": [
                {"name": "cpg_nodes_view", "kind": "filter", "inputs": ["repo_files_v1"]},
                {
                    "name": "cpg_edges_view",
                    "kind": "filter",
                    "inputs": ["repo_files_v1", "cpg_nodes_view"],
                },
            ],
            "join_groups": join_groups,
        }

    monkeypatch.setattr("semantics.ir_pipeline.build_semantic_ir", _build_semantic_ir_stub)

    class _FakeSessionFactory:
        @staticmethod
        def from_class(_profile: str) -> object:
            return object()

    class _FakeCompiledPlan:
        @staticmethod
        def spec_hash_hex() -> str:
            return "feedbeef"

    class _FakeSemanticPlanCompiler:
        def build_spec_json(self, _semantic_ir_json: str, _request_json: str) -> str:
            spec = _spec_fixture(repo_root, "Default")
            return msgspec.json.encode(spec).decode()

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
    """Test plan text output."""
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
    """Test plan json output."""
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
    """Test plan dot output."""
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
    """Test plan tier1 fields present."""
    _wire_plan_command_stubs(monkeypatch, repo_root=tmp_path)

    exit_code = plan_command(
        repo_root=tmp_path,
        options=PlanOptions(output_format="json", engine_profile="large"),
    )

    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out)
    assert {"plan_signature", "view_count", "output_targets"} <= set(payload)
    assert payload["engine_profile"] == "large"

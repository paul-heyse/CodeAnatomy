"""Unit tests for CLI plan command runtime wiring."""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import msgspec
import pytest

from cli.commands.plan import PlanOptions, plan_command
from planning_engine.spec_contracts import (
    JoinGraph,
    OutputTarget,
    RulepackProfile,
    RuntimeConfig,
    SemanticExecutionSpec,
    ViewDefinition,
)


def _spec_fixture(
    *,
    output_targets: tuple[str, ...],
    output_locations: dict[str, str],
    rulepack_profile: RulepackProfile,
) -> SemanticExecutionSpec:
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
        ),
        join_graph=JoinGraph(edges=(), constraints=()),
        output_targets=tuple(
            OutputTarget(
                table_name=target,
                source_view="cpg_nodes_view",
                columns=(),
                delta_location=output_locations[target],
            )
            for target in output_targets
        ),
        rule_intents=(),
        rulepack_profile=rulepack_profile,
        typed_parameters=(),
        runtime=RuntimeConfig(),
        spec_hash=b"",
    )


def _configure_plan_test_doubles(
    monkeypatch: pytest.MonkeyPatch,
    *,
    fail_session: bool = False,
) -> type:
    def _build_semantic_ir_stub() -> object:
        join_groups: list[dict[str, object]] = []
        return {
            "views": [
                {"name": "cpg_nodes_view", "kind": "filter", "inputs": ["repo_files_v1"]},
            ],
            "join_groups": join_groups,
        }

    monkeypatch.setattr("semantics.ir_pipeline.build_semantic_ir", _build_semantic_ir_stub)

    class _FakeSessionFactory:
        last_profile: str | None = None

        @staticmethod
        def from_class(profile: str) -> object:
            _FakeSessionFactory.last_profile = profile
            if fail_session:
                msg = "bad session profile"
                raise RuntimeError(msg)
            return object()

    class _FakeCompiledPlan:
        @staticmethod
        def spec_hash_hex() -> str:
            return "abc123"

    class _FakeSemanticPlanCompiler:
        def build_spec_json(self, _semantic_ir_json: str, _request_json: str) -> str:
            targets = ("cpg_nodes", "cpg_edges", "cpg_symbols", "cpg_spans", "cpg_diagnostics")
            spec = _spec_fixture(
                output_targets=targets,
                output_locations={target: f"/tmp/{target}" for target in targets},
                rulepack_profile="Default",
            )
            return msgspec.json.encode(spec).decode()

        def compile(self, spec_json: str) -> object:
            _ = spec_json
            return _FakeCompiledPlan()

        def compile_metadata_json(self, _session_factory: object, _spec_json: str) -> str:
            return json.dumps(
                {
                    "spec_hash": "abc123",
                    "dependency_map": {"cpg_nodes_view": []},
                    "task_schedule": {
                        "execution_order": ["cpg_nodes_view"],
                        "critical_path": ["cpg_nodes_view"],
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
    return _FakeSessionFactory


def test_plan_command_uses_engine_profile_and_emits_runtime_metadata(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Test plan command uses engine profile and emits runtime metadata."""
    fake_session_factory = _configure_plan_test_doubles(monkeypatch)

    exit_code = plan_command(
        repo_root=tmp_path,
        options=PlanOptions(output_format="json", engine_profile="small"),
    )

    assert exit_code == 0
    assert fake_session_factory.last_profile == "small"

    payload = json.loads(capsys.readouterr().out)
    assert payload["engine_profile"] == "small"
    assert payload["runtime_profile_name"] == "small"
    assert payload["runtime_profile_hash"] == "rust_session_factory"


def test_plan_command_fails_when_session_initialization_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Test plan command fails when session initialization fails."""
    _configure_plan_test_doubles(monkeypatch, fail_session=True)

    exit_code = plan_command(
        repo_root=tmp_path,
        options=PlanOptions(output_format="json", engine_profile="medium"),
    )

    assert exit_code == 1
    assert "Plan runtime initialization failed" in capsys.readouterr().err

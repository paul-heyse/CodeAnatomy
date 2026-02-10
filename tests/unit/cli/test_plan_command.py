"""Unit tests for CLI plan command runtime wiring."""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from cli.commands.plan import PlanOptions, plan_command
from engine.spec_builder import (
    FilterTransform,
    JoinGraph,
    OutputTarget,
    RulepackProfile,
    RuntimeConfig,
    SchemaContract,
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
                transform=FilterTransform(source="repo_files_v1", predicate="TRUE"),
                output_schema=SchemaContract(),
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
    def _resolve_runtime_profile_stub(_profile: str) -> SimpleNamespace:
        return SimpleNamespace(name="resolved_profile", runtime_profile_hash="hash_123")

    monkeypatch.setattr(
        "engine.runtime_profile.resolve_runtime_profile",
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
        _ = (ir, input_locations, runtime_config)
        return _spec_fixture(
            output_targets=tuple(output_targets),
            output_locations=output_locations,
            rulepack_profile=rulepack_profile,
        )

    monkeypatch.setattr("engine.spec_builder.build_execution_spec", _build_execution_spec_stub)

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
        def compile(self, spec_json: str) -> object:
            _ = spec_json
            return _FakeCompiledPlan()

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
    fake_session_factory = _configure_plan_test_doubles(monkeypatch)

    exit_code = plan_command(
        repo_root=tmp_path,
        options=PlanOptions(output_format="json", engine_profile="small"),
    )

    assert exit_code == 0
    assert fake_session_factory.last_profile == "small"

    payload = json.loads(capsys.readouterr().out)
    assert payload["engine_profile"] == "small"
    assert payload["runtime_profile_name"] == "resolved_profile"
    assert payload["runtime_profile_hash"] == "hash_123"


def test_plan_command_fails_when_session_initialization_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _configure_plan_test_doubles(monkeypatch, fail_session=True)

    exit_code = plan_command(
        repo_root=tmp_path,
        options=PlanOptions(output_format="json", engine_profile="medium"),
    )

    assert exit_code == 1
    assert "Plan runtime initialization failed" in capsys.readouterr().err

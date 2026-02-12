"""Output contract tests for the engine execution pipeline.

Verify that output contracts are consistent and complete.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from graph.build_pipeline import BuildResult
from graph.product_build import GraphProductBuildRequest, _parse_build_result
from obs.engine_artifacts import record_engine_execution_summary, record_engine_plan_summary
from planning_engine.output_contracts import (
    CANONICAL_CPG_OUTPUTS,
    ENGINE_CPG_OUTPUTS,
    FULL_PIPELINE_OUTPUTS,
    LEGACY_CPG_OUTPUTS,
    ORCHESTRATOR_OUTPUTS,
    OUTPUT_SOURCE_MAP,
    OUTPUT_SOURCE_MAP_WITH_ALIASES,
    PYTHON_AUXILIARY_OUTPUTS,
    canonical_cpg_output_name,
    legacy_cpg_output_name,
)
from planning_engine.spec_contracts import (
    InputRelation,
    JoinGraph,
    OutputTarget,
    RuleIntent,
    RuntimeConfig,
    SemanticExecutionSpec,
    ViewDefinition,
)


class TestOutputContractConstants:
    """Verify output contract constant integrity."""

    def test_full_pipeline_outputs_count(self) -> None:
        """FULL_PIPELINE_OUTPUTS has exactly 10 entries."""
        assert len(FULL_PIPELINE_OUTPUTS) == 10

    def test_full_pipeline_outputs_exact_keys(self) -> None:
        """FULL_PIPELINE_OUTPUTS matches the canonical output names."""
        expected = {
            "cpg_nodes",
            "cpg_edges",
            "cpg_props",
            "cpg_props_map",
            "cpg_edges_by_src",
            "cpg_edges_by_dst",
            "normalize_outputs_delta",
            "extract_error_artifacts_delta",
            "run_manifest_delta",
            "run_bundle_dir",
        }
        assert set(FULL_PIPELINE_OUTPUTS) == expected

    def test_engine_cpg_outputs_count(self) -> None:
        """ENGINE_CPG_OUTPUTS has exactly 6 Rust-producible entries."""
        assert len(ENGINE_CPG_OUTPUTS) == 6

    def test_python_auxiliary_outputs_count(self) -> None:
        """PYTHON_AUXILIARY_OUTPUTS has exactly 3 Python-side entries."""
        assert len(PYTHON_AUXILIARY_OUTPUTS) == 3

    def test_orchestrator_outputs_count(self) -> None:
        """ORCHESTRATOR_OUTPUTS has exactly 1 orchestrator entry."""
        assert len(ORCHESTRATOR_OUTPUTS) == 1

    def test_no_duplicate_outputs(self) -> None:
        """No output name appears in more than one source category."""
        all_outputs = ENGINE_CPG_OUTPUTS + PYTHON_AUXILIARY_OUTPUTS + ORCHESTRATOR_OUTPUTS
        assert len(all_outputs) == len(set(all_outputs))

    def test_full_equals_union(self) -> None:
        """FULL_PIPELINE_OUTPUTS is the union of all source categories."""
        union = set(ENGINE_CPG_OUTPUTS) | set(PYTHON_AUXILIARY_OUTPUTS) | set(ORCHESTRATOR_OUTPUTS)
        assert set(FULL_PIPELINE_OUTPUTS) == union

    def test_output_source_map_covers_all(self) -> None:
        """OUTPUT_SOURCE_MAP has an entry for every output."""
        assert set(OUTPUT_SOURCE_MAP.keys()) == set(FULL_PIPELINE_OUTPUTS)

    def test_output_source_map_values(self) -> None:
        """OUTPUT_SOURCE_MAP values are valid source names."""
        valid_sources = {"rust_engine", "python_auxiliary", "orchestrator"}
        assert set(OUTPUT_SOURCE_MAP.values()).issubset(valid_sources)

    def test_legacy_aliases_present_for_all_cpg_outputs(self) -> None:
        """Every canonical CPG output has one legacy alias."""
        assert len(CANONICAL_CPG_OUTPUTS) == len(LEGACY_CPG_OUTPUTS) == len(ENGINE_CPG_OUTPUTS)

    def test_output_source_map_with_aliases_covers_legacy(self) -> None:
        """Compatibility source map includes legacy CPG aliases."""
        for name in LEGACY_CPG_OUTPUTS:
            assert OUTPUT_SOURCE_MAP_WITH_ALIASES[name] == "rust_engine"

    def test_name_conversion_helpers(self) -> None:
        """Canonical/legacy helper conversion is invertible for CPG outputs."""
        for canonical in CANONICAL_CPG_OUTPUTS:
            legacy = legacy_cpg_output_name(canonical)
            assert canonical_cpg_output_name(legacy) == canonical


def _finalize_payload(path: str) -> dict[str, object]:
    return {
        "path": path,
        "rows": 0,
        "error_rows": 0,
        "paths": {
            "data": path,
            "errors": f"{path}/_errors",
            "stats": f"{path}/_stats",
            "alignment": f"{path}/_alignment",
        },
    }


def _build_result_fixture(base_dir: str) -> BuildResult:
    return BuildResult(
        cpg_outputs={
            "cpg_nodes": _finalize_payload(f"{base_dir}/cpg_nodes"),
            "cpg_edges": _finalize_payload(f"{base_dir}/cpg_edges"),
            "cpg_props": _finalize_payload(f"{base_dir}/cpg_props"),
            "cpg_props_map": {"path": f"{base_dir}/cpg_props_map", "rows": 0},
            "cpg_edges_by_src": {"path": f"{base_dir}/cpg_edges_by_src", "rows": 0},
            "cpg_edges_by_dst": {"path": f"{base_dir}/cpg_edges_by_dst", "rows": 0},
        },
        auxiliary_outputs={},
        run_result={},
        extraction_timing={},
        warnings=[],
    )


def _spec_fixture() -> SemanticExecutionSpec:
    return SemanticExecutionSpec(
        version=1,
        input_relations=(InputRelation(logical_name="repo_files_v1", delta_location="/tmp/repo"),),
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
        output_targets=(
            OutputTarget(
                table_name="cpg_nodes",
                source_view="cpg_nodes_view",
                columns=(),
                delta_location="/tmp/cpg_nodes",
            ),
        ),
        rule_intents=(RuleIntent(name="semantic_integrity", rule_class="SemanticIntegrity"),),
        rulepack_profile="Default",
        typed_parameters=(),
        runtime=RuntimeConfig(),
        spec_hash=b"",
    )


class TestRunResultMapping:
    """RunResult and BuildResult mapping contract tests."""

    def test_run_result_to_build_result(self, tmp_path: Path) -> None:
        """Map BuildResult payloads to the public GraphProductBuildResult contract."""
        request = GraphProductBuildRequest(
            repo_root=tmp_path,
            include_extract_errors=False,
            include_manifest=False,
            include_run_bundle=False,
        )
        parsed = _parse_build_result(
            request=request,
            repo_root=tmp_path,
            build_result=_build_result_fixture(str(tmp_path / "build")),
        )
        assert parsed.cpg_nodes.paths.data.name == "cpg_nodes"
        assert parsed.cpg_edges.paths.data.name == "cpg_edges"
        assert parsed.cpg_props_map.path.name == "cpg_props_map"
        assert parsed.cpg_edges_by_dst.path.name == "cpg_edges_by_dst"

    def test_run_result_contains_all_engine_outputs(self) -> None:
        """Ensure BuildResult contains the full canonical CPG output set."""
        fixture = _build_result_fixture("/tmp/build")
        for key in ENGINE_CPG_OUTPUTS:
            assert key in fixture.cpg_outputs


class TestRustBoundaryContracts:
    """Boundary behavior contracts for graph.build_pipeline."""

    def test_execute_engine_phase_preserves_typed_engine_error(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Typed boundary errors propagate without Python re-wrapping."""
        from graph import build_pipeline as build_pipeline_mod

        class _TypedEngineError(Exception):
            def __init__(self) -> None:
                super().__init__("typed failure")
                self.stage = "runtime"
                self.code = "RUN_BUILD_EXECUTION_FAILED"
                self.details = {"cause": "unit-test"}

        class _FakeEngineModule:
            @staticmethod
            def run_build(_request_json: str) -> dict[str, object]:
                raise _TypedEngineError

        monkeypatch.setattr(
            build_pipeline_mod.importlib,
            "import_module",
            lambda _name: _FakeEngineModule(),
        )

        with pytest.raises(_TypedEngineError) as exc_info:
            build_pipeline_mod._execute_engine_phase(  # noqa: SLF001
                semantic_input_locations={},
                spec=_spec_fixture(),
                engine_profile="small",
            )
        assert exc_info.value.stage == "runtime"
        assert exc_info.value.code == "RUN_BUILD_EXECUTION_FAILED"

    def test_execute_engine_phase_uses_dict_native_run_payload(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Engine response contracts are consumed from dict-native payloads."""
        from graph import build_pipeline as build_pipeline_mod

        class _FakeEngineModule:
            @staticmethod
            def run_build(_request_json: str) -> dict[str, object]:
                return {"run_result": {"spec_hash": "abc123", "outputs": []}}

        monkeypatch.setattr(
            build_pipeline_mod.importlib,
            "import_module",
            lambda _name: _FakeEngineModule(),
        )

        payload = build_pipeline_mod._execute_engine_phase(  # noqa: SLF001
            semantic_input_locations={},
            spec=_spec_fixture(),
            engine_profile="small",
        )
        run_result, artifacts = payload
        assert run_result["spec_hash"] == "abc123"
        assert run_result["outputs"] == []
        assert artifacts == {}

    def test_execute_engine_phase_returns_artifact_contract(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Engine response artifacts are returned to orchestrator callers."""
        from graph import build_pipeline as build_pipeline_mod

        class _FakeEngineModule:
            @staticmethod
            def run_build(_request_json: str) -> dict[str, object]:
                return {
                    "run_result": {"outputs": []},
                    "artifacts": {
                        "manifest_path": "/tmp/build/run_manifest",
                        "run_bundle_dir": "/tmp/build/run_bundle",
                    },
                }

        monkeypatch.setattr(
            build_pipeline_mod.importlib,
            "import_module",
            lambda _name: _FakeEngineModule(),
        )

        run_result, artifacts = build_pipeline_mod._execute_engine_phase(  # noqa: SLF001
            semantic_input_locations={},
            spec=_spec_fixture(),
            engine_profile="small",
        )
        assert run_result["outputs"] == []
        assert artifacts["manifest_path"] == "/tmp/build/run_manifest"
        assert artifacts["run_bundle_dir"] == "/tmp/build/run_bundle"


class TestCLIOutputContracts:
    """Contract checks for CLI-facing structured output payloads."""

    def test_build_output_report_fields(self) -> None:
        """Validate required fields in structured per-table output reports."""
        payload = _finalize_payload("/tmp/build/cpg_nodes")
        assert {"path", "rows", "error_rows", "paths"} <= set(payload)
        paths = payload["paths"]
        assert isinstance(paths, dict)
        assert {"data", "errors", "stats", "alignment"} <= set(paths)

    def test_plan_targets_match_engine_contract(self) -> None:
        """Lock the plan-command output target list to canonical engine names."""
        assert list(ENGINE_CPG_OUTPUTS) == [
            "cpg_nodes",
            "cpg_edges",
            "cpg_props",
            "cpg_props_map",
            "cpg_edges_by_src",
            "cpg_edges_by_dst",
        ]


class TestDiagnosticArtifactContracts:
    """Contract checks for diagnostics artifact payload schemas."""

    def test_plan_summary_artifact_schema(self) -> None:
        """Verify required fields exist on EnginePlanSummaryArtifact."""
        summary = record_engine_plan_summary(_spec_fixture())
        assert {"spec_hash", "view_count", "join_edge_count"} <= set(summary.__struct_fields__)

    def test_execution_summary_artifact_schema(self) -> None:
        """Verify required fields exist on EngineExecutionSummaryArtifact."""
        execution_summary = record_engine_execution_summary(
            {
                "spec_hash": bytes.fromhex("10" * 32),
                "envelope_hash": bytes.fromhex("20" * 32),
                "outputs": [{"table_name": "cpg_nodes", "rows_written": 1}],
                "trace_metrics_summary": {},
                "warnings": [],
            }
        )
        assert {
            "spec_hash",
            "envelope_hash",
            "tables_materialized",
            "total_rows_written",
        } <= set(execution_summary.__struct_fields__)

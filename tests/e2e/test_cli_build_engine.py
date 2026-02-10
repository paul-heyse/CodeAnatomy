"""E2E test: CLI build command through engine-native path."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from cli.commands.build import BuildOptions, BuildRequestOptions, build_command
from cli.context import RunContext
from engine.build_orchestrator import BuildResult


def _stub_build_result(output_dir: Path) -> BuildResult:
    def _finalize_payload(name: str) -> dict[str, object]:
        data_path = output_dir / name
        return {
            "path": str(data_path),
            "rows": 0,
            "error_rows": 0,
            "paths": {
                "data": str(data_path),
                "errors": str(data_path / "_errors"),
                "stats": str(data_path / "_stats"),
                "alignment": str(data_path / "_alignment"),
            },
        }

    return BuildResult(
        cpg_outputs={
            "cpg_nodes": _finalize_payload("cpg_nodes"),
            "cpg_edges": _finalize_payload("cpg_edges"),
            "cpg_props": _finalize_payload("cpg_props"),
            "cpg_props_map": {"path": str(output_dir / "cpg_props_map"), "rows": 0},
            "cpg_edges_by_src": {"path": str(output_dir / "cpg_edges_by_src"), "rows": 0},
            "cpg_edges_by_dst": {"path": str(output_dir / "cpg_edges_by_dst"), "rows": 0},
        },
        auxiliary_outputs={},
        run_result={},
        extraction_timing={},
        warnings=[],
    )


@pytest.mark.e2e
def test_build_command_smoke_with_stubbed_orchestrator(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    (repo_root / "pkg.py").write_text("def f() -> int:\n    return 1\n", encoding="utf-8")
    observed: dict[str, Any] = {}

    def _orchestrate_build_stub(*args: Any, **kwargs: Any) -> BuildResult:
        _ = args
        observed.update(kwargs)
        return _stub_build_result(kwargs["output_dir"])

    monkeypatch.setattr("engine.build_orchestrator.orchestrate_build", _orchestrate_build_stub)

    exit_code = build_command(
        repo_root=repo_root,
        request=BuildRequestOptions(),
        options=BuildOptions(engine_profile="small", enable_tree_sitter=False),
        run_context=RunContext(run_id="test-run", log_level="INFO", config_contents={}),
    )

    assert exit_code == 0
    assert observed["engine_profile"] == "small"
    extraction_config = observed["extraction_config"]
    assert isinstance(extraction_config, dict)
    assert extraction_config["enable_tree_sitter"] is False

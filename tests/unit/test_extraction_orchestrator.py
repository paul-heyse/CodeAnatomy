"""Unit tests for the extraction orchestrator."""

from __future__ import annotations

from contextlib import nullcontext
from pathlib import Path

import pyarrow as pa
import pytest

from extraction import orchestrator as orchestrator_mod
from extraction.options import ExtractionRunOptions


def _sample_table(label: str) -> pa.Table:
    return pa.table({"name": [label], "value": [1]})


def _wire_test_doubles(
    monkeypatch: pytest.MonkeyPatch,
    *,
    stage1_extractors: dict[str, object],
    calls: list[str],
    stage1_error: str | None = None,
) -> None:
    repo_scan_outputs = {
        "repo_files_v1": _sample_table("repo"),
        "file_line_index_v1": _sample_table("line_index"),
    }

    def _run_repo_scan(_repo_root: Path, *, options: object) -> dict[str, pa.Table]:
        _ = options
        calls.append("repo_scan")
        return repo_scan_outputs

    def _build_stage1_extractors(**kwargs: object) -> dict[str, object]:
        repo_files = kwargs.get("repo_files")
        assert isinstance(repo_files, pa.Table)
        return stage1_extractors

    def _run_python_imports(_delta_locations: dict[str, str]) -> pa.Table:
        calls.append("python_imports")
        return _sample_table("python_imports")

    def _run_python_external(_delta_locations: dict[str, str], _repo_root: Path) -> pa.Table:
        calls.append("python_external")
        return _sample_table("python_external")

    def _write_delta(_table: pa.Table, location: Path, _name: str) -> str:
        return str(location)

    def _noop(*_args: object, **_kwargs: object) -> None:
        return None

    def _stage_span(*_args: object, **_kwargs: object) -> object:
        return nullcontext()

    monkeypatch.setattr(orchestrator_mod, "_run_repo_scan", _run_repo_scan)
    monkeypatch.setattr(orchestrator_mod, "_build_stage1_extractors", _build_stage1_extractors)
    monkeypatch.setattr(orchestrator_mod, "_run_python_imports", _run_python_imports)
    monkeypatch.setattr(orchestrator_mod, "_run_python_external", _run_python_external)
    monkeypatch.setattr(orchestrator_mod, "_write_delta", _write_delta)
    monkeypatch.setattr(orchestrator_mod, "stage_span", _stage_span)
    monkeypatch.setattr(orchestrator_mod, "record_error", _noop)
    monkeypatch.setattr(orchestrator_mod, "record_stage_duration", _noop)

    if stage1_error is not None:
        failing = stage1_extractors[stage1_error]

        def _raise_error() -> pa.Table:
            msg = "stage1 failure"
            raise ValueError(msg)

        stage1_extractors[stage1_error] = _raise_error
        _ = failing


def test_staged_execution_ordering(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    calls: list[str] = []
    stage1_extractors: dict[str, object] = {
        "ast_files": lambda: (calls.append("ast_files"), _sample_table("ast"))[1],
        "libcst_files": lambda: (calls.append("libcst_files"), _sample_table("cst"))[1],
    }
    _wire_test_doubles(monkeypatch, stage1_extractors=stage1_extractors, calls=calls)

    orchestrator_mod.run_extraction(repo_root=tmp_path, work_dir=tmp_path / "work")

    assert calls.index("repo_scan") < calls.index("python_imports")
    assert calls.index("repo_scan") < calls.index("ast_files")
    assert calls.index("repo_scan") < calls.index("libcst_files")
    assert calls.index("python_imports") < calls.index("python_external")


def test_parallel_stage1_extractors(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    calls: list[str] = []
    stage1_extractors: dict[str, object] = {
        "ast_files": lambda: (calls.append("ast_files"), _sample_table("ast"))[1],
        "libcst_files": lambda: (calls.append("libcst_files"), _sample_table("cst"))[1],
        "symtable_files_v1": lambda: (calls.append("symtable_files_v1"), _sample_table("sym"))[1],
    }
    _wire_test_doubles(monkeypatch, stage1_extractors=stage1_extractors, calls=calls)

    result = orchestrator_mod.run_extraction(
        repo_root=tmp_path,
        work_dir=tmp_path / "work",
        max_workers=4,
    )

    assert not result.errors
    assert "ast_files" in result.delta_locations
    assert "libcst_files" in result.delta_locations
    assert "symtable_files_v1" in result.delta_locations


def test_error_collection(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    calls: list[str] = []
    stage1_extractors: dict[str, object] = {
        "ast_files": lambda: (calls.append("ast_files"), _sample_table("ast"))[1],
        "libcst_files": lambda: (calls.append("libcst_files"), _sample_table("cst"))[1],
    }
    _wire_test_doubles(
        monkeypatch,
        stage1_extractors=stage1_extractors,
        calls=calls,
        stage1_error="libcst_files",
    )

    result = orchestrator_mod.run_extraction(repo_root=tmp_path, work_dir=tmp_path / "work")

    assert result.errors
    first_error = result.errors[0]
    assert first_error.get("extractor") == "libcst_files"
    assert "stage1 failure" in str(first_error.get("error"))


def test_delta_output_locations(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    calls: list[str] = []
    stage1_extractors: dict[str, object] = {
        "libcst_files": lambda: (calls.append("libcst_files"), _sample_table("cst"))[1],
        "symtable_files_v1": lambda: (calls.append("symtable_files_v1"), _sample_table("sym"))[1],
        "scip_index": lambda: (calls.append("scip_index"), _sample_table("scip"))[1],
    }
    _wire_test_doubles(monkeypatch, stage1_extractors=stage1_extractors, calls=calls)

    result = orchestrator_mod.run_extraction(repo_root=tmp_path, work_dir=tmp_path / "work")

    assert "repo_files_v1" in result.delta_locations
    assert "repo_files" in result.delta_locations
    assert "cst_imports" in result.semantic_input_locations
    assert "symtable_scopes" in result.semantic_input_locations
    assert "scip_occurrences" in result.semantic_input_locations


def test_timing_recorded(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    calls: list[str] = []
    stage1_extractors: dict[str, object] = {
        "ast_files": lambda: (calls.append("ast_files"), _sample_table("ast"))[1],
    }
    _wire_test_doubles(monkeypatch, stage1_extractors=stage1_extractors, calls=calls)

    result = orchestrator_mod.run_extraction(repo_root=tmp_path, work_dir=tmp_path / "work")

    assert "repo_scan" in result.timing
    assert "ast_files" in result.timing
    assert "python_imports" in result.timing
    assert "python_external" in result.timing


def test_run_repo_scan_propagates_diff_options(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        "planning_engine.runtime_profile.resolve_runtime_profile", lambda _name: object()
    )
    monkeypatch.setattr(
        "planning_engine.session_factory.build_engine_session", lambda **_kwargs: object()
    )

    def _scan_repo_tables(
        _repo_root: str,
        *,
        options: object,
        context: object,
        prefer_reader: bool,
    ) -> dict[str, pa.Table]:
        captured["options"] = options
        captured["context"] = context
        captured["prefer_reader"] = prefer_reader
        return {
            "repo_files_v1": _sample_table("repo"),
            "file_line_index_v1": _sample_table("line_index"),
        }

    monkeypatch.setattr("extract.scanning.repo_scan.scan_repo_tables", _scan_repo_tables)

    options = ExtractionRunOptions(
        diff_base_ref="origin/main",
        diff_head_ref="HEAD",
        changed_only=True,
        include_globs=("**/*.py",),
        exclude_globs=("**/.venv/**",),
    )

    outputs = orchestrator_mod._run_repo_scan(  # noqa: SLF001
        tmp_path,
        options=options,
    )

    assert "repo_files_v1" in outputs
    assert "file_line_index_v1" in outputs
    scan_options = captured["options"]
    assert getattr(scan_options, "diff_base_ref", None) == "origin/main"
    assert getattr(scan_options, "diff_head_ref", None) == "HEAD"
    assert getattr(scan_options, "changed_only", None) is True

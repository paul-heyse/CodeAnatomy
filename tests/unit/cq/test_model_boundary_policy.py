"""Policy tests for model-layer boundaries in CQ hot paths."""

from __future__ import annotations

from pathlib import Path


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _py_files_under(root: Path) -> list[Path]:
    return [path for path in root.rglob("*.py") if "__pycache__" not in path.parts]


def test_no_pydantic_import_in_hot_paths() -> None:
    repo = _repo_root()
    hot_roots = [
        repo / "tools/cq/search",
        repo / "tools/cq/query",
        repo / "tools/cq/run",
    ]
    offenders: list[Path] = []
    for hot_root in hot_roots:
        for py_file in _py_files_under(hot_root):
            text = py_file.read_text(encoding="utf-8")
            if "import pydantic" in text or "from pydantic" in text:
                offenders.append(py_file)
    assert offenders == []


def test_contract_modules_use_msgspec() -> None:
    repo = _repo_root()
    contract_files = [
        repo / "tools/cq/core/contracts.py",
        repo / "tools/cq/search/contracts.py",
        repo / "tools/cq/search/contracts_runtime_boundary.py",
        repo / "tools/cq/search/enrichment/contracts.py",
    ]
    missing: list[Path] = []
    for file_path in contract_files:
        text = file_path.read_text(encoding="utf-8")
        if "msgspec" not in text:
            missing.append(file_path)
    assert missing == []


def test_no_typeddict_in_cq_runtime_modules() -> None:
    repo = _repo_root()
    cq_root = repo / "tools/cq"
    offenders: list[Path] = []
    for py_file in _py_files_under(cq_root):
        text = py_file.read_text(encoding="utf-8")
        if "TypedDict" in text:
            offenders.append(py_file)
    assert offenders == []

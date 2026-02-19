"""Runtime policy tests for msgspec-only validation boundaries."""

from __future__ import annotations

from pathlib import Path


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def test_no_pydantic_imports_in_src() -> None:
    """Runtime source tree should not import pydantic."""
    repo = _repo_root()
    src_root = repo / "src"
    offenders: list[Path] = []
    for py_file in src_root.rglob("*.py"):
        if "__pycache__" in py_file.parts:
            continue
        text = py_file.read_text(encoding="utf-8")
        if "from pydantic" in text or "import pydantic" in text:
            offenders.append(py_file)
    assert offenders == []

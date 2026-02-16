"""Tests for test_noqa_guard."""

from __future__ import annotations

from pathlib import Path


def test_tools_cq_runtime_has_no_noqa_suppressions() -> None:
    """Fail tests if runtime code introduces noqa suppressions."""
    repo_root = Path(__file__).resolve().parents[4]
    cq_root = repo_root / "tools" / "cq"
    runtime_files = sorted(path for path in cq_root.rglob("*.py") if "tests" not in path.parts)
    offenders: list[str] = []
    for path in runtime_files:
        text = path.read_text(encoding="utf-8")
        if "noqa" in text:
            offenders.append(str(path.relative_to(repo_root)))
    assert not offenders, f"Found noqa suppressions in runtime files: {offenders}"

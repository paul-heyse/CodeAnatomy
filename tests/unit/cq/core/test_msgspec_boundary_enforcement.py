"""Tests for test_msgspec_boundary_enforcement."""

from __future__ import annotations

from pathlib import Path


def test_hot_paths_do_not_import_core_to_builtins() -> None:
    """Reject core-to-builtins imports and ad-hoc msgspec conversions in hot paths."""
    repo_root = Path(__file__).resolve().parents[4]
    hot_paths = [
        repo_root / "tools" / "cq" / "search" / "pipeline" / "smart_search.py",
        repo_root / "tools" / "cq" / "query" / "executor.py",
        repo_root / "tools" / "cq" / "macros" / "calls_target.py",
        repo_root / "tools" / "cq" / "search" / "enrichment" / "core.py",
    ]
    import_offenders: list[str] = []
    callsite_offenders: list[str] = []
    for path in hot_paths:
        text = path.read_text(encoding="utf-8")
        if "from tools.cq.core.serialization import to_builtins" in text:
            import_offenders.append(str(path.relative_to(repo_root)))
        if "msgspec.to_builtins(" in text:
            callsite_offenders.append(str(path.relative_to(repo_root)))
    assert not import_offenders, f"Hot-path modules imported core to_builtins: {import_offenders}"
    assert not callsite_offenders, (
        f"Hot-path modules used ad-hoc msgspec.to_builtins: {callsite_offenders}"
    )

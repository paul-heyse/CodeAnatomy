"""Denylist legacy imports to prevent compat surfaces from reappearing."""

from __future__ import annotations

import re
from pathlib import Path

LEGACY_IMPORT_PATTERNS = (
    re.compile(r"^\s*import\s+datafusion_ext\b", re.MULTILINE),
    re.compile(r"^\s*from\s+datafusion_ext\b", re.MULTILINE),
    re.compile(r"^\s*from\s+ibis_engine\.query_bridge\b", re.MULTILINE),
    re.compile(r"^\s*from\s+ibis_engine\.plan_bridge\b", re.MULTILINE),
    re.compile(r"^\s*from\s+arrowdsl\.plan\b", re.MULTILINE),
    re.compile(r"^\s*from\s+arrowdsl\.plan_helpers\b", re.MULTILINE),
    re.compile(r"^\s*from\s+normalize\.plan_builders\b", re.MULTILINE),
    re.compile(r"^\s*import\s+normalize\.bytecode_anchor\b", re.MULTILINE),
    re.compile(r"^\s*from\s+normalize\.bytecode_anchor\b", re.MULTILINE),
    re.compile(r"^\s*import\s+normalize\.bytecode_cfg\b", re.MULTILINE),
    re.compile(r"^\s*from\s+normalize\.bytecode_cfg\b", re.MULTILINE),
    re.compile(r"^\s*import\s+normalize\.bytecode_dfg\b", re.MULTILINE),
    re.compile(r"^\s*from\s+normalize\.bytecode_dfg\b", re.MULTILINE),
    re.compile(r"^\s*import\s+normalize\.diagnostics\b", re.MULTILINE),
    re.compile(r"^\s*from\s+normalize\.diagnostics\b", re.MULTILINE),
    re.compile(r"^\s*import\s+normalize\.types\b", re.MULTILINE),
    re.compile(r"^\s*from\s+normalize\.types\b", re.MULTILINE),
    re.compile(r"^\s*from\s+extract\.plan_helpers\b", re.MULTILINE),
)


def _read_sources() -> str:
    sources = Path("src").rglob("*.py")
    return "\n".join(path.read_text(encoding="utf-8") for path in sources)


def test_no_legacy_imports() -> None:
    """Ensure deprecated import paths do not reappear."""
    text = _read_sources()
    for pattern in LEGACY_IMPORT_PATTERNS:
        assert pattern.search(text) is None

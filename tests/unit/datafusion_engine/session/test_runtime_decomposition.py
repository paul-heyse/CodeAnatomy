# ruff: noqa: D103, INP001
"""Tests for runtime hub decomposition boundaries."""

from __future__ import annotations

import re
from pathlib import Path

_RUNTIME_IMPORT = re.compile(r"^from datafusion_engine\.session\.runtime import (.+)$")


def test_runtime_imports_are_core_profile_only() -> None:
    violations: list[str] = []
    for path in Path("src").rglob("*.py"):
        if path.as_posix() == "src/datafusion_engine/session/runtime.py":
            continue
        for idx, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
            match = _RUNTIME_IMPORT.match(line.strip())
            if not match:
                continue
            imported = match.group(1).replace(" ", "")
            if imported != "DataFusionRuntimeProfile":
                violations.append(f"{path}:{idx}:{line.strip()}")
    assert violations == []

"""Import-boundary tests for obs.otel facade usage."""

from __future__ import annotations

import re
from pathlib import Path

_PATTERN = re.compile(r"^\s*(from\s+obs\.otel\.|import\s+obs\.otel\.)")


def test_no_direct_otel_submodule_imports_outside_obs_package() -> None:
    """Non-obs modules do not import obs.otel submodules directly."""
    violations: list[str] = []
    for path in Path("src").rglob("*.py"):
        if path.as_posix().startswith("src/obs/"):
            continue
        for idx, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
            if _PATTERN.search(line):
                violations.append(f"{path}:{idx}:{line.strip()}")
    assert violations == []

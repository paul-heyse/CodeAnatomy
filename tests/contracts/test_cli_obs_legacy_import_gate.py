"""Deletion preflight gate for legacy Hamilton/rustworkx imports in CLI/OBS."""

from __future__ import annotations

import ast
from pathlib import Path

_FORBIDDEN_MODULES = ("hamilton_pipeline", "relspec", "rustworkx")
_SCAN_ROOTS = (Path("src/cli"), Path("src/obs"))


def _import_targets(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    targets: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                targets.add(alias.name)
        if isinstance(node, ast.ImportFrom) and node.module:
            targets.add(node.module)
    return targets


def test_cli_obs_do_not_import_legacy_hamilton_or_rustworkx_modules() -> None:
    """Test cli obs do not import legacy hamilton or rustworkx modules."""
    offenders: list[str] = []

    for root in _SCAN_ROOTS:
        for path in root.rglob("*.py"):
            imports = _import_targets(path)
            offenders.extend(
                f"{path}:{forbidden}"
                for forbidden in _FORBIDDEN_MODULES
                if any(
                    target == forbidden or target.startswith(f"{forbidden}.") for target in imports
                )
            )

    assert not offenders, f"Forbidden legacy imports found: {offenders}"

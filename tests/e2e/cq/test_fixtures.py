"""Test the E2E test fixtures themselves."""

from __future__ import annotations

import subprocess
from collections.abc import Callable
from pathlib import Path


def test_repo_root_fixture(repo_root: Path) -> None:
    """Verify repo_root fixture returns valid repository root.

    Parameters
    ----------
    repo_root : Path
        Repository root fixture.
    """
    assert repo_root.exists()
    assert repo_root.is_dir()
    assert (repo_root / ".git").exists()
    assert (repo_root / "pyproject.toml").exists()


def test_run_command_fixture(
    run_command: Callable[[list[str]], subprocess.CompletedProcess[str]],
) -> None:
    """Verify run_command fixture executes commands correctly.

    Parameters
    ----------
    run_command : Callable
        Command runner fixture.
    """
    result = run_command(["echo", "test"])
    assert result.returncode == 0
    assert "test" in result.stdout

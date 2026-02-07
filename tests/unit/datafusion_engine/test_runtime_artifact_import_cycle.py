"""Regression tests for runtime/artifact-spec import ordering."""

from __future__ import annotations

import subprocess
import sys


def test_runtime_and_artifact_specs_import_without_cycle() -> None:
    """Importing runtime before artifact specs should not trigger circular-import errors."""
    code = (
        "import importlib\n"
        "importlib.import_module('datafusion_engine.session.runtime')\n"
        "importlib.import_module('serde_artifact_specs')\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr

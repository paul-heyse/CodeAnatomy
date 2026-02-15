"""Tests for launcher-level cross-option invariants."""

from __future__ import annotations

from pathlib import Path

import pytest
from tools.cq.cli_app.app import app
from tools.cq.cli_app.validators import validate_launcher_invariants


def test_config_cannot_be_combined_with_no_config(tmp_path: Path) -> None:
    """Ensure launcher rejects `--config` with `--no-config`."""
    config_path = tmp_path / "cq.toml"
    config_path.write_text("[tool.cq]\n", encoding="utf-8")

    with pytest.raises(SystemExit):
        app.meta.parse_args(
            ["calls", "foo", "--config", str(config_path), "--no-config"],
            exit_on_error=True,
            print_error=False,
        )


def test_verbose_cannot_be_negative_in_validator() -> None:
    """Ensure launcher invariant rejects negative verbosity values."""

    class _GlobalOpts:
        verbose = -1

    with pytest.raises(ValueError, match="--verbose cannot be negative"):
        validate_launcher_invariants(global_opts=_GlobalOpts())

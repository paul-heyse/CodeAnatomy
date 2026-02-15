"""Tests for CQ completion helper utilities."""

from __future__ import annotations

from pathlib import Path

from tools.cq.cli_app.app import app
from tools.cq.cli_app.completion import generate_completion_scripts


def test_generate_completion_scripts(tmp_path: Path) -> None:
    """Ensure completion helper writes scripts for bash/zsh/fish."""
    written = generate_completion_scripts(app, tmp_path, program_name="cq")
    assert set(written) == {"bash", "zsh", "fish"}
    for shell in ("bash", "zsh", "fish"):
        output_path = tmp_path / f"cq.{shell}"
        assert output_path.exists()
        assert "cq" in output_path.read_text(encoding="utf-8")

"""Tests for CLI contract module exports."""

from __future__ import annotations

from tools.cq.cli_app import contracts


def test_cli_contracts_module_exists() -> None:
    """Test CLI contracts module is importable.

    Note: This module is now a placeholder for future CLI-specific contracts.
    Run step parsing has moved to tools.cq.run.step_decode.
    """
    assert contracts.__all__ == []

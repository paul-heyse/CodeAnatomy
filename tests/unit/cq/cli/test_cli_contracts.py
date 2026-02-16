"""Tests for CLI contract module exports."""

from __future__ import annotations

import importlib.util


def test_cli_contracts_module_removed() -> None:
    """CLI contracts placeholder module is removed after hard cutover.

    Run step parsing and typed contracts now live in runtime/core packages.
    """
    assert importlib.util.find_spec("tools.cq.cli_app.contracts") is None

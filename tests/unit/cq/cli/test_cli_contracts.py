"""Tests for CLI contract module exports."""

from __future__ import annotations

from tools.cq.cli_app import contracts


def test_cli_contracts_export_run_step_union() -> None:
    assert hasattr(contracts, "RunStepCli")
    assert callable(contracts.to_run_step)

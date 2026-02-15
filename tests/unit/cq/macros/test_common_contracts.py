"""Tests for common macro contract exports."""

from __future__ import annotations

from tools.cq.macros.common import contracts


def test_common_contract_exports() -> None:
    assert hasattr(contracts, "MacroExecutionRequestV1")
    assert hasattr(contracts, "MacroScorePayloadV1")

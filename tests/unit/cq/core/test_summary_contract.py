"""Tests for typed summary contracts."""

from __future__ import annotations

import pytest
from tools.cq.core.summary_contract import (
    CqSummary,
    SemanticTelemetryV1,
    build_semantic_telemetry,
    summary_from_mapping,
)

ATTEMPTED_COUNT = 5
APPLIED_COUNT = 2
FAILED_COUNT = 3
TELEMETRY_ATTEMPTED = 2


def test_build_semantic_telemetry_normalizes_failed_count() -> None:
    """Telemetry builder recomputes failed count from attempted/applied."""
    telemetry = build_semantic_telemetry(
        attempted=ATTEMPTED_COUNT,
        applied=APPLIED_COUNT,
        failed=0,
        skipped=1,
        timed_out=1,
    )

    assert telemetry.attempted == ATTEMPTED_COUNT
    assert telemetry.applied == APPLIED_COUNT
    assert telemetry.failed == FAILED_COUNT
    assert telemetry.skipped == 1
    assert telemetry.timed_out == 1


def test_summary_from_mapping_coerces_semantic_telemetry() -> None:
    """Summary mapping coercion upgrades semantic telemetry payloads."""
    summary = summary_from_mapping(
        {
            "query": "entity=function name=foo",
            "python_semantic_telemetry": {
                "attempted": TELEMETRY_ATTEMPTED,
                "applied": 1,
                "failed": 1,
            },
        }
    )

    assert summary.query == "entity=function name=foo"
    assert isinstance(summary.python_semantic_telemetry, SemanticTelemetryV1)
    assert summary.python_semantic_telemetry.attempted == TELEMETRY_ATTEMPTED


def test_summary_rejects_unknown_keys() -> None:
    """Summary update rejects unknown keys."""
    summary = CqSummary()

    with pytest.raises(KeyError, match="Unknown summary key"):
        summary.update({"unknown_key": 1})

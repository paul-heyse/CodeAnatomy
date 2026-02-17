"""Tests for enrichment telemetry accumulation helpers."""

from __future__ import annotations

import pytest
from tools.cq.search.enrichment.telemetry import (
    accumulate_rust_bundle_drift,
    accumulate_stage_status,
    accumulate_stage_timings,
)

_AST_GREP_TOTAL_MS = 3.5
_PYTHON_AST_TOTAL_MS = 3.0
_REMOVED_NODE_KIND_COUNT = 2


def test_accumulate_stage_status_counts_expected_states() -> None:
    """Stage status counters increment only for recognized status strings."""
    stages: dict[str, object] = {
        "ast_grep": {"applied": 0, "degraded": 0, "skipped": 0},
    }

    accumulate_stage_status(
        stages_bucket=stages,
        stage_status={"ast_grep": "applied", "other": "skipped", "ast_grep_2": 1},
    )

    ast_grep = stages["ast_grep"]
    assert isinstance(ast_grep, dict)
    assert ast_grep["applied"] == 1
    assert ast_grep["degraded"] == 0
    assert ast_grep["skipped"] == 0


def test_accumulate_stage_timings_adds_numeric_values() -> None:
    """Stage timings aggregate numeric stage durations only."""
    timings: dict[str, object] = {"ast_grep": 2.5}

    accumulate_stage_timings(
        timings_bucket=timings,
        stage_timings_ms={"ast_grep": 1.0, "python_ast": 3, "bad": True, 3: 2.0},
    )

    assert timings["ast_grep"] == pytest.approx(_AST_GREP_TOTAL_MS)
    assert timings["python_ast"] == pytest.approx(_PYTHON_AST_TOTAL_MS)
    assert "bad" not in timings


def test_accumulate_rust_bundle_drift_updates_profile_counters() -> None:
    """Rust bundle drift counters update based on drift payload fields."""
    bucket: dict[str, object] = {
        "distribution_profile_hits": 0,
        "drift_breaking_profile_hits": 0,
        "drift_removed_node_kinds": 0,
        "drift_removed_fields": 0,
    }

    accumulate_rust_bundle_drift(
        lang_bucket=bucket,
        bundle={
            "distribution_included": True,
            "drift_compatible": False,
            "drift_schema_diff": {
                "removed_node_kinds": ["a", "b"],
                "removed_fields": ["x"],
            },
        },
    )

    assert bucket["distribution_profile_hits"] == 1
    assert bucket["drift_breaking_profile_hits"] == 1
    assert bucket["drift_removed_node_kinds"] == _REMOVED_NODE_KIND_COUNT
    assert bucket["drift_removed_fields"] == 1

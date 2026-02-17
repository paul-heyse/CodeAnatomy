"""Tests for Python enrichment adapter."""

from __future__ import annotations

from dataclasses import dataclass
from typing import cast

from tools.cq.search.enrichment.python_adapter import PythonEnrichmentAdapter
from tools.cq.search.pipeline.enrichment_contracts import PythonEnrichmentV1

_TIMING_MS = 1.5


@dataclass(frozen=True)
class _Match:
    python_enrichment: object


def test_payload_from_match_extracts_python_payload() -> None:
    """Adapter should extract python_enrichment mapping from match object."""
    adapter = PythonEnrichmentAdapter()
    payload = adapter.payload_from_match(_Match(PythonEnrichmentV1(payload={"meta": {}})))

    assert payload == {"meta": {}}


def test_accumulate_telemetry_updates_stage_and_runtime_counters() -> None:
    """Adapter telemetry should aggregate status, timings, and runtime flags."""
    adapter = PythonEnrichmentAdapter()
    bucket: dict[str, object] = {
        "stages": {"ast": {"applied": 0, "degraded": 0, "skipped": 0}},
        "timings_ms": {},
        "query_runtime": {"did_exceed_match_limit": 0, "cancelled": 0},
    }
    payload: dict[str, object] = {
        "meta": {
            "stage_status": {"ast": "applied"},
            "stage_timings_ms": {"ast": _TIMING_MS},
        },
        "query_runtime": {"did_exceed_match_limit": True, "cancelled": True},
    }

    adapter.accumulate_telemetry(bucket, payload)

    stages = cast("dict[str, dict[str, dict[str, int]]]", {"root": bucket["stages"]})["root"]
    timings = cast("dict[str, dict[str, float]]", {"root": bucket["timings_ms"]})["root"]
    runtime = cast("dict[str, dict[str, int]]", {"root": bucket["query_runtime"]})["root"]
    assert stages["ast"]["applied"] == 1
    assert timings["ast"] == _TIMING_MS
    assert runtime["did_exceed_match_limit"] == 1
    assert runtime["cancelled"] == 1


def test_build_diagnostics_collects_tree_sitter_and_degrade_reason() -> None:
    """Adapter diagnostics should include normalized tree-sitter and degrade entries."""
    adapter = PythonEnrichmentAdapter()
    rows = adapter.build_diagnostics(
        {
            "cst_diagnostics": [{"kind": "parse", "message": "bad token", "start_line": 3}],
            "degrade_reason": "timeout",
        }
    )

    assert any(row.get("kind") == "parse" for row in rows)
    assert any(row.get("kind") == "degrade_reason" for row in rows)

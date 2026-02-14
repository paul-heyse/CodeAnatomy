"""Unit tests for typed diagnostics artifact contracts."""

from __future__ import annotations

from tools.cq.core.diagnostics_contracts import build_diagnostics_artifact_payload
from tools.cq.core.schema import CqResult, RunMeta, mk_result


def _mk_result() -> CqResult:
    run = RunMeta(
        run_id="run",
        macro="search",
        root=".",
        argv=["cq", "search", "x"],
        started_ms=0.0,
        elapsed_ms=1.0,
        schema_version="cq.v1",
    )
    return mk_result(run)


def test_build_diagnostics_artifact_payload_none_when_empty() -> None:
    result = _mk_result()
    payload = build_diagnostics_artifact_payload(result)
    assert payload is None


def test_build_diagnostics_artifact_payload_contains_rust_telemetry() -> None:
    result = _mk_result()
    result.summary.update(
        {
            "enrichment_telemetry": {"python": {"applied": 1}},
            "rust_semantic_telemetry": {"attempted": 1, "applied": 1, "failed": 0, "timed_out": 0},
            "semantic_planes": {"semantic_tokens_count": 2},
        }
    )
    payload = build_diagnostics_artifact_payload(result)
    assert payload is not None
    assert payload.rust_semantic_telemetry["attempted"] == 1
    assert payload.semantic_planes["semantic_tokens_count"] == 2

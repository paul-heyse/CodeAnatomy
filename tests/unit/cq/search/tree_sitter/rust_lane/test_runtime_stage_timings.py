"""Tests for Rust lane stage-timing parity fields."""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from tools.cq.search.tree_sitter.rust_lane import runtime_engine


def test_rust_pipeline_emits_stage_timings_and_status(monkeypatch: pytest.MonkeyPatch) -> None:
    """Rust pipeline should emit Python-parity stage timing/status fields."""

    def _fake_parse_with_session(
        _source: str,
        *,
        cache_key: str | None,
        parse_session: object | None = None,
    ) -> tuple[SimpleNamespace, bytes, tuple[()]]:
        _ = cache_key, parse_session
        return SimpleNamespace(root_node=object()), b"x", ()

    monkeypatch.setattr(
        runtime_engine,
        "_parse_with_session",
        _fake_parse_with_session,
    )
    monkeypatch.setattr(
        runtime_engine,
        "_collect_payload_with_timings",
        lambda _request: {
            "language": "rust",
            "enrichment_status": "applied",
            "enrichment_sources": [],
            "stage_timings_ms": {"query_pack": 1.0, "payload_build": 1.0},
            "stage_status": {"query_pack": "applied", "payload_build": "applied"},
        },
    )

    request_type = runtime_engine.__dict__["_RustPipelineRequestV1"]
    request = request_type(
        source="fn target() {}",
        cache_key="k",
        max_scope_depth=4,
        query_budget_ms=None,
        resolve_node=lambda _root: object(),
        byte_span_for_node=lambda _node: (0, 1),
        error_prefix="test",
    )

    run_pipeline = runtime_engine.__dict__["_run_rust_enrichment_pipeline"]
    payload = run_pipeline(request)

    assert isinstance(payload, dict)
    timings = payload.get("stage_timings_ms")
    status = payload.get("stage_status")
    assert isinstance(timings, dict)
    assert isinstance(status, dict)
    assert "ast_grep" in timings
    assert "tree_sitter" in timings
    assert "query_pack" in timings
    assert "payload_build" in timings
    assert "attachment" in timings
    assert "total" in timings
    assert status.get("ast_grep") == "skipped"
    assert status.get("tree_sitter") == "applied"

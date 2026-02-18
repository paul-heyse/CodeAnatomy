"""Tests for semantic front-door helper providers."""

from __future__ import annotations

from pathlib import Path

from _pytest.monkeypatch import MonkeyPatch
from tools.cq.search.semantic.helpers import (
    run_python_byte_range_enrichment,
    run_rust_byte_range_enrichment,
)


def test_run_python_byte_range_enrichment_returns_dict_payload(
    monkeypatch: MonkeyPatch,
) -> None:
    """Verify Python helper returns normalized mapping payload."""
    import tools.cq.search.python.extractors_orchestrator as orchestrator
    from tools.cq.search.semantic import helpers

    monkeypatch.setattr(helpers, "get_sg_root", lambda *_args, **_kwargs: object())
    payload = {"name": "target", "enrichment_status": "applied"}
    monkeypatch.setattr(
        orchestrator, "enrich_python_context_by_byte_range", lambda _request: payload
    )

    out = run_python_byte_range_enrichment(
        target_file_path=Path("pkg/module.py"),
        source_bytes=b"def target():\n    pass\n",
        byte_start=0,
        byte_end=5,
        query_budget_ms=100,
    )

    assert out == payload
    assert out is not payload


def test_run_rust_byte_range_enrichment_returns_dict_payload(
    monkeypatch: MonkeyPatch,
) -> None:
    """Verify Rust helper returns normalized mapping payload."""
    import tools.cq.search.rust.enrichment as rust_enrichment

    payload = {"name": "target", "enrichment_status": "applied"}
    monkeypatch.setattr(
        rust_enrichment,
        "enrich_rust_context_by_byte_range",
        lambda *_args, **_kwargs: payload,
    )

    out = run_rust_byte_range_enrichment(
        source="fn target() {}",
        target_file_path=Path("src/lib.rs"),
        byte_start=0,
        byte_end=5,
        query_budget_ms=100,
    )

    assert out == payload
    assert out is not payload

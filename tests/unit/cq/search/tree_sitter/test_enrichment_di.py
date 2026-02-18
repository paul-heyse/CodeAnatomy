"""Tests for enrichment dependency-injection seams."""

from __future__ import annotations

from typing import Any

import pytest
from tools.cq.search.tree_sitter.python_lane import runtime_engine as py_runtime
from tools.cq.search.tree_sitter.rust_lane import runtime_engine as rust_runtime


def test_python_enrichment_accepts_parse_session_override(monkeypatch: pytest.MonkeyPatch) -> None:
    """Python byte-range enrichment should forward injected parse sessions."""
    captured: dict[str, object] = {}
    sentinel_session: Any = object()

    monkeypatch.setattr(py_runtime, "is_tree_sitter_python_available", lambda: True)

    def _fake_parse_tree_for_enrichment(
        _payload: object,
        *,
        source: str,
        cache_key: str | None,
        parse_session: Any,
    ) -> None:
        _ = source, cache_key
        captured["parse_session"] = parse_session

    monkeypatch.setattr(py_runtime, "_parse_tree_for_enrichment", _fake_parse_tree_for_enrichment)

    result = py_runtime.enrich_python_context_by_byte_range(
        "def target():\n    return 1\n",
        byte_start=0,
        byte_end=4,
        runtime_deps=py_runtime.PythonLaneRuntimeDepsV1(
            parse_session=sentinel_session,
            cache_backend=object(),
        ),
    )

    assert captured["parse_session"] is sentinel_session
    assert isinstance(result, dict)


def test_rust_enrichment_accepts_parse_session_and_cache_backend(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Rust byte-range enrichment should pass injected seams into pipeline request."""
    captured: dict[str, object] = {}
    sentinel_session: Any = object()
    sentinel_cache = object()

    monkeypatch.setattr(rust_runtime, "is_tree_sitter_rust_available", lambda: True)
    monkeypatch.setattr(rust_runtime, "MAX_SOURCE_BYTES", 10_000)

    def _fake_run(request: Any) -> dict[str, object]:
        captured["parse_session"] = request.parse_session
        captured["cache_backend"] = request.cache_backend
        return {"language": "rust", "enrichment_status": "applied"}

    monkeypatch.setattr(rust_runtime, "_run_rust_enrichment_pipeline", _fake_run)

    result = rust_runtime.enrich_rust_context_by_byte_range(
        "fn target() {}",
        byte_start=0,
        byte_end=2,
        runtime_deps=rust_runtime.RustLaneRuntimeDepsV1(
            parse_session=sentinel_session,
            cache_backend=sentinel_cache,
        ),
    )

    assert captured["parse_session"] is sentinel_session
    assert captured["cache_backend"] is sentinel_cache
    assert result == {"language": "rust", "enrichment_status": "applied"}

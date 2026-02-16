# ruff: noqa: D103
"""Tests for obs.otel facade surface."""

from __future__ import annotations

from obs import otel


def test_otel_facade_exposes_common_symbols() -> None:
    for name in ("stage_span", "set_run_id", "get_run_id", "SCOPE_SEMANTICS"):
        assert hasattr(otel, name), name

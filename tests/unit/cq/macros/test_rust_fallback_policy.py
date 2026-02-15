"""Tests for declarative rust fallback policy helper."""

from __future__ import annotations

from pathlib import Path

import pytest
from tools.cq.core.schema import CqResult, RunMeta, mk_result
from tools.cq.macros.rust_fallback_policy import (
    RustFallbackPolicyV1,
    apply_rust_fallback_policy,
)


def _base_result() -> CqResult:
    run = RunMeta(
        macro="calls",
        argv=["cq", "calls", "foo"],
        root=".",
        started_ms=0.0,
        elapsed_ms=0.0,
    )
    return mk_result(run)


def test_apply_rust_fallback_policy_uses_summary_key(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    def _fake_apply(
        *,
        result: CqResult,
        root: Path,
        pattern: str,
        macro_name: str,
        fallback_matches: int,
        query: str | None,
    ) -> CqResult:
        captured["root"] = root
        captured["pattern"] = pattern
        captured["macro_name"] = macro_name
        captured["fallback_matches"] = fallback_matches
        captured["query"] = query
        return result

    monkeypatch.setattr(
        "tools.cq.macros.multilang_fallback.apply_rust_macro_fallback",
        _fake_apply,
    )

    result = _base_result()
    result.summary["fallback_count"] = 7
    policy = RustFallbackPolicyV1(
        macro_name="calls",
        pattern="foo\\(",
        query="foo",
        fallback_matches_summary_key="fallback_count",
    )
    out = apply_rust_fallback_policy(result, root=Path(), policy=policy)
    assert out is result
    assert captured["fallback_matches"] == 7
    assert captured["macro_name"] == "calls"
    assert captured["pattern"] == "foo\\("
    assert captured["query"] == "foo"


def test_apply_rust_fallback_policy_non_int_summary_defaults_zero(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    def _fake_apply(
        *,
        result: CqResult,
        root: Path,
        pattern: str,
        macro_name: str,
        fallback_matches: int,
        query: str | None,
    ) -> CqResult:
        _ = root
        _ = pattern
        _ = macro_name
        _ = query
        captured["fallback_matches"] = fallback_matches
        return result

    monkeypatch.setattr(
        "tools.cq.macros.multilang_fallback.apply_rust_macro_fallback",
        _fake_apply,
    )

    result = _base_result()
    result.summary["fallback_count"] = "7"
    policy = RustFallbackPolicyV1(
        macro_name="calls",
        pattern="foo\\(",
        fallback_matches_summary_key="fallback_count",
    )
    apply_rust_fallback_policy(result, root=Path(), policy=policy)
    assert captured["fallback_matches"] == 0

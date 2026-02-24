"""Tests for declarative rust fallback policy helper."""

from __future__ import annotations

from pathlib import Path

import pytest
from tools.cq.core.schema import CqResult, RunMeta, mk_result, update_result_summary
from tools.cq.macros.multilang_fallback import RustMacroFallbackRequestV1
from tools.cq.macros.rust_fallback_policy import (
    RustFallbackPolicyV1,
    apply_rust_fallback_policy,
)

FALLBACK_MATCH_COUNT = 7


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
    """Test apply rust fallback policy uses summary key."""
    captured: dict[str, object] = {}

    def _fake_apply(request: RustMacroFallbackRequestV1) -> CqResult:
        captured["root"] = request.root
        captured["pattern"] = request.pattern
        captured["macro_name"] = request.macro_name
        captured["fallback_matches"] = request.fallback_matches
        captured["query"] = request.query
        return request.result

    monkeypatch.setattr(
        "tools.cq.macros.multilang_fallback.apply_rust_macro_fallback",
        _fake_apply,
    )

    result = update_result_summary(_base_result(), {"fallback_count": FALLBACK_MATCH_COUNT})
    policy = RustFallbackPolicyV1(
        macro_name="calls",
        pattern="foo\\(",
        query="foo",
        fallback_matches_summary_key="fallback_count",
    )
    out = apply_rust_fallback_policy(result, root=Path(), policy=policy)
    assert out is result
    assert captured["fallback_matches"] == FALLBACK_MATCH_COUNT
    assert captured["macro_name"] == "calls"
    assert captured["pattern"] == "foo\\("
    assert captured["query"] == "foo"


def test_apply_rust_fallback_policy_non_int_summary_defaults_zero(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test apply rust fallback policy non int summary defaults zero."""
    captured: dict[str, object] = {}

    def _fake_apply(request: RustMacroFallbackRequestV1) -> CqResult:
        captured["fallback_matches"] = request.fallback_matches
        return request.result

    monkeypatch.setattr(
        "tools.cq.macros.multilang_fallback.apply_rust_macro_fallback",
        _fake_apply,
    )

    result = update_result_summary(_base_result(), {"query": "7"})
    policy = RustFallbackPolicyV1(
        macro_name="calls",
        pattern="foo\\(",
        fallback_matches_summary_key="query",
    )
    apply_rust_fallback_policy(result, root=Path(), policy=policy)
    assert captured["fallback_matches"] == 0


def test_apply_rust_fallback_policy_uses_unsupported_contract_for_rust_none(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Macros with rust capability=none should route to unsupported contract."""
    captured: dict[str, object] = {}

    def _fake_unsupported(
        *,
        result: CqResult,
        root: Path,
        macro_name: str,
        rust_only: bool,
        query: str | None,
    ) -> CqResult:
        captured["root"] = root
        captured["macro_name"] = macro_name
        captured["rust_only"] = rust_only
        captured["query"] = query
        return result

    def _should_not_call(*_args: object, **_kwargs: object) -> CqResult:
        msg = "rust fallback search should not run for rust:none macros"
        raise AssertionError(msg)

    monkeypatch.setattr(
        "tools.cq.macros.multilang_fallback.apply_unsupported_macro_contract",
        _fake_unsupported,
    )
    monkeypatch.setattr(
        "tools.cq.macros.multilang_fallback.apply_rust_macro_fallback",
        _should_not_call,
    )

    run = RunMeta(
        macro="impact",
        argv=["cq", "impact", "register_udf", "--include", "rust/**/*.rs"],
        root=".",
        started_ms=0.0,
        elapsed_ms=0.0,
    )
    result = mk_result(run)
    policy = RustFallbackPolicyV1(
        macro_name="impact",
        pattern="register_udf",
        query="register_udf",
    )
    out = apply_rust_fallback_policy(result, root=Path(), policy=policy)
    assert out is result
    assert captured["macro_name"] == "impact"
    assert captured["rust_only"] is True

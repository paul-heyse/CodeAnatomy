"""Unit tests for canonical LSP contract-state helpers."""

from __future__ import annotations

from tools.cq.search.lsp_contract_state import derive_lsp_contract_state


def test_derive_lsp_contract_state_unavailable() -> None:
    state = derive_lsp_contract_state(provider="pyrefly", available=False)
    assert state.status == "unavailable"
    assert state.available is False


def test_derive_lsp_contract_state_skipped() -> None:
    state = derive_lsp_contract_state(provider="pyrefly", available=True, attempted=0, applied=0)
    assert state.status == "skipped"


def test_derive_lsp_contract_state_failed() -> None:
    state = derive_lsp_contract_state(
        provider="rust_analyzer",
        available=True,
        attempted=2,
        applied=0,
        failed=2,
    )
    assert state.status == "failed"


def test_derive_lsp_contract_state_partial() -> None:
    state = derive_lsp_contract_state(
        provider="pyrefly",
        available=True,
        attempted=3,
        applied=1,
        failed=2,
    )
    assert state.status == "partial"


def test_derive_lsp_contract_state_ok() -> None:
    state = derive_lsp_contract_state(
        provider="rust_analyzer",
        available=True,
        attempted=2,
        applied=2,
        failed=0,
    )
    assert state.status == "ok"


def test_derive_lsp_contract_state_preserves_reason_order_and_dedup() -> None:
    state = derive_lsp_contract_state(
        provider="pyrefly",
        available=True,
        attempted=1,
        applied=0,
        failed=1,
        reasons=("request_failed", "request_failed", "request_timeout"),
    )
    assert state.status == "failed"
    assert state.reasons == ("request_failed", "request_timeout")

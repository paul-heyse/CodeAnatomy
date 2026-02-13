"""Canonical LSP contract-state helpers for CQ front-door semantics."""

from __future__ import annotations

from typing import Literal

from tools.cq.core.structs import CqStruct

LspProvider = Literal["pyrefly", "rust_analyzer", "none"]
LspStatus = Literal["unavailable", "skipped", "failed", "partial", "ok"]


class LspContractStateV1(CqStruct, frozen=True):
    """Deterministic LSP state for front-door degradation semantics."""

    provider: LspProvider = "none"
    available: bool = False
    attempted: int = 0
    applied: int = 0
    failed: int = 0
    timed_out: int = 0
    status: LspStatus = "unavailable"
    reasons: tuple[str, ...] = ()


class LspContractStateInputV1(CqStruct, frozen=True):
    """Input envelope for deterministic LSP contract-state derivation."""

    provider: LspProvider
    available: bool
    attempted: int = 0
    applied: int = 0
    failed: int = 0
    timed_out: int = 0
    reasons: tuple[str, ...] = ()


def derive_lsp_contract_state(input_state: LspContractStateInputV1) -> LspContractStateV1:
    """Derive canonical LSP state from capability + attempt telemetry.

    Returns:
    -------
    LspContractStateV1
        Canonical state struct for front-door degradation semantics.
    """
    attempted_count = max(0, int(input_state.attempted))
    applied_count = max(0, int(input_state.applied))
    failed_count = max(0, int(input_state.failed))
    timed_out_count = max(0, int(input_state.timed_out))
    normalized_reasons = tuple(dict.fromkeys(reason for reason in input_state.reasons if reason))

    if not input_state.available:
        return LspContractStateV1(
            provider=input_state.provider,
            available=False,
            status="unavailable",
            reasons=normalized_reasons,
        )
    if attempted_count <= 0:
        return LspContractStateV1(
            provider=input_state.provider,
            available=True,
            status="skipped",
            reasons=normalized_reasons,
        )
    if applied_count <= 0:
        return LspContractStateV1(
            provider=input_state.provider,
            available=True,
            attempted=attempted_count,
            failed=max(failed_count, attempted_count),
            timed_out=timed_out_count,
            status="failed",
            reasons=normalized_reasons,
        )
    status: LspStatus = (
        "ok" if failed_count <= 0 and applied_count >= attempted_count else "partial"
    )
    return LspContractStateV1(
        provider=input_state.provider,
        available=True,
        attempted=attempted_count,
        applied=applied_count,
        failed=failed_count,
        timed_out=timed_out_count,
        status=status,
        reasons=normalized_reasons,
    )


__all__ = [
    "LspContractStateInputV1",
    "LspContractStateV1",
    "LspProvider",
    "LspStatus",
    "derive_lsp_contract_state",
]

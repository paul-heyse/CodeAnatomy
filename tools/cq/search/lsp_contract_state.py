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


def derive_lsp_contract_state(  # noqa: PLR0913
    *,
    provider: LspProvider,
    available: bool,
    attempted: int = 0,
    applied: int = 0,
    failed: int = 0,
    timed_out: int = 0,
    reasons: tuple[str, ...] = (),
) -> LspContractStateV1:
    """Derive canonical LSP state from capability + attempt telemetry.

    Returns:
    -------
    LspContractStateV1
        Canonical state struct for front-door degradation semantics.
    """
    attempted_count = max(0, int(attempted))
    applied_count = max(0, int(applied))
    failed_count = max(0, int(failed))
    timed_out_count = max(0, int(timed_out))
    normalized_reasons = tuple(dict.fromkeys(reason for reason in reasons if reason))

    if not available:
        return LspContractStateV1(
            provider=provider,
            available=False,
            status="unavailable",
            reasons=normalized_reasons,
        )
    if attempted_count <= 0:
        return LspContractStateV1(
            provider=provider,
            available=True,
            status="skipped",
            reasons=normalized_reasons,
        )
    if applied_count <= 0:
        return LspContractStateV1(
            provider=provider,
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
        provider=provider,
        available=True,
        attempted=attempted_count,
        applied=applied_count,
        failed=failed_count,
        timed_out=timed_out_count,
        status=status,
        reasons=normalized_reasons,
    )


__all__ = [
    "LspContractStateV1",
    "LspProvider",
    "LspStatus",
    "derive_lsp_contract_state",
]

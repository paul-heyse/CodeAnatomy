"""Canonical static semantic contract-state helpers for CQ front-door semantics."""

from __future__ import annotations

from typing import Literal

from tools.cq.core.structs import CqStruct

SemanticProvider = Literal["python_static", "rust_static", "none"]
SemanticStatus = Literal["unavailable", "skipped", "failed", "partial", "ok"]


class SemanticContractStateV1(CqStruct, frozen=True):
    """Deterministic static-semantic state for front-door degradation semantics."""

    provider: SemanticProvider = "none"
    available: bool = False
    attempted: int = 0
    applied: int = 0
    failed: int = 0
    timed_out: int = 0
    status: SemanticStatus = "unavailable"
    reasons: tuple[str, ...] = ()


class SemanticContractStateInputV1(CqStruct, frozen=True):
    """Input envelope for deterministic semantic contract-state derivation."""

    provider: SemanticProvider
    available: bool
    attempted: int = 0
    applied: int = 0
    failed: int = 0
    timed_out: int = 0
    reasons: tuple[str, ...] = ()


def derive_semantic_contract_state(
    input_state: SemanticContractStateInputV1,
) -> SemanticContractStateV1:
    """Derive canonical semantic state from capability + attempt telemetry.

    Returns:
        SemanticContractStateV1: Normalized semantic contract state.
    """
    attempted_count = max(0, int(input_state.attempted))
    applied_count = max(0, int(input_state.applied))
    failed_count = max(0, int(input_state.failed))
    timed_out_count = max(0, int(input_state.timed_out))
    normalized_reasons = tuple(dict.fromkeys(reason for reason in input_state.reasons if reason))

    if not input_state.available:
        return SemanticContractStateV1(
            provider=input_state.provider,
            available=False,
            status="unavailable",
            reasons=normalized_reasons,
        )
    if attempted_count <= 0:
        return SemanticContractStateV1(
            provider=input_state.provider,
            available=True,
            status="skipped",
            reasons=normalized_reasons,
        )
    if applied_count <= 0:
        return SemanticContractStateV1(
            provider=input_state.provider,
            available=True,
            attempted=attempted_count,
            failed=max(failed_count, attempted_count),
            timed_out=timed_out_count,
            status="failed",
            reasons=normalized_reasons,
        )

    status: SemanticStatus = (
        "ok" if failed_count <= 0 and applied_count >= attempted_count else "partial"
    )
    return SemanticContractStateV1(
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
    "SemanticContractStateInputV1",
    "SemanticContractStateV1",
    "SemanticProvider",
    "SemanticStatus",
    "derive_semantic_contract_state",
]

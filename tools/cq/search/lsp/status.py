"""Canonical LSP status derivation for CQ contract semantics."""

from __future__ import annotations

from enum import StrEnum

from tools.cq.core.structs import CqStruct


class LspStatus(StrEnum):
    """Canonical LSP status values for front-door degradation."""

    unavailable = "unavailable"
    skipped = "skipped"
    failed = "failed"
    partial = "partial"
    ok = "ok"


class LspStatusTelemetry(CqStruct, frozen=True):
    """Typed telemetry for deterministic LSP state derivation."""

    available: bool = False
    attempted: int = 0
    applied: int = 0
    failed: int = 0
    timed_out: int = 0


def derive_lsp_status(telemetry: LspStatusTelemetry) -> LspStatus:
    """Derive canonical LSP status from attempt/apply counters.

    Returns:
        Canonical LSP status classification.
    """
    if not telemetry.available:
        return LspStatus.unavailable
    if telemetry.attempted <= 0:
        return LspStatus.skipped
    if telemetry.applied <= 0:
        return LspStatus.failed
    if telemetry.failed > 0 or telemetry.applied < telemetry.attempted:
        return LspStatus.partial
    return LspStatus.ok


__all__ = ["LspStatus", "LspStatusTelemetry", "derive_lsp_status"]

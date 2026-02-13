from __future__ import annotations

from tools.cq.search.lsp.status import LspStatus, LspStatusTelemetry, derive_lsp_status


def test_lsp_status_unavailable() -> None:
    status = derive_lsp_status(LspStatusTelemetry(available=False))
    assert status is LspStatus.unavailable


def test_lsp_status_skipped() -> None:
    status = derive_lsp_status(LspStatusTelemetry(available=True, attempted=0))
    assert status is LspStatus.skipped


def test_lsp_status_failed() -> None:
    status = derive_lsp_status(LspStatusTelemetry(available=True, attempted=2, applied=0, failed=2))
    assert status is LspStatus.failed


def test_lsp_status_partial() -> None:
    status = derive_lsp_status(LspStatusTelemetry(available=True, attempted=3, applied=2, failed=1))
    assert status is LspStatus.partial


def test_lsp_status_ok() -> None:
    status = derive_lsp_status(LspStatusTelemetry(available=True, attempted=2, applied=2, failed=0))
    assert status is LspStatus.ok

"""Call-analysis dispatch helpers."""

from __future__ import annotations

from pathlib import Path

from tools.cq.macros.calls.entry import scan_call_sites
from tools.cq.macros.contracts import CallsRequest

__all__ = ["dispatch_call_analysis"]


def dispatch_call_analysis(
    request: CallsRequest,
    *,
    root: Path,
) -> object:
    """Dispatch call-site scanning for calls macro request.

    Returns:
        object: `CallScanResult` payload produced by `scan_call_sites`.
    """
    return scan_call_sites(root, request.function_name)

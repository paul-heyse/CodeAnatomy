"""Unit tests for calls entry dispatch helpers."""

from __future__ import annotations

from pathlib import Path
from typing import cast

import pytest
from tools.cq.core.toolchain import Toolchain
from tools.cq.macros.calls.entry_dispatch import dispatch_call_analysis
from tools.cq.macros.contracts import CallsRequest


def test_dispatch_call_analysis_delegates(monkeypatch: pytest.MonkeyPatch) -> None:
    """Dispatch helper should forward root/name into scan_call_sites."""
    sentinel = object()

    def fake_scan(root: Path, function_name: str) -> object:
        assert root == Path()
        assert function_name == "pkg.fn"
        return sentinel

    monkeypatch.setattr("tools.cq.macros.calls.entry_dispatch.scan_call_sites", fake_scan)
    request = CallsRequest(
        tc=cast("Toolchain", object()),
        root=Path(),
        argv=[],
        function_name="pkg.fn",
    )
    assert dispatch_call_analysis(request, root=Path()) is sentinel

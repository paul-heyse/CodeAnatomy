"""Tests for shared macro contracts."""

from __future__ import annotations

from pathlib import Path

from tools.cq.macros.contracts import MacroExecutionRequestV1, MacroTargetResolutionV1


def test_macro_execution_request_defaults() -> None:
    req = MacroExecutionRequestV1(root=Path())
    assert req.include == ()
    assert req.exclude == ()


def test_macro_target_resolution_defaults() -> None:
    resolved = MacroTargetResolutionV1(target="foo")
    assert resolved.files == ()

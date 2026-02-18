"""Tests for extension runtime capability helpers."""

from __future__ import annotations

import pytest

from datafusion_engine.udf import extension_runtime


def test_validate_runtime_capabilities_delegates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Runtime compatibility alias should delegate to extension validator."""
    monkeypatch.setattr(
        extension_runtime, "validate_extension_capabilities", lambda **_kwargs: {"ok": True}
    )
    assert extension_runtime.validate_runtime_capabilities() == {"ok": True}


def test_capability_report_delegates(monkeypatch: pytest.MonkeyPatch) -> None:
    """Capability report alias should delegate to the canonical helper."""
    monkeypatch.setattr(
        extension_runtime, "extension_capabilities_report", lambda: {"available": True}
    )
    assert extension_runtime.capability_report()["available"] is True


def test_validate_extension_capabilities_strict_raises_on_mismatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Strict capability validation should hard-fail on ABI incompatibility."""
    monkeypatch.setattr(
        extension_runtime,
        "extension_capabilities_report",
        lambda: {
            "available": True,
            "compatible": False,
            "error": "abi mismatch",
        },
    )
    with pytest.raises(RuntimeError, match="abi mismatch"):
        extension_runtime.validate_extension_capabilities(strict=True)

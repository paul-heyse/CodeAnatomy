"""Tests for extension validation delegation helpers."""

from __future__ import annotations

import pytest

from datafusion_engine.udf import extension_validation


def test_validate_runtime_capabilities_delegates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Runtime capability validation delegates to extension validator."""
    monkeypatch.setattr(
        extension_validation, "validate_extension_capabilities", lambda **_kwargs: {"ok": True}
    )
    assert extension_validation.validate_runtime_capabilities() == {"ok": True}


def test_capability_report_delegates(monkeypatch: pytest.MonkeyPatch) -> None:
    """Capability report delegates to extension capability report helper."""
    monkeypatch.setattr(
        extension_validation, "extension_capabilities_report", lambda: {"available": True}
    )
    assert extension_validation.capability_report()["available"] is True

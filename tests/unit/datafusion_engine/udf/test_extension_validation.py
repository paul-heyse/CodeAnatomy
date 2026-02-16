# ruff: noqa: D100, D103, ANN001, INP001
from __future__ import annotations

from datafusion_engine.udf import extension_validation


def test_validate_runtime_capabilities_delegates(monkeypatch) -> None:
    monkeypatch.setattr(
        extension_validation, "validate_extension_capabilities", lambda **_kwargs: {"ok": True}
    )
    assert extension_validation.validate_runtime_capabilities() == {"ok": True}


def test_capability_report_delegates(monkeypatch) -> None:
    monkeypatch.setattr(
        extension_validation, "extension_capabilities_report", lambda: {"available": True}
    )
    assert extension_validation.capability_report()["available"] is True

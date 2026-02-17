"""Tests for UDF extension lifecycle delegation helpers."""

from __future__ import annotations

import pytest
from datafusion import SessionContext

from datafusion_engine.udf import extension_lifecycle


def test_install_udfs_delegates(monkeypatch: pytest.MonkeyPatch) -> None:
    """UDF install delegates to rust registration helper."""
    monkeypatch.setattr(
        extension_lifecycle, "register_rust_udfs", lambda *_args, **_kwargs: {"ok": True}
    )
    assert extension_lifecycle.install_udfs(SessionContext()) == {"ok": True}


def test_install_udf_ddl_delegates(monkeypatch: pytest.MonkeyPatch) -> None:
    """DDL install delegates to register_udfs_via_ddl helper."""
    calls: list[dict[str, object]] = []

    def _fake_register(*_args: object, **kwargs: object) -> None:
        calls.append(kwargs)

    monkeypatch.setattr(extension_lifecycle, "register_udfs_via_ddl", _fake_register)
    extension_lifecycle.install_udf_ddl(SessionContext(), snapshot={"scalar": []})
    assert calls

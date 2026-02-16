# ruff: noqa: D100, D103, ANN001, ANN002, ANN003, ANN202, INP001
from __future__ import annotations

from datafusion import SessionContext

from datafusion_engine.udf import extension_lifecycle


def test_install_udfs_delegates(monkeypatch) -> None:
    monkeypatch.setattr(
        extension_lifecycle, "register_rust_udfs", lambda *_args, **_kwargs: {"ok": True}
    )
    assert extension_lifecycle.install_udfs(SessionContext()) == {"ok": True}


def test_install_udf_ddl_delegates(monkeypatch) -> None:
    calls: list[dict[str, object]] = []

    def _fake_register(*_args, **kwargs):
        calls.append(kwargs)

    monkeypatch.setattr(extension_lifecycle, "register_udfs_via_ddl", _fake_register)
    extension_lifecycle.install_udf_ddl(SessionContext(), snapshot={"scalar": []})
    assert calls

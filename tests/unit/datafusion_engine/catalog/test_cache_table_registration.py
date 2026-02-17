"""Unit tests for cache-table registration via extension contracts."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from datafusion_engine.catalog import introspection

if TYPE_CHECKING:
    from datafusion import SessionContext
else:

    class SessionContext:
        """Fallback session context for environments without datafusion."""


def test_register_cache_tables_uses_direct_extension_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test register cache tables uses direct extension path."""
    captured: dict[str, object] = {"ctx": None, "payload": None}

    monkeypatch.setattr(
        introspection.datafusion_ext,
        "register_cache_tables",
        lambda ctx, payload: captured.update({"ctx": ctx, "payload": payload}),
    )
    monkeypatch.setattr(
        introspection,
        "_cache_table_registration_payload",
        lambda _ctx: {
            "list_files_cache_ttl": "2m",
            "list_files_cache_limit": "128 MiB",
            "metadata_cache_limit": "256 MiB",
            "predicate_cache_size": "64 MiB",
        },
    )
    ctx: SessionContext = SessionContext()
    introspection.register_cache_introspection_functions(ctx=ctx)

    assert captured["ctx"] is ctx
    payload = captured["payload"]
    assert isinstance(payload, dict)
    assert all(isinstance(value, str) for value in payload.values())


def test_register_cache_tables_raises_abi_mismatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Raise an ABI mismatch when extension registers with incompatible SessionContext."""

    def _raise_abi_mismatch(_ctx: object, _payload: dict[str, str]) -> None:
        msg = "argument 'ctx': cannot be converted"
        raise RuntimeError(msg)

    monkeypatch.setattr(
        introspection.datafusion_ext,
        "register_cache_tables",
        _raise_abi_mismatch,
    )
    monkeypatch.setattr(
        introspection,
        "_cache_table_registration_payload",
        lambda _ctx: {"list_files_cache_ttl": "2m"},
    )
    with pytest.raises(RuntimeError, match="SessionContext ABI mismatch"):
        introspection.register_cache_introspection_functions(ctx=SessionContext())

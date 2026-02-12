"""Unit tests for cache-table registration via extension contracts."""

from __future__ import annotations

import pytest

from datafusion_engine.catalog import introspection


def test_register_cache_tables_uses_direct_extension_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {"ctx": None, "payload": None}

    class _Module:
        def register_cache_tables(self, ctx: object, payload: dict[str, str]) -> None:
            captured["ctx"] = ctx
            captured["payload"] = payload

    module = _Module()
    monkeypatch.setattr(
        introspection,
        "resolve_extension_module",
        lambda *_args, **_kwargs: ("datafusion_ext", module),
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
    ctx = object()
    introspection.register_cache_introspection_functions(ctx=ctx)  # type: ignore[arg-type]

    assert captured["ctx"] is ctx
    payload = captured["payload"]
    assert isinstance(payload, dict)
    assert all(isinstance(value, str) for value in payload.values())


def test_register_cache_tables_raises_abi_mismatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _Module:
        def register_cache_tables(self, _ctx: object, _payload: dict[str, str]) -> None:
            msg = "argument 'ctx': cannot be converted"
            raise RuntimeError(msg)

    monkeypatch.setattr(
        introspection,
        "resolve_extension_module",
        lambda *_args, **_kwargs: ("datafusion_ext", _Module()),
    )
    monkeypatch.setattr(
        introspection,
        "_cache_table_registration_payload",
        lambda _ctx: {"list_files_cache_ttl": "2m"},
    )
    with pytest.raises(RuntimeError, match="SessionContext ABI mismatch"):
        introspection.register_cache_introspection_functions(ctx=object())  # type: ignore[arg-type]

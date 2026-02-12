"""Unit tests for cache-table registration via extension contracts."""

from __future__ import annotations

import logging
from typing import Any

import pytest

from datafusion_engine.catalog import introspection


def test_register_cache_tables_uses_direct_extension_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    class _Module:
        pass

    monkeypatch.setattr(
        introspection,
        "resolve_extension_module",
        lambda *_args, **_kwargs: ("datafusion_ext", _Module()),
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

    def _invoke(
        module_name: str,
        module: object,
        entrypoint: str,
        invocation: Any,
    ) -> tuple[object, None]:
        captured["module_name"] = module_name
        captured["module"] = module
        captured["entrypoint"] = entrypoint
        captured["args"] = invocation.args
        return object(), None

    monkeypatch.setattr(introspection, "invoke_entrypoint_with_adapted_context", _invoke)

    introspection.register_cache_introspection_functions(ctx=object())  # type: ignore[arg-type]

    assert captured["module_name"] == "datafusion_ext"
    assert captured["entrypoint"] == "register_cache_tables"
    args = captured["args"]
    assert isinstance(args, tuple)
    assert len(args) == 1
    payload = args[0]
    assert isinstance(payload, dict)
    assert all(isinstance(value, str) for value in payload.values())


def test_register_cache_tables_logs_abi_mismatch(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    class _Module:
        pass

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
    monkeypatch.setattr(
        introspection,
        "invoke_entrypoint_with_adapted_context",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            RuntimeError("argument 'ctx': cannot be converted")
        ),
    )

    with caplog.at_level(logging.WARNING):
        introspection.register_cache_introspection_functions(ctx=object())  # type: ignore[arg-type]

    assert "SessionContext ABI mismatch" in caplog.text

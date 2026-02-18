"""Tests for extension registry isolation and caching behavior."""

from __future__ import annotations

from types import SimpleNamespace

import msgspec
import pytest
from datafusion import SessionContext

from datafusion_engine.udf import extension_runtime
from datafusion_engine.udf.extension_runtime import ExtensionRegistries

CONTRACT_VERSION = 3
FIRST_REVISION = 1
SECOND_REVISION = 2


def test_extension_registries_isolate_context_tracking() -> None:
    """Registry context sets are isolated between instances."""
    ctx = SessionContext()
    first = ExtensionRegistries()
    second = ExtensionRegistries()

    first.udf_contexts.add(ctx)

    assert ctx in first.udf_contexts
    assert ctx not in second.udf_contexts


def test_rust_runtime_install_payload_reads_injected_registry() -> None:
    """Injected registry payload is surfaced by runtime payload helper."""
    ctx = SessionContext()
    registries = ExtensionRegistries()
    registries.runtime_payloads[ctx] = {
        "contract_version": CONTRACT_VERSION,
        "runtime_install_mode": "modular",
    }

    payload = extension_runtime.rust_runtime_install_payload(ctx, registries=registries)

    assert payload["contract_version"] == CONTRACT_VERSION
    assert payload["runtime_install_mode"] == "modular"


def test_rust_udf_docs_cache_is_scoped_by_registry(monkeypatch: pytest.MonkeyPatch) -> None:
    """Docs cache is keyed by registry instance."""
    ctx = SessionContext()
    first = ExtensionRegistries()
    second = ExtensionRegistries()
    calls: list[int] = []

    def _fake_docs_snapshot(_ctx: SessionContext) -> dict[str, object]:
        calls.append(1)
        return {"revision": len(calls)}

    monkeypatch.setattr(
        "datafusion_engine.udf.extension_runtime._build_docs_snapshot",
        _fake_docs_snapshot,
    )

    docs_first = extension_runtime.rust_udf_docs(ctx, registries=first)
    docs_first_cached = extension_runtime.rust_udf_docs(ctx, registries=first)
    docs_second = extension_runtime.rust_udf_docs(ctx, registries=second)

    assert docs_first["revision"] == FIRST_REVISION
    assert docs_first_cached["revision"] == FIRST_REVISION
    assert docs_second["revision"] == SECOND_REVISION


def test_runtime_install_snapshot_uses_keyword_runtime_args(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Runtime install snapshot forwards async settings via keyword arguments."""
    ctx = SessionContext()
    registries = ExtensionRegistries()
    calls: list[dict[str, object]] = []

    def _install_codeanatomy_runtime(
        runtime_ctx: SessionContext,
        *,
        enable_async_udfs: bool,
        async_udf_timeout_ms: int | None,
        async_udf_batch_size: int | None,
    ) -> dict[str, object]:
        calls.append(
            {
                "ctx": runtime_ctx,
                "enable_async_udfs": enable_async_udfs,
                "async_udf_timeout_ms": async_udf_timeout_ms,
                "async_udf_batch_size": async_udf_batch_size,
            }
        )
        return {
            "snapshot_msgpack": msgspec.msgpack.encode(
                {
                    "version": 1,
                    "scalar": [],
                    "aggregate": [],
                    "window": [],
                    "table": [],
                    "aliases": {},
                    "parameter_names": {},
                    "volatility": {},
                    "rewrite_tags": {},
                    "signature_inputs": {},
                    "return_types": {},
                    "simplify": {},
                    "coerce_types": {},
                    "short_circuits": {},
                    "config_defaults": {},
                    "custom_udfs": [],
                    "pycapsule_udfs": [],
                }
            )
        }

    module = SimpleNamespace(
        __name__="datafusion._internal._test_stub",
        install_codeanatomy_runtime=_install_codeanatomy_runtime,
    )
    monkeypatch.setattr(extension_runtime, "_datafusion_internal", lambda: module)

    snapshot = extension_runtime.__dict__["_install_codeanatomy_runtime_snapshot"](
        ctx,
        enable_async=True,
        async_udf_timeout_ms=321,
        async_udf_batch_size=64,
        registries=registries,
    )

    assert calls == [
        {
            "ctx": ctx,
            "enable_async_udfs": True,
            "async_udf_timeout_ms": 321,
            "async_udf_batch_size": 64,
        }
    ]
    assert snapshot["version"] == 1


def test_runtime_install_snapshot_prefers_msgpack_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Runtime install snapshot decodes typed snapshot_msgpack payloads."""
    ctx = SessionContext()
    registries = ExtensionRegistries()
    snapshot_msgpack = msgspec.msgpack.encode(
        {
            "version": 1,
            "scalar": ["stable_id"],
            "aggregate": [],
            "window": [],
            "table": [],
            "aliases": {},
            "parameter_names": {"stable_id": []},
            "volatility": {"stable_id": "stable"},
            "rewrite_tags": {},
            "signature_inputs": {"stable_id": [["Utf8"]]},
            "return_types": {"stable_id": ["Utf8"]},
            "simplify": {"stable_id": True},
            "coerce_types": {"stable_id": False},
            "short_circuits": {"stable_id": False},
            "config_defaults": {},
            "custom_udfs": ["stable_id"],
            "pycapsule_udfs": [],
        }
    )

    module = SimpleNamespace(
        __name__="datafusion._internal._test_stub",
        install_codeanatomy_runtime=lambda *_args, **_kwargs: {
            "snapshot_msgpack": snapshot_msgpack,
        },
    )
    monkeypatch.setattr(extension_runtime, "_datafusion_internal", lambda: module)

    snapshot = extension_runtime.__dict__["_install_codeanatomy_runtime_snapshot"](
        ctx,
        enable_async=False,
        async_udf_timeout_ms=None,
        async_udf_batch_size=None,
        registries=registries,
    )

    assert list(snapshot["scalar"]) == ["stable_id"]
    runtime_payload = registries.runtime_payloads[ctx]
    assert runtime_payload["snapshot_msgpack"] == snapshot_msgpack

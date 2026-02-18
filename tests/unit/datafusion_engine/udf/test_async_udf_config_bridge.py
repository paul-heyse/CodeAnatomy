"""Tests for async UDF policy propagation in canonical runtime install."""

from __future__ import annotations

from collections.abc import Mapping
from types import SimpleNamespace
from typing import cast

import msgspec
import pytest
from datafusion import SessionContext

from datafusion_engine.udf import extension_runtime
from datafusion_engine.udf.extension_runtime import AsyncUdfPolicy, ExtensionRegistries


def test_runtime_install_bridge_passes_async_udf_policy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Runtime bridge forwards async policy through keyword runtime args."""
    ctx = SessionContext()
    registries = ExtensionRegistries()
    call_log: list[tuple[str, Mapping[str, object] | None]] = []

    internal = SimpleNamespace(
        __name__="datafusion._internal._test_stub",
        install_codeanatomy_runtime=lambda *_args, **_kwargs: None,
    )

    def _fake_invoke(
        _internal: object,
        entrypoint: str,
        *,
        ctx: SessionContext,
        args: tuple[object, ...] = (),
        kwargs: Mapping[str, object] | None = None,
    ) -> object:
        del ctx, args
        call_log.append((entrypoint, kwargs))
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

    monkeypatch.setattr(extension_runtime, "_invoke_runtime_entrypoint", _fake_invoke)
    monkeypatch.setattr(extension_runtime, "_datafusion_internal", lambda: internal)
    registries.udf_policies[ctx] = AsyncUdfPolicy(enabled=True, timeout_ms=321, batch_size=64)

    extension_runtime.__dict__["_build_registry_snapshot"](ctx, registries=registries)

    assert call_log == [
        (
            "install_codeanatomy_runtime",
            {
                "enable_async_udfs": True,
                "async_udf_timeout_ms": 321,
                "async_udf_batch_size": 64,
            },
        )
    ]
    runtime_payload = registries.runtime_payloads[ctx]
    assert runtime_payload["runtime_install_mode"] == "unified"
    snapshot_payload = cast("Mapping[str, object]", runtime_payload["snapshot"])
    assert snapshot_payload["async_udf_policy"] == {
        "enabled": True,
        "timeout_ms": 321,
        "batch_size": 64,
    }

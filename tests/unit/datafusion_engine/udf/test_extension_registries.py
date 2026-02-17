"""Tests for extension registry isolation and caching behavior."""

from __future__ import annotations

import pytest
from datafusion import SessionContext

from datafusion_engine.udf import extension_core
from datafusion_engine.udf.extension_core import ExtensionRegistries

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

    payload = extension_core.rust_runtime_install_payload(ctx, registries=registries)

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
        "datafusion_engine.udf.extension_snapshot_runtime._build_docs_snapshot",
        _fake_docs_snapshot,
    )

    docs_first = extension_core.rust_udf_docs(ctx, registries=first)
    docs_first_cached = extension_core.rust_udf_docs(ctx, registries=first)
    docs_second = extension_core.rust_udf_docs(ctx, registries=second)

    assert docs_first["revision"] == FIRST_REVISION
    assert docs_first_cached["revision"] == FIRST_REVISION
    assert docs_second["revision"] == SECOND_REVISION

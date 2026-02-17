# ruff: noqa: D100, D103, ANN001, INP001, PLR2004
from __future__ import annotations

from datafusion import SessionContext

from datafusion_engine.udf import extension_core
from datafusion_engine.udf.extension_core import ExtensionRegistries


def test_extension_registries_isolate_context_tracking() -> None:
    ctx = SessionContext()
    first = ExtensionRegistries()
    second = ExtensionRegistries()

    first.udf_contexts.add(ctx)

    assert ctx in first.udf_contexts
    assert ctx not in second.udf_contexts


def test_rust_runtime_install_payload_reads_injected_registry() -> None:
    ctx = SessionContext()
    registries = ExtensionRegistries()
    registries.runtime_payloads[ctx] = {"contract_version": 3, "runtime_install_mode": "modular"}

    payload = extension_core.rust_runtime_install_payload(ctx, registries=registries)

    assert payload["contract_version"] == 3
    assert payload["runtime_install_mode"] == "modular"


def test_rust_udf_docs_cache_is_scoped_by_registry(monkeypatch) -> None:
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

    assert docs_first["revision"] == 1
    assert docs_first_cached["revision"] == 1
    assert docs_second["revision"] == 2

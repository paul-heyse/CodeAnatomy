# ruff: noqa: D100, D103, ANN001, INP001
from __future__ import annotations

from types import SimpleNamespace

from datafusion import SessionContext

from datafusion_engine.udf import extension_core
from datafusion_engine.udf.extension_core import ExtensionRegistries


def test_modular_runtime_bridge_passes_async_udf_policy(monkeypatch) -> None:
    ctx = SessionContext()
    registries = ExtensionRegistries()
    call_log: list[tuple[str, tuple[object, ...]]] = []

    internal = SimpleNamespace(
        __name__="datafusion._internal._test_stub",
        register_codeanatomy_udfs=lambda *_args, **_kwargs: None,
        registry_snapshot=lambda *_args, **_kwargs: {},
        install_codeanatomy_udf_config=lambda *_args, **_kwargs: None,
    )

    def _fake_invoke(
        _internal: object,
        entrypoint: str,
        *,
        ctx: SessionContext,
        args: tuple[object, ...] = (),
    ) -> object:
        del ctx
        call_log.append((entrypoint, args))
        if entrypoint == "registry_snapshot":
            empty_items: list[str] = []
            return {
                "scalar": ["stable_hash64"],
                "aggregate": list(empty_items),
                "window": list(empty_items),
                "table": list(empty_items),
            }
        return None

    monkeypatch.setattr(extension_core, "_invoke_runtime_entrypoint", _fake_invoke)
    monkeypatch.setattr(
        extension_core,
        "_supplement_expr_surface_snapshot",
        lambda payload, *, ctx: (ctx, payload)[1],
    )
    monkeypatch.setattr(
        "datafusion_engine.udf.factory.function_factory_policy_from_snapshot",
        lambda snapshot, allow_async: {"allow_async": allow_async, "snapshot_size": len(snapshot)},
    )
    monkeypatch.setattr(
        "datafusion_engine.udf.factory.install_function_factory",
        lambda _ctx, policy: policy,
    )
    monkeypatch.setattr(
        "datafusion_engine.expr.domain_planner.domain_planner_names_from_snapshot",
        lambda _snapshot: ("codeanatomy_domain",),
    )
    monkeypatch.setattr(
        "datafusion_engine.expr.planner.install_expr_planners",
        lambda _ctx, planner_names: planner_names,
    )

    install_runtime = extension_core.__dict__["_install_runtime_via_modular_entrypoints"]
    payload = install_runtime(
        internal,
        ctx=ctx,
        enable_async=True,
        async_udf_timeout_ms=321,
        async_udf_batch_size=64,
        registries=registries,
    )

    assert ("register_codeanatomy_udfs", (True, 321, 64)) in call_log
    assert payload["async"] == {
        "enabled": True,
        "timeout_ms": 321,
        "batch_size": 64,
    }

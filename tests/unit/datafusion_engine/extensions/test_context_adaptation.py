"""Unit tests for extension context adaptation invocation behavior."""

from __future__ import annotations

from types import SimpleNamespace

from datafusion_engine.extensions.context_adaptation import (
    ExtensionEntrypointInvocation,
    invoke_entrypoint_with_adapted_context,
)


def test_invoke_entrypoint_forwards_kwargs() -> None:
    """Keyword arguments are forwarded to extension entrypoints."""
    marker_ctx = object()
    seen: dict[str, object] = {}

    def _install_runtime(
        ctx: object,
        *,
        enable_async_udfs: bool,
        async_udf_timeout_ms: int | None,
        async_udf_batch_size: int | None,
    ) -> dict[str, object]:
        seen.update(
            {
                "ctx": ctx,
                "enable_async_udfs": enable_async_udfs,
                "async_udf_timeout_ms": async_udf_timeout_ms,
                "async_udf_batch_size": async_udf_batch_size,
            }
        )
        return {"ok": True}

    module = SimpleNamespace(install_codeanatomy_runtime=_install_runtime)
    selection, payload = invoke_entrypoint_with_adapted_context(
        "datafusion._internal._test_stub",
        module,
        "install_codeanatomy_runtime",
        ExtensionEntrypointInvocation(
            ctx=marker_ctx,
            kwargs={
                "enable_async_udfs": True,
                "async_udf_timeout_ms": 321,
                "async_udf_batch_size": 64,
            },
            allow_fallback=False,
        ),
    )

    assert selection.ctx_kind == "outer"
    assert selection.ctx is marker_ctx
    assert payload == {"ok": True}
    assert seen == {
        "ctx": marker_ctx,
        "enable_async_udfs": True,
        "async_udf_timeout_ms": 321,
        "async_udf_batch_size": 64,
    }


def test_invoke_entrypoint_forwards_args_and_kwargs() -> None:
    """Positional args and kwargs are forwarded together."""
    marker_ctx = object()

    def _entrypoint(ctx: object, left: str, right: str, *, suffix: str) -> tuple[object, ...]:
        return (ctx, left, right, suffix)

    module = SimpleNamespace(collect_payload=_entrypoint)
    _selection, payload = invoke_entrypoint_with_adapted_context(
        "datafusion._internal._test_stub",
        module,
        "collect_payload",
        ExtensionEntrypointInvocation(
            ctx=marker_ctx,
            args=("lhs", "rhs"),
            kwargs={"suffix": "joined"},
            allow_fallback=False,
        ),
    )

    assert payload == (marker_ctx, "lhs", "rhs", "joined")

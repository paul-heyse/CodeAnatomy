"""Contract tests for strict context adaptation defaults."""

from __future__ import annotations

import pytest

from datafusion_engine.delta.capabilities import delta_context_candidates
from datafusion_engine.extensions.context_adaptation import (
    ExtensionContextPolicy,
    ExtensionEntrypointInvocation,
    invoke_entrypoint_with_adapted_context,
    select_context_candidate,
)


class _Module:
    pass


def test_context_policy_defaults_to_no_fallback() -> None:
    """Policy defaults disable fallback contexts."""
    policy = ExtensionContextPolicy(module_names=("m",), entrypoint="install_codeanatomy_runtime")
    assert policy.allow_fallback is False


def test_entrypoint_invocation_defaults_to_no_fallback() -> None:
    """Entrypoint invocation defaults disable fallback contexts."""
    invocation = ExtensionEntrypointInvocation(ctx=object())
    assert invocation.allow_fallback is False


def test_select_context_candidate_excludes_fallback_by_default() -> None:
    """Selector returns only outer/internal candidates by default."""
    outer = object()
    internal = object()
    fallback = object()
    candidates = select_context_candidate(
        outer,
        internal_ctx=internal,
        fallback_ctx=fallback,
    )
    assert candidates == (("outer", outer), ("internal", internal))


def test_delta_context_candidates_are_non_fallback_by_default() -> None:
    """Delta candidate resolution excludes fallback by default."""

    class _Ctx:
        def __init__(self) -> None:
            self.ctx = object()

    ctx = _Ctx()
    candidates = delta_context_candidates(ctx, _Module())
    assert candidates[0][0] == "outer"
    assert all(kind != "fallback" for kind, _ in candidates)


def test_required_entrypoints_do_not_probe_fallback_candidates() -> None:
    """Required runtime entrypoints should ignore fallback candidates."""

    class _RuntimeModule:
        @staticmethod
        def install_codeanatomy_runtime(ctx: object) -> object:
            _ = ctx
            msg = "ctx mismatch"
            raise RuntimeError(msg)

    with pytest.raises(RuntimeError, match="context candidates") as excinfo:
        invoke_entrypoint_with_adapted_context(
            "runtime_module",
            _RuntimeModule(),
            "install_codeanatomy_runtime",
            ExtensionEntrypointInvocation(
                ctx=object(),
                internal_ctx=object(),
                allow_fallback=True,
                fallback_ctx_factory=lambda _module: object(),
            ),
        )
    assert "fallback" not in str(excinfo.value)

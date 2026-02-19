"""Tests for runtime type planner installation wiring."""

from __future__ import annotations

from types import SimpleNamespace
from typing import TYPE_CHECKING, cast

import pytest

from datafusion_engine.session import runtime_extensions

if TYPE_CHECKING:
    from datafusion import SessionContext

    from datafusion_engine.session.runtime import DataFusionRuntimeProfile


def test_install_expr_planners_uses_unified_installer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Expr planner install should also wire relation/type planner entrypoints."""
    calls: list[str] = []

    profile = cast(
        "DataFusionRuntimeProfile",
        SimpleNamespace(
            features=SimpleNamespace(enable_expr_planners=True),
            policies=SimpleNamespace(
                expr_planner_hook=None,
                expr_planner_names=("codeanatomy_domain",),
                type_planner_hook=None,
            ),
        ),
    )

    monkeypatch.setattr(runtime_extensions, "_record_expr_planners", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(runtime_extensions, "expr_planner_extension_available", lambda: True)
    monkeypatch.setattr(runtime_extensions, "relation_planner_extension_available", lambda: True)
    monkeypatch.setattr(
        runtime_extensions,
        "install_expr_planners",
        lambda _ctx, *, planner_names: calls.append(f"expr:{','.join(planner_names)}"),
    )
    monkeypatch.setattr(
        runtime_extensions,
        "install_relation_planner",
        lambda _ctx: calls.append("relation"),
    )
    monkeypatch.setattr(
        "datafusion_engine.extensions.datafusion_ext.install_type_planner",
        lambda _ctx: calls.append("type"),
    )

    runtime_extensions._install_expr_planners(  # noqa: SLF001
        profile,
        cast("SessionContext", object()),
    )

    assert calls == ["expr:codeanatomy_domain", "relation", "type"]

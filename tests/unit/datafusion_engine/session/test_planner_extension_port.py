"""Tests for planner extension protocol wiring."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from datafusion_engine.session import runtime_extensions


class _PlannerPlugin:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def install_expr_planners(self, ctx: object) -> None:
        _ = ctx
        self.calls.append("expr")

    def install_relation_planner(self, ctx: object) -> None:
        _ = ctx
        self.calls.append("relation")


def test_install_expr_planners_uses_planner_extension_port(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Planner protocol object installs expr+relation planners directly."""
    plugin = _PlannerPlugin()
    install_expr_planners = runtime_extensions.__dict__["_install_expr_planners"]

    profile = SimpleNamespace(
        features=SimpleNamespace(enable_expr_planners=True),
        policies=SimpleNamespace(expr_planner_hook=plugin, expr_planner_names=("default",)),
    )

    def _record_noop(*_args: object, **_kwargs: object) -> None:
        return None

    def _unexpected_fallback(*_args: object, **_kwargs: object) -> None:
        msg = "fallback not expected"
        raise AssertionError(msg)

    monkeypatch.setattr(runtime_extensions, "_record_expr_planners", _record_noop)
    monkeypatch.setattr(
        runtime_extensions,
        "install_expr_planners",
        _unexpected_fallback,
    )

    install_expr_planners(profile, object())

    assert plugin.calls == ["expr", "relation"]


def test_install_expr_planners_records_unavailable_when_extension_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Missing extension should be treated as unavailable without probing ImportError."""
    install_expr_planners = runtime_extensions.__dict__["_install_expr_planners"]
    profile = SimpleNamespace(
        features=SimpleNamespace(enable_expr_planners=True),
        policies=SimpleNamespace(expr_planner_hook=None, expr_planner_names=("default",)),
    )
    recorded: dict[str, object] = {}

    def _record(profile_arg: object, **kwargs: object) -> None:
        _ = profile_arg
        recorded.update(kwargs)

    def _unexpected_install(*_args: object, **_kwargs: object) -> None:
        msg = "install_expr_planners should not be called when unavailable"
        raise AssertionError(msg)

    monkeypatch.setattr(runtime_extensions, "_record_expr_planners", _record)
    monkeypatch.setattr(runtime_extensions, "expr_planner_extension_available", lambda: False)
    monkeypatch.setattr(runtime_extensions, "install_expr_planners", _unexpected_install)

    with pytest.raises(RuntimeError, match="ExprPlanner installation failed"):
        install_expr_planners(profile, object())

    assert recorded["available"] is False
    assert recorded["installed"] is False
    assert "unavailable" in str(recorded["error"]).lower()


def test_install_function_factory_records_unavailable_when_extension_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Missing function-factory extension should be recorded as unavailable."""
    install_function_factory = runtime_extensions.__dict__["_install_function_factory"]
    profile = SimpleNamespace(
        features=SimpleNamespace(enable_function_factory=True),
        policies=SimpleNamespace(function_factory_hook=None),
    )
    recorded: dict[str, object] = {}

    def _record(profile_arg: object, **kwargs: object) -> None:
        _ = profile_arg
        recorded.update(kwargs)

    def _unexpected_install(*_args: object, **_kwargs: object) -> None:
        msg = "install_function_factory should not be called when unavailable"
        raise AssertionError(msg)

    monkeypatch.setattr(runtime_extensions, "_record_function_factory", _record)
    monkeypatch.setattr(runtime_extensions, "function_factory_extension_available", lambda: False)
    monkeypatch.setattr(runtime_extensions, "install_function_factory", _unexpected_install)

    with pytest.raises(RuntimeError, match="FunctionFactory installation failed"):
        install_function_factory(profile, object())

    assert recorded["available"] is False
    assert recorded["installed"] is False
    assert "unavailable" in str(recorded["error"]).lower()

"""Tests for launcher context purity and configuration setup separation."""

from __future__ import annotations

from tools.cq.cli_app import app as app_module


def test_build_launch_context_does_not_mutate_app_config() -> None:
    """Launch-context construction should be side-effect free."""
    original_config = app_module.app.config
    launch = app_module._build_launch_context(
        argv=["search", "target"],
        global_opts=app_module.GlobalOptionArgs(),
    )

    assert launch.argv == ["search", "target"]
    assert app_module.app.config is original_config


def test_configure_app_applies_config_chain(monkeypatch) -> None:
    """App configuration should happen in dedicated configure step."""
    sentinel_config = object()
    monkeypatch.setattr(
        app_module,
        "build_config_chain",
        lambda **kwargs: sentinel_config,
    )

    app_module._configure_app(
        app_module.ConfigOptionArgs(config=".cq.toml", use_config=True),
    )

    assert app_module.app.config is sentinel_config

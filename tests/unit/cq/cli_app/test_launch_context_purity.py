"""Tests for launcher context purity and configuration setup separation."""

from __future__ import annotations

import pytest
from tools.cq.cli_app import app as app_module


def test_build_launch_context_does_not_mutate_app_config() -> None:
    """Launch-context construction should be side-effect free."""
    original_config = app_module.app.config
    build_launch_context = app_module.__dict__["_build_launch_context"]
    launch = build_launch_context(
        argv=["search", "target"],
        global_opts=app_module.GlobalOptionArgs(),
    )

    assert launch.argv == ["search", "target"]
    assert app_module.app.config is original_config


def test_configure_app_applies_config_chain(monkeypatch: pytest.MonkeyPatch) -> None:
    """App configuration should happen in dedicated configure step."""
    original_config = app_module.app.config

    def _sentinel_provider(*_args: object, **_kwargs: object) -> dict[str, object]:
        return {}

    monkeypatch.setattr(
        app_module,
        "build_config_chain",
        lambda **_kwargs: [_sentinel_provider],
    )

    configure_app = app_module.__dict__["_configure_app"]
    try:
        configure_app(
            app_module.ConfigOptionArgs(config=".cq.toml", use_config=True),
        )
        assert app_module.app.config == (_sentinel_provider,)
    finally:
        app_module.app.config = original_config

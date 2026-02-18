"""Tests for idempotent runtime extension installation."""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from datafusion import SessionContext

from datafusion_engine.session import runtime_extensions


class _InstallerStub:
    def __init__(self) -> None:
        self.config_calls = 0
        self.physical_config_calls = 0
        self.rule_calls = 0
        self.physical_calls = 0

    def config_installer(self, *_args: object) -> None:
        self.config_calls += 1

    def physical_config_installer(self, *_args: object) -> None:
        self.physical_config_calls += 1

    def rule_installer(self, *_args: object) -> None:
        self.rule_calls += 1

    def physical_installer(self, *_args: object) -> None:
        self.physical_calls += 1


def test_install_planner_rules_is_idempotent(monkeypatch: pytest.MonkeyPatch) -> None:
    """Planner rules should install only once per SessionContext."""
    planner_installed = runtime_extensions.__dict__["_PLANNER_RULES_INSTALLED"]
    install_planner_rules = runtime_extensions.__dict__["_install_planner_rules"]
    planner_installed.clear()

    installers = _InstallerStub()
    monkeypatch.setattr(runtime_extensions, "_resolve_planner_rule_installers", lambda: installers)

    profile = SimpleNamespace(
        policies=SimpleNamespace(
            sql_policy=None,
            sql_policy_name=None,
            physical_rulepack_enabled=True,
        )
    )
    ctx = SessionContext()

    install_planner_rules(profile, ctx)
    install_planner_rules(profile, ctx)

    assert installers.config_calls == 1
    assert installers.physical_config_calls == 1
    assert installers.rule_calls == 1
    assert installers.physical_calls == 1


def test_install_cache_tables_is_idempotent(monkeypatch: pytest.MonkeyPatch) -> None:
    """Cache introspection functions should install only once per SessionContext."""
    cache_installed = runtime_extensions.__dict__["_CACHE_TABLES_INSTALLED"]
    install_cache_tables = runtime_extensions.__dict__["_install_cache_tables"]
    cache_installed.clear()

    calls = {"count": 0}

    def _register(_ctx: object) -> None:
        calls["count"] += 1

    monkeypatch.setattr(runtime_extensions, "_register_cache_introspection_functions", _register)

    profile = SimpleNamespace(
        features=SimpleNamespace(
            enable_cache_manager=True,
            cache_enabled=False,
        ),
        policies=SimpleNamespace(metadata_cache_snapshot_enabled=False),
    )
    ctx = SessionContext()

    install_cache_tables(profile, ctx)
    install_cache_tables(profile, ctx)

    assert calls["count"] == 1

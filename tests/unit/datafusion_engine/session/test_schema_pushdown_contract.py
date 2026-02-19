"""Schema-pushdown adapter registration contract tests."""

from __future__ import annotations

from types import SimpleNamespace
from typing import TYPE_CHECKING, cast

import pytest

from datafusion_engine.session import runtime_extensions

if TYPE_CHECKING:
    from datafusion import SessionContext

    from datafusion_engine.session.runtime import DataFusionRuntimeProfile


class _CtxWithoutRegister:
    pass


class _CtxWithRegister:
    def __init__(self) -> None:
        self.factory = None

    def register_physical_expr_adapter_factory(self, factory: object) -> None:
        self.factory = factory


def test_adapter_registration_fails_loudly_when_required(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Missing adapter-registration API should raise when adapter is required."""
    profile = SimpleNamespace(
        policies=SimpleNamespace(physical_expr_adapter_factory=None),
        features=SimpleNamespace(enable_schema_evolution_adapter=True),
    )

    def _factory() -> object:
        return object()

    monkeypatch.setattr(
        runtime_extensions,
        "_load_schema_evolution_adapter_factory",
        _factory,
    )

    with pytest.raises(TypeError, match="Schema evolution adapter is enabled"):
        runtime_extensions._install_physical_expr_adapter_factory(  # noqa: SLF001
            cast("DataFusionRuntimeProfile", profile),
            cast("SessionContext", _CtxWithoutRegister()),
        )


def test_adapter_registration_uses_context_register_hook() -> None:
    """Available registration hook should receive configured adapter factory."""
    marker = object()
    profile = SimpleNamespace(
        policies=SimpleNamespace(physical_expr_adapter_factory=marker),
        features=SimpleNamespace(enable_schema_evolution_adapter=False),
    )
    ctx = _CtxWithRegister()

    runtime_extensions._install_physical_expr_adapter_factory(  # noqa: SLF001
        cast("DataFusionRuntimeProfile", profile),
        cast("SessionContext", ctx),
    )

    assert ctx.factory is marker

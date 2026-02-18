"""Tests for DF52 provider argument normalization."""

from __future__ import annotations

from dataclasses import dataclass

import pytest

from datafusion_engine.extensions import datafusion_ext


@dataclass(frozen=True)
class _CtxWrapper:
    ctx: object


def test_provider_normalize_args_injects_session_positional() -> None:
    """DF52 provider methods prepend session to positional args."""
    marker_session = object()
    normalize_args = datafusion_ext.__dict__["_normalize_args"]

    args, kwargs = normalize_args(
        "__datafusion_table_provider__",
        (),
        {"session": marker_session},
    )

    assert args == (marker_session,)
    assert kwargs == {}


def test_provider_normalize_args_requires_session() -> None:
    """DF52 provider methods require a session argument."""
    normalize_args = datafusion_ext.__dict__["_normalize_args"]
    with pytest.raises(ValueError, match="requires a session argument"):
        normalize_args(
            "__datafusion_catalog_provider__",
            (),
            {},
        )


def test_regular_entrypoint_normalization_unwraps_ctx() -> None:
    """Non-provider entrypoints keep existing ctx normalization behavior."""
    inner_ctx = object()
    normalize_args = datafusion_ext.__dict__["_normalize_args"]
    args, kwargs = normalize_args(
        "install_planner_rules",
        (_CtxWrapper(inner_ctx),),
        {},
    )
    assert args == (inner_ctx,)
    assert kwargs == {}

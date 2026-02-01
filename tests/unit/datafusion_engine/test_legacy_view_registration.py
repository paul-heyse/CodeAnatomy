"""Tests for legacy view registration hard failures."""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

import pytest

from datafusion_engine.session.runtime import (
    DataFusionRuntimeProfile,
    SessionRuntime,
    register_view_specs,
)
from schema_spec.view_specs import ViewSpec

if TYPE_CHECKING:
    from datafusion import SessionContext


def _session_runtime(profile: DataFusionRuntimeProfile) -> SessionRuntime:
    ctx = cast("SessionContext", object())
    return SessionRuntime(
        ctx=ctx,
        profile=profile,
        udf_snapshot_hash="",
        udf_rewrite_tags=(),
        domain_planner_names=(),
        udf_snapshot={},
        df_settings={},
    )


def test_register_view_specs_raises() -> None:
    profile = DataFusionRuntimeProfile()
    ctx = cast("SessionContext", object())
    views = [ViewSpec(name="legacy_view_v1")]

    with pytest.raises(RuntimeError, match=r"Legacy view spec registration is removed"):
        register_view_specs(ctx, views=views, runtime_profile=profile)


def test_view_spec_register_raises() -> None:
    profile = DataFusionRuntimeProfile()
    session_runtime = _session_runtime(profile)
    spec = ViewSpec(name="legacy_view_v1")

    with pytest.raises(RuntimeError, match=r"ViewSpec\.register is removed"):
        spec.register(session_runtime)

"""Unit tests for Ibis SQL option control plane."""

from __future__ import annotations

from dataclasses import replace

import ibis
import pytest

from arrowdsl.core.execution_context import ExecutionContext
from arrowdsl.core.runtime_profiles import runtime_profile_factory
from datafusion_engine.runtime import DataFusionRuntimeProfile
from ibis_engine.backend import build_backend
from ibis_engine.config import IbisBackendConfig
from ibis_engine.execution_factory import ibis_backend_from_ctx

pytest.importorskip("datafusion")

DEFAULT_LIMIT = 100


def test_ibis_sql_options_are_applied() -> None:
    """Apply runtime Ibis SQL options during backend creation."""
    previous = (
        ibis.options.sql.fuse_selects,
        ibis.options.sql.default_limit,
        ibis.options.sql.default_dialect,
        ibis.options.interactive,
    )
    cfg = IbisBackendConfig(
        datafusion_profile=DataFusionRuntimeProfile(),
        fuse_selects=True,
        default_limit=None,
        default_dialect="datafusion",
        interactive=False,
    )
    try:
        _ = build_backend(cfg)
        assert ibis.options.sql.fuse_selects is True
        assert ibis.options.sql.default_limit is None
        assert ibis.options.sql.default_dialect == "datafusion"
        assert ibis.options.interactive is False
    finally:
        (
            ibis.options.sql.fuse_selects,
            ibis.options.sql.default_limit,
            ibis.options.sql.default_dialect,
            ibis.options.interactive,
        ) = previous


def test_factory_backend_applies_runtime_options() -> None:
    """Apply runtime Ibis SQL options through the execution factory."""
    previous = (
        ibis.options.sql.fuse_selects,
        ibis.options.sql.default_limit,
        ibis.options.sql.default_dialect,
        ibis.options.interactive,
    )
    runtime = runtime_profile_factory("default")
    runtime = replace(
        runtime,
        ibis_fuse_selects=False,
        ibis_default_limit=DEFAULT_LIMIT,
        ibis_default_dialect="datafusion",
        ibis_interactive=True,
    )
    ctx = ExecutionContext(runtime=runtime)
    try:
        _ = ibis_backend_from_ctx(ctx)
        assert ibis.options.sql.fuse_selects is False
        assert ibis.options.sql.default_limit == DEFAULT_LIMIT
        assert ibis.options.sql.default_dialect == "datafusion"
        assert ibis.options.interactive is True
    finally:
        (
            ibis.options.sql.fuse_selects,
            ibis.options.sql.default_limit,
            ibis.options.sql.default_dialect,
            ibis.options.interactive,
        ) = previous

"""Unit tests for Ibis SQL option control plane."""

from __future__ import annotations

import ibis
import pytest

from datafusion_engine.runtime import DataFusionRuntimeProfile
from ibis_engine.backend import build_backend
from ibis_engine.config import IbisBackendConfig

pytest.importorskip("datafusion")


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

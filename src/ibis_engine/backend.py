"""Ibis backend factory helpers."""

from __future__ import annotations

import importlib
from typing import Protocol, cast

import ibis

from datafusion_engine.kernels import register_datafusion_udfs
from datafusion_engine.runtime import DataFusionRuntimeProfile
from ibis_engine.config import IbisBackendConfig


class _IbisDataFusionModule(Protocol):
    def connect(self, ctx: object) -> ibis.backends.BaseBackend:
        """Return an Ibis DataFusion backend."""
        ...


def _load_ibis_datafusion() -> _IbisDataFusionModule:
    try:
        module = importlib.import_module("ibis.datafusion")
    except ImportError as exc:
        msg = "Ibis datafusion backend is unavailable; install ibis-framework[datafusion]."
        raise ImportError(msg) from exc
    return cast("_IbisDataFusionModule", module)


def build_backend(cfg: IbisBackendConfig) -> ibis.backends.BaseBackend:
    """Return an Ibis backend configured for the pipeline.

    Returns
    -------
    ibis.backends.BaseBackend
        Configured backend instance.

    """
    ibis_datafusion = _load_ibis_datafusion()
    profile = cfg.datafusion_profile or DataFusionRuntimeProfile()
    ctx = profile.session_context()
    register_datafusion_udfs(ctx)
    return ibis_datafusion.connect(ctx)

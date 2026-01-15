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
    msg = "Ibis datafusion backend is unavailable; install ibis-framework[datafusion]."
    module_names = ("ibis.datafusion", "ibis.backends.datafusion")
    for module_name in module_names:
        try:
            module = importlib.import_module(module_name)
        except ModuleNotFoundError as exc:
            if exc.name == module_name:
                continue
            raise ImportError(msg) from exc
        except ImportError as exc:
            raise ImportError(msg) from exc
        connect = getattr(module, "connect", None)
        if callable(connect):
            return cast("_IbisDataFusionModule", module)
        backend_cls = getattr(module, "Backend", None)
        if callable(backend_cls):
            return cast("_IbisDataFusionModule", backend_cls())
        raise ImportError(msg)
    raise ImportError(msg)


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

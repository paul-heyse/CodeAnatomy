"""Ibis backend factory helpers."""

from __future__ import annotations

import importlib
from dataclasses import replace
from typing import Protocol, cast

import ibis

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
    if cfg.fuse_selects is not None:
        ibis.options.sql.fuse_selects = cfg.fuse_selects
    if cfg.default_limit is not None:
        ibis.options.sql.default_limit = cfg.default_limit
    if cfg.default_dialect is not None:
        ibis.options.sql.default_dialect = cfg.default_dialect
    if cfg.interactive is not None:
        ibis.options.interactive = cfg.interactive
    if cfg.datafusion_profile is not None:
        profile = cfg.datafusion_profile
    else:
        from datafusion_engine.runtime import DataFusionRuntimeProfile

        profile = DataFusionRuntimeProfile()
    if profile.default_catalog != "datafusion":
        profile = replace(profile, default_catalog="datafusion")
    ctx = profile.session_context()
    return ibis_datafusion.connect(ctx)

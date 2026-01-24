"""Ibis backend factory helpers."""

from __future__ import annotations

import importlib
import time
from dataclasses import replace
from typing import TYPE_CHECKING, Protocol, cast

import ibis

from ibis_engine.config import IbisBackendConfig

if TYPE_CHECKING:
    from datafusion_engine.runtime import DataFusionRuntimeProfile


class _IbisDataFusionModule(Protocol):
    def connect(self, ctx: object) -> ibis.backends.BaseBackend:
        """Return an Ibis DataFusion backend."""
        ...


def _load_ibis_datafusion() -> _IbisDataFusionModule:
    msg = "Ibis datafusion backend is unavailable; install ibis-framework[datafusion]."
    try:
        module = importlib.import_module("ibis.datafusion")
    except ModuleNotFoundError as exc:
        if exc.name == "ibis.datafusion":
            raise ImportError(msg) from exc
        raise ImportError(msg) from exc
    except ImportError as exc:
        raise ImportError(msg) from exc
    connect = getattr(module, "connect", None)
    if callable(connect):
        return cast("_IbisDataFusionModule", module)
    msg = "Ibis datafusion backend is missing connect()."
    raise ImportError(msg)


def _apply_ibis_options(cfg: IbisBackendConfig) -> None:
    if cfg.fuse_selects is not None:
        ibis.options.sql.fuse_selects = cfg.fuse_selects
    if cfg.default_limit is not None:
        ibis.options.sql.default_limit = cfg.default_limit
    if cfg.default_dialect is not None:
        ibis.options.sql.default_dialect = cfg.default_dialect
    if cfg.interactive is not None:
        ibis.options.interactive = cfg.interactive


def _resolve_datafusion_profile(cfg: IbisBackendConfig) -> DataFusionRuntimeProfile:
    if cfg.datafusion_profile is not None:
        profile = cfg.datafusion_profile
    else:
        from datafusion_engine.runtime import DataFusionRuntimeProfile

        profile = DataFusionRuntimeProfile()
    if profile.default_catalog != "datafusion":
        return replace(profile, default_catalog="datafusion")
    return profile


def _register_object_stores(
    *,
    ctx: object,
    cfg: IbisBackendConfig,
    profile: DataFusionRuntimeProfile,
) -> None:
    if not cfg.object_stores:
        return
    register_store = getattr(ctx, "register_object_store", None)
    if not callable(register_store):
        msg = "DataFusion SessionContext does not support register_object_store."
        raise TypeError(msg)
    from datafusion_engine.io_adapter import DataFusionIOAdapter

    adapter = DataFusionIOAdapter(ctx=ctx, profile=profile)  # type: ignore[arg-type]
    for store in cfg.object_stores:
        if not store.scheme:
            msg = "ObjectStoreConfig.scheme must be non-empty."
            raise ValueError(msg)
        adapter.register_object_store(scheme=store.scheme, store=store.store, host=store.host)
        if profile.diagnostics_sink is not None:
            profile.diagnostics_sink.record_artifact(
                "datafusion_object_stores_v1",
                {
                    "event_time_unix_ms": int(time.time() * 1000),
                    "scheme": store.scheme,
                    "host": store.host,
                    "store_type": type(store.store).__name__,
                },
            )


def build_backend(cfg: IbisBackendConfig) -> ibis.backends.BaseBackend:
    """Return an Ibis backend configured for the pipeline.

    Returns
    -------
    ibis.backends.BaseBackend
        Configured backend instance.

    """
    ibis_datafusion = _load_ibis_datafusion()
    _apply_ibis_options(cfg)
    profile = _resolve_datafusion_profile(cfg)
    ctx = profile.session_context()
    _register_object_stores(ctx=ctx, cfg=cfg, profile=profile)
    return ibis_datafusion.connect(ctx)

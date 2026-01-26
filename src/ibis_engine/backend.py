"""Ibis backend factory helpers."""

from __future__ import annotations

import importlib
import logging
from dataclasses import replace
from typing import TYPE_CHECKING, Protocol, cast

import ibis
from datafusion import SessionContext

from datafusion_engine.diagnostics import recorder_for_profile
from ibis_engine.config import IbisBackendConfig

if TYPE_CHECKING:
    from datafusion_engine.runtime import DataFusionRuntimeProfile

logger = logging.getLogger(__name__)


class _IbisDataFusionModule(Protocol):
    def connect(self, ctx: SessionContext) -> ibis.backends.BaseBackend:
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
    if cfg.datafusion_profile is None:
        msg = "Ibis backend requires a DataFusion runtime profile."
        raise ValueError(msg)
    profile = cfg.datafusion_profile
    if profile.default_catalog != "datafusion":
        return replace(profile, default_catalog="datafusion")
    return profile


def _register_object_stores(
    *,
    ctx: SessionContext,
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

    adapter = DataFusionIOAdapter(ctx=ctx, profile=profile)
    for store in cfg.object_stores:
        if not store.scheme:
            msg = "ObjectStoreConfig.scheme must be non-empty."
            raise ValueError(msg)
        adapter.register_object_store(scheme=store.scheme, store=store.store, host=store.host)
        recorder = recorder_for_profile(profile, operation_id="datafusion_object_store")
        if recorder is not None:
            recorder.record_registration(
                name=store.scheme,
                registration_type="object_store",
                location=store.host,
                schema={"store_type": type(store.store).__name__},
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
    backend = ibis_datafusion.connect(ctx)
    try:
        from datafusion_engine.udf_runtime import register_rust_udfs
        from ibis_engine.builtin_udfs import register_ibis_udf_snapshot

        async_timeout_ms = None
        async_batch_size = None
        if profile.enable_async_udfs:
            async_timeout_ms = profile.async_udf_timeout_ms
            async_batch_size = profile.async_udf_batch_size
        registry_snapshot = register_rust_udfs(
            ctx,
            enable_async=profile.enable_async_udfs,
            async_udf_timeout_ms=async_timeout_ms,
            async_udf_batch_size=async_batch_size,
        )
        register_ibis_udf_snapshot(registry_snapshot)
    except (RuntimeError, TypeError, ValueError) as exc:
        logger.warning("Failed to register Rust/Ibis UDFs: %s", exc)
    return backend

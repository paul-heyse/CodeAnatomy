"""Ibis backend factory helpers."""

from __future__ import annotations

import ibis

from ibis_engine.config import IbisBackendConfig


def build_backend(cfg: IbisBackendConfig) -> ibis.backends.BaseBackend:
    """Return an Ibis backend configured for the pipeline.

    Returns
    -------
    ibis.backends.BaseBackend
        Configured backend instance.
    """
    con = ibis.duckdb.connect(
        database=cfg.database,
        read_only=cfg.read_only,
        extensions=list(cfg.extensions) or None,
        **cfg.config,
    )
    if cfg.filesystem is not None:
        con.register_filesystem(cfg.filesystem)
    return con

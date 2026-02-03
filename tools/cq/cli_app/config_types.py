"""Typed configuration structs for cq CLI using msgspec."""

from __future__ import annotations

import msgspec


class CqConfig(msgspec.Struct, omit_defaults=True):
    """Typed configuration for cq CLI.

    Values correspond to global options and cache settings. Fields are optional
    so configs can override only what they specify.
    """

    root: str | None = None
    verbose: int | None = None
    output_format: str | None = None
    artifact_dir: str | None = None
    save_artifact: bool | None = None

    cache_dir: str | None = None
    cache_query_ttl: float | None = None
    cache_query_size: int | None = None
    cache_index_size: int | None = None
    cache_query_shards: int | None = None


__all__ = [
    "CqConfig",
]

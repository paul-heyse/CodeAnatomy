"""Typed configuration structs for cq CLI using msgspec."""

from __future__ import annotations

import msgspec


class CqConfig(msgspec.Struct, omit_defaults=True):
    """Typed configuration for cq CLI.

    Values correspond to global options. Fields are optional
    so configs can override only what they specify.
    """

    root: str | None = None
    verbose: int | None = None
    output_format: str | None = None
    artifact_dir: str | None = None
    save_artifact: bool | None = None


__all__ = [
    "CqConfig",
]

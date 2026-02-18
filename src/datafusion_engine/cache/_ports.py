"""Shared cache module protocol ports."""

from __future__ import annotations

from typing import Protocol

from datafusion_engine.io.write_core import WriteRequest, WriteResult


class _WriterPort(Protocol):
    """Minimal writer surface for cache persistence paths."""

    def write(self, request: WriteRequest) -> WriteResult: ...


__all__ = ["_WriterPort"]

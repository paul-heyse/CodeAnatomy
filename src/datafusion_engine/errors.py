"""Unified error types for DataFusion engine surfaces."""

from __future__ import annotations

from enum import StrEnum


class ErrorKind(StrEnum):
    """Categorize engine errors by subsystem."""

    GENERIC = "generic"
    DATAFUSION = "datafusion"
    ARROW = "arrow"
    DELTA = "delta"
    PLUGIN = "plugin"
    VALIDATION = "validation"
    CONFIG = "config"


class DataFusionEngineError(Exception):
    """Base exception for DataFusion engine failures."""

    def __init__(self, message: str, *, kind: ErrorKind = ErrorKind.GENERIC) -> None:
        super().__init__(message)
        self.kind = kind


__all__ = ["DataFusionEngineError", "ErrorKind"]

"""Contracts for request-surface query specialization."""

from __future__ import annotations

from tools.cq.core.structs import CqStruct


class QuerySpecializationProfileV1(CqStruct, frozen=True):
    """Pattern/capture specialization profile for one request surface."""

    request_surface: str
    disabled_pattern_settings: tuple[str, ...] = ()
    disabled_capture_prefixes: tuple[str, ...] = ()
    disabled_capture_names: tuple[str, ...] = ()


__all__ = ["QuerySpecializationProfileV1"]

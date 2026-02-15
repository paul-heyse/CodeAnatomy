"""Contracts for bounded tree-sitter query execution."""

from __future__ import annotations

from tools.cq.core.structs import CqStruct


class QueryWindowV1(CqStruct, frozen=True):
    """Inclusive-exclusive byte window for query execution."""

    start_byte: int
    end_byte: int


class QueryExecutionSettingsV1(CqStruct, frozen=True):
    """Execution bounds for one query run."""

    match_limit: int = 4096
    max_start_depth: int | None = None


class QueryExecutionTelemetryV1(CqStruct, frozen=True):
    """Telemetry emitted by bounded query runners."""

    windows_total: int = 0
    windows_executed: int = 0
    capture_count: int = 0
    match_count: int = 0
    exceeded_match_limit: bool = False
    cancelled: bool = False


__all__ = [
    "QueryExecutionSettingsV1",
    "QueryExecutionTelemetryV1",
    "QueryWindowV1",
]

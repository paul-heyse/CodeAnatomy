"""Typed contracts for Python lane extracted facts."""

from __future__ import annotations

from tools.cq.core.structs import CqStruct


class PythonScopeFactsV1(CqStruct, frozen=True):
    """Scope metadata extracted from Python tree-sitter nodes."""

    scope_kind: str | None = None
    scope_name: str | None = None
    scope_chain: tuple[str, ...] = ()


class PythonCaptureFactV1(CqStruct, frozen=True):
    """Typed capture row for one Python tree-sitter query capture."""

    capture_name: str
    node_kind: str
    start_byte: int = 0
    end_byte: int = 0
    scope: str | None = None


__all__ = ["PythonCaptureFactV1", "PythonScopeFactsV1"]

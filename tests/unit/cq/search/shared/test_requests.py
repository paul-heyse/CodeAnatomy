"""Tests for shared request contracts split from _shared.core."""

from __future__ import annotations

from pathlib import Path

from tools.cq.search._shared.requests import (
    PythonByteRangeEnrichmentRequest,
    RgRunRequest,
)
from tools.cq.search._shared.types import QueryMode, SearchLimits


def test_rg_run_request_to_settings_serializes_mode_value() -> None:
    """RG request settings should serialize enum mode values."""
    request = RgRunRequest(
        root=Path("."),
        pattern="target",
        mode=QueryMode.IDENTIFIER,
        lang_types=("py",),
        limits=SearchLimits(),
        include_globs=["*.py"],
    )
    settings = request.to_settings()

    assert settings.mode == "identifier"
    assert settings.include_globs == ("*.py",)


def test_python_byte_range_request_projects_settings_without_runtime_handles() -> None:
    """Byte-range request should split serializable settings and runtime handles."""
    request = PythonByteRangeEnrichmentRequest(
        sg_root=object(),
        source_bytes=b"def target():\n    return 1\n",
        byte_start=0,
        byte_end=10,
        cache_key="k",
        resolved_node=object(),
        resolved_line=1,
        resolved_col=0,
    )
    settings = request.to_settings()
    runtime = request.to_runtime()

    assert settings.byte_start == 0
    assert settings.byte_end == 10
    assert settings.resolved_line == 1
    assert runtime.resolved_node is not None

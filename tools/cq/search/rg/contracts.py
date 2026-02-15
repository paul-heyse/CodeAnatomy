"""Serializable contracts for ripgrep lane execution."""

from __future__ import annotations

import msgspec

from tools.cq.core.structs import CqOutputStruct, CqSettingsStruct
from tools.cq.search._shared.core import RgRunRequest
from tools.cq.search.rg.runner import RgProcessResult


class RgRunSettingsV1(CqSettingsStruct, frozen=True):
    """Serializable settings payload for native ripgrep execution."""

    pattern: str
    mode: str
    lang_types: tuple[str, ...]
    include_globs: tuple[str, ...] = ()
    exclude_globs: tuple[str, ...] = ()


class RgProcessResultV1(CqOutputStruct, frozen=True):
    """Serializable process output payload from one rg execution."""

    command: tuple[str, ...]
    timed_out: bool
    returncode: int
    stderr: str
    events: tuple[dict[str, object], ...] = msgspec.field(default_factory=tuple)


def settings_from_request(request: RgRunRequest) -> RgRunSettingsV1:
    """Build serializable run settings from the existing request contract."""
    return RgRunSettingsV1(
        pattern=request.pattern,
        mode=request.mode.value,
        lang_types=tuple(request.lang_types),
        include_globs=tuple(request.include_globs),
        exclude_globs=tuple(request.exclude_globs),
    )


def result_from_process(result: RgProcessResult) -> RgProcessResultV1:
    """Convert native process result into a transport-safe output contract."""
    return RgProcessResultV1(
        command=tuple(result.command),
        timed_out=bool(result.timed_out),
        returncode=int(result.returncode),
        stderr=result.stderr,
        events=tuple(
            row
            for row in (msgspec.to_builtins(event) for event in result.events)
            if isinstance(row, dict)
        ),
    )


__all__ = [
    "RgProcessResultV1",
    "RgRunSettingsV1",
    "result_from_process",
    "settings_from_request",
]

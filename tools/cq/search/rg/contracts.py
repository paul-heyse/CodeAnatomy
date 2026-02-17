"""Serializable contracts for ripgrep lane execution."""

from __future__ import annotations

from typing import TYPE_CHECKING

import msgspec

from tools.cq.core.contract_codec import to_contract_builtins
from tools.cq.core.structs import CqOutputStruct, CqSettingsStruct
from tools.cq.search._shared.requests import RgRunRequest

if TYPE_CHECKING:
    from tools.cq.search.rg.runner import RgProcessRuntimeResultV1


class RgRunSettingsV1(CqSettingsStruct, frozen=True):
    """Serializable settings payload for native ripgrep execution."""

    pattern: str
    mode: str
    lang_types: tuple[str, ...]
    include_globs: tuple[str, ...] = ()
    exclude_globs: tuple[str, ...] = ()
    operation: str = "json"
    paths: tuple[str, ...] = (".",)
    extra_patterns: tuple[str, ...] = ()


class RgProcessResultV1(CqOutputStruct, frozen=True):
    """Serializable process output payload from one rg execution."""

    command: tuple[str, ...]
    timed_out: bool
    returncode: int
    stderr: str
    events: tuple[dict[str, object], ...] = msgspec.field(default_factory=tuple)
    stdout_lines: tuple[str, ...] = msgspec.field(default_factory=tuple)


def settings_from_request(request: RgRunRequest) -> RgRunSettingsV1:
    """Build serializable run settings from the existing request contract.

    Returns:
        RgRunSettingsV1: Function return value.
    """
    return RgRunSettingsV1(
        pattern=request.pattern,
        mode=request.mode.value,
        lang_types=tuple(request.lang_types),
        include_globs=tuple(request.include_globs),
        exclude_globs=tuple(request.exclude_globs),
        operation=request.operation,
        paths=request.paths,
        extra_patterns=request.extra_patterns,
    )


def result_from_process(
    result: RgProcessResultV1 | RgProcessRuntimeResultV1,
) -> RgProcessResultV1:
    """Convert native process result into a transport-safe output contract.

    Returns:
        RgProcessResultV1: Function return value.
    """
    return RgProcessResultV1(
        command=tuple(result.command),
        timed_out=bool(result.timed_out),
        returncode=int(result.returncode),
        stderr=result.stderr,
        events=tuple(
            row
            for row in (to_contract_builtins(event) for event in result.events)
            if isinstance(row, dict)
        ),
        stdout_lines=tuple(result.stdout_lines),
    )


__all__ = [
    "RgProcessResultV1",
    "RgRunSettingsV1",
    "result_from_process",
    "settings_from_request",
]

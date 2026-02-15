"""Native ripgrep process runner for CQ search pipelines."""

from __future__ import annotations

import contextlib
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from tools.cq.search._shared.core import RgRunRequest
from tools.cq.search.pipeline.classifier import QueryMode
from tools.cq.search.pipeline.profiles import SearchLimits
from tools.cq.search.rg.codec import RgAnyEvent, decode_rg_event

if TYPE_CHECKING:
    from tools.cq.search.rg.contracts import RgProcessResultV1, RgRunSettingsV1


@dataclass(frozen=True, slots=True)
class RgProcessResult:
    """Result from executing a native ``rg --json`` process."""

    command: list[str]
    events: list[RgAnyEvent]
    timed_out: bool
    returncode: int
    stderr: str


def detect_rg_version() -> tuple[bool, str | None]:
    """Detect whether ``rg`` is available and return a version banner."""
    proc = subprocess.run(
        ["rg", "--version"],
        check=False,
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        return False, None
    first_line = proc.stdout.splitlines()[0].strip() if proc.stdout else ""
    return True, first_line or None


def detect_rg_types() -> set[str]:
    """Return declared ripgrep type names from ``rg --type-list``."""
    proc = subprocess.run(
        ["rg", "--type-list"],
        check=False,
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        return set()
    types: set[str] = set()
    for line in proc.stdout.splitlines():
        token, _sep, _rest = line.partition(":")
        stripped = token.strip()
        if stripped:
            types.add(stripped)
    return types


def build_rg_command(
    *,
    pattern: str,
    mode: QueryMode,
    lang_types: tuple[str, ...],
    include_globs: list[str],
    exclude_globs: list[str],
    limits: SearchLimits,
) -> list[str]:
    """Build a deterministic ``rg --json`` command."""
    command = [
        "rg",
        "--json",
        "--line-number",
        "--column",
        "--max-count",
        str(limits.max_matches_per_file),
        "--max-depth",
        str(limits.max_depth),
        "--max-filesize",
        str(limits.max_file_size_bytes),
    ]
    for lang_type in lang_types:
        command.extend(["--type", lang_type])

    for glob in include_globs:
        command.extend(["-g", glob])
    for glob in exclude_globs:
        normalized = glob[1:] if glob.startswith("!") else glob
        command.extend(["-g", f"!{normalized}"])

    if mode.value == "literal":
        command.append("-F")
    command.extend(["-e", pattern, "."])
    return command


def run_rg_json(request: RgRunRequest) -> RgProcessResult:
    """Run native ``rg --json`` and decode events."""
    command = build_rg_command(
        pattern=request.pattern,
        mode=request.mode,
        lang_types=request.lang_types,
        include_globs=request.include_globs,
        exclude_globs=request.exclude_globs,
        limits=request.limits,
    )
    process = subprocess.Popen(
        command,
        cwd=request.root,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=False,
    )

    events: list[RgAnyEvent] = []
    timed_out = False
    deadline = time.monotonic() + request.limits.timeout_seconds
    stderr_bytes = b""

    try:
        assert process.stdout is not None
        for raw_line in iter(process.stdout.readline, b""):
            event = decode_rg_event(raw_line)
            if event is not None:
                events.append(event)
            if request.limits.timeout_seconds > 0 and time.monotonic() >= deadline:
                timed_out = True
                process.kill()
                break
    finally:
        if process.poll() is None:
            try:
                process.wait(timeout=1)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=1)
        if process.stderr is not None:
            try:
                stderr_bytes = process.stderr.read()
            except OSError:
                stderr_bytes = b""
            finally:
                with contextlib.suppress(OSError):
                    process.stderr.close()
        if process.stdout is not None:
            with contextlib.suppress(OSError):
                process.stdout.close()

    stderr_text = stderr_bytes.decode("utf-8", errors="replace")

    return RgProcessResult(
        command=command,
        events=events,
        timed_out=timed_out,
        returncode=process.returncode if process.returncode is not None else -1,
        stderr=stderr_text,
    )


def run_rg_json_contract(request: RgRunRequest) -> RgProcessResultV1:
    """Run rg and return the consolidated serializable result contract."""
    from tools.cq.search.rg.contracts import result_from_process

    return result_from_process(run_rg_json(request))


def _mode_from_value(mode: str) -> QueryMode:
    for query_mode in QueryMode:
        if query_mode.value == mode:
            return query_mode
    return QueryMode.REGEX


def build_command_from_settings(
    settings: RgRunSettingsV1,
    *,
    limits: SearchLimits,
) -> tuple[str, ...]:
    """Build deterministic rg command arguments from serialized settings."""
    return tuple(
        build_rg_command(
            pattern=settings.pattern,
            mode=_mode_from_value(settings.mode),
            lang_types=settings.lang_types,
            include_globs=list(settings.include_globs),
            exclude_globs=list(settings.exclude_globs),
            limits=limits,
        )
    )


def run_with_settings(
    *,
    root: Path,
    limits: SearchLimits,
    settings: RgRunSettingsV1,
) -> RgProcessResultV1:
    """Execute rg with serialized settings and return output contract."""
    from tools.cq.search.rg.contracts import result_from_process

    request = RgRunRequest(
        root=root,
        pattern=settings.pattern,
        mode=_mode_from_value(settings.mode),
        lang_types=settings.lang_types,
        limits=limits,
        include_globs=list(settings.include_globs),
        exclude_globs=list(settings.exclude_globs),
    )
    return result_from_process(run_rg_json(request))


def run_with_request(request: RgRunRequest) -> RgProcessResultV1:
    """Execute rg from the legacy request contract and normalize output."""
    from tools.cq.search.rg.contracts import result_from_process

    return result_from_process(run_rg_json(request))


__all__ = [
    "RgProcessResult",
    "build_command_from_settings",
    "build_rg_command",
    "detect_rg_types",
    "detect_rg_version",
    "run_rg_json",
    "run_rg_json_contract",
    "run_with_request",
    "run_with_settings",
]

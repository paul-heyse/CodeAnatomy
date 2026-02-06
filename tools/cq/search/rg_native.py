"""Native ripgrep process runner for CQ search pipelines."""

from __future__ import annotations

import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from tools.cq.search.rg_events import RgEvent, decode_rg_event

if TYPE_CHECKING:
    from tools.cq.search.classifier import QueryMode
    from tools.cq.search.profiles import SearchLimits


@dataclass(frozen=True, slots=True)
class RgProcessResult:
    """Result from executing a native ``rg --json`` process."""

    command: list[str]
    events: list[RgEvent]
    timed_out: bool
    returncode: int
    stderr: str


def detect_rg_version() -> tuple[bool, str | None]:
    """Detect whether ``rg`` is available and return a version banner.

    Returns:
    -------
    tuple[bool, str | None]
        Availability and first version banner line.
    """
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
    """Return declared ripgrep type names from ``rg --type-list``.

    Returns:
    -------
    set[str]
        Declared ripgrep file type names.
    """
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


def build_rg_command(  # noqa: PLR0913
    *,
    pattern: str,
    mode: QueryMode,
    lang_types: tuple[str, ...],
    include_globs: list[str],
    exclude_globs: list[str],
    limits: SearchLimits,
) -> list[str]:
    """Build a deterministic ``rg --json`` command.

    Returns:
    -------
    list[str]
        Shell argv for ripgrep invocation.
    """
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


def run_rg_json(  # noqa: PLR0913
    *,
    root: Path,
    pattern: str,
    mode: QueryMode,
    lang_types: tuple[str, ...],
    include_globs: list[str],
    exclude_globs: list[str],
    limits: SearchLimits,
) -> RgProcessResult:
    """Run native ``rg --json`` and decode events.

    Returns:
    -------
    RgProcessResult
        Process metadata and decoded event stream.
    """
    command = build_rg_command(
        pattern=pattern,
        mode=mode,
        lang_types=lang_types,
        include_globs=include_globs,
        exclude_globs=exclude_globs,
        limits=limits,
    )
    process = subprocess.Popen(
        command,
        cwd=root,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=False,
    )

    events: list[RgEvent] = []
    timed_out = False
    deadline = time.monotonic() + limits.timeout_seconds

    assert process.stdout is not None
    assert process.stderr is not None
    try:
        for raw_line in iter(process.stdout.readline, b""):
            event = decode_rg_event(raw_line)
            if event is not None:
                events.append(event)
            if limits.timeout_seconds > 0 and time.monotonic() >= deadline:
                timed_out = True
                process.kill()
                break
    finally:
        # Drain stderr regardless of process state to avoid zombies.
        stderr_text = process.stderr.read().decode("utf-8", errors="replace")
        if process.poll() is None:
            process.wait(timeout=1)

    return RgProcessResult(
        command=command,
        events=events,
        timed_out=timed_out,
        returncode=process.returncode if process.returncode is not None else -1,
        stderr=stderr_text,
    )


__all__ = [
    "RgProcessResult",
    "build_rg_command",
    "detect_rg_types",
    "detect_rg_version",
    "run_rg_json",
]

"""Native ripgrep process runner for CQ search pipelines."""

from __future__ import annotations

import re
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from tools.cq.search._shared.core import RgRunRequest
from tools.cq.search.pipeline.classifier import QueryMode
from tools.cq.search.pipeline.profiles import INTERACTIVE, SearchLimits
from tools.cq.search.rg.codec import RgAnyEvent, decode_rg_event

if TYPE_CHECKING:
    from tools.cq.search.rg.contracts import RgProcessResultV1, RgRunSettingsV1

_LOOKAROUND_RE = re.compile(r"\(\?(?:[=!]|<[=!])")


@dataclass(frozen=True, slots=True)
class RgProcessResult:
    """Result from executing a native ripgrep process."""

    command: list[str]
    events: list[RgAnyEvent]
    timed_out: bool
    returncode: int
    stderr: str
    stdout_lines: list[str]


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


def build_rg_command(  # noqa: C901, PLR0912, PLR0913
    *,
    pattern: str,
    mode: QueryMode,
    lang_types: tuple[str, ...],
    include_globs: list[str],
    exclude_globs: list[str],
    limits: SearchLimits,
    operation: str = "json",
    paths: tuple[str, ...] = (".",),
    extra_patterns: tuple[str, ...] = (),
    pcre2_available: bool = False,
) -> list[str]:
    """Build a deterministic rg command for the requested operation.

    Raises:
        ValueError: If ``operation`` is not one of the supported ripgrep modes.
    """
    command = ["rg"]

    if operation == "json":
        command.extend(["--json", "--line-number", "--column"])
    elif operation == "count":
        command.extend(["--count-matches", "--no-heading"])
    elif operation == "files":
        command.append("--files")
    elif operation == "files_with_matches":
        command.extend(["--files-with-matches", "--no-messages", "--no-heading"])
    else:
        msg = f"Unsupported rg operation: {operation}"
        raise ValueError(msg)

    command.extend(["--max-depth", str(limits.max_depth)])
    command.extend(["--max-filesize", str(limits.max_file_size_bytes)])
    if operation in {"json", "files_with_matches"}:
        command.extend(["--max-count", str(limits.max_matches_per_file)])

    if limits.context_before > 0 and operation == "json":
        command.extend(["--before-context", str(limits.context_before)])
    if limits.context_after > 0 and operation == "json":
        command.extend(["--after-context", str(limits.context_after)])
    if limits.sort_by_path:
        command.extend(["--sort", "path"])

    for lang_type in lang_types:
        command.extend(["--type", lang_type])

    for glob in include_globs:
        command.extend(["-g", glob])
    for glob in exclude_globs:
        normalized = glob[1:] if glob.startswith("!") else glob
        command.extend(["-g", f"!{normalized}"])

    if operation != "files":
        if mode.value == "literal":
            command.append("-F")
        elif mode.value == "identifier":
            command.append("-w")

        if limits.multiline:
            command.extend(["-U", "--multiline-dotall"])

        all_patterns = (pattern, *extra_patterns)
        if pcre2_available and any(_LOOKAROUND_RE.search(item) for item in all_patterns):
            command.append("-P")

        command.extend(["-e", pattern])
        for extra in extra_patterns:
            command.extend(["-e", extra])

    search_paths = [path for path in paths if path.strip()] or ["."]
    command.extend(search_paths)
    return command


def _mode_from_value(mode: str) -> QueryMode:
    for query_mode in QueryMode:
        if query_mode.value == mode:
            return query_mode
    return QueryMode.REGEX


def _decode_stdout_lines(stdout_bytes: bytes) -> list[str]:
    text = stdout_bytes.decode("utf-8", errors="replace")
    return [line for line in text.splitlines() if line.strip()]


def run_rg_json(request: RgRunRequest, *, pcre2_available: bool = False) -> RgProcessResult:
    """Run native ``rg`` and decode structured output when requested."""
    command = build_rg_command(
        pattern=request.pattern,
        mode=request.mode,
        lang_types=request.lang_types,
        include_globs=request.include_globs,
        exclude_globs=request.exclude_globs,
        limits=request.limits,
        operation=request.operation,
        paths=request.paths,
        extra_patterns=request.extra_patterns,
        pcre2_available=pcre2_available,
    )
    process = subprocess.Popen(
        command,
        cwd=request.root,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=False,
    )

    timeout = request.limits.timeout_seconds if request.limits.timeout_seconds > 0 else None
    timed_out = False
    try:
        stdout_bytes, stderr_bytes = process.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        timed_out = True
        process.kill()
        stdout_bytes, stderr_bytes = process.communicate()

    events: list[RgAnyEvent] = []
    stdout_lines: list[str] = []
    if request.operation == "json":
        for raw_line in stdout_bytes.splitlines():
            event = decode_rg_event(raw_line)
            if event is not None:
                events.append(event)
    else:
        stdout_lines = _decode_stdout_lines(stdout_bytes)

    return RgProcessResult(
        command=command,
        events=events,
        timed_out=timed_out,
        returncode=process.returncode if process.returncode is not None else -1,
        stderr=stderr_bytes.decode("utf-8", errors="replace"),
        stdout_lines=stdout_lines,
    )


def run_rg_json_contract(request: RgRunRequest) -> RgProcessResultV1:
    """Run rg and return the consolidated serializable result contract."""
    from tools.cq.search.rg.contracts import result_from_process

    return result_from_process(run_rg_json(request))


def build_command_from_settings(
    settings: RgRunSettingsV1,
    *,
    limits: SearchLimits,
    pcre2_available: bool = False,
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
            operation=settings.operation,
            paths=settings.paths,
            extra_patterns=settings.extra_patterns,
            pcre2_available=pcre2_available,
        )
    )


def run_with_settings(
    *,
    root: Path,
    limits: SearchLimits,
    settings: RgRunSettingsV1,
    pcre2_available: bool = False,
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
        operation=settings.operation,
        paths=settings.paths,
        extra_patterns=settings.extra_patterns,
    )
    return result_from_process(run_rg_json(request, pcre2_available=pcre2_available))


def run_with_request(request: RgRunRequest) -> RgProcessResultV1:
    """Execute rg from the legacy request contract and normalize output."""
    from tools.cq.search.rg.contracts import result_from_process

    return result_from_process(run_rg_json(request))


def run_rg_count(  # noqa: PLR0913
    *,
    root: Path,
    pattern: str,
    mode: QueryMode,
    lang_types: tuple[str, ...],
    include_globs: list[str] | None = None,
    exclude_globs: list[str] | None = None,
    paths: tuple[str, ...] = (".",),
    limits: SearchLimits | None = None,
) -> dict[str, int]:
    """Run ripgrep count mode and return per-file match cardinality."""
    from tools.cq.search.rg.contracts import RgRunSettingsV1

    settings = RgRunSettingsV1(
        pattern=pattern,
        mode=mode.value,
        lang_types=lang_types,
        include_globs=tuple(include_globs or ()),
        exclude_globs=tuple(exclude_globs or ()),
        operation="count",
        paths=paths,
    )
    result = run_with_settings(root=root, limits=limits or INTERACTIVE, settings=settings)
    if result.returncode not in {0, 1}:
        return {}

    counts: dict[str, int] = {}
    for line in result.stdout_lines:
        path_text, sep, count_text = line.rpartition(":")
        if not sep:
            continue
        try:
            counts[path_text.strip()] = int(count_text.strip())
        except ValueError:
            continue
    return counts


def run_rg_files_with_matches(  # noqa: PLR0913
    *,
    root: Path,
    patterns: tuple[str, ...],
    mode: QueryMode,
    lang_types: tuple[str, ...],
    include_globs: list[str] | None = None,
    exclude_globs: list[str] | None = None,
    paths: tuple[str, ...] = (".",),
    limits: SearchLimits | None = None,
) -> list[str] | None:
    """Run ripgrep file-list mode for files containing any provided pattern."""
    from tools.cq.search.rg.contracts import RgRunSettingsV1

    if not patterns:
        return []
    settings = RgRunSettingsV1(
        pattern=patterns[0],
        mode=mode.value,
        lang_types=lang_types,
        include_globs=tuple(include_globs or ()),
        exclude_globs=tuple(exclude_globs or ()),
        operation="files_with_matches",
        paths=paths,
        extra_patterns=tuple(patterns[1:]),
    )
    result = run_with_settings(root=root, limits=limits or INTERACTIVE, settings=settings)
    if result.returncode not in {0, 1}:
        return None
    if result.returncode == 1:
        return []
    rows = [line.strip() for line in result.stdout_lines if line.strip()]
    return sorted(set(rows))


__all__ = [
    "RgProcessResult",
    "build_command_from_settings",
    "build_rg_command",
    "detect_rg_types",
    "detect_rg_version",
    "run_rg_count",
    "run_rg_files_with_matches",
    "run_rg_json",
    "run_rg_json_contract",
    "run_with_request",
    "run_with_settings",
]

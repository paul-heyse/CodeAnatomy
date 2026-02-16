"""Native ripgrep process runner for CQ search pipelines."""

from __future__ import annotations

import re
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from tools.cq.search._shared.core import RgRunRequest
from tools.cq.search._shared.types import QueryMode, SearchLimits
from tools.cq.search.pipeline.profiles import INTERACTIVE
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


@dataclass(frozen=True, slots=True)
class RgCountRequest:
    """Inputs for ripgrep count-mode execution."""

    root: Path
    pattern: str
    mode: QueryMode
    lang_types: tuple[str, ...]
    include_globs: tuple[str, ...] = ()
    exclude_globs: tuple[str, ...] = ()
    paths: tuple[str, ...] = (".",)
    limits: SearchLimits | None = None


@dataclass(frozen=True, slots=True)
class RgFilesWithMatchesRequest:
    """Inputs for ripgrep files-with-matches execution."""

    root: Path
    patterns: tuple[str, ...]
    mode: QueryMode
    lang_types: tuple[str, ...]
    include_globs: tuple[str, ...] = ()
    exclude_globs: tuple[str, ...] = ()
    paths: tuple[str, ...] = (".",)
    limits: SearchLimits | None = None


def detect_rg_version() -> tuple[bool, str | None]:
    """Detect whether ``rg`` is available and return a version banner.

    Returns:
        tuple[bool, str | None]: Function return value.
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


def _operation_args(operation: str) -> list[str]:
    operation_args: dict[str, list[str]] = {
        "json": ["--json", "--line-number", "--column"],
        "count": ["--count-matches", "--no-heading"],
        "files": ["--files"],
        "files_with_matches": ["--files-with-matches", "--no-messages", "--no-heading"],
    }
    args = operation_args.get(operation)
    if args is None:
        msg = f"Unsupported rg operation: {operation}"
        raise ValueError(msg)
    return args


def _limit_args(*, limits: SearchLimits, operation: str) -> list[str]:
    args = [
        "--max-depth",
        str(limits.max_depth),
        "--max-filesize",
        str(limits.max_file_size_bytes),
    ]
    if operation in {"json", "files_with_matches"}:
        args.extend(["--max-count", str(limits.max_matches_per_file)])
    return args


def _context_and_sort_args(*, limits: SearchLimits, operation: str) -> list[str]:
    args: list[str] = []
    if operation == "json" and limits.context_before > 0:
        args.extend(["--before-context", str(limits.context_before)])
    if operation == "json" and limits.context_after > 0:
        args.extend(["--after-context", str(limits.context_after)])
    if limits.sort_by_path:
        args.extend(["--sort", "path"])
    return args


def _glob_args(*, include_globs: tuple[str, ...], exclude_globs: tuple[str, ...]) -> list[str]:
    args: list[str] = []
    for glob in include_globs:
        args.extend(["-g", glob])
    for glob in exclude_globs:
        normalized = glob[1:] if glob.startswith("!") else glob
        args.extend(["-g", f"!{normalized}"])
    return args


def _pattern_args(
    *,
    pattern: str,
    extra_patterns: tuple[str, ...],
    mode: QueryMode,
    limits: SearchLimits,
    operation: str,
    pcre2_available: bool = False,
) -> list[str]:
    if operation == "files":
        return []
    args: list[str] = []
    if mode == QueryMode.LITERAL:
        args.append("-F")
    elif mode == QueryMode.IDENTIFIER:
        args.append("-w")
    if limits.multiline:
        args.extend(["-U", "--multiline-dotall"])
    all_patterns = (pattern, *extra_patterns)
    if pcre2_available and any(_LOOKAROUND_RE.search(item) for item in all_patterns):
        args.append("-P")
    args.extend(["-e", pattern])
    for extra in extra_patterns:
        args.extend(["-e", extra])
    return args


def _search_paths(paths: tuple[str, ...]) -> list[str]:
    return [path for path in paths if path.strip()] or ["."]


def build_rg_command(request: RgRunRequest, *, pcre2_available: bool = False) -> list[str]:
    """Build deterministic command arguments for the provided rg request.

    Returns:
        list[str]: Function return value.
    """
    command = ["rg"]
    command.extend(_operation_args(request.operation))
    command.extend(_limit_args(limits=request.limits, operation=request.operation))
    command.extend(_context_and_sort_args(limits=request.limits, operation=request.operation))
    for lang_type in request.lang_types:
        command.extend(["--type", lang_type])
    command.extend(
        _glob_args(
            include_globs=tuple(request.include_globs),
            exclude_globs=tuple(request.exclude_globs),
        )
    )
    command.extend(
        _pattern_args(
            pattern=request.pattern,
            extra_patterns=request.extra_patterns,
            mode=request.mode,
            limits=request.limits,
            operation=request.operation,
            pcre2_available=pcre2_available,
        )
    )
    command.extend(_search_paths(request.paths))
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
    """Run native ``rg`` and decode structured output when requested.

    Returns:
        RgProcessResult: Function return value.
    """
    command = build_rg_command(request, pcre2_available=pcre2_available)
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
    """Run rg and return the consolidated serializable result contract.

    Returns:
        RgProcessResultV1: Function return value.
    """
    from tools.cq.search.rg.contracts import result_from_process

    return result_from_process(run_rg_json(request))


def build_command_from_settings(
    settings: RgRunSettingsV1,
    *,
    limits: SearchLimits,
    pcre2_available: bool = False,
) -> tuple[str, ...]:
    """Build deterministic rg command arguments from serialized settings.

    Returns:
        tuple[str, ...]: Function return value.
    """
    request = RgRunRequest(
        root=Path(),
        pattern=settings.pattern,
        mode=_mode_from_value(settings.mode),
        lang_types=settings.lang_types,
        include_globs=list(settings.include_globs),
        exclude_globs=list(settings.exclude_globs),
        limits=limits,
        operation=settings.operation,
        paths=settings.paths,
        extra_patterns=settings.extra_patterns,
    )
    return tuple(build_rg_command(request, pcre2_available=pcre2_available))


def run_with_settings(
    *,
    root: Path,
    limits: SearchLimits,
    settings: RgRunSettingsV1,
    pcre2_available: bool = False,
) -> RgProcessResultV1:
    """Execute rg with serialized settings and return output contract.

    Returns:
        RgProcessResultV1: Function return value.
    """
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
    """Execute rg from the legacy request contract and normalize output.

    Returns:
        RgProcessResultV1: Function return value.
    """
    from tools.cq.search.rg.contracts import result_from_process

    return result_from_process(run_rg_json(request))


def run_rg_count(request: RgCountRequest) -> dict[str, int]:
    """Run ripgrep count mode and return per-file match cardinality.

    Returns:
        dict[str, int]: Function return value.
    """
    from tools.cq.search.rg.contracts import RgRunSettingsV1

    settings = RgRunSettingsV1(
        pattern=request.pattern,
        mode=request.mode.value,
        lang_types=request.lang_types,
        include_globs=request.include_globs,
        exclude_globs=request.exclude_globs,
        operation="count",
        paths=request.paths,
    )
    result = run_with_settings(
        root=request.root,
        limits=request.limits or INTERACTIVE,
        settings=settings,
    )
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


def run_rg_files_with_matches(request: RgFilesWithMatchesRequest) -> list[str] | None:
    """Run ripgrep file-list mode for files containing any provided pattern.

    Returns:
        list[str] | None: Function return value.
    """
    from tools.cq.search.rg.contracts import RgRunSettingsV1

    if not request.patterns:
        return []
    settings = RgRunSettingsV1(
        pattern=request.patterns[0],
        mode=request.mode.value,
        lang_types=request.lang_types,
        include_globs=request.include_globs,
        exclude_globs=request.exclude_globs,
        operation="files_with_matches",
        paths=request.paths,
        extra_patterns=tuple(request.patterns[1:]),
    )
    result = run_with_settings(
        root=request.root,
        limits=request.limits or INTERACTIVE,
        settings=settings,
    )
    if result.returncode not in {0, 1}:
        return None
    if result.returncode == 1:
        return []
    rows = [line.strip() for line in result.stdout_lines if line.strip()]
    return sorted(set(rows))


__all__ = [
    "RgCountRequest",
    "RgFilesWithMatchesRequest",
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

"""Scan a repository for source files and capture file metadata."""

from __future__ import annotations

import fnmatch
import hashlib
import io
import tokenize
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from pathlib import Path

import pyarrow as pa

from core_types import PathLike, ensure_path

SCHEMA_VERSION = 1


def stable_id(prefix: str, *parts: str) -> str:
    """Build a deterministic string ID.

    Parameters
    ----------
    prefix:
        ID prefix to namespace the hash.
    *parts:
        String parts used for hashing.

    Returns
    -------
    str
        Stable identifier string.
    """
    h = hashlib.sha1()
    for part in parts:
        h.update(part.encode("utf-8"))
        h.update(b"\x1f")
    return f"{prefix}:{h.hexdigest()}"


@dataclass(frozen=True)
class RepoScanOptions:
    """Configure repository scanning behavior."""

    repo_id: str | None = None
    include_globs: Sequence[str] = ("**/*.py",)
    exclude_dirs: Sequence[str] = (
        ".git",
        "__pycache__",
        ".venv",
        "venv",
        "node_modules",
        "dist",
        "build",
        ".mypy_cache",
        ".pytest_cache",
    )
    exclude_globs: Sequence[str] = ()
    follow_symlinks: bool = False
    include_bytes: bool = True
    include_text: bool = True
    max_file_bytes: int | None = None
    max_files: int | None = None


REPO_FILES_SCHEMA = pa.schema(
    [
        ("schema_version", pa.int32()),
        ("file_id", pa.string()),
        ("path", pa.string()),
        ("abs_path", pa.string()),
        ("size_bytes", pa.int64()),
        ("file_sha256", pa.string()),
        ("encoding", pa.string()),
        ("text", pa.string()),
        ("bytes", pa.binary()),
    ]
)


def _is_excluded_dir(rel_path: Path, exclude_dirs: Sequence[str]) -> bool:
    parts = set(rel_path.parts)
    return any(d in parts for d in exclude_dirs)


def _matches_any_glob(path_posix: str, globs: Sequence[str]) -> bool:
    return any(fnmatch.fnmatch(path_posix, g) for g in globs)


def _sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _detect_encoding_and_decode(data: bytes) -> tuple[str, str | None]:
    """Detect encoding using tokenize rules and decode best-effort.

    Parameters
    ----------
    data:
        Raw file bytes.

    Returns
    -------
    tuple[str, str | None]
        Detected encoding and decoded text (if available).
    """
    try:
        encoding, _ = tokenize.detect_encoding(io.BytesIO(data).readline)
    except (LookupError, SyntaxError):
        encoding = "utf-8"
    try:
        text = data.decode(encoding, errors="replace")
    except (LookupError, UnicodeError):
        return encoding, None
    else:
        return encoding, text


def iter_repo_files(repo_root: Path, options: RepoScanOptions) -> Iterator[Path]:
    """Iterate over repo files that match include/exclude rules.

    Parameters
    ----------
    repo_root:
        Repository root path.
    options:
        Scan options.

    Yields
    ------
    pathlib.Path
        Relative paths for matching files.
    """
    repo_root = repo_root.resolve()
    seen: set[str] = set()

    for pat in options.include_globs:
        for path in repo_root.glob(pat):
            if path.is_dir():
                continue
            if not options.follow_symlinks and path.is_symlink():
                continue

            rel = path.relative_to(repo_root)
            if _is_excluded_dir(rel, options.exclude_dirs):
                continue

            rel_posix = rel.as_posix()
            if options.exclude_globs and _matches_any_glob(rel_posix, options.exclude_globs):
                continue

            if rel_posix not in seen:
                seen.add(rel_posix)
                yield rel


def scan_repo(repo_root: PathLike, options: RepoScanOptions | None = None) -> pa.Table:
    """Scan the repo for Python files and return a repo_files table.

    Parameters
    ----------
    repo_root:
        Repository root path.
    options:
        Scan options.

    Returns
    -------
    pyarrow.Table
        Table of repo file metadata.
    """
    options = options or RepoScanOptions()
    repo_root_path = ensure_path(repo_root).resolve()

    max_files = options.max_files
    if max_files is not None and max_files <= 0:
        return pa.Table.from_pylist([], schema=REPO_FILES_SCHEMA)

    rows: list[dict[str, object]] = []
    for rel in sorted(iter_repo_files(repo_root_path, options), key=lambda p: p.as_posix()):
        abs_path = (repo_root_path / rel).resolve()
        rel_posix = rel.as_posix()

        try:
            data = abs_path.read_bytes()
        except OSError:
            continue

        if options.max_file_bytes is not None and len(data) > options.max_file_bytes:
            continue

        file_sha256 = _sha256_hex(data)
        file_id = stable_id("file", *(filter(None, [options.repo_id, rel_posix])))

        encoding = "utf-8"
        text: str | None = None
        if options.include_text:
            encoding, text = _detect_encoding_and_decode(data)

        rows.append(
            {
                "schema_version": SCHEMA_VERSION,
                "file_id": file_id,
                "path": rel_posix,
                "abs_path": str(abs_path),
                "size_bytes": len(data),
                "file_sha256": file_sha256,
                "encoding": encoding,
                "text": text,
                "bytes": data if options.include_bytes else None,
            }
        )
        if max_files is not None and len(rows) >= max_files:
            break

    return pa.Table.from_pylist(rows, schema=REPO_FILES_SCHEMA)

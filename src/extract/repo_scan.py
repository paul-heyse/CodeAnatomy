from __future__ import annotations

import fnmatch
import hashlib
import io
import os
import tokenize
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable, Iterator, Optional, Sequence

import pyarrow as pa


SCHEMA_VERSION = 1


def stable_id(prefix: str, *parts: str) -> str:
    """
    Stable, deterministic ID builder (content-addressed over string parts).

    Use this for:
      - file_id
      - extraction-local ids (cst_def_id, scip_occ_id, etc.)
    """
    h = hashlib.sha1()
    for p in parts:
        h.update(p.encode("utf-8"))
        h.update(b"\x1f")
    return f"{prefix}:{h.hexdigest()}"


@dataclass(frozen=True)
class RepoScanOptions:
    repo_id: Optional[str] = None
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
    max_file_bytes: Optional[int] = None


REPO_FILES_SCHEMA = pa.schema(
    [
        ("schema_version", pa.int32()),
        ("file_id", pa.string()),
        ("path", pa.string()),         # repo-relative POSIX
        ("abs_path", pa.string()),     # absolute path on disk
        ("size_bytes", pa.int64()),
        ("file_sha256", pa.string()),  # sha256 hex of file bytes
        ("encoding", pa.string()),
        ("text", pa.string()),         # decoded best-effort (nullable)
        ("bytes", pa.binary()),        # raw bytes (nullable if include_bytes=False)
    ]
)


def _is_excluded_dir(rel_path: Path, exclude_dirs: Sequence[str]) -> bool:
    parts = set(rel_path.parts)
    return any(d in parts for d in exclude_dirs)


def _matches_any_glob(path_posix: str, globs: Sequence[str]) -> bool:
    return any(fnmatch.fnmatch(path_posix, g) for g in globs)


def _sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _detect_encoding_and_decode(data: bytes) -> tuple[str, Optional[str]]:
    """
    Detect encoding using tokenize rules (PEP 263) and decode best-effort.
    """
    try:
        encoding, _ = tokenize.detect_encoding(io.BytesIO(data).readline)
    except Exception:
        encoding = "utf-8"
    try:
        text = data.decode(encoding, errors="replace")
        return encoding, text
    except Exception:
        return encoding, None


def iter_repo_files(repo_root: Path, options: RepoScanOptions) -> Iterator[Path]:
    repo_root = repo_root.resolve()
    seen: set[str] = set()

    for pat in options.include_globs:
        for p in repo_root.glob(pat):
            if p.is_dir():
                continue
            if not options.follow_symlinks and p.is_symlink():
                continue

            rel = p.relative_to(repo_root)
            if _is_excluded_dir(rel, options.exclude_dirs):
                continue

            rel_posix = rel.as_posix()
            if options.exclude_globs and _matches_any_glob(rel_posix, options.exclude_globs):
                continue

            if rel_posix not in seen:
                seen.add(rel_posix)
                yield rel


def scan_repo(repo_root: Path, options: Optional[RepoScanOptions] = None) -> pa.Table:
    """
    Scans repo for Python files and returns a "repo_files" Arrow table.

    Output contract is used by all downstream extractors.
    """
    options = options or RepoScanOptions()
    repo_root = repo_root.resolve()

    rows: list[dict] = []
    for rel in sorted(iter_repo_files(repo_root, options), key=lambda p: p.as_posix()):
        abs_path = (repo_root / rel).resolve()
        rel_posix = rel.as_posix()

        try:
            data = abs_path.read_bytes()
        except Exception:
            # unreadable file -> skip; you can also choose to emit an error table later
            continue

        if options.max_file_bytes is not None and len(data) > options.max_file_bytes:
            continue

        file_sha256 = _sha256_hex(data)
        file_id = stable_id("file", *(filter(None, [options.repo_id, rel_posix])))

        encoding = "utf-8"
        text: Optional[str] = None
        if options.include_text:
            encoding, text = _detect_encoding_and_decode(data)

        row = {
            "schema_version": SCHEMA_VERSION,
            "file_id": file_id,
            "path": rel_posix,
            "abs_path": str(abs_path),
            "size_bytes": int(len(data)),
            "file_sha256": file_sha256,
            "encoding": encoding,
            "text": text,
            "bytes": data if options.include_bytes else None,
        }
        rows.append(row)

    return pa.Table.from_pylist(rows, schema=REPO_FILES_SCHEMA)

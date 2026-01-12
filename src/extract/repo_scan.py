"""Scan a repository for source files and capture file metadata using shared helpers."""

from __future__ import annotations

import fnmatch
import hashlib
import io
import tokenize
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, overload

import pyarrow as pa

from arrowdsl.compute.expr_specs import HashExprSpec
from arrowdsl.core.context import ExecutionContext, OrderingLevel, RuntimeProfile
from arrowdsl.core.interop import RecordBatchReaderLike, TableLike
from arrowdsl.plan.plan import Plan
from arrowdsl.plan.query import ProjectionSpec, QuerySpec
from arrowdsl.plan.runner import run_plan
from arrowdsl.schema.schema import empty_table
from core_types import PathLike, ensure_path
from extract.hash_specs import repo_file_id_spec
from extract.spec_helpers import (
    DatasetRegistration,
    merge_metadata_specs,
    options_metadata_spec,
    ordering_metadata_spec,
    register_dataset,
)
from extract.tables import align_plan, plan_from_rows
from schema_spec.specs import ArrowFieldSpec, file_identity_bundle

SCHEMA_VERSION = 1


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


REPO_FILES_FIELDS = [
    ArrowFieldSpec(name="abs_path", dtype=pa.string()),
    ArrowFieldSpec(name="size_bytes", dtype=pa.int64()),
    ArrowFieldSpec(name="encoding", dtype=pa.string()),
    ArrowFieldSpec(name="text", dtype=pa.string()),
    ArrowFieldSpec(name="bytes", dtype=pa.binary()),
]

REPO_FILES_BASE_COLUMNS = tuple(
    field.name for field in (*file_identity_bundle().fields, *REPO_FILES_FIELDS)
)

_REPO_METADATA = ordering_metadata_spec(
    OrderingLevel.IMPLICIT,
    keys=(("path", "ascending"),),
    extra={
        b"extractor_name": b"repo_scan",
        b"extractor_version": str(SCHEMA_VERSION).encode("utf-8"),
    },
)


def repo_files_query(repo_id: str | None) -> QuerySpec:
    """Return the QuerySpec for repo file scanning.

    Returns
    -------
    QuerySpec
        QuerySpec for repo file projection.
    """
    return QuerySpec(
        projection=ProjectionSpec(
            base=REPO_FILES_BASE_COLUMNS,
            derived={"file_id": HashExprSpec(spec=repo_file_id_spec(repo_id))},
        )
    )


REPO_FILES_QUERY = repo_files_query(None)

REPO_FILES_SPEC = register_dataset(
    name="repo_files_v1",
    version=SCHEMA_VERSION,
    bundles=(file_identity_bundle(),),
    fields=REPO_FILES_FIELDS,
    registration=DatasetRegistration(
        query_spec=REPO_FILES_QUERY,
        metadata_spec=_REPO_METADATA,
    ),
)

REPO_FILES_SCHEMA = REPO_FILES_SPEC.schema()


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


def _build_repo_file_row(
    *,
    rel: Path,
    repo_root: Path,
    options: RepoScanOptions,
) -> dict[str, object] | None:
    abs_path = (repo_root / rel).resolve()
    rel_posix = rel.as_posix()

    try:
        data = abs_path.read_bytes()
    except OSError:
        return None

    if options.max_file_bytes is not None and len(data) > options.max_file_bytes:
        return None

    file_sha256 = _sha256_hex(data)
    encoding = "utf-8"
    text: str | None = None
    if options.include_text:
        encoding, text = _detect_encoding_and_decode(data)

    return {
        "file_id": None,
        "path": rel_posix,
        "abs_path": str(abs_path),
        "size_bytes": len(data),
        "file_sha256": file_sha256,
        "encoding": encoding,
        "text": text,
        "bytes": data if options.include_bytes else None,
    }


@overload
def scan_repo(
    repo_root: PathLike,
    options: RepoScanOptions | None = None,
    ctx: ExecutionContext | None = None,
    *,
    prefer_reader: Literal[False] = False,
) -> TableLike: ...


@overload
def scan_repo(
    repo_root: PathLike,
    options: RepoScanOptions | None = None,
    ctx: ExecutionContext | None = None,
    *,
    prefer_reader: Literal[True],
) -> TableLike | RecordBatchReaderLike: ...


def scan_repo(
    repo_root: PathLike,
    options: RepoScanOptions | None = None,
    ctx: ExecutionContext | None = None,
    *,
    prefer_reader: bool = False,
) -> TableLike | RecordBatchReaderLike:
    """Scan the repo for Python files and return a repo_files table.

    Parameters
    ----------
    repo_root:
        Repository root path.
    options:
        Scan options.
    ctx:
        Execution context for plan execution.
    prefer_reader:
        When True, return a streaming reader when possible.

    Returns
    -------
    TableLike | RecordBatchReaderLike
        Repo file metadata output.
    """
    options = options or RepoScanOptions()
    ctx = ctx or ExecutionContext(runtime=RuntimeProfile(name="DEFAULT"))
    metadata_spec = merge_metadata_specs(
        _REPO_METADATA,
        options_metadata_spec(options=options, repo_id=options.repo_id),
    )
    max_files = options.max_files
    if max_files is not None and max_files <= 0:
        empty_plan = Plan.table_source(empty_table(REPO_FILES_SCHEMA))
        return run_plan(
            empty_plan,
            ctx=ctx,
            prefer_reader=prefer_reader,
            metadata_spec=metadata_spec,
            attach_ordering_metadata=True,
        ).value

    plan = scan_repo_plan(repo_root, options=options, ctx=ctx)
    return run_plan(
        plan,
        ctx=ctx,
        prefer_reader=prefer_reader,
        metadata_spec=metadata_spec,
        attach_ordering_metadata=True,
    ).value


def scan_repo_plan(
    repo_root: PathLike,
    *,
    options: RepoScanOptions,
    ctx: ExecutionContext,
) -> Plan:
    """Build the plan for repository scanning.

    Returns
    -------
    Plan
        Plan emitting repo file metadata.
    """
    repo_root_path = ensure_path(repo_root).resolve()

    def iter_rows() -> Iterator[dict[str, object]]:
        count = 0
        for rel in sorted(iter_repo_files(repo_root_path, options), key=lambda p: p.as_posix()):
            row = _build_repo_file_row(rel=rel, repo_root=repo_root_path, options=options)
            if row is None:
                continue
            yield row
            count += 1
            if options.max_files is not None and count >= options.max_files:
                break

    plan = plan_from_rows(iter_rows(), schema=REPO_FILES_SCHEMA, label="repo_files")
    plan = repo_files_query(options.repo_id).apply_to_plan(plan, ctx=ctx)
    return align_plan(
        plan,
        schema=REPO_FILES_SCHEMA,
        available=REPO_FILES_SCHEMA.names,
        ctx=ctx,
    )

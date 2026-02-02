"""Administrative commands for cq CLI.

This module contains the index and cache management commands.
"""

from __future__ import annotations

import datetime
import sys
from pathlib import Path
from typing import Annotated

from cyclopts import Parameter

# Import CliContext at runtime for cyclopts type hint resolution
from tools.cq.cli_app.context import CliContext


def index(
    *,
    rebuild: Annotated[bool, Parameter(help="Force full rebuild of the index", negative="")] = False,
    stats: Annotated[bool, Parameter(help="Show index statistics", negative="")] = False,
    clear: Annotated[bool, Parameter(help="Clear the index cache", negative="")] = False,
    ctx: Annotated[CliContext | None, Parameter(parse=False)] = None,
) -> int:
    """Manage the ast-grep scan index cache.

    The index caches ast-grep scan results and only rescans files that have changed.
    This significantly speeds up repeated queries on large codebases.

    Operations:
        cq index           Update index for changed files (default)
        cq index --rebuild Full rebuild of the index
        cq index --stats   Show index statistics
        cq index --clear   Clear the index
    """
    from tools.cq.index.files import build_repo_file_index, tabulate_files
    from tools.cq.index.repo import resolve_repo_context
    from tools.cq.index.sqlite_cache import IndexCache
    from tools.cq.query.sg_parser import ALL_RECORD_TYPES, group_records_by_file, sg_scan

    if ctx is None:
        msg = "Context not injected"
        raise RuntimeError(msg)

    repo_context = resolve_repo_context(ctx.root)

    rule_version = ctx.toolchain.sgpy_version or "unknown"

    with IndexCache(ctx.root, rule_version) as cache:
        if clear:
            cache.clear()
            sys.stdout.write("Index cleared.\n")
            return 0

        if stats:
            cache_stats = cache.get_stats()
            sys.stdout.write("Index Statistics:\n")
            sys.stdout.write(f"  Files cached: {cache_stats.total_files}\n")
            sys.stdout.write(f"  Total records: {cache_stats.total_records}\n")
            sys.stdout.write(f"  Rule version: {cache_stats.rule_version}\n")
            sys.stdout.write(f"  Database size: {cache_stats.database_size_bytes:,} bytes\n")
            return 0

        if rebuild:
            cache.clear()
            sys.stdout.write("Index cleared for rebuild.\n")

        # Update index by scanning Python files
        repo_index = build_repo_file_index(repo_context)
        record_types_set = set(ALL_RECORD_TYPES)
        file_result = tabulate_files(
            repo_index,
            [ctx.root],
            None,
            extensions=(".py",),
        )
        py_files = file_result.files

        sys.stdout.write(f"Found {len(py_files)} Python files.\n")

        # Check which files need rescanning
        files_to_scan: list[Path] = []
        for path in py_files:
            if cache.needs_rescan(path):
                files_to_scan.append(path)

        if not files_to_scan:
            sys.stdout.write("Index is up to date.\n")
            return 0

        sys.stdout.write(f"Scanning {len(files_to_scan)} changed files...\n")

        # Scan in batches
        batch_size = 100
        total_records = 0

        for i in range(0, len(files_to_scan), batch_size):
            batch = files_to_scan[i : i + batch_size]
            records = sg_scan(batch, root=ctx.root)
            records_by_file = group_records_by_file(records)

            for file_path_str, file_records in records_by_file.items():
                records_data: list[dict[str, object]] = [
                    {
                        "record": r.record,
                        "kind": r.kind,
                        "file": r.file,
                        "start_line": r.start_line,
                        "start_col": r.start_col,
                        "end_line": r.end_line,
                        "end_col": r.end_col,
                        "text": r.text,
                        "rule_id": r.rule_id,
                    }
                    for r in file_records
                ]
                file_path_obj = ctx.root / file_path_str
                if file_path_obj.exists():
                    cache.store(file_path_obj, records_data, record_types_set)
                    total_records += len(file_records)

            for file_path in batch:
                rel_path = file_path.relative_to(ctx.root).as_posix()
                if rel_path not in records_by_file:
                    cache.store(file_path, [], record_types_set)

            sys.stdout.write(f"  Processed {min(i + batch_size, len(files_to_scan))}/{len(files_to_scan)} files\n")

        sys.stdout.write(f"Index updated: {len(files_to_scan)} files, {total_records} records.\n")
        return 0


def cache(
    *,
    stats: Annotated[bool, Parameter(help="Show cache statistics", negative="")] = False,
    clear: Annotated[bool, Parameter(help="Clear the query cache", negative="")] = False,
    ctx: Annotated[CliContext | None, Parameter(parse=False)] = None,
) -> int:
    """Manage the query result cache.

    The cache stores query results and invalidates them when source files change.
    This significantly speeds up repeated queries on large codebases.

    Operations:
        cq cache --stats   Show cache statistics
        cq cache --clear   Clear the cache
    """
    from tools.cq.index.query_cache import QueryCache

    if ctx is None:
        msg = "Context not injected"
        raise RuntimeError(msg)

    cache_dir = ctx.root / ".cq" / "cache"

    with QueryCache(cache_dir) as qcache:
        if clear:
            qcache.clear()
            sys.stdout.write("Query cache cleared.\n")
            return 0

        if stats:
            cache_stats = qcache.stats()
            sys.stdout.write("Query Cache Statistics:\n")
            sys.stdout.write(f"  Total entries: {cache_stats.total_entries}\n")
            sys.stdout.write(f"  Unique files: {cache_stats.unique_files}\n")
            sys.stdout.write(f"  Database size: {cache_stats.database_size_bytes:,} bytes\n")
            if cache_stats.oldest_entry:
                oldest = datetime.datetime.fromtimestamp(cache_stats.oldest_entry, tz=datetime.UTC)
                newest = datetime.datetime.fromtimestamp(cache_stats.newest_entry or 0, tz=datetime.UTC)
                sys.stdout.write(f"  Oldest entry: {oldest.isoformat()}\n")
                sys.stdout.write(f"  Newest entry: {newest.isoformat()}\n")
            return 0

        # Default: show stats
        cache_stats = qcache.stats()
        sys.stdout.write("Query Cache Statistics:\n")
        sys.stdout.write(f"  Total entries: {cache_stats.total_entries}\n")
        sys.stdout.write(f"  Unique files: {cache_stats.unique_files}\n")
        sys.stdout.write(f"  Database size: {cache_stats.database_size_bytes:,} bytes\n")
        return 0

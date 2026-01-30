"""Backward-compatible re-exports for repository scanning helpers."""

from __future__ import annotations

from extract.scanning.repo_scan import (
    RepoScanBundle,
    RepoScanOptions,
    _sha256_path,
    default_repo_scan_options,
    repo_files_query,
    scan_repo,
    scan_repo_plan,
    scan_repo_plans,
    scan_repo_tables,
)

__all__ = [
    "RepoScanBundle",
    "RepoScanOptions",
    "_sha256_path",
    "default_repo_scan_options",
    "repo_files_query",
    "scan_repo",
    "scan_repo_plan",
    "scan_repo_plans",
    "scan_repo_tables",
]

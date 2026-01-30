"""Repository scanning and scope filtering.

This subpackage provides:
- File enumeration and scanning (repo_scan)
- Repository scope resolution (repo_scope)
- Pathspec-backed scope rules (scope_rules)
- Scope manifest construction (scope_manifest)
"""

from __future__ import annotations

from extract.scanning.repo_scan import (
    RepoScanBundle,
    RepoScanOptions,
    default_repo_scan_options,
    repo_files_query,
    scan_repo,
    scan_repo_plan,
    scan_repo_plans,
    scan_repo_tables,
)
from extract.scanning.repo_scope import (
    DEFAULT_EXCLUDE_GLOBS,
    RepoScope,
    RepoScopeOptions,
    default_repo_scope_options,
    resolve_repo_scope,
    scope_rule_lines,
)
from extract.scanning.scope_manifest import (
    ScopeManifest,
    ScopeManifestEntry,
    ScopeManifestOptions,
    build_scope_manifest,
)
from extract.scanning.scope_rules import (
    ScopeRuleDecision,
    ScopeRuleSet,
    build_scope_rules,
    check_scope_path,
    explain_scope_paths,
)

__all__ = [
    # repo_scope
    "DEFAULT_EXCLUDE_GLOBS",
    "RepoScanBundle",
    "RepoScanOptions",
    "RepoScope",
    "RepoScopeOptions",
    # scope_manifest
    "ScopeManifest",
    "ScopeManifestEntry",
    "ScopeManifestOptions",
    # scope_rules
    "ScopeRuleDecision",
    "ScopeRuleSet",
    "build_scope_manifest",
    "build_scope_rules",
    "check_scope_path",
    "default_repo_scan_options",
    "default_repo_scope_options",
    "explain_scope_paths",
    "repo_files_query",
    "resolve_repo_scope",
    "scan_repo",
    "scan_repo_plan",
    "scan_repo_plans",
    "scan_repo_tables",
    "scope_rule_lines",
]

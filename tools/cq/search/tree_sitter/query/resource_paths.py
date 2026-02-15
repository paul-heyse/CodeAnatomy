"""Canonical query resource path helpers for tree-sitter lanes."""

from __future__ import annotations

from pathlib import Path

_QUERY_ROOT = Path(__file__).resolve().parents[2] / "queries"


def query_pack_dir(language: str) -> Path:
    """Return the query pack directory for one language lane."""
    return _QUERY_ROOT / language


def query_pack_path(language: str, pack_name: str) -> Path:
    """Return the absolute path for one query pack source file."""
    return query_pack_dir(language) / pack_name


def query_contracts_path(language: str) -> Path:
    """Return the absolute path for one query contracts YAML file."""
    return query_pack_dir(language) / "contracts.yaml"


def diagnostics_query_path(language: str) -> Path:
    """Return the absolute path for one diagnostics query pack file."""
    return query_pack_dir(language) / "95_diagnostics.scm"


__all__ = [
    "diagnostics_query_path",
    "query_contracts_path",
    "query_pack_dir",
    "query_pack_path",
]

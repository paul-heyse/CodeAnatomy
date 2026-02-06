"""Cached schema fingerprint utilities for extraction modules."""

from __future__ import annotations

from functools import cache

from datafusion_engine.extract.registry import dataset_schema
from datafusion_engine.identity import schema_identity_hash


@cache
def cached_schema_identity_hash(dataset_name: str) -> str:
    """Return cached schema fingerprint for a dataset.

    Parameters
    ----------
    dataset_name
        Name of the dataset to fingerprint.

    Returns:
    -------
    str
        SHA-256 fingerprint of the dataset schema.
    """
    return schema_identity_hash(dataset_schema(dataset_name))


def ast_files_fingerprint() -> str:
    """Return cached schema fingerprint for AST dataset.

    Returns:
    -------
    str
        Cached schema fingerprint.
    """
    return cached_schema_identity_hash("ast_files_v1")


def bytecode_files_fingerprint() -> str:
    """Return cached schema fingerprint for bytecode dataset.

    Returns:
    -------
    str
        Cached schema fingerprint.
    """
    return cached_schema_identity_hash("bytecode_files_v1")


def libcst_files_fingerprint() -> str:
    """Return cached schema fingerprint for LibCST dataset.

    Returns:
    -------
    str
        Cached schema fingerprint.
    """
    return cached_schema_identity_hash("libcst_files_v1")


def symtable_files_fingerprint() -> str:
    """Return cached schema fingerprint for symtable dataset.

    Returns:
    -------
    str
        Cached schema fingerprint.
    """
    return cached_schema_identity_hash("symtable_files_v1")


def tree_sitter_files_fingerprint() -> str:
    """Return cached schema fingerprint for tree-sitter dataset.

    Returns:
    -------
    str
        Cached schema fingerprint.
    """
    return cached_schema_identity_hash("tree_sitter_files_v1")


def repo_file_blobs_fingerprint() -> str:
    """Return cached schema fingerprint for repo blob dataset.

    Returns:
    -------
    str
        Cached schema fingerprint.
    """
    return cached_schema_identity_hash("repo_file_blobs_v1")


def repo_files_fingerprint() -> str:
    """Return cached schema fingerprint for repo files dataset.

    Returns:
    -------
    str
        Cached schema fingerprint.
    """
    return cached_schema_identity_hash("repo_files_v1")


def file_line_index_fingerprint() -> str:
    """Return cached schema fingerprint for line index dataset.

    Returns:
    -------
    str
        Cached schema fingerprint.
    """
    return cached_schema_identity_hash("file_line_index_v1")


__all__ = [
    "ast_files_fingerprint",
    "bytecode_files_fingerprint",
    "cached_schema_identity_hash",
    "file_line_index_fingerprint",
    "libcst_files_fingerprint",
    "repo_file_blobs_fingerprint",
    "repo_files_fingerprint",
    "symtable_files_fingerprint",
    "tree_sitter_files_fingerprint",
]

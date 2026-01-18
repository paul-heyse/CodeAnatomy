"""Tests for filesystem bridge helpers."""

from __future__ import annotations

import pyarrow.fs as pafs

from arrowdsl.plan.source_normalize import from_uri


def test_from_uri_wraps_uri_in_subtree_filesystem() -> None:
    """Wrap URI sources in SubTreeFileSystem for normalized paths."""
    fs, path = from_uri("file:///tmp/arrowdsl_test")
    assert isinstance(fs, pafs.SubTreeFileSystem)
    assert not path


def test_from_uri_wraps_with_provided_filesystem() -> None:
    """Wrap provided filesystem when given a URI source."""
    base_fs = pafs.LocalFileSystem()
    fs, path = from_uri("file:///tmp/arrowdsl_test", filesystem=base_fs)
    assert isinstance(fs, pafs.SubTreeFileSystem)
    assert not path

"""Tests for common macro target export surface."""

from __future__ import annotations

from tools.cq.macros.common import targets


def test_common_targets_exports_resolver() -> None:
    assert callable(targets.resolve_target_files)

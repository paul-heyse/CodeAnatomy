"""Tests for shared macros constants."""

from __future__ import annotations

from tools.cq.macros.constants import (
    CALLS_TARGET_CALLEE_PREVIEW,
    FRONT_DOOR_PREVIEW_PER_SLICE,
)

EXPECTED_FRONT_DOOR_PREVIEW_PER_SLICE = 5
EXPECTED_CALLS_TARGET_CALLEE_PREVIEW = 10


def test_shared_macro_constants_values() -> None:
    """Shared macro constants retain expected default preview counts."""
    assert FRONT_DOOR_PREVIEW_PER_SLICE == EXPECTED_FRONT_DOOR_PREVIEW_PER_SLICE
    assert CALLS_TARGET_CALLEE_PREVIEW == EXPECTED_CALLS_TARGET_CALLEE_PREVIEW

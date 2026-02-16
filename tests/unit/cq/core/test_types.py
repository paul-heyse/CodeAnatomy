"""Tests for core shared type enums."""

from __future__ import annotations

from tools.cq.core.types import LdmdSliceMode


def test_ldmd_slice_mode_values() -> None:
    """Slice mode enum exposes stable wire values."""
    assert LdmdSliceMode.full.value == "full"
    assert LdmdSliceMode.preview.value == "preview"
    assert LdmdSliceMode.tldr.value == "tldr"


def test_ldmd_slice_mode_str() -> None:
    """Slice mode string conversion returns the wire value."""
    assert str(LdmdSliceMode.preview) == "preview"

"""Tests for write destination validation helpers."""

from __future__ import annotations

import pytest

from datafusion_engine.io.write_validation import validate_destination


def test_validate_destination_rejects_empty_strings() -> None:
    """Blank destinations are rejected with a clear validation error."""
    with pytest.raises(ValueError, match="destination"):
        validate_destination("   ")

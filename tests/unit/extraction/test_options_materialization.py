"""Extraction options tests for materialization-mode normalization."""

from __future__ import annotations

import pytest

from extraction.options import normalize_extraction_options


def test_normalize_extraction_options_accepts_materialization_mode() -> None:
    """Valid materialization_mode should be preserved by normalization."""
    options = normalize_extraction_options({"materialization_mode": "datafusion_copy"})
    assert options.materialization_mode == "datafusion_copy"


def test_normalize_extraction_options_rejects_invalid_materialization_mode() -> None:
    """Invalid materialization_mode should raise validation error."""
    with pytest.raises(ValueError, match="materialization_mode"):
        normalize_extraction_options({"materialization_mode": "invalid"})

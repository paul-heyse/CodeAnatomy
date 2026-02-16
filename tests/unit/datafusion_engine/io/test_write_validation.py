# ruff: noqa: D100, D103, INP001, PT011
from __future__ import annotations

import pytest

from datafusion_engine.io.write_validation import validate_destination


def test_validate_destination_rejects_empty_strings() -> None:
    with pytest.raises(ValueError):
        validate_destination("   ")

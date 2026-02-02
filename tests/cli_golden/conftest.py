"""Pytest configuration for CLI golden tests."""

from __future__ import annotations

import pytest


@pytest.fixture
def update_golden(request: pytest.FixtureRequest) -> bool:
    """Fixture to check if golden files should be updated.

    Returns
    -------
    bool
        True if --update-golden was passed.
    """
    return bool(request.config.getoption("--update-golden"))

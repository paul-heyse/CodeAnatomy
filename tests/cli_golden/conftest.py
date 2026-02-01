"""Pytest configuration for CLI golden tests."""

from __future__ import annotations

import pytest


def pytest_addoption(parser: pytest.Parser) -> None:
    """Add --update-golden option for regenerating golden files."""
    parser.addoption(
        "--update-golden",
        action="store_true",
        default=False,
        help="Update golden snapshot files with current output",
    )


@pytest.fixture
def update_golden(request: pytest.FixtureRequest) -> bool:
    """Fixture to check if golden files should be updated.

    Returns
    -------
    bool
        True if --update-golden was passed.
    """
    return bool(request.config.getoption("--update-golden"))

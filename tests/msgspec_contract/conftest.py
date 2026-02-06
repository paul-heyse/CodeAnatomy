"""Pytest config for msgspec contract tests."""

from __future__ import annotations

import os

import pytest


def pytest_addoption(parser: pytest.Parser) -> None:
    """Register msgspec contract pytest options."""
    parser.addoption(
        "--update-goldens",
        action="store_true",
        default=False,
        help="Regenerate msgspec contract snapshots.",
    )


@pytest.fixture(scope="session")
def update_goldens(pytestconfig: pytest.Config) -> bool:
    """Return True when golden files should be regenerated.

    Returns:
    -------
    bool
        True when golden files should be regenerated.
    """
    update_flag = pytestconfig.getoption("--update-goldens")
    env_flag = os.getenv("UPDATE_GOLDENS") == "1"
    return bool(update_flag or env_flag)

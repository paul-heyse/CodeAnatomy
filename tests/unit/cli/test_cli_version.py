"""Tests for CLI version reporting."""

from __future__ import annotations

import json

import pytest

from cli.commands.version import get_version, get_version_info, version_command


def test_get_version_returns_string() -> None:
    """Ensure get_version returns a non-empty string."""
    version = get_version()
    assert isinstance(version, str)
    assert version


def test_get_version_info_structure() -> None:
    """Ensure version info payload includes expected keys."""
    payload = get_version_info()
    assert "codeanatomy" in payload
    assert "python" in payload
    assert "dependencies" in payload
    assert isinstance(payload["dependencies"], dict)


def test_version_command_outputs_json(capsys: pytest.CaptureFixture[str]) -> None:
    """Ensure version_command writes JSON to stdout."""
    exit_code = version_command()
    assert exit_code == 0
    captured = capsys.readouterr().out
    payload = json.loads(captured)
    assert "codeanatomy" in payload
    assert "python" in payload
    assert "dependencies" in payload

"""Golden snapshot tests for config output."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from cli.commands.config import show_config
from tests.cli_golden._support.goldens import assert_text_snapshot


def _write_config(path: Path) -> None:
    contents = """
[plan]
allow_partial = true

[cache]
policy_profile = "default"
"""
    path.write_text(contents.strip() + "\n", encoding="utf-8")


def _capture_with_sources(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> str:
    config_path = tmp_path / "codeanatomy.toml"
    _write_config(config_path)
    monkeypatch.chdir(tmp_path)

    exit_code = show_config(with_sources=True, run_context=None)
    assert exit_code == 0

    captured = capsys.readouterr().out
    normalized = captured.replace(str(tmp_path), "<CONFIG_DIR>")

    # Ensure stable key ordering for golden comparison
    payload = json.loads(normalized)
    return json.dumps(payload, indent=2, sort_keys=True) + "\n"


def test_config_show_with_sources(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    *,
    update_golden: bool,
) -> None:
    """Ensure config show --with-sources output matches golden snapshot."""
    output = _capture_with_sources(tmp_path, monkeypatch, capsys)
    assert_text_snapshot("config_show_with_sources.txt", output, update=update_golden)

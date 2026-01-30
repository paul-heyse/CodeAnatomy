"""Integration tests for the CLI meta launcher."""

from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING

from cli.app import meta_launcher

if TYPE_CHECKING:
    import pytest


def test_meta_launcher_loads_config_from_cwd(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Ensure meta launcher forwards config file contents to commands."""
    config_path = tmp_path / "codeanatomy.toml"
    config_path.write_text(
        """
[plan]
allow_partial = true

[cache]
policy_profile = "integration"
""".lstrip(),
        encoding="utf-8",
    )

    monkeypatch.chdir(tmp_path)
    exit_code = meta_launcher("config", "show")

    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert exit_code == 0
    assert payload["plan_allow_partial"] is True
    assert payload["cache_policy_profile"] == "integration"

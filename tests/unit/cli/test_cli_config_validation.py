"""Tests for typed configuration validation errors."""

from __future__ import annotations

from pathlib import Path

import pytest

from cli.config_loader import load_effective_config


def test_config_validation_unknown_field(tmp_path: Path) -> None:
    """Unknown top-level fields should raise a validation error."""
    config_path = tmp_path / "codeanatomy.toml"
    config_path.write_text("unknown_field = 123\n", encoding="utf-8")

    with pytest.raises(ValueError, match="Config validation failed") as excinfo:
        load_effective_config(str(config_path))

    message = str(excinfo.value)
    assert "Config validation failed" in message
    assert "unknown_field" in message


def test_config_validation_type_error(tmp_path: Path) -> None:
    """Invalid field types should raise a validation error."""
    config_path = tmp_path / "codeanatomy.toml"
    config_path.write_text("runtime_profile_name = 123\n", encoding="utf-8")

    with pytest.raises(ValueError, match="Config validation failed") as excinfo:
        load_effective_config(str(config_path))

    message = str(excinfo.value)
    assert "Config validation failed" in message
    assert "runtime_profile_name" in message

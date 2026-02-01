"""Tests for CLI environment variable support."""

from __future__ import annotations

from cli.config_loader import _parse_env_value


class TestParseEnvValue:
    """Test environment value parsing."""

    def test_parses_true_values(self) -> None:
        """Should parse various true representations."""
        for value in ("true", "True", "TRUE", "1", "yes", "Yes", "on", "ON"):
            assert _parse_env_value(value) is True

    def test_parses_false_values(self) -> None:
        """Should parse various false representations."""
        for value in ("false", "False", "FALSE", "0", "no", "No", "off", "OFF"):
            assert _parse_env_value(value) is False

    def test_parses_integers(self) -> None:
        """Should parse integer values."""
        assert _parse_env_value("42") == 42
        assert _parse_env_value("-1") == -1
        assert _parse_env_value("0") is False  # 0 is falsy

    def test_parses_floats(self) -> None:
        """Should parse float values."""
        assert _parse_env_value("3.14") == 3.14
        assert _parse_env_value("-2.5") == -2.5

    def test_returns_string_for_other(self) -> None:
        """Should return string for non-parseable values."""
        assert _parse_env_value("hello") == "hello"
        assert _parse_env_value("/path/to/file") == "/path/to/file"


class TestEnvVarMappings:
    """Test environment variable mappings."""

    def test_mappings_exist(self) -> None:
        """Should have standard env var mappings."""
        from cli.config_loader import _get_env_var_mappings

        mappings = _get_env_var_mappings()
        assert "log_level" in mappings
        assert "runtime_profile_name" in mappings
        assert "output_dir" in mappings

    def test_mappings_use_codeanatomy_prefix(self) -> None:
        """All env var mappings should use CODEANATOMY_ prefix."""
        from cli.config_loader import _get_env_var_mappings

        mappings = _get_env_var_mappings()
        for env_var in mappings.values():
            assert env_var.startswith("CODEANATOMY_")

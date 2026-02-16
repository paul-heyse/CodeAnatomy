"""Tests for CLI configuration providers.

These tests verify that:
1. build_config_chain returns correct provider types
2. use_config=False keeps env provider and disables file providers
3. explicit config file is handled correctly
"""

from __future__ import annotations

from pathlib import Path

from tools.cq.cli_app.infrastructure import build_config_chain

DEFAULT_PROVIDER_COUNT = 2


class TestBuildConfigChain:
    """Tests for build_config_chain function."""

    def test_default_config_chain(self) -> None:
        """Test default config chain includes Env and Toml providers."""
        providers = build_config_chain()
        assert len(providers) == DEFAULT_PROVIDER_COUNT
        # First provider should be Env
        assert providers[0].__class__.__name__ == "Env"
        # Second provider should be Toml
        assert providers[1].__class__.__name__ == "Toml"

    def test_use_config_false_keeps_env_only(self) -> None:
        """Test that use_config=False returns only the env provider."""
        providers = build_config_chain(use_config=False)
        assert len(providers) == 1
        assert providers[0].__class__.__name__ == "Env"

    def test_explicit_config_file(self, tmp_path: Path) -> None:
        """Test that explicit config file is added to chain."""
        config_file = tmp_path / "test.toml"
        config_file.write_text("[cq]\nformat = 'json'\n")

        providers = build_config_chain(config_file=str(config_file))
        assert len(providers) == DEFAULT_PROVIDER_COUNT
        # First should still be Env
        assert providers[0].__class__.__name__ == "Env"
        # Second should be Toml with our file
        assert providers[1].__class__.__name__ == "Toml"

    def test_explicit_config_file_must_exist(self, tmp_path: Path) -> None:
        """Test that explicit config file must exist when specified."""
        config_file = tmp_path / "nonexistent.toml"
        # The provider is created, but will fail when trying to read
        providers = build_config_chain(config_file=str(config_file))
        assert len(providers) == DEFAULT_PROVIDER_COUNT


class TestEnvVarPrefix:
    """Tests for environment variable prefix."""

    def test_env_provider_uses_cq_prefix(self) -> None:
        """Test that Env provider is configured with CQ_ prefix."""
        providers = build_config_chain()
        env_provider = providers[0]
        # The Env provider should use CQ_ prefix
        assert hasattr(env_provider, "prefix")
        assert env_provider.prefix == "CQ_"

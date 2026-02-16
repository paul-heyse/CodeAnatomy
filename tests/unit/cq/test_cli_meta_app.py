"""Tests for meta-app launcher pattern.

These tests verify that:
1. Global options appear in help from meta app
2. Context injection works for commands
3. Config precedence is correct
"""

from __future__ import annotations

from tools.cq.cli_app.app import (
    ConfigOptionArgs,
    GlobalOptionArgs,
    _build_launch_context,
    app,
)

VERBOSE_LEVEL_TWO = 2
DEFAULT_CONFIG_PROVIDER_COUNT = 2


class TestMetaAppParsing:
    """Tests for meta-app token parsing."""

    def test_meta_parses_global_options(self) -> None:
        """Test that meta app parses global options like --format."""
        # The meta app should intercept global options
        _cmd, bound, _extra = app.meta.parse_args(["calls", "foo", "--format", "json"])
        # Global options go to the meta launcher
        assert bound.kwargs["global_opts"].output_format is not None

    def test_meta_parses_verbose(self) -> None:
        """Test that repeated verbosity flags are counted by meta app."""
        _cmd, bound, _extra = app.meta.parse_args(["calls", "foo", "-vv"])
        assert bound.kwargs["global_opts"].verbose == VERBOSE_LEVEL_TWO

    def test_meta_parses_verbose_long_flag(self) -> None:
        """Test that --verbose increments verbosity without a value."""
        _cmd, bound, _extra = app.meta.parse_args(["calls", "foo", "--verbose"])
        assert bound.kwargs["global_opts"].verbose == 1

    def test_meta_parses_root(self) -> None:
        """Test that --root is parsed by meta app."""
        _cmd, bound, _extra = app.meta.parse_args(["calls", "foo", "--root", "/tmp"])
        assert bound.kwargs["global_opts"].root is not None


class TestCommandRegistration:
    """Tests for command registration via meta-app pattern."""

    def test_analysis_commands_parse(self) -> None:
        """Test that analysis commands can be parsed."""
        # Impact command
        _cmd, _bound, _ignored = app.parse_args(
            ["impact", "foo", "--param", "x"],
            exit_on_error=False,
            print_error=False,
        )
        # Calls command
        _cmd, _bound, _ignored = app.parse_args(
            ["calls", "foo"],
            exit_on_error=False,
            print_error=False,
        )

    def test_query_command_parse(self) -> None:
        """Test that query command can be parsed."""
        _cmd, _bound, _ignored = app.parse_args(
            ["q", "entity=function"],
            exit_on_error=False,
            print_error=False,
        )

    def test_report_command_parse(self) -> None:
        """Test that report command can be parsed."""
        _cmd, _bound, _ignored = app.parse_args(
            ["report", "refactor-impact", "--target", "function:foo"],
            exit_on_error=False,
            print_error=False,
        )

    def test_admin_commands_parse(self) -> None:
        """Test that admin commands can be parsed.

        Note: index and cache commands are deprecated stubs that print deprecation notices.
        """
        # Index command (deprecated)
        _cmd, _bound, _ignored = app.parse_args(
            ["index"],
            exit_on_error=False,
            print_error=False,
        )
        # Cache command (deprecated)
        _cmd, _bound, _ignored = app.parse_args(
            ["cache"],
            exit_on_error=False,
            print_error=False,
        )

    def test_run_command_parse(self) -> None:
        """Test that run command can be parsed."""
        _cmd, _bound, _ignored = app.parse_args(
            ["run", "--step", '{"type":"q","query":"entity=function name=foo"}'],
            exit_on_error=False,
            print_error=False,
        )

    def test_chain_command_parse(self) -> None:
        """Test that chain command can be parsed."""
        _cmd, _bound, _ignored = app.parse_args(
            ["chain", "q", "entity=function name=foo", "AND", "calls", "foo"],
            exit_on_error=False,
            print_error=False,
        )


class TestContextInjection:
    """Tests for context injection via parse=False."""

    def test_commands_have_ctx_param(self) -> None:
        """Test that commands have ctx parameter marked parse=False."""
        # Parse a command and check the ignored dict
        _cmd, _bound, ignored = app.parse_args(
            ["calls", "foo"],
            exit_on_error=False,
            print_error=False,
        )
        # 'ctx' should be in ignored (marked parse=False)
        assert "ctx" in ignored

    def test_admin_index_has_ctx_param(self) -> None:
        """Test that index command has ctx parameter.

        Note: index command is deprecated and no longer takes --stats.
        """
        _cmd, _bound, ignored = app.parse_args(
            ["index"],
            exit_on_error=False,
            print_error=False,
        )
        assert "ctx" in ignored

    def test_admin_cache_has_ctx_param(self) -> None:
        """Test that cache command has ctx parameter.

        Note: cache command is deprecated and no longer takes --stats.
        """
        _cmd, _bound, ignored = app.parse_args(
            ["cache"],
            exit_on_error=False,
            print_error=False,
        )
        assert "ctx" in ignored


class TestConfigChain:
    """Tests for configuration chain."""

    def test_app_has_config(self) -> None:
        """Test that app has config chain configured."""
        assert app.config is not None

    def test_no_config_option_exists(self) -> None:
        """Test that --no-config option is available."""
        _cmd, bound, _extra = app.meta.parse_args(["calls", "foo", "--no-config"])
        assert bound.kwargs["config_opts"].use_config is False

    def test_save_artifact_negative_flag(self) -> None:
        """Test that --no-save-artifact sets save_artifact to false."""
        _cmd, bound, _extra = app.meta.parse_args(["calls", "foo", "--no-save-artifact"])
        assert bound.kwargs["global_opts"].save_artifact is False

    def test_build_launch_context_config_enabled(self) -> None:
        """Test launch context config chain with config files enabled."""
        launch = _build_launch_context(
            argv=["calls", "foo"],
            config_opts=ConfigOptionArgs(),
            global_opts=GlobalOptionArgs(),
        )
        assert launch.output_format is not None
        assert app.config is not None
        assert len(app.config) == DEFAULT_CONFIG_PROVIDER_COUNT

    def test_build_launch_context_no_config_keeps_env(self) -> None:
        """Test launch context config chain with --no-config."""
        launch = _build_launch_context(
            argv=["calls", "foo"],
            config_opts=ConfigOptionArgs(use_config=False),
            global_opts=GlobalOptionArgs(),
        )
        assert launch.output_format is not None
        assert app.config is not None
        assert len(app.config) == 1
        assert app.config[0].__class__.__name__ == "Env"

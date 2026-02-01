"""Tests for CLI return type contract."""

from __future__ import annotations

from typing import get_args, get_origin, get_type_hints

import pytest

from cli.result import CliResult


@pytest.mark.parametrize(
    "command_path",
    [
        "cli.commands.build:build_command",
        "cli.commands.plan:plan_command",
        "cli.commands.diag:diag_command",
        "cli.commands.config:show_config",
        "cli.commands.config:validate_config",
        "cli.commands.config:init_config",
        "cli.commands.delta:vacuum_command",
        "cli.commands.delta:checkpoint_command",
        "cli.commands.delta:cleanup_log_command",
        "cli.commands.delta:export_command",
        "cli.commands.delta:restore_command",
        "cli.commands.version:version_command",
    ],
)
def test_command_return_type_contract(command_path: str) -> None:
    """Ensure commands return int or CliResult (or a Union thereof)."""
    module_path, func_name = command_path.rsplit(":", 1)
    module = __import__(module_path, fromlist=[func_name])
    func = getattr(module, func_name)

    return_type = get_type_hints(func).get("return")
    if return_type is None:
        pytest.fail(f"{command_path} has no return type annotation")

    valid_types = (int, CliResult)
    if return_type in valid_types:
        return

    origin = get_origin(return_type)
    if origin is None:
        pytest.fail(f"{command_path} return type {return_type!r} is not int or CliResult")

    args = get_args(return_type)
    if args and all(arg in valid_types for arg in args):
        return

    pytest.fail(f"{command_path} return type {return_type!r} is not int or CliResult")

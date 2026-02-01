"""Configuration management commands."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Annotated

from cyclopts import Parameter

from cli.config_loader import (
    load_effective_config,
    load_effective_config_with_sources,
    normalize_config_contents,
)
from cli.context import RunContext
from cli.groups import admin_group
from cli.validation import validate_config_mutual_exclusion

_TEMPLATE = """# codeanatomy.toml

[plan]
allow_partial = false
requested_tasks = []
impacted_tasks = []
enable_metric_scheduling = true
enable_plan_diagnostics = true

[cache]
policy_profile = "default"
path = ""
log_to_file = false

[graph_adapter]
kind = ""
options = {}

[incremental]
enabled = false
state_dir = "build/state"
repo_id = ""
impact_strategy = "hybrid"

[scip]
output_dir = "build/scip"
scip_python_bin = "scip-python"
scip_cli_bin = "scip"
extra_args = []
"""


def show_config(
    *,
    with_sources: Annotated[
        bool,
        Parameter(
            name="--with-sources",
            help="Show the source of each configuration value.",
        ),
    ] = False,
    run_context: Annotated[RunContext | None, Parameter(parse=False)] = None,
) -> int:
    """Show the effective configuration payload.

    Returns
    -------
    int
        Exit status code.
    """
    if with_sources:
        config_with_sources = load_effective_config_with_sources(None)
        payload = json.dumps(config_with_sources.to_display_dict(), indent=2, sort_keys=True)
        sys.stdout.write(payload + "\n")
        return 0

    if run_context is None:
        config_contents = load_effective_config(None)
        normalized = normalize_config_contents(config_contents)
    else:
        normalized = dict(run_context.config_contents)
    validate_config_mutual_exclusion(normalized)
    payload = json.dumps(normalized, indent=2, sort_keys=True)
    sys.stdout.write(payload + "\n")
    return 0


def validate_config(
    *,
    run_context: Annotated[RunContext | None, Parameter(parse=False)] = None,
) -> int:
    """Validate configuration payloads for mutual exclusion rules.

    Returns
    -------
    int
        Exit status code.
    """
    if run_context is None:
        config_contents = load_effective_config(None)
        normalized = normalize_config_contents(config_contents)
    else:
        normalized = dict(run_context.config_contents)
    validate_config_mutual_exclusion(normalized)
    return 0


def init_config(
    *,
    path: Annotated[
        Path | None,
        Parameter(
            name="--path",
            help="Path to write the configuration template (default: codeanatomy.toml).",
        ),
    ] = None,
    force: Annotated[
        bool,
        Parameter(
            name="--force",
            help="Overwrite existing config file.",
            group=admin_group,
        ),
    ] = False,
) -> int:
    """Write a configuration template to disk.

    Returns
    -------
    int
        Exit status code.

    Raises
    ------
    FileExistsError
        Raised when the config file exists and overwrite is not enabled.
    """
    target_path = path if path is not None else Path("codeanatomy.toml")
    if target_path.exists() and not force:
        msg = f"Config file already exists: {target_path}."
        raise FileExistsError(msg)
    target_path.write_text(_TEMPLATE, encoding="utf-8")
    return 0


__all__ = ["init_config", "show_config", "validate_config"]

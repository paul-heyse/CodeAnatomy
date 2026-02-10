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
enable_plan_task_submission_hook = true
enable_plan_task_grouping_hook = true
enforce_plan_task_submission = false

[cache]
policy_profile = "default"
path = "build/cache"
log_to_file = false
opt_in = true

[incremental]
enabled = false
state_dir = "build/state"
impact_strategy = "hybrid"
git_changed_only = false

# [engine]
# profile = "medium"
# rulepack_profile = "Default"
# compliance_capture = false
# rule_tracing = false
# plan_preview = false
# tracing_preset = "ProductionLean"
# instrument_object_store = false

# [delta.restore]
# version = 0
# timestamp = ""

# [delta.export]
# version = 0
# timestamp = ""

# [docstrings.policy]
# coverage-threshold = 0.85
# coverage-action = "warn"
# missing-params-action = "warn"
# missing-returns-action = "warn"

# [otel]
# enable_node_tracing = false
# enable_plan_tracing = false
# endpoint = "http://localhost:4317"
# protocol = "grpc"
# sampler = "parentbased_always_on"
# sampler_arg = 1
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

    Returns:
    -------
    int
        Exit status code.
    """
    if with_sources:
        config_with_sources = (
            run_context.config_sources
            if run_context and run_context.config_sources is not None
            else load_effective_config_with_sources(None)
        )
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

    Returns:
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

    Args:
        path: Optional output path for the template file.
        force: Whether to overwrite an existing file.

    Returns:
        int: Result.

    Raises:
        FileExistsError: If the target path exists and `force` is false.
    """
    target_path = path if path is not None else Path("codeanatomy.toml")
    if target_path.exists() and not force:
        msg = f"Config file already exists: {target_path}."
        raise FileExistsError(msg)
    target_path.write_text(_TEMPLATE, encoding="utf-8")
    return 0


__all__ = ["init_config", "show_config", "validate_config"]

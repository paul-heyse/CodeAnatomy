"""Shared help-panel groups for the CodeAnatomy CLI."""

from __future__ import annotations

from cyclopts import Group, Parameter, validators

from cli.validators import ConditionalDisabled

session_group = Group(
    "Session",
    help="Session and run context options.",
    sort_key=0,
)

output_group = Group(
    "Output",
    help="Configure output directory and artifact selection.",
    sort_key=1,
)

execution_group = Group(
    "Execution",
    help="Control pipeline execution strategy and parallelism.",
    sort_key=2,
)

incremental_group = Group(
    "Incremental Processing",
    help="Configure incremental/CDF-based processing.",
    sort_key=4,
)

repo_scope_group = Group(
    "Repository Scope",
    help="Control which files are included in analysis.",
    sort_key=5,
)

graph_adapter_group = Group(
    "Graph Adapter",
    help="Configure Hamilton graph adapter backend.",
    sort_key=6,
)

advanced_group = Group(
    "Advanced",
    help="Advanced configuration options (most set via config file).",
    sort_key=7,
)

observability_group = Group(
    "Observability",
    help="Configure OpenTelemetry tracing, metrics, and logging.",
    sort_key=8,
)

admin_group = Group(
    "Admin",
    help="Administrative commands and help.",
    sort_key=99,
)

restore_target_group = Group(
    "Restore Target",
    help="Specify exactly one of version or timestamp.",
    validator=validators.MutuallyExclusive(),
    default_parameter=Parameter(show_default=False),
)

scip_group = Group(
    "SCIP Configuration",
    help="Configure SCIP semantic index extraction.",
    sort_key=3,
    validator=ConditionalDisabled(
        condition_param="disable_scip",
        condition_value=True,
        disabled_params=(
            "scip_output_dir",
            "scip_index_path_override",
            "scip_env_json",
            "scip_python_bin",
            "scip_target_only",
            "scip_timeout_s",
            "node_max_old_space_mb",
            "scip_extra_args",
        ),
    ),
)

__all__ = [
    "admin_group",
    "advanced_group",
    "execution_group",
    "graph_adapter_group",
    "incremental_group",
    "observability_group",
    "output_group",
    "repo_scope_group",
    "restore_target_group",
    "scip_group",
    "session_group",
]

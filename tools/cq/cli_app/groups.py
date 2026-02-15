"""Shared command group definitions for CQ CLI."""

from __future__ import annotations

from cyclopts import Group

global_group = Group(
    "Global Options",
    help="Options applied to every CQ command.",
    sort_key=0,
    help_formatter="default",
)
analysis_group = Group(
    "Analysis",
    help="",
    sort_key=1,
    help_formatter="default",
)
admin_group = Group(
    "Administration",
    help="",
    sort_key=2,
    help_formatter="default",
)
protocol_group = Group(
    "Protocols",
    help="",
    sort_key=3,
    help_formatter="default",
)
setup_group = Group(
    "Setup",
    help="Shell and developer setup commands.",
    sort_key=4,
    help_formatter="plain",
)

__all__ = [
    "admin_group",
    "analysis_group",
    "global_group",
    "protocol_group",
    "setup_group",
]

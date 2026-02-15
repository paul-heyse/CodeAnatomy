"""Shared command group definitions for CQ CLI."""

from __future__ import annotations

from cyclopts import Group

global_group = Group("Global Options", help="Options applied to every CQ command.", sort_key=0)
analysis_group = Group("Analysis", help="Static and structural analysis commands.", sort_key=1)
admin_group = Group("Administration", help="Maintenance and schema export commands.", sort_key=2)
protocol_group = Group("Protocols", help="Protocol/data access subcommands.", sort_key=3)
setup_group = Group("Setup", help="Shell and developer setup commands.", sort_key=4)

__all__ = [
    "admin_group",
    "analysis_group",
    "global_group",
    "protocol_group",
    "setup_group",
]

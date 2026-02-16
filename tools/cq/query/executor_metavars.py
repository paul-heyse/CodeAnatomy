"""Metavariable handling (re-export).

Metavariable functionality is already in metavar.py module.
This module provides a convenience re-export.
"""

from __future__ import annotations

from tools.cq.query.metavar import (
    apply_metavar_filters,
    extract_rule_metavars,
    extract_rule_variadic_metavars,
    partition_metavar_filters,
)

__all__ = [
    "apply_metavar_filters",
    "extract_rule_metavars",
    "extract_rule_variadic_metavars",
    "partition_metavar_filters",
]

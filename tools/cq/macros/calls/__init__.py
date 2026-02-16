"""Calls macro package - call site census with argument shape analysis.

Public API for finding and analyzing call sites, with AST-based pattern
matching, argument analysis, and semantic enrichment.
"""

from __future__ import annotations

from tools.cq.macros.calls.analysis import CallSite, collect_call_sites
from tools.cq.macros.calls.context_snippet import extract_calls_context_snippet
from tools.cq.macros.calls.entry import cmd_calls
from tools.cq.macros.calls.neighborhood import compute_calls_context_window
from tools.cq.macros.calls.scanning import group_candidates, rg_find_candidates

__all__ = [
    "CallSite",
    "cmd_calls",
    "collect_call_sites",
    "compute_calls_context_window",
    "extract_calls_context_snippet",
    "group_candidates",
    "rg_find_candidates",
]

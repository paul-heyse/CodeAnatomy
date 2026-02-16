"""Calls macro package - call site census with argument shape analysis.

Public API for finding and analyzing call sites, with AST-based pattern
matching, argument analysis, and semantic enrichment.
"""

from __future__ import annotations

from tools.cq.macros.calls.analysis import CallSite, collect_call_sites

# Private exports for internal use (tests)
from tools.cq.macros.calls.context_snippet import (
    _extract_context_snippet,
    extract_calls_context_snippet,
)
from tools.cq.macros.calls.entry import cmd_calls
from tools.cq.macros.calls.insight import _find_function_signature
from tools.cq.macros.calls.neighborhood import compute_calls_context_window
from tools.cq.macros.calls.scanning import _rg_find_candidates, group_candidates, rg_find_candidates
from tools.cq.macros.calls.semantic import _calls_payload_reason

__all__ = [
    "CallSite",
    "_calls_payload_reason",
    "_extract_context_snippet",
    "_find_function_signature",
    "_rg_find_candidates",
    "cmd_calls",
    "collect_call_sites",
    "compute_calls_context_window",
    "extract_calls_context_snippet",
    "group_candidates",
    "rg_find_candidates",
]

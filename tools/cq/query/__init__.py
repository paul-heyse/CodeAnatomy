"""Query module for cq tool.

Provides a declarative query DSL for searching Python codebases using ast-grep.
"""

from __future__ import annotations

from tools.cq.astgrep.metavar import (
    apply_metavar_filters,
    parse_metavariables,
    validate_pattern_metavars,
)
from tools.cq.query.ir import (
    CompositeRule,
    Expander,
    MetaVarCapture,
    MetaVarFilter,
    NthChildSpec,
    PatternSpec,
    Query,
    RelationalConstraint,
    Scope,
    ScopeFilter,
)
from tools.cq.query.parser import parse_query

__all__ = [
    "CompositeRule",
    "Expander",
    "MetaVarCapture",
    "MetaVarFilter",
    "NthChildSpec",
    "PatternSpec",
    "Query",
    "RelationalConstraint",
    "Scope",
    "ScopeFilter",
    "apply_metavar_filters",
    "parse_metavariables",
    "parse_query",
    "validate_pattern_metavars",
]

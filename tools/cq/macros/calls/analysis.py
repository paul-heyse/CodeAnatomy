"""AST-based call analysis and site collection.

Handles call expression parsing, argument analysis, hazard detection,
and call site construction from both Python AST and Rust ast-grep records.
"""

from __future__ import annotations

import ast
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import msgspec

from tools.cq.core.python_ast_utils import get_call_name
from tools.cq.macros.calls.analysis_primitives import (
    CallAnalysis,
)
from tools.cq.macros.calls.analysis_primitives import (
    analyze_call as _analyze_call,
)
from tools.cq.macros.calls.analysis_primitives import (
    build_call_index as _build_call_index,
)
from tools.cq.macros.calls.analysis_primitives import (
    detect_hazards as _detect_hazards,
)
from tools.cq.macros.calls.analysis_primitives import (
    get_containing_function as _get_containing_function,
)
from tools.cq.macros.calls.analysis_primitives import (
    matches_target_expr as _matches_target_expr,
)
from tools.cq.macros.calls.analysis_primitives import (
    parse_call_expr as _parse_call_expr,
)
from tools.cq.macros.calls.analysis_primitives import (
    stable_callsite_id as _stable_callsite_id,
)
from tools.cq.query.sg_parser import SgRecord
from tools.cq.search.pipeline.context_window import ContextWindow

CallBinding = Literal["ok", "ambiguous", "would_break", "unresolved"]


class CallSite(msgspec.Struct, frozen=True):
    """Information about a call site.

    Parameters
    ----------
    file : str
        File path.
    line : int
        Line number.
    col : int
        Column offset.
    num_args : int
        Number of positional arguments.
    num_kwargs : int
        Number of keyword arguments.
    kwargs : list[str]
        Keyword argument names.
    has_star_args : bool
        Whether *args is used.
    has_star_kwargs : bool
        Whether **kwargs is used.
    context : str
        Calling context (function/method name).
    arg_preview : str
        Preview of arguments.
    callee : str
        Resolved callee name from the call expression.
    receiver : str | None
        Receiver name for attribute calls, if available.
    resolution_confidence : str
        Call resolution confidence (exact/likely/ambiguous/unresolved).
    resolution_path : str
        Resolution strategy label.
    binding : CallBinding
        Binding classification (ok/ambiguous/would_break/unresolved).
    target_names : list[str]
        Resolved target names, if any.
    call_id : str
        Stable callsite identifier.
    hazards : list[str]
        Detected dynamic hazards for this callsite.
    symtable_info : dict[str, object] | None
        Symtable analysis of containing function, if available.
    bytecode_info : dict[str, object] | None
        Bytecode analysis of containing function, if available.
    context_window : ContextWindow | None
        Line range for containing definition context.
    context_snippet : str | None
        Source code snippet for the containing context.
    """

    file: str
    line: int
    col: int
    num_args: int
    num_kwargs: int
    kwargs: list[str]
    has_star_args: bool
    has_star_kwargs: bool
    context: str
    arg_preview: str
    callee: str
    receiver: str | None
    resolution_confidence: str
    resolution_path: str
    binding: CallBinding
    target_names: list[str]
    call_id: str
    hazards: list[str]
    symtable_info: dict[str, object] | None = None
    bytecode_info: dict[str, object] | None = None
    context_window: ContextWindow | None = None
    context_snippet: str | None = None


@dataclass(frozen=True)
class CallSiteBuildContext:
    """Cached context for call-site construction from records."""

    root: Path
    rel_path: str
    source: str
    source_lines: list[str]
    def_lines: list[tuple[int, int]]
    total_lines: int
    tree: ast.AST
    call_index: dict[tuple[int, int], ast.Call]
    function_name: str


def _enrich_call_site(
    source: str,
    containing_fn: str,
    rel_path: str,
) -> dict[str, dict[str, object] | None]:
    """Enrich call site with symtable and bytecode signals.

    Parameters
    ----------
    source
        File source code.
    containing_fn
        Name of the containing function.
    rel_path
        Relative file path.

    Returns:
    -------
    dict[str, dict[str, object] | None]
        Enrichment data with 'symtable' and 'bytecode' keys.
    """
    from tools.cq.query.enrichment import analyze_bytecode, analyze_symtable

    enrichment: dict[str, dict[str, object] | None] = {
        "symtable": None,
        "bytecode": None,
    }

    # Symtable analysis
    symtable_info = analyze_symtable(source, rel_path)
    if containing_fn in symtable_info:
        st_info = symtable_info[containing_fn]
        enrichment["symtable"] = {
            "is_closure": len(st_info.free_vars) > 0,
            "free_vars": list(st_info.free_vars),
            "globals_used": list(st_info.globals_used),
            "nested_scopes": st_info.nested_scopes,
        }

    # Bytecode analysis (best-effort, requires compile)
    try:
        code = compile(source, rel_path, "exec")
        for const in code.co_consts:
            if hasattr(const, "co_name") and const.co_name == containing_fn:
                bc_info = analyze_bytecode(const)
                enrichment["bytecode"] = {
                    "load_globals": list(bc_info.load_globals),
                    "load_attrs": list(bc_info.load_attrs),
                    "call_functions": list(bc_info.call_functions),
                }
                break
    except (SyntaxError, ValueError, TypeError):
        pass  # Best-effort

    return enrichment


class CallFinder(ast.NodeVisitor):
    """Find all calls to a specific function."""

    def __init__(self, file: str, target_name: str, tree: ast.AST) -> None:
        """__init__."""
        self.file = file
        self.target_name = target_name
        self.tree = tree
        self.sites: list[CallSite] = []

    def visit_Call(self, node: ast.Call) -> None:
        """Record matching call sites."""
        if _matches_target_expr(node.func, self.target_name):
            info = _analyze_call(node)
            context = _get_containing_function(self.tree, node.lineno)
            callee, _is_method, receiver = get_call_name(node.func)
            hazards = _detect_hazards(node, info)
            self.sites.append(
                CallSite(
                    file=self.file,
                    line=node.lineno,
                    col=node.col_offset,
                    num_args=info.num_args,
                    num_kwargs=info.num_kwargs,
                    kwargs=info.kwargs,
                    has_star_args=info.has_star_args,
                    has_star_kwargs=info.has_star_kwargs,
                    context=context,
                    arg_preview=info.arg_preview,
                    callee=callee,
                    receiver=receiver,
                    resolution_confidence="unresolved",
                    resolution_path="",
                    binding="unresolved",
                    target_names=[],
                    call_id=_stable_callsite_id(
                        file=self.file,
                        line=node.lineno,
                        col=node.col_offset,
                        callee=callee,
                        context=context,
                    ),
                    hazards=hazards,
                )
            )
        self.generic_visit(node)


def collect_call_sites(
    root: Path,
    by_file: dict[Path, list[int]],
    function_name: str,
) -> list[CallSite]:
    """Collect callsite records for the requested function.

    Returns:
        list[CallSite]: Collected call-site rows.
    """
    from tools.cq.macros.calls.analysis_scan import collect_call_sites as collect_call_sites_impl

    return collect_call_sites_impl(root, by_file, function_name)


def _collect_call_sites_from_records(
    root: Path,
    records: list[SgRecord],
    function_name: str,
) -> tuple[list[CallSite], int]:
    from tools.cq.macros.calls.analysis_scan import (
        collect_call_sites_from_records as collect_call_sites_from_records_impl,
    )

    return collect_call_sites_from_records_impl(root, records, function_name)


def _analyze_sites(
    all_sites: list[CallSite],
) -> tuple[
    Counter[str],
    Counter[str],
    int,
    Counter[str],
    Counter[str],
]:
    from tools.cq.macros.calls.analysis_scan import analyze_sites as analyze_sites_impl

    return analyze_sites_impl(all_sites)


def build_call_index(tree: ast.AST) -> dict[tuple[int, int], ast.Call]:
    """Build a position index for call nodes.

    Returns:
        dict[tuple[int, int], ast.Call]: Call nodes keyed by line/column.
    """
    return _build_call_index(tree)


def parse_call_expr(text: str) -> ast.Call | None:
    """Parse one expression string into a call node when possible.

    Returns:
        ast.Call | None: Parsed call node when expression is a call.
    """
    return _parse_call_expr(text)


def matches_target_expr(func: ast.expr, target_name: str) -> bool:
    """Return whether a call expression matches target name."""
    return _matches_target_expr(func, target_name)


def analyze_call(node: ast.Call) -> CallAnalysis:
    """Analyze one call node for argument structure.

    Returns:
        CallAnalysis: Structured call argument metrics and preview.
    """
    return _analyze_call(node)


def get_containing_function(tree: ast.AST, lineno: int) -> str:
    """Find the containing function name for a line number.

    Returns:
        str: Containing function name or ``<module>`` fallback.
    """
    return _get_containing_function(tree, lineno)


def detect_hazards(call: ast.Call, info: CallAnalysis) -> list[str]:
    """Detect hazard flags for one call node.

    Returns:
        list[str]: Hazard labels derived from call shape/target.
    """
    return _detect_hazards(call, info)


def stable_callsite_id(
    *,
    file: str,
    line: int,
    col: int,
    callee: str,
    context: str,
) -> str:
    """Build a stable deterministic call-site identifier.

    Returns:
        str: Stable call-site identifier hash.
    """
    return _stable_callsite_id(
        file=file,
        line=line,
        col=col,
        callee=callee,
        context=context,
    )


__all__ = [
    "CallSite",
    "analyze_call",
    "build_call_index",
    "collect_call_sites",
    "detect_hazards",
    "get_containing_function",
    "matches_target_expr",
    "parse_call_expr",
    "stable_callsite_id",
]

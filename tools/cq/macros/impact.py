"""Impact macro - approximate taint/dependency flow analysis.

Traces how data flows from a specific parameter through the codebase,
identifying downstream consumers and potential impacts of changes.
"""

from __future__ import annotations

import ast
from contextlib import suppress
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

from tools.cq.core.schema import (
    Anchor,
    CqResult,
    Finding,
    Section,
    mk_result,
    mk_runmeta,
    ms,
)
from tools.cq.core.scoring import (
    ConfidenceSignals,
    ImpactSignals,
    bucket,
    confidence_score,
    impact_score,
)
from tools.cq.index.arg_binder import bind_call_to_params, tainted_params_from_bound_call
from tools.cq.index.call_resolver import CallInfo, resolve_call_targets
from tools.cq.index.def_index import DefIndex, FnDecl
from tools.cq.search import INTERACTIVE, SearchLimits, find_callers

if TYPE_CHECKING:
    from tools.cq.core.toolchain import Toolchain

_DEFAULT_MAX_DEPTH = 5
_SECTION_SITE_LIMIT = 50
_CALLER_LIMIT = 30


@dataclass
class TaintedSite:
    """A location where tainted data flows.

    Parameters
    ----------
    file : str
        File path.
    line : int
        Line number.
    kind : str
        Site type: "source", "call", "return", "assign".
    description : str
        Human-readable description.
    param : str | None
        Tainted parameter at this site.
    depth : int
        Taint propagation depth from source.
    """

    file: str
    line: int
    kind: str
    description: str
    param: str | None = None
    depth: int = 0


@dataclass
class TaintState:
    """Taint analysis state.

    Parameters
    ----------
    tainted_vars : set[str]
        Currently tainted variable names.
    tainted_sites : list[TaintedSite]
        Recorded taint sites.
    visited : set[str]
        Visited function keys to prevent cycles.
    """

    tainted_vars: set[str] = field(default_factory=set)
    tainted_sites: list[TaintedSite] = field(default_factory=list)
    visited: set[str] = field(default_factory=set)


@dataclass(frozen=True)
class ImpactRequest:
    """Inputs required for the impact macro."""

    tc: Toolchain
    root: Path
    argv: list[str]
    function_name: str
    param_name: str
    max_depth: int = _DEFAULT_MAX_DEPTH


class TaintVisitor(ast.NodeVisitor):
    """AST visitor that tracks taint flow within a function."""

    def __init__(
        self,
        file: str,
        tainted_params: set[str],
        depth: int,
    ) -> None:
        self.file = file
        self.tainted: set[str] = set(tainted_params)
        self.sites: list[TaintedSite] = []
        self.depth = depth
        self.calls: list[tuple[CallInfo, set[int | str]]] = []  # (call, tainted_args)

    def visit_Assign(self, node: ast.Assign) -> None:
        """Record taint propagation on assignment."""
        # Check if RHS uses tainted values
        rhs_tainted = self.expr_tainted(node.value)

        if rhs_tainted:
            # Propagate taint to LHS targets
            for target in node.targets:
                if isinstance(target, ast.Name):
                    self.tainted.add(target.id)
                    self.sites.append(
                        TaintedSite(
                            file=self.file,
                            line=node.lineno,
                            kind="assign",
                            description=f"Taint propagates to {target.id}",
                            param=target.id,
                            depth=self.depth,
                        )
                    )
                elif isinstance(target, ast.Tuple):
                    for elt in target.elts:
                        if isinstance(elt, ast.Name):
                            self.tainted.add(elt.id)

        self.generic_visit(node)

    def visit_Call(self, node: ast.Call) -> None:
        """Record taint propagation through call sites."""
        # Check which arguments are tainted
        tainted_arg_indices: set[int] = set()
        tainted_arg_values: set[str] = set()

        for i, arg in enumerate(node.args):
            if self.expr_tainted(arg):
                tainted_arg_indices.add(i)
                with suppress(ValueError, TypeError):
                    tainted_arg_values.add(ast.unparse(arg))

        for kw in node.keywords:
            if kw.arg and self.expr_tainted(kw.value):
                tainted_arg_values.add(kw.arg)

        if tainted_arg_indices or tainted_arg_values:
            # Record the call for inter-procedural analysis
            callee_name = self._get_call_name(node.func)
            call_info = CallInfo(
                file=self.file,
                line=node.lineno,
                col=node.col_offset,
                callee_name=callee_name,
                args=node.args,
                keywords=node.keywords,
                is_method_call=isinstance(node.func, ast.Attribute),
                receiver_name=self._get_receiver(node.func),
            )
            self.calls.append((call_info, tainted_arg_indices | tainted_arg_values))

            self.sites.append(
                TaintedSite(
                    file=self.file,
                    line=node.lineno,
                    kind="call",
                    description=f"Tainted args passed to {callee_name}",
                    depth=self.depth,
                )
            )

        self.generic_visit(node)

    def visit_Return(self, node: ast.Return) -> None:
        """Record tainted return values."""
        if node.value and self.expr_tainted(node.value):
            self.sites.append(
                TaintedSite(
                    file=self.file,
                    line=node.lineno,
                    kind="return",
                    description="Tainted value returned",
                    depth=self.depth,
                )
            )
        self.generic_visit(node)

    def expr_tainted(self, expr: ast.expr) -> bool:
        """Check if expression uses tainted values.

        Returns
        -------
        bool
            True when expression references tainted values.
        """
        if isinstance(expr, ast.Name):
            return expr.id in self.tainted
        handler = _EXPR_TAINT_HANDLERS.get(type(expr))
        if handler is not None:
            return handler(self, expr)
        if isinstance(expr, (ast.List, ast.Tuple, ast.Set)):
            return any(self.expr_tainted(e) for e in expr.elts)
        if isinstance(expr, ast.Dict):
            keys_tainted = any(self.expr_tainted(k) for k in expr.keys if k is not None)
            return keys_tainted or any(self.expr_tainted(v) for v in expr.values)
        return False

    @staticmethod
    def _safe_unparse(expr: ast.AST, *, default: str) -> str:
        with suppress(ValueError, TypeError):
            return ast.unparse(expr)
        return default

    @classmethod
    def _get_call_name(cls, func: ast.expr) -> str:
        if isinstance(func, ast.Name):
            return func.id
        if isinstance(func, ast.Attribute):
            return cls._safe_unparse(func, default=func.attr)
        return cls._safe_unparse(func, default="<unknown>")

    @staticmethod
    def _get_receiver(func: ast.expr) -> str | None:
        if isinstance(func, ast.Attribute) and isinstance(func.value, ast.Name):
            return func.value.id
        return None


def _tainted_attribute(visitor: TaintVisitor, expr: ast.Attribute) -> bool:
    return visitor.expr_tainted(expr.value)


def _tainted_subscript(visitor: TaintVisitor, expr: ast.Subscript) -> bool:
    return visitor.expr_tainted(expr.value) or visitor.expr_tainted(expr.slice)


def _tainted_binop(visitor: TaintVisitor, expr: ast.BinOp) -> bool:
    return visitor.expr_tainted(expr.left) or visitor.expr_tainted(expr.right)


def _tainted_unary(visitor: TaintVisitor, expr: ast.UnaryOp) -> bool:
    return visitor.expr_tainted(expr.operand)


def _tainted_call(visitor: TaintVisitor, expr: ast.Call) -> bool:
    # Check if any positional args are tainted
    if any(visitor.expr_tainted(arg) for arg in expr.args):
        return True
    # Check if any keyword args are tainted
    if any(visitor.expr_tainted(kw.value) for kw in expr.keywords):
        return True
    # Check if method receiver is tainted (e.g., `tainted_obj.method()`)
    if isinstance(expr.func, ast.Attribute):
        if visitor.expr_tainted(expr.func.value):
            return True
    return False


def _tainted_ifexp(visitor: TaintVisitor, expr: ast.IfExp) -> bool:
    return (
        visitor.expr_tainted(expr.test)
        or visitor.expr_tainted(expr.body)
        or visitor.expr_tainted(expr.orelse)
    )


def _tainted_compare(visitor: TaintVisitor, expr: ast.Compare) -> bool:
    return visitor.expr_tainted(expr.left) or any(
        visitor.expr_tainted(cmp) for cmp in expr.comparators
    )


def _tainted_formatted(visitor: TaintVisitor, expr: ast.FormattedValue) -> bool:
    return visitor.expr_tainted(expr.value)


def _tainted_joined(visitor: TaintVisitor, expr: ast.JoinedStr) -> bool:
    return any(
        visitor.expr_tainted(val) for val in expr.values if isinstance(val, ast.FormattedValue)
    )


def _tainted_boolop(visitor: TaintVisitor, expr: ast.BoolOp) -> bool:
    """Handle `or` and `and` expressions (e.g., `sources or {}`)."""
    return any(visitor.expr_tainted(val) for val in expr.values)


def _tainted_namedexpr(visitor: TaintVisitor, expr: ast.NamedExpr) -> bool:
    """Handle walrus operator (e.g., `(x := tainted_val)`)."""
    return visitor.expr_tainted(expr.value)


def _tainted_starred(visitor: TaintVisitor, expr: ast.Starred) -> bool:
    """Handle starred expressions (e.g., `*tainted_list`)."""
    return visitor.expr_tainted(expr.value)


def _tainted_lambda(_visitor: TaintVisitor, _expr: ast.Lambda) -> bool:
    """Lambda captures are not tracked; assume not tainted for simplicity."""
    return False


def _tainted_generator(visitor: TaintVisitor, expr: ast.GeneratorExp) -> bool:
    """Handle generator expressions - tainted if iterating over tainted data."""
    for gen in expr.generators:
        if visitor.expr_tainted(gen.iter):
            return True
    return visitor.expr_tainted(expr.elt)


def _tainted_listcomp(visitor: TaintVisitor, expr: ast.ListComp) -> bool:
    """Handle list comprehensions - tainted if iterating over tainted data."""
    for gen in expr.generators:
        if visitor.expr_tainted(gen.iter):
            return True
    return visitor.expr_tainted(expr.elt)


def _tainted_setcomp(visitor: TaintVisitor, expr: ast.SetComp) -> bool:
    """Handle set comprehensions - tainted if iterating over tainted data."""
    for gen in expr.generators:
        if visitor.expr_tainted(gen.iter):
            return True
    return visitor.expr_tainted(expr.elt)


def _tainted_dictcomp(visitor: TaintVisitor, expr: ast.DictComp) -> bool:
    """Handle dict comprehensions - tainted if iterating over tainted data."""
    for gen in expr.generators:
        if visitor.expr_tainted(gen.iter):
            return True
    return visitor.expr_tainted(expr.key) or visitor.expr_tainted(expr.value)


_EXPR_TAINT_HANDLERS: dict[type[ast.AST], callable[[TaintVisitor, ast.AST], bool]] = {
    ast.Attribute: _tainted_attribute,
    ast.Subscript: _tainted_subscript,
    ast.BinOp: _tainted_binop,
    ast.UnaryOp: _tainted_unary,
    ast.Call: _tainted_call,
    ast.IfExp: _tainted_ifexp,
    ast.Compare: _tainted_compare,
    ast.FormattedValue: _tainted_formatted,
    ast.JoinedStr: _tainted_joined,
    ast.BoolOp: _tainted_boolop,
    ast.NamedExpr: _tainted_namedexpr,
    ast.Starred: _tainted_starred,
    ast.Lambda: _tainted_lambda,
    ast.GeneratorExp: _tainted_generator,
    ast.ListComp: _tainted_listcomp,
    ast.SetComp: _tainted_setcomp,
    ast.DictComp: _tainted_dictcomp,
}


def _find_top_level_function(
    tree: ast.AST,
    fn: FnDecl,
) -> ast.FunctionDef | ast.AsyncFunctionDef | None:
    for node in ast.walk(tree):
        if (
            isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
            and node.name == fn.name
            and node.lineno == fn.line
        ):
            return node
    return None


def _find_class_method(
    tree: ast.AST,
    fn: FnDecl,
) -> ast.FunctionDef | ast.AsyncFunctionDef | None:
    if fn.class_name is None:
        return None
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef) and node.name == fn.class_name:
            for child in node.body:
                if (
                    isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef))
                    and child.name == fn.name
                    and child.lineno == fn.line
                ):
                    return child
    return None


def _find_function_node(source: str, fn: FnDecl) -> ast.FunctionDef | ast.AsyncFunctionDef | None:
    """Find the AST node for a function declaration.

    Returns
    -------
    ast.FunctionDef | ast.AsyncFunctionDef | None
        Matched AST node if found.
    """
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return None
    return _find_class_method(tree, fn) or _find_top_level_function(tree, fn)


@dataclass(frozen=True)
class _AnalyzeContext:
    index: DefIndex
    root: Path
    state: TaintState
    max_depth: int


def _analyze_function(
    fn: FnDecl,
    tainted_params: set[str],
    context: _AnalyzeContext,
    current_depth: int = 0,
) -> None:
    """Analyze taint flow within a function and propagate to callees."""
    if current_depth >= context.max_depth:
        return

    key = fn.key
    if key in context.state.visited:
        return
    context.state.visited.add(key)

    # Read source
    filepath = context.root / fn.file
    if not filepath.exists():
        return

    try:
        source = filepath.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError):
        return

    # Find function AST
    fn_node = _find_function_node(source, fn)
    if fn_node is None:
        return

    # Run taint visitor
    visitor = TaintVisitor(fn.file, tainted_params, current_depth)
    visitor.visit(fn_node)
    context.state.tainted_sites.extend(visitor.sites)

    # Propagate to callees
    for call_info, tainted_args in visitor.calls:
        resolved = resolve_call_targets(context.index, call_info)
        if resolved.targets:
            for target in resolved.targets:
                # Bind args to params
                bound = bind_call_to_params(
                    call_info.args,
                    call_info.keywords,
                    target,
                )
                new_tainted = tainted_params_from_bound_call(bound, tainted_args)
                if new_tainted:
                    _analyze_function(
                        target,
                        new_tainted,
                        context,
                        current_depth + 1,
                    )


def _find_callers_via_search(
    function_name: str,
    root: Path,
    limits: SearchLimits | None = None,
) -> list[tuple[str, int]]:
    """Use search adapter to find potential callers of a function.

    Parameters
    ----------
    function_name : str
        Function name to search for.
    root : Path
        Root directory to search from.
    limits : SearchLimits | None
        Search limits (defaults to INTERACTIVE profile).

    Returns
    -------
    list[tuple[str, int]]
        Candidate caller (file, line) pairs with relative paths.
    """
    limits = limits or INTERACTIVE
    callers: list[tuple[str, int]] = []

    # Use the adapter's find_callers function
    results = find_callers(root, function_name, limits=limits)

    # Convert absolute paths to relative paths
    for abs_path, lineno in results:
        with suppress(ValueError, TypeError):
            rel = abs_path.relative_to(root)
            callers.append((str(rel), lineno))

    return callers


def _collect_depth_stats(all_sites: list[TaintedSite]) -> tuple[dict[int, int], set[str]]:
    depth_counts: dict[int, int] = {}
    files_affected: set[str] = set()
    for site in all_sites:
        depth_counts[site.depth] = depth_counts.get(site.depth, 0) + 1
        files_affected.add(site.file)
    return depth_counts, files_affected


def _append_depth_findings(
    result: CqResult,
    *,
    request: ImpactRequest,
    depth_counts: dict[int, int],
    files_affected: set[str],
    site_count: int,
    scoring_details: dict[str, object],
) -> None:
    if not site_count:
        return
    result.key_findings.append(
        Finding(
            category="summary",
            message=(
                f"Taint from {request.function_name}.{request.param_name} "
                f"reaches {site_count} sites in {len(files_affected)} files"
            ),
            severity="info",
            details=dict(scoring_details),
        )
    )
    for depth, count in sorted(depth_counts.items()):
        result.key_findings.append(
            Finding(
                category="depth",
                message=f"Depth {depth}: {count} taint sites",
                severity="info",
                details=dict(scoring_details),
            )
        )


def _group_sites_by_kind(all_sites: list[TaintedSite]) -> dict[str, list[TaintedSite]]:
    by_kind: dict[str, list[TaintedSite]] = {}
    for site in all_sites:
        by_kind.setdefault(site.kind, []).append(site)
    return by_kind


def _append_kind_sections(
    result: CqResult,
    by_kind: dict[str, list[TaintedSite]],
    scoring_details: dict[str, object],
) -> None:
    for kind, sites in by_kind.items():
        section = Section(title=f"Taint {kind.title()} Sites")
        for site in sites[:_SECTION_SITE_LIMIT]:
            details = {"depth": site.depth, "param": site.param, **scoring_details}
            section.findings.append(
                Finding(
                    category=kind,
                    message=site.description,
                    anchor=Anchor(file=site.file, line=site.line),
                    severity="info",
                    details=details,
                )
            )
        result.sections.append(section)


def _append_callers_section(
    result: CqResult,
    caller_sites: list[tuple[str, int]],
    scoring_details: dict[str, object],
) -> None:
    if not caller_sites:
        return
    caller_section = Section(title="Callers (via rg)")
    for file, line in caller_sites[:_CALLER_LIMIT]:
        caller_section.findings.append(
            Finding(
                category="caller",
                message="Potential call site",
                anchor=Anchor(file=file, line=line),
                severity="info",
                details=dict(scoring_details),
            )
        )
    result.sections.append(caller_section)


def _append_evidence(
    result: CqResult,
    all_sites: list[TaintedSite],
    scoring_details: dict[str, object],
) -> None:
    seen: set[tuple[str, int]] = set()
    for site in all_sites:
        key = (site.file, site.line)
        if key in seen:
            continue
        seen.add(key)
        details = {"depth": site.depth, **scoring_details}
        result.evidence.append(
            Finding(
                category=site.kind,
                message=site.description,
                anchor=Anchor(file=site.file, line=site.line),
                details=details,
            )
        )


def cmd_impact(request: ImpactRequest) -> CqResult:
    """Analyze impact/taint flow from a function parameter.

    Parameters
    ----------
    request : ImpactRequest
        Impact analysis request payload.

    Returns
    -------
    CqResult
        Analysis result.
    """
    started = ms()

    # Build definition index
    index = DefIndex.build(request.root)

    # Find the source function
    functions = index.find_function_by_name(request.function_name)
    if not functions:
        # Try qualified name
        functions = index.find_function_by_qualified_name(request.function_name)

    run = mk_runmeta("impact", request.argv, str(request.root), started, request.tc.to_dict())
    result = mk_result(run)

    if not functions:
        result.summary = {
            "status": "not_found",
            "function": request.function_name,
        }
        result.key_findings.append(
            Finding(
                category="error",
                message=f"Function '{request.function_name}' not found in index",
                severity="error",
            )
        )
        return result

    # Analyze each matching function
    all_sites: list[TaintedSite] = []

    for fn in functions:
        # Verify param exists
        param_names = [p.name for p in fn.params]
        if request.param_name not in param_names:
            result.key_findings.append(
                Finding(
                    category="warning",
                    message=f"Parameter '{request.param_name}' not found in {fn.qualified_name}",
                    anchor=Anchor(file=fn.file, line=fn.line),
                    severity="warning",
                )
            )
            continue

        state = TaintState()
        analyze_context = _AnalyzeContext(
            index=index,
            root=request.root,
            state=state,
            max_depth=request.max_depth,
        )
        _analyze_function(
            fn,
            {request.param_name},
            analyze_context,
        )
        all_sites.extend(state.tainted_sites)

    # Also find callers via search adapter for broader impact
    caller_sites = _find_callers_via_search(request.function_name, request.root)

    # Build result
    result.summary = {
        "function": request.function_name,
        "parameter": request.param_name,
        "taint_sites": len(all_sites),
        "max_depth": request.max_depth,
        "functions_analyzed": len(functions),
        "callers_found": len(caller_sites),
    }

    depth_counts, files_affected = _collect_depth_stats(all_sites)

    # Compute scoring signals
    max_depth = max(depth_counts.keys()) if depth_counts else 0
    imp_signals = ImpactSignals(
        sites=len(all_sites),
        files=len(files_affected),
        depth=max_depth,
        breakages=0,
        ambiguities=0,
    )
    # Lower confidence for cross-file taint (depth > 0)
    evidence_kind = "resolved_ast" if max_depth == 0 else "cross_file_taint"
    conf_signals = ConfidenceSignals(evidence_kind=evidence_kind)
    imp = impact_score(imp_signals)
    conf = confidence_score(conf_signals)
    scoring_details = {
        "impact_score": imp,
        "impact_bucket": bucket(imp),
        "confidence_score": conf,
        "confidence_bucket": bucket(conf),
        "evidence_kind": conf_signals.evidence_kind,
    }

    _append_depth_findings(
        result,
        request=request,
        depth_counts=depth_counts,
        files_affected=files_affected,
        site_count=len(all_sites),
        scoring_details=scoring_details,
    )
    by_kind = _group_sites_by_kind(all_sites)
    _append_kind_sections(result, by_kind, scoring_details)
    _append_callers_section(result, caller_sites, scoring_details)
    _append_evidence(result, all_sites, scoring_details)

    return result

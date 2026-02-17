"""Pure taint-flow analysis helpers for CQ macros."""

from __future__ import annotations

import ast
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass
from typing import Literal, cast

import msgspec

from tools.cq.core.python_ast_utils import get_call_name

TaintSiteKind = Literal["source", "call", "return", "assign"]


class TaintedSite(msgspec.Struct, frozen=True):
    """A location where tainted data flows."""

    file: str
    line: int
    kind: TaintSiteKind
    description: str
    param: str | None = None
    depth: int = 0


@dataclass(frozen=True, slots=True)
class TaintCallSite:
    """Inter-procedural callsite discovered by local taint analysis."""

    file: str
    line: int
    col: int
    callee_name: str
    args: list[ast.expr]
    keywords: list[ast.keyword]
    is_method_call: bool
    receiver_name: str | None


@dataclass(frozen=True, slots=True)
class TaintAnalysisResult:
    """Pure taint-analysis output for one function body."""

    sites: list[TaintedSite]
    calls: list[tuple[TaintCallSite, set[int | str]]]
    tainted_vars: set[str]


class TaintVisitor(ast.NodeVisitor):
    """AST visitor that tracks taint flow within one function."""

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
        self.calls: list[tuple[TaintCallSite, set[int | str]]] = []

    def visit_Assign(self, node: ast.Assign) -> None:  # noqa: N802
        rhs_tainted = self.expr_tainted(node.value)

        if rhs_tainted:
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

    def visit_Call(self, node: ast.Call) -> None:  # noqa: N802
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
            callee_name, is_method, receiver = get_call_name(node.func)
            self.calls.append(
                (
                    TaintCallSite(
                        file=self.file,
                        line=node.lineno,
                        col=node.col_offset,
                        callee_name=callee_name,
                        args=node.args,
                        keywords=node.keywords,
                        is_method_call=is_method,
                        receiver_name=receiver,
                    ),
                    tainted_arg_indices | tainted_arg_values,
                )
            )
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

    def visit_Return(self, node: ast.Return) -> None:  # noqa: N802
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
        """Check whether an expression references tainted values."""
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


# Expression handlers

def _tainted_attribute(visitor: TaintVisitor, expr: ast.Attribute) -> bool:
    return visitor.expr_tainted(expr.value)


def _tainted_subscript(visitor: TaintVisitor, expr: ast.Subscript) -> bool:
    return visitor.expr_tainted(expr.value) or visitor.expr_tainted(expr.slice)


def _tainted_binop(visitor: TaintVisitor, expr: ast.BinOp) -> bool:
    return visitor.expr_tainted(expr.left) or visitor.expr_tainted(expr.right)


def _tainted_unary(visitor: TaintVisitor, expr: ast.UnaryOp) -> bool:
    return visitor.expr_tainted(expr.operand)


def _tainted_call(visitor: TaintVisitor, expr: ast.Call) -> bool:
    if any(visitor.expr_tainted(arg) for arg in expr.args):
        return True
    if any(visitor.expr_tainted(kw.value) for kw in expr.keywords):
        return True
    return isinstance(expr.func, ast.Attribute) and visitor.expr_tainted(expr.func.value)


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
    return any(visitor.expr_tainted(val) for val in expr.values)


def _tainted_namedexpr(visitor: TaintVisitor, expr: ast.NamedExpr) -> bool:
    return visitor.expr_tainted(expr.value)


def _tainted_starred(visitor: TaintVisitor, expr: ast.Starred) -> bool:
    return visitor.expr_tainted(expr.value)


def _tainted_lambda(_visitor: TaintVisitor, _expr: ast.Lambda) -> bool:
    return False


def _tainted_comprehension(
    visitor: TaintVisitor,
    *,
    generators: list[ast.comprehension],
    expressions: tuple[ast.expr, ...],
) -> bool:
    for gen in generators:
        if visitor.expr_tainted(gen.iter):
            return True
    return any(visitor.expr_tainted(expr) for expr in expressions)


TaintHandler = Callable[[TaintVisitor, ast.AST], bool]

_EXPR_TAINT_HANDLERS: dict[type[ast.AST], TaintHandler] = {
    ast.Attribute: cast("TaintHandler", _tainted_attribute),
    ast.Subscript: cast("TaintHandler", _tainted_subscript),
    ast.BinOp: cast("TaintHandler", _tainted_binop),
    ast.UnaryOp: cast("TaintHandler", _tainted_unary),
    ast.Call: cast("TaintHandler", _tainted_call),
    ast.IfExp: cast("TaintHandler", _tainted_ifexp),
    ast.Compare: cast("TaintHandler", _tainted_compare),
    ast.FormattedValue: cast("TaintHandler", _tainted_formatted),
    ast.JoinedStr: cast("TaintHandler", _tainted_joined),
    ast.BoolOp: cast("TaintHandler", _tainted_boolop),
    ast.NamedExpr: cast("TaintHandler", _tainted_namedexpr),
    ast.Starred: cast("TaintHandler", _tainted_starred),
    ast.Lambda: cast("TaintHandler", _tainted_lambda),
    ast.GeneratorExp: cast(
        "TaintHandler",
        lambda visitor, expr: _tainted_comprehension(
            visitor,
            generators=cast("ast.GeneratorExp", expr).generators,
            expressions=(cast("ast.GeneratorExp", expr).elt,),
        ),
    ),
    ast.ListComp: cast(
        "TaintHandler",
        lambda visitor, expr: _tainted_comprehension(
            visitor,
            generators=cast("ast.ListComp", expr).generators,
            expressions=(cast("ast.ListComp", expr).elt,),
        ),
    ),
    ast.SetComp: cast(
        "TaintHandler",
        lambda visitor, expr: _tainted_comprehension(
            visitor,
            generators=cast("ast.SetComp", expr).generators,
            expressions=(cast("ast.SetComp", expr).elt,),
        ),
    ),
    ast.DictComp: cast(
        "TaintHandler",
        lambda visitor, expr: _tainted_comprehension(
            visitor,
            generators=cast("ast.DictComp", expr).generators,
            expressions=(
                cast("ast.DictComp", expr).key,
                cast("ast.DictComp", expr).value,
            ),
        ),
    ),
}


def find_function_node(
    source: str,
    *,
    function_name: str,
    function_line: int,
    class_name: str | None,
) -> ast.FunctionDef | ast.AsyncFunctionDef | None:
    """Locate a function or method node by declared identity."""
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return None

    for node in ast.walk(tree):
        if class_name is not None:
            if isinstance(node, ast.ClassDef) and node.name == class_name:
                for child in node.body:
                    if (
                        isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef))
                        and child.name == function_name
                        and child.lineno == function_line
                    ):
                        return child
            continue

        if (
            isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
            and node.name == function_name
            and node.lineno == function_line
        ):
            return node
    return None


def analyze_function_node(
    *,
    file: str,
    function_node: ast.FunctionDef | ast.AsyncFunctionDef,
    tainted_params: set[str],
    depth: int,
) -> TaintAnalysisResult:
    """Analyze taint flow within one parsed function node."""
    visitor = TaintVisitor(file=file, tainted_params=tainted_params, depth=depth)
    visitor.visit(function_node)
    return TaintAnalysisResult(
        sites=list(visitor.sites),
        calls=list(visitor.calls),
        tainted_vars=set(visitor.tainted),
    )


__all__ = [
    "TaintAnalysisResult",
    "TaintCallSite",
    "TaintSiteKind",
    "TaintedSite",
    "analyze_function_node",
    "find_function_node",
]

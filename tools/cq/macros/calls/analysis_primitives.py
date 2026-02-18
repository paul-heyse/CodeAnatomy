"""Core call-analysis primitives used by calls macro modules."""

from __future__ import annotations

import ast
import hashlib
from typing import Literal

import msgspec

from tools.cq.core.python_ast_utils import safe_unparse

CallBinding = Literal["ok", "ambiguous", "would_break", "unresolved"]

_ARG_PREVIEW_LIMIT = 3
_KW_PREVIEW_LIMIT = 2
_ARG_TEXT_LIMIT = 20
_ARG_TEXT_TRIM = 17
_KW_TEXT_LIMIT = 15
_KW_TEXT_TRIM = 12
_MIN_QUAL_PARTS = 2


class CallAnalysis(msgspec.Struct, frozen=True):
    """Structured analysis output for a call expression."""

    num_args: int
    num_kwargs: int
    kwargs: list[str]
    has_star_args: bool
    has_star_kwargs: bool
    arg_preview: str


def _truncate_text(text: str, *, limit: int, trim: int) -> str:
    if len(text) <= limit:
        return text
    return f"{text[:trim]}..."


def _preview_arg(arg: ast.expr) -> str:
    if isinstance(arg, ast.Starred):
        return f"*{safe_unparse(arg.value, default='...')}"
    text = safe_unparse(arg, default="?")
    return _truncate_text(text, limit=_ARG_TEXT_LIMIT, trim=_ARG_TEXT_TRIM)


def _preview_kwarg(kw: ast.keyword) -> str:
    if kw.arg is None:
        return f"**{safe_unparse(kw.value, default='...')}"
    val = safe_unparse(kw.value, default="?")
    val = _truncate_text(val, limit=_KW_TEXT_LIMIT, trim=_KW_TEXT_TRIM)
    return f"{kw.arg}={val}"


def get_containing_function(tree: ast.AST, lineno: int) -> str:
    """Find the function/method containing a line.

    Returns:
        str: Name of the containing function, or ``"<module>"``.
    """
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            end_lineno = getattr(node, "end_lineno", None)
            if end_lineno is not None and node.lineno <= lineno <= end_lineno:
                return node.name
    return "<module>"


def analyze_call(node: ast.Call) -> CallAnalysis:
    """Analyze a call node for argument patterns.

    Returns:
        CallAnalysis: Structured argument/hazard-ready call metadata.
    """
    num_args = 0
    num_kwargs = 0
    kwargs: list[str] = []
    has_star_args = False
    has_star_kwargs = False

    for arg in node.args:
        if isinstance(arg, ast.Starred):
            has_star_args = True
        else:
            num_args += 1

    for kw in node.keywords:
        if kw.arg is None:
            has_star_kwargs = True
        else:
            num_kwargs += 1
            kwargs.append(kw.arg)

    parts: list[str] = []
    parts.extend([_preview_arg(arg) for arg in node.args[:_ARG_PREVIEW_LIMIT]])
    if len(node.args) > _ARG_PREVIEW_LIMIT:
        parts.append(f"...+{len(node.args) - _ARG_PREVIEW_LIMIT}")
    parts.extend([_preview_kwarg(kw) for kw in node.keywords[:_KW_PREVIEW_LIMIT]])
    if len(node.keywords) > _KW_PREVIEW_LIMIT:
        parts.append(f"...+{len(node.keywords) - _KW_PREVIEW_LIMIT}kw")

    return CallAnalysis(
        num_args=num_args,
        num_kwargs=num_kwargs,
        kwargs=kwargs,
        has_star_args=has_star_args,
        has_star_kwargs=has_star_kwargs,
        arg_preview=", ".join(parts) if parts else "()",
    )


def matches_target_expr(func: ast.expr, target_name: str) -> bool:
    """Check whether call expression matches target name.

    Returns:
        bool: ``True`` when the expression resolves to the target token.
    """
    parts = target_name.split(".")
    if isinstance(func, ast.Name):
        return func.id == target_name or func.id == parts[-1]
    if isinstance(func, ast.Attribute):
        if func.attr == target_name:
            return True
        if (
            len(parts) >= _MIN_QUAL_PARTS
            and func.attr == parts[-1]
            and isinstance(func.value, ast.Name)
        ):
            return func.value.id == parts[-2]
        return func.attr == parts[-1]
    return False


def build_call_index(tree: ast.AST) -> dict[tuple[int, int], ast.Call]:
    """Build lookup from (line,col) to call AST node.

    Returns:
        dict[tuple[int, int], ast.Call]: Call nodes indexed by source coordinates.
    """
    index: dict[tuple[int, int], ast.Call] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            index[node.lineno, node.col_offset] = node
    return index


def parse_call_expr(text: str) -> ast.Call | None:
    """Parse expression text and return call node when present.

    Returns:
        ast.Call | None: Parsed call expression when valid.
    """
    try:
        parsed = ast.parse(text.strip(), mode="eval")
    except (SyntaxError, ValueError):
        return None
    if isinstance(parsed, ast.Expression) and isinstance(parsed.body, ast.Call):
        return parsed.body
    return None


def detect_hazards(call: ast.Call, info: CallAnalysis) -> list[str]:
    """Detect dynamic-call hazards for one call expression.

    Returns:
        list[str]: Hazard tags describing dynamic invocation risk.
    """
    hazards: list[str] = []
    if info.has_star_args:
        hazards.append("star_args")
    if info.has_star_kwargs:
        hazards.append("star_kwargs")
    func = call.func
    if isinstance(func, ast.Name):
        name = func.id
        if (
            name in {"getattr", "setattr", "delattr", "hasattr"}
            or name in {"eval", "exec", "__import__"}
            or name in {"globals", "locals", "vars"}
        ):
            hazards.append(name)
    elif isinstance(func, ast.Attribute):
        receiver = func.value
        if isinstance(receiver, ast.Name):
            if receiver.id == "importlib" and func.attr == "import_module":
                hazards.append("importlib.import_module")
            elif receiver.id == "operator" and func.attr in {"attrgetter", "methodcaller"}:
                hazards.append(f"operator.{func.attr}")
            elif receiver.id == "functools" and func.attr == "partial":
                hazards.append("functools.partial")
    return hazards


def stable_callsite_id(
    *,
    file: str,
    line: int,
    col: int,
    callee: str,
    context: str,
) -> str:
    """Build deterministic callsite id for one row.

    Returns:
        str: Stable callsite identifier hash prefix.
    """
    seed = f"{file}:{line}:{col}:{callee}:{context}"
    return hashlib.sha256(seed.encode("utf-8")).hexdigest()[:24]


__all__ = [
    "CallAnalysis",
    "analyze_call",
    "build_call_index",
    "detect_hazards",
    "get_containing_function",
    "matches_target_expr",
    "parse_call_expr",
    "stable_callsite_id",
]

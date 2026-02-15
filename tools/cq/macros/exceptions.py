"""Exceptions macro - raise/catch analysis and exception flow.

Analyzes exception handling patterns, identifies uncaught exceptions,
and maps exception propagation through the codebase.
"""

from __future__ import annotations

import ast
from collections import defaultdict
from pathlib import Path

import msgspec

from tools.cq.core.run_context import RunContext
from tools.cq.core.schema import (
    Anchor,
    CqResult,
    Finding,
    Section,
    mk_result,
    ms,
)
from tools.cq.core.scoring import build_detail_payload
from tools.cq.index.repo import resolve_repo_context
from tools.cq.macros.contracts import ScopedMacroRequestBase
from tools.cq.macros.scan_utils import iter_files
from tools.cq.macros.scope_filters import scope_filter_applied
from tools.cq.macros.scoring_utils import macro_scoring_details

_TOP_EXCEPTION_TYPES = 15
_BARE_EXCEPT_LIMIT = 20
_MAX_MESSAGE_LEN = 50
_MESSAGE_TRIM = 47


class RaiseSite(msgspec.Struct):
    """A location where an exception is raised.

    Parameters
    ----------
    file : str
        File path.
    line : int
        Line number.
    exception_type : str
        Exception class name.
    in_function : str
        Containing function name.
    in_class : str | None
        Containing class name.
    message : str | None
        Exception message if extractable.
    is_reraise : bool
        Whether this is a bare raise.
    """

    file: str
    line: int
    exception_type: str
    in_function: str
    in_class: str | None = None
    message: str | None = None
    is_reraise: bool = False


class CatchSite(msgspec.Struct):
    """A location where an exception is caught.

    Parameters
    ----------
    file : str
        File path.
    line : int
        Line number.
    exception_types : list[str]
        Exception classes caught.
    in_function : str
        Containing function name.
    in_class : str | None
        Containing class name.
    has_handler : bool
        Whether there's actual handling logic.
    is_bare_except : bool
        Whether this is a bare except:.
    reraises : bool
        Whether the handler re-raises.
    """

    file: str
    line: int
    exception_types: list[str]
    in_function: str
    in_class: str | None = None
    has_handler: bool = True
    is_bare_except: bool = False
    reraises: bool = False


class ExceptionsRequest(ScopedMacroRequestBase, frozen=True):
    """Inputs required for exception handling analysis."""

    function: str | None = None


def _safe_unparse(node: ast.AST, *, default: str) -> str:
    try:
        return ast.unparse(node)
    except (ValueError, TypeError):
        return default


class ExceptionVisitor(ast.NodeVisitor):
    """Extract raise and except sites from a module."""

    def __init__(self, file: str) -> None:
        """__init__."""
        self.file = file
        self.raises: list[RaiseSite] = []
        self.catches: list[CatchSite] = []
        self._current_function: str = "<module>"
        self._current_class: str | None = None

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        """Track context for function definitions."""
        old_func = self._current_function
        self._current_function = node.name
        self.generic_visit(node)
        self._current_function = old_func

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        """Track context for async function definitions."""
        old_func = self._current_function
        self._current_function = node.name
        self.generic_visit(node)
        self._current_function = old_func

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        """Track context for class definitions."""
        old_class = self._current_class
        self._current_class = node.name
        self.generic_visit(node)
        self._current_class = old_class

    def visit_Raise(self, node: ast.Raise) -> None:
        """Record raise statements."""
        if node.exc is None:
            # Bare raise
            self.raises.append(
                RaiseSite(
                    file=self.file,
                    line=node.lineno,
                    exception_type="<reraise>",
                    in_function=self._current_function,
                    in_class=self._current_class,
                    is_reraise=True,
                )
            )
        else:
            exc_type = self._extract_exception_type(node.exc)
            message = self._extract_message(node.exc)
            self.raises.append(
                RaiseSite(
                    file=self.file,
                    line=node.lineno,
                    exception_type=exc_type,
                    in_function=self._current_function,
                    in_class=self._current_class,
                    message=message,
                )
            )
        self.generic_visit(node)

    def visit_Try(self, node: ast.Try) -> None:
        """Record exception handlers."""
        for handler in node.handlers:
            exc_types: list[str] = []
            is_bare = False

            if handler.type is None:
                is_bare = True
                exc_types = ["<any>"]
            elif isinstance(handler.type, ast.Tuple):
                for elt in handler.type.elts:
                    exc_types.append(self._get_name(elt))
            else:
                exc_types.append(self._get_name(handler.type))

            # Check if handler has meaningful body
            has_handler = bool(handler.body)
            if len(handler.body) == 1 and isinstance(handler.body[0], ast.Pass):
                has_handler = False

            # Check if handler re-raises
            reraises = False
            for stmt in handler.body:
                if isinstance(stmt, ast.Raise):
                    reraises = True
                    break

            self.catches.append(
                CatchSite(
                    file=self.file,
                    line=handler.lineno,
                    exception_types=exc_types,
                    in_function=self._current_function,
                    in_class=self._current_class,
                    has_handler=has_handler,
                    is_bare_except=is_bare,
                    reraises=reraises,
                )
            )

        self.generic_visit(node)

    def _extract_exception_type(self, exc: ast.expr) -> str:
        """Extract exception type from raise expression.

        Returns:
        -------
        str
            Exception type name.
        """
        if isinstance(exc, ast.Name):
            return exc.id
        if isinstance(exc, ast.Call):
            return self._get_name(exc.func)
        if isinstance(exc, ast.Attribute):
            return _safe_unparse(exc, default=exc.attr)
        return "<unknown>"

    @staticmethod
    def _extract_message(exc: ast.expr) -> str | None:
        """Extract message from exception constructor.

        Returns:
        -------
        str | None
            Message string when extractable.
        """
        if isinstance(exc, ast.Call) and exc.args:
            first_arg = exc.args[0]
            if isinstance(first_arg, ast.Constant) and isinstance(first_arg.value, str):
                msg = first_arg.value
                if len(msg) > _MAX_MESSAGE_LEN:
                    return f"{msg[:_MESSAGE_TRIM]}..."
                return msg
        return None

    @staticmethod
    def _get_name(node: ast.expr) -> str:
        """Get name from an expression node.

        Returns:
        -------
        str
            Best-effort name string.
        """
        if isinstance(node, ast.Name):
            return node.id
        if isinstance(node, ast.Attribute):
            return _safe_unparse(node, default=node.attr)
        return "<unknown>"


def _scan_exceptions(
    root: Path,
    *,
    include: list[str] | None = None,
    exclude: list[str] | None = None,
) -> tuple[list[RaiseSite], list[CatchSite], int]:
    """Scan Python files for raise/catch sites.

    Returns:
    -------
    tuple[list[RaiseSite], list[CatchSite]]
        Raise and catch site lists.
    """
    repo_context = resolve_repo_context(root)
    repo_root = repo_context.repo_root
    all_raises: list[RaiseSite] = []
    all_catches: list[CatchSite] = []
    files_scanned = 0
    for pyfile in iter_files(
        root=repo_root,
        include=include,
        exclude=exclude,
        extensions=(".py",),
    ):
        rel_str = str(pyfile.relative_to(repo_root))
        try:
            source = pyfile.read_text(encoding="utf-8")
            tree = ast.parse(source, filename=rel_str)
        except (SyntaxError, OSError, UnicodeDecodeError):
            continue
        visitor = ExceptionVisitor(rel_str)
        visitor.visit(tree)
        all_raises.extend(visitor.raises)
        all_catches.extend(visitor.catches)
        files_scanned += 1
    return all_raises, all_catches, files_scanned


def _summarize_exception_types(
    all_raises: list[RaiseSite],
    all_catches: list[CatchSite],
) -> tuple[dict[str, int], dict[str, int]]:
    """Summarize exception type frequency.

    Returns:
    -------
    tuple[dict[str, int], dict[str, int]]
        Raised and caught exception type counts.
    """
    raise_types: dict[str, int] = defaultdict(int)
    catch_types: dict[str, int] = defaultdict(int)
    for raised in all_raises:
        if not raised.is_reraise:
            raise_types[raised.exception_type] += 1
    for caught in all_catches:
        for exc_type in caught.exception_types:
            catch_types[exc_type] += 1
    return raise_types, catch_types


def _append_exception_sections(
    result: CqResult,
    *,
    raise_types: dict[str, int],
    catch_types: dict[str, int],
    scoring_details: dict[str, object],
) -> None:
    raise_section = Section(title="Raised Exception Types")
    for exc_type, count in sorted(raise_types.items(), key=lambda item: -item[1])[
        :_TOP_EXCEPTION_TYPES
    ]:
        raise_section.findings.append(
            Finding(
                category="raise",
                message=f"{exc_type}: {count} sites",
                severity="info",
                details=build_detail_payload(scoring=scoring_details),
            )
        )
    result.sections.append(raise_section)
    catch_section = Section(title="Caught Exception Types")
    for exc_type, count in sorted(catch_types.items(), key=lambda item: -item[1])[
        :_TOP_EXCEPTION_TYPES
    ]:
        catch_section.findings.append(
            Finding(
                category="catch",
                message=f"{exc_type}: {count} handlers",
                severity="info",
                details=build_detail_payload(scoring=scoring_details),
            )
        )
    result.sections.append(catch_section)


def _has_matching_catch(
    raise_site: RaiseSite,
    catches: list[CatchSite],
) -> bool:
    for caught in catches:
        if caught.file != raise_site.file or caught.in_function != raise_site.in_function:
            continue
        if raise_site.exception_type in caught.exception_types or "<any>" in caught.exception_types:
            return True
        if {"Exception", "BaseException"} & set(caught.exception_types):
            return True
    return False


def _append_uncaught_section(
    result: CqResult,
    *,
    all_raises: list[RaiseSite],
    all_catches: list[CatchSite],
    scoring_details: dict[str, object],
) -> None:
    uncaught_section = Section(title="Potentially Uncaught Exceptions")
    for raised in all_raises:
        if raised.is_reraise:
            continue
        if _has_matching_catch(raised, all_catches):
            continue
        details: dict[str, object] = {}
        if raised.message:
            details["message"] = raised.message
        uncaught_section.findings.append(
            Finding(
                category="uncaught",
                message=f"{raised.exception_type} raised in {raised.in_function}",
                anchor=Anchor(file=raised.file, line=raised.line),
                severity="warning",
                details=build_detail_payload(scoring=scoring_details, data=details),
            )
        )
    result.sections.append(uncaught_section)


def _append_bare_except_section(
    result: CqResult,
    *,
    bare_excepts: list[CatchSite],
    scoring_details: dict[str, object],
) -> None:
    if not bare_excepts:
        return
    bare_section = Section(title="Bare Except Clauses")
    for caught in bare_excepts[:_BARE_EXCEPT_LIMIT]:
        details = {"reraises": caught.reraises}
        bare_section.findings.append(
            Finding(
                category="bare_except",
                message=f"in {caught.in_function}",
                anchor=Anchor(file=caught.file, line=caught.line),
                severity="warning",
                details=build_detail_payload(scoring=scoring_details, data=details),
            )
        )
    result.sections.append(bare_section)


def _append_exception_evidence(
    result: CqResult,
    *,
    all_raises: list[RaiseSite],
    all_catches: list[CatchSite],
    scoring_details: dict[str, object],
) -> None:
    for raised in all_raises:
        message = f"raise {raised.exception_type}"
        if raised.message:
            message = f"{message}: {raised.message}"
        details = {
            "function": raised.in_function,
            "class": raised.in_class,
            "is_reraise": raised.is_reraise,
        }
        result.evidence.append(
            Finding(
                category="raise",
                message=message,
                anchor=Anchor(file=raised.file, line=raised.line),
                details=build_detail_payload(scoring=scoring_details, data=details),
            )
        )
    for caught in all_catches:
        details = {
            "function": caught.in_function,
            "class": caught.in_class,
            "bare": caught.is_bare_except,
            "reraises": caught.reraises,
        }
        result.evidence.append(
            Finding(
                category="catch",
                message=f"except {', '.join(caught.exception_types)}",
                anchor=Anchor(file=caught.file, line=caught.line),
                details=build_detail_payload(scoring=scoring_details, data=details),
            )
        )


def _apply_rust_fallback(
    result: CqResult,
    root: Path,
    function: str | None,
) -> CqResult:
    """Append Rust fallback findings and multilang summary to an exceptions result.

    Args:
        result: Existing Python-only CqResult.
        root: Repository root path.
        function: Optional function focus for Rust search pattern.

    Returns:
        The mutated result with Rust fallback data merged in.
    """
    from tools.cq.macros.multilang_fallback import apply_rust_macro_fallback

    pattern = function if function else "panic!\\|unwrap\\|expect\\|Result<\\|Err("
    return apply_rust_macro_fallback(
        result=result,
        root=root,
        pattern=pattern,
        macro_name="exceptions",
        query=function,
    )


def cmd_exceptions(request: ExceptionsRequest) -> CqResult:
    """Analyze exception handling patterns.

    Parameters
    ----------
    request : ExceptionsRequest
        Exception-analysis request payload.

    Returns:
    -------
    CqResult
        Analysis result.
    """
    started = ms()

    all_raises, all_catches, files_scanned = _scan_exceptions(
        request.root,
        include=request.include,
        exclude=request.exclude,
    )

    # Filter by function if specified
    if request.function:
        all_raises = [r for r in all_raises if r.in_function == request.function]
        all_catches = [c for c in all_catches if c.in_function == request.function]

    run = RunContext.from_parts(
        root=request.root,
        argv=request.argv,
        tc=request.tc,
        started_ms=started,
    ).to_runmeta("exceptions")
    result = mk_result(run)

    raise_types, catch_types = _summarize_exception_types(all_raises, all_catches)

    result.summary = {
        "files_scanned": files_scanned,
        "scope_file_count": files_scanned,
        "scope_filter_applied": scope_filter_applied(request.include, request.exclude),
        "total_raises": len(all_raises),
        "total_catches": len(all_catches),
        "unique_exception_types": len(raise_types),
        "bare_excepts": sum(1 for c in all_catches if c.is_bare_except),
        "reraises": sum(1 for r in all_raises if r.is_reraise),
    }

    bare_excepts = [caught for caught in all_catches if caught.is_bare_except]
    scoring_details = macro_scoring_details(
        sites=len(all_raises),
        files=len({r.file for r in all_raises} | {c.file for c in all_catches}),
        breakages=len(bare_excepts),
        evidence_kind="resolved_ast",
    )

    # Key findings
    if bare_excepts:
        result.key_findings.append(
            Finding(
                category="warning",
                message=f"Found {len(bare_excepts)} bare except: clauses",
                severity="warning",
                details=build_detail_payload(scoring=scoring_details),
            )
        )

    empty_handlers = [c for c in all_catches if not c.has_handler]
    if empty_handlers:
        result.key_findings.append(
            Finding(
                category="warning",
                message=f"Found {len(empty_handlers)} empty exception handlers",
                severity="warning",
                details=build_detail_payload(scoring=scoring_details),
            )
        )

    _append_exception_sections(
        result,
        raise_types=raise_types,
        catch_types=catch_types,
        scoring_details=scoring_details,
    )
    _append_uncaught_section(
        result,
        all_raises=all_raises,
        all_catches=all_catches,
        scoring_details=scoring_details,
    )
    _append_bare_except_section(result, bare_excepts=bare_excepts, scoring_details=scoring_details)
    _append_exception_evidence(
        result,
        all_raises=all_raises,
        all_catches=all_catches,
        scoring_details=scoring_details,
    )

    return _apply_rust_fallback(result, request.root, request.function)

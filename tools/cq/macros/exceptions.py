"""Exceptions macro - raise/catch analysis and exception flow.

Analyzes exception handling patterns, identifies uncaught exceptions,
and maps exception propagation through the codebase.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from pathlib import Path
from typing import TYPE_CHECKING

import msgspec

from tools.cq.core.python_ast_utils import safe_unparse
from tools.cq.core.schema import (
    Anchor,
    CqResult,
    Finding,
    Section,
    append_section_finding,
    ms,
)
from tools.cq.core.scoring import build_detail_payload
from tools.cq.core.summary_types import summary_from_mapping
from tools.cq.macros.contracts import ScopedMacroRequestBase, ScoringDetailsV1
from tools.cq.macros.result_builder import MacroResultBuilder
from tools.cq.macros.rust_fallback_policy import RustFallbackPolicyV1, apply_rust_fallback_policy
from tools.cq.macros.shared import macro_scoring_details, scan_python_files, scope_filter_applied

if TYPE_CHECKING:
    from tools.cq.analysis.visitors.exception_visitor import ExceptionVisitor

_TOP_EXCEPTION_TYPES = 15
_BARE_EXCEPT_LIMIT = 20
logger = logging.getLogger(__name__)


class RaiseSite(msgspec.Struct, frozen=True):
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


class CatchSite(msgspec.Struct, frozen=True):
    """A location where an exception is caught.

    Parameters
    ----------
    file : str
        File path.
    line : int
        Line number.
    exception_types : tuple[str, ...]
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
    in_function: str
    exception_types: tuple[str, ...] = ()
    in_class: str | None = None
    has_handler: bool = True
    is_bare_except: bool = False
    reraises: bool = False


class ExceptionsRequest(ScopedMacroRequestBase, frozen=True):
    """Inputs required for exception handling analysis."""

    function: str | None = None


def _exception_visitor_factory(file: str) -> ExceptionVisitor[RaiseSite, CatchSite]:
    from tools.cq.analysis.visitors.exception_visitor import ExceptionVisitor

    return ExceptionVisitor(
        file,
        make_raise_site=RaiseSite,
        make_catch_site=CatchSite,
        safe_unparse=safe_unparse,
    )


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
    all_raises: list[RaiseSite] = []
    all_catches: list[CatchSite] = []
    visitors, files_scanned = scan_python_files(
        root,
        include=include,
        exclude=exclude,
        visitor_factory=_exception_visitor_factory,
    )
    for visitor in visitors:
        all_raises.extend(visitor.raises)
        all_catches.extend(visitor.catches)
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
    *,
    raise_types: dict[str, int],
    catch_types: dict[str, int],
    scoring_details: ScoringDetailsV1,
) -> tuple[Section, Section]:
    raise_section = Section(title="Raised Exception Types")
    for exc_type, count in sorted(raise_types.items(), key=lambda item: -item[1])[
        :_TOP_EXCEPTION_TYPES
    ]:
        raise_section = append_section_finding(
            raise_section,
            Finding(
                category="raise",
                message=f"{exc_type}: {count} sites",
                severity="info",
                details=build_detail_payload(scoring=scoring_details),
            ),
        )
    catch_section = Section(title="Caught Exception Types")
    for exc_type, count in sorted(catch_types.items(), key=lambda item: -item[1])[
        :_TOP_EXCEPTION_TYPES
    ]:
        catch_section = append_section_finding(
            catch_section,
            Finding(
                category="catch",
                message=f"{exc_type}: {count} handlers",
                severity="info",
                details=build_detail_payload(scoring=scoring_details),
            ),
        )
    return raise_section, catch_section


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
    *,
    all_raises: list[RaiseSite],
    all_catches: list[CatchSite],
    scoring_details: ScoringDetailsV1,
) -> Section:
    uncaught_section = Section(title="Potentially Uncaught Exceptions")
    for raised in all_raises:
        if raised.is_reraise:
            continue
        if _has_matching_catch(raised, all_catches):
            continue
        details: dict[str, object] = {}
        if raised.message:
            details["message"] = raised.message
        uncaught_section = append_section_finding(
            uncaught_section,
            Finding(
                category="uncaught",
                message=f"{raised.exception_type} raised in {raised.in_function}",
                anchor=Anchor(file=raised.file, line=raised.line),
                severity="warning",
                details=build_detail_payload(scoring=scoring_details, data=details),
            ),
        )
    return uncaught_section


def _append_bare_except_section(
    *,
    bare_excepts: list[CatchSite],
    scoring_details: ScoringDetailsV1,
) -> Section | None:
    if not bare_excepts:
        return None
    bare_section = Section(title="Bare Except Clauses")
    for caught in bare_excepts[:_BARE_EXCEPT_LIMIT]:
        details = {"reraises": caught.reraises}
        bare_section = append_section_finding(
            bare_section,
            Finding(
                category="bare_except",
                message=f"in {caught.in_function}",
                anchor=Anchor(file=caught.file, line=caught.line),
                severity="warning",
                details=build_detail_payload(scoring=scoring_details, data=details),
            ),
        )
    return bare_section


def _append_exception_evidence(
    *,
    all_raises: list[RaiseSite],
    all_catches: list[CatchSite],
    scoring_details: ScoringDetailsV1,
) -> list[Finding]:
    evidence: list[Finding] = []
    for raised in all_raises:
        message = f"raise {raised.exception_type}"
        if raised.message:
            message = f"{message}: {raised.message}"
        details = {
            "function": raised.in_function,
            "class": raised.in_class,
            "is_reraise": raised.is_reraise,
        }
        evidence.append(
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
        evidence.append(
            Finding(
                category="catch",
                message=f"except {', '.join(caught.exception_types)}",
                anchor=Anchor(file=caught.file, line=caught.line),
                details=build_detail_payload(scoring=scoring_details, data=details),
            )
        )
    return evidence


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
    logger.debug(
        "Running exceptions macro root=%s function=%s include=%d exclude=%d",
        request.root,
        request.function,
        len(request.include),
        len(request.exclude),
    )

    all_raises, all_catches, files_scanned = _scan_exceptions(
        request.root,
        include=request.include,
        exclude=request.exclude,
    )

    # Filter by function if specified
    if request.function:
        all_raises = [r for r in all_raises if r.in_function == request.function]
        all_catches = [c for c in all_catches if c.in_function == request.function]

    builder = MacroResultBuilder(
        "exceptions",
        root=request.root,
        argv=request.argv,
        tc=request.tc,
        started_ms=started,
    )
    raise_types, catch_types = _summarize_exception_types(all_raises, all_catches)

    builder.with_summary(
        summary_from_mapping(
            {
                "files_scanned": files_scanned,
                "scope_file_count": files_scanned,
                "scope_filter_applied": scope_filter_applied(request.include, request.exclude),
                "total_raises": len(all_raises),
                "total_catches": len(all_catches),
                "unique_exception_types": len(raise_types),
                "bare_excepts": sum(1 for c in all_catches if c.is_bare_except),
                "reraises": sum(1 for r in all_raises if r.is_reraise),
            }
        )
    )

    bare_excepts = [caught for caught in all_catches if caught.is_bare_except]
    scoring_details = macro_scoring_details(
        sites=len(all_raises),
        files=len({r.file for r in all_raises} | {c.file for c in all_catches}),
        breakages=len(bare_excepts),
        evidence_kind="resolved_ast",
    )

    # Key findings
    if bare_excepts:
        builder.add_finding(
            Finding(
                category="warning",
                message=f"Found {len(bare_excepts)} bare except: clauses",
                severity="warning",
                details=build_detail_payload(scoring=scoring_details),
            )
        )

    empty_handlers = [c for c in all_catches if not c.has_handler]
    if empty_handlers:
        builder.add_finding(
            Finding(
                category="warning",
                message=f"Found {len(empty_handlers)} empty exception handlers",
                severity="warning",
                details=build_detail_payload(scoring=scoring_details),
            )
        )

    raised_section, caught_section = _append_exception_sections(
        raise_types=raise_types,
        catch_types=catch_types,
        scoring_details=scoring_details,
    )
    builder.add_section(raised_section)
    builder.add_section(caught_section)
    builder.add_section(
        _append_uncaught_section(
            all_raises=all_raises,
            all_catches=all_catches,
            scoring_details=scoring_details,
        )
    )
    bare_section = _append_bare_except_section(
        bare_excepts=bare_excepts,
        scoring_details=scoring_details,
    )
    if bare_section is not None:
        builder.add_section(bare_section)
    builder.add_evidences(
        _append_exception_evidence(
            all_raises=all_raises,
            all_catches=all_catches,
            scoring_details=scoring_details,
        )
    )

    result = apply_rust_fallback_policy(
        builder.build(),
        root=request.root,
        policy=RustFallbackPolicyV1(
            macro_name="exceptions",
            pattern=(
                request.function
                if request.function
                else "panic!\\|unwrap\\|expect\\|Result<\\|Err("
            ),
            query=request.function,
        ),
    )
    logger.debug(
        "Completed exceptions macro raises=%d catches=%d files_scanned=%d",
        len(all_raises),
        len(all_catches),
        files_scanned,
    )
    return result

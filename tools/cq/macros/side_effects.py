"""Side effects analysis - import-time calls and global state.

Detects function calls at module top-level, global state writes, and ambient reads.
"""

from __future__ import annotations

import ast
import logging
from pathlib import Path
from typing import TYPE_CHECKING, Literal

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
from tools.cq.core.summary_contract import summary_from_mapping
from tools.cq.macros.contracts import ScopedMacroRequestBase, ScoringDetailsV1
from tools.cq.macros.result_builder import MacroResultBuilder
from tools.cq.macros.rust_fallback_policy import RustFallbackPolicyV1, apply_rust_fallback_policy
from tools.cq.macros.shared import macro_scoring_details, scan_python_files, scope_filter_applied

if TYPE_CHECKING:
    from tools.cq.analysis.visitors.side_effect_visitor import SideEffectVisitor

_MAX_EFFECTS_DISPLAY = 40
logger = logging.getLogger(__name__)

# Known ambient state patterns
AMBIENT_PATTERNS = {
    "os.environ": "environment",
    "sys.argv": "argv",
    "pathlib.Path.cwd": "cwd",
    "Path.cwd": "cwd",
    "os.getcwd": "cwd",
}

# Skip patterns - known safe top-level calls
SAFE_TOP_LEVEL = {
    "TypeVar",
    "Generic",
    "Protocol",
    "dataclass",
    "dataclasses.dataclass",
    "field",
    "dataclasses.field",
    "NamedTuple",
    "typing.NamedTuple",
    "Enum",
    "IntEnum",
    "StrEnum",
    "enum.Enum",
    "enum.IntEnum",
    "abstractmethod",
    "abc.abstractmethod",
    "property",
    "staticmethod",
    "classmethod",
    "contextmanager",
    "contextlib.contextmanager",
    "overload",
    "typing.overload",
    "final",
    "typing.final",
    "runtime_checkable",
    "typing.runtime_checkable",
}


class SideEffect(msgspec.Struct, frozen=True):
    """A detected side effect.

    Parameters
    ----------
    file : str
        File path.
    line : int
        Line number.
    kind : str
        Effect kind: "top_level_call", "global_write", "ambient_read".
    description : str
        Human-readable description.
    """

    file: str
    line: int
    kind: str
    description: str


def _is_main_guard(node: ast.If) -> bool:
    """Check if an If node is `if __name__ == "__main__":`.

    Returns:
    -------
    bool
        True if this is a main guard block.
    """
    test = node.test
    # Check for `__name__ == "__main__"` or `"__main__" == __name__`
    if not (
        isinstance(test, ast.Compare)
        and len(test.ops) == 1
        and isinstance(test.ops[0], ast.Eq)
        and len(test.comparators) == 1
    ):
        return False

    left = test.left
    right = test.comparators[0]

    return (
        isinstance(left, ast.Name)
        and left.id == "__name__"
        and isinstance(right, ast.Constant)
        and right.value == "__main__"
    ) or (
        isinstance(right, ast.Name)
        and right.id == "__name__"
        and isinstance(left, ast.Constant)
        and left.value == "__main__"
    )


def _side_effect_visitor_factory(file: str) -> SideEffectVisitor[SideEffect]:
    from tools.cq.analysis.visitors.side_effect_visitor import SideEffectVisitor

    return SideEffectVisitor(
        file,
        make_side_effect=SideEffect,
        safe_unparse=safe_unparse,
        safe_top_level=SAFE_TOP_LEVEL,
        ambient_patterns=AMBIENT_PATTERNS,
        is_main_guard=_is_main_guard,
    )


class SideEffectsRequest(ScopedMacroRequestBase, frozen=True):
    """Inputs required for side effect analysis."""

    max_files: int = 2000


def _scan_side_effects(
    root: Path,
    max_files: int,
    *,
    include: list[str] | None = None,
    exclude: list[str] | None = None,
) -> tuple[list[SideEffect], int]:
    all_effects: list[SideEffect] = []
    visitors, files_scanned = scan_python_files(
        root,
        include=include,
        exclude=exclude,
        max_files=max_files,
        visitor_factory=_side_effect_visitor_factory,
    )
    for visitor in visitors:
        all_effects.extend(visitor.effects)

    return all_effects, files_scanned


def _group_effects_by_kind(all_effects: list[SideEffect]) -> dict[str, list[SideEffect]]:
    by_kind: dict[str, list[SideEffect]] = {}
    for effect in all_effects:
        by_kind.setdefault(effect.kind, []).append(effect)
    return by_kind


def _append_kind_sections(
    by_kind: dict[str, list[SideEffect]],
    scoring_details: ScoringDetailsV1,
) -> list[Section]:
    sections: list[Section] = []
    kind_titles = {
        "top_level_call": "Top Level Calls",
        "global_write": "Global Writes",
        "ambient_read": "Ambient Reads",
    }
    severity_map: dict[str, Literal["info", "warning", "error"]] = {
        "top_level_call": "warning",
        "global_write": "warning",
        "ambient_read": "info",
    }

    for kind in ("top_level_call", "global_write", "ambient_read"):
        effects = by_kind.get(kind, [])
        if not effects:
            continue
        section = Section(title=kind_titles[kind])
        for effect in effects[:_MAX_EFFECTS_DISPLAY]:
            section = append_section_finding(
                section,
                Finding(
                    category=kind,
                    message=effect.description,
                    anchor=Anchor(file=effect.file, line=effect.line),
                    severity=severity_map[kind],
                    details=build_detail_payload(scoring=scoring_details),
                ),
            )
        if len(effects) > _MAX_EFFECTS_DISPLAY:
            section = append_section_finding(
                section,
                Finding(
                    category="truncated",
                    message=f"... and {len(effects) - _MAX_EFFECTS_DISPLAY} more",
                    severity="info",
                    details=build_detail_payload(scoring=scoring_details),
                ),
            )
        sections.append(section)
    return sections


def _append_evidence(
    all_effects: list[SideEffect],
    scoring_details: ScoringDetailsV1,
) -> list[Finding]:
    return [
        Finding(
            category=effect.kind,
            message=effect.description,
            anchor=Anchor(file=effect.file, line=effect.line),
            details=build_detail_payload(scoring=scoring_details),
        )
        for effect in all_effects
    ]


def cmd_side_effects(request: SideEffectsRequest) -> CqResult:
    """Analyze import-time side effects.

    Parameters
    ----------
    request : SideEffectsRequest
        Side effects request payload.

    Returns:
    -------
    CqResult
        Analysis result.
    """
    started = ms()
    logger.debug(
        "Running side-effects macro root=%s max_files=%d include=%d exclude=%d",
        request.root,
        request.max_files,
        len(request.include),
        len(request.exclude),
    )

    all_effects, files_scanned = _scan_side_effects(
        request.root,
        request.max_files,
        include=request.include,
        exclude=request.exclude,
    )

    builder = MacroResultBuilder(
        "side-effects",
        root=request.root,
        argv=request.argv,
        tc=request.tc,
        started_ms=started,
    )
    # Categorize effects
    by_kind = _group_effects_by_kind(all_effects)

    updated_summary = summary_from_mapping(
        {
            "files_scanned": files_scanned,
            "scope_file_count": files_scanned,
            "scope_filter_applied": scope_filter_applied(
                request.include,
                request.exclude,
            ),
            "total_effects": len(all_effects),
            "top_level_calls": len(by_kind.get("top_level_call", [])),
            "global_writes": len(by_kind.get("global_write", [])),
            "ambient_reads": len(by_kind.get("ambient_read", [])),
        }
    )
    builder.with_summary(updated_summary)

    # Compute scoring signals
    unique_files = len({e.file for e in all_effects})
    scoring_details = macro_scoring_details(
        sites=len(all_effects),
        files=unique_files,
        evidence_kind="resolved_ast",
    )

    # Key findings
    if by_kind.get("top_level_call"):
        builder.add_finding(
            Finding(
                category="warning",
                message=f"{len(by_kind['top_level_call'])} import-time function calls",
                severity="warning",
                details=build_detail_payload(scoring=scoring_details),
            )
        )
    if by_kind.get("global_write"):
        builder.add_finding(
            Finding(
                category="warning",
                message=f"{len(by_kind['global_write'])} module-level mutations",
                severity="warning",
                details=build_detail_payload(scoring=scoring_details),
            )
        )
    if by_kind.get("ambient_read"):
        builder.add_finding(
            Finding(
                category="info",
                message=f"{len(by_kind['ambient_read'])} ambient state accesses",
                severity="info",
                details=build_detail_payload(scoring=scoring_details),
            )
        )

    if not all_effects:
        builder.add_finding(
            Finding(
                category="info",
                message="No import-time side effects detected",
                severity="info",
                details=build_detail_payload(scoring=scoring_details),
            )
        )

    for section in _append_kind_sections(by_kind, scoring_details):
        builder.add_section(section)
    builder.add_evidences(_append_evidence(all_effects, scoring_details))

    result = apply_rust_fallback_policy(
        builder.build(),
        root=request.root,
        policy=RustFallbackPolicyV1(
            macro_name="side-effects",
            pattern="static mut \\|lazy_static\\|thread_local\\|unsafe ",
        ),
    )
    logger.debug(
        "Completed side-effects macro total_effects=%d files_scanned=%d",
        len(all_effects),
        files_scanned,
    )
    return result

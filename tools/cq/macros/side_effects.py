"""Side effects analysis - import-time calls and global state.

Detects function calls at module top-level, global state writes, and ambient reads.
"""

from __future__ import annotations

import ast
from pathlib import Path
from typing import Literal

import msgspec

from tools.cq.core.python_ast_utils import safe_unparse
from tools.cq.core.schema import (
    Anchor,
    CqResult,
    Finding,
    Section,
    ms,
)
from tools.cq.core.scoring import build_detail_payload
from tools.cq.index.repo import resolve_repo_context
from tools.cq.macros.contracts import ScopedMacroRequestBase, ScoringDetailsV1
from tools.cq.macros.result_builder import MacroResultBuilder
from tools.cq.macros.rust_fallback_policy import RustFallbackPolicyV1, apply_rust_fallback_policy
from tools.cq.macros.shared import iter_files, macro_scoring_details, scope_filter_applied

_MAX_EFFECTS_DISPLAY = 40

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


class SideEffect(msgspec.Struct):
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


class SideEffectVisitor(ast.NodeVisitor):
    """Detect side effects at module top level."""

    def __init__(self, file: str) -> None:
        """__init__."""
        self.file = file
        self.effects: list[SideEffect] = []
        self._in_def = 0  # Nesting depth in function/class
        self._in_main_guard = False  # Inside if __name__ == "__main__"

    def visit_If(self, node: ast.If) -> None:
        """Visit an if statement, checking for main guard."""
        if _is_main_guard(node) and self._in_def == 0:
            # Skip the body of if __name__ == "__main__":
            self._in_main_guard = True
            self.generic_visit(node)
            self._in_main_guard = False
        else:
            self.generic_visit(node)

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        """Visit a function definition."""
        self._in_def += 1
        self.generic_visit(node)
        self._in_def -= 1

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        """Visit an async function definition."""
        self._in_def += 1
        self.generic_visit(node)
        self._in_def -= 1

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        """Visit a class definition."""
        self._in_def += 1
        self.generic_visit(node)
        self._in_def -= 1

    def visit_Call(self, node: ast.Call) -> None:
        """Visit a call expression for import-time calls."""
        if self._in_def == 0 and not self._in_main_guard:
            callee = safe_unparse(node.func, default="<unknown>")

            # Skip decorators and type-related calls
            if callee not in SAFE_TOP_LEVEL:
                self.effects.append(
                    SideEffect(
                        file=self.file,
                        line=node.lineno,
                        kind="top_level_call",
                        description=f"Import-time call: {callee}(...)",
                    )
                )
        self.generic_visit(node)

    def visit_Assign(self, node: ast.Assign) -> None:
        """Visit an assignment for module-level mutations."""
        if self._in_def == 0 and not self._in_main_guard:
            for target in node.targets:
                if isinstance(target, ast.Subscript):
                    # Dict/list mutation at module level
                    target_str = safe_unparse(target, default="<unknown>")
                    self.effects.append(
                        SideEffect(
                            file=self.file,
                            line=node.lineno,
                            kind="global_write",
                            description=f"Module-level mutation: {target_str} = ...",
                        )
                    )
        self.generic_visit(node)

    def visit_AugAssign(self, node: ast.AugAssign) -> None:
        """Visit an augmented assignment for module-level mutations."""
        if self._in_def == 0 and not self._in_main_guard:
            target_str = safe_unparse(node.target, default="<unknown>")
            self.effects.append(
                SideEffect(
                    file=self.file,
                    line=node.lineno,
                    kind="global_write",
                    description=f"Module-level augmented assign: {target_str} {ast.dump(node.op)} ...",
                )
            )
        self.generic_visit(node)

    def visit_Attribute(self, node: ast.Attribute) -> None:
        """Visit attribute access for ambient reads."""
        if self._in_def == 0 and not self._in_main_guard:
            full = safe_unparse(node, default="<unknown>")
            for pattern, category in AMBIENT_PATTERNS.items():
                if full.endswith(pattern) or pattern in full:
                    self.effects.append(
                        SideEffect(
                            file=self.file,
                            line=node.lineno,
                            kind="ambient_read",
                            description=f"Ambient {category} access: {full}",
                        )
                    )
                    break
        self.generic_visit(node)


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
    repo_context = resolve_repo_context(root)
    repo_root = repo_context.repo_root
    all_effects: list[SideEffect] = []
    files_scanned = 0

    for pyfile in iter_files(
        root=repo_root,
        include=include,
        exclude=exclude,
        extensions=(".py",),
        max_files=max_files,
    ):
        rel = pyfile.relative_to(repo_root)
        try:
            source = pyfile.read_text(encoding="utf-8")
            tree = ast.parse(source, filename=str(rel))
        except (SyntaxError, OSError, UnicodeDecodeError):
            continue
        visitor = SideEffectVisitor(str(rel))
        visitor.visit(tree)
        all_effects.extend(visitor.effects)
        files_scanned += 1

    return all_effects, files_scanned


def _group_effects_by_kind(all_effects: list[SideEffect]) -> dict[str, list[SideEffect]]:
    by_kind: dict[str, list[SideEffect]] = {}
    for effect in all_effects:
        by_kind.setdefault(effect.kind, []).append(effect)
    return by_kind


def _append_kind_sections(
    result: CqResult,
    by_kind: dict[str, list[SideEffect]],
    scoring_details: ScoringDetailsV1,
) -> None:
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
            section.findings.append(
                Finding(
                    category=kind,
                    message=effect.description,
                    anchor=Anchor(file=effect.file, line=effect.line),
                    severity=severity_map[kind],
                    details=build_detail_payload(scoring=scoring_details),
                )
            )
        if len(effects) > _MAX_EFFECTS_DISPLAY:
            section.findings.append(
                Finding(
                    category="truncated",
                    message=f"... and {len(effects) - _MAX_EFFECTS_DISPLAY} more",
                    severity="info",
                    details=build_detail_payload(scoring=scoring_details),
                )
            )
        result.sections.append(section)


def _append_evidence(
    result: CqResult,
    all_effects: list[SideEffect],
    scoring_details: ScoringDetailsV1,
) -> None:
    for effect in all_effects:
        result.evidence.append(
            Finding(
                category=effect.kind,
                message=effect.description,
                anchor=Anchor(file=effect.file, line=effect.line),
                details=build_detail_payload(scoring=scoring_details),
            )
        )


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
    result = builder.result

    # Categorize effects
    by_kind = _group_effects_by_kind(all_effects)

    result.summary = {
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

    # Compute scoring signals
    unique_files = len({e.file for e in all_effects})
    scoring_details = macro_scoring_details(
        sites=len(all_effects),
        files=unique_files,
        evidence_kind="resolved_ast",
    )

    # Key findings
    if by_kind.get("top_level_call"):
        result.key_findings.append(
            Finding(
                category="warning",
                message=f"{len(by_kind['top_level_call'])} import-time function calls",
                severity="warning",
                details=build_detail_payload(scoring=scoring_details),
            )
        )
    if by_kind.get("global_write"):
        result.key_findings.append(
            Finding(
                category="warning",
                message=f"{len(by_kind['global_write'])} module-level mutations",
                severity="warning",
                details=build_detail_payload(scoring=scoring_details),
            )
        )
    if by_kind.get("ambient_read"):
        result.key_findings.append(
            Finding(
                category="info",
                message=f"{len(by_kind['ambient_read'])} ambient state accesses",
                severity="info",
                details=build_detail_payload(scoring=scoring_details),
            )
        )

    if not all_effects:
        result.key_findings.append(
            Finding(
                category="info",
                message="No import-time side effects detected",
                severity="info",
                details=build_detail_payload(scoring=scoring_details),
            )
        )

    _append_kind_sections(result, by_kind, scoring_details)
    _append_evidence(result, all_effects, scoring_details)

    return apply_rust_fallback_policy(
        result,
        root=request.root,
        policy=RustFallbackPolicyV1(
            macro_name="side-effects",
            pattern="static mut \\|lazy_static\\|thread_local\\|unsafe ",
        ),
    )

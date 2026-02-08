#!/usr/bin/env python3
"""AST-backed drift surface audit for programmatic architecture guardrails.

This script replaces text-grep heuristics with deterministic AST checks for:

1. Compile-context fallback drift
2. Resolver identity instrumentation drift
3. Typed artifact adherence for ``record_artifact(...)``
4. Manifest-authoritative naming drift
5. ViewKind sole authority
6. Builder dispatch coverage
7. Entity registry derivation

Usage:
    scripts/check_drift_surfaces.py
    scripts/check_drift_surfaces.py --strict
    scripts/check_drift_surfaces.py --json
    scripts/check_drift_surfaces.py --root /path/to/repo
"""

from __future__ import annotations

import argparse
import ast
import json
import sys
from dataclasses import dataclass
from pathlib import Path

_ALLOWED_COMPILE_CONTEXT_PATH = "src/semantics/compile_context.py"
_ALLOWED_CANONICAL_WITHOUT_MANIFEST: set[tuple[str, str]] = {
    ("src/datafusion_engine/dataset/semantic_catalog.py", "_register_semantic_outputs"),
    ("src/semantics/catalog/semantic_singletons_registry.py", "<module>"),
    ("src/semantics/compile_context.py", "_resolved_outputs"),
    ("src/semantics/ir_pipeline.py", "_build_cpg_output_rows"),
    ("src/semantics/ir_pipeline.py", "build_semantic_ir"),
    ("src/semantics/ir_pipeline.py", "compile_semantics"),
    ("src/semantics/registry.py", "_build_output_specs"),
    ("src/semantics/registry.py", "SemanticNormalizationSpec.output_name"),
    ("src/semantics/registry.py", "build_semantic_model"),
    ("src/semantics/registry.py", "normalization_output_name"),
}

_REQUIRED_COMPILE_TRACKING_SCOPES: tuple[tuple[str, str], ...] = (
    ("src/hamilton_pipeline/driver_factory.py", "build_view_graph_context"),
    ("src/datafusion_engine/plan/pipeline.py", "plan_with_delta_pins"),
)

_REQUIRED_RESOLVER_IDENTITY_TRACKING_SCOPES: tuple[tuple[str, str], ...] = (
    ("src/hamilton_pipeline/driver_factory.py", "build_view_graph_context"),
    ("src/datafusion_engine/plan/pipeline.py", "plan_with_delta_pins"),
)

_REQUIRED_RESOLVER_BOUNDARY_SCOPES: tuple[tuple[str, str], ...] = (
    ("src/datafusion_engine/views/registration.py", "ensure_view_graph"),
    ("src/datafusion_engine/dataset/resolution.py", "apply_scan_unit_overrides"),
    ("src/datafusion_engine/delta/cdf.py", "register_cdf_inputs"),
    ("src/datafusion_engine/session/runtime.py", "record_dataset_readiness"),
)
_RECORD_ARTIFACT_MIN_ARGS = 2
_MAX_VIOLATION_PREVIEW = 5

# ---------------------------------------------------------------------------
# Programmatic architecture drift constants
# ---------------------------------------------------------------------------

_VIEW_KIND_AUTHORITY_PATH = "src/semantics/view_kinds.py"
_VIEW_KIND_VALUES: frozenset[str] = frozenset(
    {
        "normalize",
        "scip_normalize",
        "bytecode_line_index",
        "span_unnest",
        "symtable",
        "diagnostic",
        "export",
        "projection",
        "finalize",
        "artifact",
        "join_group",
        "relate",
        "union_nodes",
        "union_edges",
    }
)
_CONSOLIDATED_KIND_VALUES: frozenset[str] = frozenset(
    {
        "normalize",
        "derive",
        "relate",
        "union",
        "project",
        "diagnostic",
    }
)

_BUILDER_DISPATCH_PATH = "src/semantics/pipeline.py"
_LEGACY_BUILDER_DISPATCH_VAR = "_BUILDER_HANDLERS"
_CONSOLIDATED_BUILDER_DISPATCH_VAR = "_CONSOLIDATED_BUILDER_HANDLERS"

_ENTITY_REGISTRY_PATH = "src/semantics/registry.py"


@dataclass(frozen=True, order=True)
class Violation:
    """Single drift violation."""

    path: str
    line: int
    scope: str
    detail: str

    def to_dict(self) -> dict[str, object]:
        """Return a JSON-serializable mapping for this violation."""
        return {
            "path": self.path,
            "line": self.line,
            "scope": self.scope,
            "detail": self.detail,
        }


@dataclass(frozen=True)
class CheckResult:
    """Result for one drift check."""

    id: str
    title: str
    target: int
    count: int
    violations: tuple[Violation, ...]

    @property
    def status(self) -> str:
        """Return status for this check based on target adherence."""
        return "warning" if self.count > self.target else "ok"

    def to_dict(self) -> dict[str, object]:
        """Return a JSON-serializable mapping for this check."""
        return {
            "id": self.id,
            "title": self.title,
            "target": self.target,
            "count": self.count,
            "status": self.status,
            "violations": [item.to_dict() for item in self.violations],
        }


@dataclass(frozen=True)
class AuditReport:
    """Full drift audit report."""

    repo_root: str
    checks: tuple[CheckResult, ...]

    @property
    def warnings(self) -> int:
        """Return number of checks that exceeded target."""
        return sum(1 for item in self.checks if item.status == "warning")

    @property
    def total_checks(self) -> int:
        """Return total number of executed checks."""
        return len(self.checks)

    def to_dict(self) -> dict[str, object]:
        """Return a JSON-serializable report payload."""
        return {
            "repo_root": self.repo_root,
            "summary": {
                "checks_run": self.total_checks,
                "warnings": self.warnings,
            },
            "checks": [item.to_dict() for item in self.checks],
        }


class _DriftVisitor(ast.NodeVisitor):
    """Collect relevant drift evidence from a single Python module."""

    def __init__(self, rel_path: str) -> None:
        self.rel_path = rel_path
        self.scope_stack: list[str] = []
        self.calls_by_scope: dict[str, set[str]] = {}

        self.compile_context_instantiations: list[Violation] = []
        self.dataset_bindings_fallback_calls: list[Violation] = []
        self.untyped_record_artifact_strings: list[Violation] = []
        self.untyped_record_artifact_inline_specs: list[Violation] = []
        self.canonical_name_missing_manifest: list[Violation] = []

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        self.scope_stack.append(node.name)
        self.generic_visit(node)
        self.scope_stack.pop()

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self.scope_stack.append(node.name)
        self.generic_visit(node)
        self.scope_stack.pop()

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        self.scope_stack.append(node.name)
        self.generic_visit(node)
        self.scope_stack.pop()

    def visit_Call(self, node: ast.Call) -> None:
        callee_name = _call_name(node.func)
        scope = _scope_name(self.scope_stack)
        if callee_name is not None:
            self.calls_by_scope.setdefault(scope, set()).add(callee_name)

        if callee_name == "CompileContext" and self.rel_path != _ALLOWED_COMPILE_CONTEXT_PATH:
            self.compile_context_instantiations.append(
                Violation(
                    path=self.rel_path,
                    line=node.lineno,
                    scope=scope,
                    detail="CompileContext(...) used outside compile_context.py",
                )
            )

        if (
            callee_name == "dataset_bindings_for_profile"
            and self.rel_path != _ALLOWED_COMPILE_CONTEXT_PATH
        ):
            self.dataset_bindings_fallback_calls.append(
                Violation(
                    path=self.rel_path,
                    line=node.lineno,
                    scope=scope,
                    detail="dataset_bindings_for_profile(...) fallback usage",
                )
            )

        if callee_name == "record_artifact":
            self._check_record_artifact_typing(node=node, scope=scope)

        if callee_name == "canonical_output_name":
            has_manifest_kw = any(
                isinstance(keyword, ast.keyword) and keyword.arg == "manifest"
                for keyword in node.keywords
            )
            if (
                not has_manifest_kw
                and (self.rel_path, scope) not in _ALLOWED_CANONICAL_WITHOUT_MANIFEST
            ):
                self.canonical_name_missing_manifest.append(
                    Violation(
                        path=self.rel_path,
                        line=node.lineno,
                        scope=scope,
                        detail="canonical_output_name(...) missing manifest= keyword",
                    )
                )

        self.generic_visit(node)

    def _check_record_artifact_typing(self, *, node: ast.Call, scope: str) -> None:
        target_arg: ast.expr | None = None
        if isinstance(node.func, ast.Name) and len(node.args) >= _RECORD_ARTIFACT_MIN_ARGS:
            target_arg = node.args[1]
        elif isinstance(node.func, ast.Attribute) and node.args:
            target_arg = node.args[0]

        if target_arg is None:
            return

        if isinstance(target_arg, ast.Constant) and isinstance(target_arg.value, str):
            self.untyped_record_artifact_strings.append(
                Violation(
                    path=self.rel_path,
                    line=node.lineno,
                    scope=scope,
                    detail="record_artifact(...) uses string literal name",
                )
            )
            return

        if isinstance(target_arg, ast.Call) and _call_name(target_arg.func) == "ArtifactSpec":
            self.untyped_record_artifact_inline_specs.append(
                Violation(
                    path=self.rel_path,
                    line=node.lineno,
                    scope=scope,
                    detail="record_artifact(...) uses inline ArtifactSpec(...) instead of spec constant",
                )
            )


def _call_name(node: ast.AST) -> str | None:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return node.attr
    return None


def _scope_name(scope_stack: list[str]) -> str:
    if not scope_stack:
        return "<module>"
    if len(scope_stack) == 1:
        return scope_stack[0]
    return ".".join(scope_stack)


def _sorted_violations(items: list[Violation]) -> tuple[Violation, ...]:
    return tuple(sorted(items))


def _missing_scope_calls(
    *,
    call_map: dict[tuple[str, str], set[str]],
    required_scopes: tuple[tuple[str, str], ...],
    required_call: str,
    detail: str,
) -> tuple[Violation, ...]:
    missing: list[Violation] = []
    for path, scope in sorted(required_scopes):
        calls = call_map.get((path, scope), set())
        if required_call not in calls:
            missing.append(
                Violation(
                    path=path,
                    line=0,
                    scope=scope,
                    detail=detail,
                )
            )
    return tuple(missing)


def _check_result(
    *,
    check_id: str,
    title: str,
    target: int,
    violations: tuple[Violation, ...],
) -> CheckResult:
    return CheckResult(
        id=check_id,
        title=title,
        target=target,
        count=len(violations),
        violations=violations,
    )


def _is_strenum_class(node: ast.ClassDef) -> bool:
    """Check whether a class definition inherits from StrEnum.

    Returns:
        True when any base class is ``StrEnum``.
    """
    return any(
        (isinstance(base, ast.Name) and base.id == "StrEnum")
        or (isinstance(base, ast.Attribute) and base.attr == "StrEnum")
        for base in node.bases
    )


def _strenum_member_values(node: ast.ClassDef) -> set[str]:
    """Extract string constant member values from a StrEnum class body.

    Returns:
        Set of string literal values assigned in the class body.
    """
    values: set[str] = set()
    for item in node.body:
        if not isinstance(item, ast.Assign):
            continue
        if isinstance(item.value, ast.Constant) and isinstance(item.value.value, str):
            for target in item.targets:
                if isinstance(target, ast.Name):
                    values.add(item.value.value)
    return values


def _check_viewkind_sole_authority(
    src_root: Path,
    repo_root: Path,
) -> tuple[Violation, ...]:
    """Verify ViewKind is the sole StrEnum defining view kind values.

    Scan Python files in ``src/semantics`` for StrEnum class definitions that
    contain member values overlapping with the canonical ViewKind values.
    The canonical definition in ``view_kinds.py`` is excluded.

    Returns:
        Violations for any file with overlapping StrEnum values.
    """
    violations: list[Violation] = []
    semantics_root = src_root / "semantics"
    search_root = semantics_root if semantics_root.exists() else src_root
    for path in sorted(search_root.rglob("*.py")):
        rel_path = path.relative_to(repo_root).as_posix()
        if rel_path == _VIEW_KIND_AUTHORITY_PATH:
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if not isinstance(node, ast.ClassDef):
                continue
            if not _is_strenum_class(node):
                continue
            overlap = _strenum_member_values(node) & _VIEW_KIND_VALUES
            if overlap:
                violations.append(
                    Violation(
                        path=rel_path,
                        line=node.lineno,
                        scope=node.name,
                        detail=(
                            f"StrEnum {node.name} has ViewKind-overlapping values: "
                            f"{sorted(overlap)}"
                        ),
                    )
                )
    return _sorted_violations(violations)


def _extract_dict_string_keys(tree: ast.Module, var_name: str) -> set[str]:
    """Extract string keys from a module-level dict assignment by variable name.

    Returns:
        Set of string keys found in the dict literal.
    """
    keys: set[str] = set()
    for node in ast.walk(tree):
        dict_value: ast.Dict | None = None
        if isinstance(node, ast.Assign):
            targets_match = any(isinstance(t, ast.Name) and t.id == var_name for t in node.targets)
            if targets_match and isinstance(node.value, ast.Dict):
                dict_value = node.value
        elif isinstance(node, ast.AnnAssign):
            target = node.target
            if (
                isinstance(target, ast.Name)
                and target.id == var_name
                and isinstance(node.value, ast.Dict)
            ):
                dict_value = node.value
        if dict_value is None:
            continue
        for key in dict_value.keys:
            if isinstance(key, ast.Constant) and isinstance(key.value, str):
                keys.add(key.value)
    return keys


def _check_builder_dispatch_coverage(
    repo_root: Path,
) -> tuple[Violation, ...]:
    """Verify semantic builder dispatch covers expected kind values.

    Supports both the legacy per-kind map (``_BUILDER_HANDLERS`` with 14 keys)
    and the consolidated dispatch map (``_CONSOLIDATED_BUILDER_HANDLERS`` with
    6 keys).

    Returns:
        Violations for missing dispatch coverage or absent dispatch map.
    """
    pipeline_path = repo_root / _BUILDER_DISPATCH_PATH
    if not pipeline_path.exists():
        return (
            Violation(
                path=_BUILDER_DISPATCH_PATH,
                line=0,
                scope="<module>",
                detail=f"{_BUILDER_DISPATCH_PATH} not found",
            ),
        )

    tree = ast.parse(pipeline_path.read_text(encoding="utf-8"))
    consolidated_keys = _extract_dict_string_keys(tree, _CONSOLIDATED_BUILDER_DISPATCH_VAR)
    if consolidated_keys:
        missing = sorted(_CONSOLIDATED_KIND_VALUES - consolidated_keys)
        violations = [
            Violation(
                path=_BUILDER_DISPATCH_PATH,
                line=0,
                scope=_CONSOLIDATED_BUILDER_DISPATCH_VAR,
                detail=(
                    f"Consolidated kind {kind!r} missing from {_CONSOLIDATED_BUILDER_DISPATCH_VAR}"
                ),
            )
            for kind in missing
        ]
        return _sorted_violations(violations)

    legacy_keys = _extract_dict_string_keys(tree, _LEGACY_BUILDER_DISPATCH_VAR)
    if legacy_keys:
        missing = sorted(_VIEW_KIND_VALUES - legacy_keys)
        violations = [
            Violation(
                path=_BUILDER_DISPATCH_PATH,
                line=0,
                scope=_LEGACY_BUILDER_DISPATCH_VAR,
                detail=f"ViewKind {kind!r} missing from {_LEGACY_BUILDER_DISPATCH_VAR}",
            )
            for kind in missing
        ]
        return _sorted_violations(violations)

    return (
        Violation(
            path=_BUILDER_DISPATCH_PATH,
            line=0,
            scope="<module>",
            detail=(
                f"Neither {_CONSOLIDATED_BUILDER_DISPATCH_VAR} nor "
                f"{_LEGACY_BUILDER_DISPATCH_VAR} found"
            ),
        ),
    )


def _check_entity_registry_derivation(
    repo_root: Path,
) -> tuple[Violation, ...]:
    """Verify SEMANTIC_TABLE_SPECS derives from entity_registry.

    Confirm that ``registry.py`` imports from ``entity_registry`` and
    uses ``generate_table_specs`` to produce the specs dict.

    Returns:
        Violations when the derivation pattern is not detected.
    """
    registry_path = repo_root / _ENTITY_REGISTRY_PATH
    if not registry_path.exists():
        return (
            Violation(
                path=_ENTITY_REGISTRY_PATH,
                line=0,
                scope="<module>",
                detail=f"{_ENTITY_REGISTRY_PATH} not found",
            ),
        )

    text = registry_path.read_text(encoding="utf-8")
    violations: list[Violation] = []

    if "generate_table_specs" not in text:
        violations.append(
            Violation(
                path=_ENTITY_REGISTRY_PATH,
                line=0,
                scope="<module>",
                detail=(
                    "registry.py does not reference generate_table_specs; "
                    "SEMANTIC_TABLE_SPECS may not derive from entity_registry"
                ),
            )
        )

    if "entity_registry" not in text:
        violations.append(
            Violation(
                path=_ENTITY_REGISTRY_PATH,
                line=0,
                scope="<module>",
                detail=(
                    "registry.py does not import from entity_registry; "
                    "SEMANTIC_TABLE_SPECS may use a static dict"
                ),
            )
        )

    return _sorted_violations(violations)


def _aggregate_visitor_violations(
    visitors: list[_DriftVisitor],
) -> dict[str, list[Violation]]:
    """Aggregate violation lists from all visitors into a single mapping.

    Returns:
        Mapping from violation category name to aggregated violation list.
    """
    result: dict[str, list[Violation]] = {
        "compile_context_instantiations": [],
        "dataset_bindings_fallback_calls": [],
        "untyped_record_artifact_strings": [],
        "untyped_record_artifact_inline_specs": [],
        "canonical_name_missing_manifest": [],
    }
    for visitor in visitors:
        result["compile_context_instantiations"].extend(visitor.compile_context_instantiations)
        result["dataset_bindings_fallback_calls"].extend(visitor.dataset_bindings_fallback_calls)
        result["untyped_record_artifact_strings"].extend(visitor.untyped_record_artifact_strings)
        result["untyped_record_artifact_inline_specs"].extend(
            visitor.untyped_record_artifact_inline_specs
        )
        result["canonical_name_missing_manifest"].extend(visitor.canonical_name_missing_manifest)
    return result


def _build_call_map(
    visitors: list[_DriftVisitor],
) -> dict[tuple[str, str], set[str]]:
    """Build call map from all visitors.

    Returns:
        Mapping from ``(path, scope)`` to set of call names.
    """
    call_map: dict[tuple[str, str], set[str]] = {}
    for visitor in visitors:
        for scope, names in visitor.calls_by_scope.items():
            call_map.setdefault((visitor.rel_path, scope), set()).update(names)
    return call_map


def run_audit(repo_root: Path) -> AuditReport:
    """Run drift audit and return a deterministic report.

    Returns:
    -------
    AuditReport
        Deterministic drift report for the requested repository root.
    """
    src_root = repo_root / "src"
    visitors: list[_DriftVisitor] = []
    for path in sorted(src_root.rglob("*.py")):
        rel_path = path.relative_to(repo_root).as_posix()
        tree = ast.parse(path.read_text(encoding="utf-8"))
        visitor = _DriftVisitor(rel_path)
        visitor.visit(tree)
        visitors.append(visitor)

    agg = _aggregate_visitor_violations(visitors)
    call_map = _build_call_map(visitors)

    missing_compile_tracking = _missing_scope_calls(
        call_map=call_map,
        required_scopes=_REQUIRED_COMPILE_TRACKING_SCOPES,
        required_call="compile_tracking",
        detail="missing compile_tracking(...) instrumentation",
    )
    missing_resolver_identity_tracking = _missing_scope_calls(
        call_map=call_map,
        required_scopes=_REQUIRED_RESOLVER_IDENTITY_TRACKING_SCOPES,
        required_call="resolver_identity_tracking",
        detail="missing resolver_identity_tracking(...) instrumentation",
    )
    missing_resolver_boundary_tracking = _missing_scope_calls(
        call_map=call_map,
        required_scopes=_REQUIRED_RESOLVER_BOUNDARY_SCOPES,
        required_call="record_resolver_if_tracking",
        detail="missing record_resolver_if_tracking(...) boundary instrumentation",
    )

    # Programmatic architecture drift checks.
    viewkind_authority_violations = _check_viewkind_sole_authority(src_root, repo_root)
    builder_dispatch_violations = _check_builder_dispatch_coverage(repo_root)
    entity_registry_violations = _check_entity_registry_derivation(repo_root)

    checks = (
        _check_result(
            check_id="compile_context_fallback.compile_context_instantiations",
            title="CompileContext(...) outside compile boundary",
            target=0,
            violations=_sorted_violations(agg["compile_context_instantiations"]),
        ),
        _check_result(
            check_id="compile_context_fallback.dataset_bindings_for_profile",
            title="dataset_bindings_for_profile(...) compatibility fallback usage",
            target=0,
            violations=_sorted_violations(agg["dataset_bindings_fallback_calls"]),
        ),
        _check_result(
            check_id="resolver_identity.compile_tracking_entrypoints",
            title="compile_tracking(...) entrypoint instrumentation",
            target=0,
            violations=missing_compile_tracking,
        ),
        _check_result(
            check_id="resolver_identity.resolver_identity_tracking_entrypoints",
            title="resolver_identity_tracking(...) entrypoint instrumentation",
            target=0,
            violations=missing_resolver_identity_tracking,
        ),
        _check_result(
            check_id="resolver_identity.boundary_recording",
            title="record_resolver_if_tracking(...) boundary instrumentation",
            target=0,
            violations=missing_resolver_boundary_tracking,
        ),
        _check_result(
            check_id="typed_artifacts.record_artifact_string_literal",
            title="record_artifact(...) string-literal names",
            target=0,
            violations=_sorted_violations(agg["untyped_record_artifact_strings"]),
        ),
        _check_result(
            check_id="typed_artifacts.record_artifact_inline_spec",
            title="record_artifact(...) inline ArtifactSpec(...) constructors",
            target=0,
            violations=_sorted_violations(agg["untyped_record_artifact_inline_specs"]),
        ),
        _check_result(
            check_id="manifest_naming.canonical_output_without_manifest",
            title="canonical_output_name(...) without manifest= outside allowlist",
            target=0,
            violations=_sorted_violations(agg["canonical_name_missing_manifest"]),
        ),
        _check_result(
            check_id="programmatic.viewkind_sole_authority",
            title="ViewKind sole authority (no competing StrEnum definitions)",
            target=0,
            violations=viewkind_authority_violations,
        ),
        _check_result(
            check_id="programmatic.builder_dispatch_coverage",
            title="_BUILDER_HANDLERS covers all ViewKind values",
            target=0,
            violations=builder_dispatch_violations,
        ),
        _check_result(
            check_id="programmatic.entity_registry_derivation",
            title="SEMANTIC_TABLE_SPECS derives from entity_registry",
            target=0,
            violations=entity_registry_violations,
        ),
    )

    return AuditReport(repo_root=str(repo_root), checks=checks)


def _render_human(report: AuditReport, *, strict: bool) -> str:
    lines: list[str] = []
    lines.append("=== Drift Surface Audit (CQ/AST) ===")
    lines.append(f"Source root: {Path(report.repo_root) / 'src'}")
    lines.append("")
    for index, check in enumerate(report.checks, start=1):
        level = "WARNING" if check.status == "warning" else "OK"
        lines.append(f"{index}. {check.title}")
        lines.append(f"   {level:<7} {check.id}: {check.count} (target: {check.target})")
        for violation in check.violations[:_MAX_VIOLATION_PREVIEW]:
            location = f"{violation.path}:{violation.line}" if violation.line else violation.path
            lines.append(f"      - {location} [{violation.scope}] {violation.detail}")
        if len(check.violations) > _MAX_VIOLATION_PREVIEW:
            lines.append(f"      ... {len(check.violations) - _MAX_VIOLATION_PREVIEW} more")
        lines.append("")

    lines.append("=== Summary ===")
    lines.append(f"Checks run: {report.total_checks}")
    lines.append(f"Warnings:   {report.warnings}")
    lines.append("")

    if strict and report.warnings > 0:
        lines.append(f"FAIL: {report.warnings} drift surface warning(s) detected in strict mode.")
    elif report.warnings > 0:
        lines.append(
            f"ADVISORY: {report.warnings} drift surface warning(s) detected (non-blocking)."
        )
    else:
        lines.append("PASS: All drift surfaces within target.")
    return "\n".join(lines)


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run AST-backed drift surface audit.")
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Exit non-zero when any check exceeds target.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit machine-readable JSON to stdout.",
    )
    parser.add_argument(
        "--root",
        type=Path,
        default=Path(__file__).resolve().parents[1],
        help="Repository root (default: parent of scripts/).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Run drift audit CLI.

    Returns:
    -------
    int
        Process exit code.
    """
    args = _parse_args(argv or sys.argv[1:])
    report = run_audit(args.root.resolve())
    output: str
    if args.json:
        output = json.dumps(report.to_dict(), indent=2, sort_keys=True)
    else:
        output = _render_human(report, strict=args.strict)
    sys.stdout.write(f"{output}\n")
    if args.strict and report.warnings > 0:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

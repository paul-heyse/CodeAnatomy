"""Bundle runner for cq report presets."""

# Bundle outputs rely on CqResult schema stability; follow schema evolution rules in core/schema.py.

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Literal, cast

from tools.cq.core.run_context import RunContext
from tools.cq.core.schema import (
    CqResult,
    DetailPayload,
    Finding,
    Section,
    mk_result,
    ms,
)
from tools.cq.macros.bytecode import BytecodeSurfaceRequest, cmd_bytecode_surface
from tools.cq.macros.calls import cmd_calls
from tools.cq.macros.exceptions import cmd_exceptions
from tools.cq.macros.impact import ImpactRequest, cmd_impact
from tools.cq.macros.imports import ImportRequest, cmd_imports
from tools.cq.macros.scopes import ScopeRequest, cmd_scopes
from tools.cq.macros.side_effects import SideEffectsRequest, cmd_side_effects
from tools.cq.macros.sig_impact import SigImpactRequest, cmd_sig_impact
from tools.cq.query.executor import execute_plan
from tools.cq.query.ir import Scope
from tools.cq.query.parser import parse_query
from tools.cq.query.planner import compile_query

if TYPE_CHECKING:
    from tools.cq.core.toolchain import Toolchain

TargetKind = Literal["function", "class", "method", "module", "path"]


@dataclass(frozen=True)
class TargetSpec:
    """Target specification for report bundles."""

    kind: TargetKind
    value: str


@dataclass(frozen=True)
class TargetScope:
    """Resolved target scope for filtering results."""

    files: frozenset[Path]
    dirs: frozenset[Path]

    def matches(self, file_path: Path) -> bool:
        """Return True if file_path is within scope.

        Returns
        -------
        bool
            True if the file is included by file or directory scope.
        """
        if file_path in self.files:
            return True
        return any(file_path.is_relative_to(directory) for directory in self.dirs)


@dataclass(frozen=True)
class BundleContext:
    """Context for running report bundles."""

    tc: Toolchain
    root: Path
    argv: list[str]
    target: TargetSpec
    in_dir: str | None = None
    param: str | None = None
    signature: str | None = None
    bytecode_show: str | None = None


@dataclass(frozen=True)
class BundleStepResult:
    """Result for a bundle step with scope filtering behavior."""

    result: CqResult
    apply_scope: bool = True


def parse_target_spec(value: str) -> TargetSpec:
    """Parse a target spec string like 'function:foo'.

    Returns
    -------
    TargetSpec
        Parsed target kind and value.

    Raises
    ------
    ValueError
        If the spec is missing a kind/value separator or is unsupported.
    """
    if ":" not in value:
        msg = "Target spec must be in the form kind:value"
        raise ValueError(msg)
    kind, target_value = value.split(":", maxsplit=1)
    kind = kind.strip().lower()
    target_value = target_value.strip()
    if kind not in {"function", "class", "method", "module", "path"}:
        msg = f"Unsupported target kind: {kind}"
        raise ValueError(msg)
    if not target_value:
        msg = "Target value cannot be empty"
        raise ValueError(msg)
    kind_literal = cast("TargetKind", kind)
    return TargetSpec(kind=kind_literal, value=target_value)


def run_bundle(preset: str, ctx: BundleContext) -> CqResult:
    """Run a report bundle preset.

    Returns
    -------
    CqResult
        Merged result for the selected bundle.
    """
    target_scope = resolve_target_scope(ctx)
    results: list[CqResult] = []

    for step in _bundle_steps(preset, ctx):
        result = step.result
        if step.apply_scope:
            result = filter_result_by_scope(result, ctx.root, target_scope)
        results.append(result)

    return merge_bundle_results(preset, ctx, results)


def resolve_target_scope(ctx: BundleContext) -> TargetScope:
    """Resolve target scope to file and directory sets.

    Returns
    -------
    TargetScope
        File and directory scope derived from the target.
    """
    root = ctx.root
    target = ctx.target

    if target.kind == "path":
        path = (root / target.value).resolve()
        if path.is_dir():
            return TargetScope(files=frozenset(), dirs=frozenset({path}))
        if path.is_file():
            return TargetScope(files=frozenset({path}), dirs=frozenset())
        return TargetScope(files=frozenset(), dirs=frozenset())

    if target.kind == "module":
        module_path = Path(target.value.replace(".", "/"))
        module_file = (root / module_path).with_suffix(".py")
        package_init = root / module_path / "__init__.py"
        files = {path for path in (module_file, package_init) if path.exists()}
        dirs = {root / module_path} if (root / module_path).is_dir() else set()
        return TargetScope(files=frozenset(files), dirs=frozenset(dirs))

    query = parse_query(f"entity={target.kind} name={target.value}")
    if ctx.in_dir:
        query = query.with_scope(Scope(in_dir=ctx.in_dir))
    plan = compile_query(query)
    result = execute_plan(
        plan,
        query,
        ctx.tc,
        root,
        argv=ctx.argv,
    )
    files = set()
    for finding in result.key_findings:
        if finding.anchor is None:
            continue
        files.add(root / finding.anchor.file)
    return TargetScope(files=frozenset(files), dirs=frozenset())


def filter_result_by_scope(
    result: CqResult,
    root: Path,
    scope: TargetScope,
) -> CqResult:
    """Filter findings in a result to the target scope.

    Returns
    -------
    CqResult
        Result containing findings within the target scope.
    """

    def _in_scope(finding: Finding) -> bool:
        if finding.anchor is None:
            return True
        file_path = (root / finding.anchor.file).resolve()
        return scope.matches(file_path)

    key_findings = [f for f in result.key_findings if _in_scope(f)]
    evidence = [f for f in result.evidence if _in_scope(f)]
    sections: list[Section] = []
    for section in result.sections:
        section_findings = [f for f in section.findings if _in_scope(f)]
        if not section_findings:
            continue
        sections.append(
            Section(
                title=section.title,
                findings=section_findings,
                collapsed=section.collapsed,
            )
        )
    return CqResult(
        run=result.run,
        summary=result.summary,
        key_findings=key_findings,
        evidence=evidence,
        sections=sections,
        artifacts=result.artifacts,
    )


def merge_bundle_results(preset: str, ctx: BundleContext, results: list[CqResult]) -> CqResult:
    """Merge macro results into a single bundle report.

    Returns
    -------
    CqResult
        Combined bundle report result.
    """
    started = ms()
    run_ctx = RunContext.from_parts(
        root=ctx.root,
        argv=ctx.argv,
        tc=ctx.tc,
        started_ms=started,
    )
    run = run_ctx.to_runmeta(f"report:{preset}")
    merged = mk_result(run)

    merged.summary = {
        "bundle": preset,
        "target": f"{ctx.target.kind}:{ctx.target.value}",
        "in_dir": ctx.in_dir,
        "steps": [result.run.macro for result in results],
        "macro_summaries": {result.run.macro: result.summary for result in results},
    }

    for result in results:
        macro = result.run.macro
        for finding in result.key_findings:
            merged.key_findings.append(_clone_finding_with_macro(finding, macro))
        for evidence in result.evidence:
            merged.evidence.append(_clone_finding_with_macro(evidence, macro))
        for section in result.sections:
            merged.sections.append(
                Section(
                    title=f"{macro}: {section.title}",
                    findings=[_clone_finding_with_macro(f, macro) for f in section.findings],
                    collapsed=section.collapsed,
                )
            )
        merged.artifacts.extend(result.artifacts)

    return merged


def _clone_finding_with_macro(finding: Finding, macro: str) -> Finding:
    details = finding.details
    data = dict(details.data)
    data["source_macro"] = macro
    details = DetailPayload(kind=details.kind, score=details.score, data=data)
    return Finding(
        category=finding.category,
        message=finding.message,
        anchor=finding.anchor,
        severity=finding.severity,
        details=details,
    )


def _bundle_steps(preset: str, ctx: BundleContext) -> list[BundleStepResult]:
    preset = preset.lower()
    if preset == "refactor-impact":
        return _run_refactor_impact(ctx)
    if preset == "safety-reliability":
        return _run_safety_reliability(ctx)
    if preset == "change-propagation":
        return _run_change_propagation(ctx)
    if preset == "dependency-health":
        return _run_dependency_health(ctx)
    msg = f"Unknown report preset: {preset}"
    raise ValueError(msg)


def _run_refactor_impact(ctx: BundleContext) -> list[BundleStepResult]:
    results: list[BundleStepResult] = []
    target = ctx.target

    if target.kind in {"function", "method"}:
        results.append(
            BundleStepResult(
                result=cmd_calls(ctx.tc, ctx.root, ctx.argv, target.value),
                apply_scope=False,
            )
        )
        if ctx.param:
            results.append(
                BundleStepResult(
                    result=cmd_impact(
                        ImpactRequest(
                            tc=ctx.tc,
                            root=ctx.root,
                            argv=ctx.argv,
                            function_name=target.value,
                            param_name=ctx.param,
                        )
                    ),
                    apply_scope=False,
                )
            )
        else:
            results.append(BundleStepResult(result=_skip_result(ctx, "impact", "missing --param")))
        if ctx.signature:
            results.append(
                BundleStepResult(
                    result=cmd_sig_impact(
                        SigImpactRequest(
                            tc=ctx.tc,
                            root=ctx.root,
                            argv=ctx.argv,
                            symbol=target.value,
                            to=ctx.signature,
                        )
                    ),
                    apply_scope=False,
                )
            )
        else:
            results.append(BundleStepResult(result=_skip_result(ctx, "sig-impact", "missing --to")))
    else:
        results.append(
            BundleStepResult(result=_skip_result(ctx, "calls", "requires function target"))
        )
        results.append(
            BundleStepResult(result=_skip_result(ctx, "impact", "requires function target"))
        )
        results.append(
            BundleStepResult(result=_skip_result(ctx, "sig-impact", "requires function target"))
        )

    module_filter = target.value if target.kind == "module" else None
    results.append(
        BundleStepResult(
            result=cmd_imports(
                ImportRequest(
                    tc=ctx.tc,
                    root=ctx.root,
                    argv=ctx.argv,
                    module=module_filter,
                )
            )
        )
    )
    function_filter = target.value if target.kind in {"function", "method"} else None
    results.append(
        BundleStepResult(
            result=cmd_exceptions(ctx.tc, ctx.root, ctx.argv, function=function_filter)
        )
    )
    results.append(
        BundleStepResult(
            result=cmd_side_effects(SideEffectsRequest(tc=ctx.tc, root=ctx.root, argv=ctx.argv))
        )
    )
    return results


def _run_safety_reliability(ctx: BundleContext) -> list[BundleStepResult]:
    results: list[BundleStepResult] = []
    function_filter = ctx.target.value if ctx.target.kind in {"function", "method"} else None
    results.append(
        BundleStepResult(
            result=cmd_exceptions(ctx.tc, ctx.root, ctx.argv, function=function_filter)
        )
    )
    results.append(
        BundleStepResult(
            result=cmd_side_effects(SideEffectsRequest(tc=ctx.tc, root=ctx.root, argv=ctx.argv))
        )
    )
    return results


def _run_change_propagation(ctx: BundleContext) -> list[BundleStepResult]:
    results: list[BundleStepResult] = []
    target = ctx.target

    if target.kind in {"function", "method"}:
        if ctx.param:
            results.append(
                BundleStepResult(
                    result=cmd_impact(
                        ImpactRequest(
                            tc=ctx.tc,
                            root=ctx.root,
                            argv=ctx.argv,
                            function_name=target.value,
                            param_name=ctx.param,
                        )
                    ),
                    apply_scope=False,
                )
            )
        else:
            results.append(BundleStepResult(result=_skip_result(ctx, "impact", "missing --param")))
        results.append(
            BundleStepResult(
                result=cmd_calls(ctx.tc, ctx.root, ctx.argv, target.value),
                apply_scope=False,
            )
        )
    else:
        results.append(
            BundleStepResult(result=_skip_result(ctx, "impact", "requires function target"))
        )
        results.append(
            BundleStepResult(result=_skip_result(ctx, "calls", "requires function target"))
        )

    results.append(
        BundleStepResult(
            result=cmd_bytecode_surface(
                BytecodeSurfaceRequest(
                    tc=ctx.tc,
                    root=ctx.root,
                    argv=ctx.argv,
                    target=target.value,
                    show=ctx.bytecode_show or "globals,attrs,constants",
                )
            ),
            apply_scope=False,
        )
    )
    results.append(
        BundleStepResult(
            result=cmd_scopes(
                ScopeRequest(
                    tc=ctx.tc,
                    root=ctx.root,
                    argv=ctx.argv,
                    target=target.value,
                )
            )
        )
    )
    return results


def _run_dependency_health(ctx: BundleContext) -> list[BundleStepResult]:
    results: list[BundleStepResult] = []
    module_filter = ctx.target.value if ctx.target.kind == "module" else None
    results.append(
        BundleStepResult(
            result=cmd_imports(
                ImportRequest(
                    tc=ctx.tc,
                    root=ctx.root,
                    argv=ctx.argv,
                    cycles=True,
                    module=module_filter,
                )
            )
        )
    )
    results.append(
        BundleStepResult(
            result=cmd_side_effects(SideEffectsRequest(tc=ctx.tc, root=ctx.root, argv=ctx.argv))
        )
    )
    results.append(
        BundleStepResult(
            result=cmd_scopes(
                ScopeRequest(
                    tc=ctx.tc,
                    root=ctx.root,
                    argv=ctx.argv,
                    target=ctx.target.value,
                )
            )
        )
    )
    return results


def _skip_result(ctx: BundleContext, macro: str, reason: str) -> CqResult:
    started = ms()
    run_ctx = RunContext.from_parts(
        root=ctx.root,
        argv=ctx.argv,
        tc=ctx.tc,
        started_ms=started,
    )
    run = run_ctx.to_runmeta(macro)
    result = mk_result(run)
    result.summary["skipped"] = reason
    return result

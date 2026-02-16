"""Bundle runner for cq report presets."""

# Bundle outputs rely on CqResult schema stability; follow schema evolution rules in core/schema.py.

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from tools.cq.core.bootstrap import resolve_runtime_services
from tools.cq.core.merge import merge_step_results
from tools.cq.core.run_context import RunContext
from tools.cq.core.schema import CqResult, Finding, Section, mk_result, ms
from tools.cq.core.target_specs import BundleTargetKind, TargetSpecV1
from tools.cq.macros.bytecode import cmd_bytecode_surface
from tools.cq.macros.exceptions import cmd_exceptions
from tools.cq.macros.impact import cmd_impact
from tools.cq.macros.imports import cmd_imports
from tools.cq.macros.scopes import cmd_scopes
from tools.cq.macros.side_effects import cmd_side_effects
from tools.cq.macros.sig_impact import cmd_sig_impact
from tools.cq.orchestration.request_factory import RequestContextV1, RequestFactory
from tools.cq.query.executor import ExecutePlanRequestV1, execute_plan
from tools.cq.query.ir import Scope
from tools.cq.query.parser import parse_query
from tools.cq.query.planner import compile_query

if TYPE_CHECKING:
    from tools.cq.core.toolchain import Toolchain


@dataclass(frozen=True)
class TargetScope:
    """Resolved target scope for filtering results."""

    files: frozenset[Path]
    dirs: frozenset[Path]

    def matches(self, file_path: Path) -> bool:
        """Return True if file_path is within scope.

        Returns:
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
    target: TargetSpecV1
    in_dir: str | None = None
    param: str | None = None
    signature: str | None = None
    bytecode_show: str | None = None


def _bundle_target_kind(target: TargetSpecV1) -> BundleTargetKind:
    kind = target.bundle_kind
    if kind is None:
        msg = "Bundle target kind is required."
        raise ValueError(msg)
    return kind


def _bundle_target_value(target: TargetSpecV1) -> str:
    value = target.bundle_value
    if value is None:
        msg = "Bundle target value is required."
        raise ValueError(msg)
    return value


@dataclass(frozen=True)
class BundleStepResult:
    """Result for a bundle step with scope filtering behavior."""

    result: CqResult
    apply_scope: bool = True


def run_bundle(preset: str, ctx: BundleContext) -> CqResult:
    """Run a report bundle preset.

    Returns:
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

    Returns:
    -------
    TargetScope
        File and directory scope derived from the target.
    """
    root = ctx.root
    target = ctx.target
    target_kind = _bundle_target_kind(target)
    target_value = _bundle_target_value(target)

    if target_kind == "path":
        path = (root / target_value).resolve()
        if path.is_dir():
            return TargetScope(files=frozenset(), dirs=frozenset({path}))
        if path.is_file():
            return TargetScope(files=frozenset({path}), dirs=frozenset())
        return TargetScope(files=frozenset(), dirs=frozenset())

    if target_kind == "module":
        module_path = Path(target_value.replace(".", "/"))
        module_file = (root / module_path).with_suffix(".py")
        package_init = root / module_path / "__init__.py"
        files = {path for path in (module_file, package_init) if path.exists()}
        dirs = {root / module_path} if (root / module_path).is_dir() else set()
        return TargetScope(files=frozenset(files), dirs=frozenset(dirs))

    query = parse_query(f"entity={target_kind} name={target_value}")
    if ctx.in_dir:
        query = query.with_scope(Scope(in_dir=ctx.in_dir))
    plan = compile_query(query)
    result = execute_plan(
        ExecutePlanRequestV1(
            plan=plan,
            query=query,
            root=str(root),
            argv=tuple(ctx.argv),
        ),
        tc=ctx.tc,
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

    Returns:
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

    Returns:
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
        "target": f"{_bundle_target_kind(ctx.target)}:{_bundle_target_value(ctx.target)}",
        "in_dir": ctx.in_dir,
    }

    for result in results:
        macro = result.run.macro
        merge_step_results(merged, macro, result)

    merged.summary["macro_summaries"] = merged.summary.get("step_summaries", {})

    return merged


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
    target_kind = _bundle_target_kind(target)
    target_value = _bundle_target_value(target)
    request_ctx = RequestContextV1(root=ctx.root, argv=ctx.argv, tc=ctx.tc)
    services = resolve_runtime_services(ctx.root)

    if target_kind in {"function", "method"}:
        request = RequestFactory.calls(request_ctx, function_name=target_value)
        results.append(
            BundleStepResult(
                result=services.calls.execute(request),
                apply_scope=False,
            )
        )
        if ctx.param:
            request = RequestFactory.impact(
                request_ctx,
                function_name=target_value,
                param_name=ctx.param,
            )
            results.append(
                BundleStepResult(
                    result=cmd_impact(request),
                    apply_scope=False,
                )
            )
        else:
            results.append(BundleStepResult(result=_skip_result(ctx, "impact", "missing --param")))
        if ctx.signature:
            request = RequestFactory.sig_impact(
                request_ctx,
                symbol=target_value,
                to=ctx.signature,
            )
            results.append(
                BundleStepResult(
                    result=cmd_sig_impact(request),
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

    module_filter = target_value if target_kind == "module" else None
    request = RequestFactory.imports_cmd(request_ctx, module=module_filter)
    results.append(BundleStepResult(result=cmd_imports(request)))

    function_filter = target_value if target_kind in {"function", "method"} else None
    request = RequestFactory.exceptions(request_ctx, function=function_filter)
    results.append(BundleStepResult(result=cmd_exceptions(request)))

    request = RequestFactory.side_effects(request_ctx)
    results.append(BundleStepResult(result=cmd_side_effects(request)))

    return results


def _run_safety_reliability(ctx: BundleContext) -> list[BundleStepResult]:
    results: list[BundleStepResult] = []
    request_ctx = RequestContextV1(root=ctx.root, argv=ctx.argv, tc=ctx.tc)
    target_kind = _bundle_target_kind(ctx.target)
    target_value = _bundle_target_value(ctx.target)
    function_filter = target_value if target_kind in {"function", "method"} else None

    request = RequestFactory.exceptions(request_ctx, function=function_filter)
    results.append(BundleStepResult(result=cmd_exceptions(request)))

    request = RequestFactory.side_effects(request_ctx)
    results.append(BundleStepResult(result=cmd_side_effects(request)))

    return results


def _run_change_propagation(ctx: BundleContext) -> list[BundleStepResult]:
    results: list[BundleStepResult] = []
    target = ctx.target
    target_kind = _bundle_target_kind(target)
    target_value = _bundle_target_value(target)
    request_ctx = RequestContextV1(root=ctx.root, argv=ctx.argv, tc=ctx.tc)
    services = resolve_runtime_services(ctx.root)

    if target_kind in {"function", "method"}:
        if ctx.param:
            request = RequestFactory.impact(
                request_ctx,
                function_name=target_value,
                param_name=ctx.param,
            )
            results.append(
                BundleStepResult(
                    result=cmd_impact(request),
                    apply_scope=False,
                )
            )
        else:
            results.append(BundleStepResult(result=_skip_result(ctx, "impact", "missing --param")))

        request = RequestFactory.calls(request_ctx, function_name=target_value)
        results.append(
            BundleStepResult(
                result=services.calls.execute(request),
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

    request = RequestFactory.bytecode_surface(
        request_ctx,
        target=target_value,
        show=ctx.bytecode_show or "globals,attrs,constants",
    )
    results.append(
        BundleStepResult(
            result=cmd_bytecode_surface(request),
            apply_scope=False,
        )
    )

    request = RequestFactory.scopes(request_ctx, target=target_value)
    results.append(BundleStepResult(result=cmd_scopes(request)))

    return results


def _run_dependency_health(ctx: BundleContext) -> list[BundleStepResult]:
    results: list[BundleStepResult] = []
    request_ctx = RequestContextV1(root=ctx.root, argv=ctx.argv, tc=ctx.tc)
    target_kind = _bundle_target_kind(ctx.target)
    target_value = _bundle_target_value(ctx.target)
    module_filter = target_value if target_kind == "module" else None

    request = RequestFactory.imports_cmd(request_ctx, cycles=True, module=module_filter)
    results.append(BundleStepResult(result=cmd_imports(request)))

    request = RequestFactory.side_effects(request_ctx)
    results.append(BundleStepResult(result=cmd_side_effects(request)))

    request = RequestFactory.scopes(request_ctx, target=target_value)
    results.append(BundleStepResult(result=cmd_scopes(request)))

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

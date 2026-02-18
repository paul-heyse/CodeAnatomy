"""Impact macro - approximate taint/dependency flow analysis.

Traces how data flows from a specific parameter through the codebase,
identifying downstream consumers and potential impacts of changes.
"""

from __future__ import annotations

from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path

import msgspec

from tools.cq.analysis.taint import (
    TaintCallSite,
    TaintedSite,
    analyze_function_node,
    find_function_node,
)
from tools.cq.core.contracts import require_mapping as require_contract_mapping
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
from tools.cq.core.summary_update_contracts import ImpactSummaryUpdateV1
from tools.cq.index.arg_binder import bind_call_to_params, tainted_params_from_bound_call
from tools.cq.index.call_resolver import CallInfo, resolve_call_targets
from tools.cq.index.def_index import DefIndex, FnDecl
from tools.cq.macros.contracts import MacroRequestBase, ScoringDetailsV1
from tools.cq.macros.result_builder import MacroResultBuilder
from tools.cq.macros.rust_fallback_policy import RustFallbackPolicyV1, apply_rust_fallback_policy
from tools.cq.macros.shared import macro_scoring_details
from tools.cq.search._shared.types import SearchLimits
from tools.cq.search.pipeline.profiles import INTERACTIVE
from tools.cq.search.rg.adapter import find_callers

_DEFAULT_MAX_DEPTH = 5
_SECTION_SITE_LIMIT = 50
_CALLER_LIMIT = 30


class TaintState(msgspec.Struct):
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

    tainted_vars: set[str] = msgspec.field(default_factory=set)
    tainted_sites: list[TaintedSite] = msgspec.field(default_factory=list)
    visited: set[str] = msgspec.field(default_factory=set)

    def has_visited(self, key: str) -> bool:
        """Return whether one function key has already been analyzed."""
        return key in self.visited

    def record_visit(self, key: str) -> None:
        """Record one analyzed function key."""
        self.visited.add(key)

    def add_sites(self, sites: list[TaintedSite]) -> None:
        """Append taint sites collected during one function visit."""
        self.tainted_sites.extend(sites)

    def mark_tainted(self, symbol: str) -> None:
        """Record a tainted symbol name."""
        self.tainted_vars.add(symbol)


class ImpactRequest(MacroRequestBase, frozen=True):
    """Inputs required for the impact macro."""

    function_name: str
    param_name: str
    max_depth: int = _DEFAULT_MAX_DEPTH


@dataclass(frozen=True)
class ImpactContext:
    """Execution context for impact analysis."""

    request: ImpactRequest
    index: DefIndex
    functions: list[FnDecl]


@dataclass(frozen=True)
class ImpactDepthSummary:
    """Summary payload for depth findings."""

    request: ImpactRequest
    depth_counts: dict[int, int]
    files_affected: set[str]
    site_count: int
    scoring_details: ScoringDetailsV1


def _to_call_info(callsite: TaintCallSite) -> CallInfo:
    return CallInfo(
        file=callsite.file,
        line=callsite.line,
        col=callsite.col,
        callee_name=callsite.callee_name,
        args=callsite.args,
        keywords=callsite.keywords,
        is_method_call=callsite.is_method_call,
        receiver_name=callsite.receiver_name,
    )


class _AnalyzeContext(msgspec.Struct, frozen=True):
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
    if context.state.has_visited(key):
        return
    context.state.record_visit(key)

    # Read source
    filepath = context.root / fn.file
    if not filepath.exists():
        return

    try:
        source = filepath.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError):
        return

    # Find function AST
    fn_node = find_function_node(
        source,
        function_name=fn.name,
        function_line=fn.line,
        class_name=fn.class_name,
    )
    if fn_node is None:
        return

    # Run pure taint analysis
    analysis = analyze_function_node(
        file=fn.file,
        function_node=fn_node,
        tainted_params=tainted_params,
        depth=current_depth,
    )
    context.state.add_sites(analysis.sites)

    # Propagate to callees
    for callsite, tainted_args in analysis.calls:
        call_info = _to_call_info(callsite)
        resolved = resolve_call_targets(context.index, call_info)
        if resolved.targets:
            for target in resolved.targets:
                # Bind args to params
                bound = bind_call_to_params(
                    call_info.args,
                    call_info.keywords,
                    target,
                )
                tainted_indices = {arg for arg in tainted_args if isinstance(arg, int)}
                tainted_values = {arg for arg in tainted_args if isinstance(arg, str)}
                new_tainted = tainted_params_from_bound_call(bound, tainted_indices)
                new_tainted |= tainted_params_from_bound_call(bound, tainted_values)
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

    Returns:
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


def _append_depth_findings(builder: MacroResultBuilder, summary: ImpactDepthSummary) -> None:
    if not summary.site_count:
        return
    request = summary.request
    scoring_details = summary.scoring_details
    builder.add_finding(
        Finding(
            category="summary",
            message=(
                f"Taint from {request.function_name}.{request.param_name} "
                f"reaches {summary.site_count} sites in {len(summary.files_affected)} files"
            ),
            severity="info",
            details=build_detail_payload(scoring=scoring_details),
        ),
    )
    for depth, count in sorted(summary.depth_counts.items()):
        builder.add_finding(
            Finding(
                category="depth",
                message=f"Depth {depth}: {count} taint sites",
                severity="info",
                details=build_detail_payload(scoring=scoring_details),
            ),
        )


def _group_sites_by_kind(all_sites: list[TaintedSite]) -> dict[str, list[TaintedSite]]:
    by_kind: dict[str, list[TaintedSite]] = {}
    for site in all_sites:
        by_kind.setdefault(site.kind, []).append(site)
    return by_kind


def _append_kind_sections(
    builder: MacroResultBuilder,
    by_kind: dict[str, list[TaintedSite]],
    scoring_details: ScoringDetailsV1,
) -> None:
    for kind, sites in by_kind.items():
        section = Section(title=f"Taint {kind.title()} Sites")
        for site in sites[:_SECTION_SITE_LIMIT]:
            details = {"depth": site.depth, "param": site.param}
            section = append_section_finding(
                section,
                Finding(
                    category=kind,
                    message=site.description,
                    anchor=Anchor(file=site.file, line=site.line),
                    severity="info",
                    details=build_detail_payload(scoring=scoring_details, data=details),
                ),
            )
        builder.add_section(section)


def _append_callers_section(
    builder: MacroResultBuilder,
    caller_sites: list[tuple[str, int]],
    scoring_details: ScoringDetailsV1,
) -> None:
    if not caller_sites:
        return
    caller_section = Section(title="Callers (via rg)")
    for file, line in caller_sites[:_CALLER_LIMIT]:
        caller_section = append_section_finding(
            caller_section,
            Finding(
                category="caller",
                message="Potential call site",
                anchor=Anchor(file=file, line=line),
                severity="info",
                details=build_detail_payload(scoring=scoring_details),
            ),
        )
    builder.add_section(caller_section)


def _append_evidence(
    builder: MacroResultBuilder,
    all_sites: list[TaintedSite],
    scoring_details: ScoringDetailsV1,
) -> None:
    seen: set[tuple[str, int]] = set()
    for site in all_sites:
        key = (site.file, site.line)
        if key in seen:
            continue
        seen.add(key)
        details = {"depth": site.depth}
        builder.add_evidence(
            Finding(
                category=site.kind,
                message=site.description,
                anchor=Anchor(file=site.file, line=site.line),
                details=build_detail_payload(scoring=scoring_details, data=details),
            ),
        )


def _resolve_functions(index: DefIndex, function_name: str) -> list[FnDecl]:
    functions = index.find_function_by_name(function_name)
    if not functions:
        functions = index.find_function_by_qualified_name(function_name)
    return functions


def _build_not_found_result(request: ImpactRequest, *, started_ms: float) -> CqResult:
    builder = MacroResultBuilder(
        "impact",
        root=request.root,
        argv=request.argv,
        tc=request.tc,
        started_ms=started_ms,
    )
    builder.with_summary(
        summary_from_mapping(
            {
                "mode": "impact",
                "query": request.function_name,
                "status": "not_found",
                "function": request.function_name,
            }
        )
    )
    builder.add_finding(
        Finding(
            category="error",
            message=f"Function '{request.function_name}' not found in index",
            severity="error",
        ),
    )
    return builder.build()


def _analyze_functions(ctx: ImpactContext) -> list[TaintedSite]:
    all_sites: list[TaintedSite] = []
    request = ctx.request
    for fn in ctx.functions:
        param_names = [p.name for p in fn.params]
        if request.param_name not in param_names:
            continue

        state = TaintState()
        analyze_context = _AnalyzeContext(
            index=ctx.index,
            root=request.root,
            state=state,
            max_depth=request.max_depth,
        )
        _analyze_function(fn, {request.param_name}, analyze_context)
        all_sites.extend(state.tainted_sites)
    return all_sites


def _append_missing_param_warnings(
    builder: MacroResultBuilder,
    functions: list[FnDecl],
    *,
    request: ImpactRequest,
) -> None:
    for fn in functions:
        param_names = [p.name for p in fn.params]
        if request.param_name not in param_names:
            builder.add_finding(
                Finding(
                    category="warning",
                    message=f"Parameter '{request.param_name}' not found in {fn.qualified_name}",
                    anchor=Anchor(file=fn.file, line=fn.line),
                    severity="warning",
                ),
            )


def _build_impact_scoring(
    all_sites: list[TaintedSite],
) -> tuple[ScoringDetailsV1, dict[int, int], set[str]]:
    depth_counts, files_affected = _collect_depth_stats(all_sites)
    max_depth = max(depth_counts.keys()) if depth_counts else 0
    evidence_kind = "resolved_ast" if max_depth == 0 else "cross_file_taint"
    scoring_details = macro_scoring_details(
        sites=len(all_sites),
        files=len(files_affected),
        depth=max_depth,
        evidence_kind=evidence_kind,
    )
    return scoring_details, depth_counts, files_affected


def _build_impact_summary(
    request: ImpactRequest,
    *,
    functions: list[FnDecl],
    all_sites: list[TaintedSite],
    caller_sites: list[tuple[str, int]],
) -> ImpactSummaryUpdateV1:
    return ImpactSummaryUpdateV1(
        query=f"{request.function_name} --param {request.param_name}",
        mode="impact",
        function=request.function_name,
        parameter=request.param_name,
        taint_sites=len(all_sites),
        max_depth=request.max_depth,
        functions_analyzed=len(functions),
        callers_found=len(caller_sites),
    )


def _build_impact_result(
    ctx: ImpactContext,
    *,
    started_ms: float,
) -> CqResult:
    request = ctx.request
    if not ctx.functions:
        return _build_not_found_result(request, started_ms=started_ms)

    builder = MacroResultBuilder(
        "impact",
        root=request.root,
        argv=request.argv,
        tc=request.tc,
        started_ms=started_ms,
    )
    _append_missing_param_warnings(builder, ctx.functions, request=request)
    all_sites = _analyze_functions(ctx)
    caller_sites = _find_callers_via_search(request.function_name, request.root)

    summary_mapping = msgspec.to_builtins(
        _build_impact_summary(
            request,
            functions=ctx.functions,
            all_sites=all_sites,
            caller_sites=caller_sites,
        ),
        order="deterministic",
    )
    try:
        summary_mapping_dict = require_contract_mapping(summary_mapping)
    except TypeError:
        summary_mapping_dict: dict[str, object] = {}
    builder.with_summary(summary_from_mapping(summary_mapping_dict))

    scoring_details, depth_counts, files_affected = _build_impact_scoring(all_sites)
    _append_depth_findings(
        builder,
        ImpactDepthSummary(
            request=request,
            depth_counts=depth_counts,
            files_affected=files_affected,
            site_count=len(all_sites),
            scoring_details=scoring_details,
        ),
    )
    by_kind = _group_sites_by_kind(all_sites)
    _append_kind_sections(builder, by_kind, scoring_details)
    _append_callers_section(builder, caller_sites, scoring_details)
    _append_evidence(builder, all_sites, scoring_details)
    return builder.build()


def cmd_impact(request: ImpactRequest) -> CqResult:
    """Analyze impact/taint flow from a function parameter.

    Parameters
    ----------
    request : ImpactRequest
        Impact analysis request payload.

    Returns:
    -------
    CqResult
        Analysis result.
    """
    started = ms()
    index = DefIndex.build(request.root)
    functions = _resolve_functions(index, request.function_name)
    ctx = ImpactContext(request=request, index=index, functions=functions)
    result = _build_impact_result(ctx, started_ms=started)
    return apply_rust_fallback_policy(
        result,
        root=request.root,
        policy=RustFallbackPolicyV1(
            macro_name="impact",
            pattern=request.function_name,
            query=request.function_name,
        ),
    )


__all__ = ["ImpactRequest", "cmd_impact"]

"""Scan/runtime ownership for calls analysis site collection."""

from __future__ import annotations

import ast
from collections import Counter
from pathlib import Path
from typing import TYPE_CHECKING

import msgspec

from tools.cq.core.runtime.worker_scheduler import get_worker_scheduler
from tools.cq.macros.calls.context_snippet import _extract_context_snippet
from tools.cq.macros.calls.enrichment import enrich_call_site as enrich_call_site_module
from tools.cq.macros.calls.neighborhood import _compute_context_window
from tools.cq.macros.calls.rust_calls import (
    collect_rust_call_sites as collect_rust_call_sites_module,
)
from tools.cq.query.sg_parser import SgRecord, group_records_by_file
from tools.cq.search.rg.adapter import find_def_lines

if TYPE_CHECKING:
    from tools.cq.macros.calls.analysis import CallSite, CallSiteBuildContext


def collect_call_sites(
    root_path: Path,
    by_file: dict[Path, list[int]],
    target_name: str,
) -> list[CallSite]:
    """Collect callsite rows from grouped candidate line matches.

    Returns:
        list[CallSite]: Collected call-site rows.
    """
    filepaths = list(by_file)
    if len(filepaths) <= 1:
        return _collect_call_sites_sequential(root_path, filepaths, target_name)
    scheduler = get_worker_scheduler()
    workers = max(1, int(scheduler.policy.calls_file_workers))
    if workers <= 1:
        return _collect_call_sites_sequential(root_path, filepaths, target_name)
    futures = [
        scheduler.submit_io(_collect_call_sites_for_file, root_path, filepath, target_name)
        for filepath in filepaths
    ]
    batch = scheduler.collect_bounded(
        futures,
        timeout_seconds=max(1.0, float(len(filepaths)) * 10.0),
    )
    if batch.timed_out > 0:
        return _collect_call_sites_sequential(root_path, filepaths, target_name)
    all_sites: list[CallSite] = []
    for per_file_sites in batch.done:
        all_sites.extend(per_file_sites)
    return all_sites


def collect_call_sites_from_records(
    root: Path,
    call_records: list[SgRecord],
    target_name: str,
) -> tuple[list[CallSite], int]:
    """Collect call-site rows from ast-grep records.

    Returns:
        tuple[list[CallSite], int]: Collected call sites and distinct file count.
    """
    from tools.cq.macros.calls import analysis as analysis_module

    by_file = group_records_by_file(call_records)
    all_sites: list[CallSite] = []
    for rel_path, file_records in by_file.items():
        if Path(rel_path).suffix == ".rs":
            all_sites.extend(
                collect_rust_call_sites_module(
                    rel_path,
                    file_records,
                    target_name,
                    make_call_site=analysis_module.CallSite,
                )
            )
            continue
        filepath = root / rel_path
        if not filepath.exists():
            continue
        try:
            source = filepath.read_text(encoding="utf-8")
            tree = ast.parse(source, filename=rel_path)
        except (SyntaxError, OSError, UnicodeDecodeError, ValueError):
            continue
        source_lines = source.splitlines()
        ctx = analysis_module.CallSiteBuildContext(
            root=root,
            rel_path=rel_path,
            source=source,
            source_lines=source_lines,
            def_lines=find_def_lines(filepath),
            total_lines=len(source_lines),
            tree=tree,
            call_index=analysis_module.build_call_index(tree),
            function_name=target_name,
        )
        for record in file_records:
            site = _build_call_site_from_record(ctx, record)
            if site is not None:
                all_sites.append(site)
    files_with_calls = len({site.file for site in all_sites})
    return all_sites, files_with_calls


def analyze_sites(
    sites: list[CallSite],
) -> tuple[
    Counter[str],
    Counter[str],
    int,
    Counter[str],
    Counter[str],
]:
    """Analyze argument shapes, forwarding, contexts, and hazards for call sites.

    Returns:
        tuple[Counter[str], Counter[str], int, Counter[str], Counter[str]]: Argument
            shapes, kwarg usage, forwarding count, contexts, and hazard counts.
    """
    arg_shapes: Counter[str] = Counter()
    kwarg_usage: Counter[str] = Counter()
    forwarding_count = 0
    contexts: Counter[str] = Counter()
    hazard_counts: Counter[str] = Counter()
    for site in sites:
        shape = f"args={site.num_args}, kwargs={site.num_kwargs}"
        if site.has_star_args:
            shape += ", *"
        if site.has_star_kwargs:
            shape += ", **"
        arg_shapes[shape] += 1
        for kw in site.kwargs:
            kwarg_usage[kw] += 1
        if site.has_star_args or site.has_star_kwargs:
            forwarding_count += 1
        contexts[site.context] += 1
        for hazard in site.hazards:
            hazard_counts[hazard] += 1
    return (
        arg_shapes,
        kwarg_usage,
        forwarding_count,
        contexts,
        hazard_counts,
    )


def _collect_call_sites_sequential(
    root: Path,
    filepaths: list[Path],
    function_name: str,
) -> list[CallSite]:
    all_sites: list[CallSite] = []
    for filepath in filepaths:
        all_sites.extend(_collect_call_sites_for_file(root, filepath, function_name))
    return all_sites


def _collect_call_sites_for_file(
    root: Path,
    filepath: Path,
    function_name: str,
) -> list[CallSite]:
    from tools.cq.macros.calls import analysis as analysis_module

    if not filepath.exists():
        return []
    try:
        source = filepath.read_text(encoding="utf-8")
        rel_path = str(filepath.relative_to(root))
        tree = ast.parse(source, filename=rel_path)
    except (SyntaxError, OSError, UnicodeDecodeError, ValueError):
        return []

    source_lines = source.splitlines()
    def_lines = find_def_lines(filepath)
    total_lines = len(source_lines)
    finder = analysis_module.CallFinder(rel_path, function_name, tree)
    finder.visit(tree)
    all_sites: list[CallSite] = []
    for site in finder.sites:
        context_window = _compute_context_window(site.line, def_lines, total_lines)
        context_snippet = _extract_context_snippet(
            source_lines,
            context_window.start_line,
            context_window.end_line,
            match_line=site.line,
        )
        all_sites.append(
            msgspec.structs.replace(
                site,
                context_window=context_window,
                context_snippet=context_snippet,
            )
        )
    return all_sites


def _build_call_site_from_record(
    ctx: CallSiteBuildContext,
    record: SgRecord,
) -> CallSite | None:
    from tools.cq.macros.calls import analysis as analysis_module

    call_node = ctx.call_index.get((record.start_line, record.start_col))
    if call_node is None:
        call_node = analysis_module.parse_call_expr(record.text)
    if call_node is None or not analysis_module.matches_target_expr(
        call_node.func, ctx.function_name
    ):
        return None
    info = analysis_module.analyze_call(call_node)
    context = analysis_module.get_containing_function(ctx.tree, record.start_line)
    callee, _is_method, receiver = analysis_module.get_call_name(call_node.func)
    hazards = analysis_module.detect_hazards(call_node, info)
    enrichment = enrich_call_site_module(ctx.source, context, ctx.rel_path)
    context_window = _compute_context_window(record.start_line, ctx.def_lines, ctx.total_lines)
    context_snippet = _extract_context_snippet(
        ctx.source_lines,
        context_window.start_line,
        context_window.end_line,
        match_line=record.start_line,
    )
    return analysis_module.CallSite(
        file=ctx.rel_path,
        line=record.start_line,
        col=record.start_col,
        num_args=info.num_args,
        num_kwargs=info.num_kwargs,
        kwargs=info.kwargs,
        has_star_args=info.has_star_args,
        has_star_kwargs=info.has_star_kwargs,
        context=context,
        arg_preview=info.arg_preview,
        callee=callee,
        receiver=receiver,
        resolution_confidence="unresolved",
        resolution_path="",
        binding="unresolved",
        target_names=[],
        call_id=analysis_module.stable_callsite_id(
            file=ctx.rel_path,
            line=record.start_line,
            col=record.start_col,
            callee=callee,
            context=context,
        ),
        hazards=hazards,
        symtable_info=enrichment.get("symtable"),
        bytecode_info=enrichment.get("bytecode"),
        context_window=context_window,
        context_snippet=context_snippet,
    )


__all__ = ["analyze_sites", "collect_call_sites", "collect_call_sites_from_records"]

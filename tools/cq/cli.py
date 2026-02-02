"""CLI entry point for cq tool."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from tools.cq.core.artifacts import save_artifact_json
from tools.cq.core.bundles import BundleContext, parse_target_spec, run_bundle
from tools.cq.core.findings_table import apply_filters, build_frame, flatten_result, rehydrate_result
from tools.cq.core.renderers import (
    render_dot,
    render_mermaid_class_diagram,
    render_mermaid_flowchart,
)
from tools.cq.core.report import render_markdown, render_summary
from tools.cq.core.schema import CqResult
from tools.cq.core.toolchain import Toolchain
from tools.cq.macros.async_hazards import AsyncHazardsRequest, cmd_async_hazards
from tools.cq.macros.bytecode import BytecodeSurfaceRequest, cmd_bytecode_surface
from tools.cq.macros.calls import cmd_calls
from tools.cq.macros.exceptions import cmd_exceptions
from tools.cq.macros.impact import ImpactRequest, cmd_impact
from tools.cq.macros.imports import ImportRequest, cmd_imports
from tools.cq.macros.scopes import ScopeRequest, cmd_scopes
from tools.cq.macros.side_effects import SideEffectsRequest, cmd_side_effects
from tools.cq.macros.sig_impact import SigImpactRequest, cmd_sig_impact
from tools.cq.index.query_cache import QueryCache
from tools.cq.index.sqlite_cache import IndexCache
from tools.cq.query.executor import execute_plan
from tools.cq.query.parser import QueryParseError, parse_query
from tools.cq.query.planner import compile_query


def _find_repo_root(start: Path | None = None) -> Path:
    """Find git repo root or return cwd.

    Returns
    -------
    Path
        Resolved repository root path.
    """
    if start is None:
        start = Path.cwd()

    current = start.resolve()
    while current != current.parent:
        if (current / ".git").exists():
            return current
        current = current.parent

    return Path.cwd().resolve()


def _apply_cli_filters(
    result: CqResult,
    *,
    include: list[str] | None,
    exclude: list[str] | None,
    impact: list[str] | None,
    confidence: list[str] | None,
    severity: list[str] | None,
    limit: int | None,
) -> CqResult:
    """Apply CLI filter options to a result.

    Parameters
    ----------
    result : CqResult
        Original analysis result.
    include : list[str] | None
        File include patterns.
    exclude : list[str] | None
        File exclude patterns.
    impact : list[str] | None
        Impact bucket filters.
    confidence : list[str] | None
        Confidence bucket filters.
    severity : list[str] | None
        Severity level filters.
    limit : int | None
        Maximum results.

    Returns
    -------
    CqResult
        Filtered result.
    """
    # Skip filtering if no filters specified
    if not any([include, exclude, impact, confidence, severity, limit]):
        return result

    records = flatten_result(result)
    if not records:
        return result

    df = build_frame(records)
    filtered_df = apply_filters(
        df,
        include=include,
        exclude=exclude,
        impact=impact,
        confidence=confidence,
        severity=severity,
        limit=limit,
    )
    return rehydrate_result(result, filtered_df)


def _output_result(
    result: CqResult,
    format_type: str,
    artifact_dir: str | None,
    *,
    no_save: bool,
    include: list[str] | None = None,
    exclude: list[str] | None = None,
    impact: list[str] | None = None,
    confidence: list[str] | None = None,
    severity: list[str] | None = None,
    limit: int | None = None,
) -> None:
    """Output result in requested format.

    Parameters
    ----------
    result : CqResult
        Analysis result.
    format_type : str
        Output format.
    artifact_dir : str | None
        Artifact directory.
    no_save : bool
        Skip artifact saving.
    include : list[str] | None
        File include patterns.
    exclude : list[str] | None
        File exclude patterns.
    impact : list[str] | None
        Impact bucket filters.
    confidence : list[str] | None
        Confidence bucket filters.
    severity : list[str] | None
        Severity level filters.
    limit : int | None
        Maximum results.
    """
    # Apply filters before output
    result = _apply_cli_filters(
        result,
        include=include,
        exclude=exclude,
        impact=impact,
        confidence=confidence,
        severity=severity,
        limit=limit,
    )

    # Save artifact unless disabled
    if not no_save:
        artifact = save_artifact_json(result, artifact_dir)
        result.artifacts.append(artifact)

    if format_type == "json":
        sys.stdout.write(f"{json.dumps(result.to_dict(), indent=2)}\n")
    elif format_type == "md":
        sys.stdout.write(f"{render_markdown(result)}\n")
    elif format_type == "summary":
        sys.stdout.write(f"{render_summary(result)}\n")
    elif format_type == "both":
        sys.stdout.write(f"{render_markdown(result)}\n")
        sys.stdout.write("\n---\n\n")
        sys.stdout.write(f"{json.dumps(result.to_dict(), indent=2)}\n")
    elif format_type == "mermaid":
        sys.stdout.write(f"{render_mermaid_flowchart(result)}\n")
    elif format_type == "mermaid-class":
        sys.stdout.write(f"{render_mermaid_class_diagram(result)}\n")
    elif format_type == "dot":
        sys.stdout.write(f"{render_dot(result)}\n")
    else:
        sys.stdout.write(f"{render_markdown(result)}\n")


def _add_common_args(parser: argparse.ArgumentParser) -> None:
    """Add common arguments to a subparser."""
    parser.add_argument(
        "--root",
        type=Path,
        default=None,
        help="Repository root (default: auto-detect)",
    )
    parser.add_argument(
        "--format",
        choices=["md", "json", "both", "summary", "mermaid", "mermaid-class", "dot"],
        default="md",
        dest="output_format",
        help="Output format (default: md). Visualization formats: mermaid, mermaid-class, dot",
    )
    parser.add_argument(
        "--artifact-dir",
        type=str,
        default=None,
        help="Directory for JSON artifacts (default: .cq/artifacts)",
    )
    parser.add_argument(
        "--no-save-artifact",
        action="store_true",
        help="Don't save JSON artifact",
    )
    parser.add_argument(
        "--impact",
        type=str,
        default=None,
        help="Filter by impact bucket (comma-separated: low,med,high)",
    )
    parser.add_argument(
        "--confidence",
        type=str,
        default=None,
        help="Filter by confidence bucket (comma-separated: low,med,high)",
    )
    parser.add_argument(
        "--include",
        action="append",
        default=None,
        help="Include files matching pattern (glob or ~regex, repeatable)",
    )
    parser.add_argument(
        "--exclude",
        action="append",
        default=None,
        help="Exclude files matching pattern (glob or ~regex, repeatable)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximum number of findings",
    )
    parser.add_argument(
        "--severity",
        type=str,
        default=None,
        help="Filter by severity (comma-separated: error,warning,info)",
    )


def _cmd_impact_cli(args: argparse.Namespace, tc: Toolchain) -> CqResult:
    """Handle impact command.

    Returns
    -------
    CqResult
        Impact command result.
    """
    root = args.root or _find_repo_root()
    request = ImpactRequest(
        tc=tc,
        root=root,
        argv=sys.argv[1:],
        function_name=args.function,
        param_name=args.param,
        max_depth=args.depth,
    )
    return cmd_impact(request)


def _cmd_calls_cli(args: argparse.Namespace, tc: Toolchain) -> CqResult:
    """Handle calls command.

    Returns
    -------
    CqResult
        Calls command result.
    """
    root = args.root or _find_repo_root()
    return cmd_calls(
        tc=tc,
        root=root,
        argv=sys.argv[1:],
        function_name=args.function,
    )


def _cmd_imports_cli(args: argparse.Namespace, tc: Toolchain) -> CqResult:
    """Handle imports command.

    Returns
    -------
    CqResult
        Imports command result.
    """
    root = args.root or _find_repo_root()
    request = ImportRequest(
        tc=tc,
        root=root,
        argv=sys.argv[1:],
        cycles=args.cycles,
        module=args.module,
    )
    return cmd_imports(request)


def _cmd_exceptions_cli(args: argparse.Namespace, tc: Toolchain) -> CqResult:
    """Handle exceptions command.

    Returns
    -------
    CqResult
        Exceptions command result.
    """
    root = args.root or _find_repo_root()
    return cmd_exceptions(
        tc=tc,
        root=root,
        argv=sys.argv[1:],
        function=args.function,
    )


def _cmd_sig_impact_cli(args: argparse.Namespace, tc: Toolchain) -> CqResult:
    """Handle sig-impact command.

    Returns
    -------
    CqResult
        Signature impact command result.
    """
    root = args.root or _find_repo_root()
    request = SigImpactRequest(
        tc=tc,
        root=root,
        argv=sys.argv[1:],
        symbol=args.symbol,
        to=args.to,
    )
    return cmd_sig_impact(request)


def _cmd_side_effects_cli(args: argparse.Namespace, tc: Toolchain) -> CqResult:
    """Handle side-effects command.

    Returns
    -------
    CqResult
        Side effects command result.
    """
    root = args.root or _find_repo_root()
    request = SideEffectsRequest(
        tc=tc,
        root=root,
        argv=sys.argv[1:],
        max_files=args.max_files,
    )
    return cmd_side_effects(request)


def _cmd_scopes_cli(args: argparse.Namespace, tc: Toolchain) -> CqResult:
    """Handle scopes command.

    Returns
    -------
    CqResult
        Scopes command result.
    """
    root = args.root or _find_repo_root()
    request = ScopeRequest(
        tc=tc,
        root=root,
        argv=sys.argv[1:],
        target=args.target,
    )
    return cmd_scopes(request)


def _cmd_async_hazards_cli(args: argparse.Namespace, tc: Toolchain) -> CqResult:
    """Handle async-hazards command.

    Returns
    -------
    CqResult
        Async hazards command result.
    """
    root = args.root or _find_repo_root()
    request = AsyncHazardsRequest(
        tc=tc,
        root=root,
        argv=sys.argv[1:],
        profiles=args.profiles,
    )
    return cmd_async_hazards(request)


def _cmd_bytecode_cli(args: argparse.Namespace, tc: Toolchain) -> CqResult:
    """Handle bytecode-surface command.

    Returns
    -------
    CqResult
        Bytecode surface command result.
    """
    root = args.root or _find_repo_root()
    request = BytecodeSurfaceRequest(
        tc=tc,
        root=root,
        argv=sys.argv[1:],
        target=args.target,
        show=args.show,
    )
    return cmd_bytecode_surface(request)


def _cmd_index_cli(args: argparse.Namespace, tc: Toolchain) -> int:
    """Handle index command.

    Returns
    -------
    int
        Exit code (0 for success).
    """
    root = args.root or _find_repo_root()

    # Get rule version from ast-grep if available
    rule_version = tc.sg_version or "unknown"

    with IndexCache(root, rule_version) as cache:
        if args.clear:
            cache.clear()
            sys.stdout.write("Index cleared.\n")
            return 0

        if args.stats:
            stats = cache.get_stats()
            sys.stdout.write(f"Index Statistics:\n")
            sys.stdout.write(f"  Files cached: {stats.total_files}\n")
            sys.stdout.write(f"  Total records: {stats.total_records}\n")
            sys.stdout.write(f"  Rule version: {stats.rule_version}\n")
            sys.stdout.write(f"  Database size: {stats.database_size_bytes:,} bytes\n")
            return 0

        if args.rebuild:
            cache.clear()
            sys.stdout.write("Index cleared for rebuild.\n")

        # Update index by scanning Python files
        from tools.cq.query.sg_parser import sg_scan

        # Find all Python files
        py_files = list(root.glob("**/*.py"))
        # Exclude common non-source directories
        py_files = [
            f for f in py_files
            if not any(
                part in f.parts
                for part in ("__pycache__", ".git", ".venv", "venv", "node_modules", "build", "dist")
            )
        ]

        sys.stdout.write(f"Found {len(py_files)} Python files.\n")

        # Check which files need rescanning
        files_to_scan: list[Path] = []
        for path in py_files:
            if cache.needs_rescan(path):
                files_to_scan.append(path)

        if not files_to_scan:
            sys.stdout.write("Index is up to date.\n")
            return 0

        sys.stdout.write(f"Scanning {len(files_to_scan)} changed files...\n")

        # Scan in batches to avoid memory issues
        batch_size = 100
        total_records = 0

        for i in range(0, len(files_to_scan), batch_size):
            batch = files_to_scan[i : i + batch_size]
            records = sg_scan(batch, root=root)

            # Group records by file and store
            from tools.cq.query.sg_parser import group_records_by_file

            records_by_file = group_records_by_file(records)

            # Store records for files that had matches
            for file_path_str, file_records in records_by_file.items():
                # Convert records to serializable format
                records_data = [
                    {
                        "record": r.record,
                        "kind": r.kind,
                        "file": r.file,
                        "start_line": r.start_line,
                        "start_col": r.start_col,
                        "end_line": r.end_line,
                        "end_col": r.end_col,
                        "text": r.text,
                        "rule_id": r.rule_id,
                    }
                    for r in file_records
                ]

                # Store uses Path and computes hash/mtime internally
                file_path_obj = Path(file_path_str)
                if file_path_obj.exists():
                    cache.store(file_path_obj, records_data)
                    total_records += len(file_records)

            # Also store empty records for files in the batch that had no matches
            for file_path in batch:
                if str(file_path) not in records_by_file:
                    cache.store(file_path, [])

            sys.stdout.write(f"  Processed {min(i + batch_size, len(files_to_scan))}/{len(files_to_scan)} files\n")

        sys.stdout.write(f"Index updated: {len(files_to_scan)} files, {total_records} records.\n")
        return 0


def _cmd_cache_cli(args: argparse.Namespace) -> int:
    """Handle cache command.

    Returns
    -------
    int
        Exit code (0 for success).
    """
    root = args.root or _find_repo_root()
    cache_dir = root / ".cq" / "cache"

    with QueryCache(cache_dir) as cache:
        if args.clear:
            cache.clear()
            sys.stdout.write("Query cache cleared.\n")
            return 0

        if args.stats:
            stats = cache.stats()
            sys.stdout.write("Query Cache Statistics:\n")
            sys.stdout.write(f"  Total entries: {stats.total_entries}\n")
            sys.stdout.write(f"  Unique files: {stats.unique_files}\n")
            sys.stdout.write(f"  Database size: {stats.database_size_bytes:,} bytes\n")
            if stats.oldest_entry:
                import datetime

                oldest = datetime.datetime.fromtimestamp(stats.oldest_entry)
                newest = datetime.datetime.fromtimestamp(stats.newest_entry or 0)
                sys.stdout.write(f"  Oldest entry: {oldest.isoformat()}\n")
                sys.stdout.write(f"  Newest entry: {newest.isoformat()}\n")
            return 0

        # Default: show stats
        stats = cache.stats()
        sys.stdout.write("Query Cache Statistics:\n")
        sys.stdout.write(f"  Total entries: {stats.total_entries}\n")
        sys.stdout.write(f"  Unique files: {stats.unique_files}\n")
        sys.stdout.write(f"  Database size: {stats.database_size_bytes:,} bytes\n")
        return 0


def _cmd_query_cli(args: argparse.Namespace, tc: Toolchain) -> CqResult:
    """Handle q (query) command.

    Returns
    -------
    CqResult
        Query command result.
    """
    root = args.root or _find_repo_root()

    # Parse the query string
    try:
        query = parse_query(args.query_string)
    except QueryParseError as e:
        from tools.cq.core.schema import mk_result, mk_runmeta, ms

        started_ms = ms()
        run = mk_runmeta(
            macro="q",
            argv=sys.argv[1:],
            root=str(root),
            started_ms=started_ms,
            toolchain=tc.to_dict(),
        )
        result = mk_result(run)
        result.summary["error"] = str(e)
        return result

    # Compile and execute
    plan = compile_query(query)
    use_cache = not args.no_cache
    index_cache: IndexCache | None = None
    query_cache: QueryCache | None = None

    if use_cache:
        rule_version = tc.sg_version or "unknown"
        index_cache = IndexCache(root, rule_version)
        index_cache.initialize()
        query_cache = QueryCache(root / ".cq" / "cache")

    if index_cache is None or query_cache is None:
        return execute_plan(
            plan=plan,
            query=query,
            tc=tc,
            root=root,
            argv=sys.argv[1:],
            use_cache=False,
        )

    with index_cache, query_cache:
        return execute_plan(
            plan=plan,
            query=query,
            tc=tc,
            root=root,
            argv=sys.argv[1:],
            index_cache=index_cache,
            query_cache=query_cache,
            use_cache=use_cache,
        )


def _cmd_report_cli(args: argparse.Namespace, tc: Toolchain) -> CqResult:
    """Handle report bundle command."""
    root = args.root or _find_repo_root()
    try:
        target = parse_target_spec(args.target)
    except ValueError as exc:
        from tools.cq.core.schema import mk_result, mk_runmeta, ms

        started_ms = ms()
        run = mk_runmeta(
            "report",
            argv=sys.argv[1:],
            root=str(root),
            started_ms=started_ms,
            toolchain=tc.to_dict(),
        )
        result = mk_result(run)
        result.summary["error"] = str(exc)
        return result
    use_cache = not args.no_cache
    index_cache: IndexCache | None = None
    query_cache: QueryCache | None = None

    if use_cache:
        rule_version = tc.sg_version or "unknown"
        index_cache = IndexCache(root, rule_version)
        index_cache.initialize()
        query_cache = QueryCache(root / ".cq" / "cache")

    ctx = BundleContext(
        tc=tc,
        root=root,
        argv=sys.argv[1:],
        target=target,
        in_dir=args.in_dir,
        param=args.param,
        signature=args.signature,
        bytecode_show=args.bytecode_show,
        use_cache=use_cache,
        index_cache=index_cache,
        query_cache=query_cache,
    )

    if index_cache is None or query_cache is None:
        return run_bundle(args.preset, ctx)

    with index_cache, query_cache:
        return run_bundle(args.preset, ctx)


def build_parser() -> argparse.ArgumentParser:
    """Build the argument parser.

    Returns
    -------
    argparse.ArgumentParser
        Configured argument parser.
    """
    parser = argparse.ArgumentParser(
        prog="cq",
        description="Code Query - High-signal code analysis macros",
    )
    parser.add_argument(
        "--version",
        action="version",
        version="%(prog)s 0.2.0",
    )

    subparsers = parser.add_subparsers(
        dest="command",
        required=True,
        help="Available commands",
    )

    # impact command
    impact_parser = subparsers.add_parser(
        "impact",
        help="Trace data flow from a function parameter",
    )
    impact_parser.add_argument(
        "function",
        help="Function name to analyze",
    )
    impact_parser.add_argument(
        "--param",
        required=True,
        help="Parameter name to trace",
    )
    impact_parser.add_argument(
        "--depth",
        type=int,
        default=5,
        help="Maximum call depth (default: 5)",
    )
    _add_common_args(impact_parser)

    # calls command
    calls_parser = subparsers.add_parser(
        "calls",
        help="Census all call sites for a function",
    )
    calls_parser.add_argument(
        "function",
        help="Function name to find calls for",
    )
    _add_common_args(calls_parser)

    # imports command
    imports_parser = subparsers.add_parser(
        "imports",
        help="Analyze import structure and cycles",
    )
    imports_parser.add_argument(
        "--cycles",
        action="store_true",
        help="Run cycle detection",
    )
    imports_parser.add_argument(
        "--module",
        type=str,
        default=None,
        help="Focus on specific module",
    )
    _add_common_args(imports_parser)

    # exceptions command
    exceptions_parser = subparsers.add_parser(
        "exceptions",
        help="Analyze exception handling patterns",
    )
    exceptions_parser.add_argument(
        "--function",
        type=str,
        default=None,
        help="Focus on specific function",
    )
    _add_common_args(exceptions_parser)

    # sig-impact command (Phase 2)
    sig_impact_parser = subparsers.add_parser(
        "sig-impact",
        help="Analyze impact of a signature change",
    )
    sig_impact_parser.add_argument(
        "symbol",
        help="Function name to analyze",
    )
    sig_impact_parser.add_argument(
        "--to",
        required=True,
        help='New signature (e.g., "foo(a, b, *, c=None)")',
    )
    _add_common_args(sig_impact_parser)

    # side-effects command (Phase 2)
    side_effects_parser = subparsers.add_parser(
        "side-effects",
        help="Detect import-time side effects",
    )
    side_effects_parser.add_argument(
        "--max-files",
        type=int,
        default=2000,
        help="Maximum files to scan (default: 2000)",
    )
    _add_common_args(side_effects_parser)

    # scopes command (Phase 2)
    scopes_parser = subparsers.add_parser(
        "scopes",
        help="Analyze scope capture (closures)",
    )
    scopes_parser.add_argument(
        "target",
        help="File path or symbol name to analyze",
    )
    _add_common_args(scopes_parser)

    # async-hazards command (Phase 2)
    async_hazards_parser = subparsers.add_parser(
        "async-hazards",
        help="Find blocking calls in async functions",
    )
    async_hazards_parser.add_argument(
        "--profiles",
        type=str,
        default="",
        help="Additional blocking patterns (comma-separated)",
    )
    _add_common_args(async_hazards_parser)

    # bytecode-surface command (Phase 2)
    bytecode_parser = subparsers.add_parser(
        "bytecode-surface",
        help="Analyze bytecode for hidden dependencies",
    )
    bytecode_parser.add_argument(
        "target",
        help="File path or symbol name to analyze",
    )
    bytecode_parser.add_argument(
        "--show",
        type=str,
        default="globals,attrs,constants",
        help="What to show: globals,attrs,constants,opcodes (default: globals,attrs,constants)",
    )
    _add_common_args(bytecode_parser)

    # q (query) command - declarative search DSL
    query_parser = subparsers.add_parser(
        "q",
        help="Declarative code query using ast-grep",
        description="""
Query syntax: key=value pairs separated by spaces.

Required:
  entity=TYPE    Entity type (function, class, method, module, callsite, import)

Optional:
  name=PATTERN   Name to match (exact or ~regex)
  expand=KIND    Graph expansion (callers, callees, imports, raises, scope)
                 Use KIND(depth=N) to set depth (default: 1)
  in=DIR         Search only in directory
  exclude=DIRS   Exclude directories (comma-separated)
  fields=FIELDS  Output fields (def,loc,callers,callees,evidence,hazards)
  limit=N        Maximum results
  explain=true   Include query plan explanation

Examples:
  cq q "entity=function name=build_graph_product"
  cq q "entity=function name=detect expand=callers(depth=2) in=tools/cq/"
  cq q "entity=class in=src/relspec/ fields=def,hazards"
""",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    query_parser.add_argument(
        "query_string",
        help='Query string (e.g., "entity=function name=foo")',
    )
    query_cache_group = query_parser.add_mutually_exclusive_group()
    query_cache_group.add_argument(
        "--cache",
        dest="no_cache",
        action="store_false",
        help="Enable query result caching (default)",
    )
    query_cache_group.add_argument(
        "--no-cache",
        dest="no_cache",
        action="store_true",
        help="Disable query result caching",
    )
    query_parser.set_defaults(no_cache=False)
    _add_common_args(query_parser)

    # report command - bundled, target-scoped reports
    report_parser = subparsers.add_parser(
        "report",
        help="Run target-scoped report bundles",
    )
    report_parser.add_argument(
        "preset",
        choices=["refactor-impact", "safety-reliability", "change-propagation", "dependency-health"],
        help="Report preset to run",
    )
    report_parser.add_argument(
        "--target",
        required=True,
        help="Target spec (function:foo, class:Bar, module:pkg.mod, path:src/...)",
    )
    report_parser.add_argument(
        "--in",
        dest="in_dir",
        default=None,
        help="Restrict analysis to a directory",
    )
    report_parser.add_argument(
        "--param",
        default=None,
        help="Parameter name for impact analysis",
    )
    report_parser.add_argument(
        "--to",
        dest="signature",
        default=None,
        help="Proposed signature for sig-impact analysis",
    )
    report_parser.add_argument(
        "--bytecode-show",
        default=None,
        help="Bytecode surface fields to show (globals,attrs,constants,opcodes)",
    )
    report_cache_group = report_parser.add_mutually_exclusive_group()
    report_cache_group.add_argument(
        "--cache",
        dest="no_cache",
        action="store_false",
        help="Enable query/index caching for report steps (default)",
    )
    report_cache_group.add_argument(
        "--no-cache",
        dest="no_cache",
        action="store_true",
        help="Disable query/index caching for report steps",
    )
    report_parser.set_defaults(no_cache=False)
    _add_common_args(report_parser)

    # index command - manage ast-grep scan cache
    index_parser = subparsers.add_parser(
        "index",
        help="Manage the ast-grep scan index cache",
        description="""
Manage the incremental scan index for faster queries.

The index caches ast-grep scan results and only rescans files that have changed.
This significantly speeds up repeated queries on large codebases.

Operations:
  cq index           Update index for changed files (default)
  cq index --rebuild Full rebuild of the index
  cq index --stats   Show index statistics
  cq index --clear   Clear the index
""",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    index_parser.add_argument(
        "--rebuild",
        action="store_true",
        help="Force full rebuild of the index",
    )
    index_parser.add_argument(
        "--stats",
        action="store_true",
        help="Show index statistics",
    )
    index_parser.add_argument(
        "--clear",
        action="store_true",
        help="Clear the index cache",
    )
    index_parser.add_argument(
        "--root",
        type=Path,
        default=None,
        help="Repository root (default: auto-detect)",
    )

    # cache command - manage query result cache
    cache_parser = subparsers.add_parser(
        "cache",
        help="Manage the query result cache",
        description="""
Manage the query result cache for faster repeated queries.

The cache stores query results and invalidates them when source files change.
This significantly speeds up repeated queries on large codebases.

Operations:
  cq cache --stats   Show cache statistics
  cq cache --clear   Clear the cache
""",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    cache_parser.add_argument(
        "--stats",
        action="store_true",
        help="Show cache statistics",
    )
    cache_parser.add_argument(
        "--clear",
        action="store_true",
        help="Clear the query cache",
    )
    cache_parser.add_argument(
        "--root",
        type=Path,
        default=None,
        help="Repository root (default: auto-detect)",
    )

    return parser


def main(argv: list[str] | None = None) -> int:
    """Run the CLI entry point.

    Parameters
    ----------
    argv : list[str] | None
        Command line arguments. If None, uses sys.argv[1:].

    Returns
    -------
    int
        Exit code.
    """
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        tc = Toolchain.detect()

        # Handle index command specially (doesn't return CqResult)
        if args.command == "index":
            return _cmd_index_cli(args, tc)

        # Handle cache command specially (doesn't return CqResult)
        if args.command == "cache":
            return _cmd_cache_cli(args)

        handlers = {
            "impact": _cmd_impact_cli,
            "calls": _cmd_calls_cli,
            "imports": _cmd_imports_cli,
            "exceptions": _cmd_exceptions_cli,
            "sig-impact": _cmd_sig_impact_cli,
            "side-effects": _cmd_side_effects_cli,
            "scopes": _cmd_scopes_cli,
            "async-hazards": _cmd_async_hazards_cli,
            "bytecode-surface": _cmd_bytecode_cli,
            "q": _cmd_query_cli,
            "report": _cmd_report_cli,
        }

        handler = handlers.get(args.command)
        if handler is None:
            parser.print_help()
            return 1

        result = handler(args, tc)

    except RuntimeError as exc:
        sys.stderr.write(f"Error: {exc}\n")
        return 1
    except KeyboardInterrupt:
        sys.stderr.write("\nInterrupted\n")
        return 130
    else:
        # Parse filter options
        impact_filter = args.impact.split(",") if args.impact else None
        confidence_filter = args.confidence.split(",") if args.confidence else None
        severity_filter = args.severity.split(",") if args.severity else None

        _output_result(
            result,
            args.output_format,
            args.artifact_dir,
            no_save=args.no_save_artifact,
            include=args.include,
            exclude=args.exclude,
            impact=impact_filter,
            confidence=confidence_filter,
            severity=severity_filter,
            limit=args.limit,
        )
        return 0


if __name__ == "__main__":
    sys.exit(main())

"""CLI entry point for cq tool."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from tools.cq.core.artifacts import save_artifact_json
from tools.cq.core.findings_table import apply_filters, build_frame, flatten_result, rehydrate_result
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
        choices=["md", "json", "both", "summary"],
        default="md",
        dest="output_format",
        help="Output format (default: md)",
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

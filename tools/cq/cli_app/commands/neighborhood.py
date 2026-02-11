"""Neighborhood command for cq CLI.

This module contains the 'neighborhood' command for semantic neighborhood analysis.
"""

from __future__ import annotations

from typing import Annotated

from cyclopts import App, Parameter

from tools.cq.cli_app.context import CliContext, CliResult
from tools.cq.core.schema import Finding, Section, mk_result, mk_runmeta, ms

# Create the main app and alias
neighborhood_app = App(name="neighborhood", help="Analyze semantic neighborhood of a target")
nb_app = App(name="nb", help="Analyze semantic neighborhood of a target (alias)")


_MIN_PARTS_WITH_COL = 3
_MIN_PARTS_WITH_LINE = 2


def _parse_target_location(
    target: str,
) -> tuple[str, str | None, int | None]:
    """Parse target string into file, symbol, and line.

    Supports formats:
    - file:line:col
    - file:line
    - symbol_name (fallback)

    Parameters
    ----------
    target : str
        Target specification string.

    Returns:
    -------
    tuple[str, str | None, int | None]
        (file_or_symbol, symbol_hint, line).
    """
    if ":" not in target:
        return target, target, None

    parts = target.split(":")
    if len(parts) >= _MIN_PARTS_WITH_COL:
        file_path = parts[0]
        try:
            line = int(parts[1])
        except ValueError:
            return target, target, None
        else:
            return file_path, None, line

    if len(parts) == _MIN_PARTS_WITH_LINE:
        file_path = parts[0]
        try:
            line = int(parts[1])
        except ValueError:
            return target, target, None
        else:
            return file_path, None, line

    return target, target, None


@neighborhood_app.default
@nb_app.default
def neighborhood(
    target: Annotated[str, Parameter(help="Target location (file:line or symbol)")],
    *,
    lang: Annotated[str, Parameter(name="--lang", help="Query language")] = "python",
    top_k: Annotated[int, Parameter(name="--top-k", help="Max items per slice")] = 10,
    no_lsp: Annotated[bool, Parameter(name="--no-lsp", help="Disable LSP enrichment")] = False,
    ctx: Annotated[CliContext | None, Parameter(parse=False)] = None,
) -> CliResult:
    """Analyze semantic neighborhood of a target symbol or location.

    Parameters
    ----------
    target : str
        Target location (file:line) or symbol name.
    lang : str
        Query language (python, rust).
    top_k : int
        Maximum preview items per slice.
    no_lsp : bool
        Disable LSP enrichment.
    ctx : CliContext | None
        Injected CLI context.

    Returns:
    -------
    CliResult
        Renderable command result payload.

    Raises:
        RuntimeError: If command context is not injected.
    """
    from tools.cq.neighborhood.bundle_builder import BundleBuildRequest, build_neighborhood_bundle
    from tools.cq.neighborhood.scan_snapshot import ScanSnapshot
    from tools.cq.neighborhood.section_layout import materialize_section_layout
    from tools.cq.query.sg_parser import sg_scan

    if ctx is None:
        msg = "Context not injected"
        raise RuntimeError(msg)

    started = ms()

    # Parse target
    file_or_symbol, symbol_hint, line = _parse_target_location(target)

    # Determine target name and file
    if line is not None:
        # file:line:col format
        target_file = file_or_symbol
        target_name = symbol_hint or f"{file_or_symbol}:{line}"
    else:
        # Symbol name fallback
        target_name = file_or_symbol
        target_file = ""

    # Build scan snapshot
    # Per plan: use sg_scan → ScanSnapshot.from_records (not _build_scan_context)
    records = sg_scan(
        paths=[ctx.root],
        lang=lang if lang in {"python", "rust"} else "python",  # type: ignore[arg-type]
        root=ctx.root,
    )
    snapshot = ScanSnapshot.from_records(records)

    # Build neighborhood bundle
    request = BundleBuildRequest(
        target_name=target_name,
        target_file=target_file,
        root=ctx.root,
        snapshot=snapshot,
        language=lang,
        symbol_hint=symbol_hint,
        top_k=top_k,
        enable_lsp=not no_lsp,
        artifact_dir=ctx.artifact_dir,
    )

    bundle = build_neighborhood_bundle(request)

    # Materialize section layout
    view = materialize_section_layout(bundle)

    # Convert to CqResult
    # Create RunMeta
    run = mk_runmeta(
        macro="neighborhood",
        argv=ctx.argv,
        root=str(ctx.root),
        started_ms=started,
        toolchain=ctx.toolchain.to_dict(),
    )

    # Create CqResult
    result = mk_result(run)

    # Populate summary
    result.summary["target"] = target_name
    result.summary["target_file"] = target_file
    result.summary["language"] = lang
    result.summary["top_k"] = top_k
    result.summary["enable_lsp"] = not no_lsp
    result.summary["total_slices"] = len(bundle.slices)
    if bundle.graph:
        result.summary["total_nodes"] = bundle.graph.node_count
        result.summary["total_edges"] = bundle.graph.edge_count

    # Convert key_findings from section_layout FindingV1 to CqResult Finding
    for finding_v1 in view.key_findings:
        result.key_findings.append(
            Finding(
                category=finding_v1.category,
                message=f"{finding_v1.label}: {finding_v1.value}",
            )
        )

    # Convert sections from section_layout SectionV1 to CqResult Section
    for section_v1 in view.sections:
        findings = [
            Finding(
                category="neighborhood",
                message=item,
            )
            for item in section_v1.items
        ]
        result.sections.append(
            Section(
                title=section_v1.title,
                findings=findings,
                collapsed=section_v1.collapsed,
            )
        )

    return CliResult(result=result, context=ctx, filters=None)

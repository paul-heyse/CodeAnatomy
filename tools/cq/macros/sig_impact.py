"""Signature change impact analysis.

Simulates a signature change and classifies call sites as would_break, ambiguous, or ok.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path
from typing import TYPE_CHECKING, Literal

import msgspec

from tools.cq.core.run_context import RunContext
from tools.cq.core.schema import (
    Anchor,
    CqResult,
    Finding,
    Section,
    mk_result,
    ms,
)
from tools.cq.core.scoring import (
    ConfidenceSignals,
    ImpactSignals,
    bucket,
    build_detail_payload,
    confidence_score,
    impact_score,
)
from tools.cq.macros.calls import _collect_call_sites, _group_candidates, _rg_find_candidates
from tools.cq.search import INTERACTIVE, SearchLimits

if TYPE_CHECKING:
    from tools.cq.core.toolchain import Toolchain
    from tools.cq.macros.calls import CallSite

_MAX_SITES_DISPLAY = 30


class SigParam(msgspec.Struct):
    """Parsed signature parameter.

    Parameters
    ----------
    name : str
        Parameter name.
    has_default : bool
        Whether parameter has a default value.
    is_kwonly : bool
        Whether parameter is keyword-only.
    is_vararg : bool
        Whether this is *args.
    is_kwarg : bool
        Whether this is **kwargs.
    """

    name: str
    has_default: bool
    is_kwonly: bool
    is_vararg: bool
    is_kwarg: bool


class SigImpactRequest(msgspec.Struct, frozen=True):
    """Inputs required for signature impact analysis."""

    tc: Toolchain
    root: Path
    argv: list[str]
    symbol: str
    to: str


def _parse_signature(sig: str) -> list[SigParam]:
    """Parse a signature string like 'foo(a, b, *, c=None)' into params.

    Parameters
    ----------
    sig : str
        Signature string to parse.

    Returns
    -------
    list[SigParam]
        Parsed parameter list.
    """
    # Extract inside parens
    m = re.match(r"^\s*\w+\s*\((.*)\)\s*$", sig.strip())
    inside = m.group(1) if m else sig.strip().strip("()")

    # Parse as function def
    tmp = f"def _tmp({inside}):\n  pass\n"
    try:
        tree = ast.parse(tmp)
        fn = tree.body[0]
        if not isinstance(fn, ast.FunctionDef):
            return []
        args = fn.args
    except SyntaxError:
        return []

    params: list[SigParam] = []

    # Positional params
    defaults_start = len(args.args) - len(args.defaults)
    for i, arg in enumerate(args.args):
        params.append(
            SigParam(
                name=arg.arg,
                has_default=i >= defaults_start,
                is_kwonly=False,
                is_vararg=False,
                is_kwarg=False,
            )
        )

    # *args
    if args.vararg:
        params.append(
            SigParam(
                name=args.vararg.arg,
                has_default=False,
                is_kwonly=False,
                is_vararg=True,
                is_kwarg=False,
            )
        )

    # Keyword-only params
    for i, arg in enumerate(args.kwonlyargs):
        has_default = args.kw_defaults[i] is not None
        params.append(
            SigParam(
                name=arg.arg,
                has_default=has_default,
                is_kwonly=True,
                is_vararg=False,
                is_kwarg=False,
            )
        )

    # **kwargs
    if args.kwarg:
        params.append(
            SigParam(
                name=args.kwarg.arg,
                has_default=False,
                is_kwonly=False,
                is_vararg=False,
                is_kwarg=True,
            )
        )

    return params


def _classify_call(
    site: CallSite,
    new_params: list[SigParam],
) -> tuple[str, str]:
    """Classify a call site against new signature.

    Parameters
    ----------
    site : CallSite
        Call site to classify.
    new_params : list[SigParam]
        New signature parameters.

    Returns
    -------
    tuple[str, str]
        (bucket, reason) where bucket is 'would_break', 'ambiguous', or 'ok'.
    """
    # Check if call has *args/**kwargs (can't analyze statically)
    if site.has_star_args or site.has_star_kwargs:
        return ("ambiguous", "call uses *args/**kwargs - cannot verify")

    # Build list of required param names (positional params without defaults)
    required_params = [
        p.name
        for p in new_params
        if not p.has_default and not p.is_kwonly and not p.is_vararg and not p.is_kwarg
    ]

    # For each required param, check coverage by position or keyword
    kwargs_set = set(site.kwargs)
    for i, param_name in enumerate(required_params):
        # Covered by positional arg?
        if i < site.num_args:
            continue
        # Covered by keyword arg?
        if param_name in kwargs_set:
            continue
        return ("would_break", f"parameter '{param_name}' not provided")

    # Check for excess positional args (if no *args in new sig)
    has_vararg = any(p.is_vararg for p in new_params)
    max_positional = sum(
        1 for p in new_params if not p.is_kwonly and not p.is_vararg and not p.is_kwarg
    )
    if not has_vararg and site.num_args > max_positional:
        return ("would_break", f"too many positional args: {site.num_args} > {max_positional}")

    # Check keyword args exist in new signature
    has_kwarg = any(p.is_kwarg for p in new_params)
    param_names = {p.name for p in new_params if not p.is_vararg and not p.is_kwarg}
    for kw in site.kwargs:
        if kw not in param_names and not has_kwarg:
            return ("would_break", f"keyword '{kw}' not in new signature")

    return ("ok", "compatible")


def _collect_sites(
    root: Path,
    symbol: str,
    limits: SearchLimits | None = None,
) -> list[CallSite]:
    """Collect call sites for a symbol using ripgrep.

    Parameters
    ----------
    root : Path
        Root directory to search from.
    symbol : str
        Symbol name to find calls for.
    limits : SearchLimits | None
        Search limits (defaults to INTERACTIVE profile).

    Returns
    -------
    list[CallSite]
        Collected call sites.
    """
    limits = limits or INTERACTIVE
    candidates = _rg_find_candidates(symbol, root, limits=limits)
    by_file = _group_candidates(candidates)
    return _collect_call_sites(root, by_file, symbol)


def _classify_sites(
    all_sites: list[CallSite],
    new_params: list[SigParam],
) -> dict[str, list[tuple[CallSite, str]]]:
    buckets: dict[str, list[tuple[CallSite, str]]] = {
        "would_break": [],
        "ambiguous": [],
        "ok": [],
    }
    for site in all_sites:
        bucket, reason = _classify_call(site, new_params)
        buckets[bucket].append((site, reason))
    return buckets


def _append_bucket_sections(
    result: CqResult,
    buckets: dict[str, list[tuple[CallSite, str]]],
    symbol: str,
    scoring_details: dict[str, object],
) -> None:
    severity_map: dict[str, Literal["info", "warning", "error"]] = {
        "would_break": "error",
        "ambiguous": "warning",
        "ok": "info",
    }
    for bucket_name in ("would_break", "ambiguous", "ok"):
        bucket_sites = buckets.get(bucket_name, [])
        if not bucket_sites:
            continue
        section = Section(title=bucket_name.replace("_", " ").title() + " Sites")
        for site, reason in bucket_sites[:_MAX_SITES_DISPLAY]:
            section.findings.append(
                Finding(
                    category=bucket_name,
                    message=f"{symbol}({site.arg_preview}): {reason}",
                    anchor=Anchor(file=site.file, line=site.line),
                    severity=severity_map[bucket_name],
                    details=build_detail_payload(scoring=scoring_details),
                )
            )
        if len(bucket_sites) > _MAX_SITES_DISPLAY:
            section.findings.append(
                Finding(
                    category="truncated",
                    message=f"... and {len(bucket_sites) - _MAX_SITES_DISPLAY} more",
                    severity="info",
                    details=build_detail_payload(scoring=scoring_details),
                )
            )
        result.sections.append(section)


def _append_evidence(
    result: CqResult,
    all_sites: list[CallSite],
    new_params: list[SigParam],
    symbol: str,
    scoring_details: dict[str, object],
) -> None:
    for site in all_sites:
        bucket_name, reason = _classify_call(site, new_params)
        details = {"preview": site.arg_preview}
        result.evidence.append(
            Finding(
                category=bucket_name,
                message=f"{site.context} calls {symbol}: {reason}",
                anchor=Anchor(file=site.file, line=site.line),
                details=build_detail_payload(scoring=scoring_details, data=details),
            )
        )


def cmd_sig_impact(request: SigImpactRequest) -> CqResult:
    """Analyze impact of changing a function signature.

    Parameters
    ----------
    request : SigImpactRequest
        Signature impact request payload.

    Returns
    -------
    CqResult
        Analysis result with call sites classified.
    """
    started = ms()
    new_params = _parse_signature(request.to)
    all_sites = _collect_sites(request.root, request.symbol)
    buckets = _classify_sites(all_sites, new_params)

    run_ctx = RunContext.from_parts(
        root=request.root,
        argv=request.argv,
        tc=request.tc,
        started_ms=started,
    )
    run = run_ctx.to_runmeta("sig-impact")
    result = mk_result(run)

    result.summary = {
        "symbol": request.symbol,
        "new_signature": request.to,
        "call_sites": len(all_sites),
        "would_break": len(buckets["would_break"]),
        "ambiguous": len(buckets["ambiguous"]),
        "ok": len(buckets["ok"]),
    }

    # Compute scoring signals
    unique_files = len(
        {site.file for site, _ in buckets["would_break"]}
        | {site.file for site, _ in buckets["ambiguous"]}
        | {site.file for site, _ in buckets["ok"]}
    )
    imp_signals = ImpactSignals(
        sites=len(all_sites),
        files=unique_files,
        depth=0,
        breakages=len(buckets["would_break"]),
        ambiguities=len(buckets["ambiguous"]),
    )
    conf_signals = ConfidenceSignals(evidence_kind="resolved_ast")
    imp = impact_score(imp_signals)
    conf = confidence_score(conf_signals)
    scoring_details: dict[str, object] = {
        "impact_score": imp,
        "impact_bucket": bucket(imp),
        "confidence_score": conf,
        "confidence_bucket": bucket(conf),
        "evidence_kind": conf_signals.evidence_kind,
    }

    # Key findings
    if buckets["would_break"]:
        result.key_findings.append(
            Finding(
                category="break",
                message=f"{len(buckets['would_break'])} call sites would break",
                severity="error",
                details=build_detail_payload(scoring=scoring_details),
            )
        )
    if buckets["ambiguous"]:
        result.key_findings.append(
            Finding(
                category="ambiguous",
                message=f"{len(buckets['ambiguous'])} call sites need manual review",
                severity="warning",
                details=build_detail_payload(scoring=scoring_details),
            )
        )
    if buckets["ok"]:
        result.key_findings.append(
            Finding(
                category="ok",
                message=f"{len(buckets['ok'])} call sites are compatible",
                severity="info",
                details=build_detail_payload(scoring=scoring_details),
            )
        )

    if not all_sites:
        result.key_findings.append(
            Finding(
                category="info",
                message=f"No call sites found for '{request.symbol}'",
                severity="info",
                details=build_detail_payload(scoring=scoring_details),
            )
        )

    _append_bucket_sections(result, buckets, request.symbol, scoring_details)
    _append_evidence(result, all_sites, new_params, request.symbol, scoring_details)

    return result

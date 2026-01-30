"""Signature change impact analysis.

Simulates a signature change and classifies call sites as would_break, ambiguous, or ok.
"""

from __future__ import annotations

import ast
import re
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from tools.cq.core.schema import (
    Anchor,
    CqResult,
    Finding,
    Section,
    mk_result,
    mk_runmeta,
    ms,
)
from tools.cq.macros.calls import _collect_call_sites, _group_candidates, _rg_find_candidates

if TYPE_CHECKING:
    from tools.cq.core.toolchain import Toolchain
    from tools.cq.macros.calls import CallSite

_MAX_SITES_DISPLAY = 30


@dataclass
class SigParam:
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


@dataclass(frozen=True)
class SigImpactRequest:
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
    # Count required positional params
    required_pos = sum(
        1
        for p in new_params
        if not p.has_default and not p.is_kwonly and not p.is_vararg and not p.is_kwarg
    )

    # Check if call has *args/**kwargs (can't analyze statically)
    if site.has_star_args or site.has_star_kwargs:
        return ("ambiguous", "call uses *args/**kwargs - cannot verify")

    # Check positional arg count
    if site.num_args < required_pos:
        return ("would_break", f"needs {required_pos} positional args, got {site.num_args}")

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
    tc: Toolchain,
    root: Path,
    symbol: str,
) -> list[CallSite]:
    rg_path = tc.require_rg()
    candidates = _rg_find_candidates(rg_path, symbol, root)
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
) -> None:
    severity_map = {"would_break": "error", "ambiguous": "warning", "ok": "info"}
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
                )
            )
        if len(bucket_sites) > _MAX_SITES_DISPLAY:
            section.findings.append(
                Finding(
                    category="truncated",
                    message=f"... and {len(bucket_sites) - _MAX_SITES_DISPLAY} more",
                    severity="info",
                )
            )
        result.sections.append(section)


def _append_evidence(
    result: CqResult,
    all_sites: list[CallSite],
    new_params: list[SigParam],
    symbol: str,
) -> None:
    for site in all_sites:
        bucket, reason = _classify_call(site, new_params)
        result.evidence.append(
            Finding(
                category=bucket,
                message=f"{site.context} calls {symbol}: {reason}",
                anchor=Anchor(file=site.file, line=site.line),
                details={"preview": site.arg_preview},
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
    all_sites = _collect_sites(request.tc, request.root, request.symbol)
    buckets = _classify_sites(all_sites, new_params)

    run = mk_runmeta(
        "sig-impact",
        request.argv,
        str(request.root),
        started,
        request.tc.to_dict(),
    )
    result = mk_result(run)

    result.summary = {
        "symbol": request.symbol,
        "new_signature": request.to,
        "call_sites": len(all_sites),
        "would_break": len(buckets["would_break"]),
        "ambiguous": len(buckets["ambiguous"]),
        "ok": len(buckets["ok"]),
    }

    # Key findings
    if buckets["would_break"]:
        result.key_findings.append(
            Finding(
                category="break",
                message=f"{len(buckets['would_break'])} call sites would break",
                severity="error",
            )
        )
    if buckets["ambiguous"]:
        result.key_findings.append(
            Finding(
                category="ambiguous",
                message=f"{len(buckets['ambiguous'])} call sites need manual review",
                severity="warning",
            )
        )
    if buckets["ok"]:
        result.key_findings.append(
            Finding(
                category="ok",
                message=f"{len(buckets['ok'])} call sites are compatible",
                severity="info",
            )
        )

    if not all_sites:
        result.key_findings.append(
            Finding(
                category="info",
                message=f"No call sites found for '{request.symbol}'",
                severity="info",
            )
        )

    _append_bucket_sections(result, buckets, request.symbol)
    _append_evidence(result, all_sites, new_params, request.symbol)

    return result

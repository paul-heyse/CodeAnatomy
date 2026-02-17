"""Signature change impact analysis.

Simulates a signature change and classifies call sites as would_break, ambiguous, or ok.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Literal

from tools.cq.analysis.calls import (
    CallClassification,
    classify_call_against_signature,
    classify_calls_against_signature,
)
from tools.cq.analysis.signature import SigParam, parse_signature
from tools.cq.core.schema import (
    Anchor,
    CqResult,
    Finding,
    Section,
    append_section_finding,
    ms,
)
from tools.cq.core.scoring import build_detail_payload
from tools.cq.core.summary_contract import summary_from_mapping
from tools.cq.macros.calls import collect_call_sites, group_candidates, rg_find_candidates
from tools.cq.macros.contracts import MacroRequestBase, ScoringDetailsV1
from tools.cq.macros.result_builder import MacroResultBuilder
from tools.cq.macros.rust_fallback_policy import RustFallbackPolicyV1, apply_rust_fallback_policy
from tools.cq.macros.shared import macro_scoring_details
from tools.cq.search._shared.types import SearchLimits
from tools.cq.search.pipeline.profiles import INTERACTIVE

if TYPE_CHECKING:
    from tools.cq.macros.calls import CallSite

_MAX_SITES_DISPLAY = 30


class SigImpactRequest(MacroRequestBase, frozen=True):
    """Inputs required for signature impact analysis."""

    symbol: str
    to: str


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

    Returns:
    -------
    list[CallSite]
        Collected call sites.
    """
    limits = limits or INTERACTIVE
    candidates = rg_find_candidates(symbol, root, limits=limits)
    by_file = group_candidates(candidates)
    return collect_call_sites(root, by_file, symbol)


def _classify_sites(
    all_sites: list[CallSite],
    new_params: list[SigParam],
) -> dict[CallClassification, list[tuple[CallSite, str]]]:
    return classify_calls_against_signature(all_sites, new_params)


def _append_bucket_sections(
    builder: MacroResultBuilder,
    buckets: dict[CallClassification, list[tuple[CallSite, str]]],
    symbol: str,
    scoring_details: ScoringDetailsV1,
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
            section = append_section_finding(
                section,
                Finding(
                    category=bucket_name,
                    message=f"{symbol}({site.arg_preview}): {reason}",
                    anchor=Anchor(file=site.file, line=site.line),
                    severity=severity_map[bucket_name],
                    details=build_detail_payload(scoring=scoring_details),
                ),
            )
        if len(bucket_sites) > _MAX_SITES_DISPLAY:
            section = append_section_finding(
                section,
                Finding(
                    category="truncated",
                    message=f"... and {len(bucket_sites) - _MAX_SITES_DISPLAY} more",
                    severity="info",
                    details=build_detail_payload(scoring=scoring_details),
                ),
            )
        builder.add_section(section)


def _append_evidence(
    builder: MacroResultBuilder,
    all_sites: list[CallSite],
    new_params: list[SigParam],
    symbol: str,
    scoring_details: ScoringDetailsV1,
) -> None:
    for site in all_sites:
        bucket_name, reason = classify_call_against_signature(site, new_params)
        details = {"preview": site.arg_preview}
        builder.add_evidence(
            Finding(
                category=bucket_name,
                message=f"{site.context} calls {symbol}: {reason}",
                anchor=Anchor(file=site.file, line=site.line),
                details=build_detail_payload(scoring=scoring_details, data=details),
            ),
        )


def cmd_sig_impact(request: SigImpactRequest) -> CqResult:
    """Analyze impact of changing a function signature.

    Parameters
    ----------
    request : SigImpactRequest
        Signature impact request payload.

    Returns:
    -------
    CqResult
        Analysis result with call sites classified.
    """
    started = ms()
    new_params = parse_signature(request.to)
    all_sites = _collect_sites(request.root, request.symbol)
    buckets = _classify_sites(all_sites, new_params)

    builder = MacroResultBuilder(
        "sig-impact",
        root=request.root,
        argv=request.argv,
        tc=request.tc,
        started_ms=started,
    )
    builder.with_summary(
        summary_from_mapping(
            {
                "query": request.symbol,
                "mode": "sig-impact",
                "symbol": request.symbol,
                "new_signature": request.to,
                "call_sites": len(all_sites),
                "would_break": len(buckets["would_break"]),
                "ambiguous": len(buckets["ambiguous"]),
                "ok": len(buckets["ok"]),
            }
        )
    )

    # Compute scoring signals
    unique_files = len(
        {site.file for site, _ in buckets["would_break"]}
        | {site.file for site, _ in buckets["ambiguous"]}
        | {site.file for site, _ in buckets["ok"]}
    )
    scoring_details = macro_scoring_details(
        sites=len(all_sites),
        files=unique_files,
        breakages=len(buckets["would_break"]),
        ambiguities=len(buckets["ambiguous"]),
        evidence_kind="resolved_ast",
    )

    # Key findings
    if buckets["would_break"]:
        builder.add_finding(
            Finding(
                category="break",
                message=f"{len(buckets['would_break'])} call sites would break",
                severity="error",
                details=build_detail_payload(scoring=scoring_details),
            ),
        )
    if buckets["ambiguous"]:
        builder.add_finding(
            Finding(
                category="ambiguous",
                message=f"{len(buckets['ambiguous'])} call sites need manual review",
                severity="warning",
                details=build_detail_payload(scoring=scoring_details),
            ),
        )
    if buckets["ok"]:
        builder.add_finding(
            Finding(
                category="ok",
                message=f"{len(buckets['ok'])} call sites are compatible",
                severity="info",
                details=build_detail_payload(scoring=scoring_details),
            ),
        )

    if not all_sites:
        builder.add_finding(
            Finding(
                category="info",
                message=f"No call sites found for '{request.symbol}'",
                severity="info",
                details=build_detail_payload(scoring=scoring_details),
            ),
        )

    _append_bucket_sections(builder, buckets, request.symbol, scoring_details)
    _append_evidence(builder, all_sites, new_params, request.symbol, scoring_details)
    result = builder.build()

    return apply_rust_fallback_policy(
        result,
        root=request.root,
        policy=RustFallbackPolicyV1(
            macro_name="sig-impact",
            pattern=request.symbol,
            query=request.symbol,
        ),
    )

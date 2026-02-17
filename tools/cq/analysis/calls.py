"""Pure callsite classification helpers for signature-impact analysis."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Literal, Protocol

from tools.cq.analysis.signature import SigParam

CallClassification = Literal["would_break", "ambiguous", "ok"]


class CallSiteLike(Protocol):
    """Protocol for callsite inputs consumed by classifiers."""

    num_args: int
    kwargs: list[str]
    has_star_args: bool
    has_star_kwargs: bool


def classify_call_against_signature(
    site: CallSiteLike,
    new_params: Sequence[SigParam],
) -> tuple[CallClassification, str]:
    """Classify one callsite against a proposed signature."""
    if site.has_star_args or site.has_star_kwargs:
        return ("ambiguous", "call uses *args/**kwargs - cannot verify")

    required_params = [
        p.name
        for p in new_params
        if not p.has_default and not p.is_kwonly and not p.is_vararg and not p.is_kwarg
    ]

    kwargs_set = set(site.kwargs)
    for i, param_name in enumerate(required_params):
        if i < site.num_args:
            continue
        if param_name in kwargs_set:
            continue
        return ("would_break", f"parameter '{param_name}' not provided")

    has_vararg = any(p.is_vararg for p in new_params)
    max_positional = sum(
        1 for p in new_params if not p.is_kwonly and not p.is_vararg and not p.is_kwarg
    )
    if not has_vararg and site.num_args > max_positional:
        return ("would_break", f"too many positional args: {site.num_args} > {max_positional}")

    has_kwarg = any(p.is_kwarg for p in new_params)
    param_names = {p.name for p in new_params if not p.is_vararg and not p.is_kwarg}
    for kw in site.kwargs:
        if kw not in param_names and not has_kwarg:
            return ("would_break", f"keyword '{kw}' not in new signature")

    return ("ok", "compatible")


def classify_calls_against_signature[
    CallSiteT: CallSiteLike,
](
    sites: Sequence[CallSiteT],
    new_params: Sequence[SigParam],
) -> dict[CallClassification, list[tuple[CallSiteT, str]]]:
    """Classify all callsites against a proposed signature."""
    buckets: dict[CallClassification, list[tuple[CallSiteT, str]]] = {
        "would_break": [],
        "ambiguous": [],
        "ok": [],
    }
    for site in sites:
        bucket, reason = classify_call_against_signature(site, new_params)
        buckets[bucket].append((site, reason))
    return buckets


__all__ = [
    "CallClassification",
    "CallSiteLike",
    "classify_call_against_signature",
    "classify_calls_against_signature",
]

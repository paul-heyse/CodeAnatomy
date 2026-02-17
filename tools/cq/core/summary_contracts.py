"""Typed summary envelope contracts for CQ output assembly."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import msgspec

from tools.cq.core.contract_codec import require_mapping
from tools.cq.core.structs import CqStrictOutputStruct


class SummaryOutputEnvelopeV1(CqStrictOutputStruct, frozen=True):
    """Canonical summary envelope for report/render boundaries."""

    summary: dict[str, Any] = msgspec.field(default_factory=dict)
    diagnostics: list[dict[str, Any]] = msgspec.field(default_factory=list)
    telemetry: dict[str, Any] = msgspec.field(default_factory=dict)


def build_summary_envelope(
    *,
    summary: Mapping[str, Any],
    diagnostics: list[dict[str, Any]] | None = None,
    telemetry: Mapping[str, Any] | None = None,
) -> SummaryOutputEnvelopeV1:
    """Build a typed summary envelope from mapping surfaces.

    Returns:
        SummaryOutputEnvelopeV1: Function return value.
    """
    return SummaryOutputEnvelopeV1(
        summary=dict(summary),
        diagnostics=[dict(row) for row in (diagnostics or [])],
        telemetry=dict(telemetry) if isinstance(telemetry, Mapping) else {},
    )


def summary_envelope_to_mapping(envelope: SummaryOutputEnvelopeV1) -> dict[str, Any]:
    """Convert summary envelope to mapping payload.

    Returns:
        dict[str, Any]: Function return value.
    """
    return require_mapping(envelope)


class RunCommandSummaryV1(CqStrictOutputStruct, frozen=True):
    """Typed run summary for structured summary access.

    Provides typed access to common summary fields while remaining
    convertible to/from the dict[str, object] transport format.
    """

    query: str | None = None
    mode: str | None = None
    lang_scope: str | None = None
    total_matches: int = 0
    matched_files: int = 0
    scanned_files: int = 0
    step_summaries: dict[str, dict[str, Any]] = msgspec.field(default_factory=dict)


def run_summary_from_dict(raw: Mapping[str, Any] | None) -> RunCommandSummaryV1:
    """Parse a raw summary dict into a typed RunCommandSummaryV1.

    Returns:
        RunCommandSummaryV1: Typed summary with defaults for missing keys.
    """
    if not raw:
        return RunCommandSummaryV1()
    query = raw.get("query")
    mode = raw.get("mode")
    lang_scope = raw.get("lang_scope")
    total_matches = raw.get("total_matches", 0)
    matched_files = raw.get("matched_files", 0)
    scanned_files = raw.get("scanned_files", 0)
    step_summaries = raw.get("step_summaries")
    return RunCommandSummaryV1(
        query=query if isinstance(query, str) else None,
        mode=mode if isinstance(mode, str) else None,
        lang_scope=lang_scope if isinstance(lang_scope, str) else None,
        total_matches=total_matches if isinstance(total_matches, int) else 0,
        matched_files=matched_files if isinstance(matched_files, int) else 0,
        scanned_files=scanned_files if isinstance(scanned_files, int) else 0,
        step_summaries=dict(step_summaries) if isinstance(step_summaries, dict) else {},
    )


def run_summary_to_dict(summary: RunCommandSummaryV1) -> dict[str, Any]:
    """Convert a typed RunCommandSummaryV1 to a plain dict.

    Returns:
        dict[str, Any]: Summary as mapping payload.
    """
    return require_mapping(summary)


__all__ = [
    "RunCommandSummaryV1",
    "SummaryOutputEnvelopeV1",
    "build_summary_envelope",
    "run_summary_from_dict",
    "run_summary_to_dict",
    "summary_envelope_to_mapping",
]

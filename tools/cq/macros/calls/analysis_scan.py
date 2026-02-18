"""Scan-oriented wrappers for calls analysis."""

from __future__ import annotations

from collections import Counter
from pathlib import Path
from typing import TYPE_CHECKING

from tools.cq.astgrep.sgpy_scanner import SgRecord

if TYPE_CHECKING:
    from tools.cq.macros.calls.analysis import CallSite


def collect_call_sites(
    root_path: Path,
    by_file: dict[Path, list[int]],
    target_name: str,
) -> list[CallSite]:
    """Collect call sites from grouped candidate rows.

    Returns:
        Collected call-site rows for the target symbol.
    """
    from tools.cq.macros.calls.analysis import collect_call_sites as collect_call_sites_impl

    return collect_call_sites_impl(root_path, by_file, target_name)


def collect_call_sites_from_records(
    root: Path,
    call_records: list[SgRecord],
    target_name: str,
) -> tuple[list[CallSite], int]:
    """Collect call-site rows from ast-grep call records.

    Returns:
        Call-site rows and count of files with matching call records.
    """
    from tools.cq.macros.calls.analysis import (
        _collect_call_sites_from_records as collect_call_sites_from_records_impl,
    )

    return collect_call_sites_from_records_impl(root, call_records, target_name)


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
        Argument-shape counts, keyword-usage counts, forwarding total, context counts,
        and hazard counts.
    """
    from tools.cq.macros.calls.analysis import _analyze_sites as analyze_sites_impl

    return analyze_sites_impl(sites)


__all__ = ["analyze_sites", "collect_call_sites", "collect_call_sites_from_records"]

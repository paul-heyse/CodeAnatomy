"""Call-analysis dispatch helpers."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from tools.cq.macros.calls.analysis_scan import (
    collect_call_sites,
)
from tools.cq.macros.calls.analysis_scan import (
    collect_call_sites_from_records as _collect_call_sites_from_records,
)
from tools.cq.macros.calls.insight import _find_function_signature
from tools.cq.macros.calls.scanning import _group_candidates, _rg_find_candidates
from tools.cq.macros.calls.target_runtime import infer_target_language
from tools.cq.query.sg_parser import SgRecord, list_scan_files, sg_scan
from tools.cq.search.pipeline.profiles import INTERACTIVE
from tools.cq.search.rg.adapter import FilePatternSearchOptions, find_files_with_pattern

if TYPE_CHECKING:
    from tools.cq.macros.calls.entry_runtime import CallScanResult
    from tools.cq.macros.contracts import CallsRequest

__all__ = ["dispatch_call_analysis", "scan_call_sites"]


def scan_call_sites(root_path: Path, function_name: str) -> CallScanResult:
    """Scan for call sites using ast-grep, with ripgrep fallback.

    Returns:
        CallScanResult with candidate/scanned files and discovered call sites.
    """
    from tools.cq.macros.calls.entry_runtime import CallScanResult

    search_name = function_name.rsplit(".", maxsplit=1)[-1]
    pattern = rf"\b{search_name}\s*\("
    target_language = infer_target_language(root_path, function_name) or "python"
    candidate_files = find_files_with_pattern(
        root_path,
        pattern,
        options=FilePatternSearchOptions(
            limits=INTERACTIVE,
            lang_scope=target_language,
        ),
    )

    all_scan_files = list_scan_files([root_path], root=root_path, lang=target_language)
    total_py_files = len(all_scan_files)
    scan_files = candidate_files if candidate_files else all_scan_files

    used_fallback = False
    call_records: list[SgRecord] = []
    if scan_files:
        try:
            call_records = sg_scan(
                paths=scan_files,
                record_types={"call"},
                root=root_path,
                lang=target_language,
            )
        except (OSError, RuntimeError, ValueError):
            used_fallback = True
    else:
        used_fallback = True

    signature_info = (
        _find_function_signature(root_path, function_name) if target_language == "python" else ""
    )

    if not used_fallback:
        all_sites, files_with_calls = _collect_call_sites_from_records(
            root_path,
            call_records,
            function_name,
        )
        rg_candidates = 0
    else:
        candidates = _rg_find_candidates(
            function_name,
            root_path,
            lang_scope=target_language,
        )
        by_file = _group_candidates(candidates)
        all_sites = collect_call_sites(root_path, by_file, function_name)
        files_with_calls = len({site.file for site in all_sites})
        rg_candidates = len(candidates)

    return CallScanResult(
        candidate_files=candidate_files,
        scan_files=scan_files,
        total_py_files=total_py_files,
        call_records=call_records,
        used_fallback=used_fallback,
        all_sites=all_sites,
        files_with_calls=files_with_calls,
        rg_candidates=rg_candidates,
        signature_info=signature_info,
    )


def dispatch_call_analysis(
    request: CallsRequest,
    *,
    root: Path,
) -> CallScanResult:
    """Dispatch call-site scan for calls macro request.

    Returns:
        CallScanResult: Scan output bundle for the requested function target.
    """
    return scan_call_sites(root, request.function_name)

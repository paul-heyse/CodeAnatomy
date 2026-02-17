"""Rust call-site helpers extracted from calls analysis."""

from __future__ import annotations

import hashlib
from collections.abc import Callable

from tools.cq.core.definition_parser import extract_symbol_name
from tools.cq.query.sg_parser import SgRecord
from tools.cq.search.pipeline.context_window import ContextWindow

_MAX_RUST_ARG_PREVIEW_CHARS = 80


__all__ = ["build_rust_call_site_from_record", "collect_rust_call_sites"]


def _stable_callsite_id(
    *,
    file: str,
    line: int,
    col: int,
    callee: str,
    context: str,
) -> str:
    seed = f"{file}:{line}:{col}:{callee}:{context}"
    return hashlib.sha256(seed.encode("utf-8")).hexdigest()[:24]


def _rust_call_arg_segment(text: str) -> str:
    raw = text.strip()
    start = raw.find("(")
    end = raw.rfind(")")
    if start < 0 or end <= start:
        return ""
    return raw[start + 1 : end]


def _rust_call_arg_count(text: str) -> int:
    segment = _rust_call_arg_segment(text)
    if not segment:
        return 0
    depth = 0
    count = 1
    for char in segment:
        if char in "([{":
            depth += 1
        elif char in ")]}":
            depth = max(0, depth - 1)
        elif char == "," and depth == 0:
            count += 1
    return count


def _rust_call_preview(text: str) -> str:
    segment = _rust_call_arg_segment(text)
    if not segment:
        return "()"
    preview = segment.strip()
    if len(preview) > _MAX_RUST_ARG_PREVIEW_CHARS:
        preview = f"{preview[: _MAX_RUST_ARG_PREVIEW_CHARS - 3]}..."
    return preview


def build_rust_call_site_from_record[CallSiteT](
    *,
    rel_path: str,
    record: SgRecord,
    target_symbol: str,
    make_call_site: Callable[..., CallSiteT],
) -> CallSiteT | None:
    """Build one Rust call-site object using caller-provided factory.

    Returns:
        CallSiteT | None: Built call-site object when record matches the target symbol.
    """
    callee = extract_symbol_name(record.text, fallback="")
    if callee != target_symbol:
        return None
    args_preview = _rust_call_preview(record.text)
    num_args = _rust_call_arg_count(record.text)
    context_window = ContextWindow(start_line=record.start_line, end_line=record.start_line)
    return make_call_site(
        file=rel_path,
        line=record.start_line,
        col=record.start_col,
        num_args=num_args,
        num_kwargs=0,
        kwargs=[],
        has_star_args=False,
        has_star_kwargs=False,
        context="<module>",
        arg_preview=args_preview,
        callee=callee,
        receiver=None,
        resolution_confidence="heuristic",
        resolution_path="ast_grep_rust",
        binding="ok",
        target_names=[target_symbol],
        call_id=_stable_callsite_id(
            file=rel_path,
            line=record.start_line,
            col=record.start_col,
            callee=callee,
            context="<module>",
        ),
        hazards=[],
        symtable_info=None,
        bytecode_info=None,
        context_window=context_window,
        context_snippet=record.text.strip(),
    )


def collect_rust_call_sites[CallSiteT](
    rel_path: str,
    records: list[SgRecord],
    function_name: str,
    *,
    make_call_site: Callable[..., CallSiteT],
) -> list[CallSiteT]:
    """Collect Rust call-site objects from ast-grep records.

    Returns:
        list[CallSiteT]: Collected Rust call-site objects for the requested symbol.
    """
    target_symbol = function_name.rsplit(".", maxsplit=1)[-1]
    sites: list[CallSiteT] = []
    for record in records:
        site = build_rust_call_site_from_record(
            rel_path=rel_path,
            record=record,
            target_symbol=target_symbol,
            make_call_site=make_call_site,
        )
        if site is not None:
            sites.append(site)
    return sites

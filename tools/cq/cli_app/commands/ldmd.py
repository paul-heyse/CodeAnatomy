"""LDMD progressive disclosure protocol commands."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Annotated

import cyclopts
from cyclopts import validators

from tools.cq.cli_app.context import CliContext, CliResult
from tools.cq.cli_app.infrastructure import protocol_group, require_context
from tools.cq.cli_app.protocol_output import text_result, wants_json
from tools.cq.cli_app.types import LdmdSliceMode
from tools.cq.ldmd.format import (
    LdmdParseError,
    build_index,
    get_neighbors,
    get_slice,
    search_sections,
)

ldmd_app = cyclopts.App(
    name="ldmd",
    help="LDMD progressive disclosure protocol",
    group=protocol_group,
)


@ldmd_app.command
def index(
    path: Annotated[str, cyclopts.Parameter(help="Path to LDMD document")],
    *,
    ctx: Annotated[CliContext | None, cyclopts.Parameter(parse=False)] = None,
) -> CliResult:
    """Index an LDMD document and return section metadata.

    Parameters
    ----------
    path
        Path to LDMD document file.
    ctx
        Injected CLI context.

    Returns:
    -------
    CliResult
        JSON output with section metadata.
    """
    ctx = require_context(ctx)
    doc_path = Path(path)
    if not doc_path.exists():
        return text_result(ctx, f"Document not found: {path}", exit_code=2)

    content = doc_path.read_bytes()
    try:
        idx = build_index(content)
    except LdmdParseError as exc:
        return text_result(ctx, f"Failed to index document: {exc}", exit_code=2)

    sections_json = [
        {
            "id": s.id,
            "start_offset": s.start_offset,
            "end_offset": s.end_offset,
            "depth": s.depth,
            "collapsed": s.collapsed,
        }
        for s in idx.sections
    ]

    return text_result(
        ctx,
        json.dumps(
            {
                "sections": sections_json,
                "total_bytes": idx.total_bytes,
            },
            indent=2,
        ),
        media_type="application/json",
    )


@ldmd_app.command
def get(
    path: Annotated[str, cyclopts.Parameter(help="Path to LDMD document")],
    *,
    section_id: Annotated[str, cyclopts.Parameter(name="--id")],
    mode: Annotated[
        LdmdSliceMode,
        cyclopts.Parameter(
            name="--mode",
            help="Extraction mode: full, preview, or tldr",
        ),
    ] = LdmdSliceMode.full,
    depth: Annotated[
        int,
        cyclopts.Parameter(
            name="--depth",
            validator=validators.Number(gte=0, lte=100),
        ),
    ] = 0,
    limit_bytes: Annotated[
        int,
        cyclopts.Parameter(
            name="--limit-bytes",
            validator=validators.Number(gte=0, lte=10_000_000),
        ),
    ] = 0,
    ctx: Annotated[CliContext | None, cyclopts.Parameter(parse=False)] = None,
) -> CliResult:
    """Extract content from a section by ID.

    Parameters
    ----------
    path
        Path to LDMD document file.
    section_id
        Section ID to extract.
    mode
        Extraction mode: "full", "preview", or "tldr".
    depth
        Include nested sections up to this depth (0 = no nesting).
    limit_bytes
        Max bytes to return (0 = unlimited).
    ctx
        Injected CLI context.

    Returns:
    -------
    CliResult
        Extracted section content.
    """
    ctx = require_context(ctx)
    doc_path = Path(path)
    if not doc_path.exists():
        return text_result(ctx, f"Document not found: {path}", exit_code=2)

    content = doc_path.read_bytes()
    try:
        idx = build_index(content)
        resolved_id = section_id
        if section_id == "root" and idx.sections:
            resolved_id = min(
                idx.sections,
                key=lambda section: section.start_offset,
            ).id
        slice_data = get_slice(
            content,
            idx,
            section_id=resolved_id,
            mode=str(mode),
            depth=depth,
            limit_bytes=limit_bytes,
        )
    except LdmdParseError as exc:
        return text_result(ctx, f"Failed to extract section: {exc}", exit_code=2)
    extracted_text = slice_data.decode("utf-8", errors="replace")
    if wants_json(ctx):
        payload = {
            "section_id": resolved_id,
            "mode": str(mode),
            "depth": depth,
            "limit_bytes": limit_bytes,
            "content": extracted_text,
        }
        return text_result(
            ctx,
            json.dumps(payload, indent=2),
            media_type="application/json",
        )
    return text_result(ctx, extracted_text)


@ldmd_app.command
def search(
    path: Annotated[str, cyclopts.Parameter(help="Path to LDMD document")],
    *,
    query: str,
    ctx: Annotated[CliContext | None, cyclopts.Parameter(parse=False)] = None,
) -> CliResult:
    """Search within LDMD sections.

    Parameters
    ----------
    path
        Path to LDMD document file.
    query
        Search query string.
    ctx
        Injected CLI context.

    Returns:
    -------
    CliResult
        JSON output with search matches.
    """
    ctx = require_context(ctx)
    doc_path = Path(path)
    if not doc_path.exists():
        return text_result(ctx, f"Document not found: {path}", exit_code=2)

    content = doc_path.read_bytes()
    try:
        idx = build_index(content)
        matches = search_sections(content, idx, query=query)
    except LdmdParseError as exc:
        return text_result(ctx, f"Search failed: {exc}", exit_code=2)

    return text_result(ctx, json.dumps(matches, indent=2), media_type="application/json")


@ldmd_app.command
def neighbors(
    path: Annotated[str, cyclopts.Parameter(help="Path to LDMD document")],
    *,
    section_id: Annotated[str, cyclopts.Parameter(name="--id")],
    ctx: Annotated[CliContext | None, cyclopts.Parameter(parse=False)] = None,
) -> CliResult:
    """Get neighboring sections for navigation.

    Parameters
    ----------
    path
        Path to LDMD document file.
    section_id
        Section ID to get neighbors for.
    ctx
        Injected CLI context.

    Returns:
    -------
    CliResult
        JSON output with prev/next section IDs.
    """
    ctx = require_context(ctx)
    doc_path = Path(path)
    if not doc_path.exists():
        return text_result(ctx, f"Document not found: {path}", exit_code=2)

    content = doc_path.read_bytes()
    try:
        idx = build_index(content)
        nav = get_neighbors(idx, section_id=section_id)
    except LdmdParseError as exc:
        return text_result(ctx, f"Navigation failed: {exc}", exit_code=2)
    if wants_json(ctx):
        payload = {
            "section_id": nav.get("section_id"),
            "neighbors": {
                "prev": nav.get("prev"),
                "next": nav.get("next"),
            },
        }
        return text_result(
            ctx,
            json.dumps(payload, indent=2),
            media_type="application/json",
        )
    return text_result(ctx, json.dumps(nav, indent=2), media_type="application/json")


def get_app() -> cyclopts.App:
    """Return LDMD sub-app for lazy registration."""
    return ldmd_app


__all__ = ["get_app", "ldmd_app"]

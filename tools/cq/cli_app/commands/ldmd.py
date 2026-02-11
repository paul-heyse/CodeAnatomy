"""LDMD progressive disclosure protocol commands."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Annotated

import cyclopts

from tools.cq.cli_app.context import CliContext, CliResult, CliTextResult
from tools.cq.ldmd.format import (
    LdmdParseError,
    build_index,
    get_neighbors,
    get_slice,
    search_sections,
)

ldmd_app = cyclopts.App(name="ldmd", help="LDMD progressive disclosure protocol")


def _text_result(
    ctx: CliContext,
    text: str,
    *,
    media_type: str = "text/plain",
    exit_code: int = 0,
) -> CliResult:
    """Build a CLI text result handled by unified output pipeline.

    Parameters
    ----------
    ctx
        CLI context.
    text
        Text content to output.
    media_type
        MIME type of the content.
    exit_code
        Exit code for the result.

    Returns:
    -------
    CliResult
        Wrapped CLI result with text payload.
    """
    return CliResult(
        result=CliTextResult(text=text, media_type=media_type),
        context=ctx,
        exit_code=exit_code,
        filters=None,
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

    Raises:
        RuntimeError: If command context is not injected.
    """
    if ctx is None:
        msg = "CliContext not injected"
        raise RuntimeError(msg)
    doc_path = Path(path)
    if not doc_path.exists():
        return _text_result(ctx, f"Document not found: {path}", exit_code=2)

    content = doc_path.read_bytes()
    try:
        idx = build_index(content)
    except LdmdParseError as exc:
        return _text_result(ctx, f"Failed to index document: {exc}", exit_code=2)

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

    return _text_result(
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
    mode: str = "full",
    depth: int = 0,
    limit_bytes: int = 0,
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

    Raises:
        RuntimeError: If command context is not injected.
    """
    if ctx is None:
        msg = "CliContext not injected"
        raise RuntimeError(msg)
    doc_path = Path(path)
    if not doc_path.exists():
        return _text_result(ctx, f"Document not found: {path}", exit_code=2)

    content = doc_path.read_bytes()
    try:
        idx = build_index(content)
        slice_data = get_slice(
            content,
            idx,
            section_id=section_id,
            mode=mode,
            depth=depth,
            limit_bytes=limit_bytes,
        )
    except LdmdParseError as exc:
        return _text_result(ctx, f"Failed to extract section: {exc}", exit_code=2)

    return _text_result(ctx, slice_data.decode("utf-8", errors="replace"))


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

    Raises:
        RuntimeError: If command context is not injected.
    """
    if ctx is None:
        msg = "CliContext not injected"
        raise RuntimeError(msg)
    doc_path = Path(path)
    if not doc_path.exists():
        return _text_result(ctx, f"Document not found: {path}", exit_code=2)

    content = doc_path.read_bytes()
    try:
        idx = build_index(content)
        matches = search_sections(content, idx, query=query)
    except LdmdParseError as exc:
        return _text_result(ctx, f"Search failed: {exc}", exit_code=2)

    return _text_result(ctx, json.dumps(matches, indent=2), media_type="application/json")


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

    Raises:
        RuntimeError: If command context is not injected.
    """
    if ctx is None:
        msg = "CliContext not injected"
        raise RuntimeError(msg)
    doc_path = Path(path)
    if not doc_path.exists():
        return _text_result(ctx, f"Document not found: {path}", exit_code=2)

    content = doc_path.read_bytes()
    try:
        idx = build_index(content)
        nav = get_neighbors(idx, section_id=section_id)
    except LdmdParseError as exc:
        return _text_result(ctx, f"Navigation failed: {exc}", exit_code=2)

    return _text_result(ctx, json.dumps(nav, indent=2), media_type="application/json")

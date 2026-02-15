"""Cache-backed artifact retrieval commands."""

from __future__ import annotations

import json
from typing import Annotated, Literal

import cyclopts
import msgspec
from cyclopts import validators

from tools.cq.cli_app.context import CliContext, CliResult, CliTextResult
from tools.cq.cli_app.decorators import require_context, require_ctx
from tools.cq.cli_app.groups import protocol_group
from tools.cq.cli_app.types import OutputFormat
from tools.cq.core.artifacts import list_search_artifact_index_entries, load_search_artifact_bundle
from tools.cq.core.cache.contracts import SearchArtifactBundleV1

artifact_app = cyclopts.App(
    name="artifact",
    help="Retrieve cache-backed CQ artifacts",
    group=protocol_group,
)

_ArtifactKind = Literal[
    "search_bundle",
    "summary",
    "object_summaries",
    "occurrences",
    "snippets",
    "diagnostics",
]


def _text_result(
    ctx: CliContext,
    text: str,
    *,
    media_type: str = "text/plain",
    exit_code: int = 0,
) -> CliResult:
    return CliResult(
        result=CliTextResult(text=text, media_type=media_type),
        context=ctx,
        exit_code=exit_code,
        filters=None,
    )


def _wants_json(ctx: CliContext) -> bool:
    return ctx.output_format == OutputFormat.json


@artifact_app.command(name="list")
@require_ctx
def list_artifacts(
    *,
    run_id: Annotated[
        str | None,
        cyclopts.Parameter(name="--run-id", help="Filter entries to a run id"),
    ] = None,
    limit: Annotated[
        int,
        cyclopts.Parameter(
            name="--limit",
            help="Max rows to return",
            validator=validators.Number(gte=1, lte=10_000),
        ),
    ] = 20,
    ctx: Annotated[CliContext | None, cyclopts.Parameter(parse=False)] = None,
) -> CliResult:
    """List cached search artifact bundles.

    Returns:
        CliResult: Human-readable listing of cached artifact index entries.
    """
    ctx = require_context(ctx)

    entries = list_search_artifact_index_entries(
        root=ctx.root,
        run_id=run_id,
        limit=max(1, int(limit)),
    )
    payload = {
        "count": len(entries),
        "entries": [
            {
                "run_id": row.run_id,
                "query": row.query,
                "macro": row.macro,
                "created_ms": row.created_ms,
                "cache_key": row.cache_key,
            }
            for row in entries
        ],
    }
    if _wants_json(ctx):
        return _text_result(ctx, json.dumps(payload, indent=2), media_type="application/json")
    if not entries:
        return _text_result(ctx, "No cached search artifacts found.")
    lines = [f"Cached search artifacts: {len(entries)}"]
    lines.extend(f"- run_id={row.run_id} query={row.query} key={row.cache_key}" for row in entries)
    return _text_result(ctx, "\n".join(lines))


@artifact_app.command
@require_ctx
def get(
    *,
    run_id: Annotated[str, cyclopts.Parameter(name="--run-id", help="Run id to retrieve")],
    kind: Annotated[
        _ArtifactKind,
        cyclopts.Parameter(name="--kind", help="Artifact payload kind"),
    ] = "search_bundle",
    ctx: Annotated[CliContext | None, cyclopts.Parameter(parse=False)] = None,
) -> CliResult:
    """Fetch a cached search artifact payload by run id.

    Returns:
        CliResult: Serialized payload for the selected artifact.
    """
    ctx = require_context(ctx)

    bundle, entry = load_search_artifact_bundle(root=ctx.root, run_id=run_id)
    if bundle is None:
        if entry is None:
            return _text_result(ctx, f"No cached artifact found for run_id={run_id}", exit_code=2)
        return _text_result(ctx, f"Cache decode failed for run_id={run_id}", exit_code=2)

    payload = _artifact_payload_for_kind(bundle, kind=kind)
    serialized = json.dumps(msgspec.to_builtins(payload), indent=2)
    if _wants_json(ctx):
        return _text_result(ctx, serialized, media_type="application/json")
    return _text_result(ctx, serialized)


def _artifact_payload_for_kind(
    bundle: SearchArtifactBundleV1,
    *,
    kind: _ArtifactKind,
) -> object:
    if kind == "summary":
        return bundle.summary
    if kind == "object_summaries":
        return bundle.object_summaries
    if kind == "occurrences":
        return bundle.occurrences
    if kind == "snippets":
        return bundle.snippets
    if kind == "diagnostics":
        return bundle.diagnostics
    return bundle


def get_app() -> cyclopts.App:
    """Return artifact sub-app for lazy registration."""
    return artifact_app


__all__ = ["artifact_app", "get_app"]

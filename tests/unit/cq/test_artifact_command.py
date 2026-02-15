"""Tests for cache-backed `cq artifact` command handlers."""

from __future__ import annotations

import json
from pathlib import Path

from tools.cq.cli_app.commands.artifact import get, list_artifacts
from tools.cq.cli_app.context import CliContext
from tools.cq.cli_app.types import OutputFormat
from tools.cq.core.artifacts import save_search_artifact_bundle_cache
from tools.cq.core.cache.contracts import SearchArtifactBundleV1
from tools.cq.core.schema import CqResult, RunMeta


def _seed_search_artifact(tmp_path: Path) -> None:
    run = RunMeta(
        macro="search",
        argv=["cq", "search", "stable_id"],
        root=str(tmp_path),
        started_ms=1.0,
        elapsed_ms=2.0,
        toolchain={},
        run_id="run-artifact",
    )
    result = CqResult(run=run, summary={"query": "stable_id"})
    bundle = SearchArtifactBundleV1(
        run_id="run-artifact",
        query="stable_id",
        summary={"query": "stable_id"},
        created_ms=1.0,
    )
    save_search_artifact_bundle_cache(result, bundle)


def test_artifact_list_and_get_json(tmp_path: Path) -> None:
    _seed_search_artifact(tmp_path)
    ctx = CliContext.build(
        argv=["artifact", "list"],
        root=tmp_path,
        output_format=OutputFormat.json,
    )

    listed = list_artifacts(run_id="run-artifact", ctx=ctx)
    listed_payload = json.loads(listed.result.text)
    assert listed_payload["count"] >= 1

    fetched = get(run_id="run-artifact", kind="summary", ctx=ctx)
    fetched_payload = json.loads(fetched.result.text)
    assert fetched_payload["query"] == "stable_id"

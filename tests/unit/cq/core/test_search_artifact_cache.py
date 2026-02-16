"""Tests for cache-backed search artifact persistence."""

from __future__ import annotations

from pathlib import Path

from tools.cq.core.artifacts import (
    list_search_artifact_index_entries,
    load_search_artifact_bundle,
    save_search_artifact_bundle_cache,
)
from tools.cq.core.cache.contracts import SearchArtifactBundleV1
from tools.cq.core.schema import CqResult, RunMeta
from tools.cq.search.objects.render import ResolvedObjectRef, SearchObjectSummaryV1


def test_save_and_load_search_artifact_bundle(tmp_path: Path) -> None:
    """Test save and load search artifact bundle."""
    run = RunMeta(
        macro="search",
        argv=["cq", "search", "build_graph"],
        root=str(tmp_path),
        started_ms=1.0,
        elapsed_ms=2.0,
        toolchain={},
        run_id="run-abc",
    )
    result = CqResult(run=run, summary={"query": "build_graph"})
    bundle = SearchArtifactBundleV1(
        run_id="run-abc",
        query="build_graph",
        summary={"query": "build_graph"},
        object_summaries=[
            SearchObjectSummaryV1(
                object_ref=ResolvedObjectRef(
                    object_id="obj_1",
                    language="python",
                    symbol="build_graph",
                    kind="function",
                    canonical_file="src/module.py",
                    canonical_line=1,
                ),
                occurrence_count=1,
                files=["src/module.py"],
            )
        ],
        occurrences=[],
        snippets={},
        created_ms=1.0,
    )

    artifact = save_search_artifact_bundle_cache(result, bundle)
    assert artifact is not None
    assert artifact.path.startswith("cache://search_artifacts/")

    entries = list_search_artifact_index_entries(root=tmp_path, run_id="run-abc")
    assert entries
    loaded_bundle, entry = load_search_artifact_bundle(root=tmp_path, run_id="run-abc")
    assert entry is not None
    assert loaded_bundle is not None
    assert loaded_bundle.query == "build_graph"

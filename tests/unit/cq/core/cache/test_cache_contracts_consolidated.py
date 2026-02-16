"""Tests for consolidated cache contracts in tools/cq/core/cache/contracts.py.

This module verifies that all cache-related contracts serialize and deserialize
correctly using msgspec, and that the consolidation maintains all expected
contract definitions.
"""

from __future__ import annotations

import msgspec
from tools.cq.core.cache.base_contracts import (
    CacheMaintenanceSnapshotV1,
    CacheRuntimeTuningV1,
    LaneCoordinationPolicyV1,
    TreeSitterBlobRefV1,
    TreeSitterCacheEnvelopeV1,
)
from tools.cq.core.cache.contracts import (
    CallsTargetCacheV1,
    ScopeFileStatCacheV1,
    ScopeSnapshotCacheV1,
    SearchArtifactBundleV1,
    SearchArtifactIndexEntryV1,
    SearchArtifactIndexV1,
)
from tools.cq.search.cache.contracts import (
    PatternFragmentCacheV1,
    QueryEntityScanCacheV1,
    SearchCandidatesCacheV1,
    SearchEnrichmentAnchorCacheV1,
    SgRecordCacheV1,
)

BLOB_SIZE_BYTES = 1024
DEFAULT_LANE_LIMIT = 4
DEFAULT_TTL_SECONDS = 15
CACHE_HITS = 100
CACHE_MISSES = 20
EXPIRED_REMOVED = 5
CULLED_REMOVED = 3
DEFAULT_CULL_LIMIT = 16
CREATED_TIMESTAMP_MS = 1234567890.0
CALLSITE_FOO_COUNT = 5


def test_tree_sitter_blob_ref_roundtrip() -> None:
    """Test TreeSitterBlobRefV1 msgspec roundtrip."""
    ref = TreeSitterBlobRefV1(
        blob_id="abc123",
        storage_key="cq:tree_sitter_blob:v1:abc123",
        size_bytes=BLOB_SIZE_BYTES,
        path="/tmp/blobs/abc123.bin",
    )
    encoded = msgspec.msgpack.encode(ref)
    decoded = msgspec.msgpack.decode(encoded, type=TreeSitterBlobRefV1)
    assert decoded.blob_id == "abc123"
    assert decoded.size_bytes == BLOB_SIZE_BYTES
    assert decoded.path == "/tmp/blobs/abc123.bin"


def test_lane_coordination_policy_defaults() -> None:
    """Test LaneCoordinationPolicyV1 default values."""
    policy = LaneCoordinationPolicyV1()
    assert policy.semaphore_key == "cq:tree_sitter:lanes"
    assert policy.lock_key_suffix == ":lock"
    assert policy.lane_limit == DEFAULT_LANE_LIMIT
    assert policy.ttl_seconds == DEFAULT_TTL_SECONDS


def test_cache_maintenance_snapshot_roundtrip() -> None:
    """Test CacheMaintenanceSnapshotV1 msgspec roundtrip."""
    snapshot = CacheMaintenanceSnapshotV1(
        hits=CACHE_HITS,
        misses=CACHE_MISSES,
        expired_removed=EXPIRED_REMOVED,
        culled_removed=CULLED_REMOVED,
        integrity_errors=0,
    )
    encoded = msgspec.msgpack.encode(snapshot)
    decoded = msgspec.msgpack.decode(encoded, type=CacheMaintenanceSnapshotV1)
    assert decoded.hits == CACHE_HITS
    assert decoded.misses == CACHE_MISSES
    assert decoded.expired_removed == EXPIRED_REMOVED
    assert decoded.culled_removed == CULLED_REMOVED


def test_cache_runtime_tuning_defaults() -> None:
    """Test CacheRuntimeTuningV1 default values."""
    tuning = CacheRuntimeTuningV1()
    assert tuning.cull_limit == DEFAULT_CULL_LIMIT
    assert tuning.eviction_policy == "least-recently-stored"
    assert tuning.statistics_enabled is False
    assert tuning.create_tag_index is True


def test_tree_sitter_cache_envelope_roundtrip() -> None:
    """Test TreeSitterCacheEnvelopeV1 msgspec roundtrip."""
    envelope = TreeSitterCacheEnvelopeV1(
        language="python",
        file_hash="f" * 24,
        grammar_hash="g" * 24,
        query_pack_hash="q" * 24,
        scope_hash="s" * 24,
        payload={"enrichment": "applied"},
    )
    encoded = msgspec.msgpack.encode(envelope)
    decoded = msgspec.msgpack.decode(encoded, type=TreeSitterCacheEnvelopeV1)
    assert decoded.language == "python"
    assert decoded.file_hash == "f" * 24
    assert decoded.payload.get("enrichment") == "applied"


def test_sg_record_cache_roundtrip() -> None:
    """Test SgRecordCacheV1 msgspec roundtrip."""
    record = SgRecordCacheV1(
        record="def",
        kind="function_definition",
        file="src/test.py",
        start_line=10,
        start_col=0,
        end_line=15,
        end_col=0,
        text="def foo(): pass",
        rule_id="python.def",
    )
    encoded = msgspec.msgpack.encode(record)
    decoded = msgspec.msgpack.decode(encoded, type=SgRecordCacheV1)
    assert decoded.record == "def"
    assert decoded.kind == "function_definition"
    assert decoded.file == "src/test.py"


def test_scope_snapshot_cache_roundtrip() -> None:
    """Test ScopeSnapshotCacheV1 msgspec roundtrip."""
    snapshot = ScopeSnapshotCacheV1(
        language="python",
        scope_globs=("**/*.py",),
        scope_roots=("src/", "tests/"),
        inventory_token={"total_files": 100},
        files=[
            ScopeFileStatCacheV1(path="src/test.py", size_bytes=1024, mtime_ns=1000000),
        ],
        digest="abc123",
    )
    encoded = msgspec.msgpack.encode(snapshot)
    decoded = msgspec.msgpack.decode(encoded, type=ScopeSnapshotCacheV1)
    assert decoded.language == "python"
    assert len(decoded.files) == 1
    assert decoded.files[0].path == "src/test.py"


def test_search_artifact_bundle_roundtrip() -> None:
    """Test SearchArtifactBundleV1 msgspec roundtrip."""
    bundle = SearchArtifactBundleV1(
        run_id="run123",
        query="build_graph",
        macro="search",
        summary={"total": 10},
        object_summaries=[],
        occurrences=[],
        diagnostics={},
        snippets={},
        created_ms=CREATED_TIMESTAMP_MS,
    )
    encoded = msgspec.msgpack.encode(bundle)
    decoded = msgspec.msgpack.decode(encoded, type=SearchArtifactBundleV1)
    assert decoded.run_id == "run123"
    assert decoded.query == "build_graph"
    assert decoded.created_ms == CREATED_TIMESTAMP_MS


def test_calls_target_cache_roundtrip() -> None:
    """Test CallsTargetCacheV1 msgspec roundtrip."""
    cache = CallsTargetCacheV1(
        target_location=("src/test.py", 10),
        target_callees={"foo": CALLSITE_FOO_COUNT, "bar": CULLED_REMOVED},
        snapshot_digest="digest123",
    )
    encoded = msgspec.msgpack.encode(cache)
    decoded = msgspec.msgpack.decode(encoded, type=CallsTargetCacheV1)
    assert decoded.target_location == ("src/test.py", 10)
    assert decoded.target_callees["foo"] == CALLSITE_FOO_COUNT


def test_pattern_fragment_cache_defaults() -> None:
    """Test PatternFragmentCacheV1 default values."""
    fragment = PatternFragmentCacheV1()
    assert fragment.findings == []
    assert fragment.records == []
    assert fragment.raw_matches == []


def test_query_entity_scan_cache_roundtrip() -> None:
    """Test QueryEntityScanCacheV1 msgspec roundtrip."""
    scan = QueryEntityScanCacheV1(
        records=[
            SgRecordCacheV1(
                record="def",
                kind="function_definition",
                file="src/test.py",
                start_line=5,
                start_col=0,
                end_line=10,
                end_col=0,
                text="def test(): pass",
                rule_id="python.def",
            )
        ]
    )
    encoded = msgspec.msgpack.encode(scan)
    decoded = msgspec.msgpack.decode(encoded, type=QueryEntityScanCacheV1)
    assert len(decoded.records) == 1
    assert decoded.records[0].file == "src/test.py"


def test_search_candidates_cache_roundtrip() -> None:
    """Test SearchCandidatesCacheV1 msgspec roundtrip."""
    candidates = SearchCandidatesCacheV1(
        pattern="build_graph",
        raw_matches=[{"file": "src/graph.py", "line": 10}],
        stats={"total": 1},
    )
    encoded = msgspec.msgpack.encode(candidates)
    decoded = msgspec.msgpack.decode(encoded, type=SearchCandidatesCacheV1)
    assert decoded.pattern == "build_graph"
    assert len(decoded.raw_matches) == 1


def test_search_enrichment_anchor_cache_roundtrip() -> None:
    """Test SearchEnrichmentAnchorCacheV1 msgspec roundtrip."""
    anchor = SearchEnrichmentAnchorCacheV1(
        file="src/test.py",
        line=10,
        col=4,
        match_text="build_graph",
        file_content_hash="hash123",
        language="python",
        enriched_match={"kind": "definition"},
    )
    encoded = msgspec.msgpack.encode(anchor)
    decoded = msgspec.msgpack.decode(encoded, type=SearchEnrichmentAnchorCacheV1)
    assert decoded.file == "src/test.py"
    assert decoded.match_text == "build_graph"


def test_search_artifact_index_roundtrip() -> None:
    """Test SearchArtifactIndexV1 msgspec roundtrip."""
    index = SearchArtifactIndexV1(
        entries=[
            SearchArtifactIndexEntryV1(
                run_id="run123",
                cache_key="key123",
                query="build_graph",
                macro="search",
                created_ms=1234567890.0,
            )
        ]
    )
    encoded = msgspec.msgpack.encode(index)
    decoded = msgspec.msgpack.decode(encoded, type=SearchArtifactIndexV1)
    assert len(decoded.entries) == 1
    assert decoded.entries[0].run_id == "run123"

"""Plan cache roundtrip tests."""

from __future__ import annotations

from pathlib import Path

from cache.diskcache_factory import DiskCacheProfile
from datafusion_engine.plan.cache import PlanCacheEntry, PlanProtoCache


def test_plan_proto_cache_roundtrip(tmp_path: Path) -> None:
    """Round-trip cached plan proto entries."""
    profile = DiskCacheProfile(root=tmp_path)
    cache = PlanProtoCache(cache_profile=profile)
    entry = PlanCacheEntry(
        plan_identity_hash="plan-123",
        plan_fingerprint="fingerprint-abc",
        substrait_bytes=b"substrait",
        logical_plan_proto=b"logical",
        optimized_plan_proto=b"optimized",
        execution_plan_proto=b"execution",
    )
    cache.put(entry)
    assert cache.contains("plan-123")
    loaded = cache.get("plan-123")
    assert loaded == entry


def test_plan_proto_cache_snapshot(tmp_path: Path) -> None:
    """Collect cached plan proto snapshot entries."""
    profile = DiskCacheProfile(root=tmp_path)
    cache = PlanProtoCache(cache_profile=profile)
    entry = PlanCacheEntry(plan_identity_hash="plan-456", substrait_bytes=b"substrait")
    cache.put(entry)
    snapshot = cache.snapshot()
    assert any(item.plan_identity_hash == "plan-456" for item in snapshot)

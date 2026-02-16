"""Tests for test_scope_snapshot_cache."""

from __future__ import annotations

import os
from collections.abc import Generator
from pathlib import Path

import pytest
from tools.cq.core.cache.diskcache_backend import close_cq_cache_backend
from tools.cq.core.cache.snapshot_fingerprint import build_scope_snapshot_fingerprint
from tools.cq.core.cache.telemetry import reset_cache_telemetry, snapshot_cache_telemetry


@pytest.fixture(autouse=True)
def _cache_env(tmp_path: Path) -> Generator[None]:
    close_cq_cache_backend()
    reset_cache_telemetry()
    os.environ["CQ_CACHE_ENABLED"] = "1"
    os.environ["CQ_CACHE_DIR"] = str(tmp_path / "cq_cache")
    yield
    close_cq_cache_backend()
    os.environ.pop("CQ_CACHE_ENABLED", None)
    os.environ.pop("CQ_CACHE_DIR", None)


def test_scope_snapshot_digest_is_deterministic_and_cacheable(tmp_path: Path) -> None:
    """Verify scope snapshots are deterministic and benefit from cache hits."""
    root = tmp_path / "repo"
    root.mkdir(parents=True, exist_ok=True)
    a_file = root / "a.py"
    b_file = root / "b.py"
    a_file.write_text("def alpha() -> int:\n    return 1\n", encoding="utf-8")
    b_file.write_text("def beta() -> int:\n    return 2\n", encoding="utf-8")

    snapshot_a = build_scope_snapshot_fingerprint(
        root=root,
        files=[b_file, a_file],
        language="python",
        scope_globs=["*.py"],
        scope_roots=[root],
    )
    telemetry_after_first = snapshot_cache_telemetry().get("scope_snapshot")

    snapshot_b = build_scope_snapshot_fingerprint(
        root=root,
        files=[a_file, b_file],
        language="python",
        scope_globs=["*.py"],
        scope_roots=[root],
    )
    telemetry_after_second = snapshot_cache_telemetry().get("scope_snapshot")

    assert snapshot_a.digest == snapshot_b.digest
    assert [item.path for item in snapshot_a.files] == ["a.py", "b.py"]
    assert [item.path for item in snapshot_b.files] == ["a.py", "b.py"]
    assert telemetry_after_first is not None
    assert telemetry_after_second is not None
    assert telemetry_after_second.hits > telemetry_after_first.hits

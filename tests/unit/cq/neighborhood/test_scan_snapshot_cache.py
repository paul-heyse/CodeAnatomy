from __future__ import annotations

import os
from collections.abc import Generator
from pathlib import Path

import pytest
from tools.cq.core.cache import (
    close_cq_cache_backend,
    reset_cache_telemetry,
    snapshot_cache_telemetry,
)
from tools.cq.neighborhood.scan_snapshot import ScanSnapshot


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


def test_neighborhood_fragment_cache_uses_content_addressing(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    root.mkdir(parents=True, exist_ok=True)
    file_path = root / "module.py"
    file_path.write_text(
        "def helper() -> int:\n    return 1\n\ndef target() -> int:\n    return helper()\n",
        encoding="utf-8",
    )

    first_snapshot = ScanSnapshot.build_from_repo(root, lang="python", run_id="run-1")
    metrics_first = snapshot_cache_telemetry().get("neighborhood_fragment")

    stat = file_path.stat()
    os.utime(file_path, ns=(stat.st_atime_ns + 1_000_000, stat.st_mtime_ns + 1_000_000))

    second_snapshot = ScanSnapshot.build_from_repo(root, lang="python", run_id="run-2")
    metrics_second = snapshot_cache_telemetry().get("neighborhood_fragment")

    assert len(first_snapshot.def_records) == len(second_snapshot.def_records)
    assert metrics_first is not None
    assert metrics_second is not None
    assert metrics_second.hits > metrics_first.hits
    assert metrics_second.misses == metrics_first.misses

    file_path.write_text(
        "def helper() -> int:\n"
        "    return 2\n"
        "\n"
        "def target() -> int:\n"
        "    value = helper()\n"
        "    return value\n",
        encoding="utf-8",
    )

    _ = ScanSnapshot.build_from_repo(root, lang="python", run_id="run-3")
    metrics_third = snapshot_cache_telemetry().get("neighborhood_fragment")

    assert metrics_third is not None
    assert metrics_third.misses > metrics_second.misses

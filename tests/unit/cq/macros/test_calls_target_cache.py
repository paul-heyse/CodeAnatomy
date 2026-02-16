"""Tests for test_calls_target_cache."""

from __future__ import annotations

import os
from collections.abc import Generator
from pathlib import Path

import pytest
from tools.cq.core.cache.diskcache_backend import close_cq_cache_backend
from tools.cq.core.schema import CqResult, RunMeta
from tools.cq.macros.calls_target import AttachTargetMetadataRequestV1, attach_target_metadata


@pytest.fixture(autouse=True)
def _cache_env(tmp_path: Path) -> Generator[None]:
    close_cq_cache_backend()
    os.environ["CQ_CACHE_ENABLED"] = "1"
    os.environ["CQ_CACHE_DIR"] = str(tmp_path / "cq_cache")
    yield
    close_cq_cache_backend()
    os.environ.pop("CQ_CACHE_ENABLED", None)
    os.environ.pop("CQ_CACHE_DIR", None)


def _empty_result(root: Path) -> CqResult:
    return CqResult(
        run=RunMeta(
            macro="calls",
            argv=[],
            root=str(root),
            started_ms=0.0,
            elapsed_ms=0.0,
            run_id="test-run",
        )
    )


def test_calls_target_cache_revalidates_when_target_file_changes(tmp_path: Path) -> None:
    """Invalidate cached callees when target file body changes."""
    root = tmp_path / "repo"
    root.mkdir(parents=True, exist_ok=True)
    file_path = root / "module.py"
    file_path.write_text(
        "def helper() -> int:\n"
        "    return 1\n"
        "\n"
        "def other() -> int:\n"
        "    return 2\n"
        "\n"
        "def target() -> int:\n"
        "    return helper()\n",
        encoding="utf-8",
    )

    first_result = _empty_result(root)
    _, first_callees, _ = attach_target_metadata(
        first_result,
        AttachTargetMetadataRequestV1(
            root=root,
            function_name="target",
            score=None,
            target_language="python",
            run_id="run-1",
        ),
    )

    assert first_callees["helper"] == 1
    assert first_callees["other"] == 0

    file_path.write_text(
        "def helper() -> int:\n"
        "    return 1\n"
        "\n"
        "def other() -> int:\n"
        "    return 2\n"
        "\n"
        "def target() -> int:\n"
        "    return other()\n",
        encoding="utf-8",
    )

    second_result = _empty_result(root)
    _, second_callees, _ = attach_target_metadata(
        second_result,
        AttachTargetMetadataRequestV1(
            root=root,
            function_name="target",
            score=None,
            target_language="python",
            run_id="run-2",
        ),
    )

    assert second_callees["helper"] == 0
    assert second_callees["other"] == 1

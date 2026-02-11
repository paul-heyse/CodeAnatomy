# ruff: noqa: FBT001
"""Command-level e2e tests for `cq ldmd` protocol commands."""

from __future__ import annotations

import json
import re
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from tests.e2e.cq._support.goldens import assert_json_snapshot_data, load_golden_spec

_DURATION_PATTERN = re.compile(r"(\*\*Elapsed:\*\*)\s+[0-9]+(?:\.[0-9]+)?ms")


def _normalize_ldmd_line(line: str) -> str:
    return _DURATION_PATTERN.sub(r"\1 <duration_ms>", line)


@pytest.mark.e2e
def test_ldmd_index_search_get_neighbors(
    run_cq_text: Callable[..., str],
    update_golden: bool,
    tmp_path: Path,
) -> None:
    ldmd_text = run_cq_text(
        [
            "search",
            "AsyncService",
            "--in",
            "tests/e2e/cq/_golden_workspace/python_project",
            "--format",
            "ldmd",
            "--no-save-artifact",
        ]
    )
    ldmd_path = tmp_path / "search_output.ldmd"
    ldmd_path.write_text(ldmd_text, encoding="utf-8")

    index_payload = json.loads(run_cq_text(["ldmd", "index", str(ldmd_path)]))
    search_payload = json.loads(
        run_cq_text(["ldmd", "search", str(ldmd_path), "--query", "AsyncService"])
    )
    run_meta_slice = run_cq_text(
        [
            "ldmd",
            "get",
            str(ldmd_path),
            "--id",
            "run_meta",
            "--mode",
            "preview",
            "--depth",
            "1",
        ]
    )
    neighbors_payload = json.loads(
        run_cq_text(["ldmd", "neighbors", str(ldmd_path), "--id", "summary"])
    )

    spec = load_golden_spec("golden_specs/ldmd_roundtrip_spec.json")
    _assert_ldmd_spec(index_payload, search_payload, run_meta_slice, neighbors_payload, spec)

    snapshot_payload: dict[str, Any] = {
        "index_section_ids": [
            item.get("id")
            for item in index_payload.get("sections", [])
            if isinstance(item, dict) and isinstance(item.get("id"), str)
        ],
        "search_match_count": len(search_payload),
        "run_meta_slice_prefix": [
            _normalize_ldmd_line(line) for line in run_meta_slice.splitlines()[:8]
        ],
        "neighbors": neighbors_payload,
    }
    assert_json_snapshot_data(
        "ldmd_roundtrip.json",
        snapshot_payload,
        update=update_golden,
    )


def _assert_ldmd_spec(
    index_payload: object,
    search_payload: object,
    run_meta_slice: str,
    neighbors_payload: object,
    spec: dict[str, Any],
) -> None:
    assert isinstance(index_payload, dict)
    assert isinstance(search_payload, list)
    assert isinstance(neighbors_payload, dict)

    sections = index_payload.get("sections")
    assert isinstance(sections, list)

    required_ids = spec.get("required_section_ids", [])
    present_ids = {item.get("id") for item in sections if isinstance(item, dict)}
    for section_id in required_ids:
        if isinstance(section_id, str):
            assert section_id in present_ids

    min_total_bytes = spec.get("min_total_bytes")
    if isinstance(min_total_bytes, int):
        total_bytes = index_payload.get("total_bytes")
        assert isinstance(total_bytes, int)
        assert total_bytes >= min_total_bytes

    required_search_hits = spec.get("required_search_hits")
    if isinstance(required_search_hits, int):
        assert len(search_payload) >= required_search_hits

    run_meta_contains = spec.get("run_meta_contains", [])
    if isinstance(run_meta_contains, list):
        for fragment in run_meta_contains:
            if isinstance(fragment, str):
                assert fragment in run_meta_slice

    assert "prev" in neighbors_payload
    assert "next" in neighbors_payload

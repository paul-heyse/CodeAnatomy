# ruff: noqa: D103
"""Tests for commit-payload helpers."""

from __future__ import annotations

from datafusion_engine.delta.commit_payload import CommitPayloadParts


def test_commit_payload_parts_fields() -> None:
    parts = CommitPayloadParts(
        metadata_payload=[("k", "v")],
        app_id="a",
        app_version=1,
        app_last_updated=2,
        max_retries=3,
        create_checkpoint=True,
    )
    assert parts.app_id == "a"
    assert parts.app_version == 1

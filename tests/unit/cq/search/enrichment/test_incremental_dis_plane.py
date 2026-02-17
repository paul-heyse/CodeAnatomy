"""Tests for incremental dis plane."""

from __future__ import annotations

import msgspec
from tools.cq.search.enrichment.incremental_dis_plane import build_dis_bundle


def _sample(x: int) -> int:
    return x + 1


def _sample_with_nested() -> int:
    def inner() -> int:
        return 1

    return inner()


def test_build_dis_bundle_shapes_payload() -> None:
    """Build dis bundle with expected top-level sections."""
    payload = build_dis_bundle(_sample.__code__, anchor_name="y")
    instruction_facts = payload.get("instruction_facts")
    assert isinstance(instruction_facts, list)
    assert instruction_facts
    cfg = payload.get("cfg")
    assert isinstance(cfg, dict)
    functions_count = cfg.get("functions_count")
    assert isinstance(functions_count, int)
    assert functions_count >= 1
    assert isinstance(payload.get("defuse_events"), list)
    assert isinstance(payload.get("anchor_metrics"), dict)


def test_build_dis_bundle_normalizes_non_builtin_argvals() -> None:
    """Keep dis payload msgspec-serializable when argvals include code objects."""
    payload = build_dis_bundle(_sample_with_nested.__code__, anchor_name="inner")
    instruction_facts = payload.get("instruction_facts")
    assert isinstance(instruction_facts, list)
    assert any(
        isinstance(row, dict)
        and isinstance(row.get("argval"), dict)
        and row.get("argval", {}).get("kind") == "code"
        for row in instruction_facts
    )
    msgspec.to_builtins(payload, order="deterministic", str_keys=True)

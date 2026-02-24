"""Rust-specific tests for neighborhood target resolution scoring."""

from __future__ import annotations

from pathlib import Path

import pytest
from tools.cq.core.target_specs import parse_target_spec
from tools.cq.neighborhood import target_resolution

PREFERRED_TOP_LEVEL_LINE = 12
TIE_SELECTION_LINE = 5


def test_resolve_rust_symbol_prefers_top_level_function(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Rust fallback should prefer top-level function definitions over impl methods."""
    monkeypatch.setattr(
        target_resolution,
        "find_symbol_candidates",
        lambda **_kwargs: [
            ("src/impls.rs", 31, "    fn register_udf(&self) -> Result<()> {"),
            (
                "src/api.rs",
                PREFERRED_TOP_LEVEL_LINE,
                "pub fn register_udf(ctx: &SessionContext) -> Result<()> {",
            ),
        ],
    )

    resolved = target_resolution.resolve_target(
        parse_target_spec("register_udf"),
        root=tmp_path,
        language="rust",
    )

    assert resolved.resolution_kind == "symbol_fallback"
    assert resolved.target_file == "src/api.rs"
    assert resolved.target_line == PREFERRED_TOP_LEVEL_LINE


def test_resolve_rust_symbol_tie_emits_ambiguity_degrade(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Tied Rust candidates should emit deterministic ambiguity diagnostics."""
    monkeypatch.setattr(
        target_resolution,
        "find_symbol_candidates",
        lambda **_kwargs: [
            ("src/a.rs", 5, "pub fn tie_symbol() {}"),
            ("src/b.rs", 2, "pub fn tie_symbol() {}"),
        ],
    )

    resolved = target_resolution.resolve_target(
        parse_target_spec("tie_symbol"),
        root=tmp_path,
        language="rust",
    )

    assert resolved.resolution_kind == "symbol_fallback"
    assert resolved.target_file == "src/a.rs"
    assert resolved.target_line == TIE_SELECTION_LINE
    assert any(event.category == "ambiguous_symbol" for event in resolved.degrade_events)

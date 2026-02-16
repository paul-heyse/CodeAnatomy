"""Tests for model injection in semantic IR pipeline phases."""

from __future__ import annotations

from semantics.ir_pipeline import (
    build_semantic_ir,
    compile_semantics,
    emit_semantics,
    infer_semantics,
    optimize_semantics,
)
from semantics.registry import SEMANTIC_MODEL


def test_ir_phases_accept_injected_model() -> None:
    """All IR phases operate with an explicitly injected model."""
    compiled = compile_semantics(SEMANTIC_MODEL)
    inferred = infer_semantics(compiled, SEMANTIC_MODEL)
    optimized = optimize_semantics(inferred, SEMANTIC_MODEL)
    emitted = emit_semantics(optimized, SEMANTIC_MODEL)

    assert emitted.views
    assert emitted.model_hash is not None


def test_build_semantic_ir_uses_injected_model() -> None:
    """build_semantic_ir accepts explicit model injection."""
    ir = build_semantic_ir(model=SEMANTIC_MODEL)

    assert ir.views
    assert ir.model_hash is not None
    assert ir.ir_hash is not None

"""Contract-lock tests for public semantics module exports."""

from __future__ import annotations

import inspect
from collections.abc import Callable
from typing import Any, cast

import semantics

EXPECTED_EXPORTS = {
    "build_cpg",
    "build_cpg_from_inferred_deps",
    "CpgBuildOptions",
}


def test_semantics_exports_expected_symbols() -> None:
    """Public semantics exports include pipeline entrypoints."""
    exports = getattr(semantics, "__all__", ())
    assert EXPECTED_EXPORTS.issubset(set(exports))


def test_build_cpg_signature_contains_stable_parameters() -> None:
    """build_cpg keeps expected core parameters."""
    symbol = "build_cpg"
    build_cpg = cast("Callable[..., Any]", getattr(semantics, symbol))
    sig = inspect.signature(build_cpg)
    params = sig.parameters

    assert "ctx" in params
    assert "runtime_profile" in params
    assert "options" in params
    assert "execution_context" in params

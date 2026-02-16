"""Tests for semantic diagnostics emission helpers."""

from __future__ import annotations

from pathlib import Path

import pytest

from obs.otel.run_context import get_run_id
from semantics.diagnostics_emission import (
    resolve_semantic_diagnostics_state_store,
    run_context_guard,
)


def test_run_context_guard_provides_run_id() -> None:
    """run_context_guard sets a run id when absent."""
    before = get_run_id()

    with run_context_guard():
        assert get_run_id() is not None

    after = get_run_id()
    if before is None:
        assert after is None
    else:
        assert after == before


def test_resolve_semantic_diagnostics_state_store(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """State store is resolved from CODEANATOMY_STATE_DIR."""
    monkeypatch.setenv("CODEANATOMY_STATE_DIR", str(tmp_path))

    store = resolve_semantic_diagnostics_state_store()

    assert store is not None
    assert store.root == tmp_path

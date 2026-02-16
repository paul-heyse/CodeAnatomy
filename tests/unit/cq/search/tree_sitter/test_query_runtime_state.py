"""Tests for query runtime state consolidation."""

from __future__ import annotations

import threading
from collections.abc import Generator

import pytest
from tools.cq.search.tree_sitter.contracts.query_models import GrammarDriftReportV1
from tools.cq.search.tree_sitter.query.contract_snapshot import QueryContractSnapshotV1
from tools.cq.search.tree_sitter.query.runtime_state import (
    QueryRuntimeState,
    get_query_runtime_state,
    set_query_runtime_state,
)


def test_query_runtime_state_creation() -> None:
    """Test QueryRuntimeState can be instantiated with default empty dicts."""
    state = QueryRuntimeState()
    assert state.last_contract_snapshots == {}
    assert state.last_drift_reports == {}


def test_query_runtime_state_with_data() -> None:
    """Test QueryRuntimeState can hold contract snapshots and drift reports."""
    snapshot = QueryContractSnapshotV1(
        language="python",
        grammar_digest="test_digest",
        query_digest="query_digest",
    )
    report = GrammarDriftReportV1(
        language="python",
        grammar_digest="test_digest",
        query_digest="query_digest",
        compatible=True,
        errors=(),
        schema_diff={},
    )

    state = QueryRuntimeState(
        last_contract_snapshots={"python": snapshot},
        last_drift_reports={"python": report},
    )

    assert state.last_contract_snapshots["python"] == snapshot
    assert state.last_drift_reports["python"] == report


def test_get_query_runtime_state_singleton() -> None:
    """Test get_query_runtime_state returns singleton instance."""
    # Clear any existing state
    set_query_runtime_state(None)

    state1 = get_query_runtime_state()
    state2 = get_query_runtime_state()

    assert state1 is state2


def test_get_query_runtime_state_lazy_init() -> None:
    """Test get_query_runtime_state creates state on first access."""
    set_query_runtime_state(None)

    state = get_query_runtime_state()

    assert isinstance(state, QueryRuntimeState)
    assert state.last_contract_snapshots == {}
    assert state.last_drift_reports == {}


def test_set_query_runtime_state_custom() -> None:
    """Test set_query_runtime_state can install custom state."""
    custom_state = QueryRuntimeState(
        last_contract_snapshots={
            "rust": QueryContractSnapshotV1(
                language="rust",
                grammar_digest="rust_digest",
                query_digest="query_digest",
            )
        },
    )

    set_query_runtime_state(custom_state)
    retrieved = get_query_runtime_state()

    assert retrieved is custom_state
    assert "rust" in retrieved.last_contract_snapshots


def test_set_query_runtime_state_clear() -> None:
    """Test set_query_runtime_state can clear state with None."""
    set_query_runtime_state(QueryRuntimeState())
    assert get_query_runtime_state() is not None

    set_query_runtime_state(None)
    # Next get creates new instance
    new_state = get_query_runtime_state()

    assert isinstance(new_state, QueryRuntimeState)
    assert new_state.last_contract_snapshots == {}


def test_runtime_state_thread_safety() -> None:
    """Test get/set are thread-safe."""
    set_query_runtime_state(None)
    results: list[QueryRuntimeState] = []

    def get_state() -> None:
        state = get_query_runtime_state()
        results.append(state)

    threads = [threading.Thread(target=get_state) for _ in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # All threads should get same singleton
    assert len({id(state) for state in results}) == 1


def test_runtime_state_mutation() -> None:
    """Test runtime state can be mutated after retrieval."""
    set_query_runtime_state(None)
    state = get_query_runtime_state()

    snapshot = QueryContractSnapshotV1(
        language="python",
        grammar_digest="test",
        query_digest="test",
    )
    state.last_contract_snapshots["python"] = snapshot

    # Should be visible in subsequent access
    assert get_query_runtime_state().last_contract_snapshots["python"] == snapshot


@pytest.fixture(autouse=True)
def _reset_state() -> Generator[None]:
    """Reset global runtime state around each test.

    Yields:
        None: Control returns to the test body before state reset.
    """
    yield
    set_query_runtime_state(None)

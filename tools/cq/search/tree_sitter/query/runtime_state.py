"""Consolidated runtime state for tree-sitter query subsystem."""

from __future__ import annotations

import threading
from dataclasses import dataclass, field

from tools.cq.search.tree_sitter.contracts.query_models import GrammarDriftReportV1
from tools.cq.search.tree_sitter.query.contract_snapshot import QueryContractSnapshotV1


@dataclass
class QueryRuntimeState:
    """Consolidated runtime state for query subsystem.

    Attributes
    ----------
    last_contract_snapshots : dict[str, QueryContractSnapshotV1]
        Last contract snapshot captured per language by drift detection.
    last_drift_reports : dict[str, GrammarDriftReportV1]
        Last drift report generated per language by query registry.
    """

    last_contract_snapshots: dict[str, QueryContractSnapshotV1] = field(default_factory=dict)
    last_drift_reports: dict[str, GrammarDriftReportV1] = field(default_factory=dict)


_STATE_LOCK = threading.Lock()
_GLOBAL_STATE: QueryRuntimeState | None = None


def get_query_runtime_state() -> QueryRuntimeState:
    """Get or create the global query runtime state.

    Thread-safe singleton accessor with lazy initialization.

    Returns
    -------
    QueryRuntimeState
        The global runtime state instance.
    """
    global _GLOBAL_STATE
    with _STATE_LOCK:
        if _GLOBAL_STATE is None:
            _GLOBAL_STATE = QueryRuntimeState()
        return _GLOBAL_STATE


def set_query_runtime_state(state: QueryRuntimeState | None) -> None:
    """Set the global query runtime state.

    Thread-safe setter for testing or state reset.

    Parameters
    ----------
    state : QueryRuntimeState | None
        New state to install, or None to clear.
    """
    global _GLOBAL_STATE
    with _STATE_LOCK:
        _GLOBAL_STATE = state


__all__ = [
    "QueryRuntimeState",
    "get_query_runtime_state",
    "set_query_runtime_state",
]

"""Configuration helpers for relspec policy injection."""

from __future__ import annotations

from dataclasses import dataclass

from ibis_engine.param_tables import ParamTablePolicy
from relspec.list_filter_gate import ListFilterGatePolicy
from relspec.pipeline_policy import KernelLanePolicy

# -----------------------------------------------------------------------------
# Feature Flags for Calculation-Driven Scheduling Migration
# -----------------------------------------------------------------------------

# Phase 2: Use inferred dependencies for scheduling instead of declared
USE_INFERRED_DEPS: bool = True

# Threshold for significant order changes in migration validation
_SIGNIFICANT_ORDER_CHANGE_THRESHOLD: int = 2

# Phase 1: Log mismatches between declared and inferred dependencies
COMPARE_DECLARED_INFERRED: bool = True

# Phase 4: Generate Hamilton DAG modules from rustworkx graphs
HAMILTON_DAG_OUTPUT: bool = True


@dataclass(frozen=True)
class RelspecConfig:
    """Centralized configuration bundle for relspec rule wiring."""

    param_table_policy: ParamTablePolicy | None = None
    list_filter_gate_policy: ListFilterGatePolicy | None = None
    kernel_lane_policy: KernelLanePolicy | None = None


@dataclass(frozen=True)
class InferredDepsConfig:
    """Configuration for inferred dependencies feature.

    Attributes
    ----------
    use_inferred_deps : bool
        Use inferred dependencies for scheduling.
    compare_declared_inferred : bool
        Log comparison between declared and inferred.
    hamilton_dag_output : bool
        Generate Hamilton DAG modules.
    log_level : str
        Log level for mismatch warnings.
    allowlist : tuple[str, ...]
        Rules exempt from mismatch warnings.
    """

    use_inferred_deps: bool = USE_INFERRED_DEPS
    compare_declared_inferred: bool = COMPARE_DECLARED_INFERRED
    hamilton_dag_output: bool = HAMILTON_DAG_OUTPUT
    log_level: str = "WARNING"
    allowlist: tuple[str, ...] = ()


def get_inferred_deps_config() -> InferredDepsConfig:
    """Return current inferred dependencies configuration.

    Returns
    -------
    InferredDepsConfig
        Current configuration from module-level flags.
    """
    return InferredDepsConfig(
        use_inferred_deps=USE_INFERRED_DEPS,
        compare_declared_inferred=COMPARE_DECLARED_INFERRED,
        hamilton_dag_output=HAMILTON_DAG_OUTPUT,
    )


__all__ = [
    "COMPARE_DECLARED_INFERRED",
    "HAMILTON_DAG_OUTPUT",
    "USE_INFERRED_DEPS",
    "InferredDepsConfig",
    "RelspecConfig",
    "get_inferred_deps_config",
]

"""Configuration helpers for relspec policy injection."""

from __future__ import annotations

from dataclasses import dataclass

from ibis_engine.param_tables import ParamTablePolicy
from relspec.list_filter_gate import ListFilterGatePolicy
from relspec.pipeline_policy import KernelLanePolicy


@dataclass(frozen=True)
class RelspecConfig:
    """Centralized configuration bundle for relspec rule wiring."""

    param_table_policy: ParamTablePolicy | None = None
    list_filter_gate_policy: ListFilterGatePolicy | None = None
    kernel_lane_policy: KernelLanePolicy | None = None


__all__ = ["RelspecConfig"]

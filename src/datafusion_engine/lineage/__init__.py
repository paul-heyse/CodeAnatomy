"""Lineage and diagnostics."""

from __future__ import annotations

from typing import TYPE_CHECKING

from utils.lazy_module import make_lazy_loader

if TYPE_CHECKING:
    from datafusion_engine.lineage.reporting import LineageReport, extract_lineage
    from datafusion_engine.lineage.scheduling import ScanUnit, plan_scan_unit, plan_scan_units

__all__ = [
    "LineageReport",
    "ScanUnit",
    "extract_lineage",
    "plan_scan_unit",
    "plan_scan_units",
]

_EXPORT_MAP: dict[str, tuple[str, str]] = {
    "LineageReport": ("datafusion_engine.lineage.reporting", "LineageReport"),
    "ScanUnit": ("datafusion_engine.lineage.scheduling", "ScanUnit"),
    "extract_lineage": ("datafusion_engine.lineage.reporting", "extract_lineage"),
    "plan_scan_unit": ("datafusion_engine.lineage.scheduling", "plan_scan_unit"),
    "plan_scan_units": ("datafusion_engine.lineage.scheduling", "plan_scan_units"),
}

__getattr__, __dir__ = make_lazy_loader(_EXPORT_MAP, __name__, globals())

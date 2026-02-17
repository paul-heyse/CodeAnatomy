"""Delta Lake integration."""

from __future__ import annotations

from typing import TYPE_CHECKING

from utils.lazy_module import make_lazy_loader

__all__ = [
    "DeltaMutationRequest",
    "DeltaService",
    "enforce_schema_evolution",
]

_EXPORT_MAP: dict[str, tuple[str, str]] = {
    "DeltaMutationRequest": ("datafusion_engine.delta.service", "DeltaMutationRequest"),
    "DeltaService": ("datafusion_engine.delta.service", "DeltaService"),
    "enforce_schema_evolution": ("datafusion_engine.delta.contracts", "enforce_schema_evolution"),
}

if TYPE_CHECKING:
    from datafusion_engine.delta.contracts import enforce_schema_evolution
    from datafusion_engine.delta.service import DeltaMutationRequest, DeltaService


__getattr__, __dir__ = make_lazy_loader(_EXPORT_MAP, __name__, globals())

"""Session package namespace with lazy public exports."""

from __future__ import annotations

from typing import TYPE_CHECKING

from utils.lazy_module import make_lazy_loader

if TYPE_CHECKING:
    from datafusion_engine.session.config_structs import RuntimeProfileConfig
    from datafusion_engine.session.facade import DataFusionExecutionFacade
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile

__all__ = [
    "DataFusionExecutionFacade",
    "DataFusionRuntimeProfile",
    "RuntimeProfileConfig",
]

_EXPORT_MAP: dict[str, tuple[str, str]] = {
    "DataFusionExecutionFacade": (
        "datafusion_engine.session.facade",
        "DataFusionExecutionFacade",
    ),
    "DataFusionRuntimeProfile": (
        "datafusion_engine.session.runtime",
        "DataFusionRuntimeProfile",
    ),
    "RuntimeProfileConfig": (
        "datafusion_engine.session.config_structs",
        "RuntimeProfileConfig",
    ),
}


__getattr__, __dir__ = make_lazy_loader(_EXPORT_MAP, __name__, globals())

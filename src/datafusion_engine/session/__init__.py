"""Session package namespace with lazy public exports."""

from __future__ import annotations

import importlib
from typing import TYPE_CHECKING

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


def __getattr__(name: str) -> object:
    export = _EXPORT_MAP.get(name)
    if export is None:
        msg = f"module {__name__!r} has no attribute {name!r}"
        raise AttributeError(msg)
    module_name, attr_name = export
    module = importlib.import_module(module_name)
    value = getattr(module, attr_name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(__all__)

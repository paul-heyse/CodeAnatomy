"""Hamilton pipeline entry points for CodeAnatomy."""

from __future__ import annotations

from typing import TYPE_CHECKING

__all__ = [
    "FULL_PIPELINE_OUTPUTS",
    "DriverFactory",
    "PipelineExecutionOptions",
    "build_driver",
    "config_fingerprint",
    "default_modules",
    "execute_pipeline",
]

if TYPE_CHECKING:
    from hamilton_pipeline.driver_factory import (
        DriverFactory,
        build_driver,
        config_fingerprint,
        default_modules,
    )
    from hamilton_pipeline.execution import (
        FULL_PIPELINE_OUTPUTS,
        PipelineExecutionOptions,
        execute_pipeline,
    )


def __getattr__(name: str) -> object:
    if name in {
        "DriverFactory",
        "build_driver",
        "config_fingerprint",
        "default_modules",
    }:
        from hamilton_pipeline import driver_factory as _driver_factory

        return getattr(_driver_factory, name)
    if name in {
        "FULL_PIPELINE_OUTPUTS",
        "PipelineExecutionOptions",
        "execute_pipeline",
    }:
        from hamilton_pipeline import execution as _execution

        return getattr(_execution, name)
    msg = f"module {__name__!r} has no attribute {name!r}"
    raise AttributeError(msg)


def __dir__() -> list[str]:
    return sorted(set(__all__))

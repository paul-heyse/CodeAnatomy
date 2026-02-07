"""Centralized extract adapter execution registry.

Adapter template names from ``datafusion_engine.extract.adapter_registry``
are the canonical key source. Execution callables live here in the orchestration
layer to prevent import cycles between ``datafusion_engine`` and
``hamilton_pipeline``.

Registration is deferred: ``task_execution`` lazily invokes
:func:`register_extract_executor` before extract dispatch so this module carries
no back-import of private helpers.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from extract.session import ExtractSession
    from hamilton_pipeline.modules.task_execution import TaskExecutionInputs

_ExtractExecutorFn = Callable[
    ["TaskExecutionInputs", "ExtractSession", str],
    Mapping[str, object],
]

_EXTRACT_ADAPTER_EXECUTORS: dict[str, _ExtractExecutorFn] = {}


def register_extract_executor(
    adapter_name: str,
    executor: _ExtractExecutorFn,
) -> None:
    """Register an extract executor for an adapter template name.

    Parameters
    ----------
    adapter_name
        Adapter template name (e.g., ``"ast"``, ``"cst"``).
    executor
        Callable implementing the extract execution contract.
    """
    _EXTRACT_ADAPTER_EXECUTORS[adapter_name] = executor


def get_extract_executor(
    adapter_name: str,
) -> _ExtractExecutorFn:
    """Return extract executor callable for adapter template name.

    Parameters
    ----------
    adapter_name
        Adapter template name (e.g., ``"ast"``, ``"cst"``).

    Returns:
    -------
    Callable
        Executor callable.

    Raises:
        ValueError: If adapter name is unknown.
    """
    executor = _EXTRACT_ADAPTER_EXECUTORS.get(adapter_name)
    if executor is None:
        msg = f"Unknown extract adapter: {adapter_name!r}"
        raise ValueError(msg)
    return executor


def registered_adapter_names() -> frozenset[str]:
    """Return all registered adapter names."""
    return frozenset(_EXTRACT_ADAPTER_EXECUTORS)


__all__ = [
    "get_extract_executor",
    "register_extract_executor",
    "registered_adapter_names",
]

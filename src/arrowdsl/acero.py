"""Typed wrapper for pyarrow.acero."""

from __future__ import annotations

from collections.abc import Callable
from typing import Protocol, cast

from pyarrow import acero as _acero

from arrowdsl.pyarrow_protocols import DeclarationLike


class AceroModule(Protocol):
    """Protocol for the subset of pyarrow.acero used in this repo."""

    Declaration: Callable[..., DeclarationLike]
    ScanNodeOptions: Callable[..., object]
    FilterNodeOptions: Callable[..., object]
    ProjectNodeOptions: Callable[..., object]
    TableSourceNodeOptions: Callable[..., object]
    OrderByNodeOptions: Callable[..., object]
    AggregateNodeOptions: Callable[..., object]
    HashJoinNodeOptions: Callable[..., object]


acero = cast("AceroModule", _acero)

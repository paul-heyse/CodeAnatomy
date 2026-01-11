"""Typed wrapper around pyarrow.compute."""

from __future__ import annotations

from typing import cast

import pyarrow.compute as _pc

from arrowdsl.pyarrow_protocols import ComputeModule

pc = cast("ComputeModule", _pc)

__all__ = ["pc"]

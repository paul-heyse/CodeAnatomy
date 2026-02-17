"""Delta types shared between engine and storage layers."""

from __future__ import annotations

from typing import Annotated

import msgspec

from datafusion_engine.generated.delta_types import DeltaFeatureGate
from serde_msgspec import StructBaseCompat

NonNegInt = Annotated[int, msgspec.Meta(ge=0)]


class DeltaProtocolSnapshot(StructBaseCompat, frozen=True):
    """Snapshot of Delta protocol versions and feature flags."""

    min_reader_version: NonNegInt | None = None
    min_writer_version: NonNegInt | None = None
    reader_features: tuple[str, ...] = ()
    writer_features: tuple[str, ...] = ()


__all__ = ["DeltaFeatureGate", "DeltaProtocolSnapshot", "NonNegInt"]

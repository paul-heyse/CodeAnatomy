"""Shared msgspec struct base for CQ internal models."""

from __future__ import annotations

import msgspec


class CqStruct(msgspec.Struct, kw_only=True, frozen=True, omit_defaults=True):
    """Base struct for CQ internal data models."""

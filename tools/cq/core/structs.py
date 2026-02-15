"""Shared msgspec struct base for CQ internal models."""

from __future__ import annotations

import msgspec


class CqStruct(msgspec.Struct, kw_only=True, frozen=True, omit_defaults=True):
    """Base struct for CQ internal data models."""


class CqSettingsStruct(
    msgspec.Struct,
    kw_only=True,
    frozen=True,
    omit_defaults=True,
    forbid_unknown_fields=True,
):
    """Base struct for serializable CQ settings/config contracts."""


class CqOutputStruct(msgspec.Struct, kw_only=True, frozen=True, omit_defaults=True):
    """Base struct for serializable CQ output/public contracts."""


class CqStrictOutputStruct(
    msgspec.Struct,
    kw_only=True,
    frozen=True,
    omit_defaults=True,
    forbid_unknown_fields=True,
):
    """Strict output boundary contract for persisted/external payloads."""


class CqCacheStruct(
    msgspec.Struct,
    kw_only=True,
    frozen=True,
    omit_defaults=True,
    forbid_unknown_fields=True,
):
    """Base struct for serializable CQ cache payload contracts."""

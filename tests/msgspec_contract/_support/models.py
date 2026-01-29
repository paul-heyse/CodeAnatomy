"""Contract models for msgspec tests."""

from __future__ import annotations

import datetime as dt
from typing import Annotated, Literal

import msgspec

from serde_msgspec import StructBaseStrict

PositiveInt = Annotated[int, msgspec.Meta(ge=0)]
NonEmptyStr = Annotated[str, msgspec.Meta(min_length=1)]


class User(StructBaseStrict, frozen=True):
    """Sample user model for contract testing."""

    id: NonEmptyStr
    name: NonEmptyStr
    created_at: Annotated[dt.datetime, msgspec.Meta(tz=True)]
    is_active: bool = True
    flags: set[str] = msgspec.field(default_factory=set)
    note: str | msgspec.UnsetType = msgspec.UNSET


class Click(StructBaseStrict, frozen=True, tag="click"):
    """Sample tagged union member."""

    ts: Annotated[dt.datetime, msgspec.Meta(tz=True)]
    page: NonEmptyStr


class Purchase(StructBaseStrict, frozen=True, tag="purchase"):
    """Sample tagged union member."""

    ts: Annotated[dt.datetime, msgspec.Meta(tz=True)]
    amount_cents: PositiveInt
    currency: Literal["USD", "EUR"]


Event = Click | Purchase


class Envelope(StructBaseStrict, frozen=True):
    """Sample envelope with nested models."""

    user: User
    event: Event
    trace_id: str | msgspec.UnsetType = msgspec.UNSET
    metadata: dict[str, str] = msgspec.field(default_factory=dict)


class StrictUser(StructBaseStrict, frozen=True):
    """Struct with strict field enforcement for error tests."""

    id: NonEmptyStr
    name: NonEmptyStr
    created_at: Annotated[dt.datetime, msgspec.Meta(tz=True)]

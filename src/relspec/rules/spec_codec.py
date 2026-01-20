"""Codec abstractions for relspec spec tables."""

from __future__ import annotations

from collections.abc import Sequence
from importlib import import_module
from typing import TYPE_CHECKING, Protocol, TypeVar, cast

import pyarrow as pa

if TYPE_CHECKING:
    from relspec.rules.definitions import RuleDefinition

SpecT = TypeVar("SpecT")


class SpecCodec(Protocol[SpecT]):
    """Protocol for encoding/decoding spec tables."""

    @property
    def schema(self) -> pa.Schema:
        """Return the Arrow schema for the spec table."""
        ...

    def encode_table(self, items: Sequence[SpecT]) -> pa.Table:
        """Encode spec items into an Arrow table."""
        ...

    def decode_table(self, table: pa.Table) -> tuple[SpecT, ...]:
        """Decode spec items from an Arrow table."""
        ...


def rule_definition_codec() -> SpecCodec[RuleDefinition]:
    """Return the codec for rule definition tables.

    Returns
    -------
    SpecCodec[RuleDefinition]
        Codec for rule definition spec tables.
    """
    module = import_module("relspec.rules.spec_tables")
    codec_cls = cast("type[SpecCodec[RuleDefinition]]", module.RuleDefinitionCodec)
    return codec_cls()


__all__ = ["SpecCodec", "rule_definition_codec"]

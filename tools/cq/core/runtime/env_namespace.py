"""Shared environment parsing helpers for namespace-scoped cache overrides."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Annotated

import msgspec

from tools.cq.core.structs import CqStruct

PositiveInt = Annotated[int, msgspec.Meta(ge=1)]

_TRUE_VALUES = {"1", "true", "yes", "on"}
_FALSE_VALUES = {"0", "false", "no", "off"}


class NamespacePatternV1(CqStruct, frozen=True):
    """One namespace extraction pattern for env variable keys."""

    prefix: str
    suffix: str = ""


class NamespaceEnvParseResultV1(CqStruct, frozen=True):
    """Namespace overrides parsed from environment mappings."""

    values: dict[str, object] = msgspec.field(default_factory=dict)


def namespace_from_env_suffix(suffix: str) -> str:
    """Normalize an env-variable suffix to namespace format.

    Returns:
        str: Lower-case underscore namespace string.
    """
    return suffix.strip("_").lower()


def parse_namespace_int_overrides(
    *,
    env: Mapping[str, str],
    patterns: Sequence[NamespacePatternV1],
    minimum: PositiveInt = 1,
) -> NamespaceEnvParseResultV1:
    """Parse namespace-scoped integer overrides from env mapping.

    Returns:
        NamespaceEnvParseResultV1: Parsed namespace-to-int overrides.
    """
    output: dict[str, object] = {}
    for key, raw in env.items():
        for pattern in patterns:
            if not key.startswith(pattern.prefix):
                continue
            if pattern.suffix and not key.endswith(pattern.suffix):
                continue
            end = -len(pattern.suffix) if pattern.suffix else None
            namespace = namespace_from_env_suffix(key[len(pattern.prefix) : end])
            if not namespace:
                continue
            try:
                value = int(raw)
            except ValueError:
                continue
            if value >= int(minimum):
                output[namespace] = value
    return NamespaceEnvParseResultV1(values=output)


def parse_namespace_bool_overrides(
    *,
    env: Mapping[str, str],
    patterns: Sequence[NamespacePatternV1],
) -> NamespaceEnvParseResultV1:
    """Parse namespace-scoped boolean overrides from env mapping.

    Returns:
        NamespaceEnvParseResultV1: Parsed namespace-to-bool overrides.
    """
    output: dict[str, object] = {}
    for key, raw in env.items():
        for pattern in patterns:
            if not key.startswith(pattern.prefix):
                continue
            if pattern.suffix and not key.endswith(pattern.suffix):
                continue
            end = -len(pattern.suffix) if pattern.suffix else None
            namespace = namespace_from_env_suffix(key[len(pattern.prefix) : end])
            if not namespace:
                continue
            value = raw.strip().lower()
            if value in _TRUE_VALUES:
                output[namespace] = True
            elif value in _FALSE_VALUES:
                output[namespace] = False
    return NamespaceEnvParseResultV1(values=output)


__all__ = [
    "NamespaceEnvParseResultV1",
    "NamespacePatternV1",
    "namespace_from_env_suffix",
    "parse_namespace_bool_overrides",
    "parse_namespace_int_overrides",
]

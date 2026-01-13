"""Template defaults for normalize dataset metadata."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from arrowdsl.core.context import OrderingLevel


@dataclass(frozen=True)
class RegistryTemplate:
    """Template defaults for normalize datasets."""

    stage: str
    ordering_level: OrderingLevel = OrderingLevel.IMPLICIT
    metadata_extra: Mapping[bytes, bytes] | None = None


_TEMPLATES: dict[str, RegistryTemplate] = {
    "normalize": RegistryTemplate(stage="normalize"),
}


def template(name: str) -> RegistryTemplate:
    """Return a registry template by name.

    Returns
    -------
    RegistryTemplate
        Template defaults.
    """
    return _TEMPLATES[name]


def template_names() -> tuple[str, ...]:
    """Return the template registry keys.

    Returns
    -------
    tuple[str, ...]
        Template names.
    """
    return tuple(_TEMPLATES.keys())


__all__ = ["RegistryTemplate", "template", "template_names"]

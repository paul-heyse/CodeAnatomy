"""Template defaults for CPG dataset registry rows."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class RegistryTemplate:
    """Default metadata settings for registry rows."""

    stage: str
    determinism_tier: str


_TEMPLATES: dict[str, RegistryTemplate] = {
    "cpg": RegistryTemplate(stage="cpg", determinism_tier="best_effort"),
}


def template(name: str) -> RegistryTemplate:
    """Return the template for a catalog name.

    Returns
    -------
    RegistryTemplate
        Registry template settings.
    """
    return _TEMPLATES[name]


__all__ = ["RegistryTemplate", "template"]

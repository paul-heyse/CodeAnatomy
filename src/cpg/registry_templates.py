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


def registry_templates() -> dict[str, RegistryTemplate]:
    """Return the full registry template mapping.

    Returns
    -------
    dict[str, RegistryTemplate]
        Mapping of template name to template settings.
    """
    return dict(_TEMPLATES)


__all__ = ["RegistryTemplate", "registry_templates", "template"]

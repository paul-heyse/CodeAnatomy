"""Shared configuration resolution for the CLI and Cyclopts."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from cyclopts.config import Dict

from cli.config_loader import load_effective_config_with_sources

if TYPE_CHECKING:
    from collections.abc import Mapping

    from cli.config_source import ConfigWithSources
    from core_types import JsonValue


@dataclass(frozen=True)
class ConfigResolution:
    """Resolved configuration payload plus source tracking."""

    contents: dict[str, JsonValue]
    sources: ConfigWithSources


def resolve_config(config_file: str | None) -> ConfigResolution:
    """Resolve config contents and source metadata from disk/env.

    Returns
    -------
    ConfigResolution
        Resolved configuration contents and source metadata.
    """
    sources = load_effective_config_with_sources(config_file)
    return ConfigResolution(contents=sources.to_flat_dict(), sources=sources)


def build_cyclopts_config(contents: Mapping[str, JsonValue]) -> list[Dict]:
    """Build Cyclopts config providers from resolved config contents.

    Returns
    -------
    list[Config]
        Cyclopts config providers for CLI defaults.
    """
    return [
        Dict(
            dict(contents),
            allow_unknown=True,
            use_commands_as_keys=False,
            source="codeanatomy",
        ),
    ]


__all__ = ["ConfigResolution", "build_cyclopts_config", "resolve_config"]

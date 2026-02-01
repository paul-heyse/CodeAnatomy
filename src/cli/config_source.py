"""Configuration source tracking for CLI."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from core_types import JsonValue


class ConfigSource(StrEnum):
    """Source of a configuration value."""

    CLI = "cli"
    ENV = "env"
    CONFIG_FILE = "config_file"
    DEFAULT = "default"
    DERIVED = "derived"


@dataclass(frozen=True)
class ConfigValue:
    """Configuration value with source tracking.

    Parameters
    ----------
    key
        Configuration key name.
    value
        The resolved value.
    source
        Source of the value.
    location
        Optional additional detail (e.g., env var name, file path).
    """

    key: str
    value: JsonValue
    source: ConfigSource
    location: str | None = None

    def to_dict(self) -> dict[str, object]:
        """Convert to dictionary representation.

        Returns
        -------
        dict[str, object]
            Dictionary with value, source, and optional detail.
        """
        result: dict[str, object] = {
            "value": self.value,
            "source": self.source.value,
        }
        if self.location:
            result["location"] = self.location
        return result


@dataclass(frozen=True)
class ConfigWithSources:
    """Complete configuration with source tracking.

    Parameters
    ----------
    values
        Mapping of configuration keys to ConfigValue instances.
    """

    values: dict[str, ConfigValue]

    def to_dict(self) -> dict[str, dict[str, object]]:
        """Convert to dictionary representation.

        Returns
        -------
        dict[str, dict[str, object]]
            Nested dictionary with values and their sources.
        """
        return self.to_display_dict()

    def to_display_dict(self) -> dict[str, dict[str, object]]:
        """Convert to display-ready dictionary representation.

        Returns
        -------
        dict[str, dict[str, object]]
            Nested dictionary with values and their sources.
        """
        return {key: cv.to_dict() for key, cv in self.values.items()}

    def to_flat_dict(self) -> dict[str, JsonValue]:
        """Get plain configuration values without source tracking.

        Returns
        -------
        dict[str, JsonValue]
            Plain key-value configuration dictionary.
        """
        return {key: cv.value for key, cv in self.values.items()}

    def plain_values(self) -> dict[str, JsonValue]:
        """Alias for to_flat_dict().

        Returns
        -------
        dict[str, JsonValue]
            Plain key-value configuration dictionary.
        """
        return self.to_flat_dict()


__all__ = ["ConfigSource", "ConfigValue", "ConfigWithSources"]

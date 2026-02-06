"""Validation helpers for CLI configuration payloads."""

from __future__ import annotations

from collections.abc import Mapping

from core_types import JsonValue


def validate_config_mutual_exclusion(config: Mapping[str, JsonValue]) -> None:
    """Validate config-level mutual exclusion rules.

    Args:
        config: Description.

    Raises:
        ValueError: If the operation cannot be completed.
    """
    delta_config = config.get("delta", {}) or {}
    if isinstance(delta_config, dict):
        restore_config = delta_config.get("restore", {}) or {}
        export_config = delta_config.get("export", {}) or {}

        if (
            isinstance(restore_config, dict)
            and restore_config.get("version")
            and restore_config.get("timestamp")
        ):
            msg = "Config error: delta.restore cannot specify both 'version' and 'timestamp'."
            raise ValueError(msg)

        if (
            isinstance(export_config, dict)
            and export_config.get("version")
            and export_config.get("timestamp")
        ):
            msg = "Config error: delta.export cannot specify both 'version' and 'timestamp'."
            raise ValueError(msg)


__all__ = ["validate_config_mutual_exclusion"]

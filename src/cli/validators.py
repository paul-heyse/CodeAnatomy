"""Custom validators for CLI parameter groups."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from cyclopts import Group

_LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class ConditionalRequired:
    """Validator that requires certain parameters when a condition is met.

    Parameters
    ----------
    condition_param
        Name of the parameter that triggers the condition.
    condition_value
        Value that triggers the requirement.
    required_params
        Parameters that are required when condition is met.
    """

    condition_param: str
    condition_value: Any
    required_params: tuple[str, ...]

    def __call__(
        self,
        group: Group,
        resolved_params: dict[str, Any],
    ) -> None:
        """Validate that required parameters are present when condition is met.

        Parameters
        ----------
        group
            The parameter group being validated.
        resolved_params
            Dictionary of resolved parameter values.

        Raises
        ------
        ValueError
            When required parameters are missing.
        """
        _ = group  # Unused but required by cyclopts validator protocol
        condition = resolved_params.get(self.condition_param)
        if condition == self.condition_value:
            missing = [
                param for param in self.required_params if resolved_params.get(param) is None
            ]
            if missing:
                msg = (
                    f"When {self.condition_param}={self.condition_value!r}, "
                    f"the following parameters are required: {', '.join(missing)}"
                )
                raise ValueError(msg)


@dataclass(frozen=True)
class ConditionalDisabled:
    """Validator that warns when parameters are set but a condition disables them.

    This validator emits a warning when parameters are configured but will be
    ignored due to a disabling condition (e.g., --disable-scip with --scip-output-dir).

    Parameters
    ----------
    condition_param
        Name of the parameter that disables functionality.
    condition_value
        Value that indicates the feature is disabled.
    disabled_params
        Parameters that are ignored when the condition is met.
    """

    condition_param: str
    condition_value: Any
    disabled_params: tuple[str, ...]

    def __call__(
        self,
        group: Group,
        resolved_params: dict[str, Any],
    ) -> None:
        """Warn when disabled parameters are configured.

        Parameters
        ----------
        group
            The parameter group being validated.
        resolved_params
            Dictionary of resolved parameter values.
        """
        _ = group  # Unused but required by cyclopts validator protocol
        condition = resolved_params.get(self.condition_param)
        if condition == self.condition_value:
            configured = [
                param for param in self.disabled_params if resolved_params.get(param) is not None
            ]
            if configured:
                _LOGGER.warning(
                    "The following parameters are ignored when %s=%r: %s",
                    self.condition_param,
                    self.condition_value,
                    ", ".join(configured),
                )


__all__ = ["ConditionalDisabled", "ConditionalRequired"]

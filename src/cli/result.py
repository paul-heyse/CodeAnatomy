"""CLI result contract for structured command returns."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

from cli.exit_codes import ExitCode

if TYPE_CHECKING:
    from collections.abc import Mapping


@dataclass(frozen=True)
class CliResult:
    """Structured result from CLI command execution.

    Parameters
    ----------
    exit_code
        Integer exit code for the command.
    summary
        Optional human-readable summary of the result.
    artifacts
        Mapping of artifact names to file paths produced.
    metrics
        Mapping of metric names to numeric values.
    """

    exit_code: int
    summary: str | None = None
    artifacts: Mapping[str, Path] = field(default_factory=dict)
    metrics: Mapping[str, float] = field(default_factory=dict)

    @classmethod
    def success(
        cls,
        *,
        summary: str | None = None,
        artifacts: Mapping[str, Path] | None = None,
        metrics: Mapping[str, float] | None = None,
    ) -> CliResult:
        """Create a successful result.

        Parameters
        ----------
        summary
            Optional summary message.
        artifacts
            Optional artifact paths.
        metrics
            Optional metrics.

        Returns:
        -------
        CliResult
            Success result with exit code 0.
        """
        return cls(
            exit_code=ExitCode.SUCCESS,
            summary=summary,
            artifacts=artifacts or {},
            metrics=metrics or {},
        )

    @classmethod
    def error(
        cls,
        exit_code: ExitCode | int,
        *,
        summary: str | None = None,
    ) -> CliResult:
        """Create an error result.

        Parameters
        ----------
        exit_code
            Exit code for the error.
        summary
            Optional error summary.

        Returns:
        -------
        CliResult
            Error result with the specified exit code.
        """
        code = int(exit_code) if isinstance(exit_code, ExitCode) else exit_code
        return cls(
            exit_code=code,
            summary=summary,
            artifacts={},
            metrics={},
        )

    @classmethod
    def from_exception(
        cls,
        exc: BaseException,
        *,
        summary: str | None = None,
    ) -> CliResult:
        """Create an error result from an exception.

        Parameters
        ----------
        exc
            Exception that caused the error.
        summary
            Optional custom summary (defaults to exception message).

        Returns:
        -------
        CliResult
            Error result with exit code derived from exception type.
        """
        exit_code = ExitCode.from_exception(exc)
        return cls.error(
            exit_code,
            summary=summary or str(exc),
        )

    @property
    def ok(self) -> bool:
        """Check if the result indicates success.

        Returns:
        -------
        bool
            True if exit_code is 0.
        """
        return self.exit_code == ExitCode.SUCCESS


__all__ = ["CliResult"]

"""CLI context and result types for cq."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

from tools.cq.index.repo import resolve_repo_context

if TYPE_CHECKING:
    from tools.cq.cli_app.types import OutputFormat
    from tools.cq.core.toolchain import Toolchain


@dataclass
class CliContext:
    """Runtime context for CLI commands.

    Parameters
    ----------
    argv
        Original command-line arguments.
    root
        Resolved repository root path.
    toolchain
        Detected toolchain with available tools.
    verbose
        Verbosity level (0=normal, 1=verbose, 2+=debug).
    output_format
        Default output format from global options.
    artifact_dir
        Directory for saving artifacts, or None for default.
    save_artifact
        Whether to save artifacts (can be disabled via --no-save-artifact).
    """

    argv: list[str]
    root: Path
    toolchain: Toolchain
    verbose: int = 0
    output_format: OutputFormat | None = None
    artifact_dir: Path | None = None
    save_artifact: bool = True

    @classmethod
    def build(
        cls,
        argv: list[str],
        root: Path | None = None,
        verbose: int = 0,
        output_format: OutputFormat | None = None,
        artifact_dir: Path | None = None,
        save_artifact: bool = True,
    ) -> CliContext:
        """Build a CLI context from arguments.

        Parameters
        ----------
        argv
            Command-line arguments.
        root
            Optional explicit repository root.
        verbose
            Verbosity level.
        output_format
            Default output format.
        artifact_dir
            Directory for artifacts.
        save_artifact
            Whether to save artifacts.

        Returns
        -------
        CliContext
            Resolved context.
        """
        from tools.cq.core.toolchain import Toolchain

        # Resolve root from explicit arg, env var, or auto-detect
        if root is None:
            env_root = os.environ.get("CQ_ROOT")
            if env_root:
                root = Path(env_root)
            else:
                repo_context = resolve_repo_context()
                root = repo_context.repo_root

        root = root.resolve()
        toolchain = Toolchain.detect()

        return cls(
            argv=argv,
            root=root,
            toolchain=toolchain,
            verbose=verbose,
            output_format=output_format,
            artifact_dir=artifact_dir,
            save_artifact=save_artifact,
        )


@dataclass
class FilterConfig:
    """Configuration for result filtering.

    Parameters
    ----------
    include
        File include patterns.
    exclude
        File exclude patterns.
    impact
        Impact bucket filters.
    confidence
        Confidence bucket filters.
    severity
        Severity level filters.
    limit
        Maximum number of results.
    """

    include: list[str] = field(default_factory=list)
    exclude: list[str] = field(default_factory=list)
    impact: list[str] = field(default_factory=list)
    confidence: list[str] = field(default_factory=list)
    severity: list[str] = field(default_factory=list)
    limit: int | None = None

    @property
    def has_filters(self) -> bool:
        """Check if any filters are configured."""
        return bool(
            self.include
            or self.exclude
            or self.impact
            or self.confidence
            or self.severity
            or self.limit is not None
        )


@dataclass
class CliResult:
    """Result from a CLI command execution.

    Parameters
    ----------
    result
        The command result (CqResult or int exit code).
    context
        The CLI context used for execution.
    exit_code
        Optional explicit exit code.
    filters
        Filter configuration for result filtering.
    """

    result: Any
    context: CliContext
    exit_code: int | None = None
    filters: FilterConfig | None = None

    @property
    def is_cq_result(self) -> bool:
        """Check if result is a CqResult."""
        from tools.cq.core.schema import CqResult

        return isinstance(self.result, CqResult)

    def get_exit_code(self) -> int:
        """Get the exit code for this result.

        Returns
        -------
        int
            Exit code (0 for success).
        """
        if self.exit_code is not None:
            return self.exit_code
        if isinstance(self.result, int):
            return self.result
        return 0

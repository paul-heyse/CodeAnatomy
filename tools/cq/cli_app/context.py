"""CLI context and result types for cq."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, TypedDict, Unpack

from tools.cq.cli_app.options import CommonFilters
from tools.cq.core.structs import CqStruct
from tools.cq.index.repo import resolve_repo_context

if TYPE_CHECKING:
    from tools.cq.cli_app.types import OutputFormat
    from tools.cq.core.toolchain import Toolchain


class CliContextKwargs(TypedDict, total=False):
    """Keyword overrides for CLI context construction."""

    root: Path | None
    verbose: int
    output_format: OutputFormat | None
    artifact_dir: Path | None
    save_artifact: bool


@dataclass(frozen=True, slots=True)
class CliContextOptions:
    """Optional CLI context overrides."""

    root: Path | None = None
    verbose: int = 0
    output_format: OutputFormat | None = None
    artifact_dir: Path | None = None
    save_artifact: bool = True

    @classmethod
    def from_kwargs(cls, kwargs: CliContextKwargs) -> CliContextOptions:
        """Build options from legacy keyword arguments.

        Returns
        -------
        CliContextOptions
            Normalized option bundle.
        """
        return cls(
            root=kwargs.get("root"),
            verbose=int(kwargs.get("verbose", 0)),
            output_format=kwargs.get("output_format"),
            artifact_dir=kwargs.get("artifact_dir"),
            save_artifact=bool(kwargs.get("save_artifact", True)),
        )


class CliContext(CqStruct, frozen=True):
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
        options: CliContextOptions | None = None,
        **kwargs: Unpack[CliContextKwargs],
    ) -> CliContext:
        """Build a CLI context from arguments.

        Parameters
        ----------
        argv
            Command-line arguments.
        options
            Pre-constructed CLI context options.
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

        Raises
        ------
        ValueError
            If both options and legacy keyword overrides are provided.
        """
        from tools.cq.core.toolchain import Toolchain

        if options is None:
            options = CliContextOptions.from_kwargs(kwargs)
        elif kwargs:
            msg = "Pass either options or legacy keyword overrides, not both."
            raise ValueError(msg)

        # Resolve root from explicit arg or auto-detect
        root = options.root
        if root is None:
            repo_context = resolve_repo_context()
            root = repo_context.repo_root

        root = root.resolve()
        toolchain = Toolchain.detect()

        return cls(
            argv=argv,
            root=root,
            toolchain=toolchain,
            verbose=options.verbose,
            output_format=options.output_format,
            artifact_dir=options.artifact_dir,
            save_artifact=options.save_artifact,
        )


FilterConfig = CommonFilters


class CliResult(CqStruct, frozen=True):
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

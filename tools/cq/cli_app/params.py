"""Parameter groups for cq CLI commands.

These dataclasses define reusable parameter groups that can be
flattened into command signatures.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Annotated

from cyclopts import Parameter

if TYPE_CHECKING:
    from tools.cq.cli_app.context import FilterConfig

from tools.cq.cli_app.types import (
    ConfidenceBucket,
    ImpactBucket,
    OutputFormat,
    SeverityLevel,
    comma_separated_enum,
    comma_separated_list,
)


def _get_default_root() -> Path | None:
    """Get default root from environment.

    Returns
    -------
    Path | None
        Root from CQ_ROOT env var, or None.
    """
    env_root = os.environ.get("CQ_ROOT")
    return Path(env_root) if env_root else None


@dataclass
class CommonOptions:
    """Common options shared by most commands.

    These are flattened into the command signature.
    """

    root: Annotated[
        Path | None,
        Parameter(
            name="--root",
            help="Repository root (default: auto-detect from CQ_ROOT or git)",
        ),
    ] = field(default_factory=_get_default_root)

    output_format: Annotated[
        OutputFormat,
        Parameter(
            name="--format",
            help="Output format (md, json, both, summary, mermaid, mermaid-class, dot)",
        ),
    ] = OutputFormat.md

    artifact_dir: Annotated[
        str | None,
        Parameter(
            name="--artifact-dir",
            help="Directory for JSON artifacts (default: .cq/artifacts)",
        ),
    ] = None

    no_save_artifact: Annotated[
        bool,
        Parameter(
            name="--no-save-artifact",
            help="Don't save JSON artifact",
        ),
    ] = False


@dataclass
class FilterOptions:
    """Filter options for result filtering.

    These are flattened into the command signature via Parameter(name="*").
    """

    include: Annotated[
        list[str],
        Parameter(
            name="--include",
            help="Include files matching pattern (glob or ~regex, repeatable)",
            converter=comma_separated_list(str),
        ),
    ] = field(default_factory=list)

    exclude: Annotated[
        list[str],
        Parameter(
            name="--exclude",
            help="Exclude files matching pattern (glob or ~regex, repeatable)",
            converter=comma_separated_list(str),
        ),
    ] = field(default_factory=list)

    impact: Annotated[
        list[ImpactBucket],
        Parameter(
            name="--impact",
            help="Filter by impact bucket (comma-separated: low,med,high)",
            converter=comma_separated_enum(ImpactBucket),
        ),
    ] = field(default_factory=list)

    confidence: Annotated[
        list[ConfidenceBucket],
        Parameter(
            name="--confidence",
            help="Filter by confidence bucket (comma-separated: low,med,high)",
            converter=comma_separated_enum(ConfidenceBucket),
        ),
    ] = field(default_factory=list)

    severity: Annotated[
        list[SeverityLevel],
        Parameter(
            name="--severity",
            help="Filter by severity (comma-separated: error,warning,info)",
            converter=comma_separated_enum(SeverityLevel),
        ),
    ] = field(default_factory=list)

    limit: Annotated[
        int | None,
        Parameter(
            name="--limit",
            help="Maximum number of findings",
        ),
    ] = None

    def to_filter_config(self) -> FilterConfig:
        """Convert to FilterConfig for result handling.

        Returns
        -------
        FilterConfig
            Filter configuration with string values.
        """
        from tools.cq.cli_app.context import FilterConfig

        return FilterConfig(
            include=self.include,
            exclude=self.exclude,
            impact=[str(b) for b in self.impact],
            confidence=[str(b) for b in self.confidence],
            severity=[str(s) for s in self.severity],
            limit=self.limit,
        )

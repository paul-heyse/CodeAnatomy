"""Parameter groups for cq CLI commands.

These dataclasses define reusable parameter groups that can be
flattened into command signatures.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Annotated

from cyclopts import Parameter

from tools.cq.cli_app.types import (
    ConfidenceBucket,
    ImpactBucket,
    SeverityLevel,
    comma_separated_enum,
    comma_separated_list,
)


@dataclass(kw_only=True)
class FilterParams:
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


@dataclass(kw_only=True)
class QueryParams(FilterParams):
    """Options for the q query command."""

    explain_files: Annotated[
        bool, Parameter(name="--explain-files", help="Include file filtering diagnostics")
    ] = False


@dataclass(kw_only=True)
class SearchParams(FilterParams):
    """Options for the search command."""

    regex: Annotated[bool, Parameter(name="--regex", help="Treat query as regex")] = False
    literal: Annotated[bool, Parameter(name="--literal", help="Treat query as literal")] = False
    include_strings: Annotated[
        bool,
        Parameter(
            name="--include-strings",
            help="Include matches in strings/comments/docstrings",
        ),
    ] = False
    in_dir: Annotated[str | None, Parameter(name="--in", help="Restrict to directory")] = None


@dataclass(kw_only=True)
class ReportParams(FilterParams):
    """Options for the report command."""

    target: Annotated[
        str,
        Parameter(
            help="Target spec (function:foo, class:Bar, module:pkg.mod, path:src/...)",
        ),
    ]
    in_dir: Annotated[
        str | None, Parameter(name="--in", help="Restrict analysis to a directory")
    ] = None
    param: Annotated[str | None, Parameter(help="Parameter name for impact analysis")] = None
    signature: Annotated[
        str | None, Parameter(name="--to", help="Proposed signature for sig-impact analysis")
    ] = None
    bytecode_show: Annotated[
        str | None, Parameter(name="--bytecode-show", help="Bytecode surface fields")
    ] = None


@dataclass(kw_only=True)
class ImpactParams(FilterParams):
    """Options for the impact command."""

    param: Annotated[str, Parameter(help="Parameter name to trace")]
    depth: Annotated[int, Parameter(help="Maximum call depth")] = 5


@dataclass(kw_only=True)
class ImportsParams(FilterParams):
    """Options for the imports command."""

    cycles: Annotated[bool, Parameter(help="Run cycle detection")] = False
    module: Annotated[str | None, Parameter(help="Focus on specific module")] = None


@dataclass(kw_only=True)
class ExceptionsParams(FilterParams):
    """Options for the exceptions command."""

    function: Annotated[str | None, Parameter(help="Focus on specific function")] = None


@dataclass(kw_only=True)
class SigImpactParams(FilterParams):
    """Options for the sig-impact command."""

    to: Annotated[str, Parameter(help='New signature (e.g., "foo(a, b, *, c=None)")')]


@dataclass(kw_only=True)
class SideEffectsParams(FilterParams):
    """Options for the side-effects command."""

    max_files: Annotated[int, Parameter(help="Maximum files to scan")] = 2000


@dataclass(kw_only=True)
class BytecodeSurfaceParams(FilterParams):
    """Options for the bytecode-surface command."""

    show: Annotated[str, Parameter(help="What to show: globals,attrs,constants,opcodes")] = (
        "globals,attrs,constants"
    )

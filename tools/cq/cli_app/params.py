"""Parameter groups for cq CLI commands.

These dataclasses define reusable parameter groups that can be
flattened into command signatures.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Annotated

from cyclopts import Group, Parameter, validators

from tools.cq.cli_app.types import (
    ConfidenceBucket,
    ImpactBucket,
    QueryLanguageToken,
    SeverityLevel,
    _converter_value,
    comma_separated_list,
)
from tools.cq.run.spec import RunStep
from tools.cq.run.step_decode import parse_run_step_json, parse_run_steps_json

search_mode = Group("Search Mode", validator=validators.mutually_exclusive)
filter_group = Group(
    "Filters",
    default_parameter=Parameter(
        show_choices=True,
        negative_iterable=(),
        show_env_var=False,
    ),
)
run_input = Group(
    "Run Input",
    validator=validators.LimitedChoice(min=1, max=3),
    default_parameter=Parameter(
        negative_iterable=(),
        show_env_var=False,
    ),
)

_LIMIT_VALIDATOR = validators.Number(gte=1, lte=1_000_000)
_DEPTH_VALIDATOR = validators.Number(gte=1, lte=10_000)
_MAX_FILES_VALIDATOR = validators.Number(gte=1, lte=1_000_000)


def _token_value_for_converter(*args: object) -> object:
    return _converter_value(args)


def _iter_converter_texts(*args: object) -> list[str]:
    value = _token_value_for_converter(*args)
    items = value if isinstance(value, (list, tuple)) else [value]
    texts: list[str] = []
    for item in items:
        token_value = getattr(item, "value", item)
        if isinstance(token_value, str):
            texts.append(token_value)
    return texts


def _run_step_converter(*args: object) -> list[RunStep]:
    return [parse_run_step_json(raw) for raw in _iter_converter_texts(*args)]


def _run_steps_converter(*args: object) -> list[RunStep]:
    parsed: list[RunStep] = []
    for raw in _iter_converter_texts(*args):
        text = raw.lstrip()
        if text.startswith("["):
            parsed.extend(parse_run_steps_json(raw))
        else:
            parsed.append(parse_run_step_json(raw))
    return parsed


@dataclass(kw_only=True)
class FilterParams:
    """Filter options for result filtering.

    These are flattened into the command signature via Parameter(name="*").
    """

    include: Annotated[
        list[str],
        Parameter(
            name="--include",
            group=filter_group,
            help="Include files matching pattern (glob or ~regex, repeatable)",
            consume_multiple=True,
            converter=comma_separated_list(str),
        ),
    ] = field(default_factory=list)

    exclude: Annotated[
        list[str],
        Parameter(
            name="--exclude",
            group=filter_group,
            help="Exclude files matching pattern (glob or ~regex, repeatable)",
            consume_multiple=True,
            converter=comma_separated_list(str),
        ),
    ] = field(default_factory=list)

    impact: Annotated[
        list[ImpactBucket],
        Parameter(
            name="--impact",
            group=filter_group,
            help="Filter by impact bucket (comma-separated: low,med,high)",
            consume_multiple=True,
            converter=comma_separated_list(ImpactBucket),
        ),
    ] = field(default_factory=list)

    confidence: Annotated[
        list[ConfidenceBucket],
        Parameter(
            name="--confidence",
            group=filter_group,
            help="Filter by confidence bucket (comma-separated: low,med,high)",
            consume_multiple=True,
            converter=comma_separated_list(ConfidenceBucket),
        ),
    ] = field(default_factory=list)

    severity: Annotated[
        list[SeverityLevel],
        Parameter(
            name="--severity",
            group=filter_group,
            help="Filter by severity (comma-separated: error,warning,info)",
            consume_multiple=True,
            converter=comma_separated_list(SeverityLevel),
        ),
    ] = field(default_factory=list)

    limit: Annotated[
        int | None,
        Parameter(
            name="--limit",
            group=filter_group,
            validator=_LIMIT_VALIDATOR,
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

    regex: Annotated[
        bool,
        Parameter(
            name="--regex",
            help="Treat query as regex",
            group=search_mode,
        ),
    ] = False
    literal: Annotated[
        bool,
        Parameter(
            name="--literal",
            help="Treat query as literal",
            group=search_mode,
        ),
    ] = False
    include_strings: Annotated[
        bool,
        Parameter(
            name="--include-strings",
            help="Include matches in strings/comments/docstrings",
        ),
    ] = False
    with_neighborhood: Annotated[
        bool,
        Parameter(
            name="--with-neighborhood",
            help="Include structural neighborhood preview (slower)",
        ),
    ] = False
    in_dir: Annotated[str | None, Parameter(name="--in", help="Restrict to directory")] = None
    lang: Annotated[
        QueryLanguageToken,
        Parameter(name="--lang", help="Search language scope (auto, python, rust)"),
    ] = QueryLanguageToken.auto


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
    depth: Annotated[int, Parameter(help="Maximum call depth", validator=_DEPTH_VALIDATOR)] = 5


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

    max_files: Annotated[
        int, Parameter(help="Maximum files to scan", validator=_MAX_FILES_VALIDATOR)
    ] = 2000


@dataclass(kw_only=True)
class BytecodeSurfaceParams(FilterParams):
    """Options for the bytecode-surface command."""

    show: Annotated[str, Parameter(help="What to show: globals,attrs,constants,opcodes")] = (
        "globals,attrs,constants"
    )


@dataclass(kw_only=True)
class RunParams(FilterParams):
    """Options for the run command."""

    plan: Annotated[
        Path | None,
        Parameter(
            name="--plan",
            group=run_input,
            validator=validators.Path(exists=True, file_okay=True, dir_okay=False),
            help="Path to a run plan TOML file",
        ),
    ] = None
    step: Annotated[
        list[RunStep],
        Parameter(
            name="--step",
            group=run_input,
            n_tokens=1,
            accepts_keys=False,
            converter=_run_step_converter,
            help='Repeatable JSON step object (e.g., \'{"type":"q","query":"..."}\')',
        ),
    ] = field(default_factory=list)
    steps: Annotated[
        list[RunStep],
        Parameter(
            name="--steps",
            group=run_input,
            n_tokens=1,
            accepts_keys=False,
            converter=_run_steps_converter,
            help='JSON array of steps (e.g., \'[{"type":"q",...},{"type":"calls",...}]\')',
        ),
    ] = field(default_factory=list)
    stop_on_error: Annotated[
        bool,
        Parameter(
            name="--stop-on-error",
            help="Stop execution on the first step error",
        ),
    ] = False

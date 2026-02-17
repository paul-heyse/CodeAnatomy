"""Parameter groups for cq CLI commands.

Cyclopts-compatible dataclass params are generated from the canonical command schema
(`tools.cq.cli_app.command_schema`) so params/options stay in lockstep.
"""

from __future__ import annotations

from dataclasses import field, make_dataclass
from typing import TYPE_CHECKING, Annotated, get_type_hints

import msgspec
from cyclopts import Group, Parameter, validators

from tools.cq.cli_app.command_schema import (
    BytecodeSurfaceCommandSchema,
    CommonFilterCommandSchema,
    ExceptionsCommandSchema,
    ImpactCommandSchema,
    ImportsCommandSchema,
    NeighborhoodCommandSchema,
    QueryCommandSchema,
    ReportCommandSchema,
    RunCommandSchema,
    SearchCommandSchema,
    SideEffectsCommandSchema,
    SigImpactCommandSchema,
)
from tools.cq.cli_app.types import (
    ConfidenceBucket,
    ImpactBucket,
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


def _schema_struct_fields(schema_type: type[object]) -> tuple[str, ...]:
    raw_fields = getattr(schema_type, "__struct_fields__", None)
    if not isinstance(raw_fields, tuple):
        msg = f"{schema_type.__name__} is missing msgspec __struct_fields__"
        raise TypeError(msg)
    if not all(isinstance(field_name, str) for field_name in raw_fields):
        msg = f"{schema_type.__name__} has non-string struct field names"
        raise TypeError(msg)
    return tuple(raw_fields)


def _schema_defaults(schema_type: type[object]) -> dict[str, object]:
    defaults = getattr(schema_type, "__struct_defaults__", None)
    if not isinstance(defaults, tuple):
        msg = f"{schema_type.__name__} is missing msgspec __struct_defaults__"
        raise TypeError(msg)
    fields = _schema_struct_fields(schema_type)
    if len(fields) != len(defaults):
        msg = (
            f"{schema_type.__name__} has mismatched struct metadata: "
            f"{len(fields)} fields vs {len(defaults)} defaults"
        )
        raise ValueError(msg)
    return dict(zip(fields, defaults, strict=True))


def _to_dataclass_default(default: object) -> object:
    if default is msgspec.NODEFAULT:
        return msgspec.NODEFAULT
    factory = getattr(default, "factory", None)
    if callable(factory):
        return field(default_factory=factory)
    return default


def _build_params_class(
    *,
    name: str,
    schema_type: type[object],
    field_parameters: dict[str, Parameter],
    base: type[object] | None = None,
    doc: str,
) -> type[object]:
    schema_hints = get_type_hints(schema_type, include_extras=True)
    schema_defaults = _schema_defaults(schema_type)
    schema_fields = _schema_struct_fields(schema_type)
    inherited_fields = set(getattr(base, "__dataclass_fields__", {}).keys()) if base else set()

    class_fields: list[tuple[str, object] | tuple[str, object, object]] = []
    for field_name in schema_fields:
        if field_name in inherited_fields:
            continue
        parameter = field_parameters.get(field_name)
        if parameter is None:
            msg = f"Missing cyclopts parameter metadata for {name}.{field_name}"
            raise ValueError(msg)
        annotated_type = Annotated[schema_hints[field_name], parameter]
        dataclass_default = _to_dataclass_default(schema_defaults[field_name])
        if dataclass_default is msgspec.NODEFAULT:
            class_fields.append((field_name, annotated_type))
        else:
            class_fields.append((field_name, annotated_type, dataclass_default))

    bases = (base,) if base is not None else ()
    generated = make_dataclass(name, class_fields, bases=bases, kw_only=True)
    generated.__module__ = __name__
    generated.__doc__ = doc
    return generated


_FILTER_PARAM_FIELDS: dict[str, Parameter] = {
    "include": Parameter(
        name="--include",
        group=filter_group,
        help="Include files matching pattern (glob or ~regex, repeatable)",
        consume_multiple=True,
        converter=comma_separated_list(str),
    ),
    "exclude": Parameter(
        name="--exclude",
        group=filter_group,
        help="Exclude files matching pattern (glob or ~regex, repeatable)",
        consume_multiple=True,
        converter=comma_separated_list(str),
    ),
    "impact": Parameter(
        name="--impact",
        group=filter_group,
        help="Filter by impact bucket (comma-separated: low,med,high)",
        consume_multiple=True,
        converter=comma_separated_list(ImpactBucket),
    ),
    "confidence": Parameter(
        name="--confidence",
        group=filter_group,
        help="Filter by confidence bucket (comma-separated: low,med,high)",
        consume_multiple=True,
        converter=comma_separated_list(ConfidenceBucket),
    ),
    "severity": Parameter(
        name="--severity",
        group=filter_group,
        help="Filter by severity (comma-separated: error,warning,info)",
        consume_multiple=True,
        converter=comma_separated_list(SeverityLevel),
    ),
    "limit": Parameter(
        name="--limit",
        group=filter_group,
        validator=_LIMIT_VALIDATOR,
        help="Maximum number of findings",
    ),
}

_QUERY_PARAM_FIELDS: dict[str, Parameter] = {
    "explain_files": Parameter(name="--explain-files", help="Include file filtering diagnostics"),
}

_SEARCH_PARAM_FIELDS: dict[str, Parameter] = {
    "regex": Parameter(
        name="--regex",
        help="Treat query as regex",
        group=search_mode,
    ),
    "literal": Parameter(
        name="--literal",
        help="Treat query as literal",
        group=search_mode,
    ),
    "include_strings": Parameter(
        name="--include-strings",
        help="Include matches in strings/comments/docstrings",
    ),
    "with_neighborhood": Parameter(
        name="--with-neighborhood",
        help="Include structural neighborhood preview (slower)",
    ),
    "enrich": Parameter(
        name="--enrich",
        negative="--no-enrich",
        negative_bool=(),
        help="Enable incremental enrichment",
    ),
    "enrich_mode": Parameter(
        name="--enrich-mode",
        help="Incremental enrichment mode (ts_only, ts_sym, ts_sym_dis, full)",
    ),
    "in_dir": Parameter(name="--in", help="Restrict to directory"),
    "lang": Parameter(name="--lang", help="Search language scope (auto, python, rust)"),
}

_REPORT_PARAM_FIELDS: dict[str, Parameter] = {
    "target": Parameter(
        help="Target spec (function:foo, class:Bar, module:pkg.mod, path:src/...)",
    ),
    "in_dir": Parameter(name="--in", help="Restrict analysis to a directory"),
    "param": Parameter(help="Parameter name for impact analysis"),
    "signature": Parameter(name="--to", help="Proposed signature for sig-impact analysis"),
    "bytecode_show": Parameter(name="--bytecode-show", help="Bytecode surface fields"),
}

_IMPACT_PARAM_FIELDS: dict[str, Parameter] = {
    "param": Parameter(help="Parameter name to trace"),
    "depth": Parameter(help="Maximum call depth", validator=_DEPTH_VALIDATOR),
}

_IMPORTS_PARAM_FIELDS: dict[str, Parameter] = {
    "cycles": Parameter(help="Run cycle detection"),
    "module": Parameter(help="Focus on specific module"),
}

_EXCEPTIONS_PARAM_FIELDS: dict[str, Parameter] = {
    "function": Parameter(help="Focus on specific function"),
}

_SIG_IMPACT_PARAM_FIELDS: dict[str, Parameter] = {
    "to": Parameter(help='New signature (e.g., "foo(a, b, *, c=None)")'),
}

_SIDE_EFFECTS_PARAM_FIELDS: dict[str, Parameter] = {
    "max_files": Parameter(help="Maximum files to scan", validator=_MAX_FILES_VALIDATOR),
}

_BYTECODE_PARAM_FIELDS: dict[str, Parameter] = {
    "show": Parameter(help="What to show: globals,attrs,constants,opcodes"),
}

_RUN_PARAM_FIELDS: dict[str, Parameter] = {
    "plan": Parameter(
        name="--plan",
        group=run_input,
        validator=validators.Path(exists=True, file_okay=True, dir_okay=False),
        help="Path to a run plan TOML file",
    ),
    "step": Parameter(
        name="--step",
        group=run_input,
        n_tokens=1,
        accepts_keys=False,
        converter=_run_step_converter,
        help='Repeatable JSON step object (e.g., \'{"type":"q","query":"..."}\')',
    ),
    "steps": Parameter(
        name="--steps",
        group=run_input,
        n_tokens=1,
        accepts_keys=False,
        converter=_run_steps_converter,
        help='JSON array of steps (e.g., \'[{"type":"q",...},{"type":"calls",...}]\')',
    ),
    "stop_on_error": Parameter(
        name="--stop-on-error",
        help="Stop execution on the first step error",
    ),
}

_NEIGHBORHOOD_PARAM_FIELDS: dict[str, Parameter] = {
    "lang": Parameter(name="--lang", help="Query language (python, rust)"),
    "top_k": Parameter(
        name="--top-k",
        help="Max items per slice",
        validator=validators.Number(gte=1, lte=10_000),
    ),
    "semantic_enrichment": Parameter(
        name="--semantic-enrichment",
        negative="--no-semantic-enrichment",
        negative_bool=(),
        help="Enable semantic enrichment",
    ),
    "enrich": Parameter(
        name="--enrich",
        negative="--no-enrich",
        negative_bool=(),
        help="Enable incremental enrichment for neighborhood target analysis",
    ),
    "enrich_mode": Parameter(
        name="--enrich-mode",
        help="Incremental enrichment mode (ts_only, ts_sym, ts_sym_dis, full)",
    ),
}

if TYPE_CHECKING:
    FilterParams = CommonFilterCommandSchema
    QueryParams = QueryCommandSchema
    SearchParams = SearchCommandSchema
    ReportParams = ReportCommandSchema
    ImpactParams = ImpactCommandSchema
    ImportsParams = ImportsCommandSchema
    ExceptionsParams = ExceptionsCommandSchema
    SigImpactParams = SigImpactCommandSchema
    SideEffectsParams = SideEffectsCommandSchema
    BytecodeSurfaceParams = BytecodeSurfaceCommandSchema
    RunParams = RunCommandSchema
    NeighborhoodParams = NeighborhoodCommandSchema
else:
    FilterParams = _build_params_class(
        name="FilterParams",
        schema_type=CommonFilterCommandSchema,
        field_parameters=_FILTER_PARAM_FIELDS,
        base=None,
        doc="Filter options for result filtering.",
    )
    QueryParams = _build_params_class(
        name="QueryParams",
        schema_type=QueryCommandSchema,
        field_parameters=_QUERY_PARAM_FIELDS,
        base=FilterParams,
        doc="Options for the q query command.",
    )
    SearchParams = _build_params_class(
        name="SearchParams",
        schema_type=SearchCommandSchema,
        field_parameters=_SEARCH_PARAM_FIELDS,
        base=FilterParams,
        doc="Options for the search command.",
    )
    ReportParams = _build_params_class(
        name="ReportParams",
        schema_type=ReportCommandSchema,
        field_parameters=_REPORT_PARAM_FIELDS,
        base=FilterParams,
        doc="Options for the report command.",
    )
    ImpactParams = _build_params_class(
        name="ImpactParams",
        schema_type=ImpactCommandSchema,
        field_parameters=_IMPACT_PARAM_FIELDS,
        base=FilterParams,
        doc="Options for the impact command.",
    )
    ImportsParams = _build_params_class(
        name="ImportsParams",
        schema_type=ImportsCommandSchema,
        field_parameters=_IMPORTS_PARAM_FIELDS,
        base=FilterParams,
        doc="Options for the imports command.",
    )
    ExceptionsParams = _build_params_class(
        name="ExceptionsParams",
        schema_type=ExceptionsCommandSchema,
        field_parameters=_EXCEPTIONS_PARAM_FIELDS,
        base=FilterParams,
        doc="Options for the exceptions command.",
    )
    SigImpactParams = _build_params_class(
        name="SigImpactParams",
        schema_type=SigImpactCommandSchema,
        field_parameters=_SIG_IMPACT_PARAM_FIELDS,
        base=FilterParams,
        doc="Options for the sig-impact command.",
    )
    SideEffectsParams = _build_params_class(
        name="SideEffectsParams",
        schema_type=SideEffectsCommandSchema,
        field_parameters=_SIDE_EFFECTS_PARAM_FIELDS,
        base=FilterParams,
        doc="Options for the side-effects command.",
    )
    BytecodeSurfaceParams = _build_params_class(
        name="BytecodeSurfaceParams",
        schema_type=BytecodeSurfaceCommandSchema,
        field_parameters=_BYTECODE_PARAM_FIELDS,
        base=FilterParams,
        doc="Options for the bytecode-surface command.",
    )
    RunParams = _build_params_class(
        name="RunParams",
        schema_type=RunCommandSchema,
        field_parameters=_RUN_PARAM_FIELDS,
        base=FilterParams,
        doc="Options for the run command.",
    )
    NeighborhoodParams = _build_params_class(
        name="NeighborhoodParams",
        schema_type=NeighborhoodCommandSchema,
        field_parameters=_NEIGHBORHOOD_PARAM_FIELDS,
        base=None,
        doc="Options for the neighborhood command.",
    )

__all__ = [
    "BytecodeSurfaceParams",
    "ExceptionsParams",
    "FilterParams",
    "ImpactParams",
    "ImportsParams",
    "NeighborhoodParams",
    "QueryParams",
    "ReportParams",
    "RunParams",
    "SearchParams",
    "SideEffectsParams",
    "SigImpactParams",
    "filter_group",
    "run_input",
    "search_mode",
]

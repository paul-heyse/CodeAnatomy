"""Projection utilities from CLI params into canonical command schemas/options."""

from __future__ import annotations

from typing import Any

from tools.cq.cli_app.command_schema import (
    BytecodeSurfaceCommandSchema,
    CommonFilterCommandSchema,
    ExceptionsCommandSchema,
    ImpactCommandSchema,
    ImportsCommandSchema,
    QueryCommandSchema,
    ReportCommandSchema,
    RunCommandSchema,
    SearchCommandSchema,
    SideEffectsCommandSchema,
    SigImpactCommandSchema,
)
from tools.cq.cli_app.options import (
    BytecodeSurfaceOptions,
    CommonFilters,
    ExceptionsOptions,
    ImpactOptions,
    ImportsOptions,
    QueryOptions,
    ReportOptions,
    RunOptions,
    SearchOptions,
    SideEffectsOptions,
    SigImpactOptions,
)
from tools.cq.cli_app.params import (
    BytecodeSurfaceParams,
    ExceptionsParams,
    FilterParams,
    ImpactParams,
    ImportsParams,
    QueryParams,
    ReportParams,
    RunParams,
    SearchParams,
    SideEffectsParams,
    SigImpactParams,
)
from tools.cq.core.typed_boundary import convert_strict


def _project[SchemaT](params: Any, *, schema_type: type[SchemaT]) -> SchemaT:
    return convert_strict(params, type_=schema_type, from_attributes=True)


def _options[SchemaT, OptionsT](
    params: Any,
    *,
    schema_type: type[SchemaT],
    options_type: type[OptionsT],
) -> OptionsT:
    schema = _project(params, schema_type=schema_type)
    return convert_strict(schema, type_=options_type, from_attributes=True)


def project_filter_params(params: FilterParams | None) -> CommonFilterCommandSchema:
    return _project(params or FilterParams(), schema_type=CommonFilterCommandSchema)


def filter_options_from_projected_params(params: FilterParams | None) -> CommonFilters:
    return _options(
        params or FilterParams(),
        schema_type=CommonFilterCommandSchema,
        options_type=CommonFilters,
    )


def project_query_params(params: QueryParams | None) -> QueryCommandSchema:
    return _project(params or QueryParams(), schema_type=QueryCommandSchema)


def query_options_from_projected_params(params: QueryParams | None) -> QueryOptions:
    return _options(params or QueryParams(), schema_type=QueryCommandSchema, options_type=QueryOptions)


def project_search_params(params: SearchParams | None) -> SearchCommandSchema:
    return _project(params or SearchParams(), schema_type=SearchCommandSchema)


def search_options_from_projected_params(params: SearchParams | None) -> SearchOptions:
    return _options(
        params or SearchParams(),
        schema_type=SearchCommandSchema,
        options_type=SearchOptions,
    )


def project_report_params(params: ReportParams) -> ReportCommandSchema:
    return _project(params, schema_type=ReportCommandSchema)


def report_options_from_projected_params(params: ReportParams) -> ReportOptions:
    return _options(params, schema_type=ReportCommandSchema, options_type=ReportOptions)


def project_impact_params(params: ImpactParams) -> ImpactCommandSchema:
    return _project(params, schema_type=ImpactCommandSchema)


def impact_options_from_projected_params(params: ImpactParams) -> ImpactOptions:
    return _options(params, schema_type=ImpactCommandSchema, options_type=ImpactOptions)


def project_imports_params(params: ImportsParams | None) -> ImportsCommandSchema:
    return _project(params or ImportsParams(), schema_type=ImportsCommandSchema)


def imports_options_from_projected_params(params: ImportsParams | None) -> ImportsOptions:
    return _options(
        params or ImportsParams(),
        schema_type=ImportsCommandSchema,
        options_type=ImportsOptions,
    )


def project_exceptions_params(params: ExceptionsParams | None) -> ExceptionsCommandSchema:
    return _project(params or ExceptionsParams(), schema_type=ExceptionsCommandSchema)


def exceptions_options_from_projected_params(
    params: ExceptionsParams | None,
) -> ExceptionsOptions:
    return _options(
        params or ExceptionsParams(),
        schema_type=ExceptionsCommandSchema,
        options_type=ExceptionsOptions,
    )


def project_sig_impact_params(params: SigImpactParams) -> SigImpactCommandSchema:
    return _project(params, schema_type=SigImpactCommandSchema)


def sig_impact_options_from_projected_params(params: SigImpactParams) -> SigImpactOptions:
    return _options(params, schema_type=SigImpactCommandSchema, options_type=SigImpactOptions)


def project_side_effects_params(params: SideEffectsParams | None) -> SideEffectsCommandSchema:
    return _project(params or SideEffectsParams(), schema_type=SideEffectsCommandSchema)


def side_effects_options_from_projected_params(
    params: SideEffectsParams | None,
) -> SideEffectsOptions:
    return _options(
        params or SideEffectsParams(),
        schema_type=SideEffectsCommandSchema,
        options_type=SideEffectsOptions,
    )


def project_bytecode_surface_params(
    params: BytecodeSurfaceParams | None,
) -> BytecodeSurfaceCommandSchema:
    return _project(params or BytecodeSurfaceParams(), schema_type=BytecodeSurfaceCommandSchema)


def bytecode_surface_options_from_projected_params(
    params: BytecodeSurfaceParams | None,
) -> BytecodeSurfaceOptions:
    return _options(
        params or BytecodeSurfaceParams(),
        schema_type=BytecodeSurfaceCommandSchema,
        options_type=BytecodeSurfaceOptions,
    )


def project_run_params(params: RunParams | None) -> RunCommandSchema:
    return _project(params or RunParams(), schema_type=RunCommandSchema)


def run_options_from_projected_params(params: RunParams | None) -> RunOptions:
    return _options(params or RunParams(), schema_type=RunCommandSchema, options_type=RunOptions)


__all__ = [
    "bytecode_surface_options_from_projected_params",
    "exceptions_options_from_projected_params",
    "filter_options_from_projected_params",
    "impact_options_from_projected_params",
    "imports_options_from_projected_params",
    "query_options_from_projected_params",
    "report_options_from_projected_params",
    "run_options_from_projected_params",
    "search_options_from_projected_params",
    "side_effects_options_from_projected_params",
    "sig_impact_options_from_projected_params",
]

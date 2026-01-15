"""Core spec-table helpers and shared Arrow structs."""

from __future__ import annotations

import importlib
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from functools import cache
from typing import TYPE_CHECKING, TypedDict, Unpack, cast

import pyarrow as pa

from arrowdsl.compute.expr_core import ExprSpec, or_exprs
from arrowdsl.core.context import ExecutionContext
from arrowdsl.core.interop import ComputeExpression, SchemaLike, TableLike, ensure_expression, pc
from arrowdsl.plan.query import QuerySpec
from arrowdsl.schema.schema import SchemaEvolutionSpec, SchemaMetadataSpec, SchemaTransform
from arrowdsl.schema.validation import ArrowValidationOptions, ValidationReport, validate_table
from arrowdsl.spec.tables.base import SpecTableCodec
from schema_spec.specs import ArrowFieldSpec, DerivedFieldSpec, FieldBundle, TableSchemaSpec
from schema_spec.system import (
    ContractSpec,
    DatasetSpec,
    SchemaRegistry,
    make_dataset_spec,
    make_table_spec,
    register_dataset_spec,
    table_spec_from_schema,
)

if TYPE_CHECKING:
    from arrowdsl.plan.plan import Plan


@cache
def _plan_class() -> type[Plan]:
    module = importlib.import_module("arrowdsl.plan.plan")
    return cast("type[Plan]", module.Plan)


SORT_KEY_STRUCT = pa.struct(
    [
        pa.field("column", pa.string(), nullable=False),
        pa.field("order", pa.string(), nullable=False),
    ]
)

DEDUPE_STRUCT = pa.struct(
    [
        pa.field("keys", pa.list_(pa.string()), nullable=False),
        pa.field("tie_breakers", pa.list_(SORT_KEY_STRUCT), nullable=True),
        pa.field("strategy", pa.string(), nullable=False),
    ]
)

VALIDATION_STRUCT = pa.struct(
    [
        pa.field("strict", pa.string(), nullable=False),
        pa.field("coerce", pa.bool_(), nullable=False),
        pa.field("max_errors", pa.int64(), nullable=True),
        pa.field("emit_invalid_rows", pa.bool_(), nullable=False),
        pa.field("emit_error_table", pa.bool_(), nullable=False),
    ]
)

SCALAR_UNION_FIELDS = (
    pa.field("null", pa.null()),
    pa.field("bool", pa.bool_()),
    pa.field("int", pa.int64()),
    pa.field("float", pa.float64()),
    pa.field("string", pa.string()),
    pa.field("binary", pa.binary()),
)

SCALAR_UNION_TYPE = pa.union(list(SCALAR_UNION_FIELDS), mode="dense")

DATASET_REF_STRUCT = pa.struct(
    [
        pa.field("name", pa.string(), nullable=False),
        pa.field("label", pa.string(), nullable=False),
        pa.field("query_json", pa.string(), nullable=True),
    ]
)


@dataclass(frozen=True)
class SpecTableSpec:
    """Schema definition and constraints for a spec table."""

    name: str
    schema: SchemaLike
    required_non_null: tuple[str, ...] = ()
    key_fields: tuple[str, ...] = ()
    metadata_spec: SchemaMetadataSpec = field(default_factory=SchemaMetadataSpec)

    def schema_with_metadata(self) -> SchemaLike:
        """Return the schema with metadata applied.

        Returns
        -------
        SchemaLike
            Schema with metadata merged in.
        """
        return self.metadata_spec.apply(self.schema)

    def table_spec(self) -> TableSchemaSpec:
        """Return a TableSchemaSpec with constraints applied.

        Returns
        -------
        TableSchemaSpec
            Table schema spec for validation.
        """
        spec = table_spec_from_schema(self.name, self.schema)
        return spec.with_constraints(
            required_non_null=self.required_non_null,
            key_fields=self.key_fields,
        )

    def align(self, table: TableLike) -> TableLike:
        """Align a table to the spec schema.

        Returns
        -------
        TableLike
            Table aligned to the schema.
        """
        transform = SchemaTransform(
            schema=self.schema_with_metadata(),
            safe_cast=True,
            keep_extra_columns=False,
            on_error="unsafe",
        )
        return transform.apply(table)

    def codec[SpecT](
        self,
        *,
        encode_row: Callable[[SpecT], dict[str, object]],
        decode_row: Callable[[Mapping[str, object]], SpecT],
        sort_keys: tuple[str, ...] = (),
    ) -> SpecTableCodec[SpecT]:
        """Return a SpecTableCodec for the schema.

        Returns
        -------
        SpecTableCodec[SpecT]
            Codec for encoding/decoding spec tables.
        """
        return SpecTableCodec(
            schema=self.schema_with_metadata(),
            spec=self,
            encode_row=encode_row,
            decode_row=decode_row,
            sort_keys=sort_keys,
        )

    def validate(
        self,
        table: TableLike,
        *,
        ctx: ExecutionContext,
        options: ArrowValidationOptions | None = None,
    ) -> ValidationReport:
        """Validate a table with Arrow-native constraints.

        Returns
        -------
        ValidationReport
            Validation report with errors and stats.
        """
        options = options or ArrowValidationOptions.from_policy(ctx.schema_validation)
        return validate_table(table, spec=self.table_spec(), options=options, ctx=ctx)


@dataclass(frozen=True)
class SpecValidationRule:
    """Spec validation rule expressed as a compute predicate."""

    code: str
    predicate: ComputeExpression


@dataclass(frozen=True)
class SpecValidationSuite:
    """Collection of validation rules for spec tables."""

    rules: tuple[SpecValidationRule, ...] = ()

    def invalid_mask(self) -> ComputeExpression:
        """Return a combined invalid-row mask.

        Returns
        -------
        ComputeExpression
            Boolean expression marking invalid rows.
        """
        if not self.rules:
            return ensure_expression(pc.scalar(pa.scalar(value=False)))
        exprs = [rule.predicate for rule in self.rules]
        return or_exprs(exprs)

    def invalid_rows_plan(self, plan: Plan, *, ctx: ExecutionContext) -> Plan:
        """Return a plan filtering invalid rows.

        Returns
        -------
        Plan
            Plan yielding invalid rows.
        """
        return plan.filter(self.invalid_mask(), ctx=ctx)

    def invalid_rows_table(self, table: TableLike, *, ctx: ExecutionContext) -> TableLike:
        """Return invalid rows from a table.

        Returns
        -------
        TableLike
            Invalid rows table.
        """
        plan_cls = _plan_class()
        plan = plan_cls.table_source(table, label="spec_validate")
        return self.invalid_rows_plan(plan, ctx=ctx).to_table(ctx=ctx)


@dataclass(frozen=True)
class DatasetRegistration:
    """Optional registration settings for dataset specs."""

    query_spec: QuerySpec | None = None
    contract_spec: ContractSpec | None = None
    derived_fields: Sequence[DerivedFieldSpec] = ()
    predicate: ExprSpec | None = None
    pushdown_predicate: ExprSpec | None = None
    evolution_spec: SchemaEvolutionSpec | None = None
    metadata_spec: SchemaMetadataSpec | None = None
    validation: ArrowValidationOptions | None = None


class TableSpecInputKwargs(TypedDict, total=False):
    """Keyword arguments supported by register_dataset table construction."""

    name: str
    version: int | None
    fields: Sequence[ArrowFieldSpec]
    bundles: Sequence[FieldBundle]


def register_dataset(
    *,
    table_spec: TableSchemaSpec | None = None,
    registration: DatasetRegistration | None = None,
    registry: SchemaRegistry | None = None,
    **table_kwargs: Unpack[TableSpecInputKwargs],
) -> DatasetSpec:
    """Register a dataset spec with the global schema registry.

    Returns
    -------
    DatasetSpec
        Registered dataset specification.

    Raises
    ------
    ValueError
        Raised when table_spec or name/fields are missing.
    """
    registration = registration or DatasetRegistration()
    if table_spec is None:
        name = table_kwargs.get("name")
        fields = table_kwargs.get("fields")
        if name is None or fields is None:
            msg = "register_dataset requires name/fields or an explicit table_spec."
            raise ValueError(msg)
        bundles = table_kwargs.get("bundles", ())
        version = table_kwargs.get("version")
        table_spec = make_table_spec(
            name=name,
            version=version,
            bundles=tuple(bundles),
            fields=list(fields),
        )
    spec = make_dataset_spec(
        table_spec=table_spec,
        query_spec=registration.query_spec,
        contract_spec=registration.contract_spec,
        derived_fields=registration.derived_fields,
        predicate=registration.predicate,
        pushdown_predicate=registration.pushdown_predicate,
        evolution_spec=registration.evolution_spec,
        metadata_spec=registration.metadata_spec,
        validation=registration.validation,
    )
    return register_dataset_spec(spec, registry=registry)


__all__ = [
    "DATASET_REF_STRUCT",
    "DEDUPE_STRUCT",
    "SCALAR_UNION_FIELDS",
    "SCALAR_UNION_TYPE",
    "SORT_KEY_STRUCT",
    "VALIDATION_STRUCT",
    "DatasetRegistration",
    "SpecTableSpec",
    "SpecValidationRule",
    "SpecValidationSuite",
    "register_dataset",
]

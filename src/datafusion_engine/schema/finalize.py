"""Finalize gate for schema alignment, invariants, and deterministic output."""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Literal

import pyarrow as pa

from arrow_utils.core.schema_constants import PROVENANCE_COLS
from core_types import DeterminismTier
from datafusion_engine.arrow.chunking import ChunkPolicy
from datafusion_engine.arrow.encoding import EncodingPolicy
from datafusion_engine.arrow.interop import (
    ArrayLike,
    SchemaLike,
    TableLike,
)
from datafusion_engine.arrow.metadata import SchemaMetadataSpec
from datafusion_engine.kernels import (
    DedupeSpec,
    SortKey,
    canonical_sort_if_canonical,
    dedupe_kernel,
)
from datafusion_engine.schema.alignment import AlignmentInfo, align_table
from datafusion_engine.schema.finalize_errors import (
    ERROR_ARTIFACT_SPEC,
    ErrorArtifactSpec,
    InvariantResult,
)
from datafusion_engine.schema.finalize_errors import (
    aggregate_error_details as _aggregate_error_details,
)
from datafusion_engine.schema.finalize_errors import (
    build_error_table as _build_error_table,
)
from datafusion_engine.schema.finalize_errors import (
    build_stats_table as _build_stats_table,
)
from datafusion_engine.schema.finalize_errors import (
    collect_invariant_results as _collect_invariant_results,
)
from datafusion_engine.schema.finalize_errors import (
    filter_good_rows as _filter_good_rows,
)
from datafusion_engine.schema.finalize_errors import (
    raise_on_errors_if_strict as _raise_on_errors_if_strict,
)
from datafusion_engine.schema.finalize_runtime import validate_with_arrow
from datafusion_engine.schema.policy import SchemaPolicyOptions, schema_policy_factory
from datafusion_engine.schema.validation import ArrowValidationOptions
from schema_spec.specs import TableSchemaSpec

if TYPE_CHECKING:
    from datafusion_engine.schema.policy import SchemaPolicy
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile


_LOGGER = logging.getLogger(__name__)


type InvariantFn = Callable[[TableLike], tuple[ArrayLike, str]]


def _table_spec_from_schema(
    name: str,
    schema: SchemaLike,
    *,
    version: int | None = None,
) -> TableSchemaSpec:
    return TableSchemaSpec.from_schema(name, schema, version=version)


@dataclass(frozen=True)
class Contract:
    """Output contract: schema, invariants, and determinism policy."""

    name: str
    schema: SchemaLike
    schema_spec: TableSchemaSpec | None = None

    key_fields: tuple[str, ...] = ()
    required_non_null: tuple[str, ...] = ()
    invariants: tuple[InvariantFn, ...] = ()
    constraints: tuple[str, ...] = ()

    dedupe: DedupeSpec | None = None
    canonical_sort: tuple[SortKey, ...] = ()

    version: int | None = None

    virtual_fields: tuple[str, ...] = ()
    virtual_field_docs: dict[str, str] | None = None
    validation: ArrowValidationOptions | None = None

    def with_versioned_schema(self) -> SchemaLike:
        """Return the schema with contract metadata attached.

        Returns:
        -------
        SchemaLike
            Schema with contract metadata attached.
        """
        meta = {b"contract_name": str(self.name).encode("utf-8")}
        if self.version is not None:
            meta[b"contract_version"] = str(self.version).encode("utf-8")
        return SchemaMetadataSpec(schema_metadata=meta).apply(self.schema)

    def available_fields(self) -> tuple[str, ...]:
        """Return all visible field names (columns + virtual fields).

        Returns:
        -------
        tuple[str, ...]
            Visible field names.
        """
        cols = tuple(self.schema.names)
        return cols + tuple(self.virtual_fields)


@dataclass(frozen=True)
class FinalizeResult:
    """Finalize output bundles.

    Parameters
    ----------
    good:
        Finalized table that passed invariants.
    errors:
        Error rows grouped by ``row_id`` with ``error_detail`` list<struct>.
    stats:
        Error statistics table.
    alignment:
        Schema alignment summary table.
    """

    good: TableLike
    errors: TableLike
    stats: TableLike
    alignment: TableLike


def _list_str_array(values: list[list[str]]) -> ArrayLike:
    """Build a list<str> array from nested string lists.

    Parameters
    ----------
    values:
        Nested string values to convert.

    Returns:
    -------
    ArrayLike
        Array of list<string> values.
    """
    return pa.array(values, type=pa.list_(pa.string()))


def _provenance_columns(table: TableLike, schema: SchemaLike) -> list[str]:
    cols = [col for col in PROVENANCE_COLS if col in table.column_names]
    return [col for col in cols if col not in schema.names]


def _relax_nullable_schema(schema: SchemaLike) -> pa.Schema:
    fields = [
        pa.field(field.name, field.type, nullable=True, metadata=field.metadata) for field in schema
    ]
    return pa.schema(fields, metadata=schema.metadata)


def _relax_nullable_table(table: TableLike) -> TableLike:
    relaxed_schema = _relax_nullable_schema(table.schema)
    arrays = [table[name] for name in table.column_names]
    return pa.Table.from_arrays(arrays, schema=relaxed_schema)


@dataclass(frozen=True)
class FinalizeOptions:
    """Configuration overrides for finalize behavior."""

    error_spec: ErrorArtifactSpec = ERROR_ARTIFACT_SPEC
    schema_policy: SchemaPolicy | None = None
    chunk_policy: ChunkPolicy | None = None
    skip_canonical_sort: bool = False
    runtime_profile: DataFusionRuntimeProfile | None = None
    determinism_tier: DeterminismTier = DeterminismTier.BEST_EFFORT
    mode: Literal["strict", "tolerant"] = "tolerant"
    provenance: bool = False
    schema_validation: ArrowValidationOptions | None = None


def _build_alignment_table(
    contract: Contract,
    info: AlignmentInfo,
    *,
    good_rows: int,
    error_rows: int,
) -> TableLike:
    """Build the schema alignment summary table.

    Parameters
    ----------
    contract:
        Contract that produced the output.
    info:
        Alignment metadata.
    good_rows:
        Number of good rows.
    error_rows:
        Number of error rows.

    Returns:
    -------
    pyarrow.Table
        Alignment summary table.
    """
    return pa.table(
        {
            "contract": [contract.name],
            "input_rows": [info["input_rows"]],
            "good_rows": [good_rows],
            "error_rows": [error_rows],
            "missing_cols": _list_str_array([list(info["missing_cols"])]),
            "dropped_cols": _list_str_array([list(info["dropped_cols"])]),
            "casted_cols": _list_str_array([list(info["casted_cols"])]),
        }
    )


def finalize(
    table: TableLike,
    *,
    contract: Contract,
    options: FinalizeOptions | None = None,
) -> FinalizeResult:
    """Finalize a table at the contract boundary.

    Parameters
    ----------
    table:
        Input table.
    contract:
        Output contract.
    options:
        Optional finalize options for error specs, transforms, and chunk policies.

    Returns:
    -------
    FinalizeResult
        Finalized table bundle.
    """
    options = options or FinalizeOptions()
    input_rows = int(getattr(table, "num_rows", 0))
    _LOGGER.debug(
        "schema.finalize.start",
        extra={
            "contract": contract.name,
            "input_rows": input_rows,
            "mode": options.mode,
            "determinism_tier": str(options.determinism_tier),
        },
    )
    schema_policy = _resolve_schema_policy(contract, schema_policy=options.schema_policy)
    schema = schema_policy.resolved_schema()
    aligned, align_info = schema_policy.apply_with_info(table)
    if schema_policy.encoding is None:
        aligned = (options.chunk_policy or ChunkPolicy()).apply(aligned)
    aligned = validate_with_arrow(
        aligned,
        contract=contract,
        schema_policy=schema_policy,
        schema_validation=options.schema_validation,
        runtime_profile=options.runtime_profile,
    )
    provenance_cols: list[str] = _provenance_columns(aligned, schema) if options.provenance else []

    results, bad_any = _collect_invariant_results(aligned, contract)

    good = _filter_good_rows(aligned, bad_any)

    raw_errors = _build_error_table(aligned, results, error_spec=options.error_spec)
    errors = _aggregate_error_details(
        raw_errors,
        runtime_profile=options.runtime_profile,
        contract=contract,
        schema=schema,
        provenance_cols=provenance_cols,
    )
    errors = _relax_nullable_table(errors)

    _raise_on_errors_if_strict(raw_errors, mode=options.mode, contract=contract)

    if contract.dedupe is not None:
        good = dedupe_kernel(
            good,
            spec=contract.dedupe,
            runtime_profile=options.runtime_profile,
        )

    if not options.skip_canonical_sort:
        good = canonical_sort_if_canonical(
            good,
            sort_keys=contract.canonical_sort,
            determinism_tier=options.determinism_tier,
        )

    keep_extras = bool(options.provenance and provenance_cols)
    good = align_table(
        good,
        schema=schema,
        safe_cast=schema_policy.safe_cast,
        keep_extra_columns=keep_extras,
        on_error=schema_policy.on_error,
    )

    if keep_extras:
        keep = list(schema.names) + [col for col in provenance_cols if col in good.column_names]
        good = good.select(keep)
    elif good.column_names != schema.names:
        good = good.select(schema.names)
    result = FinalizeResult(
        good=good,
        errors=errors,
        stats=_build_stats_table(raw_errors, error_spec=options.error_spec),
        alignment=_build_alignment_table(
            contract, align_info, good_rows=good.num_rows, error_rows=errors.num_rows
        ),
    )
    _LOGGER.debug(
        "schema.finalize.complete",
        extra={
            "contract": contract.name,
            "input_rows": input_rows,
            "good_rows": result.good.num_rows,
            "error_rows": result.errors.num_rows,
        },
    )
    return result


def normalize_only(
    table: TableLike,
    *,
    contract: Contract,
    options: FinalizeOptions | None = None,
) -> TableLike:
    """Normalize a table using finalize schema policy without invariants.

    Returns:
    -------
    TableLike
        Normalized (aligned/encoded) table.
    """
    options = options or FinalizeOptions()
    schema_policy = _resolve_schema_policy(contract, schema_policy=options.schema_policy)
    schema = schema_policy.resolved_schema()
    aligned, _ = schema_policy.apply_with_info(table)
    if schema_policy.encoding is None:
        aligned = (options.chunk_policy or ChunkPolicy()).apply(aligned)
    if not schema_policy.keep_extra_columns and aligned.column_names != schema.names:
        aligned = aligned.select(schema.names)
    return aligned


def _resolve_schema_policy(
    contract: Contract,
    *,
    schema_policy: SchemaPolicy | None,
) -> SchemaPolicy:
    if schema_policy is not None:
        return schema_policy
    schema_spec = contract.schema_spec
    if schema_spec is None:
        schema_spec = _table_spec_from_schema(
            contract.name,
            contract.schema,
            version=contract.version,
        )
        policy_options = SchemaPolicyOptions(
            schema=contract.with_versioned_schema(),
            encoding=EncodingPolicy(dictionary_cols=frozenset()),
            validation=contract.validation,
        )
    else:
        policy_options = SchemaPolicyOptions(
            schema=contract.with_versioned_schema(),
            validation=contract.validation,
        )
    return schema_policy_factory(schema_spec, options=policy_options)


@dataclass(frozen=True)
class FinalizeContext:
    """Reusable finalize configuration for a contract."""

    contract: Contract
    error_spec: ErrorArtifactSpec = ERROR_ARTIFACT_SPEC
    schema_policy: SchemaPolicy | None = None
    chunk_policy: ChunkPolicy = field(default_factory=ChunkPolicy)
    skip_canonical_sort: bool = False

    def run(
        self,
        table: TableLike,
        *,
        request: FinalizeRunRequest,
    ) -> FinalizeResult:
        """Finalize a table using the stored contract and options.

        Returns:
        -------
        FinalizeResult
            Finalized table bundle.
        """
        options = FinalizeOptions(
            error_spec=self.error_spec,
            schema_policy=self.schema_policy,
            chunk_policy=self.chunk_policy,
            skip_canonical_sort=self.skip_canonical_sort,
            runtime_profile=request.runtime_profile,
            determinism_tier=request.determinism_tier,
            mode=request.mode,
            provenance=request.provenance,
            schema_validation=request.schema_validation,
        )
        return finalize(
            table,
            contract=self.contract,
            options=options,
        )


@dataclass(frozen=True)
class FinalizeRunRequest:
    """Inputs required to run a finalize pass."""

    runtime_profile: DataFusionRuntimeProfile | None
    determinism_tier: DeterminismTier
    mode: Literal["strict", "tolerant"] = "tolerant"
    provenance: bool = False
    schema_validation: ArrowValidationOptions | None = None


__all__ = [
    "ERROR_ARTIFACT_SPEC",
    "Contract",
    "ErrorArtifactSpec",
    "FinalizeContext",
    "FinalizeOptions",
    "FinalizeResult",
    "InvariantFn",
    "InvariantResult",
    "finalize",
    "normalize_only",
]

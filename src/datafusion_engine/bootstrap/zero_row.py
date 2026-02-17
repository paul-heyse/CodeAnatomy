"""Zero-row Delta bootstrap planning and execution helpers."""

from __future__ import annotations

import contextlib
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from decimal import Decimal
from functools import partial
from pathlib import Path
from typing import TYPE_CHECKING, Literal, cast

import msgspec
import pyarrow as pa

from datafusion_engine.arrow.semantic import apply_semantic_types
from datafusion_engine.cache.inventory import (
    CACHE_INVENTORY_TABLE_NAME,
    ensure_cache_inventory_table,
)
from datafusion_engine.dataset.registry import DatasetLocation
from datafusion_engine.delta.observability import (
    DELTA_MAINTENANCE_TABLE_NAME,
    DELTA_MUTATION_TABLE_NAME,
    DELTA_SCAN_PLAN_TABLE_NAME,
    DELTA_SNAPSHOT_TABLE_NAME,
    ensure_delta_observability_tables,
)
from datafusion_engine.extract.registry import (
    dataset_schema as extract_dataset_schema,
)
from datafusion_engine.identity import schema_identity_hash
from datafusion_engine.io.adapter import DataFusionIOAdapter
from datafusion_engine.schema import (
    extract_nested_dataset_names,
    extract_schema_for,
    relationship_schema_for,
    relationship_schema_names,
)
from datafusion_engine.session.facade import DataFusionExecutionFacade
from semantics.catalog.dataset_specs import (
    dataset_schema as semantic_dataset_schema,
)
from semantics.catalog.dataset_specs import (
    maybe_dataset_spec as maybe_semantic_dataset_spec,
)
from semantics.input_registry import SEMANTIC_INPUT_SPECS
from semantics.registry import SEMANTIC_MODEL
from semantics.validation import (
    SemanticInputValidationError,
    validate_semantic_inputs,
)
from serde_msgspec import StructBaseStrict

if TYPE_CHECKING:
    from datafusion import SessionContext

    from datafusion_engine.session.runtime import DataFusionRuntimeProfile
    from semantics.program_manifest import SemanticProgramManifest


ZeroRowRole = Literal[
    "semantic_input",
    "extract_nested",
    "semantic_output",
    "relationship_schema",
    "internal",
]
_INTERNAL_TABLE_ROLE: Literal["internal"] = "internal"
ZeroRowBootstrapMode = Literal["strict_zero_rows", "seeded_minimal_rows"]


@dataclass(frozen=True)
class ZeroRowDatasetPlan:
    """Dataset bootstrap plan entry."""

    name: str
    role: ZeroRowRole
    schema: pa.Schema
    location: DatasetLocation | None
    required: bool
    materialize: bool = True

    def payload(self) -> dict[str, object]:
        """Return a diagnostics payload for this plan."""
        schema_hash = schema_identity_hash(self.schema.remove_metadata())
        return {
            "name": self.name,
            "role": self.role,
            "required": self.required,
            "materialize": self.materialize,
            "location": str(self.location.path) if self.location is not None else None,
            "format": self.location.format if self.location is not None else None,
            "schema_identity_hash": schema_hash,
        }


class ZeroRowBootstrapRequest(StructBaseStrict, frozen=True):
    """Request for zero-row bootstrap execution."""

    include_semantic_outputs: bool = True
    include_internal_tables: bool = True
    strict: bool = True
    allow_semantic_row_probe_fallback: bool = False
    bootstrap_mode: ZeroRowBootstrapMode = "strict_zero_rows"
    seeded_datasets: tuple[str, ...] = ()


class ZeroRowBootstrapEvent(StructBaseStrict, frozen=True):
    """Deterministic event emitted during zero-row bootstrap."""

    event_time_unix_ms: int
    dataset: str
    role: ZeroRowRole
    status: str
    required: bool
    path: str | None = None
    message: str | None = None

    def payload(self) -> dict[str, object]:
        """Return a JSON-serializable event payload."""
        return {
            "event_time_unix_ms": self.event_time_unix_ms,
            "dataset": self.dataset,
            "role": self.role,
            "status": self.status,
            "required": self.required,
            "path": self.path,
            "message": self.message,
        }


class ZeroRowBootstrapReport(StructBaseStrict, frozen=True):
    """Zero-row bootstrap execution report."""

    success: bool
    strict: bool
    planned_count: int
    bootstrapped_count: int
    registered_count: int
    skipped_count: int
    failed_count: int
    seeded_count: int
    seeded_datasets: tuple[str, ...]
    plans: tuple[dict[str, object], ...]
    events: tuple[ZeroRowBootstrapEvent, ...]
    validation_errors: tuple[str, ...] = ()

    def payload(self) -> dict[str, object]:
        """Return a JSON-serializable report payload."""
        return {
            "success": self.success,
            "strict": self.strict,
            "planned_count": self.planned_count,
            "bootstrapped_count": self.bootstrapped_count,
            "registered_count": self.registered_count,
            "skipped_count": self.skipped_count,
            "failed_count": self.failed_count,
            "seeded_count": self.seeded_count,
            "seeded_datasets": list(self.seeded_datasets),
            "plans": list(self.plans),
            "events": [event.payload() for event in self.events],
            "validation_errors": list(self.validation_errors),
        }


@dataclass
class _ZeroRowBootstrapTally:
    bootstrapped_count: int = 0
    registered_count: int = 0
    skipped_count: int = 0
    failed_count: int = 0
    seeded_count: int = 0
    seeded_dataset_names: set[str] = field(default_factory=set)
    events: list[ZeroRowBootstrapEvent] = field(default_factory=list)

    def add_event(
        self,
        event: ZeroRowBootstrapEvent,
        *,
        counted_skipped: bool = False,
        counted_failed: bool = False,
    ) -> None:
        if counted_skipped:
            self.skipped_count += 1
        if counted_failed:
            self.failed_count += 1
        self.events.append(event)


def build_zero_row_plan(
    profile: DataFusionRuntimeProfile,
    *,
    request: ZeroRowBootstrapRequest,
    manifest: SemanticProgramManifest,
) -> tuple[ZeroRowDatasetPlan, ...]:
    """Build a deterministic zero-row bootstrap plan for runtime datasets.

    Returns:
    -------
    tuple[ZeroRowDatasetPlan, ...]
        Ordered bootstrap plan entries.
    """
    _ = profile
    dataset_locations = {
        name: _bootstrap_safe_location(location)
        for name, location in manifest.dataset_bindings.locations.items()
    }
    plans: dict[str, ZeroRowDatasetPlan] = {}

    semantic_input_sources = sorted({spec.extraction_source for spec in SEMANTIC_INPUT_SPECS})
    required_inputs = {spec.extraction_source for spec in SEMANTIC_INPUT_SPECS if spec.required}
    for name in semantic_input_sources:
        _add_plan(
            plans,
            ZeroRowDatasetPlan(
                name=name,
                role="semantic_input",
                schema=pa.schema(extract_dataset_schema(name)),
                location=dataset_locations.get(name),
                required=name in required_inputs,
            ),
        )

    for name in sorted(extract_nested_dataset_names()):
        _add_plan(
            plans,
            ZeroRowDatasetPlan(
                name=name,
                role="extract_nested",
                schema=pa.schema(extract_schema_for(name)),
                location=dataset_locations.get(name),
                required=False,
            ),
        )

    for name in sorted(relationship_schema_names()):
        _add_plan(
            plans,
            ZeroRowDatasetPlan(
                name=name,
                role="relationship_schema",
                schema=pa.schema(relationship_schema_for(name)),
                location=None,
                required=False,
                materialize=False,
            ),
        )

    if request.include_semantic_outputs:
        for name in sorted(spec.name for spec in SEMANTIC_MODEL.outputs):
            spec = maybe_semantic_dataset_spec(name)
            if spec is None:
                _add_plan(
                    plans,
                    ZeroRowDatasetPlan(
                        name=name,
                        role="semantic_output",
                        schema=pa.schema([]),
                        location=dataset_locations.get(name),
                        required=False,
                        materialize=False,
                    ),
                )
                continue
            _add_plan(
                plans,
                ZeroRowDatasetPlan(
                    name=name,
                    role="semantic_output",
                    schema=apply_semantic_types(pa.schema(semantic_dataset_schema(name))),
                    location=dataset_locations.get(name),
                    required=False,
                ),
            )

    return tuple(sorted(plans.values(), key=lambda item: (item.role, item.name)))


def run_zero_row_bootstrap_validation(
    profile: DataFusionRuntimeProfile,
    *,
    request: ZeroRowBootstrapRequest,
    ctx: SessionContext,
    manifest: SemanticProgramManifest,
) -> ZeroRowBootstrapReport:
    """Execute zero-row bootstrap materialization and post-bootstrap validation.

    Returns:
    -------
    ZeroRowBootstrapReport
        Deterministic bootstrap execution report.
    """
    plan = build_zero_row_plan(profile, request=request, manifest=manifest)
    tally = _ZeroRowBootstrapTally()
    seeded_targets = (
        set(request.seeded_datasets) if request.bootstrap_mode == "seeded_minimal_rows" else set()
    )

    for item in plan:
        _process_zero_row_plan_item(
            tally=tally,
            item=item,
            seeded_targets=seeded_targets,
            profile=profile,
            ctx=ctx,
            event_time=int(time.time() * 1000),
        )

    if request.include_internal_tables:
        internal_events = _bootstrap_internal_tables(ctx=ctx, profile=profile)
        _append_internal_bootstrap_events(tally, internal_events)

    validation_errors = _validation_errors(
        ctx=ctx,
        manifest=manifest,
    )
    if request.strict and validation_errors:
        tally.failed_count += len(validation_errors)

    success = tally.failed_count == 0 and not validation_errors
    return ZeroRowBootstrapReport(
        success=success,
        strict=request.strict,
        planned_count=len(plan),
        bootstrapped_count=tally.bootstrapped_count,
        registered_count=tally.registered_count,
        skipped_count=tally.skipped_count,
        failed_count=tally.failed_count,
        seeded_count=tally.seeded_count,
        seeded_datasets=tuple(sorted(tally.seeded_dataset_names)),
        plans=tuple(item.payload() for item in plan),
        events=tuple(tally.events),
        validation_errors=tuple(validation_errors),
    )


def _append_internal_bootstrap_events(
    tally: _ZeroRowBootstrapTally,
    internal_events: list[ZeroRowBootstrapEvent],
) -> None:
    tally.events.extend(internal_events)
    for event in internal_events:
        if event.status == "registered":
            tally.registered_count += 1
            tally.bootstrapped_count += 1
        elif event.status.startswith("bootstrap_failed") and event.required:
            tally.failed_count += 1


def _process_zero_row_plan_item(
    *,
    tally: _ZeroRowBootstrapTally,
    item: ZeroRowDatasetPlan,
    seeded_targets: set[str],
    profile: DataFusionRuntimeProfile,
    ctx: SessionContext,
    event_time: int,
) -> None:
    if not item.materialize:
        tally.add_event(
            ZeroRowBootstrapEvent(
                event_time_unix_ms=event_time,
                dataset=item.name,
                role=item.role,
                status="schema_only",
                required=item.required,
                message="schema-only dataset; external materialization is not required",
            ),
            counted_skipped=True,
        )
        return

    location = item.location
    if location is None:
        tally.add_event(
            ZeroRowBootstrapEvent(
                event_time_unix_ms=event_time,
                dataset=item.name,
                role=item.role,
                status="missing_location",
                required=item.required,
                message="dataset location is not configured",
            ),
            counted_skipped=True,
            counted_failed=item.required,
        )
        return

    if location.format != "delta":
        tally.add_event(
            ZeroRowBootstrapEvent(
                event_time_unix_ms=event_time,
                dataset=item.name,
                role=item.role,
                status="unsupported_format",
                required=item.required,
                path=str(location.path),
                message=f"only delta format is supported (got {location.format!r})",
            ),
            counted_skipped=True,
            counted_failed=item.required,
        )
        return

    should_seed = item.name in seeded_targets
    _seed_and_register_dataset(
        context=_SeedAndRegisterContext(
            tally=tally,
            item=item,
            location=location,
            ctx=ctx,
            profile=profile,
            should_seed=should_seed,
            event_time=event_time,
        ),
    )


def _seed_and_register_dataset(
    *,
    context: _SeedAndRegisterContext,
) -> None:
    tally = context.tally
    item = context.item
    location = context.location
    should_seed = context.should_seed
    event_time = context.event_time
    ctx = context.ctx
    profile = context.profile
    path = Path(str(location.path))
    try:
        _materialize_delta_table(
            ctx=ctx,
            path=path,
            schema=item.schema,
            seed_row=_seed_row_for_schema(item.schema) if should_seed else None,
        )
    except (RuntimeError, TypeError, ValueError, OSError, ImportError) as exc:
        tally.add_event(
            ZeroRowBootstrapEvent(
                event_time_unix_ms=event_time,
                dataset=item.name,
                role=item.role,
                status="bootstrap_failed",
                required=item.required,
                path=str(path),
                message=str(exc),
            ),
            counted_failed=item.required,
        )
        return

    tally.bootstrapped_count += 1
    if should_seed:
        tally.seeded_count += 1
        tally.seeded_dataset_names.add(item.name)

    try:
        _register_dataset(ctx=ctx, profile=profile, name=item.name, location=location)
    except (RuntimeError, TypeError, ValueError, OSError, KeyError) as exc:
        tally.add_event(
            ZeroRowBootstrapEvent(
                event_time_unix_ms=event_time,
                dataset=item.name,
                role=item.role,
                status="register_failed",
                required=item.required,
                path=str(path),
                message=str(exc),
            ),
            counted_failed=item.required,
        )
        return

    tally.registered_count += 1
    tally.add_event(
        ZeroRowBootstrapEvent(
            event_time_unix_ms=event_time,
            dataset=item.name,
            role=item.role,
            status="seeded_registered" if should_seed else "registered",
            required=item.required,
            path=str(path),
        )
    )


@dataclass(frozen=True)
class _SeedAndRegisterContext:
    tally: _ZeroRowBootstrapTally
    item: ZeroRowDatasetPlan
    location: DatasetLocation
    ctx: SessionContext
    profile: DataFusionRuntimeProfile
    should_seed: bool
    event_time: int


def _validation_errors(
    *,
    ctx: SessionContext,
    manifest: SemanticProgramManifest,
) -> list[str]:
    errors: list[str] = []
    validation = validate_semantic_inputs(
        ctx=ctx,
        manifest=manifest,
        policy=manifest.validation_policy,
    )
    if not validation.valid:
        errors.append(str(SemanticInputValidationError(validation)))
    return errors


def _bootstrap_safe_location(location: DatasetLocation) -> DatasetLocation:
    return msgspec.structs.replace(
        location,
        dataset_spec=None,
        delta_cdf_options=None,
        datafusion_provider=None,
        overrides=None,
    )


def _materialize_delta_table(
    *,
    ctx: SessionContext,
    path: Path,
    schema: pa.Schema,
    seed_row: Mapping[str, object] | None = None,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    compatible = _delta_compatible_schema(schema)
    rows: list[dict[str, object]] = [] if seed_row is None else [dict(seed_row)]
    table = pa.Table.from_pylist(rows, schema=compatible)
    from datafusion_engine.delta.transactions import write_transaction
    from datafusion_engine.delta.write_ipc_payload import (
        DeltaWriteRequestOptions,
        build_delta_write_request,
    )

    request = build_delta_write_request(
        table_uri=str(path),
        table=table,
        options=DeltaWriteRequestOptions(
            mode="overwrite",
            schema_mode="overwrite",
            storage_options=None,
        ),
    )
    write_transaction(ctx, request=request)


def _seed_row_for_schema(schema: pa.Schema) -> dict[str, object]:
    compatible = _delta_compatible_schema(schema)
    return {field.name: _seed_value_for_field(field) for field in compatible}


def _seed_value_for_field(field: pa.Field) -> object:
    if field.nullable:
        return None
    return _seed_value_for_type(field.type)


def _seed_value_for_type(
    data_type: pa.DataType,
) -> object:
    if isinstance(data_type, pa.ExtensionType):
        return _seed_value_for_type(data_type.storage_type)
    scalar_value = _seed_scalar_value(data_type)
    if scalar_value is not None:
        return scalar_value
    complex_value = _seed_complex_value(data_type)
    if complex_value is not None:
        return complex_value
    return pa.scalar(0, type=data_type).as_py()


def _seed_scalar_value(data_type: pa.DataType) -> object | None:
    scalar_factory = _seed_scalar_value_factory(data_type)
    if scalar_factory is not None:
        return scalar_factory()
    if pa.types.is_dictionary(data_type):
        return _seed_value_for_type(data_type.value_type)
    return None


def _seed_temporal_scalar(data_type: pa.DataType) -> object:
    return pa.scalar(0, type=data_type).as_py()


def _seed_boolean_scalar(_data_type: pa.DataType) -> object:
    return False


def _seed_integer_scalar(_data_type: pa.DataType) -> object:
    return 0


def _seed_floating_scalar(_data_type: pa.DataType) -> object:
    return 0.0


def _seed_decimal_scalar(_data_type: pa.DataType) -> object:
    return Decimal(0)


def _seed_string_scalar(_data_type: pa.DataType) -> object:
    return ""


def _seed_binary_scalar(_data_type: pa.DataType) -> object:
    return b""


def _seed_fixed_size_binary_scalar(data_type: pa.DataType) -> object:
    return b"\x00" * data_type.byte_width


def _seed_null_scalar(_data_type: pa.DataType) -> object:
    return None


def _seed_scalar_value_factory(data_type: pa.DataType) -> Callable[[], object] | None:
    rules: tuple[
        tuple[Callable[[pa.DataType], bool], Callable[[pa.DataType], object]],
        ...,
    ] = (
        (_is_temporal_type, _seed_temporal_scalar),
        (pa.types.is_boolean, _seed_boolean_scalar),
        (pa.types.is_integer, _seed_integer_scalar),
        (pa.types.is_unsigned_integer, _seed_integer_scalar),
        (pa.types.is_floating, _seed_floating_scalar),
        (pa.types.is_decimal, _seed_decimal_scalar),
        (pa.types.is_string, _seed_string_scalar),
        (pa.types.is_large_string, _seed_string_scalar),
        (pa.types.is_binary, _seed_binary_scalar),
        (pa.types.is_large_binary, _seed_binary_scalar),
        (pa.types.is_fixed_size_binary, _seed_fixed_size_binary_scalar),
        (pa.types.is_null, _seed_null_scalar),
    )
    for is_type, factory in rules:
        if is_type(data_type):
            return partial(factory, data_type)
    return None


def _is_temporal_type(data_type: pa.DataType) -> bool:
    return (
        pa.types.is_date32(data_type)
        or pa.types.is_date64(data_type)
        or pa.types.is_time32(data_type)
        or pa.types.is_time64(data_type)
        or pa.types.is_timestamp(data_type)
        or pa.types.is_duration(data_type)
    )


def _seed_complex_value(data_type: pa.DataType) -> object | None:
    if pa.types.is_struct(data_type):
        return {field.name: _seed_value_for_field(field) for field in data_type}
    if pa.types.is_list(data_type) or pa.types.is_large_list(data_type):
        return cast("list[object]", [])
    if pa.types.is_fixed_size_list(data_type):
        value = _seed_value_for_field(data_type.value_field)
        return [value for _ in range(data_type.list_size)]
    if pa.types.is_map(data_type):
        return cast("dict[str, object]", {})
    return None


def _delta_compatible_schema(schema: pa.Schema) -> pa.Schema:
    fields = [_delta_compatible_field(field) for field in schema]
    return pa.schema(fields, metadata=schema.metadata)


def _delta_compatible_field(field: pa.Field) -> pa.Field:
    return pa.field(
        field.name,
        _delta_compatible_type(field.type),
        nullable=field.nullable,
        metadata=field.metadata,
    )


def _delta_compatible_type(data_type: pa.DataType) -> pa.DataType:
    if isinstance(data_type, pa.ExtensionType):
        return _delta_compatible_type(data_type.storage_type)
    if pa.types.is_struct(data_type):
        return pa.struct([_delta_compatible_field(field) for field in data_type])
    if pa.types.is_list(data_type):
        return pa.list_(_delta_compatible_field(data_type.value_field))
    if pa.types.is_large_list(data_type):
        return pa.large_list(_delta_compatible_field(data_type.value_field))
    if pa.types.is_map(data_type):
        return pa.map_(
            _delta_compatible_field(data_type.key_field).type,
            _delta_compatible_field(data_type.item_field).type,
            keys_sorted=data_type.keys_sorted,
        )
    return data_type


def _register_dataset(
    *,
    ctx: SessionContext,
    profile: DataFusionRuntimeProfile,
    name: str,
    location: DatasetLocation,
) -> None:
    adapter = DataFusionIOAdapter(ctx=ctx, profile=profile)
    if ctx.table_exist(name):
        with contextlib.suppress(KeyError, RuntimeError, TypeError, ValueError):
            adapter.deregister_table(name)
    _ = DataFusionExecutionFacade(
        ctx=ctx,
        runtime_profile=profile,
    ).register_dataset(
        name=name,
        location=location,
        overwrite=True,
    )


def _bootstrap_internal_tables(
    *,
    ctx: SessionContext,
    profile: DataFusionRuntimeProfile,
) -> list[ZeroRowBootstrapEvent]:
    events: list[ZeroRowBootstrapEvent] = []
    event_time = int(time.time() * 1000)
    location = ensure_cache_inventory_table(ctx, profile)
    if location is None:
        events.append(
            ZeroRowBootstrapEvent(
                event_time_unix_ms=event_time,
                dataset=CACHE_INVENTORY_TABLE_NAME,
                role=_INTERNAL_TABLE_ROLE,
                status="bootstrap_failed_cache_inventory",
                required=False,
            )
        )
    else:
        events.append(
            ZeroRowBootstrapEvent(
                event_time_unix_ms=event_time,
                dataset=CACHE_INVENTORY_TABLE_NAME,
                role=_INTERNAL_TABLE_ROLE,
                status="registered",
                required=False,
                path=str(location.path),
            )
        )
    observability_locations = ensure_delta_observability_tables(ctx, profile)
    for name in (
        DELTA_SNAPSHOT_TABLE_NAME,
        DELTA_MUTATION_TABLE_NAME,
        DELTA_SCAN_PLAN_TABLE_NAME,
        DELTA_MAINTENANCE_TABLE_NAME,
    ):
        location = observability_locations.get(name)
        if location is None:
            events.append(
                ZeroRowBootstrapEvent(
                    event_time_unix_ms=event_time,
                    dataset=name,
                    role=_INTERNAL_TABLE_ROLE,
                    status="bootstrap_failed_observability",
                    required=False,
                )
            )
            continue
        events.append(
            ZeroRowBootstrapEvent(
                event_time_unix_ms=event_time,
                dataset=name,
                role=_INTERNAL_TABLE_ROLE,
                status="registered",
                required=False,
                path=str(location.path),
            )
        )
    return events


def _add_plan(plans: dict[str, ZeroRowDatasetPlan], plan: ZeroRowDatasetPlan) -> None:
    existing = plans.get(plan.name)
    if existing is None:
        plans[plan.name] = plan
        return
    required = existing.required or plan.required
    location = existing.location or plan.location
    if existing.required:
        chosen_role = existing.role
    elif plan.required:
        chosen_role = plan.role
    elif existing.role == "semantic_input" and plan.role != "semantic_input":
        chosen_role = existing.role
    else:
        chosen_role = plan.role
    materialize = existing.materialize or plan.materialize
    plans[plan.name] = ZeroRowDatasetPlan(
        name=plan.name,
        role=chosen_role,
        schema=existing.schema,
        location=location,
        required=required,
        materialize=materialize,
    )


__all__ = [
    "ZeroRowBootstrapEvent",
    "ZeroRowBootstrapReport",
    "ZeroRowBootstrapRequest",
    "ZeroRowDatasetPlan",
    "build_zero_row_plan",
    "run_zero_row_bootstrap_validation",
]

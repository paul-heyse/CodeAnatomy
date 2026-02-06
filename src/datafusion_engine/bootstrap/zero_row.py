"""Zero-row Delta bootstrap planning and execution helpers."""

from __future__ import annotations

import contextlib
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Literal

import pyarrow as pa
from deltalake.writer import write_deltalake

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
from datafusion_engine.schema.registry import (
    extract_nested_dataset_names,
    extract_nested_schema_for,
    relationship_schema_for,
    relationship_schema_names,
    validate_semantic_types,
)
from datafusion_engine.session.facade import DataFusionExecutionFacade
from semantics.catalog.dataset_specs import (
    dataset_schema as semantic_dataset_schema,
    maybe_dataset_spec as maybe_semantic_dataset_spec,
)
from semantics.input_registry import SEMANTIC_INPUT_SPECS, require_semantic_inputs
from semantics.registry import SEMANTIC_MODEL
from serde_msgspec import StructBaseStrict

if TYPE_CHECKING:
    from datafusion import SessionContext

    from datafusion_engine.session.runtime import DataFusionRuntimeProfile


ZeroRowRole = Literal[
    "semantic_input",
    "extract_nested",
    "semantic_output",
    "relationship_schema",
    "internal",
]
_INTERNAL_TABLE_ROLE: Literal["internal"] = "internal"


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
            "plans": list(self.plans),
            "events": [event.payload() for event in self.events],
            "validation_errors": list(self.validation_errors),
        }


def build_zero_row_plan(
    profile: DataFusionRuntimeProfile,
    *,
    request: ZeroRowBootstrapRequest,
) -> tuple[ZeroRowDatasetPlan, ...]:
    """Build a deterministic zero-row bootstrap plan for runtime datasets."""
    semantic_output_locations = _semantic_output_locations(profile)
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
                location=_semantic_input_location(profile, name),
                required=name in required_inputs,
            ),
        )

    for name in sorted(extract_nested_dataset_names()):
        _add_plan(
            plans,
            ZeroRowDatasetPlan(
                name=name,
                role="extract_nested",
                schema=pa.schema(extract_nested_schema_for(name)),
                location=profile.catalog_ops.dataset_location(name),
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
                        location=semantic_output_locations.get(name),
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
                    schema=pa.schema(semantic_dataset_schema(name)),
                    location=semantic_output_locations.get(name),
                    required=False,
                ),
            )

    return tuple(sorted(plans.values(), key=lambda item: (item.role, item.name)))


def run_zero_row_bootstrap_validation(
    profile: DataFusionRuntimeProfile,
    *,
    request: ZeroRowBootstrapRequest,
    ctx: SessionContext,
) -> ZeroRowBootstrapReport:
    """Execute zero-row bootstrap materialization and post-bootstrap validation."""
    plan = build_zero_row_plan(profile, request=request)
    events: list[ZeroRowBootstrapEvent] = []
    bootstrapped_count = 0
    registered_count = 0
    skipped_count = 0
    failed_count = 0

    for item in plan:
        event_time = int(time.time() * 1000)
        if not item.materialize:
            skipped_count += 1
            events.append(
                ZeroRowBootstrapEvent(
                    event_time_unix_ms=event_time,
                    dataset=item.name,
                    role=item.role,
                    status="schema_only",
                    required=item.required,
                    message="schema-only dataset; external materialization is not required",
                )
            )
            continue
        location = item.location
        if location is None:
            skipped_count += 1
            if item.required:
                failed_count += 1
            events.append(
                ZeroRowBootstrapEvent(
                    event_time_unix_ms=event_time,
                    dataset=item.name,
                    role=item.role,
                    status="missing_location",
                    required=item.required,
                    message="dataset location is not configured",
                )
            )
            continue
        if location.format != "delta":
            skipped_count += 1
            if item.required:
                failed_count += 1
            events.append(
                ZeroRowBootstrapEvent(
                    event_time_unix_ms=event_time,
                    dataset=item.name,
                    role=item.role,
                    status="unsupported_format",
                    required=item.required,
                    path=str(location.path),
                    message=f"only delta format is supported (got {location.format!r})",
                )
            )
            continue
        path = Path(str(location.path))
        try:
            _materialize_empty_delta(path=path, schema=item.schema)
            bootstrapped_count += 1
        except (RuntimeError, TypeError, ValueError, OSError, ImportError) as exc:
            failed_count += 1
            events.append(
                ZeroRowBootstrapEvent(
                    event_time_unix_ms=event_time,
                    dataset=item.name,
                    role=item.role,
                    status="bootstrap_failed",
                    required=item.required,
                    path=str(path),
                    message=str(exc),
                )
            )
            continue
        try:
            _register_dataset(ctx=ctx, profile=profile, name=item.name, location=location)
        except (RuntimeError, TypeError, ValueError, OSError, KeyError) as exc:
            failed_count += 1
            events.append(
                ZeroRowBootstrapEvent(
                    event_time_unix_ms=event_time,
                    dataset=item.name,
                    role=item.role,
                    status="register_failed",
                    required=item.required,
                    path=str(path),
                    message=str(exc),
                )
            )
            continue
        registered_count += 1
        events.append(
            ZeroRowBootstrapEvent(
                event_time_unix_ms=event_time,
                dataset=item.name,
                role=item.role,
                status="registered",
                required=item.required,
                path=str(path),
            )
        )

    if request.include_internal_tables:
        internal_events = _bootstrap_internal_tables(ctx=ctx, profile=profile)
        events.extend(internal_events)
        for event in internal_events:
            if event.status == "registered":
                registered_count += 1
                bootstrapped_count += 1
            elif event.status.startswith("bootstrap_failed"):
                failed_count += 1

    validation_errors = _validation_errors(
        ctx=ctx,
        allow_semantic_row_probe_fallback=request.allow_semantic_row_probe_fallback,
    )
    if request.strict and validation_errors:
        failed_count += len(validation_errors)

    success = failed_count == 0 and not validation_errors
    return ZeroRowBootstrapReport(
        success=success,
        strict=request.strict,
        planned_count=len(plan),
        bootstrapped_count=bootstrapped_count,
        registered_count=registered_count,
        skipped_count=skipped_count,
        failed_count=failed_count,
        plans=tuple(item.payload() for item in plan),
        events=tuple(events),
        validation_errors=tuple(validation_errors),
    )


def _validation_errors(
    *,
    ctx: SessionContext,
    allow_semantic_row_probe_fallback: bool,
) -> list[str]:
    errors: list[str] = []
    try:
        require_semantic_inputs(ctx)
    except ValueError as exc:
        errors.append(str(exc))
    try:
        validate_semantic_types(
            ctx,
            allow_row_probe_fallback=allow_semantic_row_probe_fallback,
        )
    except (RuntimeError, TypeError, ValueError) as exc:
        errors.append(str(exc))
    return errors


def _semantic_output_locations(
    profile: DataFusionRuntimeProfile,
) -> Mapping[str, DatasetLocation]:
    semantic_output = profile.data_sources.semantic_output
    if semantic_output.locations:
        return semantic_output.locations
    if semantic_output.output_catalog_name is not None:
        catalog = profile.catalog.registry_catalogs.get(semantic_output.output_catalog_name)
        if catalog is None:
            return {}
        names = {spec.name for spec in SEMANTIC_MODEL.outputs}
        return {name: catalog.get(name) for name in names if catalog.has(name)}
    if semantic_output.output_root is None:
        return {}
    root = Path(semantic_output.output_root)
    return {
        spec.name: DatasetLocation(path=str(root / spec.name), format="delta")
        for spec in SEMANTIC_MODEL.outputs
    }


def _semantic_input_location(
    profile: DataFusionRuntimeProfile,
    name: str,
) -> DatasetLocation | None:
    location = profile.catalog_ops.dataset_location(name)
    if location is not None:
        return location
    extract_root = profile.data_sources.extract_output.output_root
    if extract_root is None:
        return None
    return DatasetLocation(
        path=str(Path(extract_root) / name),
        format="delta",
    )


def _materialize_empty_delta(*, path: Path, schema: pa.Schema) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pylist([], schema=_delta_compatible_schema(schema))
    write_deltalake(
        str(path),
        table,
        mode="overwrite",
        schema_mode="overwrite",
    )


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
    facade = DataFusionExecutionFacade(ctx=ctx, runtime_profile=profile)
    _ = facade.register_dataset(name=name, location=location)


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

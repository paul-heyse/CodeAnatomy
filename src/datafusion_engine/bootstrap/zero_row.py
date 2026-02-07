"""Zero-row Delta bootstrap planning and execution helpers."""

from __future__ import annotations

import contextlib
import time
from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING, Literal, cast

import msgspec
import pyarrow as pa
from deltalake.writer import write_deltalake

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
from datafusion_engine.schema.registry import (
    extract_nested_dataset_names,
    extract_nested_schema_for,
    relationship_schema_for,
    relationship_schema_names,
)
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
                schema=pa.schema(extract_nested_schema_for(name)),
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


def run_zero_row_bootstrap_validation(  # noqa: C901, PLR0912, PLR0914, PLR0915
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
    events: list[ZeroRowBootstrapEvent] = []
    bootstrapped_count = 0
    registered_count = 0
    skipped_count = 0
    failed_count = 0
    seeded_count = 0
    seeded_dataset_names: set[str] = set()
    seeded_targets = (
        set(request.seeded_datasets) if request.bootstrap_mode == "seeded_minimal_rows" else set()
    )

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
        should_seed = item.name in seeded_targets
        try:
            _materialize_delta_table(
                path=path,
                schema=item.schema,
                seed_row=_seed_row_for_schema(item.schema) if should_seed else None,
            )
            bootstrapped_count += 1
            if should_seed:
                seeded_count += 1
                seeded_dataset_names.add(item.name)
        except (RuntimeError, TypeError, ValueError, OSError, ImportError) as exc:
            if item.required:
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
            if item.required:
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
                status="seeded_registered" if should_seed else "registered",
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
            elif event.status.startswith("bootstrap_failed") and event.required:
                failed_count += 1

    validation_errors = _validation_errors(
        ctx=ctx,
        manifest=manifest,
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
        seeded_count=seeded_count,
        seeded_datasets=tuple(sorted(seeded_dataset_names)),
        plans=tuple(item.payload() for item in plan),
        events=tuple(events),
        validation_errors=tuple(validation_errors),
    )


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
    path: Path,
    schema: pa.Schema,
    seed_row: Mapping[str, object] | None = None,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    compatible = _delta_compatible_schema(schema)
    rows: list[dict[str, object]] = [] if seed_row is None else [dict(seed_row)]
    table = pa.Table.from_pylist(rows, schema=compatible)
    write_deltalake(
        str(path),
        table,
        mode="overwrite",
        schema_mode="overwrite",
    )


def _seed_row_for_schema(schema: pa.Schema) -> dict[str, object]:
    compatible = _delta_compatible_schema(schema)
    return {field.name: _seed_value_for_field(field) for field in compatible}


def _seed_value_for_field(field: pa.Field) -> object:
    if field.nullable:
        return None
    return _seed_value_for_type(field.type)


def _seed_value_for_type(  # noqa: C901, PLR0911, PLR0912
    data_type: pa.DataType,
) -> object:
    if isinstance(data_type, pa.ExtensionType):
        return _seed_value_for_type(data_type.storage_type)
    if pa.types.is_boolean(data_type):
        return False
    if pa.types.is_integer(data_type) or pa.types.is_unsigned_integer(data_type):
        return 0
    if pa.types.is_floating(data_type):
        return 0.0
    if pa.types.is_decimal(data_type):
        return Decimal(0)
    if pa.types.is_string(data_type) or pa.types.is_large_string(data_type):
        return ""
    if pa.types.is_binary(data_type) or pa.types.is_large_binary(data_type):
        return b""
    if pa.types.is_fixed_size_binary(data_type):
        return b"\x00" * data_type.byte_width
    if pa.types.is_date32(data_type):
        return pa.scalar(0, type=data_type).as_py()
    if pa.types.is_date64(data_type):
        return pa.scalar(0, type=data_type).as_py()
    if pa.types.is_time32(data_type):
        return pa.scalar(0, type=data_type).as_py()
    if pa.types.is_time64(data_type):
        return pa.scalar(0, type=data_type).as_py()
    if pa.types.is_timestamp(data_type):
        return pa.scalar(0, type=data_type).as_py()
    if pa.types.is_duration(data_type):
        return pa.scalar(0, type=data_type).as_py()
    if pa.types.is_dictionary(data_type):
        return _seed_value_for_type(data_type.value_type)
    if pa.types.is_struct(data_type):
        return {field.name: _seed_value_for_field(field) for field in data_type}
    if pa.types.is_list(data_type) or pa.types.is_large_list(data_type):
        return cast("list[object]", [])
    if pa.types.is_fixed_size_list(data_type):
        value = _seed_value_for_field(data_type.value_field)
        return [value for _ in range(data_type.list_size)]
    if pa.types.is_map(data_type):
        return cast("dict[str, object]", {})
    if pa.types.is_null(data_type):
        return None
    return pa.scalar(0, type=data_type).as_py()


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
    from datafusion_engine.registry_facade import registry_facade_for_context

    registry_facade = registry_facade_for_context(ctx, runtime_profile=profile)
    _ = registry_facade.register_dataset_df(
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

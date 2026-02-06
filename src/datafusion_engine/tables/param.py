"""Parameter table specifications and Arrow-native helpers."""

from __future__ import annotations

import re
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from enum import Enum
from functools import cache
from typing import cast

import pyarrow as pa
from datafusion import SessionContext
from datafusion.dataframe import DataFrame

from arrow_utils.core.array_iter import iter_array_values
from core.config_base import FingerprintableConfig, config_fingerprint
from datafusion_engine.arrow.coercion import to_arrow_table
from datafusion_engine.identity import schema_identity_hash
from datafusion_engine.io.adapter import DataFusionIOAdapter
from storage.ipc_utils import payload_hash
from utils.hashing import hash_sha256_hex
from utils.registry_protocol import ImmutableRegistry, MutableRegistry
from utils.uuid_factory import uuid7_suffix

SCALAR_PARAM_SIGNATURE_VERSION = 1
_PARAM_SIGNATURE_SEPARATOR = "\x1f"


@cache
def _scalar_param_signature_schema() -> pa.Schema:
    from datafusion_engine.session.runtime import dataset_schema_from_context

    schema = dataset_schema_from_context("scalar_param_signature_v1")
    if isinstance(schema, pa.Schema):
        return schema
    to_pyarrow = getattr(schema, "to_pyarrow", None)
    if callable(to_pyarrow):
        resolved = to_pyarrow()
        if isinstance(resolved, pa.Schema):
            return resolved
    msg = "DataFusion schema for scalar_param_signature_v1 is not a pyarrow.Schema."
    raise TypeError(msg)


class ParamTableScope(Enum):
    """Registration scope for parameter tables."""

    PER_RUN = "per_run"
    PER_SESSION = "per_session"
    PER_RULE = "per_rule"


@dataclass(frozen=True)
class ParamTablePolicy(FingerprintableConfig):
    """Policy controlling param table naming and registration."""

    scope: ParamTableScope = ParamTableScope.PER_RUN
    prefix: str = "p_"

    def fingerprint_payload(self) -> Mapping[str, object]:
        """Return fingerprint payload for the param table policy.

        Returns:
        -------
        Mapping[str, object]
            Payload describing param table policy settings.
        """
        return {
            "scope": self.scope.value,
            "prefix": self.prefix,
        }

    def fingerprint(self) -> str:
        """Return fingerprint for the param table policy.

        Returns:
        -------
        str
            Deterministic fingerprint for the policy.
        """
        return config_fingerprint(self.fingerprint_payload())


@dataclass(frozen=True)
class ParamTableSpec:
    """Declarative spec for a parameter table."""

    logical_name: str
    key_col: str
    schema: pa.Schema
    empty_semantics: str = "empty_result"
    distinct: bool = True


ListParamSpec = ParamTableSpec


@dataclass(frozen=True)
class ParamTableArtifact:
    """Materialized Arrow table plus signature metadata."""

    logical_name: str
    table: pa.Table
    signature: str
    rows: int
    schema_identity_hash: str


@dataclass
class ParamTableRegistry:
    """Registry for parameter table specs and artifacts."""

    specs: ImmutableRegistry[str, ParamTableSpec]
    policy: ParamTablePolicy = field(default_factory=ParamTablePolicy)
    artifacts: MutableRegistry[str, ParamTableArtifact] = field(default_factory=MutableRegistry)
    scope_key: str | None = None

    def __post_init__(self) -> None:
        """Normalize registry scope key after initialization."""
        if self.scope_key is None and self.policy.scope in {
            ParamTableScope.PER_RUN,
            ParamTableScope.PER_SESSION,
            ParamTableScope.PER_RULE,
        }:
            self.scope_key = _new_scope_key()
        if self.scope_key is not None:
            self.scope_key = _normalize_scope_key(self.scope_key)

    @classmethod
    def from_specs(
        cls,
        specs: Mapping[str, ParamTableSpec],
        *,
        policy: ParamTablePolicy,
        scope_key: str | None = None,
    ) -> ParamTableRegistry:
        """Build a registry from raw spec mappings.

        Returns:
        -------
        ParamTableRegistry
            Registry initialized with the provided specs.
        """
        return cls(
            specs=ImmutableRegistry.from_dict(specs),
            policy=policy,
            scope_key=scope_key,
        )

    def register_values(self, logical_name: str, values: Sequence[object]) -> ParamTableArtifact:
        """Register values for a param table and return the artifact.

        Args:
            logical_name: Logical parameter table name.
            values: Values to register for the parameter table.

        Returns:
            ParamTableArtifact: Result.

        Raises:
            KeyError: If no param table spec exists for `logical_name`.
            ValueError: If the spec is invalid for value registration.
        """
        spec = self.specs.get(logical_name)
        if spec is None:
            msg = f"Unknown param table spec: {logical_name!r}."
            raise KeyError(msg)
        if spec.key_col not in spec.schema.names:
            msg = f"ParamTableSpec missing key column: {spec.key_col!r}."
            raise ValueError(msg)
        values_array = _param_values_array(spec, values)
        signature = param_signature_from_array(logical_name=logical_name, values=values_array)
        schema_sig = schema_identity_hash(spec.schema)
        existing = self.artifacts.get(logical_name)
        if (
            existing is not None
            and existing.signature == signature
            and existing.schema_identity_hash == schema_sig
        ):
            return existing
        table = pa.table({spec.key_col: values_array}, schema=spec.schema)
        artifact = ParamTableArtifact(
            logical_name=logical_name,
            table=table,
            signature=signature,
            rows=table.num_rows,
            schema_identity_hash=schema_sig,
        )
        self.artifacts.register(logical_name, artifact, overwrite=True)
        return artifact

    def datafusion_tables(self, ctx: SessionContext) -> dict[str, DataFrame]:
        """Return DataFusion DataFrames for registered param tables.

        Returns:
        -------
        dict[str, datafusion.dataframe.DataFrame]
            DataFrames keyed by logical name.
        """
        adapter = DataFusionIOAdapter(ctx=ctx, profile=None)
        tables: dict[str, DataFrame] = {}
        for logical_name, artifact in self.artifacts.items():
            physical_name = _param_table_physical_name(self.policy, logical_name, self.scope_key)
            adapter.register_arrow_table(physical_name, artifact.table, overwrite=True)
            tables[logical_name] = ctx.table(physical_name)
        return tables


@dataclass(frozen=True)
class DataFusionParamBindings:
    """Resolved parameter bindings for DataFusion SQL execution."""

    param_values: Mapping[str, object]
    named_tables: Mapping[str, object]


def resolve_param_bindings(
    values: Mapping[str, object] | None,
    *,
    allowlist: Sequence[str] | None = None,
    validate_names: bool = True,
) -> DataFusionParamBindings:
    """Resolve scalar and table-like parameter bindings.

    Args:
        values: Input parameter mapping.
        allowlist: Optional allowed parameter names.
        validate_names: Whether to validate parameter naming rules.

    Returns:
        DataFusionParamBindings: Result.

    Raises:
        ValueError: If parameter names or values are invalid.
    """
    if not values:
        return DataFusionParamBindings(param_values={}, named_tables={})
    if allowlist is not None:
        allowed = set(allowlist)
        for name in values:
            if name not in allowed:
                msg = f"Parameter name {name!r} is not allowlisted."
                raise ValueError(msg)
    param_values: dict[str, object] = {}
    named_tables: dict[str, object] = {}
    for name, value in values.items():
        if validate_names and not _IDENT_RE.match(name):
            msg = f"Invalid parameter name: {name!r}."
            raise ValueError(msg)
        if _is_table_param(value):
            named_tables[name] = value
        else:
            param_values[name] = value
    return DataFusionParamBindings(param_values=param_values, named_tables=named_tables)


@contextmanager
def register_table_params(
    ctx: SessionContext,
    bindings: DataFusionParamBindings,
) -> Iterator[None]:
    """Register table-like parameters for SQL execution.

    Parameters
    ----------
    ctx
        DataFusion session context.
    bindings
        Resolved table bindings to register.
    """
    adapter = DataFusionIOAdapter(ctx=ctx, profile=None)
    registered: list[str] = []
    for name, value in bindings.named_tables.items():
        if isinstance(value, DataFrame):
            adapter.register_view(name, value, overwrite=True, temporary=True)
        else:
            table = to_arrow_table(value)
            adapter.register_arrow_table(name, table, overwrite=True)
        registered.append(name)
    try:
        yield None
    finally:
        for name in registered:
            adapter.deregister_table(name)


@contextmanager
def apply_bindings_to_context(
    ctx: SessionContext,
    bindings: DataFusionParamBindings,
) -> Iterator[None]:
    """Apply bindings to a DataFusion context for the scope of a block."""
    with register_table_params(ctx, bindings):
        yield None


def _is_table_param(value: object) -> bool:
    if isinstance(value, (pa.Table, pa.RecordBatch, pa.RecordBatchReader)):
        return True
    return isinstance(value, DataFrame)


_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_SCOPE_RE = re.compile(r"[^A-Za-z0-9_]+")


def param_table_name(policy: ParamTablePolicy, logical_name: str) -> str:
    """Return the physical table name for a param logical name.

    Args:
        policy: Description.
        logical_name: Description.

    Raises:
        ValueError: If the operation cannot be completed.
    """
    if not _IDENT_RE.match(logical_name):
        msg = f"Invalid param logical name: {logical_name!r}."
        raise ValueError(msg)
    return f"{policy.prefix}{logical_name}"


def _param_table_physical_name(
    policy: ParamTablePolicy,
    logical_name: str,
    scope_key: str | None,
) -> str:
    base = param_table_name(policy, logical_name)
    if scope_key is None:
        return base
    return f"{base}_{scope_key}"


def param_signature_from_array(
    *,
    logical_name: str,
    values: pa.Array | pa.ChunkedArray,
) -> str:
    """Return a stable signature for a parameter list from Arrow values.

    Returns:
    -------
    str
        Deterministic parameter signature.
    """
    normalized = values.combine_chunks() if isinstance(values, pa.ChunkedArray) else values
    resolved = cast("pa.Array", normalized)
    if resolved.num_rows == 0:
        payload = logical_name
        return hash_sha256_hex(payload.encode("utf-8"))
    raw_values = [value for value in resolved.to_pylist() if value is not None]
    if not raw_values:
        payload = logical_name
        return hash_sha256_hex(payload.encode("utf-8"))
    distinct = sorted({str(value) for value in raw_values})
    payload = _PARAM_SIGNATURE_SEPARATOR.join([logical_name, *distinct])
    return hash_sha256_hex(payload.encode("utf-8"))


def scalar_param_signature(values: Mapping[str, object]) -> str:
    """Return a stable signature for scalar parameter values.

    Returns:
    -------
    str
        Deterministic signature for scalar parameters.
    """
    entries = [{"key": str(key), "value": str(value)} for key, value in sorted(values.items())]
    payload = {"version": SCALAR_PARAM_SIGNATURE_VERSION, "entries": entries}
    return payload_hash(payload, _scalar_param_signature_schema())


def _param_values_array(
    spec: ParamTableSpec,
    values: Sequence[object],
) -> pa.Array | pa.ChunkedArray:
    field = spec.schema.field(spec.key_col)
    array = pa.array(list(values), type=field.type)
    if spec.distinct:
        array = unique_values(array)
    return array


def build_param_table(spec: ParamTableSpec, values: Sequence[object]) -> pa.Table:
    """Return an Arrow table for a parameter spec.

    Args:
        spec: Description.
        values: Description.

    Raises:
        ValueError: If the operation cannot be completed.
    """
    if spec.key_col not in spec.schema.names:
        msg = f"ParamTableSpec missing key column: {spec.key_col!r}."
        raise ValueError(msg)
    array = _param_values_array(spec, values)
    return pa.table({spec.key_col: array}, schema=spec.schema)


def unique_values(values: pa.Array | pa.ChunkedArray) -> pa.Array | pa.ChunkedArray:
    """Return unique values from an array-like input.

    Returns:
    -------
    pyarrow.Array | pyarrow.ChunkedArray
        Array with unique values preserved.
    """
    seen: set[object | None] = set()
    unique: list[object | None] = []
    for value in iter_array_values(values):
        if value in seen:
            continue
        seen.add(value)
        unique.append(value)
    return pa.array(unique, type=values.type)


def _new_scope_key() -> str:
    return uuid7_suffix(12)


def _normalize_scope_key(value: str) -> str:
    normalized = _SCOPE_RE.sub("_", value).strip("_")
    return normalized or "scope"


__all__ = [
    "DataFusionParamBindings",
    "ListParamSpec",
    "ParamTableArtifact",
    "ParamTablePolicy",
    "ParamTableRegistry",
    "ParamTableScope",
    "ParamTableSpec",
    "apply_bindings_to_context",
    "build_param_table",
    "param_signature_from_array",
    "param_table_name",
    "register_table_params",
    "resolve_param_bindings",
    "scalar_param_signature",
    "unique_values",
]

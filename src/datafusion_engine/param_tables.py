"""Parameter table specifications and Arrow-native helpers."""

from __future__ import annotations

import hashlib
import re
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from enum import Enum
from functools import cache
from typing import cast

import pyarrow as pa
from datafusion import SessionContext
from datafusion.dataframe import DataFrame

from arrowdsl.core.interop import pc
from arrowdsl.schema.abi import schema_fingerprint
from datafusion_engine.io_adapter import DataFusionIOAdapter
from storage.ipc import payload_hash

SCALAR_PARAM_SIGNATURE_VERSION = 1
_PARAM_SIGNATURE_SEPARATOR = "\x1f"


@cache
def _scalar_param_signature_schema() -> pa.Schema:
    from datafusion_engine.runtime import dataset_schema_from_context

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
class ParamTablePolicy:
    """Policy controlling param table naming and registration."""

    scope: ParamTableScope = ParamTableScope.PER_RUN
    prefix: str = "p_"


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
    schema_fingerprint: str


@dataclass
class ParamTableRegistry:
    """Registry for parameter table specs and artifacts."""

    specs: Mapping[str, ParamTableSpec]
    policy: ParamTablePolicy = field(default_factory=ParamTablePolicy)
    artifacts: dict[str, ParamTableArtifact] = field(default_factory=dict)
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

    def register_values(self, logical_name: str, values: Sequence[object]) -> ParamTableArtifact:
        """Register values for a param table and return the artifact.

        Returns
        -------
        ParamTableArtifact
            Materialized param table artifact.

        Raises
        ------
        KeyError
            Raised when the logical name is not registered.
        ValueError
            Raised when the spec key column is missing from the schema.
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
        schema_sig = schema_fingerprint(spec.schema)
        existing = self.artifacts.get(logical_name)
        if (
            existing is not None
            and existing.signature == signature
            and existing.schema_fingerprint == schema_sig
        ):
            return existing
        table = pa.table({spec.key_col: values_array}, schema=spec.schema)
        artifact = ParamTableArtifact(
            logical_name=logical_name,
            table=table,
            signature=signature,
            rows=table.num_rows,
            schema_fingerprint=schema_sig,
        )
        self.artifacts[logical_name] = artifact
        return artifact

    def datafusion_tables(self, ctx: SessionContext) -> dict[str, DataFrame]:
        """Return DataFusion DataFrames for registered param tables.

        Returns
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


_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_SCOPE_RE = re.compile(r"[^A-Za-z0-9_]+")


def param_table_name(policy: ParamTablePolicy, logical_name: str) -> str:
    """Return the physical table name for a param logical name.

    Returns
    -------
    str
        Physical table name.

    Raises
    ------
    ValueError
        Raised when the logical name is invalid.
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

    Returns
    -------
    str
        Deterministic parameter signature.
    """
    normalized = values.combine_chunks() if isinstance(values, pa.ChunkedArray) else values
    resolved = cast("pa.Array", normalized)
    if resolved.num_rows == 0:
        payload = logical_name
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()
    raw_values = [value for value in resolved.to_pylist() if value is not None]
    if not raw_values:
        payload = logical_name
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()
    distinct = sorted({str(value) for value in raw_values})
    payload = _PARAM_SIGNATURE_SEPARATOR.join([logical_name, *distinct])
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def scalar_param_signature(values: Mapping[str, object]) -> str:
    """Return a stable signature for scalar parameter values.

    Returns
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

    Returns
    -------
    pyarrow.Table
        Arrow table with parameter values.

    Raises
    ------
    ValueError
        Raised when the spec key column is missing from the schema.
    """
    if spec.key_col not in spec.schema.names:
        msg = f"ParamTableSpec missing key column: {spec.key_col!r}."
        raise ValueError(msg)
    array = _param_values_array(spec, values)
    return pa.table({spec.key_col: array}, schema=spec.schema)


def unique_values(values: pa.Array | pa.ChunkedArray) -> pa.Array | pa.ChunkedArray:
    """Return unique values from an array-like input.

    Returns
    -------
    pyarrow.Array | pyarrow.ChunkedArray
        Array with unique values preserved.
    """
    return pc.unique(values)


def _new_scope_key() -> str:
    return uuid.uuid4().hex[:12]


def _normalize_scope_key(value: str) -> str:
    normalized = _SCOPE_RE.sub("_", value).strip("_")
    return normalized or "scope"


__all__ = [
    "ListParamSpec",
    "ParamTableArtifact",
    "ParamTablePolicy",
    "ParamTableRegistry",
    "ParamTableScope",
    "ParamTableSpec",
    "build_param_table",
    "param_signature_from_array",
    "param_table_name",
    "scalar_param_signature",
    "unique_values",
]

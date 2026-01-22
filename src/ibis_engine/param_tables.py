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
from ibis.backends import BaseBackend
from ibis.expr.types import Table

from arrowdsl.core.interop import pc
from arrowdsl.schema.serialization import schema_fingerprint
from registry_common.arrow_payloads import payload_hash

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
    catalog: str = "codeintel"
    schema: str = "params"
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
    registered_signatures: dict[str, str] = field(default_factory=dict)
    registered_schema: str | None = None
    registered_catalog: str | None = None

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
            Arrow table plus signature metadata.

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

    def ibis_tables(self, backend: BaseBackend) -> dict[str, Table]:
        """Return Ibis table handles for registered param tables.

        Returns
        -------
        dict[str, ibis.expr.types.Table]
            Ibis table expressions keyed by logical name.
        """
        tables: dict[str, Table] = {}
        schema_name = param_table_schema(self.policy, scope_key=self.scope_key)
        for logical_name in self.artifacts:
            table_name = param_table_name(self.policy, logical_name)
            tables[logical_name] = backend.table(table_name, database=schema_name)
        return tables

    def register_into_backend(self, backend: BaseBackend) -> dict[str, str]:
        """Register param tables into an Ibis backend.

        Returns
        -------
        dict[str, str]
            Mapping of logical param names to qualified names.

        Raises
        ------
        TypeError
            Raised when the backend does not support table creation.
        """
        schema_name = param_table_schema(self.policy, scope_key=self.scope_key)
        if self.registered_catalog != self.policy.catalog:
            create_catalog = getattr(backend, "create_catalog", None)
            if callable(create_catalog):
                create_catalog(self.policy.catalog, force=True)
            self.registered_catalog = self.policy.catalog
        if self.registered_schema != schema_name:
            self.registered_signatures.clear()
            self.registered_schema = schema_name
        if self.artifacts:
            create_database = getattr(backend, "create_database", None)
            if callable(create_database):
                create_database(
                    schema_name,
                    catalog=self.policy.catalog,
                    force=True,
                )
        mapping: dict[str, str] = {}
        for logical_name, artifact in self.artifacts.items():
            table_name = param_table_name(self.policy, logical_name)
            qualified = f"{self.policy.catalog}.{schema_name}.{table_name}"
            if self.registered_signatures.get(logical_name) == artifact.signature:
                mapping[logical_name] = qualified
                continue
            create_table = getattr(backend, "create_table", None)
            if not callable(create_table):
                msg = "Ibis backend is missing create_table."
                raise TypeError(msg)
            create_table(
                table_name,
                artifact.table,
                database=schema_name,
                overwrite=True,
            )
            mapping[logical_name] = qualified
            self.registered_signatures[logical_name] = artifact.signature
        return mapping


_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_SCOPE_RE = re.compile(r"[^A-Za-z0-9_]+")


def param_table_name(policy: ParamTablePolicy, logical_name: str) -> str:
    """Return the physical table name for a param logical name.

    Returns
    -------
    str
        Fully qualified parameter table name suffix.

    Raises
    ------
    ValueError
        Raised when the logical name is not a valid identifier.
    """
    if not _IDENT_RE.match(logical_name):
        msg = f"Invalid param logical name: {logical_name!r}."
        raise ValueError(msg)
    return f"{policy.prefix}{logical_name}"


def param_table_schema(policy: ParamTablePolicy, *, scope_key: str | None) -> str:
    """Return the schema name for parameter tables.

    Returns
    -------
    str
        Schema name with scope suffix when configured.
    """
    if policy.scope in {
        ParamTableScope.PER_RUN,
        ParamTableScope.PER_SESSION,
        ParamTableScope.PER_RULE,
    }:
        return _scoped_schema(policy.schema, scope_key=scope_key)
    return policy.schema


def qualified_param_table_name(
    policy: ParamTablePolicy,
    logical_name: str,
    *,
    scope_key: str | None = None,
) -> str:
    """Return a fully qualified param table name.

    Returns
    -------
    str
        Qualified name in catalog.schema.table form.
    """
    schema_name = param_table_schema(policy, scope_key=scope_key)
    table_name = param_table_name(policy, logical_name)
    return f"{policy.catalog}.{schema_name}.{table_name}"


def param_signature_from_array(
    *,
    logical_name: str,
    values: pa.Array | pa.ChunkedArray,
) -> str:
    """Return a stable signature for a parameter list from Arrow values.

    Returns
    -------
    str
        Hex-encoded signature string.
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
        Hex-encoded signature string.
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
        Arrow table with optional distinct enforcement.

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
        Unique values for the input array-like.
    """
    return pc.unique(values)


def _new_scope_key() -> str:
    return uuid.uuid4().hex[:12]


def _normalize_scope_key(value: str) -> str:
    normalized = _SCOPE_RE.sub("_", value).strip("_")
    return normalized or "scope"


def _scoped_schema(base: str, *, scope_key: str | None) -> str:
    if not scope_key:
        return base
    return f"{base}_{scope_key}"


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
    "param_table_schema",
    "qualified_param_table_name",
    "scalar_param_signature",
    "unique_values",
]

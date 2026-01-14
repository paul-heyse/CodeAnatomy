"""Parameter table specifications and Arrow-native helpers."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from enum import Enum

import pyarrow as pa
import pyarrow.compute as pc
from ibis.backends import BaseBackend
from ibis.expr.types import Table

from arrowdsl.schema.schema import schema_fingerprint


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
        """
        spec = self.specs.get(logical_name)
        if spec is None:
            msg = f"Unknown param table spec: {logical_name!r}."
            raise KeyError(msg)
        table = build_param_table(spec, values)
        signature = param_signature(logical_name=logical_name, values=values)
        artifact = ParamTableArtifact(
            logical_name=logical_name,
            table=table,
            signature=signature,
            rows=table.num_rows,
            schema_fingerprint=schema_fingerprint(table.schema),
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
        for logical_name in self.artifacts:
            table_name = param_table_name(self.policy, logical_name)
            tables[logical_name] = backend.table(table_name, database=self.policy.schema)
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
        if self.artifacts:
            create_database = getattr(backend, "create_database", None)
            if callable(create_database):
                create_database(
                    self.policy.schema,
                    catalog=self.policy.catalog,
                    force=True,
                )
        mapping: dict[str, str] = {}
        for logical_name, artifact in self.artifacts.items():
            table_name = param_table_name(self.policy, logical_name)
            create_table = getattr(backend, "create_table", None)
            if not callable(create_table):
                msg = "Ibis backend is missing create_table."
                raise TypeError(msg)
            create_table(
                table_name,
                artifact.table,
                database=self.policy.schema,
                overwrite=True,
            )
            mapping[logical_name] = f"{self.policy.catalog}.{self.policy.schema}.{table_name}"
        return mapping


_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


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


def param_signature(*, logical_name: str, values: Sequence[object]) -> str:
    """Return a stable signature for a parameter list.

    Returns
    -------
    str
        Hex-encoded signature string.
    """
    normalized = sorted(str(value) for value in values)
    payload = {"name": logical_name, "values": normalized}
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


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
    field = spec.schema.field(spec.key_col)
    array = pa.array(list(values), type=field.type)
    table = pa.table({spec.key_col: array}, schema=spec.schema)
    if spec.distinct:
        unique = unique_values(table[spec.key_col])
        table = pa.table({spec.key_col: unique}, schema=spec.schema)
    return table


def unique_values(values: pa.Array | pa.ChunkedArray) -> pa.Array | pa.ChunkedArray:
    """Return unique values from an array-like input.

    Returns
    -------
    pyarrow.Array | pyarrow.ChunkedArray
        Unique values for the input array-like.

    Raises
    ------
    RuntimeError
        Raised when pyarrow.compute.unique is unavailable.
    """
    unique_fn = getattr(pc, "unique", None)
    if unique_fn is None:
        msg = "pyarrow.compute.unique is unavailable."
        raise RuntimeError(msg)
    return unique_fn(values)


__all__ = [
    "ListParamSpec",
    "ParamTableArtifact",
    "ParamTablePolicy",
    "ParamTableRegistry",
    "ParamTableScope",
    "ParamTableSpec",
    "build_param_table",
    "param_signature",
    "param_table_name",
    "unique_values",
]

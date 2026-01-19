"""Registry signature helpers for Delta Lake exports."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, cast

import pyarrow as pa

from arrowdsl.spec.io import table_from_rows
from core_types import PathLike, ensure_path
from registry_common.arrow_payloads import ipc_hash
from storage.deltalake.delta import read_table_delta

if TYPE_CHECKING:
    from storage.deltalake.delta import StorageOptions

REGISTRY_SIGNATURE_TABLE = "registry_signatures"

REGISTRY_SIGNATURE_SCHEMA = pa.schema(
    [
        pa.field("registry", pa.string(), nullable=False),
        pa.field("signature", pa.string(), nullable=False),
        pa.field("source", pa.string(), nullable=False),
    ],
    metadata={b"spec_kind": b"registry_signatures"},
)


@dataclass(frozen=True)
class RegistrySignature:
    """Signature payload for a registry export."""

    registry: str
    signature: str
    source: str

    def to_row(self) -> dict[str, object]:
        """Return a row mapping for Arrow serialization.

        Returns
        -------
        dict[str, object]
            Row payload for Arrow serialization.
        """
        return {
            "registry": self.registry,
            "signature": self.signature,
            "source": self.source,
        }


def registry_signature_table(signatures: tuple[RegistrySignature, ...]) -> pa.Table:
    """Return a registry signature table.

    Returns
    -------
    pyarrow.Table
        Arrow table of registry signatures.
    """
    rows = [sig.to_row() for sig in signatures]
    return table_from_rows(REGISTRY_SIGNATURE_SCHEMA, rows)


def registry_signature_from_tables(
    registry: str,
    tables: dict[str, pa.Table],
    *,
    source: str = "table_ipc",
) -> RegistrySignature:
    """Return a signature based on IPC hashes of tables.

    Returns
    -------
    RegistrySignature
        Registry signature derived from table IPC hashes.
    """
    ordered = {name: ipc_hash(table) for name, table in sorted(tables.items())}
    payload = json.dumps(ordered, sort_keys=True, separators=(",", ":")).encode("utf-8")
    digest = hashlib.sha256(payload).hexdigest()
    return RegistrySignature(registry=registry, signature=digest, source=source)


def should_regenerate(
    *,
    current: str | None,
    next_signature: str,
    force: bool,
) -> bool:
    """Return whether registry outputs should be regenerated.

    Returns
    -------
    bool
        True when regeneration is required.
    """
    return force or current is None or current != next_signature


def read_registry_signature(
    base_dir: PathLike,
    *,
    registry: str,
    storage_options: StorageOptions | None = None,
) -> RegistrySignature | None:
    """Read the stored registry signature for a registry target.

    Returns
    -------
    RegistrySignature | None
        Stored signature payload when available.

    Raises
    ------
    TypeError
        Raised when the signature table is not a pyarrow.Table.
    """
    table_path = _signature_table_path(base_dir)
    if not _delta_table_exists(table_path):
        return None
    table = read_table_delta(str(table_path), storage_options=storage_options)
    if not isinstance(table, pa.Table):
        msg = f"Expected pyarrow.Table for registry signature, got {type(table)!r}."
        raise TypeError(msg)
    table = cast("pa.Table", table)
    for row in table.to_pylist():
        if row.get("registry") == registry:
            signature = row.get("signature")
            source = row.get("source")
            if isinstance(signature, str) and isinstance(source, str):
                return RegistrySignature(registry=registry, signature=signature, source=source)
    return None


def _signature_table_path(base_dir: PathLike) -> Path:
    """Return the registry signature table path.

    Returns
    -------
    pathlib.Path
        Filesystem path to the signature table.
    """
    return ensure_path(base_dir) / REGISTRY_SIGNATURE_TABLE


def _delta_table_exists(path: Path) -> bool:
    """Return whether the delta table exists at the path.

    Returns
    -------
    bool
        True when the delta table path exists.
    """
    return path.is_dir() and (path / "_delta_log").exists()


__all__ = [
    "REGISTRY_SIGNATURE_SCHEMA",
    "REGISTRY_SIGNATURE_TABLE",
    "RegistrySignature",
    "read_registry_signature",
    "registry_signature_from_tables",
    "registry_signature_table",
    "should_regenerate",
]

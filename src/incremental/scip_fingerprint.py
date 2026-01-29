"""SCIP index fingerprint tracking for incremental runs."""

from __future__ import annotations

from pathlib import Path

from incremental.state_store import StateStore
from utils.hashing import hash_file_sha256

_CHUNK_SIZE = 1024 * 1024


def scip_index_fingerprint(path: Path) -> str:
    """Return a stable SHA-256 fingerprint for a SCIP index file.

    Returns
    -------
    str
        Hex-encoded fingerprint of the file contents.
    """
    return hash_file_sha256(path, chunk_size=_CHUNK_SIZE)


def read_scip_fingerprint(state_store: StateStore) -> str | None:
    """Return the previous SCIP fingerprint when present.

    Returns
    -------
    str | None
        Stored fingerprint, or None if missing.
    """
    path = state_store.scip_fingerprint_path()
    if not path.exists():
        return None
    value = path.read_text(encoding="utf-8").strip()
    return value or None


def write_scip_fingerprint(state_store: StateStore, fingerprint: str) -> None:
    """Persist the SCIP fingerprint to the state store."""
    state_store.ensure_dirs()
    path = state_store.scip_fingerprint_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(fingerprint, encoding="utf-8")


def scip_fingerprint_changed(
    *,
    state_store: StateStore,
    scip_index_path: str | Path | None,
) -> bool:
    """Return True when the SCIP index fingerprint differs from the previous run.

    Returns
    -------
    bool
        True if the fingerprint changed, otherwise False.
    """
    if scip_index_path is None:
        return False
    path = Path(scip_index_path)
    if not path.exists():
        return False
    current = scip_index_fingerprint(path)
    previous = read_scip_fingerprint(state_store)
    write_scip_fingerprint(state_store, current)
    if previous is None:
        return False
    return current != previous


__all__ = [
    "read_scip_fingerprint",
    "scip_fingerprint_changed",
    "scip_index_fingerprint",
    "write_scip_fingerprint",
]

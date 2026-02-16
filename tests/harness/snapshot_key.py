"""Snapshot-key helpers for Delta identity conformance tests."""

from __future__ import annotations

from storage.deltalake.delta_metadata import snapshot_key_for_table


def snapshot_key_payload(table_uri: str, version: int) -> dict[str, object]:
    """Build a normalized snapshot-key payload.

    Returns:
    -------
    dict[str, object]
        Canonical URI and resolved table version for snapshot identity.
    """
    key = snapshot_key_for_table(table_uri, version)
    return {"canonical_uri": key.canonical_uri, "resolved_version": key.version}


def assert_snapshot_key_payload(
    payload: dict[str, object],
    *,
    table_uri: str,
    version: int,
) -> None:
    """Assert payload matches canonical snapshot-key expectations."""
    expected = snapshot_key_payload(table_uri, version)
    assert payload["canonical_uri"] == expected["canonical_uri"]
    assert payload["resolved_version"] == expected["resolved_version"]

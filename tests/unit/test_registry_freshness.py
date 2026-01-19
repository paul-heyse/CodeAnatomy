"""Registry freshness hashing helpers."""

from __future__ import annotations

import pyarrow as pa

from storage.deltalake.registry_freshness import registry_signature_from_tables, should_regenerate


def _sample_table(value: int) -> pa.Table:
    return pa.table({"value": [value]})


def test_registry_signature_is_stable() -> None:
    """Signature stays stable for the same inputs."""
    tables = {"alpha": _sample_table(1)}
    sig_a = registry_signature_from_tables("demo", tables)
    sig_b = registry_signature_from_tables("demo", tables)
    assert sig_a.signature == sig_b.signature


def test_registry_signature_changes_with_data() -> None:
    """Signature changes when table contents change."""
    sig_a = registry_signature_from_tables("demo", {"alpha": _sample_table(1)})
    sig_b = registry_signature_from_tables("demo", {"alpha": _sample_table(2)})
    assert sig_a.signature != sig_b.signature


def test_should_regenerate_respects_force() -> None:
    """Force flag always triggers regeneration."""
    assert should_regenerate(current="a", next_signature="a", force=True) is True

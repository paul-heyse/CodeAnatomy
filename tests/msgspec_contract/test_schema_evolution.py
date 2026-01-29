"""Schema evolution snapshots for msgspec contracts."""

from __future__ import annotations

from msgspec.inspect import multi_type_info

from serde_msgspec import dumps_json_sorted, to_builtins_sorted
from serde_schema_registry import SCHEMA_TYPES
from tests.msgspec_contract._support.goldens import GOLDENS_DIR, assert_text_snapshot


def _schema_evolution_snapshot() -> dict[str, object]:
    info = multi_type_info(SCHEMA_TYPES)
    field_order = {
        schema_type.__name__: list(schema_type.__struct_fields__) for schema_type in SCHEMA_TYPES
    }
    return {
        "type_info": to_builtins_sorted(info, str_keys=True),
        "field_order": field_order,
    }


def test_schema_evolution_snapshot(*, update_goldens: bool) -> None:
    """Snapshot schema evolution metadata for all registered types."""
    payload = _schema_evolution_snapshot()
    text = dumps_json_sorted(payload, pretty=True).decode("utf-8")
    assert_text_snapshot(
        path=GOLDENS_DIR / "schema_evolution.json",
        text=text,
        update=update_goldens,
    )

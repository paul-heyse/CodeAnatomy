"""Golden plan artifact tests for DataFusion planning outputs."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from typing import TYPE_CHECKING

import pyarrow as pa
import pytest

from datafusion_engine.sql_options import planning_sql_options
from serde_msgspec import dumps_msgpack

datafusion = pytest.importorskip("datafusion")

if TYPE_CHECKING:
    from collections.abc import Mapping

    from datafusion import SessionContext


_FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures"
_GOLDEN_PATH = _FIXTURES_DIR / "simple_query.json"
_SQL = "SELECT id FROM events WHERE id > 1"


def _normalize_text(value: str) -> str:
    """Normalize text to ASCII-safe output.

    Returns
    -------
    str
        ASCII-safe representation of the input.
    """
    return value.encode("ascii", "backslashreplace").decode("ascii")


def _normalize_rows(
    rows: list[dict[str, object]],
    *,
    sort_rows: bool,
) -> list[dict[str, object]]:
    """Normalize and optionally sort row dictionaries for deterministic output.

    Returns
    -------
    list[dict[str, object]]
        Normalized row dictionaries.
    """
    normalized = [{key: _normalize_value(val) for key, val in row.items()} for row in rows]
    if sort_rows:
        normalized.sort(key=lambda row: json.dumps(row, sort_keys=True))
    return normalized


def _normalize_value(value: object) -> object:
    """Normalize nested values for stable JSON comparisons.

    Returns
    -------
    object
        Normalized value for JSON serialization.
    """
    if isinstance(value, str):
        return _normalize_text(value)
    if isinstance(value, dict):
        return {key: _normalize_value(val) for key, val in sorted(value.items())}
    if isinstance(value, list):
        return [_normalize_value(item) for item in value]
    if isinstance(value, tuple):
        return [_normalize_value(item) for item in value]
    return value


def _df_settings_mapping(rows: list[dict[str, object]]) -> dict[str, str]:
    """Build a key/value mapping from df_settings rows.

    Returns
    -------
    dict[str, str]
        Mapping of settings names to string values.
    """
    mapping: dict[str, str] = {}
    for row in rows:
        name = row.get("name") or row.get("setting_name") or row.get("key")
        if name is None:
            continue
        value = row.get("value")
        mapping[str(name)] = "" if value is None else str(value)
    return mapping


def _information_schema_rows(ctx: SessionContext, *, table: str) -> list[dict[str, object]]:
    """Fetch rows from the information_schema table using planning SQL options.

    Returns
    -------
    list[dict[str, object]]
        Rows from the selected information_schema table.
    """
    df = ctx.sql_with_options(
        f"SELECT * FROM information_schema.{table}",
        planning_sql_options(None),
    )
    return df.to_arrow_table().to_pylist()


def _information_schema_snapshot(ctx: SessionContext) -> dict[str, object]:
    """Capture an information_schema snapshot for plan hashing.

    Returns
    -------
    dict[str, object]
        Snapshot payload of information_schema tables.
    """
    settings = _information_schema_rows(ctx, table="df_settings")
    routines = _information_schema_rows(ctx, table="routines")
    return {
        "df_settings": _df_settings_mapping(settings),
        "settings": settings,
        "tables": _information_schema_rows(ctx, table="tables"),
        "schemata": _information_schema_rows(ctx, table="schemata"),
        "columns": _information_schema_rows(ctx, table="columns"),
        "routines": routines,
        "parameters": _information_schema_rows(ctx, table="parameters"),
        "function_catalog": routines,
    }


def _info_schema_hash(snapshot: Mapping[str, object]) -> str:
    """Hash the information schema snapshot for change detection.

    Returns
    -------
    str
        Hex digest of the snapshot payload.
    """
    payload = dumps_msgpack(snapshot)
    return hashlib.sha256(payload).hexdigest()


def _explain_rows(ctx: SessionContext, *, prefix: str, sql: str) -> list[dict[str, object]]:
    """Execute an explain query and return rows.

    Returns
    -------
    list[dict[str, object]]
        Explain rows for the specified query.
    """
    df = ctx.sql(f"{prefix} {sql}")
    table = df.to_arrow_table()
    return table.to_pylist()


def _fixture_payload(ctx: SessionContext) -> dict[str, object]:
    """Generate the golden fixture payload for the simple query.

    Returns
    -------
    dict[str, object]
        JSON-serializable fixture payload.
    """
    tree_rows = _explain_rows(ctx, prefix="EXPLAIN FORMAT TREE", sql=_SQL)
    verbose_rows = _explain_rows(ctx, prefix="EXPLAIN VERBOSE", sql=_SQL)
    info_snapshot = _information_schema_snapshot(ctx)
    info_hash = _info_schema_hash(info_snapshot)
    return {
        "sql": _SQL,
        "explain_tree": _normalize_rows(tree_rows, sort_rows=False),
        "explain_verbose": _normalize_rows(verbose_rows, sort_rows=False),
        "df_settings": _normalize_value(info_snapshot["df_settings"]),
        "information_schema_snapshot": {
            key: _normalize_rows(value, sort_rows=True)
            if isinstance(value, list)
            else _normalize_value(value)
            for key, value in info_snapshot.items()
        },
        "information_schema_hash": info_hash,
    }


def _load_golden(path: Path) -> dict[str, object]:
    """Load the golden fixture JSON from disk.

    Returns
    -------
    dict[str, object]
        Parsed JSON payload.

    Raises
    ------
    TypeError
        Raised when the fixture does not contain a JSON object.
    """
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        msg = "Golden fixture must contain a JSON object."
        raise TypeError(msg)
    return data


def _write_golden(path: Path, payload: Mapping[str, object]) -> None:
    """Write the golden fixture JSON to disk."""
    path.parent.mkdir(parents=True, exist_ok=True)
    rendered = json.dumps(payload, indent=2, sort_keys=True)
    path.write_text(rendered + "\n", encoding="utf-8")


def test_plan_artifact_golden_fixture() -> None:
    """Compare plan artifacts to the golden fixture for regressions."""
    ctx = datafusion.SessionContext()
    ctx.register_record_batches(
        "events",
        [pa.table({"id": [1, 2], "label": ["a", "b"]}).to_batches()],
    )
    payload = _fixture_payload(ctx)
    if os.environ.get("CODEANATOMY_UPDATE_GOLDEN") == "1":
        _write_golden(_GOLDEN_PATH, payload)
        pytest.skip("Golden fixture updated.")
    expected = _load_golden(_GOLDEN_PATH)
    assert payload == expected

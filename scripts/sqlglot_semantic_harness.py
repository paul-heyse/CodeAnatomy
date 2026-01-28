"""Run semantic equivalence checks for SQLGlot rewrites."""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path

from sqlglot.executor import execute

from serde_msgspec import json_default


def _load_text(path: Path) -> str:
    with path.open("r", encoding="utf-8") as handle:
        return handle.read()


def _load_tables(path: Path | None) -> dict[str, list[dict[str, object]]]:
    if path is None:
        return {}
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, Mapping):
        msg = "Tables JSON must be an object mapping table names to row lists."
        raise TypeError(msg)
    tables: dict[str, list[dict[str, object]]] = {}
    for name, rows in payload.items():
        if not isinstance(name, str):
            msg = "Tables JSON keys must be strings."
            raise TypeError(msg)
        if not isinstance(rows, Sequence):
            msg = "Tables JSON values must be sequences of row mappings."
            raise TypeError(msg)
        row_list: list[dict[str, object]] = []
        for row in rows:
            if not isinstance(row, Mapping):
                msg = "Each table row must be an object mapping column names to values."
                raise TypeError(msg)
            row_list.append(dict(row))
        tables[name] = row_list
    return tables


def _execute_sql(
    sql: str,
    *,
    tables: Mapping[str, Sequence[Mapping[str, object]]],
) -> list[tuple[object, ...]]:
    return execute(sql, tables=tables)


def _write_output(path: Path, payload: Mapping[str, object]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, default=json_default, ensure_ascii=True, separators=(",", ":"))


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Compare SQLGlot rewrite outputs.")
    parser.add_argument("--original-sql", default=None, help="Original SQL string.")
    parser.add_argument("--rewritten-sql", default=None, help="Rewritten SQL string.")
    parser.add_argument("--original-file", default=None, help="Path to original SQL file.")
    parser.add_argument("--rewritten-file", default=None, help="Path to rewritten SQL file.")
    parser.add_argument("--tables-json", default=None, help="JSON file with table fixtures.")
    parser.add_argument("--output-json", default=None, help="Write results to JSON file.")
    parser.add_argument(
        "--fail-on-mismatch",
        action="store_true",
        help="Exit 1 when outputs differ.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Run the SQLGlot semantic equivalence harness.

    Returns
    -------
    int
        Process exit code.
    """
    parser = _build_parser()
    args = parser.parse_args(argv)
    original_sql = args.original_sql
    rewritten_sql = args.rewritten_sql
    if args.original_file:
        original_sql = _load_text(Path(args.original_file))
    if args.rewritten_file:
        rewritten_sql = _load_text(Path(args.rewritten_file))
    if not original_sql or not rewritten_sql:
        parser.error("Provide --original-sql/--original-file and --rewritten-sql/--rewritten-file.")
    tables = _load_tables(Path(args.tables_json)) if args.tables_json else {}
    left = _execute_sql(original_sql, tables=tables)
    right = _execute_sql(rewritten_sql, tables=tables)
    matches = left == right
    payload = {
        "matches": matches,
        "left_rows": left,
        "right_rows": right,
    }
    if args.output_json:
        _write_output(Path(args.output_json), payload)
    sys.stdout.write(f"match: {str(matches).lower()}\n")
    if not matches and args.fail_on_mismatch:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

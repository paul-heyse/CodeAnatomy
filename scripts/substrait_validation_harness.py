"""Run a Substrait cross-engine validation harness."""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path

from datafusion import SessionContext
from datafusion_engine.bridge import collect_plan_artifacts, sqlglot_to_datafusion
from ibis_engine.registry import DatasetLocation
from sqlglot_tools.optimizer import parse_sql_strict

from arrow_utils.schema.build import table_from_rows
from datafusion_engine.compile_options import DataFusionCompileOptions
from datafusion_engine.registry_bridge import register_dataset_df
from datafusion_engine.runtime import DataFusionRuntimeProfile
from serde_msgspec import json_default


def _load_rows(path: Path) -> list[dict[str, object]]:
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, Sequence):
        msg = "Rows JSON must be a list of row objects."
        raise TypeError(msg)
    rows: list[dict[str, object]] = []
    for row in payload:
        if not isinstance(row, Mapping):
            msg = "Each row entry must be an object mapping column names to values."
            raise TypeError(msg)
        rows.append(dict(row))
    return rows


def _register_input(
    ctx: SessionContext,
    *,
    table_name: str,
    delta_path: Path | None,
    parquet_path: Path | None,
    rows_path: Path | None,
) -> None:
    sources = [delta_path is not None, parquet_path is not None, rows_path is not None]
    if sum(sources) > 1:
        msg = "Provide only one of --delta, --parquet, or --rows-json."
        raise ValueError(msg)
    if delta_path is not None:
        if not delta_path.exists():
            msg = f"Delta path does not exist: {delta_path}"
            raise FileNotFoundError(msg)
        location = DatasetLocation(path=str(delta_path), format="delta")
        register_dataset_df(ctx, name=table_name, location=location)
        return
    if parquet_path is not None:
        if not parquet_path.exists():
            msg = f"Parquet path does not exist: {parquet_path}"
            raise FileNotFoundError(msg)
        location = DatasetLocation(path=str(parquet_path), format="parquet")
        register_dataset_df(ctx, name=table_name, location=location)
        return
    if rows_path is not None:
        rows = _load_rows(rows_path)
    else:
        rows = [{"id": 1, "label": "alpha"}, {"id": 2, "label": "beta"}]
    table = table_from_rows(rows)
    ctx.register_record_batches(table_name, [table.to_batches()])


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run PyArrow Substrait validation against DataFusion outputs."
    )
    parser.add_argument("--sql", default=None, help="SQL statement to validate.")
    parser.add_argument("--sql-file", default=None, help="File containing SQL to validate.")
    parser.add_argument("--table-name", default="input_table", help="Table name to register.")
    parser.add_argument("--delta", default=None, help="Delta table path for input data.")
    parser.add_argument("--parquet", default=None, help="Parquet path for input data.")
    parser.add_argument("--rows-json", default=None, help="JSON file of row mappings.")
    parser.add_argument("--output-json", default=None, help="Write results to JSON file.")
    parser.add_argument(
        "--fail-on-mismatch",
        action="store_true",
        help="Exit non-zero when validation fails or mismatches.",
    )
    return parser


def _resolve_sql(sql: str | None, sql_file: str | None) -> str:
    if sql is None and sql_file is None:
        msg = "Provide --sql or --sql-file."
        raise ValueError(msg)
    if sql is not None and sql_file is not None:
        msg = "Provide only one of --sql or --sql-file."
        raise ValueError(msg)
    if sql_file is None:
        return str(sql)
    path = Path(sql_file)
    return path.read_text(encoding="utf-8")


def _write_output(path: Path | None, payload: Mapping[str, object]) -> None:
    if path is None:
        encoded = json.dumps(
            payload, default=json_default, ensure_ascii=True, separators=(",", ":")
        )
        sys.stdout.write(f"{encoded}\n")
        return
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, default=json_default, ensure_ascii=True, separators=(",", ":"))


def main(argv: Sequence[str] | None = None) -> int:
    """Run the Substrait validation harness.

    Returns
    -------
    int
        Exit status code.
    """
    parser = _build_parser()
    args = parser.parse_args(argv)
    sql = _resolve_sql(args.sql, args.sql_file)
    ctx = DataFusionRuntimeProfile().session_context()
    _register_input(
        ctx,
        table_name=args.table_name,
        delta_path=Path(args.delta) if args.delta is not None else None,
        parquet_path=Path(args.parquet) if args.parquet is not None else None,
        rows_path=Path(args.rows_json) if args.rows_json is not None else None,
    )
    options = DataFusionCompileOptions(substrait_validation=True)
    expr = parse_sql_strict(sql, dialect=options.dialect)
    df = sqlglot_to_datafusion(expr, ctx=ctx, options=options)
    artifacts = collect_plan_artifacts(ctx, expr, options=options, df=df)
    payload = artifacts.payload()
    _write_output(Path(args.output_json) if args.output_json is not None else None, payload)
    validation = payload.get("substrait_validation")
    if args.fail_on_mismatch and isinstance(validation, Mapping):
        status = validation.get("status")
        if status not in {"match", None}:
            return 1
        if validation.get("match") is False:
            return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

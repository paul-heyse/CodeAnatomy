"""SQLGlot rewrite harness for normalization diagnostics."""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Mapping, Sequence
from dataclasses import replace

from sqlglot import ErrorLevel, parse_one
from sqlglot.diff import Delete, Insert, Keep, Update, diff
from sqlglot.serde import dump

from sqlglot_tools.optimizer import (
    NormalizeExprOptions,
    canonical_ast_fingerprint,
    default_sqlglot_policy,
    normalize_expr_with_stats,
    sqlglot_policy_snapshot,
    sqlglot_sql,
)


def _load_sql(args: argparse.Namespace) -> str:
    if args.sql:
        return str(args.sql)
    if args.path:
        return str(args.path.read_text(encoding="utf-8"))
    return sys.stdin.read()


def _load_schema(value: str | None) -> Mapping[str, Mapping[str, str]] | None:
    if not value:
        return None
    data = json.loads(value)
    if not isinstance(data, Mapping):
        msg = "schema-json must be a JSON object."
        raise TypeError(msg)
    return data


def _diff_summary(left: object, right: object) -> dict[str, object]:
    changes = list(diff(left, right))
    counts = {
        "insert": sum(isinstance(change, Insert) for change in changes),
        "delete": sum(isinstance(change, Delete) for change in changes),
        "update": sum(isinstance(change, Update) for change in changes),
        "keep": sum(isinstance(change, Keep) for change in changes),
    }
    labels = [change.__class__.__name__ for change in changes]
    return {"counts": counts, "labels": labels}


def _normalize_payload(
    sql: str, *, policy_dialect: str, schema: Mapping[str, Mapping[str, str]] | None
) -> dict[str, object]:
    policy = default_sqlglot_policy()
    policy = replace(policy, read_dialect=policy_dialect, write_dialect=policy_dialect)
    parsed = parse_one(sql, dialect=policy.read_dialect, error_level=ErrorLevel.RAISE)
    result = normalize_expr_with_stats(
        parsed,
        options=NormalizeExprOptions(schema=schema, policy=policy, sql=sql),
    )
    return {
        "raw_sql": sql,
        "normalized_sql": sqlglot_sql(result.expr, policy=policy),
        "ast": {
            "raw": dump(parsed),
            "normalized": dump(result.expr),
        },
        "fingerprint": canonical_ast_fingerprint(result.expr),
        "normalization": {
            "distance": result.stats.distance,
            "max_distance": result.stats.max_distance,
            "applied": result.stats.applied,
        },
        "diff": _diff_summary(parsed, result.expr),
        "policy_snapshot": sqlglot_policy_snapshot().payload(),
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="SQLGlot rewrite harness")
    parser.add_argument("--sql", type=str, help="SQL string to normalize")
    parser.add_argument(
        "--path",
        type=argparse.FileType("r", encoding="utf-8"),
        help="Path to a SQL file",
    )
    parser.add_argument(
        "--dialect",
        type=str,
        default="datafusion",
        help="SQL dialect to use for parsing and rendering",
    )
    parser.add_argument(
        "--schema-json",
        type=str,
        default=None,
        help="JSON mapping of table schemas for qualification",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Run the SQLGlot rewrite harness.

    Returns
    -------
    int
        Process exit code.
    """
    parser = _build_parser()
    args = parser.parse_args(argv)
    sql = _load_sql(args)
    schema = _load_schema(args.schema_json)
    payload = _normalize_payload(sql, policy_dialect=args.dialect, schema=schema)
    sys.stdout.write(json.dumps(payload, ensure_ascii=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

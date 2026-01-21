#!/usr/bin/env python3
"""Generate scip-python environment JSON manifests."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from extract.scip_indexer import scip_environment_payload, write_scip_environment_json


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Emit scip-python environment JSON (stdout or --output)."
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Optional output path for env JSON (default: stdout).",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    payload = scip_environment_payload()
    if args.output is not None:
        write_scip_environment_json(args.output)
        return 0
    json.dump(payload, sys.stdout, indent=2)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

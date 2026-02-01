#!/usr/bin/env python3
"""Compare semantic plan fingerprint snapshots for CI gating."""

from __future__ import annotations

import argparse
import json
from pathlib import Path


def _load_payload(path: Path) -> dict[str, dict[str, object]]:
    payload = json.loads(path.read_text())
    if not isinstance(payload, dict):
        msg = f"Expected JSON object in {path}."
        raise ValueError(msg)
    views = payload.get("views")
    if isinstance(views, dict):
        return {str(name): dict(value) for name, value in views.items() if isinstance(value, dict)}
    return {str(name): dict(value) for name, value in payload.items() if isinstance(value, dict)}


def _diff_views(
    before: dict[str, dict[str, object]],
    after: dict[str, dict[str, object]],
) -> list[str]:
    changed: list[str] = []
    for view_name, payload in after.items():
        before_payload = before.get(view_name)
        if before_payload is None:
            changed.append(view_name)
            continue
        if before_payload.get("logical_plan_hash") != payload.get("logical_plan_hash"):
            changed.append(view_name)
            continue
        if before_payload.get("schema_hash") != payload.get("schema_hash"):
            changed.append(view_name)
    return sorted(set(changed))


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__ or "")
    parser.add_argument("--before", type=Path, required=True, help="Baseline fingerprint JSON")
    parser.add_argument("--after", type=Path, required=True, help="New fingerprint JSON")
    return parser


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()
    before = _load_payload(args.before)
    after = _load_payload(args.after)
    changed = _diff_views(before, after)
    if not changed:
        print("Semantic plan fingerprints unchanged.")
        return 0
    print("Semantic plan fingerprints changed:")
    for name in changed:
        print(f"- {name}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())

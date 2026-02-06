#!/usr/bin/env python3
"""Generate a scip-python environment manifest JSON on stdout."""

from __future__ import annotations

import json
import sys
from importlib import metadata


def scip_env_payload() -> list[dict[str, object]]:
    """Return environment payload entries for installed distributions.

    Returns:
    -------
    list[dict[str, object]]
        Environment payload entries.
    """
    env: list[dict[str, object]] = []
    for dist in metadata.distributions():
        name = dist.metadata.get("Name") or dist.name
        files = sorted({str(path) for path in (dist.files or ())})
        env.append(
            {
                "name": str(name),
                "version": str(dist.version),
                "files": files,
            }
        )
    env.sort(key=lambda entry: str(entry["name"]).lower())
    return env


def main() -> int:
    """Emit the environment manifest payload.

    Returns:
    -------
    int
        Exit status code.
    """
    payload = scip_env_payload()
    json.dump(payload, sys.stdout, indent=2)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

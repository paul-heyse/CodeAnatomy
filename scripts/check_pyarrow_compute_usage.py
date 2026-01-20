"""Fail when pyarrow.compute is used outside datafusion_engine compute ops."""

from __future__ import annotations

import logging
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
SRC_DIR = ROOT / "src"

ALLOWED_FILES = {
    "src/datafusion_engine/compute_ops.py",
    "src/datafusion_engine/udf_registry.py",
    "src/datafusion_engine/kernels.py",
}


def _scan_file(path: pathlib.Path) -> list[str]:
    rel = str(path.relative_to(ROOT))
    if rel in ALLOWED_FILES:
        return []
    text = path.read_text(encoding="utf-8")
    hits: list[str] = []
    if "pyarrow.compute" in text:
        hits.append("pyarrow.compute import")
    if "from arrowdsl.core.interop import" in text and " pc" in text:
        hits.append("arrowdsl.core.interop pc import")
    return hits


def main() -> int:
    """Return a non-zero exit when disallowed pyarrow.compute usage is found.

    Returns
    -------
    int
        Exit code for the check.
    """
    logger = logging.getLogger(__name__)
    failures: list[str] = []
    for path in SRC_DIR.rglob("*.py"):
        hits = _scan_file(path)
        if not hits:
            continue
        rel = str(path.relative_to(ROOT))
        failures.append(f"{rel}: {', '.join(hits)}")
    if not failures:
        return 0
    for line in failures:
        logger.error("Disallowed pyarrow.compute usage detected: %s", line)
    return 1


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    sys.exit(main())

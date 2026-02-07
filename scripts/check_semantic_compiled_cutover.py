"""Advisory checker for semantic-compiled cutover guardrails."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

_DEFAULT_ROOT = Path(__file__).resolve().parents[1]

_CHECKS: tuple[tuple[str, tuple[str, ...]], ...] = (
    (
        "direct build_semantic_ir usage outside compile-context internals",
        ("build_semantic_ir(",),
    ),
    (
        "imperative extract template switch usage",
        ("_extract_outputs_for_template(",),
    ),
    (
        "legacy extractor required-input map usage",
        ("_REQUIRED_INPUTS", "_SUPPORTS_PLAN"),
    ),
    (
        "legacy relspec extractor extra-input map usage",
        ("_EXTRACTOR_EXTRA_INPUTS",),
    ),
    (
        "legacy semantic runtime bridge usage",
        ("semantic_runtime_from_profile(",),
    ),
    (
        "legacy semantic runtime bridge apply usage",
        ("apply_semantic_runtime_config(",),
    ),
    (
        "legacy semantic runtime bridge import",
        ("datafusion_engine.semantics_runtime",),
    ),
    (
        "legacy wrapper semantic input validation usage",
        ("require_semantic_inputs(",),
    ),
    (
        "legacy extract.helpers import usage",
        ("from extract.helpers import",),
    ),
    (
        "duplicate dataset-location map helper usage",
        ("_dataset_location_map(",),
    ),
    (
        "legacy cpg_nodes_v1 reference",
        ("cpg_nodes_v1",),
    ),
    (
        "legacy cpg_edges_v1 reference",
        ("cpg_edges_v1",),
    ),
    (
        "orchestration-layer dataset_catalog_from_profile usage",
        ("dataset_catalog_from_profile(",),
    ),
    (
        "task_execution-local extract adapter executor mapping",
        ("_EXTRACT_ADAPTER_EXECUTORS",),
    ),
)

_ALLOWLIST_SUFFIXES: tuple[str, ...] = (
    "src/semantics/ir_pipeline.py",
    "src/semantics/compile_context.py",
    "tests/semantics/test_semantic_ir_snapshot.py",
)

# Compile-boundary modules allowed to call dataset_catalog_from_profile.
_CATALOG_FROM_PROFILE_ALLOWLIST: tuple[str, ...] = (
    # Compile-boundary resolver construction.
    "src/semantics/compile_context.py",
    # Dataset registry source module defining dataset_catalog_from_profile itself.
    "src/datafusion_engine/dataset/registry.py",
)

_EXTRACT_EXECUTOR_ALLOWLIST: tuple[str, ...] = (
    "src/hamilton_pipeline/modules/extract_execution_registry.py",
)


def _iter_python_files(root: Path) -> list[Path]:
    return sorted(path for path in root.rglob("*.py") if ".venv" not in path.parts)


def _is_allowlisted(path: Path) -> bool:
    normalized = str(path).replace("\\", "/")
    return any(normalized.endswith(suffix) for suffix in _ALLOWLIST_SUFFIXES)


def _is_catalog_from_profile_allowlisted(path: Path) -> bool:
    """Check if a file is in the compile-boundary allowlist for dataset_catalog_from_profile."""
    normalized = str(path).replace("\\", "/")
    return any(
        normalized.endswith(suffix) if not suffix.endswith("/") else suffix in normalized
        for suffix in _CATALOG_FROM_PROFILE_ALLOWLIST
    )


def _is_extract_executor_allowlisted(path: Path) -> bool:
    normalized = str(path).replace("\\", "/")
    return any(
        normalized.endswith(suffix) if not suffix.endswith("/") else suffix in normalized
        for suffix in _EXTRACT_EXECUTOR_ALLOWLIST
    )


def _find_violations(root: Path) -> list[str]:
    findings: list[str] = []
    for path in _iter_python_files(root / "src"):
        text = path.read_text(encoding="utf-8")
        for label, patterns in _CHECKS:
            if all(pattern in text for pattern in patterns):
                if (
                    "build_semantic_ir(" in patterns
                    or "cpg_nodes_v1" in patterns
                    or "cpg_edges_v1" in patterns
                ) and _is_allowlisted(path):
                    continue
                if (
                    "dataset_catalog_from_profile(" in patterns
                    and _is_catalog_from_profile_allowlisted(path)
                ):
                    continue
                if (
                    "_EXTRACT_ADAPTER_EXECUTORS" in patterns
                    and _is_extract_executor_allowlisted(path)
                ):
                    continue
                findings.append(f"{label}: {path}")
    return findings


def _write_line(message: str) -> None:
    sys.stdout.write(f"{message}\n")


def main() -> int:
    """Run semantic-compiled cutover advisory checks.

    Returns:
        int: Exit status code.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=_DEFAULT_ROOT)
    parser.add_argument("--strict", action="store_true")
    args = parser.parse_args()

    root = args.root.resolve()
    findings = _find_violations(root)
    if not findings:
        _write_line("semantic cutover check: no findings")
        return 0

    _write_line("semantic cutover check: findings detected")
    for finding in findings:
        _write_line(f" - {finding}")
    if args.strict:
        _write_line("semantic cutover check: strict mode enabled, failing")
        return 1
    _write_line("semantic cutover check: advisory mode, continuing")
    return 0


if __name__ == "__main__":
    sys.exit(main())

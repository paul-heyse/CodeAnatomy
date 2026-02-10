# ruff: noqa: T201
"""Scan for Hamilton and rustworkx residuals outside hamilton_pipeline/.

This script detects leftover Hamilton/rustworkx imports and references
in src/ that should have been migrated to engine-native equivalents.

Usage:
    scripts/check_hamilton_residuals.py
    scripts/check_hamilton_residuals.py --root /path/to/repo
    scripts/check_hamilton_residuals.py --json
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path

FORBIDDEN_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("hamilton_import", re.compile(r"from\s+hamilton[^_]")),
    ("hamilton_import_mod", re.compile(r"import\s+hamilton[^_]")),
    ("hamilton_pipeline_from", re.compile(r"from\s+hamilton_pipeline")),
    ("hamilton_pipeline_import", re.compile(r"import\s+hamilton_pipeline")),
    ("datafusion_hamilton_events", re.compile(r"datafusion_hamilton_events")),
    ("hamilton_events_prefix", re.compile(r"hamilton_events_")),
    ("otel_node_hook", re.compile(r"OtelNodeHook")),
    ("otel_plan_hook", re.compile(r"OtelPlanHook")),
    ("hamilton_config", re.compile(r"HamiltonConfig")),
    ("hamilton_tracker", re.compile(r"HamiltonTracker")),
    ("rustworkx_from", re.compile(r"from\s+rustworkx")),
    ("rustworkx_import", re.compile(r"import\s+rustworkx")),
)

# Lines matching these patterns are excluded (comments, docstrings in context)
EXCLUSION_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"^\s*#"),  # Comment lines
    re.compile(r'^\s*"""'),  # Docstring delimiters
    re.compile(r"^\s*'''"),  # Single-quote docstring delimiters
)


@dataclass
class Residual:
    """A single Hamilton/rustworkx residual finding."""

    file: str
    line_number: int
    line_text: str
    pattern_name: str


@dataclass
class ScanResult:
    """Result of a Hamilton residual scan."""

    residuals: list[Residual] = field(default_factory=list)
    files_scanned: int = 0

    @property
    def clean(self) -> bool:
        """Return True when no residuals were found."""
        return len(self.residuals) == 0


def scan_file(path: Path, root: Path) -> list[Residual]:
    """Scan a single Python file for forbidden patterns.

    Excludes matches found in comments and docstrings.

    Returns:
    -------
    list[Residual]
        Residual findings from the file.
    """
    residuals: list[Residual] = []
    try:
        text = path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError):
        return residuals

    lines = text.splitlines()
    in_docstring = False
    docstring_delimiter = None

    for line_number, line in enumerate(lines, start=1):
        stripped = line.strip()

        # Track docstring state
        if stripped.startswith('"""'):
            if in_docstring and docstring_delimiter == '"""':
                in_docstring = False
                docstring_delimiter = None
            else:
                in_docstring = True
                docstring_delimiter = '"""'
            continue
        if stripped.startswith("'''"):
            if in_docstring and docstring_delimiter == "'''":
                in_docstring = False
                docstring_delimiter = None
            else:
                in_docstring = True
                docstring_delimiter = "'''"
            continue

        # Skip comments, docstring content
        if stripped.startswith("#") or in_docstring:
            continue

        # Check for forbidden patterns
        for pattern_name, pattern in FORBIDDEN_PATTERNS:
            if pattern.search(line):
                rel_path = str(path.relative_to(root))
                residuals.append(
                    Residual(
                        file=rel_path,
                        line_number=line_number,
                        line_text=line.strip(),
                        pattern_name=pattern_name,
                    )
                )
    return residuals


def scan_directory(
    root: Path,
    scan_dir: Path,
    exclude_dirs: tuple[str, ...] = ("hamilton_pipeline",),
) -> ScanResult:
    """Scan a directory tree for Hamilton/rustworkx residuals.

    Parameters
    ----------
    root
        Repository root for relative path reporting.
    scan_dir
        Directory to scan (typically src/).
    exclude_dirs
        Directory names to exclude from scanning.

    Returns:
    -------
    ScanResult
        Aggregated scan results with residuals and file count.
    """
    result = ScanResult()
    for path in sorted(scan_dir.rglob("*.py")):
        # Skip excluded directories
        if any(excl in path.parts for excl in exclude_dirs):
            continue
        result.files_scanned += 1
        result.residuals.extend(scan_file(path, root))
    return result


def main() -> int:
    """Run the Hamilton/rustworkx residual scanner CLI.

    Returns:
    -------
    int
        Exit code: 0 if clean, 1 if residuals found.
    """
    parser = argparse.ArgumentParser(description="Scan for Hamilton/rustworkx residuals")
    parser.add_argument("--root", type=Path, default=Path.cwd(), help="Repository root")
    parser.add_argument("--json", action="store_true", dest="json_output", help="JSON output")
    parser.add_argument(
        "--include-hamilton-pipeline",
        action="store_true",
        help="Include hamilton_pipeline/ in scan (default: exclude)",
    )
    args = parser.parse_args()

    root: Path = args.root.resolve()
    src_dir = root / "src"
    if not src_dir.is_dir():
        print(f"ERROR: {src_dir} is not a directory", file=sys.stderr)
        return 1

    exclude_dirs = () if args.include_hamilton_pipeline else ("hamilton_pipeline",)
    result = scan_directory(root, src_dir, exclude_dirs=exclude_dirs)

    if args.json_output:
        payload = {
            "clean": result.clean,
            "files_scanned": result.files_scanned,
            "residual_count": len(result.residuals),
            "residuals": [
                {
                    "file": r.file,
                    "line_number": r.line_number,
                    "line_text": r.line_text,
                    "pattern_name": r.pattern_name,
                }
                for r in result.residuals
            ],
        }
        print(json.dumps(payload, indent=2))
    else:
        print(f"Scanned {result.files_scanned} files in {src_dir}")
        if result.clean:
            print("CLEAN: No Hamilton/rustworkx residuals found.")
        else:
            print(f"FOUND {len(result.residuals)} residual(s):")
            for r in result.residuals:
                print(f"  {r.file}:{r.line_number} [{r.pattern_name}] {r.line_text}")

    return 0 if result.clean else 1


if __name__ == "__main__":
    sys.exit(main())
